"""采集任务队列与结果落库（SQLite 即队列）。

并发安全模型（认领纪元 + 心跳）：

- 每次任务被认领，claim_epoch（fencing token）+1 并返回给认领者；
- 认领进程运行期间周期性刷新 heartbeat_at，正常等待慢报告不会被误判失联；
- 仅当 running 任务的心跳超过 JOB_STALE_SECONDS 未刷新，才判定认领进程
  崩溃/被冻结，由其它进程接管（重新认领，纪元再 +1）；
- 结果保存、失败退回都必须带回认领时的 claim_epoch：纪元不匹配说明任务
  已被接替，迟到的成功/失败一律丢弃，绝不改写接替者的结果、状态与尝试次数；
- 结果与任务完成状态在同一 IMMEDIATE 事务提交，保存失败不报成功。
"""
from __future__ import annotations

import sqlite3
import time
from typing import Optional

from . import config, db


class JobOwnershipLost(Exception):
    """迟到回写：任务已被其它进程接管（claim_epoch 不匹配），本次结果必须丢弃。"""


def enqueue_job(report_no: str) -> str:
    """提交报告号；同一报告号幂等，绝不影响正在执行的任务。

    - 已完成：already_done；
    - 正在执行（running）或已排队（pending）：保持现状，不重置状态与尝试次数；
    - 已失败：重新排队并清零尝试次数（人工重新提交的新尝试）。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            now = time.time()
            existing = conn.execute(
                "SELECT id, status FROM collection_job WHERE report_no=?", (report_no,)
            ).fetchone()
            if existing:
                if existing["status"] == "done":
                    return "already_done"
                if existing["status"] in ("running", "pending"):
                    # 任务仍在执行或排队：重复提交不重新派发、不重置尝试次数
                    return f"already_{existing['status']}"
                # failed：重新排队，清零尝试预算
                conn.execute(
                    "UPDATE collection_job SET status='pending', attempts=0, last_error=NULL, "
                    "claimed_by=NULL, updated_at=? WHERE id=?",
                    (now, existing["id"]),
                )
                return "requeued"
            conn.execute(
                "INSERT INTO collection_job(report_no, status, created_at, updated_at) "
                "VALUES (?, 'pending', ?, ?)",
                (report_no, now, now),
            )
            return "queued"
    finally:
        conn.close()


def claim_next_job(worker: str) -> Optional[sqlite3.Row]:
    """原子认领一个待采集任务（跨进程互斥）。

    认领前先回收“心跳超时”的孤儿任务（认领它的进程崩溃或被冻结），
    使其重新可被采集；心跳仍新鲜的 running 任务绝不会被重复派发，
    因此正常等待慢报告、刷新采集页或重复提交都不会触发二次认领。
    回收不重置 attempts，接管是同一项工作的延续。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            # running 且心跳超过阈值才视为进程失联，回收为 pending
            stale_before = time.time() - config.JOB_STALE_SECONDS
            conn.execute(
                "UPDATE collection_job SET status='pending', claimed_by=NULL "
                "WHERE status='running' AND COALESCE(heartbeat_at, updated_at) < ?",
                (stale_before,),
            )
            row = conn.execute(
                "SELECT * FROM collection_job WHERE status='pending' "
                "ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            now = time.time()
            # 认领：纪元 +1（fencing token），心跳从认领时刻起算
            cur = conn.execute(
                "UPDATE collection_job SET status='running', claimed_by=?, "
                "attempts=attempts+1, claim_epoch=claim_epoch+1, "
                "heartbeat_at=?, updated_at=? WHERE id=?",
                (worker, now, now, row["id"]),
            )
            if cur.rowcount != 1:  # pragma: no cover - 同一事务内不会发生
                return None
            return conn.execute(
                "SELECT * FROM collection_job WHERE id=?", (row["id"],)
            ).fetchone()
    finally:
        conn.close()


def heartbeat_job(worker: str) -> int:
    """刷新本进程名下所有 running 任务的心跳。返回刷新的行数。

    只更新 heartbeat_at，不动状态与尝试次数；已被接管的任务不再属于本进程，
    自然不会被匹配到。短事务，调用方周期性执行。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            cur = conn.execute(
                "UPDATE collection_job SET heartbeat_at=? "
                "WHERE status='running' AND claimed_by=?",
                (time.time(), worker),
            )
            return cur.rowcount
    finally:
        conn.close()


def save_result(report_no: str, data: dict, actor: str,
                worker: str, claim_epoch: int) -> None:
    """结果与任务完成状态在同一事务提交；提交失败则任务保持未完成，绝不上报成功。

    带认领纪元校验：若任务已被其它进程接替（纪元不匹配或已不在本进程名下），
    本进程的迟到成功将被整体丢弃（结果行与状态更新一起回滚），
    绝不覆盖接替者已保存的结果、状态和尝试次数。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            job = conn.execute(
                "SELECT status, claim_epoch FROM collection_job WHERE report_no=?",
                (report_no,),
            ).fetchone()
            if job is None or job["status"] != "running" \
                    or job["claim_epoch"] != claim_epoch:
                db.log_event_in_txn(
                    conn, actor, "late_result_discarded",
                    f"{report_no} 已被接替或状态变化（本地纪元={claim_epoch}，"
                    f"当前纪元={None if job is None else job['claim_epoch']}），迟到成功丢弃",
                )
                raise JobOwnershipLost(report_no)
            now = time.time()
            conn.execute(
                "INSERT INTO germination_result"
                "(report_no, sample_no, expected_seeds, germinated_seeds, collected_by, collected_at) "
                "VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(report_no) DO UPDATE SET sample_no=excluded.sample_no, "
                "expected_seeds=excluded.expected_seeds, germinated_seeds=excluded.germinated_seeds, "
                "collected_by=excluded.collected_by, collected_at=excluded.collected_at",
                (
                    report_no,
                    data["sample_no"],
                    int(data["expected_seeds"]),
                    int(data["germinated_seeds"]),
                    actor,
                    now,
                ),
            )
            # 双保险：UPDATE 也带纪元条件，影响行数必须为 1
            cur = conn.execute(
                "UPDATE collection_job SET status='done', last_error=NULL, "
                "heartbeat_at=?, updated_at=? WHERE report_no=? AND claim_epoch=?",
                (now, now, report_no, claim_epoch),
            )
            if cur.rowcount != 1:  # pragma: no cover - 前置检查已覆盖
                raise JobOwnershipLost(report_no)
    finally:
        conn.close()


def requeue_job(report_no: str, error: str, worker: str, claim_epoch: int,
                count_attempt: bool = True) -> str:
    """失败退回队列；超过最大尝试次数则置 failed。

    带认领纪元校验：任务已被接替时，迟到的失败直接忽略，不得把接替者的
    任务改回 pending/failed，也不得改动尝试次数。

    账户暂停属于外部可恢复状态（重新授权即可），不计入失败重试次数，
    任务保持 pending 等待恢复，不会被误判为终态失败。

    返回 "requeued" | "failed" | "stale_ignored"。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            row = conn.execute(
                "SELECT status, attempts, claimed_by, claim_epoch "
                "FROM collection_job WHERE report_no=?",
                (report_no,),
            ).fetchone()
            now = time.time()
            if row is None or row["status"] != "running" \
                    or row["claim_epoch"] != claim_epoch \
                    or row["claimed_by"] != worker:
                db.log_event_in_txn(
                    conn, worker, "late_failure_discarded",
                    f"{report_no} 已被接替或状态变化（本地纪元={claim_epoch}），迟到失败丢弃：{error}",
                )
                return "stale_ignored"
            if count_attempt and row["attempts"] >= config.JOB_MAX_ATTEMPTS:
                conn.execute(
                    "UPDATE collection_job SET status='failed', last_error=?, "
                    "heartbeat_at=?, updated_at=? WHERE report_no=? AND claim_epoch=?",
                    (error, now, now, report_no, claim_epoch),
                )
                return "failed"
            conn.execute(
                "UPDATE collection_job SET status='pending', claimed_by=NULL, "
                "last_error=?, heartbeat_at=NULL, updated_at=? "
                "WHERE report_no=? AND claim_epoch=?",
                (error, now, report_no, claim_epoch),
            )
            if not count_attempt:
                # 退回本次尝试计数，暂停等待不消耗重试预算
                conn.execute(
                    "UPDATE collection_job SET attempts=MAX(0, attempts-1) "
                    "WHERE report_no=?",
                    (report_no,),
                )
            return "requeued"
    finally:
        conn.close()


def release_jobs_of_worker(worker: str) -> int:
    """进程优雅退出时，释放本进程仍在运行的任务，供其它进程立即接管。

    只释放仍归本进程所有的 running 任务（纪元保持不变，下一次认领自增）；
    已被接管的任务不会被触碰。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            cur = conn.execute(
                "UPDATE collection_job SET status='pending', claimed_by=NULL, "
                "heartbeat_at=NULL, updated_at=? "
                "WHERE status='running' AND claimed_by=?",
                (time.time(), worker),
            )
            return cur.rowcount
    finally:
        conn.close()


def list_jobs(limit: int = 100) -> list[sqlite3.Row]:
    conn = db.connect()
    try:
        return list(
            conn.execute(
                "SELECT j.*, r.sample_no, r.expected_seeds, r.germinated_seeds "
                "FROM collection_job j LEFT JOIN germination_result r ON r.report_no=j.report_no "
                "ORDER BY j.id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        )
    finally:
        conn.close()


def list_results() -> list[sqlite3.Row]:
    conn = db.connect()
    try:
        return list(
            conn.execute(
                "SELECT * FROM germination_result ORDER BY id DESC"
            ).fetchall()
        )
    finally:
        conn.close()


def list_events(limit: int = 80) -> list[sqlite3.Row]:
    conn = db.connect()
    try:
        return list(
            conn.execute(
                "SELECT * FROM event_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        )
    finally:
        conn.close()
