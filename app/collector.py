"""采集任务队列与结果落库（SQLite 即队列）。"""
from __future__ import annotations

import sqlite3
import time
from typing import Optional

from . import config, db


def enqueue_job(report_no: str) -> str:
    """提交报告号；同一报告号幂等，已失败的任务重新排队。"""
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

    认领前先回收“运行超时”的孤儿任务（认领它的进程可能已崩溃），
    使其重新可被采集。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            # running 超过阈值视为进程崩溃遗留，回收为 pending
            stale_before = time.time() - config.JOB_STALE_SECONDS
            conn.execute(
                "UPDATE collection_job SET status='pending', claimed_by=NULL "
                "WHERE status='running' AND updated_at < ?",
                (stale_before,),
            )
            row = conn.execute(
                "SELECT * FROM collection_job WHERE status='pending' "
                "ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE collection_job SET status='running', claimed_by=?, "
                "attempts=attempts+1, updated_at=? WHERE id=?",
                (worker, time.time(), row["id"]),
            )
            return row
    finally:
        conn.close()


def save_result(report_no: str, data: dict, worker: str) -> None:
    """结果与任务完成状态在同一事务提交；提交失败则任务保持未完成，绝不上报成功。"""
    conn = db.connect()
    try:
        with db.txn(conn):
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
                    worker,
                    now,
                ),
            )
            conn.execute(
                "UPDATE collection_job SET status='done', last_error=NULL, updated_at=? "
                "WHERE report_no=?",
                (now, report_no),
            )
    finally:
        conn.close()


def requeue_job(report_no: str, error: str, count_attempt: bool = True) -> None:
    """失败退回队列；超过最大尝试次数则置 failed。

    账户暂停属于外部可恢复状态（重新授权即可），不计入失败重试次数，
    任务保持 pending 等待恢复，不会被误判为终态失败。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            row = conn.execute(
                "SELECT attempts FROM collection_job WHERE report_no=?", (report_no,)
            ).fetchone()
            now = time.time()
            if count_attempt and row and row["attempts"] >= config.JOB_MAX_ATTEMPTS:
                conn.execute(
                    "UPDATE collection_job SET status='failed', last_error=?, updated_at=? "
                    "WHERE report_no=?",
                    (error, now, report_no),
                )
            else:
                conn.execute(
                    "UPDATE collection_job SET status='pending', claimed_by=NULL, "
                    "last_error=?, updated_at=? WHERE report_no=?",
                    (error, now, report_no),
                )
                if not count_attempt:
                    # 退回本次尝试计数，暂停等待不消耗重试预算
                    conn.execute(
                        "UPDATE collection_job SET attempts=MAX(0, attempts-1) "
                        "WHERE report_no=?",
                        (report_no,),
                    )
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
