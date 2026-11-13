"""SQLite 持久层。

凭据（令牌）与采集结果都保存在数据卷的同一个库文件中。
- WAL 模式 + busy_timeout，支持多个进程同时读写；
- 所有写操作走 BEGIN IMMEDIATE 事务，跨进程串行化，刷新认领/结果保存可原子提交；
- 保存结果失败时事务回滚，任务不会被标记为成功。
"""
from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Iterator, Optional

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS oauth_account (
    id                  TEXT PRIMARY KEY,
    status              TEXT NOT NULL,                 -- active | paused
    pause_reason        TEXT,
    access_token        TEXT,
    access_jti          TEXT,
    refresh_token       TEXT,
    access_expires_at   REAL,
    access_ttl          INTEGER,
    auth_epoch          INTEGER NOT NULL DEFAULT 0,
    updated_at          REAL
);

-- 刷新协调（每个账户一行；多进程通过 IMMEDIATE 事务抢占）
CREATE TABLE IF NOT EXISTS refresh_state (
    account_id      TEXT PRIMARY KEY,
    state           TEXT NOT NULL,                     -- idle | in_progress
    claimer         TEXT,
    auth_epoch      INTEGER NOT NULL,
    started_at      REAL,
    lease_expires_at REAL
);

CREATE TABLE IF NOT EXISTS collection_job (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_no       TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'pending',   -- pending | running | done | failed
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    claimed_by      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_status ON collection_job(status);

CREATE TABLE IF NOT EXISTS germination_result (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    report_no           TEXT NOT NULL UNIQUE,
    sample_no           TEXT NOT NULL,
    expected_seeds      INTEGER NOT NULL,              -- 应测粒数
    germinated_seeds    INTEGER NOT NULL,              -- 发芽粒数
    collected_by        TEXT,
    collected_at        REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    actor       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    detail      TEXT
);
"""


def connect() -> sqlite3.Connection:
    """新建一条连接（短连接 + WAL，天然适配多进程）。"""
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=30.0,
        isolation_level=None,  # 手动事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def txn(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """写事务：BEGIN IMMEDIATE 立即拿写锁，跨进程互斥；异常即回滚。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_db() -> None:
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        now = time.time()
        # 预置共用账户（尚未授权）
        conn.execute(
            """INSERT INTO oauth_account(id, status, auth_epoch, updated_at)
               VALUES (?, 'paused', 0, ?)
               ON CONFLICT(id) DO NOTHING""",
            (config.ACCOUNT_ID, now),
        )
        conn.execute(
            """INSERT INTO refresh_state(account_id, state, auth_epoch)
               VALUES (?, 'idle', 0)
               ON CONFLICT(account_id) DO NOTHING""",
            (config.ACCOUNT_ID,),
        )
        # 上次进程被杀死时可能残留 running 任务，启动时回收为 pending
        conn.execute(
            "UPDATE collection_job SET status='pending', claimed_by=NULL "
            "WHERE status='running'"
        )
    finally:
        conn.close()


def _insert_event(conn: sqlite3.Connection, actor: str, kind: str, detail: str) -> None:
    conn.execute(
        "INSERT INTO event_log(ts, actor, kind, detail) VALUES (?,?,?,?)",
        (time.time(), actor, kind, detail[:1000]),
    )


def log_event(actor: str, kind: str, detail: str = "") -> None:
    """事件日志：独立短事务写入（用于事务外部调用）。

    注意：严禁在已打开的写事务内部调用本函数——它会新建连接并再次
    BEGIN IMMEDIATE，形成同进程自等锁。事务内部请用 log_event_in_txn。
    失败不影响主流程。
    """
    try:
        conn = connect()
        try:
            with txn(conn):
                _insert_event(conn, actor, kind, detail)
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - 仅防御
        print(f"[event-log-fail] {kind}: {exc}", flush=True)


def log_event_in_txn(conn: sqlite3.Connection, actor: str, kind: str,
                     detail: str = "") -> None:
    """在调用方已持有的事务连接内写日志，复用同一连接、同一事务。"""
    try:
        _insert_event(conn, actor, kind, detail)
    except sqlite3.Error as exc:  # pragma: no cover - 仅防御
        print(f"[event-log-txn-fail] {kind}: {exc}", flush=True)


def get_account(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM oauth_account WHERE id=?", (config.ACCOUNT_ID,)
    ).fetchone()


def get_refresh_state(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM refresh_state WHERE account_id=?", (config.ACCOUNT_ID,)
    ).fetchone()
    assert row is not None
    return row
