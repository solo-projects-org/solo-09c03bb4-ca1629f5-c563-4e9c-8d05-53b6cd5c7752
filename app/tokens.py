"""OAuth2 令牌生命周期协调（多进程安全）。

多个独立采集进程 + web 进程共用一个 OAuth2 账户，本模块保证：

1. 单飞刷新：同一过期令牌只允许一个进程刷新。
   进程内用 asyncio.Lock 串行；跨进程用 SQLite 的 IMMEDIATE 事务抢占
   refresh_state 行（in_progress + 租约），其它进程轮询等待结果。
2. 刷新令牌轮换：每次刷新由服务端作废旧 refresh_token，本模块原子写入全新令牌对；
   写入失败绝不返回成功，账户进入暂停等待重新授权。
3. 迟到 401 不废新令牌：401 回显的 jti 与库中当前 jti 不一致时，仅重试，不触发刷新。
4. 刷新结果无法确认（网络/5xx/刷新进程中途退出导致租约超时）-> 暂停账户并提示重新授权。
5. 授权纪元 auth_epoch：重新授权纪元 +1 并清空刷新状态；迟到的刷新结果因纪元不匹配被丢弃。

所有 SQLite 同步调用通过独立 DB 线程池执行，不阻塞事件循环。
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from typing import Optional, Tuple

from .asyncio_util import run_db

from . import config, db
from .lab_client import AmbiguousAuthError, AuthError, LabClient, TokenSet

# 演示钩子：刷新 HTTP 已成功返回、新令牌尚未落盘时杀死本进程，
# 模拟“刷新进程中途退出”，由其它进程靠租约超时熔断并暂停账户
_CRASH_AFTER_REFRESH_HTTP = os.environ.get("CRASH_AFTER_REFRESH_HTTP", "")

# 进程内锁：同一进程内的并发协程串行进入协调区
_PROC_LOCKS: dict[str, asyncio.Lock] = {}


def _lock() -> asyncio.Lock:
    lock = _PROC_LOCKS.get(config.ACCOUNT_ID)
    if lock is None:
        lock = asyncio.Lock()
        _PROC_LOCKS[config.ACCOUNT_ID] = lock
    return lock


class AccountPaused(Exception):
    """账户已暂停，需要重新授权。"""


class ReauthSuperseded(Exception):
    """等待/刷新期间发生了重新授权，本次刷新结果作废。"""


class TokenPersistError(Exception):
    """新令牌无法落盘——按规则不能上报采集成功。"""


# ---------------- 读侧辅助（同步，线程池中执行） ----------------
def _valid(acct: sqlite3.Row, margin: int = config.TOKEN_EXPIRY_MARGIN) -> bool:
    return bool(
        acct["access_token"]
        and acct["access_expires_at"]
        and acct["access_expires_at"] - margin > time.time()
    )


def _snapshot_sync() -> dict:
    """供网页展示账户与刷新协调状态。"""
    conn = db.connect()
    try:
        acct = db.get_account(conn)
        rs = db.get_refresh_state(conn)
        return {
            "status": acct["status"],
            "pause_reason": acct["pause_reason"],
            "access_jti": acct["access_jti"],
            "refresh_token_masked": _mask(acct["refresh_token"]),
            "access_expires_at": acct["access_expires_at"],
            "expires_in": round(acct["access_expires_at"] - time.time(), 1)
            if acct["access_expires_at"]
            else None,
            "auth_epoch": acct["auth_epoch"],
            "refresh_state": rs["state"],
            "refresh_claimer": rs["claimer"],
            "refresh_lease_expires_at": rs["lease_expires_at"],
        }
    finally:
        conn.close()


async def snapshot() -> dict:
    return await run_db(_snapshot_sync)


def _sweep_stale_lease_sync(actor: str) -> Optional[str]:
    """巡检刷新租约：若 in_progress 已过租约，判定刷新进程中途退出 -> 暂停账户。

    返回暂停原因；租约正常或没有进行中的刷新时返回 None。
    该检查不依赖是否有待采集任务，保证刷新进程崩溃后即使队列空闲也能熔断。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            acct = db.get_account(conn)
            if acct["status"] == "paused":
                return None
            rs = db.get_refresh_state(conn)
            now = time.time()
            if rs["state"] != "in_progress":
                return None
            if rs["lease_expires_at"] and rs["lease_expires_at"] > now:
                return None  # 租约仍有效，刷新可能还在进行
            reason = (
                f"刷新租约超时（{config.REFRESH_LEASE_SECONDS}s 未完成），"
                f"刷新进程 {rs['claimer']} 疑似中途退出，刷新结果无法确认，请重新授权"
            )
            conn.execute(
                "UPDATE oauth_account SET status='paused', pause_reason=?, "
                "updated_at=? WHERE id=?",
                (reason, now, config.ACCOUNT_ID),
            )
            conn.execute(
                "UPDATE refresh_state SET state='idle', claimer=NULL, "
                "started_at=NULL, lease_expires_at=NULL WHERE account_id=?",
                (config.ACCOUNT_ID,),
            )
            db.log_event_in_txn(conn, actor, "refresh_lease_timeout", reason)
            return reason
    finally:
        conn.close()


async def sweep_stale_lease(actor: str) -> Optional[str]:
    return await run_db(_sweep_stale_lease_sync, actor)


def _mask(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    return token[:8] + "…" + token[-4:] if len(token) > 12 else "****"


# ---------------- 主入口：取一个可用访问令牌 ----------------
async def get_access_token(actor: str) -> str:
    """返回有效 access_token；必要时执行（或等待）一次刷新。"""
    async with _lock():
        while True:
            decision = await run_db(_decide_sync, actor)
            if decision[0] == "return":
                return decision[1]
            if decision[0] == "wait":
                await asyncio.sleep(config.REFRESH_POLL_INTERVAL)
                continue
            if decision[0] == "claimed":
                claimed_epoch = decision[1]
                return await _do_refresh(actor, claimed_epoch)
            if decision[0] == "paused":
                raise AccountPaused(decision[1])
            raise RuntimeError(f"未知协调决策: {decision}")


def _decide_sync(actor: str) -> tuple:
    """单次数据库决策：直接返回 / 等待 / 抢到刷新权 / 账户暂停。"""
    conn = db.connect()
    try:
        with db.txn(conn):
            acct = db.get_account(conn)
            if acct["status"] == "paused":
                return ("paused", acct["pause_reason"] or "账户已暂停，请重新授权")
            if _valid(acct):
                db.log_event_in_txn(conn, actor, "token_reuse",
                             f"复用访问令牌 jti={acct['access_jti']}")
                return ("return", acct["access_token"])

            rs = db.get_refresh_state(conn)
            now = time.time()
            if rs["state"] == "in_progress":
                if rs["lease_expires_at"] and rs["lease_expires_at"] > now:
                    # 已有其它进程（或本进程协程）在刷新 -> 等待，绝不重复刷新
                    db.log_event_in_txn(
                        conn, actor,
                        "refresh_wait",
                        f"等待 {rs['claimer']} 完成刷新（epoch={rs['auth_epoch']}）",
                    )
                    return ("wait",)
                # 租约超时：刷新进程很可能中途退出，结果无法确认 -> 暂停
                reason = (
                    f"刷新租约超时（{config.REFRESH_LEASE_SECONDS}s 未完成），"
                    "疑似刷新进程中途退出，刷新结果无法确认，请重新授权"
                )
                conn.execute(
                    "UPDATE oauth_account SET status='paused', pause_reason=?, "
                    "updated_at=? WHERE id=?",
                    (reason, now, config.ACCOUNT_ID),
                )
                conn.execute(
                    "UPDATE refresh_state SET state='idle', claimer=NULL, "
                    "started_at=NULL, lease_expires_at=NULL WHERE account_id=?",
                    (config.ACCOUNT_ID,),
                )
                db.log_event_in_txn(conn, actor, "refresh_lease_timeout", reason)
                return ("paused", reason)

            # 抢占刷新权（同一过期令牌全局只刷一次）
            claimed_epoch = acct["auth_epoch"]
            conn.execute(
                "UPDATE refresh_state SET state='in_progress', claimer=?, "
                "auth_epoch=?, started_at=?, lease_expires_at=? WHERE account_id=?",
                (actor, claimed_epoch, now, now + config.REFRESH_LEASE_SECONDS,
                 config.ACCOUNT_ID),
            )
            db.log_event_in_txn(conn, actor, "refresh_claim",
                         f"抢到刷新权，开始刷新（epoch={claimed_epoch}）")
            return ("claimed", claimed_epoch)
    finally:
        conn.close()


async def _do_refresh(actor: str, epoch: int) -> str:
    """实际执行刷新并落盘。调用时已持有刷新认领。"""
    refresh_token = await run_db(_read_refresh_token_sync)
    if not refresh_token:
        await run_db(_pause, actor, epoch, "缺少刷新令牌，请重新授权")
        raise AccountPaused("缺少刷新令牌，请重新授权")

    try:
        tokens = await LabClient().refresh(refresh_token)
    except AmbiguousAuthError as exc:
        # 结果无法确认：服务端可能已轮换作废旧令牌，不能重试 -> 暂停
        await run_db(_pause, actor, epoch,
                                 f"刷新结果无法确认（{exc}），请重新授权")
        raise AccountPaused(str(exc)) from exc
    except AuthError as exc:
        await run_db(_pause, actor, epoch,
                                 f"刷新被授权服务器拒绝（{exc}），请重新授权")
        raise AccountPaused(str(exc)) from exc

    if _CRASH_AFTER_REFRESH_HTTP and actor.startswith(_CRASH_AFTER_REFRESH_HTTP):
        # 演示：服务端已轮换并作废旧刷新令牌，但新令牌还没落盘，进程此刻退出
        print(f"[demo-crash] {actor} 在刷新成功后、落盘前退出（租约将超时熔断）", flush=True)
        os._exit(86)

    try:
        await run_db(_write_new_tokens_sync, actor, epoch, tokens)
    except ReauthSuperseded:
        raise
    except sqlite3.Error as exc:
        # 落盘失败：新令牌无法使用且旧刷新令牌大概率已作废 -> 暂停并要求重新授权
        db.log_event(actor, "token_persist_fail", f"新令牌写入数据库失败: {exc}")
        await run_db(_safe_pause, epoch,
                                 f"新令牌保存失败（{exc}），请重新授权")
        raise TokenPersistError(str(exc)) from exc

    return tokens.access_token


def _read_refresh_token_sync() -> Optional[str]:
    conn = db.connect()
    try:
        return db.get_account(conn)["refresh_token"]
    finally:
        conn.close()


def _write_new_tokens_sync(actor: str, epoch: int, tokens: TokenSet) -> None:
    """写入新令牌对；与协调状态在同一 IMMEDIATE 事务原子提交。"""
    conn = db.connect()
    try:
        with db.txn(conn):
            acct = db.get_account(conn)
            if acct["auth_epoch"] != epoch:
                # 刷新期间发生了重新授权：迟到刷新结果一律丢弃，不得覆盖新授权
                conn.execute(
                    "UPDATE refresh_state SET state='idle', claimer=NULL, "
                    "started_at=NULL, lease_expires_at=NULL WHERE account_id=?",
                    (config.ACCOUNT_ID,),
                )
                db.log_event_in_txn(
                    conn, actor,
                    "refresh_discarded_late",
                    f"刷新晚于重新授权（刷新 epoch={epoch}，"
                    f"当前 epoch={acct['auth_epoch']}），结果丢弃",
                )
                raise ReauthSuperseded("刷新结果被重新授权覆盖，已丢弃")
            now = time.time()
            conn.execute(
                "UPDATE oauth_account SET status='active', pause_reason=NULL, "
                "access_token=?, access_jti=?, refresh_token=?, access_expires_at=?, "
                "access_ttl=?, updated_at=? WHERE id=?",
                (
                    tokens.access_token,
                    tokens.jti,
                    tokens.refresh_token,
                    now + tokens.expires_in,
                    tokens.expires_in,
                    now,
                    config.ACCOUNT_ID,
                ),
            )
            conn.execute(
                "UPDATE refresh_state SET state='idle', claimer=NULL, started_at=NULL, "
                "lease_expires_at=NULL WHERE account_id=?",
                (config.ACCOUNT_ID,),
            )
            db.log_event_in_txn(
                conn, actor,
                "refresh_ok",
                "令牌轮换成功：新 jti=" + tokens.jti
                + "，新刷新令牌已落盘，旧刷新令牌已被服务端作废",
            )
    finally:
        conn.close()


# ---------------- 401 处理：迟到的 401 不能废弃新令牌 ----------------
async def handle_401(actor: str, jti_from_401: Optional[str]) -> Tuple[str, str]:
    """处理报告接口 401。

    返回 ("retry", token)：令牌未变，直接用当前令牌重试（迟到 401 场景）；
    返回 ("refreshed", token)：确认是当前令牌失效，已单飞刷新，用新令牌重试。
    """
    outcome = await run_db(_handle_401_sync, actor, jti_from_401)
    if outcome[0] == "paused":
        raise AccountPaused(outcome[1])
    if outcome[0] == "retry":
        return "retry", outcome[1]
    # outcome[0] == "need_refresh"
    new_token = await get_access_token(actor)
    return "refreshed", new_token


def _handle_401_sync(actor: str, jti_from_401: Optional[str]) -> tuple:
    conn = db.connect()
    try:
        acct = db.get_account(conn)
        if acct["status"] == "paused":
            return ("paused", acct["pause_reason"] or "账户已暂停，请重新授权")
        current_jti = acct["access_jti"]
        if jti_from_401 and current_jti and jti_from_401 != current_jti:
            # 401 来自一条迟到的旧请求，其令牌早已被轮换：不动当前新令牌，直接重试
            db.log_event(
                actor,
                "late_401_ignored",
                f"迟到 401 的 jti={jti_from_401} 与当前 jti={current_jti} "
                "不一致，保持新令牌不刷新",
            )
            return ("retry", acct["access_token"])

        # jti 一致（或无法判断）：当前令牌确实被拒，标记过期后走单飞刷新
        with db.txn(conn):
            conn.execute(
                "UPDATE oauth_account SET access_expires_at=0 WHERE id=?",
                (config.ACCOUNT_ID,),
            )
        db.log_event(
            actor,
            "token_force_expired",
            f"401 确认当前令牌失效（jti={jti_from_401}），触发刷新",
        )
        return ("need_refresh",)
    finally:
        conn.close()


# ---------------- 重新授权 ----------------
def _apply_reauthorization_sync(tokens: TokenSet, actor: str) -> None:
    """重新授权成功：纪元 +1，替换全部凭据，并解除任何进行中的刷新。

    无论刷新协调处于什么状态都可执行（等待刷新时仍可重新授权）；
    此后迟到的刷新结果会因 auth_epoch 不匹配被丢弃。
    """
    conn = db.connect()
    try:
        with db.txn(conn):
            acct = db.get_account(conn)
            new_epoch = acct["auth_epoch"] + 1
            now = time.time()
            conn.execute(
                "UPDATE oauth_account SET status='active', pause_reason=NULL, "
                "access_token=?, access_jti=?, refresh_token=?, access_expires_at=?, "
                "access_ttl=?, auth_epoch=?, updated_at=? WHERE id=?",
                (
                    tokens.access_token,
                    tokens.jti,
                    tokens.refresh_token,
                    now + tokens.expires_in,
                    tokens.expires_in,
                    new_epoch,
                    now,
                    config.ACCOUNT_ID,
                ),
            )
            # 清掉进行中的刷新认领，等待方立刻可见新令牌
            conn.execute(
                "UPDATE refresh_state SET state='idle', claimer=NULL, auth_epoch=?, "
                "started_at=NULL, lease_expires_at=NULL WHERE account_id=?",
                (new_epoch, config.ACCOUNT_ID),
            )
            db.log_event_in_txn(
                conn, actor,
                "reauthorize_ok",
                f"重新授权完成，授权纪元推进到 {new_epoch}，进行中的刷新结果将被丢弃",
            )
    finally:
        conn.close()


async def apply_reauthorization(tokens: TokenSet, actor: str) -> None:
    await run_db(_apply_reauthorization_sync, tokens, actor)


# ---------------- 暂停辅助 ----------------
def _pause(actor: str, epoch: int, reason: str) -> None:
    _safe_pause(epoch, reason)
    db.log_event(actor, "account_paused", reason)


def _safe_pause(epoch: int, reason: str) -> None:
    """仅当授权纪元未变化时暂停（避免覆盖更新的重新授权），并释放刷新认领。"""
    try:
        conn = db.connect()
        try:
            with db.txn(conn):
                acct = db.get_account(conn)
                if acct["auth_epoch"] != epoch:
                    return
                conn.execute(
                    "UPDATE oauth_account SET status='paused', pause_reason=?, "
                    "updated_at=? WHERE id=?",
                    (reason, time.time(), config.ACCOUNT_ID),
                )
                conn.execute(
                    "UPDATE refresh_state SET state='idle', claimer=NULL, "
                    "started_at=NULL, lease_expires_at=NULL WHERE account_id=?",
                    (config.ACCOUNT_ID,),
                )
        finally:
            conn.close()
    except sqlite3.Error as exc:  # pragma: no cover - 库不可用时等租约超时兜底
        print(f"[pause-fail] {reason}: {exc}", flush=True)
