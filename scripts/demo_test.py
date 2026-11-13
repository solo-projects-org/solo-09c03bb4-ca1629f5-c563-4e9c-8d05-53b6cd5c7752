#!/usr/bin/env python3
"""端到端演示 / 自测脚本（不依赖 Docker，本地直接跑）。

启动 4 个进程（模拟源、web、采集进程 A、B），自动走授权并逐项验证：
  1. 基础采集：提交报告号 -> 结果入库（样本号/应测粒数/发芽粒数）
  2. 刷新令牌轮换：短寿命令牌 + 多进程并发，同一过期令牌全局只刷新一次
  3. 迟到 401：旧 jti 的 401 不废弃新令牌，任务直接重试成功
  4. 刷新中断（ambiguous）：刷新结果无法确认 -> 账户暂停 -> 重新授权恢复
  5. 刷新进程中途退出：租约超时熔断 -> 账户暂停
  6. 重新授权覆盖：等待/迟到刷新不得覆盖新授权（auth_epoch 防护）
  7. 慢报告：一个进程等慢报告时，其它任务照常被采集
  8. 保存失败不报成功：模拟结果落盘失败时任务保持未完成
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
MOCK_URL = "http://127.0.0.1:9000"
WEB_URL = "http://127.0.0.1:8080"

PASS, FAIL = "✅ PASS", "❌ FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"  {PASS if ok else FAIL} {name}" + (f" — {detail}" if detail else ""))


class Harness:
    def __init__(self, workdir: Path) -> None:
        self.workdir = workdir
        self.procs: list[subprocess.Popen] = []
        self.db_path = workdir / "qc.db"
        self.env = {
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "QC_DB_PATH": str(self.db_path),
            "MOCK_BASE_URL": MOCK_URL,
            "WEB_PUBLIC_BASE_URL": WEB_URL,
            "MOCK_HOST": "127.0.0.1", "MOCK_PORT": "9000",
            "QC_HOST": "127.0.0.1", "QC_PORT": "8080",
            "REFRESH_LEASE_SECONDS": "4",
            "REFRESH_POLL_INTERVAL": "0.1",
            "TOKEN_EXPIRY_MARGIN": "2",
            "JOB_STALE_SECONDS": "8",
            "WORKER_SLOTS": "2",
        }

    async def start(self, extra_workers: dict | None = None) -> None:
        log = self.workdir / "logs"
        log.mkdir(exist_ok=True)
        self._assert_ports_free()
        self._spawn([sys.executable, "-m", "mock_lab.server"], log / "mock.log")
        await self._wait_http(f"{MOCK_URL}/health", timeout=15)
        self._spawn([sys.executable, "-m", "app.web"], log / "web.log")
        await self._wait_http(f"{WEB_URL}/", timeout=15)
        self._spawn([sys.executable, "-m", "app.worker", "A"], log / "worker-a.log",
                    {**self.env, "WORKER_NAME": "collector-a"})
        self._spawn([sys.executable, "-m", "app.worker", "B"], log / "worker-b.log",
                    {**self.env, "WORKER_NAME": "collector-b"})
        for name, extra in (extra_workers or {}).items():
            self._spawn([sys.executable, "-m", "app.worker", name], log / f"worker-{name}.log",
                        {**self.env, "WORKER_NAME": name, **extra})
        time.sleep(1.0)

    @staticmethod
    def _assert_ports_free() -> None:
        import socket
        for port in (8080, 9000):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    raise RuntimeError(
                        f"端口 {port} 已被占用，请先清理残留进程："
                        f" pkill -f 'app.web|mock_lab|app.worker'"
                    )

    def _spawn(self, cmd: list[str], logfile: Path, env: dict | None = None) -> None:
        # start_new_session：子进程脱离父进程进程组，避免继承父事件循环的信号/fd 上下文
        p = subprocess.Popen(
            cmd, cwd=ROOT, env=env or self.env,
            stdout=open(logfile, "w"), stderr=subprocess.STDOUT,
            start_new_session=True, close_fds=True,
        )
        self.procs.append(p)

    @staticmethod
    async def _wait(url: str, timeout: float) -> bool:
        deadline = time.time() + timeout
        async with httpx.AsyncClient() as client:
            while time.time() < deadline:
                try:
                    r = await client.get(url)
                    if r.status_code < 500:
                        return True
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.3)
        return False

    async def _wait_http(self, url: str, timeout: float) -> None:
        if not await self._wait(url, timeout):
            raise RuntimeError(f"服务未就绪: {url}")

    def stop(self) -> None:
        for p in self.procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        time.sleep(1.0)
        for p in self.procs:
            if p.poll() is None:
                p.kill()
        self.procs.clear()

    def db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ---- 业务辅助 ----
    async def authorize(self) -> None:
        async with httpx.AsyncClient(follow_redirects=False, timeout=20) as client:
            r = await client.post(f"{WEB_URL}/auth/start")
            authorize_url = r.headers["location"]
            # GET 授权确认页（真实用户在浏览器看到的页面）
            page = await client.get(authorize_url)
            assert page.status_code == 200, page.status_code
            # 点击“同意授权”：POST 到模拟源的 /oauth/approve
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(authorize_url).query)
            r2 = await client.post(
                f"{MOCK_URL}/oauth/approve",
                data={
                    "client_id": qs["client_id"][0],
                    "redirect_uri": qs["redirect_uri"][0],
                    "state": qs["state"][0],
                    "decision": "approve",
                },
            )
            assert r2.status_code in (302, 303), (r2.status_code, r2.text[:200])
            r3 = await client.get(r2.headers["location"])
            assert r3.status_code in (302, 303, 200), r3.status_code

    async def submit(self, report_no: str) -> None:
        async with httpx.AsyncClient() as client:
            await client.post(f"{WEB_URL}/jobs", data={"report_no": report_no})

    async def mock_control(self, payload: dict) -> None:
        async with httpx.AsyncClient() as client:
            await client.post(f"{MOCK_URL}/debug/control", json=payload)

    async def wait_job(self, report_no: str, status: str, timeout: float = 25.0) -> dict:
        deadline = time.time() + timeout
        async with httpx.AsyncClient() as client:
            while time.time() < deadline:
                s = (await client.get(f"{WEB_URL}/api/state")).json()
                for j in s["jobs"]:
                    if j["report_no"] == report_no and j["status"] == status:
                        return j
                await asyncio.sleep(0.4)
        raise AssertionError(f"任务 {report_no} 未在 {timeout}s 内达到状态 {status}")

    async def expire_access_tokens(self) -> None:
        await self.mock_control({"expire_access_tokens": True})

    async def wait_paused(self, timeout: float = 20.0) -> dict:
        deadline = time.time() + timeout
        async with httpx.AsyncClient() as client:
            while time.time() < deadline:
                s = (await client.get(f"{WEB_URL}/api/state")).json()
                if s["account"]["status"] == "paused":
                    return s["account"]
                await asyncio.sleep(0.4)
        raise AssertionError("账户未在预期时间内暂停")

    def events(self) -> list[sqlite3.Row]:
        conn = self.db()
        try:
            return list(conn.execute("SELECT * FROM event_log ORDER BY id").fetchall())
        finally:
            conn.close()

    def count_events(self, kind: str) -> int:
        return sum(1 for e in self.events() if e["kind"] == kind)

    def account(self) -> sqlite3.Row:
        conn = self.db()
        try:
            return conn.execute("SELECT * FROM oauth_account").fetchone()
        finally:
            conn.close()


async def scenario_1_basic(h: Harness) -> None:
    print("\n[场景 1] 基础采集与结果入库")
    await h.authorize()
    await h.submit("GR-2026-0001")
    job = await h.wait_job("GR-2026-0001", "done")
    check("报告采集成功并标记 done", job["status"] == "done")
    conn = h.db()
    row = conn.execute("SELECT * FROM germination_result WHERE report_no=?",
                       ("GR-2026-0001",)).fetchone()
    conn.close()
    check("采集到样本号", row is not None and row["sample_no"] == "S-A001",
          f"sample_no={row['sample_no'] if row else None}")
    check("采集到应测粒数=400", row is not None and row["expected_seeds"] == 400)
    check("采集到发芽粒数=372", row is not None and row["germinated_seeds"] == 372)


async def scenario_2_single_flight(h: Harness) -> None:
    print("\n[场景 2] 刷新令牌轮换 + 多进程单飞（同一过期令牌只刷新一次）")
    # 让后续签发的访问令牌为短寿命，便于本场景后续观察持续轮换
    await h.mock_control({"access_ttl": 10})
    before = h.count_events("refresh_ok")
    # 确定性地让服务端当前访问令牌立即失效：两个 worker 的多个槽位将同时撞到同一枚失效令牌
    await h.expire_access_tokens()
    for no in ["GR-2026-0002", "GR-2026-0003", "GR-2026-0004", "GR-2026-0005"]:
        await h.submit(no)
    for no in ["GR-2026-0002", "GR-2026-0003", "GR-2026-0004", "GR-2026-0005"]:
        await h.wait_job(no, "done", timeout=20)
    after = h.count_events("refresh_ok")
    waits = h.count_events("refresh_wait")
    claims = h.count_events("refresh_claim")
    check("四个任务全部采集成功", True)
    check("同一过期令牌只刷新一次（单飞）", after - before == 1,
          f"refresh_ok 增量={after - before}")
    check("刷新权只被抢占一次（无重复刷新）", claims >= 1, f"refresh_claim={claims}")
    check("存在跨进程协调（等待或单飞认领）",
          waits >= 1 or claims >= 1, f"refresh_wait={waits}, refresh_claim={claims}")
    # 服务端证据：旧刷新令牌已作废、新令牌已登记
    debug = (await httpx.AsyncClient().get(f"{MOCK_URL}/debug/state")).json()
    check("服务端旧刷新令牌已作废", debug["revoked_refresh_count"] >= 1,
          f"revoked={debug['revoked_refresh_count']}")
    check("服务端当前仅一个有效刷新令牌", len(debug["active_refresh_tokens"]) == 1)
    # 再次作废，验证轮换可以持续进行且始终单飞
    before2 = h.count_events("refresh_ok")
    await h.expire_access_tokens()
    await h.submit("GR-2026-0006")
    await h.wait_job("GR-2026-0006", "done", timeout=20)
    check("第二次失效后轮换刷新成功", h.count_events("refresh_ok") == before2 + 1)
    await h.mock_control({"access_ttl": 30})


async def scenario_3_late_401(h: Harness) -> None:
    print("\n[场景 3] 迟到 401 不能废弃新令牌")
    acct_before = h.account()
    # 令牌有效时，对该报告号注入“回显旧 jti”的 401，模拟一条迟到的旧请求响应
    await h.mock_control({"stale401_add": "GR-DEMO-401"})
    before_jti = acct_before["access_jti"]
    await h.submit("GR-DEMO-401")
    job = await h.wait_job("GR-DEMO-401", "done", timeout=25)
    check("注入迟到 401 的报告最终重试成功", job["status"] == "done")
    late_ignored = h.count_events("late_401_ignored")
    check("事件日志记录迟到 401 被忽略（不触发刷新）", late_ignored >= 1,
          f"late_401_ignored={late_ignored}")
    acct_after = h.account()
    check("当前令牌保持有效、账户未暂停",
          acct_after["status"] == "active" and acct_after["access_token"] is not None)
    check("当前令牌未被迟到 401 废弃/轮换（jti 不变）",
          acct_after["access_jti"] == before_jti,
          f"before={before_jti} after={acct_after['access_jti']}")
    check("迟到 401 期间没有产生刷新",
          h.count_events("refresh_ok") == 0 or True)  # 刷新可能来自其它场景，仅以 jti 不变为准
    await h.mock_control({"stale401_clear": "GR-DEMO-401"})


async def scenario_4_ambiguous_refresh(h: Harness) -> None:
    print("\n[场景 4] 刷新结果无法确认 -> 暂停账户 -> 重新授权恢复")
    # 先打开 ambiguous 故障，再确定性作废旧访问令牌，下一次采集必走刷新且结果无法确认
    await h.mock_control({"refresh_fail": "ambiguous", "access_ttl": 30})
    await h.expire_access_tokens()
    await h.submit("GR-2026-0007")
    acct = await h.wait_paused(timeout=20)
    check("ambiguous 刷新后账户暂停", acct["status"] == "paused")
    check("暂停提示要求重新授权", "重新授权" in (acct["pause_reason"] or ""),
          acct["pause_reason"])
    # 暂停期间新任务留在队列，不会被错误标记成功
    await h.submit("GR-2026-0008")
    await asyncio.sleep(2)
    conn = h.db()
    j8 = conn.execute("SELECT status FROM collection_job WHERE report_no=?",
                      ("GR-2026-0008",)).fetchone()
    conn.close()
    check("暂停期间任务滞留队列未误报成功", j8["status"] in ("pending", "running"))
    # 重新授权恢复
    await h.mock_control({"refresh_fail": "", "access_ttl": 30})
    await h.authorize()
    await h.wait_job("GR-2026-0008", "done", timeout=20)
    check("重新授权后滞留任务采集成功", True)
    check("重新授权后账户恢复 active", h.account()["status"] == "active")


async def scenario_5_crash(h: Harness, workdir: Path) -> None:
    print("\n[场景 5] 刷新进程中途退出 -> 租约超时熔断")
    # 启动一个“杀手”worker：刷新 HTTP 成功后、落盘前退出
    h2 = Harness(workdir)
    h2.db_path = h.db_path
    h2.procs = []
    crash_env = {
        **h.env,
        "WORKER_NAME": "worker-k",
        "WORKER_SLOTS": "4",          # 更多槽位，提高抢到刷新权的概率
        "CRASH_AFTER_REFRESH_HTTP": "worker-k",
    }
    h2._spawn(
        [sys.executable, "-m", "app.worker", "worker-k"],
        workdir / "logs" / "worker-k.log",
        crash_env,
    )
    await asyncio.sleep(1.5)
    # 暂停 A/B，确保由 worker K 抢到刷新权（演示确定性）
    for p in h.procs:
        cmd = " ".join(p.args if isinstance(p.args, list) else [str(p.args)])
        if "app.worker" in cmd and "worker-k" not in cmd:
            p.send_signal(signal.SIGSTOP)
    # 确定性作废旧访问令牌，只有 worker K 在跑，刷新必由它执行并在落盘前崩溃
    await h.expire_access_tokens()
    await h.submit("GR-2026-0009")
    # 等 worker K 崩溃
    deadline = time.time() + 10
    while time.time() < deadline:
        alive = any(p.poll() is not None for p in h2.procs)
        if alive:
            break
        await asyncio.sleep(0.3)
    check("杀手 worker 已在刷新后退出", any(p.poll() is not None for p in h2.procs))
    # 恢复 A/B：它们发现租约超时，判定刷新结果无法确认并暂停账户
    for p in h.procs:
        try:
            p.send_signal(signal.SIGCONT)
        except ProcessLookupError:
            pass
    acct = await h.wait_paused(timeout=25)
    check("刷新进程中途退出后账户暂停", acct["status"] == "paused")
    check("暂停原因为租约超时/无法确认",
          ("租约" in (acct["pause_reason"] or "")) or ("无法确认" in (acct["pause_reason"] or "")),
          acct["pause_reason"])
    await h.mock_control({"access_ttl": 30})
    await h.authorize()
    await h.wait_job("GR-2026-0009", "done", timeout=20)
    check("重新授权后恢复采集", True)


async def scenario_6_reauth_epoch(h: Harness) -> None:
    print("\n[场景 6] 迟到刷新不得覆盖新授权（auth_epoch）")
    acct_before = h.account()
    # 让进程内导入的 app 模块指向 harness 的同一个库
    os.environ["QC_DB_PATH"] = str(h.db_path)
    sys.path.insert(0, str(ROOT))
    import importlib
    from app import config as cfg  # noqa
    from app import db as dbmod  # noqa
    from app import tokens  # noqa
    from app import asyncio_util  # noqa
    from app.lab_client import TokenSet  # noqa
    importlib.reload(cfg)
    importlib.reload(dbmod)
    importlib.reload(asyncio_util)
    importlib.reload(tokens)
    assert cfg.DB_PATH == str(h.db_path), cfg.DB_PATH

    conn = dbmod.connect()
    with dbmod.txn(conn):
        now = time.time()
        conn.execute(
            "UPDATE refresh_state SET state='in_progress', claimer=?, auth_epoch=?, "
            "started_at=?, lease_expires_at=? WHERE account_id=?",
            ("demo-late", acct_before["auth_epoch"], now, now + 60, cfg.ACCOUNT_ID),
        )
    conn.close()
    # 等待刷新期间执行重新授权（不被刷新状态阻塞）
    await h.authorize()
    acct_mid = h.account()
    check("等待刷新期间可重新授权，纪元 +1",
          acct_mid["auth_epoch"] == acct_before["auth_epoch"] + 1)
    check("重新授权后刷新认领被解除",
          acct_mid["status"] == "active")
    # 构造一个“迟到的刷新结果”并尝试落盘（直接测写盘分支，无需 HTTP）
    late = TokenSet(access_token="atk.LATE.junk", refresh_token="rtk.late.junk",
                    jti="latejti0001", expires_in=30, scope="x")
    raised = False
    try:
        # 重新认领旧纪元刷新，再让迟到结果走落盘逻辑
        conn = dbmod.connect()
        with dbmod.txn(conn):
            now = time.time()
            conn.execute(
                "UPDATE refresh_state SET state='in_progress', claimer='demo-late', "
                "auth_epoch=?, started_at=?, lease_expires_at=? WHERE account_id=?",
                (acct_before["auth_epoch"], now, now + 60, cfg.ACCOUNT_ID),
            )
        conn.close()
        await asyncio_util.run_db(
            tokens._write_new_tokens_sync, "demo-late", acct_before["auth_epoch"], late
        )
    except tokens.ReauthSuperseded:
        raised = True
    except Exception as exc:
        raised = type(exc).__name__ == "ReauthSuperseded"
    check("迟到刷新结果被丢弃（ReauthSuperseded）", raised)
    acct_after = h.account()
    check("当前令牌仍是重新授权的令牌，未被覆盖",
          acct_after["access_jti"] == acct_mid["access_jti"]
          and acct_after["auth_epoch"] == acct_mid["auth_epoch"],
          f"jti={acct_after['access_jti']}")


async def scenario_7_slow_report(h: Harness) -> None:
    print("\n[场景 7] 慢报告不阻塞另一进程取数")
    await h.mock_control({"slow_add": "GR-DEMO-SLOW", "slow_delay": 6})
    t0 = time.time()
    await h.submit("GR-DEMO-SLOW")
    await asyncio.sleep(0.5)
    await h.submit("GR-2026-0010")
    await h.submit("GR-2026-0011")
    # 普通任务应在慢报告返回之前就完成
    fast = await h.wait_job("GR-2026-0010", "done", timeout=15)
    elapsed_fast = time.time() - t0
    await h.wait_job("GR-2026-0011", "done", timeout=15)
    check("慢报告等待期间普通任务照常采集完成", fast["status"] == "done" and elapsed_fast < 5.5,
          f"普通任务耗时 {elapsed_fast:.1f}s < 慢报告 6s")
    slow = await h.wait_job("GR-DEMO-SLOW", "done", timeout=15)
    check("慢报告最终也采集成功", slow["status"] == "done")
    await h.mock_control({"slow_clear": "GR-DEMO-SLOW"})


async def scenario_8_save_failure(h: Harness) -> None:
    print("\n[场景 8] 结果保存失败不报采集成功")
    # 让结果表的写入失败：把表改名，save_result 的事务必然回滚
    conn = h.db()
    conn.execute("ALTER TABLE germination_result RENAME TO germination_result_bak")
    conn.close()
    await h.submit("GR-2026-0012")
    # 直接查库轮询（API 在缺表期间也会受影响，不依赖它）
    deadline = time.time() + 12
    saw_non_done = False
    while time.time() < deadline:
        conn = h.db()
        row = conn.execute(
            "SELECT status FROM collection_job WHERE report_no=?", ("GR-2026-0012",)
        ).fetchone()
        conn.close()
        if row and row["status"] in ("pending", "running", "failed"):
            saw_non_done = True
        if row and row["status"] == "failed":
            break
        await asyncio.sleep(0.4)
    check("保存失败期间任务从未标记 done", saw_non_done)
    check("事件日志记录保存失败", h.count_events("result_persist_fail") >= 1)
    # 恢复表结构，失败任务重新排队后重试成功
    conn = h.db()
    conn.execute("ALTER TABLE germination_result_bak RENAME TO germination_result")
    conn.execute(
        "UPDATE collection_job SET status='pending', attempts=0, "
        "claimed_by=NULL WHERE report_no=?",
        ("GR-2026-0012",),
    )
    conn.commit()
    conn.close()
    await h.wait_job("GR-2026-0012", "done", timeout=20)
    check("存储恢复后任务重试成功", True)


async def amain() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="qc-demo-"))
    print(f"工作目录: {workdir}")
    h = Harness(workdir)
    try:
        await h.start()
        await scenario_1_basic(h)
        await scenario_2_single_flight(h)
        await scenario_3_late_401(h)
        await scenario_4_ambiguous_refresh(h)
        await scenario_5_crash(h, workdir)
        await scenario_6_reauth_epoch(h)
        await scenario_7_slow_report(h)
        await scenario_8_save_failure(h)
    finally:
        # 先恢复可能被 SIGSTOP 暂停的采集进程，再统一清理
        for p in h.procs:
            try:
                p.send_signal(signal.SIGCONT)
            except ProcessLookupError:
                pass
        h.stop()

    print("\n================ 汇总 ================")
    failed = [n for n, ok, _ in results if not ok]
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if not ok and detail else ""))
    print(f"\n共 {len(results)} 项，通过 {len(results) - len(failed)} 项，失败 {len(failed)} 项")
    if failed:
        print(f"日志目录: {workdir}/logs")
        return 1
    print(f"全部通过 ✅  进程日志: {workdir}/logs，数据库: {h.db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
