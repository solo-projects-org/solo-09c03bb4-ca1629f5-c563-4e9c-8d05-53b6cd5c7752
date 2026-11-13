#!/usr/bin/env python3
"""任务认领可靠性演示 / 自测脚本（不依赖 Docker，本地直接跑）。

针对“慢报告等待期间刷新页面/重复提交/另一进程启动导致重复认领、
旧请求失败又把任务改回待采”的问题，端到端验证修复效果：

  场景 A 刷新与重提：任务 running 期间反复刷新采集页、重复提交同一报告号、
         再启动第三个采集进程，都不会重复派发，也不会重置尝试次数；
         正常等待慢报告不被误判失联，其它报告照常采集。
  场景 B 退出接管：采集进程优雅退出（SIGTERM）时未完成任务立即被另一进程接管；
         被硬杀（SIGKILL）时在心跳超时后自动接管，结果正常落库。
  场景 C 暂停恢复：进程被冻结过久、任务被接替后再恢复，其迟到的成功与失败
         都因认领纪元（fencing token）不匹配被丢弃，不改写接替者的
         结果、状态和尝试次数。
"""
from __future__ import annotations

import asyncio
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
# 让本脚本进程内的 app 模块与子进程使用同一个库
os.environ.setdefault("QC_DB_PATH", "")
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
        self.db_path = workdir / "qc.db"
        os.environ["QC_DB_PATH"] = str(self.db_path)
        self.env = {
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "QC_DB_PATH": str(self.db_path),
            "MOCK_BASE_URL": MOCK_URL,
            "WEB_PUBLIC_BASE_URL": WEB_URL,
            "MOCK_HOST": "127.0.0.1", "MOCK_PORT": "9000",
            "QC_HOST": "127.0.0.1", "QC_PORT": "8080",
            "REFRESH_LEASE_SECONDS": "8",
            "REFRESH_POLL_INTERVAL": "0.1",
            "TOKEN_EXPIRY_MARGIN": "2",
            "JOB_STALE_SECONDS": "6",      # 心跳 6 秒不刷新才判定失联
            "JOB_HEARTBEAT_SECONDS": "2",  # 每 2 秒一次心跳
            "WORKER_SLOTS": "2",
            "HTTP_TIMEOUT": "30",
        }
        self.procs: dict[str, subprocess.Popen] = {}
        self.logs = workdir / "logs"
        self.logs.mkdir(exist_ok=True)

    def _assert_ports_free(self) -> None:
        import socket
        for port in (8080, 9000):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    raise RuntimeError(
                        f"端口 {port} 被占用，请先清理：pkill -f 'app.web|mock_lab|app.worker'"
                    )

    def _spawn(self, cmd: list[str], logfile: Path, env: dict | None = None) -> subprocess.Popen:
        return subprocess.Popen(
            cmd, cwd=ROOT, env=env or self.env,
            stdout=open(logfile, "w"), stderr=subprocess.STDOUT,
            start_new_session=True, close_fds=True,
        )

    async def start_base(self) -> None:
        self._assert_ports_free()
        self.procs["mock"] = self._spawn(
            [sys.executable, "-m", "mock_lab.server"], self.logs / "mock.log")
        await self._wait_http(f"{MOCK_URL}/health", 15)
        self.procs["web"] = self._spawn(
            [sys.executable, "-m", "app.web"], self.logs / "web.log")
        await self._wait_http(f"{WEB_URL}/", 15)

    def start_worker(self, name: str) -> None:
        if name in self.procs and self.procs[name].poll() is None:
            return
        self.procs[name] = self._spawn(
            [sys.executable, "-m", "app.worker", name],
            self.logs / f"worker-{name}.log",
            {**self.env, "WORKER_NAME": name},
        )

    async def start(self) -> None:
        await self.start_base()
        self.start_worker("collector-a")
        self.start_worker("collector-b")
        await asyncio.sleep(1.2)

    @staticmethod
    async def _wait_http(url: str, timeout: float) -> None:
        deadline = time.time() + timeout
        async with httpx.AsyncClient() as client:
            while time.time() < deadline:
                try:
                    if (await client.get(url)).status_code < 500:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.3)
        raise RuntimeError(f"服务未就绪: {url}")

    def signal_worker(self, name: str, sig: int) -> None:
        p = self.procs[name]
        if p.poll() is None:
            p.send_signal(sig)

    async def wait_worker_exit(self, name: str, timeout: float = 10.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.procs[name].poll() is not None:
                return True
            await asyncio.sleep(0.2)
        return self.procs[name].poll() is not None

    def stop(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        time.sleep(1.0)
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()

    def db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def job(self, report_no: str) -> dict | None:
        conn = self.db()
        try:
            row = conn.execute(
                "SELECT * FROM collection_job WHERE report_no=?", (report_no,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def result(self, report_no: str) -> dict | None:
        conn = self.db()
        try:
            row = conn.execute(
                "SELECT * FROM germination_result WHERE report_no=?", (report_no,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def count_events(self, *kinds: str) -> int:
        conn = self.db()
        try:
            rows = conn.execute(
                f"SELECT kind FROM event_log WHERE kind IN ({','.join('?' * len(kinds))})",
                kinds,
            ).fetchall()
            return len(rows)
        finally:
            conn.close()

    # ---- HTTP 辅助 ----
    async def authorize(self) -> None:
        from urllib.parse import parse_qs, urlparse
        async with httpx.AsyncClient(follow_redirects=False, timeout=20) as client:
            r = await client.post(f"{WEB_URL}/auth/start")
            authorize_url = r.headers["location"]
            await client.get(authorize_url)
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
            assert r2.status_code in (302, 303)
            r3 = await client.get(r2.headers["location"])
            assert r3.status_code in (200, 302, 303)

    async def submit(self, report_no: str) -> int:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{WEB_URL}/jobs", data={"report_no": report_no})
            return r.status_code

    async def get_page(self) -> int:
        async with httpx.AsyncClient() as client:
            return (await client.get(WEB_URL + "/")).status_code

    async def mock_control(self, payload: dict) -> None:
        async with httpx.AsyncClient() as client:
            await client.post(f"{MOCK_URL}/debug/control", json=payload)

    async def wait_job(self, report_no: str, status: str, timeout: float = 30.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            j = self.job(report_no)
            if j and j["status"] == status:
                return j
            await asyncio.sleep(0.3)
        raise AssertionError(f"任务 {report_no} 未在 {timeout}s 内达到 {status}，当前={self.job(report_no)}")

    async def wait_until(self, pred, timeout: float = 20.0):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = pred()
            if last:
                return last
            await asyncio.sleep(0.3)
        raise AssertionError(f"条件未在 {timeout}s 内满足，最后观察={last}")


# ================= 场景 A：刷新页面 / 重复提交 / 新进程都不能重复派发 =================
async def scenario_a_refresh_resubmit(h: Harness) -> None:
    print("\n[场景 A] 任务执行中刷新页面、重复提交、新进程启动均不重复派发")
    slow = "GR-DEMO-SLOW"
    # 慢报告耗时 8s，大于失联阈值 6s：靠心跳证明“等待慢报告”不是失联
    await h.mock_control({"slow_add": slow, "slow_delay": 8})
    await h.submit(slow)
    running = await h.wait_job(slow, "running")
    owner = running["claimed_by"]
    check("慢报告被唯一认领（attempts=1）", running["attempts"] == 1,
          f"claimed_by={owner}, attempts={running['attempts']}")

    # 运行期间：反复刷新采集页 + 重复提交同一报告号
    for _ in range(5):
        assert await h.get_page() == 200
        await h.submit(slow)
        await asyncio.sleep(0.6)
    # 再启动第三个采集进程，也不能抢走仍在正常心跳的任务
    h.start_worker("collector-c")
    await asyncio.sleep(2.0)

    mid = h.job(slow)
    check("刷新/重提/新进程后仍只有一个任务且保持 running",
          mid is not None and mid["status"] == "running")
    check("认领者未变化（未被重复派发）", mid and mid["claimed_by"] == owner,
          f"owner={mid['claimed_by'] if mid else None}")
    check("尝试次数未被重置/增加（仍为 1）", mid and mid["attempts"] == 1,
          f"attempts={mid['attempts'] if mid else None}")

    # 慢报告等待期间，其它报告照常被（其它槽位/进程）采集
    await h.submit("GR-2026-0002")
    fast = await h.wait_job("GR-2026-0002", "done", timeout=15)
    check("慢报告未阻塞其它报告采集", fast["status"] == "done")

    done = await h.wait_job(slow, "done", timeout=20)
    check("慢报告最终采集成功且只尝试一次", done["attempts"] == 1,
          f"attempts={done['attempts']}")
    check("结果已落库", h.result(slow) is not None)
    await h.mock_control({"slow_clear": slow})
    # 恢复双进程部署
    h.signal_worker("collector-c", signal.SIGTERM)
    await h.wait_worker_exit("collector-c")


# ================= 场景 B：进程退出后未完成任务自动接管 =================
async def scenario_b_exit_takeover(h: Harness) -> None:
    print("\n[场景 B] 进程退出（优雅/硬杀）后未完成任务自动接管")

    # --- B1 优雅退出：SIGTERM 立即释放，另一进程秒级接管 ---
    r1 = "GR-2026-0003"
    # 设为慢报告，保证进程退出时任务仍在 running，而不是已经完成
    await h.mock_control({"slow_add": r1, "slow_delay": 4})
    await h.submit(r1)
    running = await h.wait_job(r1, "running")
    owner = running["claimed_by"]
    other = "collector-b" if owner == "collector-a" else "collector-a"
    h.signal_worker(owner, signal.SIGTERM)
    assert await h.wait_worker_exit(owner), f"{owner} 未退出"
    done = await h.wait_job(r1, "done", timeout=15)
    check("优雅退出后任务被另一进程接管并完成", done["status"] == "done")
    check("接管者是另一个采集进程", done["claimed_by"] == other,
          f"claimed_by={done['claimed_by']}")
    check("结果由接管进程保存", (h.result(r1) or {}).get("collected_by", "").startswith(other))
    check("事件日志记录优雅释放", h.count_events("worker_release") >= 1)
    await h.mock_control({"slow_clear": r1})
    h.start_worker(owner)  # 恢复双进程
    await asyncio.sleep(1.0)

    # --- B2 硬杀：无释放动作，靠心跳超时自动接管 ---
    r2 = "GR-2026-0004"
    await h.mock_control({"slow_add": r2, "slow_delay": 3})
    await h.submit(r2)
    running2 = await h.wait_job(r2, "running")
    owner2 = running2["claimed_by"]
    other2 = "collector-b" if owner2 == "collector-a" else "collector-a"
    h.signal_worker(owner2, signal.SIGKILL)
    assert await h.wait_worker_exit(owner2), f"{owner2} 未退出"
    # 心跳超时（6s）后被接管，再完成采集
    done2 = await h.wait_job(r2, "done", timeout=25)
    check("硬杀后心跳超时自动接管并完成", done2["status"] == "done")
    check("接管者是存活的另一个进程", done2["claimed_by"] == other2,
          f"claimed_by={done2['claimed_by']}")
    check("接管延续同一任务：attempts=2（原认领 1 次 + 接管 1 次）",
          done2["attempts"] == 2, f"attempts={done2['attempts']}")
    check("结果正常落库且只保存一次", h.result(r2) is not None)
    await h.mock_control({"slow_clear": r2})
    h.start_worker(owner2)  # 恢复双进程
    await asyncio.sleep(1.0)


# ================= 场景 C：暂停过久被接管，恢复后迟到回写全部丢弃 =================
async def scenario_c_pause_resume(h: Harness) -> None:
    print("\n[场景 C] 冻结进程恢复后，迟到成功/失败不得改写接替者")

    # --- C1 端到端：SIGSTOP 冻结 A，B 心跳超时接管；恢复 A 后其迟到成功被丢弃 ---
    slow = "GR-DEMO-PAUSE"
    await h.mock_control({"slow_add": slow, "slow_delay": 8})
    # 先冻结 B，保证任务由 A 认领
    h.signal_worker("collector-b", signal.SIGSTOP)
    await h.submit(slow)
    running = await h.wait_job(slow, "running", timeout=10)
    assert running["claimed_by"] == "collector-a", running
    epoch_a = running["claim_epoch"]
    # 冻结 A：心跳停摆；再放开 B，由 B 在超时后接管
    h.signal_worker("collector-a", signal.SIGSTOP)
    h.signal_worker("collector-b", signal.SIGCONT)
    taken = await h.wait_until(
        lambda: (lambda j: j if j and j["claimed_by"] == "collector-b"
                 and j["status"] == "running" else None)(h.job(slow)),
        timeout=20,
    )
    check("A 心跳超时后任务被 B 接管（纪元递增）",
          taken["claim_epoch"] == epoch_a + 1, f"epoch {epoch_a} -> {taken['claim_epoch']}")
    check("接管不重置尝试预算（attempts=2）", taken["attempts"] == 2,
          f"attempts={taken['attempts']}")
    done = await h.wait_job(slow, "done", timeout=25)
    result_b = h.result(slow) or {}
    # B 完成后才恢复 A：A 在途请求返回后的迟到保存必须被拒
    h.signal_worker("collector-a", signal.SIGCONT)
    await asyncio.sleep(3.0)
    final = h.job(slow)
    check("最终状态为 done 且归属 B", final["status"] == "done"
          and final["claimed_by"] == "collector-b")
    check("结果由 B 保存，A 的迟到成功未覆盖",
          str(result_b.get("collected_by", "")).startswith("collector-b"),
          f"collected_by={result_b.get('collected_by')}")
    check("尝试次数保持接管后的值（2），未被迟到回写改动",
          final["attempts"] == 2, f"attempts={final['attempts']}")
    check("事件日志记录迟到成功被丢弃",
          h.count_events("late_result_discarded", "late_success_ignored") >= 1)
    await h.mock_control({"slow_clear": slow})

    # --- C2 函数级：直接验证迟到失败不能退回、迟到成功不能覆盖（确定性 fencing） ---
    sys.path.insert(0, str(ROOT))
    from app import collector  # noqa: E402  使用本进程 QC_DB_PATH 指向同一个库
    report = "GR-2026-0020"
    assert collector.enqueue_job(report) == "queued"
    old_job = collector.claim_next_job("demo-old")
    assert old_job is not None and old_job["claimed_by"] == "demo-old"
    epoch_old = old_job["claim_epoch"]
    # 模拟 demo-old 失联：把心跳拨到很久以前，让 demo-new 接管
    conn = h.db()
    conn.execute("UPDATE collection_job SET heartbeat_at=? WHERE report_no=?",
                 (time.time() - 60, report))
    conn.commit()
    conn.close()
    new_job = collector.claim_next_job("demo-new")
    assert new_job is not None and new_job["report_no"] == report
    epoch_new = new_job["claim_epoch"]
    check("接管时认领纪元递增", epoch_new == epoch_old + 1,
          f"{epoch_old} -> {epoch_new}")

    # 旧进程的迟到失败：必须被忽略，任务保持 running、尝试次数不变
    attempts_before = h.job(report)["attempts"]
    outcome = collector.requeue_job(report, "旧进程迟到失败", "demo-old", epoch_old)
    after_fail = h.job(report)
    check("迟到失败返回 stale_ignored", outcome == "stale_ignored", outcome)
    check("迟到失败未把任务改回 pending/failed", after_fail["status"] == "running"
          and after_fail["claimed_by"] == "demo-new")
    check("迟到失败未改动尝试次数", after_fail["attempts"] == attempts_before)

    # 旧进程的迟到成功：必须抛 JobOwnershipLost，结果不落库、状态不改写
    raised = False
    try:
        collector.save_result(
            report,
            {"sample_no": "S-OLD", "expected_seeds": 1, "germinated_seeds": 1},
            "demo-old/s0", "demo-old", epoch_old,
        )
    except collector.JobOwnershipLost:
        raised = True
    check("迟到成功抛 JobOwnershipLost 并整体回滚", raised)
    check("迟到成功未写入任何结果", h.result(report) is None)
    check("迟到成功未改写任务状态", h.job(report)["status"] == "running")

    # 新进程用新纪元正常保存：结果与 done 同事务落库
    collector.save_result(
        report,
        {"sample_no": "S-NEW", "expected_seeds": 400, "germinated_seeds": 388},
        "demo-new/s0", "demo-new", epoch_new,
    )
    final2 = h.job(report)
    res = h.result(report) or {}
    check("接替者正常保存后任务 done", final2["status"] == "done")
    check("结果属于接替者且数据正确",
          res.get("collected_by") == "demo-new/s0"
          and res.get("germinated_seeds") == 388, str(res))


async def amain() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="qc-fencing-"))
    print(f"工作目录: {workdir}")
    h = Harness(workdir)
    try:
        await h.start()
        await h.authorize()
        await scenario_a_refresh_resubmit(h)
        await scenario_b_exit_takeover(h)
        await scenario_c_pause_resume(h)
    finally:
        for name in tuple(h.procs):
            try:
                h.signal_worker(name, signal.SIGCONT)  # 防 SIGSTOP 残留
            except (ProcessLookupError, KeyError):
                pass
        h.stop()

    print("\n================ 汇总 ================")
    failed = [n for n, ok, _ in results if not ok]
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if not ok and detail else ""))
    print(f"\n共 {len(results)} 项，通过 {len(results) - len(failed)} 项，失败 {len(failed)} 项")
    if failed:
        print(f"日志目录: {h.logs}")
        return 1
    print(f"全部通过 ✅  进程日志: {h.logs}，数据库: {h.db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
