"""独立采集进程（compose 中部署两个：collector-a / collector-b）。

每个进程内含多个并发采集槽；一个槽等待慢报告时，其它槽与另一个进程照常取数。
刷新协调通过 SQLite 跨进程完成，本进程不持有任何独占的令牌状态。
所有 SQLite 同步调用通过独立 DB 线程池执行，不阻塞事件循环。

任务所有权：
- 认领到任务时同时拿到 claim_epoch（fencing token），后续保存/退回都必须带回；
- 后台心跳协程周期性刷新本进程名下 running 任务的 heartbeat_at，
  正常等待慢报告不会被其它进程误判为失联；
- 进程被 SIGTERM/SIGINT 优雅退出时释放未完成任务，其它进程立即接管；
  被杀死/冻结则由其它进程在心跳超时后接管；
- 任务被接管后，本进程迟到的成功/失败因纪元不匹配被丢弃，
  不会改写接替者的结果、状态和尝试次数。
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys

from .asyncio_util import run_db

from . import collector, config, db, tokens
from .lab_client import LabAPIError, LabClient, ReportNotFound, ReportUnauthorized
from .tokens import AccountPaused, ReauthSuperseded, TokenPersistError

# 演示用：在“刷新 HTTP 已成功、新令牌尚未落盘”时杀死本进程（模拟刷新中途退出）
CRASH_AFTER_REFRESH_HTTP = os.environ.get("CRASH_AFTER_REFRESH_HTTP", "")
# 演示用：在“采集到结果但尚未保存”时杀死本进程
CRASH_AFTER_FETCH = os.environ.get("CRASH_AFTER_FETCH", "")

# 进程级停止信号：收到 SIGTERM/SIGINT 后让各协程退出循环
_STOP_EVENT: asyncio.Event | None = None


async def _process_job(worker: str, job_id: int, report_no: str,
                       claim_epoch: int, slot: int) -> None:
    actor = f"{worker}/s{slot}"
    try:
        token = await tokens.get_access_token(actor)
    except AccountPaused as exc:
        db.log_event(actor, "job_deferred", f"账户暂停，任务 {report_no} 留在队列：{exc}")
        await run_db(
            collector.requeue_job, report_no,
            f"账户暂停待重新授权: {exc}", worker, claim_epoch, False,
        )
        return

    client = LabClient()
    # 最多容忍一次 401 重试（迟到 401 或真实过期刷新后的重试）
    for attempt in (1, 2):
        try:
            data = await client.fetch_report(token, report_no)
            break
        except ReportUnauthorized as exc:
            if attempt == 2:
                await run_db(
                    collector.requeue_job, report_no, f"重试后仍 401: {exc.body}",
                    worker, claim_epoch,
                )
                db.log_event(actor, "job_retry_401", f"{report_no} 第二次 401，退回队列")
                return
            try:
                kind, token = await tokens.handle_401(actor, exc.jti)
                db.log_event(
                    actor,
                    "job_401_retry",
                    f"{report_no} 401 处理={kind}，使用令牌 jti 对应令牌重试",
                )
            except AccountPaused as exc2:
                await run_db(
                    collector.requeue_job, report_no,
                    f"账户暂停待重新授权: {exc2}", worker, claim_epoch, False,
                )
                return
            except TokenPersistError as exc2:
                await run_db(
                    collector.requeue_job, report_no,
                    f"令牌保存失败，按未成功处理: {exc2}", worker, claim_epoch,
                )
                return
    else:  # pragma: no cover
        await run_db(collector.requeue_job, report_no, "未知重试状态",
                     worker, claim_epoch)
        return

    if CRASH_AFTER_FETCH and worker == CRASH_AFTER_FETCH:
        # 仅演示：模拟进程在“采集到结果但尚未保存”前崩溃，任务会在心跳超时后被接管
        db.log_event(actor, "demo_crash", f"演示：进程在保存 {report_no} 前退出")
        os._exit(86)

    try:
        await run_db(collector.save_result, report_no, data, actor,
                     worker, claim_epoch)
    except collector.JobOwnershipLost:
        # 任务已被接替：迟到成功被丢弃，不得改写接替者的结果与状态
        db.log_event(actor, "late_success_ignored",
                     f"{report_no} 已被接替，本进程迟到成功丢弃")
        return
    except Exception as exc:
        # 保存失败：绝不上报成功，任务退回队列稍后重试
        db.log_event(actor, "result_persist_fail", f"{report_no} 结果保存失败: {exc}")
        try:
            await run_db(collector.requeue_job, report_no,
                         f"结果保存失败: {exc}", worker, claim_epoch)
        except collector.JobOwnershipLost:  # pragma: no cover - requeue 内部已消化
            pass
        return

    db.log_event(
        actor,
        "job_done",
        f"报告 {report_no} 采集成功：样本 {data['sample_no']}，"
        f"应测 {data['expected_seeds']} 粒，发芽 {data['germinated_seeds']} 粒",
    )


async def _heartbeat_loop(worker: str) -> None:
    """周期性刷新本进程名下 running 任务的心跳。"""
    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            await run_db(collector.heartbeat_job, worker)
        except Exception as exc:  # 防御：心跳异常不杀进程
            db.log_event(worker, "heartbeat_error", str(exc))
        try:
            await asyncio.wait_for(_STOP_EVENT.wait(),
                                   timeout=config.JOB_HEARTBEAT_SECONDS)
        except asyncio.TimeoutError:
            pass


async def _slot_loop(worker: str, slot: int) -> None:
    actor = f"{worker}/s{slot}"
    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            # 即使没有待采集任务，也要巡检刷新租约，保证刷新进程崩溃后能熔断暂停
            await tokens.sweep_stale_lease(actor)
            job = await run_db(collector.claim_next_job, worker)
        except Exception as exc:
            db.log_event(actor, "claim_error", str(exc))
            try:
                await asyncio.wait_for(_STOP_EVENT.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            continue
        if job is None:
            try:
                await asyncio.wait_for(_STOP_EVENT.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            await _process_job(worker, job["id"], job["report_no"],
                               job["claim_epoch"], slot)
        except ReportNotFound as exc:
            await _safe_requeue(worker, actor, job, str(exc))
            db.log_event(actor, "job_not_found", str(exc))
        except LabAPIError as exc:
            await _safe_requeue(worker, actor, job, str(exc))
            db.log_event(actor, "job_api_error", str(exc))
        except (AccountPaused, ReauthSuperseded) as exc:
            await run_db(
                collector.requeue_job, job["report_no"],
                f"账户状态: {exc}", worker, job["claim_epoch"], False,
            )
        except collector.JobOwnershipLost:
            db.log_event(actor, "late_write_ignored",
                         f"{job['report_no']} 已被接替，迟到回写丢弃")
        except Exception as exc:  # 防御：单任务异常不杀进程
            await _safe_requeue(worker, actor, job, f"未预期错误: {exc!r}")
            db.log_event(actor, "job_unexpected", repr(exc))


async def _safe_requeue(worker: str, actor: str, job, error: str) -> None:
    """退回队列；若任务已被接替则只记录，不做任何改写。"""
    outcome = await run_db(
        collector.requeue_job, job["report_no"], error,
        worker, job["claim_epoch"],
    )
    if outcome == "stale_ignored":
        db.log_event(actor, "late_failure_ignored",
                     f"{job['report_no']} 已被接替，迟到失败丢弃：{error}")


def _install_signal_handlers(worker: str, loop: asyncio.AbstractEventLoop) -> None:
    """优雅退出：停止接新活，释放本进程未完成任务供其它进程立即接管。"""
    def _handler() -> None:
        if _STOP_EVENT is not None:
            _STOP_EVENT.set()

        def _release() -> None:
            try:
                released = collector.release_jobs_of_worker(worker)
                if released:
                    db.log_event(worker, "worker_release",
                                 f"进程退出，释放 {released} 个未完成任务供接管")
            except Exception as exc:  # pragma: no cover - 仅防御
                db.log_event(worker, "release_error", str(exc))

        loop.run_in_executor(None, _release)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handler)
        except NotImplementedError:  # pragma: no cover - Windows 等平台
            signal.signal(sig, lambda *_: _handler())


async def main(worker: str) -> None:
    global _STOP_EVENT
    _STOP_EVENT = asyncio.Event()
    await run_db(db.init_db)
    loop = asyncio.get_running_loop()
    _install_signal_handlers(worker, loop)
    db.log_event(worker, "worker_start",
                 f"采集进程 {worker} 启动，{config.WORKER_SLOTS} 个并发槽")
    tasks = [asyncio.create_task(_heartbeat_loop(worker))]
    tasks += [
        asyncio.create_task(_slot_loop(worker, i))
        for i in range(config.WORKER_SLOTS)
    ]
    await _STOP_EVENT.wait()
    # 信号处理器已先把本进程 running 任务释放为 pending；取消在途协程快速退出
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    db.log_event(worker, "worker_stop", f"采集进程 {worker} 已停止")


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else config.WORKER_NAME
    try:
        asyncio.run(main(name))
    except KeyboardInterrupt:
        pass
