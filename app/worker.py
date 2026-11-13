"""独立采集进程（compose 中部署两个：collector-a / collector-b）。

每个进程内含多个并发采集槽；一个槽等待慢报告时，其它槽与另一个进程照常取数。
刷新协调通过 SQLite 跨进程完成，本进程不持有任何独占的令牌状态。
所有 SQLite 同步调用通过独立 DB 线程池执行，不阻塞事件循环。
"""
from __future__ import annotations

import asyncio
import os
import sys

from .asyncio_util import run_db

from . import collector, config, db, tokens
from .lab_client import LabAPIError, LabClient, ReportNotFound, ReportUnauthorized
from .tokens import AccountPaused, ReauthSuperseded, TokenPersistError

# 演示用：在“刷新 HTTP 已成功、新令牌尚未落盘”时杀死本进程（模拟刷新中途退出）
CRASH_AFTER_REFRESH_HTTP = os.environ.get("CRASH_AFTER_REFRESH_HTTP", "")
# 演示用：在“采集到结果但尚未保存”时杀死本进程
CRASH_AFTER_FETCH = os.environ.get("CRASH_AFTER_FETCH", "")


async def _process_job(worker: str, job_id: int, report_no: str, slot: int) -> None:
    actor = f"{worker}/s{slot}"
    try:
        token = await tokens.get_access_token(actor)
    except AccountPaused as exc:
        db.log_event(actor, "job_deferred", f"账户暂停，任务 {report_no} 留在队列：{exc}")
        await run_db(
            collector.requeue_job, report_no,
            f"账户暂停待重新授权: {exc}", False,
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
                    collector.requeue_job, report_no, f"重试后仍 401: {exc.body}"
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
                    f"账户暂停待重新授权: {exc2}", False,
                )
                return
            except TokenPersistError as exc2:
                await run_db(
                    collector.requeue_job, report_no, f"令牌保存失败，按未成功处理: {exc2}"
                )
                return
    else:  # pragma: no cover
        await run_db(collector.requeue_job, report_no, "未知重试状态")
        return

    if CRASH_AFTER_FETCH and worker == CRASH_AFTER_FETCH:
        # 仅演示：模拟进程在“采集到结果但尚未保存”前崩溃，任务会在重启后被回收
        db.log_event(actor, "demo_crash", f"演示：进程在保存 {report_no} 前退出")
        os._exit(86)

    try:
        await run_db(collector.save_result, report_no, data, actor)
    except Exception as exc:
        # 保存失败：绝不上报成功，任务退回队列稍后重试
        db.log_event(actor, "result_persist_fail", f"{report_no} 结果保存失败: {exc}")
        await run_db(collector.requeue_job, report_no, f"结果保存失败: {exc}")
        return

    db.log_event(
        actor,
        "job_done",
        f"报告 {report_no} 采集成功：样本 {data['sample_no']}，"
        f"应测 {data['expected_seeds']} 粒，发芽 {data['germinated_seeds']} 粒",
    )


async def _slot_loop(worker: str, slot: int) -> None:
    actor = f"{worker}/s{slot}"
    while True:
        try:
            # 即使没有待采集任务，也要巡检刷新租约，保证刷新进程崩溃后能熔断暂停
            await tokens.sweep_stale_lease(actor)
            job = await run_db(collector.claim_next_job, worker)
        except Exception as exc:
            db.log_event(actor, "claim_error", str(exc))
            await asyncio.sleep(1.0)
            continue
        if job is None:
            await asyncio.sleep(0.5)
            continue
        try:
            await _process_job(worker, job["id"], job["report_no"], slot)
        except ReportNotFound as exc:
            await run_db(collector.requeue_job, job["report_no"], str(exc))
            db.log_event(actor, "job_not_found", str(exc))
        except LabAPIError as exc:
            await run_db(collector.requeue_job, job["report_no"], str(exc))
            db.log_event(actor, "job_api_error", str(exc))
        except (AccountPaused, ReauthSuperseded) as exc:
            await run_db(
                collector.requeue_job, job["report_no"],
                f"账户状态: {exc}", False,
            )
        except Exception as exc:  # 防御：单任务异常不杀进程
            await run_db(
                collector.requeue_job, job["report_no"], f"未预期错误: {exc!r}"
            )
            db.log_event(actor, "job_unexpected", repr(exc))


async def main(worker: str) -> None:
    await run_db(db.init_db)
    db.log_event(worker, "worker_start",
                 f"采集进程 {worker} 启动，{config.WORKER_SLOTS} 个并发槽")
    slots = [asyncio.create_task(_slot_loop(worker, i)) for i in range(config.WORKER_SLOTS)]
    await asyncio.gather(*slots)


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else config.WORKER_NAME
    try:
        asyncio.run(main(name))
    except KeyboardInterrupt:
        pass
