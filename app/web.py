"""质控库网页 / API。

功能：发起授权与重新授权、提交报告号入采集队列、查看采集结果与事件日志、
演示控制（缩短令牌寿命、注入迟到 401、慢报告、刷新故障）。

所有 SQLite 同步调用都通过独立 DB 线程池执行，避免阻塞事件循环。
"""
from __future__ import annotations

import secrets
import time
from pathlib import Path
from urllib.parse import quote

import httpx
import uvicorn
from .asyncio_util import run_db
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import collector, config, db, tokens
from .lab_client import AmbiguousAuthError, AuthError, LabClient

app = FastAPI(title="种子发芽试验质控采集库")
_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# 待完成的授权流程 state（单账户演示，内存表足够；进程重启重新走授权即可）
_PENDING_AUTH: dict[str, dict] = {}


def _base() -> str:
    return config.MOCK_BASE_URL.rstrip("/")


# ---------------- 页面 ----------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    await run_db(db.init_db)
    acct = await tokens.snapshot()
    jobs = await run_db(collector.list_jobs)
    results = await run_db(collector.list_results)
    events = await run_db(collector.list_events, 60)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            ctrl = (await client.get(f"{_base()}/debug/control")).json()
    except Exception:
        ctrl = {"_error": "模拟源不可达"}
    return _TEMPLATES.TemplateResponse(
        request,
        "index.html",
        {
            "acct": acct,
            "jobs": jobs,
            "results": results,
            "events": events,
            "control": ctrl,
            "now": time.time(),
        },
    )


@app.get("/api/state")
async def api_state():
    """网页轮询用：账户、任务、结果、事件的全量快照。"""
    return JSONResponse(
        {
            "now": time.time(),
            "account": await tokens.snapshot(),
            "jobs": [dict(r) for r in await run_db(collector.list_jobs)],
            "results": [dict(r) for r in await run_db(collector.list_results)],
            "events": [dict(r) for r in await run_db(collector.list_events, 40)],
        }
    )


# ---------------- OAuth2 授权 / 重新授权 ----------------
@app.post("/auth/start")
async def auth_start():
    await run_db(db.init_db)
    state = secrets.token_hex(8)
    _PENDING_AUTH[state] = {"ts": time.time()}
    redirect_uri = f"{config.WEB_PUBLIC_BASE_URL}/oauth/callback"
    url = (
        f"{_base()}/oauth/authorize?response_type=code"
        f"&client_id={quote(config.OAUTH_CLIENT_ID)}"
        f"&redirect_uri={quote(redirect_uri)}"
        f"&scope={quote(config.OAUTH_SCOPE)}&state={state}"
    )
    await run_db(db.log_event, "web", "auth_start", "跳转检测机构进行授权")
    return RedirectResponse(url, status_code=303)


@app.get("/oauth/callback")
async def oauth_callback(code: str = "", state: str = ""):
    if state not in _PENDING_AUTH:
        return HTMLResponse("授权 state 无效或已过期，请重新发起授权", status_code=400)
    _PENDING_AUTH.pop(state, None)
    redirect_uri = f"{config.WEB_PUBLIC_BASE_URL}/oauth/callback"
    try:
        token_set = await LabClient().exchange_code(code, redirect_uri)
    except (AuthError, AmbiguousAuthError) as exc:
        await run_db(db.log_event, "web", "auth_fail", str(exc))
        return HTMLResponse(f"授权失败：{exc}", status_code=400)

    # 无论是否在等待刷新，重新授权都立即生效（纪元 +1，迟到刷新结果将被丢弃）
    await tokens.apply_reauthorization(token_set, "web")
    return RedirectResponse("/?msg=reauth_ok", status_code=303)


# ---------------- 提交报告号 ----------------
@app.post("/jobs")
async def submit_job(report_no: str = Form(...)):
    report_no = report_no.strip()
    if not report_no:
        return RedirectResponse("/?msg=empty_report", status_code=303)
    outcome = await run_db(collector.enqueue_job, report_no)
    await run_db(db.log_event, "web", "job_submit",
                             f"报告号 {report_no} 入队（{outcome}）")
    return RedirectResponse(f"/?msg={outcome}", status_code=303)


# ---------------- 演示控制（代理到模拟源） ----------------
@app.post("/demo/ttl")
async def demo_ttl(ttl: int = Form(...)):
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(f"{_base()}/debug/control", json={"access_ttl": max(1, ttl)})
    await run_db(db.log_event, "web", "demo_ttl",
                             f"模拟源访问令牌寿命调整为 {ttl}s")
    return RedirectResponse("/?msg=ttl_set", status_code=303)


@app.post("/demo/stale401")
async def demo_stale401(report_no: str = Form("GR-DEMO-401"), clear: str = Form("")):
    payload = {"stale401_clear": report_no} if clear else {"stale401_add": report_no}
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(f"{_base()}/debug/control", json=payload)
    await run_db(db.log_event, "web", "demo_stale401",
                             f"迟到401演示报告 {report_no} clear={bool(clear)}")
    return RedirectResponse("/?msg=stale401_set", status_code=303)


@app.post("/demo/slow")
async def demo_slow(report_no: str = Form("GR-DEMO-SLOW"), delay: float = Form(6.0),
                    clear: str = Form("")):
    if clear:
        payload = {"slow_clear": report_no}
    else:
        payload = {"slow_add": report_no, "slow_delay": delay}
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(f"{_base()}/debug/control", json=payload)
    await run_db(db.log_event, "web", "demo_slow",
                             f"慢报告 {report_no} delay={delay} clear={bool(clear)}")
    return RedirectResponse("/?msg=slow_set", status_code=303)


@app.post("/demo/refresh-fail")
async def demo_refresh_fail(mode: str = Form(""), hang: float = Form(0.0)):
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(
            f"{_base()}/debug/control",
            json={"refresh_fail": mode, "refresh_hang": hang},
        )
    await run_db(db.log_event, "web", "demo_refresh_fail",
                             f"刷新故障模式 mode={mode!r} hang={hang}")
    return RedirectResponse("/?msg=refresh_fail_set", status_code=303)


if __name__ == "__main__":
    db.init_db()
    uvicorn.run(
        app,
        host=config.WEB_HOST,
        port=config.WEB_PORT,
    )
