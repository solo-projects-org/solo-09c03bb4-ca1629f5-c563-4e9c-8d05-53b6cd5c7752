"""本地检测机构模拟源。

模拟一个真实检测机构的 OAuth2 授权服务器与发芽试验报告 API：

- 授权码模式（/oauth/authorize, /oauth/token）；
- 刷新令牌轮换：每次刷新立即作废旧 refresh_token，旧令牌再用即 invalid_grant；
- 访问令牌短期有效，报告接口严格校验，401 响应回显令牌 jti（用于识别迟到 401）；
- 演示控制端点：缩短令牌寿命、注入刷新故障、制造迟到 401、慢报告。
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import os
import secrets
import time
import uuid
from typing import Any, Dict, Optional

import uvicorn
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from app import config

app = FastAPI(title="检测机构模拟源", docs_url="/docs")

# ---- 内存状态（模拟源重启即重新授权，符合真实授权服务器语义） ----
# 当前唯一有效 refresh_token -> 授权信息
REFRESH_TOKENS: Dict[str, Dict[str, Any]] = {}
# 已作废的 refresh_token（轮换后旧令牌进这里）
REVOKED_REFRESH: set[str] = set()
# 授权码 -> 授权信息
AUTH_CODES: Dict[str, Dict[str, Any]] = {}
# access token 字符串 -> 元数据
ACCESS_TOKENS: Dict[str, Dict[str, Any]] = {}

# ---- 演示控制位 ----
CONTROL: Dict[str, Any] = {
    "access_ttl": int(os.environ.get("MOCK_ACCESS_TTL", "20")),
    # refresh 故障模式："" | "ambiguous"（连接中断/挂起，结果无法确认）| "fast"（确定失败 invalid_grant）
    "refresh_fail": "",
    "refresh_hang": 0.0,
    # 命中的报告号让接口返回“迟到 401”（即使令牌本身有效）
    "stale401_reports": set(),
    # 命中的报告号延迟响应（模拟慢报告，期间另一进程仍可取数）
    "slow_reports": set(),
    "slow_delay": 5.0,
}

# 确定性的发芽试验报告数据（报告号 -> 样本号/应测粒数/发芽粒数）
REPORTS: Dict[str, Dict[str, Any]] = {
    "GR-2026-0001": {"sample_no": "S-A001", "expected_seeds": 400, "germinated_seeds": 372},
    "GR-2026-0002": {"sample_no": "S-A002", "expected_seeds": 400, "germinated_seeds": 388},
    "GR-2026-0003": {"sample_no": "S-B101", "expected_seeds": 200, "germinated_seeds": 181},
    "GR-2026-0004": {"sample_no": "S-B102", "expected_seeds": 200, "germinated_seeds": 195},
    "GR-2026-0005": {"sample_no": "S-C210", "expected_seeds": 100, "germinated_seeds": 92},
}
for i in range(6, 26):
    no = f"GR-2026-{i:04d}"
    expected = 400 if i % 2 == 0 else 200
    germ = expected - (i * 7) % 30
    REPORTS[no] = {
        "sample_no": f"S-D{i:03d}",
        "expected_seeds": expected,
        "germinated_seeds": germ,
    }
# 演示专用报告号
REPORTS["GR-DEMO-SLOW"] = {"sample_no": "S-SLOW1", "expected_seeds": 400, "germinated_seeds": 365}
REPORTS["GR-DEMO-401"] = {"sample_no": "S-401X1", "expected_seeds": 400, "germinated_seeds": 350}


def _issue_access(epoch: int) -> Dict[str, str]:
    jti = uuid.uuid4().hex[:12]
    token = f"atk.{jti}.{secrets.token_hex(8)}"
    ttl = int(CONTROL["access_ttl"])
    ACCESS_TOKENS[token] = {
        "jti": jti,
        "epoch": epoch,
        "expires_at": time.time() + ttl,
        "scope": config.OAUTH_SCOPE,
    }
    return {"access_token": token, "jti": jti, "ttl": ttl}


def _issue_refresh(epoch: int) -> str:
    rt = f"rtk.{secrets.token_hex(16)}"
    REFRESH_TOKENS[rt] = {"epoch": epoch, "issued_at": time.time()}
    return rt


def _token_response(epoch: int) -> Dict[str, Any]:
    access = _issue_access(epoch)
    rt = _issue_refresh(epoch)
    return {
        "access_token": access["access_token"],
        "token_type": "Bearer",
        "expires_in": access["ttl"],
        "refresh_token": rt,
        "jti": access["jti"],
        "scope": config.OAUTH_SCOPE,
    }


# ---------------- OAuth2 授权码流程 ----------------
@app.get("/oauth/authorize", response_class=HTMLResponse)
async def authorize(client_id: str, redirect_uri: str, state: str = "", scope: str = ""):
    return f"""
    <html><head><meta charset="utf-8"><title>检测机构授权页</title>
    <style>body{{font-family:sans-serif;max-width:560px;margin:60px auto;padding:0 16px}}
    button{{padding:10px 22px;font-size:15px;margin-right:12px}}</style></head>
    <body><h2>检测机构模拟源 · 授权确认</h2>
    <p>应用 <b>{client_id}</b> 请求访问发芽试验数据（scope: {scope or config.OAUTH_SCOPE}）。</p>
    <p>回调地址：<code>{redirect_uri}</code></p>
    <form method="post" action="/oauth/approve">
      <input type="hidden" name="client_id" value="{client_id}">
      <input type="hidden" name="redirect_uri" value="{redirect_uri}">
      <input type="hidden" name="state" value="{state}">
      <button name="decision" value="approve">同意授权</button>
      <button name="decision" value="deny">拒绝</button>
    </form></body></html>
    """


@app.post("/oauth/approve")
async def approve(
    client_id: str = Form(...),
    redirect_uri: str = Form(...),
    state: str = Form(""),
    decision: str = Form(...),
):
    if decision != "approve":
        return HTMLResponse("<h3>已拒绝授权</h3>", status_code=403)
    code = f"code.{secrets.token_hex(12)}"
    AUTH_CODES[code] = {"client_id": client_id, "redirect_uri": redirect_uri, "ts": time.time()}
    sep = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(f"{redirect_uri}{sep}code={code}&state={state}", status_code=303)


@app.post("/oauth/token")
async def token(
    request: Request,
    grant_type: str = Form(...),
    code: Optional[str] = Form(None),
    redirect_uri: Optional[str] = Form(None),
    refresh_token: Optional[str] = Form(None),
    client_id: Optional[str] = Form(None),
    client_secret: Optional[str] = Form(None),
):
    # 同时支持标准的 HTTP Basic 认证头与表单字段（RFC 6749 两种均合法）
    basic = request.headers.get("authorization", "")
    if basic.lower().startswith("basic "):
        try:
            raw = base64.b64decode(basic[6:]).decode()
            cid, csec = raw.split(":", 1)
            client_id, client_secret = cid, csec
        except (binascii.Error, UnicodeDecodeError, ValueError):
            raise HTTPException(status_code=401, detail="invalid_client_basic")
    if client_id != config.OAUTH_CLIENT_ID or client_secret != config.OAUTH_CLIENT_SECRET:
        raise HTTPException(status_code=401, detail="invalid_client")

    if grant_type == "authorization_code":
        entry = AUTH_CODES.pop(code or "", None)
        if entry is None or entry["redirect_uri"] != redirect_uri:
            raise HTTPException(status_code=400, detail="invalid_grant")
        return _token_response(epoch=0)

    if grant_type == "refresh_token":
        # 演示故障：刷新挂起后断开 —— 调用方无法确认刷新结果
        hang = float(CONTROL.get("refresh_hang") or 0)
        if hang:
            await asyncio.sleep(hang)
            raise HTTPException(status_code=503, detail="connection_lost_during_refresh")
        if CONTROL.get("refresh_fail") == "ambiguous":
            raise HTTPException(status_code=503, detail="connection_lost_during_refresh")
        if CONTROL.get("refresh_fail") == "fast":
            raise HTTPException(status_code=400, detail="invalid_grant (injected)")

        entry = REFRESH_TOKENS.pop(refresh_token or "", None)
        if entry is None:
            # 旧刷新令牌已在轮换时作废，再用即 invalid_grant
            if refresh_token in REVOKED_REFRESH:
                raise HTTPException(status_code=400, detail="invalid_grant: refresh token rotated and revoked")
            raise HTTPException(status_code=400, detail="invalid_grant")

        # 刷新令牌轮换：立即作废旧令牌，签发全新一对
        REVOKED_REFRESH.add(refresh_token)  # type: ignore[arg-type]
        return _token_response(epoch=entry["epoch"] + 1)

    raise HTTPException(status_code=400, detail="unsupported_grant_type")


# ---------------- 发芽试验报告 API ----------------
def _bearer(request: Request) -> Dict[str, Any]:
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing_token")
    token = auth[7:]
    meta = ACCESS_TOKENS.get(token)
    if meta is None:
        # 无法识别的令牌：回显解析出的 jti（本模拟源令牌格式 atk.<jti>.*）
        jti = token.split(".")[1] if token.startswith("atk.") and token.count(".") >= 2 else None
        raise HTTPException(
            status_code=401,
            headers={"WWW-Authenticate": f'Bearer jti="{jti or ""}"'},
            detail="invalid_or_expired_token",
        )
    if meta["expires_at"] <= time.time():
        raise HTTPException(
            status_code=401,
            headers={"WWW-Authenticate": f'Bearer jti="{meta["jti"]}"'},
            detail="access_token_expired",
        )
    return meta


@app.get("/reports/{report_no}")
async def get_report(report_no: str, request: Request):
    meta = _bearer(request)
    if report_no in CONTROL["stale401_reports"]:
        # 模拟“迟到的旧请求”：只对该报告号的第一次请求回一个带旧 jti 的 401，
        # 随后自动解除（真实场景中迟到响应只会到达一次），客户端重试即可成功
        CONTROL["stale401_reports"].discard(report_no)
        stale_jti = ("old" + meta["jti"])[:12]
        raise HTTPException(
            status_code=401,
            headers={"WWW-Authenticate": f'Bearer jti="{stale_jti}"'},
            detail="injected_late_401",
        )
    if report_no in CONTROL["slow_reports"]:
        await asyncio.sleep(float(CONTROL["slow_delay"]))
    report = REPORTS.get(report_no)
    if report is None:
        raise HTTPException(status_code=404, detail="report_not_found")
    return {
        "report_no": report_no,
        "sample_no": report["sample_no"],
        "expected_seeds": report["expected_seeds"],
        "germinated_seeds": report["germinated_seeds"],
        "issued_by": "mock-lab",
        "token_jti": meta["jti"],
    }


# ---------------- 演示控制 / 观测 ----------------
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/debug/state")
async def debug_state():
    return {
        "active_refresh_tokens": list(REFRESH_TOKENS.keys()),
        "revoked_refresh_count": len(REVOKED_REFRESH),
        "access_tokens": [
            {"jti": m["jti"], "epoch": m["epoch"], "expires_in": max(0, round(m["expires_at"] - time.time(), 1))}
            for m in ACCESS_TOKENS.values()
        ],
        "control": {
            "access_ttl": CONTROL["access_ttl"],
            "refresh_fail": CONTROL["refresh_fail"],
            "refresh_hang": CONTROL["refresh_hang"],
            "stale401_reports": sorted(CONTROL["stale401_reports"]),
            "slow_reports": sorted(CONTROL["slow_reports"]),
            "slow_delay": CONTROL["slow_delay"],
        },
    }


@app.post("/debug/control")
async def debug_control(request: Request):
    body = await request.json()
    if "access_ttl" in body:
        CONTROL["access_ttl"] = max(1, int(body["access_ttl"]))
    if "refresh_fail" in body:
        CONTROL["refresh_fail"] = body["refresh_fail"]
    if "refresh_hang" in body:
        CONTROL["refresh_hang"] = float(body["refresh_hang"])
    if "stale401_add" in body:
        CONTROL["stale401_reports"].add(body["stale401_add"])
    if "stale401_clear" in body:
        CONTROL["stale401_reports"].discard(body["stale401_clear"])
    if "slow_add" in body:
        CONTROL["slow_reports"].add(body["slow_add"])
    if "slow_clear" in body:
        CONTROL["slow_reports"].discard(body["slow_clear"])
    if "slow_delay" in body:
        CONTROL["slow_delay"] = float(body["slow_delay"])
    if "expire_access_tokens" in body:
        # 演示用：立即作废当前所有访问令牌（下一次报告请求必回 401）
        now = time.time()
        for meta in ACCESS_TOKENS.values():
            meta["expires_at"] = now
    if body.get("reset"):
        CONTROL["refresh_fail"] = ""
        CONTROL["refresh_hang"] = 0.0
        CONTROL["stale401_reports"].clear()
        CONTROL["slow_reports"].clear()
    return JSONResponse({"ok": True, "control": _public_control()})


def _public_control() -> Dict[str, Any]:
    return {
        "access_ttl": CONTROL["access_ttl"],
        "refresh_fail": CONTROL["refresh_fail"],
        "refresh_hang": CONTROL["refresh_hang"],
        "stale401_reports": sorted(CONTROL["stale401_reports"]),
        "slow_reports": sorted(CONTROL["slow_reports"]),
        "slow_delay": CONTROL["slow_delay"],
    }


@app.get("/debug/control")
async def get_control():
    return _public_control()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.environ.get("MOCK_HOST", "127.0.0.1"),
        port=int(os.environ.get("MOCK_PORT", "9000")),
    )
