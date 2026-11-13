"""集中配置：全部可通过环境变量覆盖，容器与本地运行共用。"""
from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


# 质控库
DB_PATH = os.environ.get("QC_DB_PATH", os.path.join(os.getcwd(), "data", "qc.db"))
WEB_HOST = os.environ.get("QC_HOST", "127.0.0.1")
WEB_PORT = _int("QC_PORT", 8080)

# 模拟检测机构
MOCK_BASE_URL = os.environ.get("MOCK_BASE_URL", "http://127.0.0.1:9000")
# 回调地址：浏览器可到达的 web 地址；容器内通过 env 注入
WEB_PUBLIC_BASE_URL = os.environ.get("WEB_PUBLIC_BASE_URL", "http://127.0.0.1:8080")

# OAuth2 演示客户端（模拟源登记的固定客户端）
OAUTH_CLIENT_ID = os.environ.get("OAUTH_CLIENT_ID", "qc-collector")
OAUTH_CLIENT_SECRET = os.environ.get("OAUTH_CLIENT_SECRET", "qc-secret")
OAUTH_SCOPE = "germination:read"

# 唯一的共用账户标识（两个采集进程共用一个 OAuth2 账户）
ACCOUNT_ID = "shared-lab-account"

# 刷新协调
# 刷新租约：超过该时间仍未完成，视为刷新进程中途退出（结果无法确认）-> 暂停账户
REFRESH_LEASE_SECONDS = _int("REFRESH_LEASE_SECONDS", 15)
# 等待其它进程刷新时的轮询间隔
REFRESH_POLL_INTERVAL = float(os.environ.get("REFRESH_POLL_INTERVAL", "0.2"))
# access token 到期前的提前余量
TOKEN_EXPIRY_MARGIN = _int("TOKEN_EXPIRY_MARGIN", 5)

# 采集
WORKER_NAME = os.environ.get("WORKER_NAME", "worker")
WORKER_SLOTS = _int("WORKER_SLOTS", 2)
JOB_MAX_ATTEMPTS = _int("JOB_MAX_ATTEMPTS", 5)
# running 任务超过该秒数未更新，视为采集进程崩溃遗留并回收为 pending
JOB_STALE_SECONDS = _int("JOB_STALE_SECONDS", 60)
# 采集请求超时（慢报告 demo 用）
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "30"))
