"""检测机构 OAuth2 / 报告 API 客户端。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from . import config


class AuthError(Exception):
    """确定的授权失败（4xx）。"""


class AmbiguousAuthError(Exception):
    """刷新结果无法确认（网络中断/超时/5xx），按规则必须暂停账户、重新授权。"""


@dataclass
class TokenSet:
    access_token: str
    refresh_token: str
    jti: str
    expires_in: int
    scope: str


class LabClient:
    def __init__(self, base_url: Optional[str] = None) -> None:
        self.base_url = (base_url or config.MOCK_BASE_URL).rstrip("/")

    def _auth(self) -> tuple[str, str]:
        return (config.OAUTH_CLIENT_ID, config.OAUTH_CLIENT_SECRET)

    async def exchange_code(self, code: str, redirect_uri: str) -> TokenSet:
        async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT) as client:
            try:
                resp = await client.post(
                    f"{self.base_url}/oauth/token",
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "redirect_uri": redirect_uri,
                    },
                    auth=self._auth(),
                )
            except httpx.HTTPError as exc:
                raise AmbiguousAuthError(f"授权码兑换网络错误: {exc}") from exc
        if resp.status_code >= 400:
            raise AuthError(f"授权码兑换失败: {resp.status_code} {resp.text[:200]}")
        return self._parse_token(resp.json())

    async def refresh(self, refresh_token: str) -> TokenSet:
        """用 refresh_token 换新令牌对。

        任何传输层异常 / 5xx 都视为“刷新结果无法确认”——
        服务端可能已经轮换并作废旧令牌，客户端不能再重试，必须重新授权。
        """
        async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT) as client:
            try:
                resp = await client.post(
                    f"{self.base_url}/oauth/token",
                    data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                    auth=self._auth(),
                )
            except httpx.HTTPError as exc:
                raise AmbiguousAuthError(f"刷新请求未确认（网络/超时）: {exc}") from exc
        if resp.status_code >= 500:
            raise AmbiguousAuthError(f"刷新返回 {resp.status_code}，服务端状态无法确认")
        if resp.status_code >= 400:
            raise AuthError(f"刷新被拒绝: {resp.status_code} {resp.text[:200]}")
        return self._parse_token(resp.json())

    async def fetch_report(self, access_token: str, report_no: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT) as client:
            resp = await client.get(
                f"{self.base_url}/reports/{report_no}",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        if resp.status_code == 401:
            raise ReportUnauthorized(report_no, _jti_from_401(resp), resp.text[:200])
        if resp.status_code == 404:
            raise ReportNotFound(report_no)
        if resp.status_code >= 400:
            raise LabAPIError(f"报告 {report_no} 获取失败: {resp.status_code} {resp.text[:200]}")
        return resp.json()

    @staticmethod
    def _parse_token(data: Dict[str, Any]) -> TokenSet:
        return TokenSet(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            jti=data.get("jti", ""),
            expires_in=int(data["expires_in"]),
            scope=data.get("scope", ""),
        )


def _jti_from_401(resp: httpx.Response) -> Optional[str]:
    """从 WWW-Authenticate 头解析服务端回显的 jti。"""
    header = resp.headers.get("www-authenticate", "")
    # 形如: Bearer jti="abc123"
    marker = 'jti="'
    if marker in header:
        start = header.index(marker) + len(marker)
        end = header.index('"', start)
        jti = header[start:end]
        return jti or None
    return None


class ReportUnauthorized(Exception):
    def __init__(self, report_no: str, jti: Optional[str], body: str) -> None:
        self.report_no = report_no
        self.jti = jti
        self.body = body
        super().__init__(f"报告 {report_no} 返回 401 (jti={jti})")


class ReportNotFound(Exception):
    def __init__(self, report_no: str) -> None:
        self.report_no = report_no
        super().__init__(f"报告 {report_no} 不存在")


class LabAPIError(Exception):
    pass
