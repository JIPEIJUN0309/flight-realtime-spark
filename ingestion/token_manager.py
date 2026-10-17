"""
OpenSky OAuth2 令牌管理器

OpenSky 在 2026 年 3 月停用了用户名/密码认证，改用 OAuth2 client-credentials。
流程：用 client_id + client_secret 去认证服务器换一个 Bearer token，
token 有效期约 30 分钟，过期后要重新换。

这个类的作用：自动管理 token —— 只在临近过期时才刷新，平时直接复用，
避免每次请求都去换 token（浪费、也可能触发限流）。
"""

import time
import requests
from datetime import datetime, timedelta

# OpenSky 官方认证服务器地址
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)

# 提前多少秒刷新（留一点缓冲，避免用到最后一刻正好过期）
REFRESH_MARGIN_SECONDS = 60


class TokenManager:
    def __init__(self, client_id: str, client_secret: str):
        if not client_id or not client_secret:
            raise ValueError(
                "client_id / client_secret 不能为空。"
                "请先在 OpenSky 账号里创建 API client，并填进 .env 文件。"
            )
        self._client_id = client_id
        self._client_secret = client_secret
        self._token = None
        self._expires_at = None  # 一个 datetime，表示本地何时应视为过期

    def get_token(self) -> str:
        """返回一个有效的 access token，需要时自动刷新。"""
        if (
            self._token is not None
            and self._expires_at is not None
            and datetime.now() < self._expires_at
        ):
            return self._token
        return self._refresh()

    def _refresh(self) -> str:
        """向认证服务器换取新的 access token。"""
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=15,
        )
        # 凭据错误时给出更友好的提示
        if resp.status_code == 401:
            raise RuntimeError(
                "认证失败(401)：client_id 或 client_secret 不正确。"
                "请到 https://opensky-network.org/my-opensky/account 核对。"
            )
        resp.raise_for_status()

        data = resp.json()
        self._token = data["access_token"]
        expires_in = data.get("expires_in", 1800)  # 默认按 30 分钟算
        self._expires_at = datetime.now() + timedelta(
            seconds=expires_in - REFRESH_MARGIN_SECONDS
        )
        print(f"[token] 已获取新 token，约 {expires_in} 秒后过期")
        return self._token

    def auth_header(self) -> dict:
        """返回可直接用于 requests 的 headers。"""
        return {"Authorization": f"Bearer {self.get_token()}"}
