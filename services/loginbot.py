"""LoginBot call-auth API client."""
from __future__ import annotations

from typing import Any

import aiohttp

from config import LOGINBOT_API_KEY

LOGINBOT_API_BASE = "https://api.loginbot.ru/api/v1"
LOGINBOT_DEFAULT_TIMEOUT = 180  # 3 minutes


class LoginBotError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


async def start_call_auth(
    phone: str,
    *,
    webhook: str,
    payload: str = "",
    timeout: int = LOGINBOT_DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    if not LOGINBOT_API_KEY:
        raise LoginBotError("LoginBot API key is not configured")
    url = f"{LOGINBOT_API_BASE}/{LOGINBOT_API_KEY}/call/auth/{phone}"
    body: dict[str, Any] = {"timeout": timeout, "webhook": webhook}
    if payload:
        body["payload"] = payload
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=body) as resp:
            try:
                data = await resp.json()
            except Exception:
                data = {}
            if resp.status != 200:
                detail = data.get("message") or data.get("error") or resp.reason
                raise LoginBotError(str(detail), status=resp.status)
            return data
