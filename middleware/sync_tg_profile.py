"""Сохранение Telegram username и fullname в users, если в БД ещё пусто."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Update, User

from bot import sql
from logging_config import logger
from middleware.user_throttle import _actor_from_update


def _normalize_username(raw: Optional[str]) -> Optional[str]:
    uname = (raw or "").strip().lstrip("@")
    return uname or None


def _normalize_fullname(tg: User) -> Optional[str]:
    name = (tg.full_name or tg.first_name or "").strip()
    return name or None


async def maybe_sync_tg_profile_to_db(tg: Optional[User]) -> None:
    """Дописать в users username и/или fullname, если в Telegram есть и в БД пусто."""
    if tg is None or tg.is_bot or tg.id <= 0:
        return
    username = _normalize_username(tg.username)
    fullname = _normalize_fullname(tg)
    if not username and not fullname:
        return
    try:
        await sql.sync_telegram_profile_if_missing(tg.id, username, fullname)
    except Exception as e:
        logger.debug("sync tg profile user_id={}: {}", tg.id, e)


class SyncTgProfileMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        if isinstance(event, Update):
            await maybe_sync_tg_profile_to_db(_actor_from_update(event))
        else:
            from_user = data.get("event_from_user")
            if isinstance(from_user, User):
                await maybe_sync_tg_profile_to_db(from_user)
        return await handler(event, data)
