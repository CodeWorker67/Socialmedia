"""Парсинг Telegram /start вида YD{stamp}_{yandex_id} (префикс YD — заглавные)."""

from __future__ import annotations

from typing import Optional, Tuple

_KNOWN_STAMP_HAPP_NOW_GLOBAL = "happ_now_global"


def parse_yd_start_arg(start_arg: str) -> Optional[Tuple[str, str]]:
    """
    YDhapp_now_global_1791285975313217366 → stamp=happ_now_global, yandex_id=1791285975313217366
    YDhapp_now_global1791285975313217366 — без «_» перед ClientId (короткая ссылка).
    """
    if not start_arg or not start_arg.startswith("YD"):
        return None
    rest = start_arg[2:]
    if not rest:
        return None

    if rest.startswith(_KNOWN_STAMP_HAPP_NOW_GLOBAL):
        tail = rest[len(_KNOWN_STAMP_HAPP_NOW_GLOBAL) :]
        if tail.startswith("_"):
            tail = tail[1:]
        yandex_id = tail.strip()
        if yandex_id.isdigit():
            return _KNOWN_STAMP_HAPP_NOW_GLOBAL, yandex_id[:100]

    if "_" in rest:
        stamp, yandex_id = rest.rsplit("_", 1)
        stamp = stamp.strip()
        yandex_id = yandex_id.strip()
        if stamp and yandex_id:
            return stamp[:100], yandex_id[:100]
    return None