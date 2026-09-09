"""Ключи тарифов и маппинг периода подписки для панели."""

from calendar import monthrange
from datetime import datetime, timedelta
from typing import Union

DurationLike = Union[int, str]

# 30/90/180 → календарные месяцы; 365 → 1 год (12 месяцев).
_CALENDAR_MONTHS = {
    30: 1,
    90: 3,
    120: 4,
    180: 6,
    365: 12,
}

_PERIOD_LABELS = {
    "7": "7 дней",
    "30": "1 месяц",
    "90": "3 месяца",
    "120": "4 месяца",
    "180": "6 месяцев",
    "365": "1 год",
    "5000": "Навсегда",
    "5000sale": "Навсегда",
}


def tariff_key_from_callback(data: str) -> str:
    key = data.replace("gift_r_", "").replace("r_", "")
    if "white" in key:
        key = key.replace("white_", "")
    if key.endswith("old"):
        key = key[:-3]
    return key


def panel_days_from_tariff_key(key: str) -> int:
    from config_bd.utils import _payload_duration_to_panel_days

    days = _payload_duration_to_panel_days(key)
    if days is not None:
        return days
    return int(key)


def _days_ru(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} день"
    if 2 <= n % 10 <= 4 and not (12 <= n % 100 <= 14):
        return f"{n} дня"
    return f"{n} дней"


def tariff_period_label(duration: DurationLike) -> str:
    s = str(duration).strip()
    if s in _PERIOD_LABELS:
        return _PERIOD_LABELS[s]
    try:
        n = int(s)
    except (TypeError, ValueError):
        return f"{s} дней"
    if n >= 5000:
        return "Навсегда"
    return _days_ru(n)


def add_calendar_months(dt: datetime, months: int) -> datetime:
    month_index = dt.month - 1 + int(months)
    year = dt.year + month_index // 12
    month = month_index % 12 + 1
    day = min(dt.day, monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def add_tariff_period(dt: datetime, duration: DurationLike) -> datetime:
    """Добавляет период тарифа: месяцы/год для 30/90/180/365, иначе дни."""
    n = int(duration)
    months = _CALENDAR_MONTHS.get(n)
    if months == 12:
        try:
            return dt.replace(year=dt.year + 1)
        except ValueError:
            return dt.replace(year=dt.year + 1, day=28)
    if months:
        return add_calendar_months(dt, months)
    return dt + timedelta(days=n)
