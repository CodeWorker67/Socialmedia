"""Константы лимита трафика сервера Антиглушилка (белая нода)."""

from datetime import datetime
from zoneinfo import ZoneInfo

WL_NODE_NAME = "Yandex-WL-002"
WL_TIMEZONE = ZoneInfo("Europe/Moscow")
# Сутки WL-трафика: с 03:00 до 02:59 МСК (накопление в 02:57, проверка после 03:05).
WL_DAY_RESET_HOUR = 3
WL_ACCUMULATE_HOUR = 2
WL_ACCUMULATE_MINUTE = 57
WL_CHECK_SKIP_UNTIL_HOUR = 3
WL_CHECK_SKIP_UNTIL_MINUTE = 5
WL_LEGACY_RETRIES = 3
WL_TOP_USERS_LIMIT = 5000

WL_SQUAD_LIMITED = (
    "2103d7f3-a5a7-4c79-b99c-18024f2cb8a5",
    "9876cb62-ea2e-4688-976f-5f8e831644bd",
)

WL_SQUAD_ACTIVE = (
    "28b6a3bf-8e81-42dd-9ac8-dab1c9a60b0a",
    "85f8520a-8dd7-40a6-9f27-8a7467096c6a",
)

WL_TRIAL_LIMIT_GB = 2.0
WL_GB_PER_MONTH = 10
WL_LOW_TRAFFIC_WARNING_GB = 1.0

# Тариф «Навсегда» (5000 дней) — expire далеко за 2030
FOREVER_DURATION_DAYS = 5000
FOREVER_YEAR_THRESHOLD = 2030
FOREVER_END_CUTOFF = datetime(FOREVER_YEAR_THRESHOLD, 1, 1)

# gb -> price (₽)
WL_TRAFFIC_TARIFFS: dict[str, int] = {
    "10": 50,
    "20": 79,
    "50": 149,
    "100": 259,
    "250": 629,
    "500": 1249,
}

# duration days -> months for +10 GB/month bonus on subscription payment
WL_SUBSCRIPTION_MONTHS: dict[int, int] = {
    7: 0,
    30: 1,
    90: 3,
    120: 4,
    180: 6,
    365: 12,
    FOREVER_DURATION_DAYS: 1,
}

PROFILE_CB = "user_profile"
WL_TRAFFIC_BUY_CB = "wl_traffic_buy"
WL_TRAFFIC_BUY_SUB_CB = "wl_traffic_buy_sub"
BUY_VPN_CB = "buy_vpn"
