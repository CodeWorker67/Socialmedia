"""Telegram file_id for menu screens — profile depends on bot username."""
from __future__ import annotations

from typing import Optional

from aiogram import Bot

from logging_config import logger

FASTMOBILE_BOT_USERNAME = "fastmobilevpnbot"

PHOTO_KEYS = (
    "profile",
    "subscription_manage",
    "buy_subscription",
    "buy_traffic",
    "manage_devices",
    "our_site",
    "earn_with_us",
    "about_service",
    "faq",
)

_MENU_PHOTOS_FASTMOBILE = {
    "buy_subscription": "AgACAgQAAxkBAAG2WKxqjoh33HyzoN3qb5qdAhYHaWj7HQACXRBrG-IQcFCZLC9GA0cO_QEAAwIAA3kAAz0E",
    "earn_with_us": "AgACAgQAAxkBAAG2WKZqjohjtSRn3udNYHKqUPJWC6GC7gACWhBrG-IQcFCjJ9S8ZEsvuQEAAwIAA3kAAz0E",
    "our_site": "AgACAgQAAxkBAAG2WKhqjohpxzegYhyrZIxl1jM5el47ZgACWxBrG-IQcFA4ctbHB7pBGAEAAwIAA3kAAz0E",
    "about_service": "AgACAgQAAxkBAAG2WKpqjohx3u4_F-Cd3Yc7nnUPPSnIjgACXBBrG-IQcFBZZX3knbdTdAEAAwIAA3kAAz0E",
    "buy_traffic": "AgACAgQAAxkBAAG2WK5qjoh9ouSkyJovvfBLz6lk0k7f5QACXhBrG-IQcFDepx1gMu8GwwEAAwIAA3kAAz0E",
    "profile": "AgACAgQAAxkBAAG2WLBqjoiCe-4rw6BtZX1YNJOObkS5dAACXxBrG-IQcFBSfAjOSjJL_QEAAwIAA3kAAz0E",
    "manage_devices": "AgACAgQAAxkBAAG2WLRqjoiNMgSNaO9mRdVjlMhKh5mmdgACYRBrG-IQcFBcN8yrcHxmYgEAAwIAA3kAAz0E",
    "subscription_manage": "AgACAgQAAxkBAAG2WLJqjoiIBofKGS7MHezFvQ-iH2nyKAACYBBrG-IQcFATiHeaBnIYUQEAAwIAA3kAAz0E",
    "faq": "AgACAgQAAxkBAAG2WLZqjoiRnJjy8NRgQkyDJGb6XZX38gACYhBrG-IQcFA-zSywgiNNNQEAAwIAA3kAAz0E",
}

_MENU_PHOTOS_DEFAULT = {
    "buy_subscription": "AgACAgQAAxkBAAIKamqOfBWAP82T9_-1c05A-TprkIUVAAJdEGsb4hBwUHR3a50Fw2ssAQADAgADeQADPQQ",
    "earn_with_us": "AgACAgQAAxkBAAIKZGqOfARUi5Lddo-4Pbf2pWMLYnjrAAJaEGsb4hBwUIcUEHEI1iOZAQADAgADeQADPQQ",
    "our_site": "AgACAgQAAxkBAAIKZmqOfAvQoVuisA5TxRDRWri-9nF7AAJbEGsb4hBwUK6oJmtFqPGmAQADAgADeQADPQQ",
    "about_service": "AgACAgQAAxkBAAIKaGqOfBDqcdoc83bFUXAQYcKnFE3NAAJcEGsb4hBwUD5BCLIbdM8EAQADAgADeQADPQQ",
    "buy_traffic": "AgACAgQAAxkBAAIKbGqOfBocDqlrf3SVyQg_zgz_jg9rAAJeEGsb4hBwUAZXTLQxaFkHAQADAgADeQADPQQ",
    "profile": "AgACAgQAAxkBAAIKbmqOfChKnGavNao35RswgRCoXh9JAAJfEGsb4hBwUEpsKbnY_BMbAQADAgADeQADPQQ",
    "manage_devices": "AgACAgQAAxkBAAIKcmqOfDImnQVDYnU2NwHG4KWwGcpoAAJhEGsb4hBwUL2_J-xTSEquAQADAgADeQADPQQ",
    "subscription_manage": "AgACAgQAAxkBAAIKcGqOfC3tgx6plCI6iFbx7GsQAeJTAAJgEGsb4hBwUFH3c4YdJ5G2AQADAgADeQADPQQ",
    "faq": "AgACAgQAAxkBAAIKdGqOfDfIKAxlIlEGcKjFW2IOs1jKAAJiEGsb4hBwUNAB_6cwrAYPAQADAgADeQADPQQ",
}

_CONTEST_WIN_PHOTOS_FASTMOBILE = {
    "initial": "AgACAgQAAxkBAAHJW5Nqs2nsTjz2tMR09Y2CnCx-PnTJJQACmhBrG5QdmFG9hvJo9ugwpwEAAwIAA3kAAz0E",
    "urgency": "AgACAgQAAxkBAAHJW5Vqs2n4NWSCYuUEZKPPJ559h9mFrQACnBBrG5QdmFGjxTcR5dVAygEAAwIAA3kAAz0E",
}

_CONTEST_WIN_PHOTOS_DEFAULT = {
    "initial": "AgACAgQAAxkBAAINNmqzXk_wzCwYi7YqRCRWy7g4CgrlAAKpDmsbPl6gUc6fpC0MyGmcAQADAgADeQADPQQ",
    "urgency": "AgACAgQAAxkBAAINOGqzXllRvkoiYkVuQ-Bu5nqKQyGyAAKqDmsbPl6gUSKzLrasz9ygAQADAgADeQADPQQ",
}

_IMPORT_PHOTOS_FASTMOBILE = {
    "incy": [
        "AgACAgQAAxkBAAGCd8NqQlYYedInEKDsyGCV4Rr1UohhCgACAg9rG-RnGFIdvB7Gu0GgRQEAAwIAA3gAAzwE",
        "AgACAgQAAxkBAAGCd8VqQlYgVMKvJoaoyBljiVVFJF55EQACAw9rG-RnGFKGaw6TG_0IfQEAAwIAA3gAAzwE",
    ],
    "happ": [
        "AgACAgIAAxkBAAIPmWnEKQRBvj4RG0McyGUKCyfy2MMAA84Zaxu4bCFK9rcoMhDWNSsBAAMCAAN5AAM6BA",
        "AgACAgIAAxkBAAIPu2nEKRIZIT3pNE9gsRkj4-_MVw1zAALPGWsbuGwhStCMPo97YAbTAQADAgADeQADOgQ",
    ],
    "v2": [
        "AgACAgIAAxkBAAIQf2nEKWepYcZqa1QUuJJFas95QQVfAALTGWsbuGwhSv7MqSbIZegwAQADAgADeQADOgQ",
        "AgACAgIAAxkBAAIQk2nEKW9WBjzeB5iQ4zt4VKimPHEFAALUGWsbuGwhSvPA4dR652E3AQADAgADeQADOgQ",
        "AgACAgIAAxkBAAIQnmnEKXWEhq62u6Oxqgk-VeDVASFPAALVGWsbuGwhSvRfGK_Pm9yCAQADAgADeQADOgQ",
    ],
}

_IMPORT_PHOTOS_DEFAULT = {
    "incy": [
        "AgACAgQAAxkBAAIKWmqOeqZJ2gvtUAgJr91q18lb33ctAAJVEGsb4hBwUHq4NM63sGeOAQADAgADeAADPQQ",
        "AgACAgQAAxkBAAIKXGqOeqxox8WtbiOu-TfGBwwKZncqAAJWEGsb4hBwUIcddDxfEBk1AQADAgADeAADPQQ",
    ],
    "happ": [
        "AgACAgQAAxkBAAIKVmqOepvln8_3MTD3R9BOczzjN377AALUD2sbH115UPDuUz10R_ieAQADAgADeQADPQQ",
        "AgACAgQAAxkBAAIKWGqOeqB6KdCmM9M083gi8magZdQGAAJUEGsb4hBwUKc7aCTFb9lgAQADAgADeQADPQQ",
    ],
    "v2": [
        "AgACAgQAAxkBAAIKXmqOerDOik9VPpBMpEGerE41XKKBAAJXEGsb4hBwUF-n_GADoPXaAQADAgADeQADPQQ",
        "AgACAgQAAxkBAAIKYGqOerV7nyMZa1X_N7v7WbgTCzshAAJYEGsb4hBwUAzum8XhIgABKgEAAwIAA3kAAz0E",
        "AgACAgQAAxkBAAIKYmqOertJDhjrUrcQwFzFXiVdURvaAAJZEGsb4hBwUD9bDXcbBO42AQADAgADeQADPQQ",
    ],
}

_cached_username: Optional[str] = None


def _username_from_bot_url() -> str:
    from config import BOT_URL

    slug = (BOT_URL or "").rstrip("/").split("/")[-1]
    return slug.lstrip("@").lower()


def _active_bot_username() -> str:
    return (_cached_username or _username_from_bot_url()).lower()


def is_fastmobile_bot() -> bool:
    return _active_bot_username() == FASTMOBILE_BOT_USERNAME


def _menu_photos_map() -> dict[str, str]:
    if is_fastmobile_bot():
        return _MENU_PHOTOS_FASTMOBILE
    return _MENU_PHOTOS_DEFAULT


def _import_photos_map() -> dict[str, list[str]]:
    if is_fastmobile_bot():
        return _IMPORT_PHOTOS_FASTMOBILE
    return _IMPORT_PHOTOS_DEFAULT


def menu_photo(key: str) -> str:
    photos = _menu_photos_map()
    if key not in photos:
        raise KeyError(f"Unknown menu photo key: {key}")
    return photos[key]


def contest_win_photo(phase: str) -> str:
    """phase: 'initial' | 'urgency' — картинки воронки «выигрыш в конкурсе»."""
    if phase not in ("initial", "urgency"):
        raise KeyError(f"Unknown contest win photo phase: {phase}")
    if is_fastmobile_bot():
        return _CONTEST_WIN_PHOTOS_FASTMOBILE[phase]
    return _CONTEST_WIN_PHOTOS_DEFAULT[phase]


def import_photos(app_key: str) -> list[str]:
    photos = _import_photos_map()
    if app_key not in photos:
        raise KeyError(f"Unknown import photo app key: {app_key}")
    return photos[app_key]


async def init_menu_photos(bot: Bot) -> None:
    global _cached_username
    me = await bot.get_me()
    if me and me.username:
        _cached_username = me.username.lower()
        profile = "fastmobile" if is_fastmobile_bot() else "default"
        logger.info("Menu photos: @{} ({})", _cached_username, profile)
