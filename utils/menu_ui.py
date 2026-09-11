"""UI helpers for photo-based menu screens."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Union

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    InaccessibleMessage,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from bot import bot, sql, x3
from lexicon import lexicon
from logging_config import logger
from utils.menu_photos import menu_photo
from wl_traffic.service import get_wl_used_gb_for_user, is_forever_end_date

MAIN_MENU_REPLY_TEXT = (
    "Кнопка <b>Главное меню</b> внизу — нажмите её, чтобы в любой момент вернуться в главное меню."
)
MAIN_MENU_BUTTON_TEXT = "Главное меню"

_USER_TUPLE_SUBSCRIPTION_END_DATE = 9
_USER_TUPLE_WHITE_SUBSCRIPTION_END_DATE = 10


def reply_keyboard_main_menu() -> ReplyKeyboardMarkup:
    from keyboard import STYLE_PRIMARY

    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MAIN_MENU_BUTTON_TEXT, style=STYLE_PRIMARY)],
        ],
        resize_keyboard=True,
    )


async def send_main_menu_hint(message: Message) -> None:
    await message.answer(
        MAIN_MENU_REPLY_TEXT,
        parse_mode="HTML",
        reply_markup=reply_keyboard_main_menu(),
    )


def _aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_date_msk(dt: datetime) -> str:
    from datetime import timedelta

    return (_aware_utc(dt) + timedelta(hours=3)).strftime("%d.%m.%Y")


def end_date_status_text(sub_end) -> str:
    if sub_end is None:
        return "Нет подписки"
    if is_forever_end_date(sub_end):
        return "Активна навсегда ♾️"
    aware = _aware_utc(sub_end)
    date_str = _format_date_msk(aware)
    if aware > datetime.now(timezone.utc):
        return f"Активна до {date_str}"
    return f"Истекла {date_str}"


def subscription_status_text(user_data: Optional[tuple]) -> str:
    if not user_data or len(user_data) <= _USER_TUPLE_SUBSCRIPTION_END_DATE:
        return "Нет подписки"
    return end_date_status_text(user_data[_USER_TUPLE_SUBSCRIPTION_END_DATE])


def profile_subscription_status_text(user_data: Optional[tuple]) -> str:
    if not user_data:
        return "Нет подписки"
    pro_end = (
        user_data[_USER_TUPLE_SUBSCRIPTION_END_DATE]
        if len(user_data) > _USER_TUPLE_SUBSCRIPTION_END_DATE
        else None
    )
    white_end = (
        user_data[_USER_TUPLE_WHITE_SUBSCRIPTION_END_DATE]
        if len(user_data) > _USER_TUPLE_WHITE_SUBSCRIPTION_END_DATE
        else None
    )
    if _end_is_active(pro_end):
        return f"💫 VPN PRO: {end_date_status_text(pro_end)}"
    if _end_is_active(white_end):
        return f"📱 Мобильный тариф: {end_date_status_text(white_end)}"
    if pro_end is not None:
        return f"💫 VPN PRO: {end_date_status_text(pro_end)}"
    if white_end is not None:
        return f"📱 Мобильный тариф: {end_date_status_text(white_end)}"
    return "Нет подписки"


def _end_is_active(sub_end) -> bool:
    if sub_end is None:
        return False
    if is_forever_end_date(sub_end):
        return True
    return _aware_utc(sub_end) > datetime.now(timezone.utc)


def has_active_subscription(user_data: Optional[tuple]) -> bool:
    if not user_data:
        return False
    pro = user_data[_USER_TUPLE_SUBSCRIPTION_END_DATE] if len(user_data) > _USER_TUPLE_SUBSCRIPTION_END_DATE else None
    white = (
        user_data[_USER_TUPLE_WHITE_SUBSCRIPTION_END_DATE]
        if len(user_data) > _USER_TUPLE_WHITE_SUBSCRIPTION_END_DATE
        else None
    )
    return _end_is_active(pro) or _end_is_active(white)


def profile_caption(
    fullname: str,
    user_data: Optional[tuple],
    sub_urls: Optional[list[str]] = None,
) -> str:
    status = profile_subscription_status_text(user_data)
    parts = [f"👤 {fullname}", f"📲 {status}"]
    if sub_urls:
        parts.extend(sub_urls)
    return "\n".join(parts)


def _format_autopay(row) -> str:
    if row is None or getattr(row, "status", None) != "active":
        return lexicon["profile_autopay_off"]
    next_at = getattr(row, "next_charge_at", None)
    date_s = next_at.strftime("%d.%m.%Y") if next_at else "—"
    return lexicon["profile_autopay_next"].format(amount=row.amount, date=date_s)


async def subscription_end_display(uid: int) -> str:
    result = await x3.activ(str(uid))
    return result.get("time") or "—"


async def connect_screen_extra(uid: int, user_data: tuple) -> str:
    used_gb, limit_gb = await sql.get_wl_limits(uid)
    used_gb = await get_wl_used_gb_for_user(x3, uid, used_gb)

    lines = [f"📡 Антиглушилка: {used_gb:.2f} / {limit_gb:.2f} GB"]

    devices_count = 0
    device_limit = 5
    panel_resp = await x3.get_user_by_username(str(uid))
    panel_user = x3._panel_user_from_response(panel_resp)
    if panel_user:
        device_limit = panel_user.get("hwidDeviceLimit") or 5
        panel_user_id = x3._panel_user_id(panel_user)
        if panel_user_id is not None:
            _devices, devices_count = await x3.get_user_hwid_devices(str(panel_user_id))
    lines.append(f"📱 Устройства: {devices_count} / {device_limit}")

    autopay = await sql.get_status_active_platega_autopay(uid)
    lines.append(f"💳 Автоплатежи: {_format_autopay(autopay)}")
    return "\n".join(lines)


async def _replace_photo_message(
    chat_id: int,
    message_id: int,
    photo_key: str,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest:
        pass
    await bot.send_photo(
        chat_id,
        photo=menu_photo(photo_key),
        caption=caption,
        parse_mode="HTML",
        reply_markup=reply_markup,
    )


async def edit_or_send_photo(
    source: Union[Message, CallbackQuery],
    photo_key: str,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    if isinstance(source, CallbackQuery):
        message = source.message
        chat_id = message.chat.id
        message_id = message.message_id
    else:
        message = source
        chat_id = message.chat.id
        message_id = message.message_id

    if isinstance(message, InaccessibleMessage):
        await _replace_photo_message(
            chat_id, message_id, photo_key, caption, reply_markup
        )
        return

    if message.photo:
        try:
            await message.edit_media(
                media=InputMediaPhoto(
                    media=menu_photo(photo_key),
                    caption=caption,
                    parse_mode="HTML",
                ),
                reply_markup=reply_markup,
            )
            return
        except TelegramBadRequest as e:
            logger.warning(
                "menu edit_media failed chat_id={} key={}: {}",
                chat_id,
                photo_key,
                e,
            )
            await _replace_photo_message(
                chat_id, message_id, photo_key, caption, reply_markup
            )
            return

    await bot.send_photo(
        chat_id,
        photo=menu_photo(photo_key),
        caption=caption,
        parse_mode="HTML",
        reply_markup=reply_markup,
    )


def trial_success_caption(end_time: str, sub_url: str) -> str:
    return (
        "🎉 <b>Тестовая подписка активирована!</b>\n"
        f"⏰ Доступ до: {end_time}\n\n"
        "🔗 Ваша ссылка для импорта в приложение:\n"
        f"{sub_url}\n\n"
        "📱 Нажмите кнопку ниже, чтобы получить инструкцию по настройке"
    )


def connect_screen_caption(
    fullname: str,
    user_data: tuple,
    sub_url: Optional[str],
    extra: str,
) -> str:
    status = subscription_status_text(user_data)
    parts = [
        f"👤 {fullname}",
        f"📲 {status}",
        extra,
        "",
    ]
    if sub_url:
        parts.extend(["🔗 Ссылка для импорта:", str(sub_url), ""])
    parts.append(
        "📱 Нажмите «Если страница не загружается», чтобы получить инструкцию по настройке"
    )
    return "\n".join(parts)


async def show_connect_screen(callback: CallbackQuery) -> bool:
    from keyboard import keyboard_subscription_manage

    uid = callback.from_user.id
    user_data = await sql.get_user(uid) or tuple()
    sub_url = await x3.sublink(str(uid))

    if not sub_url:
        await callback.message.answer(lexicon["no_sub"])
        return False

    fullname = callback.from_user.full_name or callback.from_user.first_name or "Пользователь"
    extra = await connect_screen_extra(uid, user_data)
    caption = connect_screen_caption(fullname, user_data, sub_url, extra)
    autopay = await sql.get_status_active_platega_autopay(uid)
    await edit_or_send_photo(
        callback,
        "subscription_manage",
        caption,
        keyboard_subscription_manage(
            sub_url,
            has_active_autopay=autopay is not None,
        ),
    )
    return True


async def show_main_menu(
    source: Union[Message, CallbackQuery],
    *,
    send_hint: bool = False,
) -> None:
    from keyboard import keyboard_start

    user = source.from_user
    user_data = await sql.get_user(user.id)
    fullname = user.full_name or user.first_name or "Пользователь"
    in_panel = bool(user_data and user_data[4])
    active = has_active_subscription(user_data)

    if send_hint and isinstance(source, Message):
        await send_main_menu_hint(source)

    sub_urls: list[str] = []
    sub_url: Optional[str] = None
    if active:
        slots = await x3.active_subscription_slots(user.id)
        for _slot_key, _label, _panel_id, username in slots:
            url = await x3.sublink(username)
            if url:
                sub_urls.append(url)
        sub_url = sub_urls[0] if sub_urls else None

    caption = profile_caption(fullname, user_data, sub_urls or None)
    kb = keyboard_start(
        has_active_sub=active,
        buy_primary=not active,
        sub_url=sub_url or None,
        show_trial=not in_panel,
    )

    if isinstance(source, CallbackQuery):
        await edit_or_send_photo(source, "profile", caption, kb)
    else:
        await source.answer_photo(
            photo=menu_photo("profile"),
            caption=caption,
            parse_mode="HTML",
            reply_markup=kb,
        )
