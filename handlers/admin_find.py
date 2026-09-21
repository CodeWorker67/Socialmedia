"""Админ: /find, /find_transaction и inline-кнопки +дни/+ГБ (как partner_service)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot import bot, sql, x3
from config import ADMIN_IDS
from config_bd.models import Users
from keyboard import BTN_BACK, create_kb, keyboard_sub_after_buy
from lexicon import lexicon
from logging_config import logger
from telegram_ids import is_telegram_chat_id
from handlers.handlers_devices import _device_display_name
from wl_traffic.service import (
    fetch_panel_user,
    get_wl_used_gb_for_user,
    reassign_to_active_squad,
    user_on_limited_squad,
)
from config_bd.utils import USER_IX_FIELD_BOOL_2

router = Router()

_MSK = timezone(timedelta(hours=3))


def _msk_dt_str(dt: Optional[datetime]) -> str:
    if dt is None:
        return "Нет"
    if dt.tzinfo is None:
        aware = dt.replace(tzinfo=timezone.utc)
    else:
        aware = dt.astimezone(timezone.utc)
    return aware.astimezone(_MSK).strftime("%d-%m-%Y %H:%M МСК")


def _split_long_text(text: str, limit: int = 3500) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    rest = text
    while rest:
        if len(rest) <= limit:
            chunks.append(rest)
            break
        cut = rest.rfind("\n\n", 0, limit)
        if cut <= 0:
            cut = rest.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        else:
            cut += 2 if rest[cut : cut + 2] == "\n\n" else 1
        chunks.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip("\n")
    return chunks


_FIND_INFO_TEXT = (
    "📖 <b>Справка по командам администратора</b>\n\n"
    "/pay — подписки, WL-трафик и все платежи пользователя\n"
    "Пример: <code>/pay 123456789</code>\n\n"
    "/find — карточка пользователя с кнопками +дни / +ГБ\n"
    "Пример: <code>/find 123456789</code>\n\n"
    "/sub — установить дату подписки в панели и БД\n"
    "Пример: <code>/sub 123456789 2026-09-15 07:48:05</code>\n\n"
    "/add_traffic — изменить лимит WL (GB, можно отрицательное)\n"
    "Пример: <code>/add_traffic 123456789 10</code>\n\n"
    "/partner — статистика партнёра и оплаты рефералов\n"
    "Пример: <code>/partner 123456789</code>\n\n"
    "/partner_remove — списать выплату с partner_balance\n"
    "Пример: <code>/partner_remove 123456789 500</code>\n\n"
    "/find_transaction — найти оплату по transaction_id\n"
    "Пример: <code>/find_transaction abc-123</code>"
)


def _admin_only_message(message: Message) -> bool:
    return message.from_user.id in ADMIN_IDS


def _admin_only_callback(callback: CallbackQuery) -> bool:
    return callback.from_user.id in ADMIN_IDS


def _dt_str_find(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d.%m.%Y")


def _dt_str_find_time(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d.%m.%Y %H:%M:%S")


def _normalize_dt(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _subscription_active_until(dt: Optional[datetime]) -> str:
    if dt is None:
        return "не активна"
    dt = _normalize_dt(dt)
    now = datetime.now(timezone.utc)
    if dt <= now:
        return "истекла"
    days_left = (dt.date() - now.date()).days
    return f"активна до {_dt_str_find(dt)} (осталось {days_left} дн.)"


async def _format_devices_for_admin(tg_id: int) -> str:
    slots = await x3.active_subscription_slots(tg_id)
    main = next((s for s in slots if s[0] == "main"), None)
    if main is None:
        return "нет"
    _slot_key, _label, panel_user_id, _username = main
    devices, _total = await x3.get_user_hwid_devices(panel_user_id)
    device_dicts = [d for d in devices if isinstance(d, dict)]
    if not device_dicts:
        return "нет"
    return "\n".join(
        f"{idx}. {_device_display_name(device)}"
        for idx, device in enumerate(device_dicts, start=1)
    )


def _format_user_header(user: Users) -> str:
    lines = [f"👤 {user.user_id}"]
    full_name = (user.fullname or "").strip()
    username = (user.username or "").strip().lstrip("@")
    if full_name and username:
        lines.append(f"{full_name} (@{username})")
    elif full_name:
        lines.append(full_name)
    elif username:
        lines.append(f"@{username}")
    return "\n".join(lines)


def _find_keyboard(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="+ 7 дней", callback_data=f"find:d7:{tg_id}"),
                InlineKeyboardButton(text="+ 30 дней", callback_data=f"find:d30:{tg_id}"),
                InlineKeyboardButton(text="+ 90 дней", callback_data=f"find:d90:{tg_id}"),
            ],
            [
                InlineKeyboardButton(text="+ 10 ГБ", callback_data=f"find:g10:{tg_id}"),
                InlineKeyboardButton(text="+ 50 ГБ", callback_data=f"find:g50:{tg_id}"),
            ],
        ]
    )


async def _send_find_message(
    message: Message,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
) -> None:
    chunks = _split_long_text(text, limit=3500)
    for i, chunk in enumerate(chunks):
        reply_markup = keyboard if i == len(chunks) - 1 else None
        await message.answer(chunk, parse_mode="HTML", reply_markup=reply_markup)


async def _refresh_find_message(
    callback: CallbackQuery,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
) -> None:
    chunks = _split_long_text(text, limit=3500)
    msg = callback.message
    if msg is None:
        return
    if len(chunks) == 1:
        try:
            await msg.edit_text(chunks[0], parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            logger.warning("find callback edit_text failed: {}", e)
            await msg.answer(chunks[0], parse_mode="HTML", reply_markup=keyboard)
        return
    try:
        await msg.edit_text(chunks[0], parse_mode="HTML")
    except Exception as e:
        logger.warning("find callback edit_text failed: {}", e)
        await msg.answer(chunks[0], parse_mode="HTML")
    for chunk in chunks[1:-1]:
        await msg.answer(chunk, parse_mode="HTML")
    await msg.answer(chunks[-1], parse_mode="HTML", reply_markup=keyboard)


async def _format_find_body(user: Users) -> str:
    tg_id = int(user.user_id)
    sub_status = _subscription_active_until(user.subscription_end_date)

    sub_link = await x3.sublink(str(tg_id)) or "—"
    devices_block = await _format_devices_for_admin(tg_id)

    trafic_wl, limit_wl = await sql.get_wl_limits(tg_id)
    used_gb = await get_wl_used_gb_for_user(x3, tg_id, trafic_wl)
    remaining_gb = max(0.0, round(limit_wl - used_gb, 1))

    pay_rows = await sql.get_user_find_payments(tg_id, limit=5)
    pay_lines: List[str] = []
    for tc, dur_l, kind, amount in pay_rows:
        pay_lines.append(f"  {_dt_str_find(tc)}  {dur_l}  {kind}  {amount} ₽")

    lines: List[str] = []

    reg = _dt_str_find(user.create_user)
    lines.append(f"Регистрация: {reg}")

    ref_total = await sql.select_ref_count(tg_id)
    if ref_total:
        ref_paid = await sql.select_ref_paid_count(tg_id)
        lines.append(f"Кол-во рефералов: {ref_paid}/{ref_total}")

    partner_total = await sql.select_partner_count(tg_id)
    if partner_total:
        partner_paid = await sql.select_partner_paid_count(tg_id)
        lines.append(f"Кол-во партнёров: {partner_paid}/{partner_total}")

    if user.partner_balance or user.partner_flag:
        lines.append(f"Баланс партнёра: {user.partner_balance or 0}")

    if user.email:
        lines.append(f"email: {user.email}")

    lines.extend([
        "",
        f"Подписка:   {sub_status}",
        f"Ссылка: {sub_link}",
        "Устройства:",
        devices_block,
    ])
    lines.extend([
        "",
        f"Трафик:     {used_gb:.1f} / {limit_wl:.0f} ГБ  (осталось {remaining_gb:.1f} ГБ)",
        "",
        "Оплаты:",
    ])
    if pay_lines:
        lines.extend(pay_lines)
    else:
        lines.append("  —")

    return "\n".join(lines)


async def _build_find_message(tg_id: int, *, notice: str = "") -> tuple[str, Optional[InlineKeyboardMarkup]]:
    user = await sql.get_user_object_by_user_id(tg_id)
    if user is None or user.is_delete:
        return f"❌ Пользователь {tg_id} не найден.", None

    header = _format_user_header(user)
    body = await _format_find_body(user)
    footer = "\n\n<i>Кнопки ниже: +дни и +ГБ для PRO-подписки (основной слот).</i>"
    notice_line = f"\n\n✅ {notice}" if notice else ""
    text = f"{header}\n\n{body}{footer}{notice_line}"
    return text, _find_keyboard(tg_id)


async def _apply_find_add_days(tg_id: int, days: int) -> tuple[bool, str]:
    user_id_str = str(tg_id)
    existing = await x3.get_user_by_username(user_id_str)
    panel_exists = bool(existing and existing.get("response"))
    if panel_exists:
        ok = await x3.updateClient(days, user_id_str, tg_id)
    else:
        ok = await x3.addClient(days, user_id_str, tg_id)

    if not ok:
        return False, "Не удалось продлить подписку в панели"

    end_dt = await sql.get_subscription_end_date(tg_id)
    if is_telegram_chat_id(tg_id):
        sub_link = await x3.sublink(user_id_str)
        user_text = lexicon["sub_granted_notify"].format(
            tier="💫 Подписка PRO — соцсети",
            end_date=_msk_dt_str(end_dt),
        )
        try:
            await bot.send_message(
                tg_id,
                user_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard_sub_after_buy(sub_link) if sub_link else None,
            )
        except Exception as e:
            logger.warning("find +days notify uid={}: {}", tg_id, e)

    return True, f"+{days} дн. → {_msk_dt_str(end_dt)}"


async def _apply_find_add_traffic(tg_id: int, gb: float) -> tuple[bool, str]:
    user_row = await sql.get_user(tg_id)
    if not user_row:
        return False, "Пользователь не найден"

    trafic_wl, _ = await sql.get_wl_limits(tg_id)
    used_gb = await get_wl_used_gb_for_user(x3, tg_id, trafic_wl)
    await sql.add_wl_limit(tg_id, gb)
    _, limit_wl = await sql.get_wl_limits(tg_id)
    remaining_gb = max(0.0, round(limit_wl - used_gb, 2))

    panel_user = await fetch_panel_user(x3, tg_id)
    if panel_user and user_on_limited_squad(panel_user) and limit_wl > used_gb:
        user_row_after = await sql.get_user(tg_id)
        field_bool_2 = bool(user_row_after[USER_IX_FIELD_BOOL_2]) if user_row_after else False
        if not field_bool_2:
            await reassign_to_active_squad(x3, panel_user)

    if is_telegram_chat_id(tg_id):
        try:
            await bot.send_message(
                tg_id,
                lexicon["wl_traffic_admin_grant"].format(
                    gb=gb,
                    limit_gb=limit_wl,
                    used_gb=used_gb,
                    remaining_gb=remaining_gb,
                ),
                parse_mode="HTML",
                reply_markup=create_kb(1, back_to_main=BTN_BACK),
            )
        except Exception as e:
            logger.warning("find +GB notify uid={}: {}", tg_id, e)

    sign = "+" if gb >= 0 else ""
    return True, f"{sign}{gb:g} ГБ → лимит {limit_wl:.1f} ГБ"


@router.message(Command(commands=["info"]))
async def cmd_admin_info(message: Message):
    if not _admin_only_message(message):
        return
    for chunk in _split_long_text(_FIND_INFO_TEXT, limit=3500):
        await message.answer(chunk, parse_mode="HTML")


@router.message(Command(commands=["find"]))
async def cmd_find(message: Message):
    if not _admin_only_message(message):
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("❌ Использование: /find <telegram_id>")
        return

    try:
        tg_id = int(args[1].strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом.")
        return

    text, keyboard = await _build_find_message(tg_id)
    await _send_find_message(message, text, keyboard)


@router.message(Command(commands=["find_transaction"]))
async def cmd_find_transaction(message: Message):
    if not _admin_only_message(message):
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("❌ Использование: /find_transaction <transaction_id>")
        return

    tx_id = args[1].strip()
    row = await sql.find_payment_by_transaction_id(tx_id)
    if row is None:
        await message.answer(f"❌ Оплата с transaction_id <code>{tx_id}</code> не найдена.", parse_mode="HTML")
        return

    gift_s = "Да" if row.is_gift else "Нет"
    text = (
        f"<b>Таблица:</b> {row.table}\n"
        f"<b>status:</b> {row.status or '—'}\n"
        f"<b>Создана:</b> {_dt_str_find_time(row.time_created)}\n"
        f"<b>user_id:</b> {row.user_id}\n"
        f"<b>duration:</b> {row.duration}\n"
        f"<b>Подарок:</b> {gift_s}\n"
        f"<b>method:</b> {row.method}\n"
        f"<b>Сумма:</b> {row.amount}"
    )
    await message.answer(text, parse_mode="HTML")


@router.callback_query(F.data.startswith("find:"))
async def find_action_callback(callback: CallbackQuery):
    if not _admin_only_callback(callback):
        await callback.answer("❌ Только для админов", show_alert=True)
        return

    parts = (callback.data or "").split(":")
    if len(parts) < 3:
        await callback.answer("❌ Некорректные данные", show_alert=True)
        return

    action = parts[1]
    try:
        tg_id = int(parts[2])
    except ValueError:
        await callback.answer("❌ Некорректный ID", show_alert=True)
        return

    notice = ""
    if action in ("d7", "d30", "d90"):
        days_map = {"d7": 7, "d30": 30, "d90": 90}
        ok, msg = await _apply_find_add_days(tg_id, days_map[action])
        if not ok:
            await callback.answer(msg, show_alert=True)
            return
        notice = msg
    elif action in ("g10", "g50"):
        gb_map = {"g10": 10.0, "g50": 50.0}
        ok, msg = await _apply_find_add_traffic(tg_id, gb_map[action])
        if not ok:
            await callback.answer(msg, show_alert=True)
            return
        notice = msg
    else:
        await callback.answer("❌ Неизвестное действие", show_alert=True)
        return

    text, keyboard = await _build_find_message(tg_id, notice=notice)
    await _refresh_find_message(callback, text, keyboard)
    await callback.answer(notice)
