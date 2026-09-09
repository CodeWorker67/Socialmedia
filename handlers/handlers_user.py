import time
import urllib.parse
import requests
from datetime import datetime, timezone

from bot import sql, x3, bot
from lead_tracker import post_user_registered, post_user_trial, tracker_source_from_ref_and_stamp
from config import CHANEL_ID, ADMIN_IDS, BOT_URL, PARTNER_PROCENT, PARTNER_MIN, PARTNER_SUPPORT_URL, PUBLIC_SITE_URL
from keyboard import (keyboard_tariff_bonus, keyboard_tariff,
                      ref_keyboard, keyboard_gift_tariff, keyboard_payment_method,
                      keyboard_payment_method_stock,
                      keyboard_inline_ref, keyboard_partner_intro, keyboard_partner_dashboard,
                      keyboard_partner_withdraw, keyboard_buy_menu, keyboard_earn_with_us,
                      create_kb, STYLE_PRIMARY, OPEN_SITE_CB, SITE_URL,
                      keyboard_subscription_manage, keyboard_about_service, ABOUT_SERVICE_CB, BTN_BACK)
from utils.menu_ui import (
    MAIN_MENU_BUTTON_TEXT,
    edit_or_send_photo,
    show_main_menu,
    show_connect_screen,
    trial_success_caption,
)
from web_api import create_bot_site_login_token
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatMemberUpdated,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import ChatMemberUpdatedFilter, KICKED, MEMBER, Command
from lexicon import lexicon
from payments.tariff_gate import panel_days_from_tariff_key, tariff_key_from_callback, tariff_period_label
from wl_traffic.texts import format_pro_payment_link

TARIFF_CALLBACKS = frozenset({
    'r_7', 'r_30', 'r_90', 'r_180', 'r_365', 'r_white_30', 'r_5000', 'r_5000sale',
})
from wl_traffic.service import credit_wl_subscription_bonus


router: Router = Router()

_TRIAL_RETURN_GET_CB = "trial_return_get"
_USER_TUPLE_SUBSCRIPTION_END_DATE = 9
_USER_TUPLE_FIELD_BOOL_3 = 26

_R120_PAYMENT_TEXT = (
    "🎁 Акция: 3 + 1 месяц в подарок!\n"
    "Ускоритель соцсетей — стабильный доступ к Instagram, YouTube и другим сервисам.\n"
    "5 устройств, безлимитный трафик на обычные сервера.\n\n"
    "📡 Антиглушилка: <b>+40 GB</b> трафика включено в тариф.\n\n"
    "<b>Подписка начисляется в течении 1 часа</b>\n\n"
    "Выберите способ оплаты:"
)


def _user_has_active_pro_subscription(user_data: tuple) -> bool:
    sub_end = user_data[_USER_TUPLE_SUBSCRIPTION_END_DATE]
    if sub_end is None:
        return False
    if sub_end.tzinfo is None:
        aware = sub_end.replace(tzinfo=timezone.utc)
    else:
        aware = sub_end.astimezone(timezone.utc)
    return aware.date() >= datetime.now(timezone.utc).date()


async def _panel_regular_subscription_is_active(uid: int) -> bool:
    existing = await x3.get_user_by_username(str(uid))
    if not existing or not existing.get("response"):
        return False
    user = existing["response"]
    if isinstance(user, list):
        user = user[0]
    expire_at_str = user.get("expireAt")
    if not expire_at_str:
        return False
    expire_at = datetime.fromisoformat(expire_at_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    return user.get("status") == "ACTIVE" and expire_at > now


# Этот хэндлер срабатывает на команду /start
@router.message(Command(commands="start"))
async def process_start_command(message: Message, command: Command):

    user_data = await sql.get_user(message.from_user.id)
    in_panel = False
    ref_login = ''
    partner_login = ''
    existing = False
    stamp = ''
    ttclid = None

    if user_data:
        in_panel = user_data[4]
        existing = True

    if len(message.text.split(' ')) == 1:
        if user_data:
            logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно')
        else:
            logger.success(f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз')

    else:
        start_arg = message.text.split(' ', 1)[1]

        if start_arg.startswith('partner_'):
            if user_data:
                logger.info(
                    f'Юзер {message.from_user.id} - {message.from_user.username} '
                    f'нажал старт повторно с партнёрской ссылкой'
                )
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} '
                    f'зашел в бота в первый раз по партнёрской ссылке'
                )
                raw_partner = start_arg.replace('partner_', '', 1)
                if raw_partner.isdigit() and raw_partner != str(message.from_user.id):
                    partner_login = raw_partner

        elif start_arg.startswith('ref') or 'ref' in start_arg:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с реферальной ссылкой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по реферальной ссылкой')
                ref_login = start_arg.replace('ref', '', 1)

        elif start_arg.startswith('gift_') or 'gift_' in start_arg:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} пытается активировать подарочную подписку')
            gift_id = start_arg.replace('gift_', '', 1)
            in_panel = await activate_gift(message, gift_id)
            await asyncio.sleep(2)
            existing = True

        elif start_arg.startswith('auth_'):
            auth_token = start_arg.replace('auth_', '', 1)
            from web_api import confirm_tg_auth_token
            ok = confirm_tg_auth_token(
                auth_token,
                message.from_user.id,
                first_name=message.from_user.first_name or "",
                username=message.from_user.username,
            )
            if ok:
                logger.info(f'Юзер {message.from_user.id} авторизован на сайте через deeplink')
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                dashboard_url = f"{PUBLIC_SITE_URL}/dashboard" if PUBLIC_SITE_URL else ""
                if dashboard_url:
                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="🌐 Перейти в личный кабинет",
                                    url=dashboard_url,
                                )
                            ]
                        ]
                    )
                    await message.answer("✅ Вы авторизованы на сайте!", reply_markup=kb)
                else:
                    await message.answer("✅ Вы авторизованы на сайте! Вернитесь во вкладку с сайтом.")
            else:
                await message.answer("❌ Ссылка устарела. Попробуйте ещё раз на сайте.")
            if not user_data:
                await sql.add_user(message.from_user.id, False, False)
            existing = True

        elif start_arg.startswith('ttclid_') or 'ttclid_' in start_arg:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой ttclid')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке ttclid')
                stamp = 'YuraTT'
                ttclid = start_arg.replace('ttclid_', '', 1).replace('_', '.')

                payload = {
                    'event_source': 'web',
                    'event_source_id': 'D5U8OFJC77U9E3ANE170',
                    'data': [
                        {
                            'event': 'Subscribe',
                            'event_time': int(time.time()),
                            'context': {
                                'ad': {
                                    'callback': ttclid
                                }
                            }
                        }
                    ]
                }
                response = requests.post(
                    'https://business-api.tiktok.com/open_api/v1.3/event/track/',
                    json=payload,
                    headers={
                        'Content-Type': 'application/json',
                        'Access-Token': '7a9d82c42eaccd2393b74f31975fb8cc96bbb5d6'
                    },
                    timeout=2
                )

                if response.status_code == 200:
                    logger.success('Пиксель успешно отправлен в TikTok')
                else:
                    logger.error(f'Ошибка TikTok API: статус {response.status_code}, ответ: {response.text}')
        else:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке')
                stamp = start_arg

    if not existing:
        inserted = await sql.add_user(
            message.from_user.id, False, False,
            ref=ref_login, stamp=stamp, partner=partner_login,
        )
        if inserted:
            logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} добавлен в БД')
            src = tracker_source_from_ref_and_stamp(ref_login, stamp, partner_login)
            await post_user_registered(
                message.from_user.id,
                message.from_user.username,
                message.from_user.full_name,
                src,
            )
        if ttclid:
            await sql.update_ttclid(message.from_user.id, ttclid)
            logger.info(f'Юзеру {message.from_user.id} - {message.from_user.username} присвоен ttclid')

    await show_main_menu(message, send_hint=True)


@router.message(F.text == MAIN_MENU_BUTTON_TEXT)
async def main_menu_reply_button(message: Message):
    await show_main_menu(message, send_hint=False)


def _site_base_url() -> str:
    return (PUBLIC_SITE_URL or SITE_URL).rstrip("/")


def _site_login_url(telegram_user_id: int, first_name: str, username: str | None) -> str:
    token = create_bot_site_login_token(
        telegram_user_id=telegram_user_id,
        first_name=first_name,
        username=username,
    )
    return f"{_site_base_url()}/auth/bot?token={urllib.parse.quote(token, safe='')}"


@router.callback_query(F.data == OPEN_SITE_CB)
async def open_site_callback(callback: CallbackQuery):
    """Ссылка на сайт с одноразовым токеном для авто-входа."""
    await callback.answer()
    u = callback.from_user
    login_url = _site_login_url(
        u.id,
        u.first_name or "",
        u.username,
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🌐 Открыть сайт",
                    url=login_url,
                )
            ],
            [
                InlineKeyboardButton(
                    text=BTN_BACK,
                    callback_data="back_to_main",
                )
            ],
        ]
    )
    await edit_or_send_photo(
        callback,
        "our_site",
        lexicon["site_login_hint"],
        kb,
    )


@router.callback_query(F.data == 'buy_vpn')
async def buy_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['buy_menu'],
        keyboard_buy_menu(),
    )


@router.callback_query(F.data == 'buy_vpn_self')
async def buy_vpn_self_cb(callback: CallbackQuery):
    await callback.answer()
    user_data = await sql.get_user(callback.from_user.id)
    in_panel = False

    if user_data is not None and len(user_data) > 4:
        in_panel = user_data[4]

    result_active = await x3.activ(str(callback.from_user.id))

    if result_active['activ'] == '🔎 - Не подключён' and not in_panel:
        kb = keyboard_tariff_bonus()
    else:
        kb = keyboard_tariff()

    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['buy'],
        kb,
    )


@router.callback_query(F.data == 'connect_vpn')
async def direct_connect_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    await show_connect_screen(callback)


@router.callback_query(F.data == _TRIAL_RETURN_GET_CB)
async def trial_return_get_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    user_data = await sql.get_user(uid)
    if user_data is None:
        await sql.add_user(uid, False)
        user_data = await sql.get_user(uid)

    if user_data[_USER_TUPLE_FIELD_BOOL_3]:
        await callback.answer("Вы уже взяли свой триал!", show_alert=True)
        return

    await callback.answer()

    user_id_str = str(uid)
    panel_user = await x3.get_user_by_username(user_id_str)
    if panel_user and panel_user.get("response"):
        ok = await x3.updateClient(7, user_id_str, uid)
    else:
        ok = await x3.addClient(7, user_id_str, uid)

    if not ok:
        await callback.message.answer(
            "Не удалось начислить дни. Попробуйте позже или напишите в поддержку."
        )
        return

    await sql.update_in_panel(uid)
    await sql.init_wl_trial_limits(uid)
    await sql.update_field_bool_3(uid, True)
    await post_user_trial(uid)
    await callback.message.answer(
        "🎉 Поздравляем! Вы получили 7 триальных дней доступа к Ускорителю соцсетей! ✨",
        reply_markup=create_kb(
            1,
            styles={"connect_vpn": STYLE_PRIMARY},
            connect_vpn="🔗 Подключить VPN",
        ),
    )


def _duration_days_from_tariff_cb(data: str) -> int:
    return panel_days_from_tariff_key(tariff_key_from_callback(data))


@router.callback_query(F.data == 'r_120')
async def process_payment_method_promo_120(callback: CallbackQuery):
    uid = callback.from_user.id
    if await sql.user_has_promo_120_payment(uid):
        await callback.answer(
            "Вы уже воспользовались этой акцией!",
            show_alert=True,
        )
        return
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        _R120_PAYMENT_TEXT,
        keyboard_payment_method_stock("r_120"),
    )


@router.callback_query(F.data.in_(TARIFF_CALLBACKS))
async def process_payment_method(callback: CallbackQuery):
    await callback.answer()
    if 'white' in callback.data:
        await sql.add_white_counter_if_not_exists(callback.from_user.id)
        text = lexicon['payment_link_white']
    else:
        text = format_pro_payment_link(_duration_days_from_tariff_cb(callback.data))
    text += '\n\nВыберите способ оплаты:'
    tariff = callback.data
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_payment_method(tariff),
    )


@router.callback_query(F.data == 'free_vpn')
async def free_vpn_cb(callback: CallbackQuery):
    day = 3

    user_data = await sql.get_user(callback.from_user.id)
    in_panel = False
    if user_data is not None and len(user_data) > 4:
        in_panel = user_data[4]
    if in_panel:
        await callback.answer()
        await show_main_menu(callback)
        return
    logger.info(await x3.addClient(day, str(callback.from_user.id), int(callback.from_user.id)))
    result_active = await x3.activ(str(callback.from_user.id))
    time = result_active['time']

    if await sql.get_user(callback.from_user.id) is not None:
        await sql.update_in_panel(callback.from_user.id)
    else:
        await sql.add_user(callback.from_user.id, True)
    await sql.init_wl_trial_limits(callback.from_user.id)
    user_id = str(callback.from_user.id)
    sub_url = await x3.sublink(user_id)

    await callback.answer()
    await edit_or_send_photo(
        callback,
        "subscription_manage",
        trial_success_caption(time, sub_url),
        keyboard_subscription_manage(sub_url or ""),
    )
    await post_user_trial(callback.from_user.id)


@router.callback_query(F.data == 'info')
async def faq(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "about_service",
        lexicon['about_service'],
        keyboard_about_service(),
    )


@router.callback_query(F.data == 'earn_with_us')
async def earn_with_us_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['earn_menu'],
        keyboard_earn_with_us(),
    )


@router.callback_query(F.data == ABOUT_SERVICE_CB)
async def about_service_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "about_service",
        lexicon['about_service'],
        keyboard_about_service(),
    )


@router.callback_query(F.data == 'back_to_earn')
async def back_to_earn_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['earn_menu'],
        keyboard_earn_with_us(),
    )


@router.callback_query(F.data == 'ref')
async def referral_program(callback: CallbackQuery):
    await callback.answer()
    count = await sql.select_ref_count(int(callback.from_user.id))
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['ref_info'].format(count, callback.from_user.id),
        ref_keyboard(callback.from_user.id),
    )


async def _ensure_user_exists(user_id: int) -> None:
    if await sql.get_user(user_id) is None:
        await sql.add_user(user_id, False, False)


async def _send_partner_dashboard(callback: CallbackQuery) -> None:
    tg_id = callback.from_user.id
    user = await sql.get_user_object_by_user_id(tg_id)
    if user is None:
        await _ensure_user_exists(tg_id)
        user = await sql.get_user_object_by_user_id(tg_id)

    referrals = await sql.select_partner_count(tg_id)
    payments_sum = await sql.select_partner_referrals_payments_sum(tg_id)
    balance = user.partner_balance or 0
    paid_out = user.partner_pay or 0
    total_earned = balance + paid_out
    link = f"{BOT_URL}?start=partner_{tg_id}"

    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['partner_dashboard'].format(
            link=link,
            procent=PARTNER_PROCENT,
            referrals=referrals,
            payments_sum=payments_sum,
            total_earned=total_earned,
            paid_out=paid_out,
            balance=balance,
        ),
        keyboard_partner_dashboard(),
    )


@router.callback_query(F.data == 'partner_earn')
async def partner_program(callback: CallbackQuery):
    await callback.answer()
    await _ensure_user_exists(callback.from_user.id)
    user = await sql.get_user_object_by_user_id(callback.from_user.id)

    if user and user.partner_flag:
        await _send_partner_dashboard(callback)
    else:
        await edit_or_send_photo(
            callback,
            "earn_with_us",
            lexicon['partner_intro'].format(
                procent=PARTNER_PROCENT,
                min_sum=PARTNER_MIN,
            ),
            keyboard_partner_intro(),
        )


@router.callback_query(F.data == 'partner_create_link')
async def partner_create_link(callback: CallbackQuery):
    await callback.answer()
    await _ensure_user_exists(callback.from_user.id)
    await sql.update_partner_flag(callback.from_user.id, True)
    await _send_partner_dashboard(callback)


@router.callback_query(F.data == 'partner_withdraw')
async def partner_withdraw(callback: CallbackQuery):
    user = await sql.get_user_object_by_user_id(callback.from_user.id)
    if user is None:
        await callback.answer()
        return

    balance = user.partner_balance or 0
    if balance < PARTNER_MIN:
        await callback.answer(
            lexicon['partner_withdraw_alert'].format(min_sum=PARTNER_MIN),
            show_alert=True,
        )
        return

    await callback.answer()
    support_url = PARTNER_SUPPORT_URL or "https://t.me/"
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['partner_withdraw_info'].format(
            balance=balance,
            min_sum=PARTNER_MIN,
        ),
        keyboard_partner_withdraw(support_url),
    )


@router.callback_query(F.data == 'buy_gift')
async def gift_subscription_start(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['gift_start'],
        keyboard_gift_tariff(),
    )


@router.callback_query(F.data.startswith('gift_'))
async def process_gift_payment_method(callback: CallbackQuery):
    await callback.answer()
    if 'white' in callback.data:
        await sql.add_white_counter_if_not_exists(callback.from_user.id)
        text = lexicon['payment_link_white']
    else:
        text = format_pro_payment_link(_duration_days_from_tariff_cb(callback.data))
    tariff = callback.data
    text += '\n\nВыберите способ оплаты <b>подарочной подписки</b>:'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_payment_method(tariff),
    )


async def activate_gift(message: Message, gift_id: str):
    """Активация подарка по gift_id"""
    result = await sql.activate_gift(gift_id, message.from_user.id)

    if not result[0]:
        await message.answer(lexicon['gift_no'])
        logger.warning(f'Ссылка на подарок протухла')
        if await sql.get_user(message.from_user.id) is None:
            await sql.add_user(message.from_user.id, False)
            logger.success(
                f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по подарочной ссылке')
        return False

    duration = result[1]
    white_flag = result[2]

    # Активируем подписку для получателя
    # await x3.test_connect()
    user_id = message.from_user.id
    user_id_str = str(message.from_user.id)
    if white_flag:
        user_id_str += '_white'

    was_in_db = await sql.get_user(message.from_user.id) is not None
    if not was_in_db:
        await sql.add_user(message.from_user.id, False)


    # Проверяем существует ли пользователь
    existing_user = await x3.get_user_by_username(user_id_str)

    if existing_user and 'response' in existing_user and existing_user['response']:
        response = await x3.updateClient(duration, user_id_str, user_id)
    else:
        response = await x3.addClient(duration, user_id_str, user_id)

    if response:
        # Получаем информацию о подписке
        result_active = await x3.activ(user_id_str)
        subscription_time = result_active.get('time', '-')

        # Обновляем базу данных
        await sql.update_in_panel(message.from_user.id)
        await credit_wl_subscription_bonus(sql, message.from_user.id, int(duration))
        if was_in_db:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} получил в подарок подписку, уже был в БД')
        else:
            logger.success(
                f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз и получил подарочную подписку')

        # Отправляем сообщение получателю
        await message.answer(lexicon['gift_yes'].format(tariff_period_label(duration), subscription_time))
        return True

    else:
        await message.answer("❌ Ошибка при активации подарка. Обратитесь в поддержку.")
        if await sql.get_user(message.from_user.id) is None:
            await sql.add_user(message.from_user.id, False)
        return False


@router.callback_query(F.data == 'video_faq')
async def video_faq(callback: CallbackQuery):
    await callback.message.answer_video(video='BAACAgQAAxkBAAFX48BqJ_yHUY5sb-uIeu3-8okY4WebXwACLx0AAgNPQVH61S2gMU3KZzsE',
                                        reply_markup=create_kb(1, back_to_main='🔙 Назад'))


@router.callback_query(F.data == 'back_to_buy_menu')
async def handle_back_to_buy_menu(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['buy_menu'],
        keyboard_buy_menu(),
    )


@router.callback_query(F.data == 'back_to_main')
async def handle_back_to_menu(callback: CallbackQuery):
    await callback.answer()
    await show_main_menu(callback)


@router.callback_query(F.data == 'back_to_gift_menu')
async def handle_back_to_gift_menu(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['gift_start'],
        keyboard_gift_tariff(),
    )


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def user_blocked_bot(event: ChatMemberUpdated):
    await sql.update_delete(event.from_user.id, True)
    logger.warning(f'Юзер {event.from_user.id} заблокировал бота')


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def user_unblocked_bot(event: ChatMemberUpdated):
    await sql.update_delete(event.from_user.id, False)
    logger.success(f'Юзер {event.from_user.id} разблокировал бота')


@router.chat_member()
async def handle_chat_member_update(update: ChatMemberUpdated):
    if str(update.chat.id) != str(CHANEL_ID):
        return
    user_id = update.new_chat_member.user.id
    user_dct = await sql.get_user(user_id)

    if not user_dct:
        logger.warning(f"User in chanel {user_id} not found in database")
        return

    if update.old_chat_member.status == "left" and update.new_chat_member.status == "member":
        await sql.update_in_chanel(user_id, True)
        logger.success(f"User {user_id} connect to chanel")
    elif update.old_chat_member.status != "left" and update.new_chat_member.status == "left":
        await sql.update_in_chanel(user_id, False)
        logger.warning(f"User {user_id} left chanel")


@router.inline_query(lambda query: query.query == 'partner')
async def inline_partner(inline_query: InlineQuery):
    user_id = inline_query.from_user.id

    text = f'''
Привет. Подключись к Ускорителю соцсетей по моей ссылке:

{BOT_URL}?start=ref{user_id}

💥 Стабильный доступ к соцсетям
💫 Без навязчивой рекламы (где доступно)
👌🏻 Стабильное соединение даже в часы пик
    '''

    result = InlineQueryResultArticle(
        id="1",
        title='🤝🤝🤝 Приглашение',
        description="Друг, перешедший по этой кнопке станет Вашим рефералом.",
        input_message_content=InputTextMessageContent(
            message_text=text,
            parse_mode='HTML',
            disable_web_page_preview=False
        ),
        reply_markup=keyboard_inline_ref(user_id),
        thumb_url="https://img.freepik.com/premium-photo/glowing-blue-neon-wifi-signal-icon-dark-background_989822-6238.jpg?semt=ais_hybrid",  # опционально: иконка
        thumb_width=50,
        thumb_height=50,
    )

    # Отправляем результат обратно в Telegram
    await bot.answer_inline_query(
        inline_query.id,
        results=[result],
        cache_time=0
    )