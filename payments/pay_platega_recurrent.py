"""Telegram-бот: рекуррентные СБП-подписки Platega."""
from aiogram import Router, F
from aiogram.types import CallbackQuery

from config import ADMIN_IDS, PLATEGA_API_KEY, PLATEGA_MERCHANT_ID
from keyboard import BTN_BACK, create_kb, keyboard_payment_sbp
from lexicon import dct_desc, dct_price, lexicon
from logging_config import logger
from payments.payload_source import BOT
from payments.platega_recurrent import create_recurrent_payment, is_recurrent_tariff
from wl_traffic.texts import format_pro_payment_link
from payments.tariff_gate import panel_days_from_tariff_key, tariff_key_from_callback
from utils.menu_ui import edit_or_send_photo

router = Router()


@router.callback_query(F.data.startswith('platega_rec_'))
async def process_platega_recurrent(callback: CallbackQuery):
    await callback.answer()
    if not PLATEGA_API_KEY or not PLATEGA_MERCHANT_ID:
        await callback.message.answer(
            lexicon['error_payment'],
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )
        return

    raw = callback.data.replace('platega_rec_', '')
    tariff_key = tariff_key_from_callback(raw)
    duration = tariff_key.replace('white_', '')
    if 'old' in duration:
        duration = duration.replace('old', '')

    if not is_recurrent_tariff(duration):
        await callback.message.answer(
            'Автоплатёж доступен для тарифов 7 дней, 1 / 3 / 6 месяцев и 1 год.',
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )
        return

    rub_amount = dct_price.get(duration) or dct_price.get(tariff_key)
    if rub_amount is None:
        await callback.message.answer(lexicon['error_payment'], reply_markup=create_kb(1, back_to_main=BTN_BACK))
        return
    if callback.from_user.id in ADMIN_IDS:
        rub_amount = 1

    user_id = callback.from_user.id
    payment_info = await create_recurrent_payment(
        user_id=user_id,
        duration=duration,
        amount=int(rub_amount),
        description=dct_desc.get(duration, dct_desc.get(tariff_key, 'Подписка')),
        white=False,
        source=BOT,
    )

    if payment_info.get('status') == 'rate_limited':
        await callback.message.answer(
            lexicon['payment_too_many_pending'].format(8),
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )
        return

    if payment_info.get('status') in ('pending', 'PENDING') and payment_info.get('url'):
        days = panel_days_from_tariff_key(tariff_key)
        text = format_pro_payment_link(days)
        text += (
            '\n\nДля привязки счёта и оплаты перейдите по ссылке:\n'
            '<i>Подписка продлевается автоматически.</i>'
        )
        await edit_or_send_photo(
            callback,
            "buy_subscription",
            text,
            keyboard_payment_sbp('⚡ Оплатить СБП', payment_info['url']),
        )
        logger.info('User {} created Platega recurrent {} ₽ duration={}', user_id, rub_amount, duration)
        return

    await callback.message.answer(lexicon['error_payment'], reply_markup=create_kb(1, back_to_main=BTN_BACK))
