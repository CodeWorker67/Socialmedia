from bot import bot
from config import ADMIN_IDS
from keyboard import keyboard_payment_stars
from logging_config import logger

from aiogram import Router, F
from aiogram.types import CallbackQuery, LabeledPrice, PreCheckoutQuery, Message
from lexicon import lexicon
from payments.process_payload import process_confirmed_payment
from payments.tariff_gate import panel_days_from_tariff_key, tariff_period_label
from wl_traffic.texts import format_pro_payment_link


router: Router = Router()


def get_stars_amount(currency: str, duration: str) -> float:
    """Возвращает цену для тарифа в указанной криптовалюте"""
    prices = {
        'Stars': {'7': 99, '30': 299, '90': 749, '120': 749, '180': 1349, '365': 2399, '5000': 4990, '5000sale': 2790, 'white_30': 499}
    }
    return prices.get(currency, {}).get(duration, 0)


@router.callback_query(F.data.startswith('stars_'))
async def process_payment_stars(callback: CallbackQuery):
    gift_flag = False
    white_flag = False
    if 'gift_' in callback.data:
        gift_flag = True
    duration = callback.data.replace('stars_r_', '').replace('stars_gift_r_', '')

    stars_amount = get_stars_amount('Stars', duration)
    if callback.from_user.id in ADMIN_IDS:
        stars_amount = 1
    user_id = str(callback.from_user.id)

    if 'white' in duration:
        duration = duration.replace('white_', '')
        white_flag = True
    if 'old' in duration:
        duration = duration.replace('old', '')

    panel_days = panel_days_from_tariff_key(duration)

    payload = f"user_id:{user_id},duration:{duration},white:{white_flag},gift:{gift_flag},method:stars,amount:{stars_amount}"

    prices = [LabeledPrice(label="XTR", amount=stars_amount)]
    dur_label = tariff_period_label(duration)
    title = f"Оплата подписки {'в подарок другу ' if gift_flag else ''}на {dur_label}."
    description = lexicon['payment_link_white'] if white_flag else format_pro_payment_link(panel_days)
    await bot.send_invoice(
        callback.from_user.id,
        title=title,
        description=description,
        prices=prices,
        provider_token="",
        payload=payload,
        currency="XTR",
        reply_markup=keyboard_payment_stars(stars_amount),
    )


@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.answer(ok=True)


@router.message(F.content_type.in_({'successful_payment'}))
async def success_payment_handler(message: Message):
    payload = message.successful_payment.invoice_payload
    if not payload:
        logger.error(f"❌ Нет payload в платеже {message.successful_payment.invoice_payload}")
        return
    await process_confirmed_payment(payload)
