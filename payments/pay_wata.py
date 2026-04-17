import uuid
from datetime import timedelta
from typing import Any, Dict, Literal

import aiohttp
from aiogram import Router, F
from aiogram.types import CallbackQuery

from bot import sql
from config import ADMIN_IDS, BOT_URL, WATA_API_BASE, WATA_API_CARD_KEY, WATA_API_SBP_KEY
from keyboard import keyboard_payment_sbp, create_kb, BTN_BACK
from lexicon import dct_price, dct_desc, lexicon
from logging_config import logger

router = Router()

WataKind = Literal["sbp", "card"]

# Не ставим canceled в БД по «только declined» в API раньше этого срока с момента создания платёжной записи:
# у WATA иногда сначала приходит Declined, а Paid появляется позже.
WATA_DECLINED_CANCEL_GRACE_AFTER_LINK = timedelta(minutes=30)


class WataPayment:
    """Клиент WATA H2H API (платёжные ссылки)."""

    def __init__(self, access_token: str, base_url: str = WATA_API_BASE):
        self.access_token = access_token
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def create_payment_link(
        self,
        amount: float,
        currency: str,
        description: str,
        order_id: str,
        success_redirect_url: str,
        fail_redirect_url: str,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/links"
        body = {
            "type": "OneTime",
            "amount": round(float(amount), 2),
            "currency": currency,
            "description": description[:500] if description else "",
            "orderId": order_id,
            "successRedirectUrl": success_redirect_url,
            "failRedirectUrl": fail_redirect_url,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=self.headers) as response:
                text = await response.text()
                if response.status != 200:
                    logger.error(f"WATA create link {response.status}: {text}")
                    raise RuntimeError(f"WATA create link HTTP {response.status}")
                return await response.json()

    async def search_transactions_by_order_id(self, order_id: str) -> list:
        url = f"{self.base_url}/transactions"
        params = {"orderId": order_id, "maxResultCount": 100}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, params=params) as response:
                text = await response.text()
                if response.status == 429:
                    logger.warning(f"WATA transactions rate limit for orderId={order_id}: {text}")
                    raise RuntimeError("WATA rate limit")
                if response.status != 200:
                    logger.error(f"WATA transactions {response.status}: {text}")
                    raise RuntimeError(f"WATA transactions HTTP {response.status}")
                data = await response.json()
        return data.get("items") or data.get("Items") or []


def _wata_amount_rub(val: str) -> float:
    x = float(val)
    if x < 10:
        return 10.0
    return round(x, 2)


def wata_transactions_status_counts(items: list) -> Dict[str, int]:
    """Счётчики по статусам строк Payment в ответе WATA (для логов)."""
    counts: Dict[str, int] = {}
    for i in items:
        kind = (i.get("kind") or i.get("Kind") or "").strip()
        if kind.lower() != "payment":
            continue
        raw = (i.get("status") or i.get("Status") or "").strip()
        s = raw.lower() if raw else "?"
        counts[s] = counts.get(s, 0) + 1
    return counts


def wata_order_payment_state(items: list, expect_type: str) -> str:
    """
    expect_type: SBP | CardCrypto
    Возвращает внутренний статус: pending | paid | declined | wrong_paid
    """
    payments = [i for i in items if (i.get("kind") or i.get("Kind")) == "Payment"]
    if not payments:
        return "pending"

    def _status(x):
        return (x.get("status") or x.get("Status") or "").strip()

    def _type(x):
        return (x.get("type") or x.get("Type") or "").strip()

    correct_paid = [p for p in payments if _status(p) == "Paid" and _type(p) == expect_type]
    if correct_paid:
        return "paid"

    wrong_paid = [p for p in payments if _status(p) == "Paid" and _type(p) != expect_type]
    if wrong_paid:
        return "wrong_paid"

    open_states = ("Created", "Pending")
    if any(_status(p) in open_states for p in payments):
        return "pending"

    if any(_status(p) == "Declined" for p in payments):
        return "declined"

    return "pending"


async def pay(
    val: str,
    des: str,
    user_id: str,
    duration: str,
    white: bool,
    kind: WataKind,
) -> Dict[str, Any]:
    token = WATA_API_SBP_KEY if kind == "sbp" else WATA_API_CARD_KEY
    if not token:
        logger.error(f"WATA: отсутствует токен для {kind}")
        return {"status": "error", "url": "", "id": ""}

    method = "wata_sbp" if kind == "sbp" else "wata_card"
    payload = f"user_id:{user_id},duration:{duration},white:{white},gift:False,method:{method},amount:{int(val)}"
    order_id = f"{method}-{uuid.uuid4().hex}"
    amount_api = _wata_amount_rub(val)

    client = WataPayment(token)
    try:
        result = await client.create_payment_link(
            amount=amount_api,
            currency="RUB",
            description=des,
            order_id=order_id,
            success_redirect_url=BOT_URL,
            fail_redirect_url=BOT_URL,
        )
        pay_url = result.get("url") or result.get("Url") or ""
        if not pay_url:
            logger.error(f"WATA: пустая ссылка в ответе {result}")
            return {"status": "error", "url": "", "id": ""}

        if kind == "sbp":
            await sql.add_wata_sbp_payment(
                int(user_id), int(val), "pending", order_id, payload, is_gift=False
            )
        else:
            await sql.add_wata_card_payment(
                int(user_id), int(val), "pending", order_id, payload, is_gift=False
            )

        logger.info(f"WATA {method}: ссылка создана orderId={order_id}")
        return {"status": "pending", "url": pay_url, "id": order_id}
    except Exception as e:
        logger.error(f"WATA create payment: {e}")
        return {"status": "error", "url": "", "id": ""}


async def pay_for_gift(
    val: str,
    des: str,
    user_id: str,
    duration: str,
    white: bool,
    kind: WataKind,
) -> Dict[str, Any]:
    token = WATA_API_SBP_KEY if kind == "sbp" else WATA_API_CARD_KEY
    if not token:
        logger.error(f"WATA: отсутствует токен для {kind}")
        return {"status": "error", "url": "", "id": ""}

    method = "wata_sbp" if kind == "sbp" else "wata_card"
    payload = f"user_id:{user_id},duration:{duration},white:{white},gift:True,method:{method},amount:{int(val)}"
    order_id = f"{method}-gift-{uuid.uuid4().hex}"
    amount_api = _wata_amount_rub(val)

    client = WataPayment(token)
    try:
        result = await client.create_payment_link(
            amount=amount_api,
            currency="RUB",
            description=des,
            order_id=order_id,
            success_redirect_url=BOT_URL,
            fail_redirect_url=BOT_URL,
        )
        pay_url = result.get("url") or result.get("Url") or ""
        if not pay_url:
            return {"status": "error", "url": "", "id": ""}

        if kind == "sbp":
            await sql.add_wata_sbp_payment(
                int(user_id), int(val), "pending", order_id, payload, is_gift=True
            )
        else:
            await sql.add_wata_card_payment(
                int(user_id), int(val), "pending", order_id, payload, is_gift=True
            )

        return {"status": "pending", "url": pay_url, "id": order_id}
    except Exception as e:
        logger.error(f"WATA gift payment: {e}")
        return {"status": "error", "url": "", "id": ""}


def _duration_from_wata_callback(data: str, prefix: str, gift_prefix: str) -> tuple[str, bool]:
    gift_flag = False
    if data.startswith(gift_prefix):
        gift_flag = True
        duration = data[len(gift_prefix) :]
    else:
        duration = data[len(prefix) :]
    return duration, gift_flag


@router.callback_query(F.data.startswith("wata_sbp_"))
async def process_payment_wata_sbp(callback: CallbackQuery):
    await callback.answer()
    data = callback.data
    duration, gift_flag = _duration_from_wata_callback(data, "wata_sbp_r_", "wata_sbp_gift_r_")
    desc_key = duration
    rub_amount = dct_price[duration]
    if callback.from_user.id in ADMIN_IDS:
        rub_amount = 1
    user_id = str(callback.from_user.id)
    white_flag = False
    if "white" in duration:
        duration = duration.replace("white_", "")
        white_flag = True
    if "old" in duration:
        duration = duration.replace("old", "")

    if gift_flag:
        payment_info = await pay_for_gift(
            val=str(rub_amount),
            des=f"Подписка в подарок {dct_desc[desc_key]}",
            user_id=user_id,
            duration=duration,
            white=white_flag,
            kind="sbp",
        )
    else:
        payment_info = await pay(
            val=str(rub_amount),
            des=dct_desc[desc_key],
            user_id=user_id,
            duration=duration,
            white=white_flag,
            kind="sbp",
        )

    if payment_info["status"] == "pending":
        try:
            text = lexicon["payment_link"]
            if white_flag:
                text = lexicon["payment_link_white"]
            if gift_flag:
                text += "\n\nДля оплаты <b>подарочной подписки</b> перейдите по ссылке:"
            else:
                text += "\n\nДля оплаты тарифа перейдите по ссылке:"
            await callback.message.edit_text(
                text=text,
                reply_markup=keyboard_payment_sbp("⚡ Оплатить СБП", payment_info["url"]),
            )
            logger.info(f"Юзер {user_id} создал WATA СБП {rub_amount} руб")
        except Exception as e:
            logger.error(f"WATA СБП UI: {e}")
            await callback.message.answer(lexicon["error_payment"], reply_markup=create_kb(1, back_to_main=BTN_BACK))


@router.callback_query(F.data.startswith("wata_card_"))
async def process_payment_wata_card(callback: CallbackQuery):
    await callback.answer()
    data = callback.data
    duration, gift_flag = _duration_from_wata_callback(data, "wata_card_r_", "wata_card_gift_r_")
    desc_key = duration
    rub_amount = dct_price[duration]
    if callback.from_user.id in ADMIN_IDS:
        rub_amount = 1
    user_id = str(callback.from_user.id)
    white_flag = False
    if "white" in duration:
        duration = duration.replace("white_", "")
        white_flag = True
    if "old" in duration:
        duration = duration.replace("old", "")

    if gift_flag:
        payment_info = await pay_for_gift(
            val=str(rub_amount),
            des=f"Подписка в подарок {dct_desc[desc_key]}",
            user_id=user_id,
            duration=duration,
            white=white_flag,
            kind="card",
        )
    else:
        payment_info = await pay(
            val=str(rub_amount),
            des=dct_desc[desc_key],
            user_id=user_id,
            duration=duration,
            white=white_flag,
            kind="card",
        )

    if payment_info["status"] == "pending":
        try:
            text = lexicon["payment_link"]
            if white_flag:
                text = lexicon["payment_link_white"]
            if gift_flag:
                text += "\n\nДля оплаты <b>подарочной подписки</b> перейдите по ссылке:"
            else:
                text += "\n\nДля оплаты тарифа перейдите по ссылке:"
            await callback.message.edit_text(
                text=text,
                reply_markup=keyboard_payment_sbp("💳 Оплатить картой РФ", payment_info["url"]),
            )
            logger.info(f"Юзер {user_id} создал WATA Карта {rub_amount} руб")
        except Exception as e:
            logger.error(f"WATA Card UI: {e}")
            await callback.message.answer(lexicon["error_payment"], reply_markup=create_kb(1, back_to_main=BTN_BACK))
