"""Офлайн-конверсии Яндекс Метрики для метки happ_now_global (счётчик 114348001)."""
from __future__ import annotations

import time
from decimal import Decimal, ROUND_HALF_UP

import aiohttp

from bot import bot, sql
from config import (
    CHECKER_ID,
    LEAD_TRACKER_STAR_RUB_PER_STAR,
    YANDEX_ACCESS_TOKEN,
    YANDEX_METRIKA_HAPP_NOW_COUNTER_ID,
    YANDEX_METRIKA_TRIAL_GOAL_TARGET,
)
from logging_config import logger

_HAPP_NOW_GLOBAL_STAMP = "happ_now_global"

_PANEL_DAYS_TO_PAYMENT_GOAL: dict[int, str] = {
    7: "payment_success_week",
    30: "payment_success_month",
    90: "payment_success_3month",
    180: "payment_success_6month",
    365: "payment_success_year",
    730: "payment_success_2year",
}


def is_enabled() -> bool:
    return bool((YANDEX_ACCESS_TOKEN or "").strip())


def _counter_id_for_stamp(stamp: str) -> int | None:
    key = (stamp or "").strip()
    if key == _HAPP_NOW_GLOBAL_STAMP:
        return YANDEX_METRIKA_HAPP_NOW_COUNTER_ID
    return None


def _payment_goal_for_panel_days(panel_days: int | None) -> str | None:
    if panel_days is None:
        return None
    return _PANEL_DAYS_TO_PAYMENT_GOAL.get(int(panel_days))


def _payment_rub(method: str, amount: int | float) -> int:
    if method == "stars":
        stars = Decimal(str(amount))
        rate = Decimal(str(LEAD_TRACKER_STAR_RUB_PER_STAR))
        rub = (stars * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return int(rub)
    return int(Decimal(str(amount)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _build_offline_csv(client_id: str, target: str, dt_sec: int, price_rub: int) -> str:
    return (
        "ClientId,Target,DateTime,Price,Currency\n"
        f"{client_id},{target},{dt_sec},{price_rub},RUB\n"
    )


def _upload_url(counter_id: int) -> str:
    return (
        "https://api-metrika.yandex.net/management/v1/counter"
        f"/{counter_id}/offline_conversions/upload"
    )


async def _upload_offline_conversion_for_user(
    billing_user_id: int,
    *,
    target: str,
    price_rub: int,
    event_label: str,
) -> None:
    if not is_enabled():
        logger.debug("Yandex Metrica: пропуск, YANDEX_ACCESS_TOKEN не задан")
        return

    try:
        user = await sql.get_user_object_by_user_id(billing_user_id)
    except Exception as e:
        logger.error("Yandex Metrica: не удалось загрузить user_id={}: {}", billing_user_id, e)
        return

    if user is None:
        return

    yandex_id = (getattr(user, "yandex_id", None) or "").strip()
    if not yandex_id:
        return

    stamp = (user.stamp or "").strip()
    counter_id = _counter_id_for_stamp(stamp)
    if counter_id is None:
        logger.debug(
            "Yandex Metrica: пропуск user_id={} stamp={!r} (нет счётчика)",
            billing_user_id,
            stamp,
        )
        return

    upload_url = _upload_url(counter_id)
    dt_sec = int(time.time())
    csv_body = _build_offline_csv(yandex_id, target, dt_sec, price_rub)
    token = (YANDEX_ACCESS_TOKEN or "").strip()
    headers = {"Authorization": f"OAuth {token}"}

    logger.info(
        "Yandex Metrica: ({}) user_id={} counter={} stamp={!r} ClientId={} Target={} Price={}",
        event_label,
        billing_user_id,
        counter_id,
        stamp,
        yandex_id,
        target,
        price_rub,
    )

    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            form = aiohttp.FormData()
            form.add_field(
                "file",
                csv_body.encode("utf-8"),
                filename="offline-conversions.csv",
                content_type="text/csv",
            )
            async with session.post(upload_url, headers=headers, data=form) as resp:
                text = await resp.text()
                if resp.status not in (200, 201):
                    logger.warning(
                        "Yandex Metrica: HTTP {} user_id={} body={}",
                        resp.status,
                        billing_user_id,
                        text[:1500],
                    )
                else:
                    logger.info(
                        "Yandex Metrica: OK HTTP {} user_id={} body={}",
                        resp.status,
                        billing_user_id,
                        text[:500],
                    )
    except Exception as e:
        logger.error(
            "Yandex Metrica: ошибка POST user_id={} ClientId={}: {}",
            billing_user_id,
            yandex_id,
            e,
        )

    if CHECKER_ID is not None and event_label.startswith("payment"):
        try:
            await bot.send_message(
                chat_id=CHECKER_ID,
                text=(
                    f"Метрика happ_now_global: {event_label}, user_id={billing_user_id}, "
                    f"{price_rub} ₽, goal={target}, ClientId={yandex_id}"
                ),
            )
        except Exception as e:
            logger.error("Yandex Metrica: CHECKER_ID: {}", e)


async def report_landing_payment_to_metrica(
    billing_user_id: int,
    method: str,
    amount: int | float,
    *,
    panel_days: int | None = None,
) -> None:
    target = _payment_goal_for_panel_days(panel_days)
    if not target:
        return
    price_rub = _payment_rub(method, amount)
    await _upload_offline_conversion_for_user(
        billing_user_id,
        target=target,
        price_rub=price_rub,
        event_label=f"payment_{panel_days}d",
    )


async def report_trial_to_metrica(billing_user_id: int) -> None:
    await _upload_offline_conversion_for_user(
        billing_user_id,
        target=YANDEX_METRIKA_TRIAL_GOAL_TARGET,
        price_rub=0,
        event_label="trial",
    )