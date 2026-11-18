"""Выгрузка статистики по Yandex Direct (метки /start YD…, users.yandex_id)."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import select, union_all

from config import LEAD_TRACKER_STAR_RUB_PER_STAR
from config_bd.models import (
    AsyncSessionLocal,
    PaymentsCryptobot,
    PaymentsStars,
    Users,
    YandexDirectSpend,
)
from config_bd.utils import (
    _BILLING_OK_STATUSES,
    _MERGE_PAYMENT_MODELS,
    _parse_payment_payload_map,
)
from wl_traffic.service import is_forever_end_date


def _subscription_active_at(sub_end: Optional[datetime], *, at: datetime) -> bool:
    if sub_end is None:
        return False
    if is_forever_end_date(sub_end):
        return True
    if sub_end.tzinfo is None:
        aware = sub_end.replace(tzinfo=timezone.utc)
    else:
        aware = sub_end.astimezone(timezone.utc)
    return aware > at

MSK = ZoneInfo("Europe/Moscow")

# С 30.10.2026 10:00 МСК — логика по оплатам (9 ₽ / >9 ₽) в payload.
YD_EXPORT_NEW_LOGIC_CUTOFF = datetime(2026, 10, 30, 10, 0, tzinfo=MSK)
# С 03.11.2026 10:20 МСК — колонки триала/промо по users.trial и оплатам >9 ₽.
YD_EXPORT_TRIAL_FIELD_CUTOFF = datetime(2026, 11, 3, 10, 20, tzinfo=MSK)
_TRIAL_PAYMENT_RUB = 9

YD_EXPORT_XLSX_HEADERS: Tuple[str, ...] = (
    "Ключ",
    "Всего пользователей",
    "С подпиской",
    "С активной оплаченной подпиской",
    "Оплатили после триала",
    "По промокоду",
    "По промокоду(актив)",
    "Без подписки",
    "Оплаты от клиентов",
)

YD_SPEND_RUB_PER_USD = Decimal("85")

YD_EXPORT_XLSX_SPEND_HEADERS: Tuple[str, ...] = (
    "Spend, руб (x85)",
    "Spend, $",
)

_TRIAL_LENGTH = timedelta(hours=24)


@dataclass
class _YdStampAgg:
    users: int = 0
    with_subscription: int = 0
    with_paid_active_subscription: int = 0
    paid_after_trial: int = 0
    by_promo: int = 0
    by_promo_active: int = 0
    no_subscription: int = 0
    client_payments_rub: int = 0


def _utc_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _paid_after_trial(
    create_user: Optional[datetime],
    first_payment_at: Optional[datetime],
) -> bool:
    if create_user is None or first_payment_at is None:
        return False
    return (
        _utc_aware(first_payment_at) - _utc_aware(create_user) > _TRIAL_LENGTH
    )


def _yd_export_logic_kind(
    create_user: Optional[datetime],
) -> str:
    """legacy | payments | trial_field — по дате регистрации create_user."""
    if create_user is None:
        return "legacy"
    cu = _utc_aware(create_user)
    if cu >= _utc_aware(YD_EXPORT_TRIAL_FIELD_CUTOFF):
        return "trial_field"
    if cu >= _utc_aware(YD_EXPORT_NEW_LOGIC_CUTOFF):
        return "payments"
    return "legacy"


def _payment_rub_from_row(model: type, amount) -> int:
    if model is PaymentsStars:
        stars = Decimal(str(amount))
        rate = Decimal(str(LEAD_TRACKER_STAR_RUB_PER_STAR))
        rub = (stars * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return int(rub)
    if model is PaymentsCryptobot:
        return int(Decimal(str(amount)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return int(Decimal(str(amount)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _is_trial_payment(rub: int, payload: Optional[str]) -> bool:
    if rub == _TRIAL_PAYMENT_RUB:
        return True
    parts = _parse_payment_payload_map(payload)
    return parts.get("duration") == "3trial"


@dataclass
class _YdUserPaymentFlags:
    paid_trial: bool = False
    paid_gt_trial: bool = False


def _flags_from_payment_rows(rows: List[Tuple[int, Optional[str]]]) -> _YdUserPaymentFlags:
    flags = _YdUserPaymentFlags()
    for rub, payload in rows:
        if _is_trial_payment(rub, payload):
            flags.paid_trial = True
        if rub > _TRIAL_PAYMENT_RUB:
            flags.paid_gt_trial = True
    return flags


def _successful_payments_rub_total(rows: List[Tuple[int, Optional[str]]]) -> int:
    return sum(rub for rub, _ in rows)


def _msk_date_from_create_user(create_user: datetime) -> date:
    if create_user.tzinfo is None:
        aware = create_user.replace(tzinfo=timezone.utc)
    else:
        aware = create_user.astimezone(timezone.utc)
    return aware.astimezone(MSK).date()


def _yd_sheet_title_for_day(day: date) -> str:
    return day.strftime("%d.%m.%y")


def _msk_calendar_week_start(day: date) -> date:
    """Понедельник календарной недели (пн–вс), в которую попадает day."""
    return day - timedelta(days=day.weekday())


def _msk_calendar_week_end(week_start: date) -> date:
    return week_start + timedelta(days=6)


def _yd_sheet_title_for_week(week_start: date, week_end: date) -> str:
    return f"Неделя {week_start.strftime('%d.%m')}-{week_end.strftime('%d.%m')}"


def _aggregate_yd_stamps_legacy_row(
    agg: _YdStampAgg,
    *,
    user_id: int,
    reserve_field,
    in_panel,
    sub_end,
    create_user,
    export_moment: datetime,
    paid_user_ids: frozenset[int],
    first_payment_at_by_user: Dict[int, datetime],
    pay_flags: _YdUserPaymentFlags,
) -> None:
    agg.users += 1
    sub_active = _subscription_active_at(sub_end, at=export_moment)
    if reserve_field:
        agg.with_subscription += 1
    if pay_flags.paid_gt_trial and sub_active:
        agg.with_paid_active_subscription += 1
    if _paid_after_trial(
        create_user,
        first_payment_at_by_user.get(user_id),
    ):
        agg.paid_after_trial += 1
    if in_panel:
        if user_id not in paid_user_ids:
            agg.by_promo += 1
            if sub_active:
                agg.by_promo_active += 1
    else:
        agg.no_subscription += 1


def _aggregate_yd_stamps_new_row(
    agg: _YdStampAgg,
    *,
    sub_end,
    export_moment: datetime,
    pay_flags: _YdUserPaymentFlags,
) -> None:
    agg.users += 1
    sub_active = _subscription_active_at(sub_end, at=export_moment)
    if pay_flags.paid_gt_trial:
        agg.with_subscription += 1
    if pay_flags.paid_gt_trial and sub_active:
        agg.with_paid_active_subscription += 1
    if pay_flags.paid_trial and pay_flags.paid_gt_trial:
        agg.paid_after_trial += 1
    if pay_flags.paid_trial:
        agg.by_promo += 1
        if sub_active:
            agg.by_promo_active += 1
    if not sub_active:
        agg.no_subscription += 1


def _aggregate_yd_stamps_trial_field_row(
    agg: _YdStampAgg,
    *,
    sub_end,
    export_moment: datetime,
    pay_flags: _YdUserPaymentFlags,
    user_trial: bool,
) -> None:
    agg.users += 1
    sub_active = _subscription_active_at(sub_end, at=export_moment)
    if pay_flags.paid_gt_trial:
        agg.with_subscription += 1
    if pay_flags.paid_gt_trial and sub_active:
        agg.with_paid_active_subscription += 1
    if user_trial and pay_flags.paid_gt_trial:
        agg.paid_after_trial += 1
    if user_trial:
        agg.by_promo += 1
        if sub_active and not pay_flags.paid_gt_trial:
            agg.by_promo_active += 1
    if not sub_active:
        agg.no_subscription += 1


def _aggregate_yd_stamps(
    rows,
    *,
    export_moment: datetime,
    paid_user_ids: frozenset[int],
    first_payment_at_by_user: Dict[int, datetime],
    payment_rows_by_user: Dict[int, List[Tuple[int, Optional[str]]]],
) -> Dict[str, _YdStampAgg]:
    by_stamp: Dict[str, _YdStampAgg] = defaultdict(_YdStampAgg)
    for (
        user_id,
        stamp,
        reserve_field,
        in_panel,
        sub_end,
        create_user,
        user_trial,
    ) in rows:
        key = (stamp or "").strip()
        if not key:
            continue
        agg = by_stamp[key]
        uid = int(user_id)
        pay_rows = payment_rows_by_user.get(uid, [])
        pay_flags = _flags_from_payment_rows(pay_rows)
        agg.client_payments_rub += _successful_payments_rub_total(pay_rows)
        logic = _yd_export_logic_kind(create_user)
        if logic == "trial_field":
            _aggregate_yd_stamps_trial_field_row(
                agg,
                sub_end=sub_end,
                export_moment=export_moment,
                pay_flags=pay_flags,
                user_trial=bool(user_trial),
            )
        elif logic == "payments":
            _aggregate_yd_stamps_new_row(
                agg,
                sub_end=sub_end,
                export_moment=export_moment,
                pay_flags=pay_flags,
            )
        else:
            _aggregate_yd_stamps_legacy_row(
                agg,
                user_id=uid,
                reserve_field=reserve_field,
                in_panel=in_panel,
                sub_end=sub_end,
                create_user=create_user,
                export_moment=export_moment,
                paid_user_ids=paid_user_ids,
                first_payment_at_by_user=first_payment_at_by_user,
                pay_flags=pay_flags,
            )
    return by_stamp


def _yd_spend_rub_from_usd(usd: Decimal) -> Decimal:
    return (usd * YD_SPEND_RUB_PER_USD).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _spend_cells_for_stamp(
    stamp: str,
    spend_by_stamp: Optional[Dict[str, Decimal]],
) -> List:
    if not spend_by_stamp:
        return []
    usd = spend_by_stamp.get(stamp)
    if usd is None:
        return ["", ""]
    rub = _yd_spend_rub_from_usd(usd)
    return [float(rub), float(usd)]


def _sum_spend_totals(
    by_stamp: Dict[str, _YdStampAgg],
    spend_by_stamp: Optional[Dict[str, Decimal]],
) -> Tuple[Optional[float], Optional[float]]:
    if not spend_by_stamp:
        return None, None
    total_usd = Decimal(0)
    for stamp in by_stamp:
        usd = spend_by_stamp.get(stamp)
        if usd is not None:
            total_usd += usd
    if total_usd == 0 and not any(s in spend_by_stamp for s in by_stamp):
        return "", ""
    total_rub = _yd_spend_rub_from_usd(total_usd)
    return float(total_rub), float(total_usd)


def _rows_from_by_stamp(
    by_stamp: Dict[str, _YdStampAgg],
    *,
    include_totals: bool,
    spend_by_stamp: Optional[Dict[str, Decimal]] = None,
) -> List[List]:
    headers = list(YD_EXPORT_XLSX_HEADERS)
    if spend_by_stamp is not None:
        headers.extend(YD_EXPORT_XLSX_SPEND_HEADERS)
    out: List[List] = [headers]
    totals = _YdStampAgg()
    for stamp in sorted(by_stamp.keys(), key=str.casefold):
        a = by_stamp[stamp]
        row = [
            stamp,
            a.users,
            a.with_subscription,
            a.with_paid_active_subscription,
            a.paid_after_trial,
            a.by_promo,
            a.by_promo_active,
            a.no_subscription,
            a.client_payments_rub,
        ]
        row.extend(_spend_cells_for_stamp(stamp, spend_by_stamp))
        out.append(row)
        totals.users += a.users
        totals.with_subscription += a.with_subscription
        totals.with_paid_active_subscription += a.with_paid_active_subscription
        totals.paid_after_trial += a.paid_after_trial
        totals.by_promo += a.by_promo
        totals.by_promo_active += a.by_promo_active
        totals.no_subscription += a.no_subscription
        totals.client_payments_rub += a.client_payments_rub

    if include_totals and by_stamp:
        total_row = [
            "ИТОГО",
            totals.users,
            totals.with_subscription,
            totals.with_paid_active_subscription,
            totals.paid_after_trial,
            totals.by_promo,
            totals.by_promo_active,
            totals.no_subscription,
            totals.client_payments_rub,
        ]
        rub_total, usd_total = _sum_spend_totals(by_stamp, spend_by_stamp)
        if spend_by_stamp is not None:
            total_row.extend([rub_total, usd_total])
        out.append(total_row)
    return out


async def _fetch_successful_payment_rub_by_user() -> Dict[int, List[Tuple[int, Optional[str]]]]:
    """Успешные оплаты: (сумма в руб, payload) по user_id."""
    out: Dict[int, List[Tuple[int, Optional[str]]]] = defaultdict(list)
    async with AsyncSessionLocal() as session:
        for model in _MERGE_PAYMENT_MODELS:
            stmt = select(model.user_id, model.amount, model.payload).where(
                model.status.in_(_BILLING_OK_STATUSES),
            )
            result = await session.execute(stmt)
            for user_id, amount, payload in result.all():
                if user_id is None:
                    continue
                rub = _payment_rub_from_row(model, amount)
                out[int(user_id)].append((rub, payload))
    return dict(out)


async def _fetch_first_successful_payment_at_by_user() -> Dict[int, datetime]:
    """Минимальный time_created успешной оплаты (confirmed/paid) по user_id."""
    parts = [
        select(model.user_id, model.time_created).where(
            model.status.in_(_BILLING_OK_STATUSES),
        )
        for model in _MERGE_PAYMENT_MODELS
    ]
    combined = union_all(*parts)
    first: Dict[int, datetime] = {}
    async with AsyncSessionLocal() as session:
        result = await session.execute(combined)
        for user_id, time_created in result.all():
            if time_created is None:
                continue
            uid = int(user_id)
            prev = first.get(uid)
            if prev is None or time_created < prev:
                first[uid] = time_created
    return first


async def _fetch_yd_spend_by_date() -> Dict[date, Dict[str, Decimal]]:
    """Траты по дате (календарный день) и метке."""
    out: Dict[date, Dict[str, Decimal]] = defaultdict(dict)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(YandexDirectSpend.stamp, YandexDirectSpend.spend_usd, YandexDirectSpend.spend_date)
        )
        for stamp, spend_usd, spend_date in result.all():
            if not stamp or spend_date is None or spend_usd is None:
                continue
            key = stamp.strip()
            out[spend_date][key] = Decimal(str(spend_usd))
    return dict(out)


def _yd_spend_for_day(
    spend_by_date: Dict[date, Dict[str, Decimal]],
    day: date,
) -> Dict[str, Decimal]:
    return dict(spend_by_date.get(day, {}))


def _yd_spend_for_week(
    spend_by_date: Dict[date, Dict[str, Decimal]],
    week_start: date,
    week_end: date,
) -> Dict[str, Decimal]:
    merged: Dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    d = week_start
    while d <= week_end:
        for stamp, usd in spend_by_date.get(d, {}).items():
            merged[stamp] += usd
        d += timedelta(days=1)
    return {k: v for k, v in merged.items() if v != 0}


async def _fetch_yd_stamp_user_rows(
    *,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None,
):
    async with AsyncSessionLocal() as session:
        stmt = select(
            Users.user_id,
            Users.stamp,
            Users.reserve_field,
            Users.in_panel,
            Users.subscription_end_date,
            Users.create_user,
        ).where(
            Users.yandex_id.isnot(None),
            Users.yandex_id != "",
        )
        if period_start is not None:
            stmt = stmt.where(Users.create_user >= period_start)
        if period_end is not None:
            stmt = stmt.where(Users.create_user < period_end)
        raw = (await session.execute(stmt)).all()
        return [
            (
                uid,
                stamp,
                reserve_field,
                in_panel,
                sub_end,
                create_user,
                bool(in_panel) and not bool(reserve_field),
            )
            for uid, stamp, reserve_field, in_panel, sub_end, create_user in raw
        ]


def _yd_stamp_rows_from_fetched(
    rows,
    *,
    export_moment: datetime,
    paid_user_ids: frozenset[int],
    first_payment_at_by_user: Dict[int, datetime],
    payment_rows_by_user: Dict[int, List[Tuple[int, Optional[str]]]],
    spend_by_stamp: Optional[Dict[str, Decimal]] = None,
) -> List[List]:
    by_stamp = _aggregate_yd_stamps(
        rows,
        export_moment=export_moment,
        paid_user_ids=paid_user_ids,
        first_payment_at_by_user=first_payment_at_by_user,
        payment_rows_by_user=payment_rows_by_user,
    )
    return _rows_from_by_stamp(
        by_stamp,
        include_totals=True,
        spend_by_stamp=spend_by_stamp,
    )


async def upsert_yandex_direct_spend(
    *,
    stamp: str,
    spend_usd: Decimal,
    spend_date: date,
) -> None:
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    key = stamp.strip()
    now = datetime.now()
    async with AsyncSessionLocal() as session:
        stmt = sqlite_insert(YandexDirectSpend).values(
            stamp=key,
            spend_usd=spend_usd,
            spend_date=spend_date,
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["stamp", "spend_date"],
            set_={
                "spend_usd": spend_usd,
                "updated_at": now,
            },
        )
        await session.execute(stmt)
        await session.commit()


async def collect_yd_export_sheet_rows(
    *,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None,
) -> List[List]:
    rows = await _fetch_yd_stamp_user_rows(
        period_start=period_start,
        period_end=period_end,
    )
    first_payment_at_by_user = await _fetch_first_successful_payment_at_by_user()
    payment_rows_by_user = await _fetch_successful_payment_rub_by_user()
    paid_user_ids = frozenset(first_payment_at_by_user.keys())
    export_moment = datetime.now(timezone.utc)
    return _yd_stamp_rows_from_fetched(
        rows,
        export_moment=export_moment,
        paid_user_ids=paid_user_ids,
        first_payment_at_by_user=first_payment_at_by_user,
        payment_rows_by_user=payment_rows_by_user,
    )


async def collect_yd_export_workbook_rows() -> Tuple[List[List], List[Tuple[str, List[List]]]]:
    """Лист ОБЩИЙ, листы по дням (МСК, новые → старые) и сводки по календарным неделям между ними."""
    rows = await _fetch_yd_stamp_user_rows()
    spend_by_date = await _fetch_yd_spend_by_date()
    first_payment_at_by_user = await _fetch_first_successful_payment_at_by_user()
    payment_rows_by_user = await _fetch_successful_payment_rub_by_user()
    paid_user_ids = frozenset(first_payment_at_by_user.keys())
    export_moment = datetime.now(timezone.utc)
    general = _yd_stamp_rows_from_fetched(
        rows,
        export_moment=export_moment,
        paid_user_ids=paid_user_ids,
        first_payment_at_by_user=first_payment_at_by_user,
        payment_rows_by_user=payment_rows_by_user,
        spend_by_stamp=None,
    )

    by_day: Dict[date, list] = defaultdict(list)
    for row in rows:
        create_user = row[5]
        if create_user is None:
            continue
        by_day[_msk_date_from_create_user(create_user)].append(row)

    sorted_days = sorted(by_day.keys(), reverse=True)
    daily_sheets: List[Tuple[str, List[List]]] = []
    fetch_kw = dict(
        export_moment=export_moment,
        paid_user_ids=paid_user_ids,
        first_payment_at_by_user=first_payment_at_by_user,
        payment_rows_by_user=payment_rows_by_user,
    )
    for i, day in enumerate(sorted_days):
        daily_sheets.append(
            (
                _yd_sheet_title_for_day(day),
                _yd_stamp_rows_from_fetched(
                    by_day[day],
                    **fetch_kw,
                    spend_by_stamp=_yd_spend_for_day(spend_by_date, day),
                ),
            )
        )
        if i + 1 >= len(sorted_days):
            continue
        next_day = sorted_days[i + 1]
        if _msk_calendar_week_start(day) == _msk_calendar_week_start(next_day):
            continue
        week_start = _msk_calendar_week_start(next_day)
        week_end = _msk_calendar_week_end(week_start)
        week_user_rows: list = []
        d = week_start
        while d <= week_end:
            week_user_rows.extend(by_day.get(d, []))
            d += timedelta(days=1)
        if not week_user_rows:
            continue
        daily_sheets.append(
            (
                _yd_sheet_title_for_week(week_start, week_end),
                _yd_stamp_rows_from_fetched(
                    week_user_rows,
                    **fetch_kw,
                    spend_by_stamp=_yd_spend_for_week(spend_by_date, week_start, week_end),
                ),
            )
        )

    return general, daily_sheets
