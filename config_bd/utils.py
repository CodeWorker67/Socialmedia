import time
import uuid

from sqlalchemy import select, update, delete, func, or_, and_, cast, Date, union_all
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List, Tuple, Dict, Any

from config_bd.models import AsyncSessionLocal, Users, Payments, Gifts, PaymentsCryptobot, PaymentsStars, Online, \
    WhiteCounter, PaymentsCards, PaymentsPlategaCrypto, PaymentsWataSBP, PaymentsWataCard, PaymentsFkSBP, \
    LinkingCodes, PasswordResetCodes, WlTrafficMeta, PlategaAutopaySubscription, PlategaRecurent
from lexicon import dct_price
from logging_config import logger

# Пакетная обработка для /stat: меньше 999 — лимит переменных SQLite в одном запросе.
_STAT_IN_CHUNK = 900

_BILLING_OK_STATUSES = ("confirmed", "paid")

_MERGE_PAYMENT_MODELS = (
    Payments,
    PaymentsCards,
    PaymentsPlategaCrypto,
    PaymentsWataSBP,
    PaymentsWataCard,
    PaymentsFkSBP,
    PaymentsStars,
    PaymentsCryptobot,
)

# Старые суммы без duration в payload (если совпадут с тарифом — max с dct_price).
_LEGACY_BILLING_AMOUNT_TO_DAYS: Dict[int, int] = {
    99: 30,
    269: 120,
    499: 180,
    2790: 5000,
    4990: 5000,
}


def _billing_days_for_tariff_key(key: str) -> Optional[int]:
    if "white" in key:
        return None
    if key == "7":
        return 7
    if key in ("30", "30old"):
        return 30
    if key == "90":
        return 90
    if key == "120":
        return 120
    if key == "180":
        return 180
    if key == "240":
        return 240
    if key == "365":
        return 365
    if key == "5000":
        return 5000
    if key == "5000sale":
        return 5000
    return None


def _parse_traffic_duration(raw: Optional[str]) -> Optional[int]:
    """duration:traffic10 -> 10 GB; иначе None."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s.startswith("traffic"):
        return None
    try:
        return int(s.replace("traffic", ""))
    except ValueError:
        return None


def _payment_method_label(raw: Optional[str]) -> str:
    if not raw:
        return "—"
    key = raw.strip().lower()
    labels = {
        "sbp": "Platega СБП",
        "card": "Platega карта",
        "crypto": "Platega крипто",
        "wata_sbp": "WATA СБП",
        "wata_card": "WATA карта",
        "fk_sbp": "FreeKassa СБП",
        "fk_card": "FreeKassa карта",
        "fk_qr_sbp": "FreeKassa СБП",
        "fk_qr_card": "FreeKassa карта",
        "stars": "Stars",
        "cryptobot": "CryptoBot",
        "platega_rec": "Platega СБП автоплатёж",
    }
    return labels.get(key, raw.strip())


def _norm_email(email: str) -> str:
    return email.strip().lower()


def _naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _user_tuple(user: Users) -> Tuple:
    return (
        user.id, user.user_id, user.ref, user.is_delete,
        user.in_panel, user.is_connect, user.create_user,
        user.in_chanel, user.reserve_field, user.subscription_end_date,
        user.white_subscription_end_date, user.last_notification_date,
        user.last_broadcast_status, user.last_broadcast_date,
        user.stamp, user.ttclid,
        user.subscribtion, user.white_subscription, user.email,
        user.password, user.activation_pass,
        user.field_str_1, user.field_str_2, user.field_str_3,
        user.field_bool_1, user.field_bool_2, user.field_bool_3,
        user.password_hash, user.linked_telegram_id,
        user.partner, user.partner_balance, user.partner_pay, user.partner_flag,
        user.trafic_wl, user.limit_wl,
    )


def _users_column_value_for_api(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return str(v)
    return v


def user_row_to_api_dict(user: Users) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for col in Users.__table__.columns:
        out[col.key] = _users_column_value_for_api(getattr(user, col.key))
    return out


def _sum_subscription_end_dates(
    a: Optional[datetime], b: Optional[datetime], now: datetime
) -> Optional[datetime]:
    if a is None and b is None:
        return None
    now_n = _naive_utc(now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc))

    def rem(dt: Optional[datetime]) -> timedelta:
        if dt is None:
            return timedelta(0)
        n = _naive_utc(dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt)
        return max(timedelta(0), n - now_n)

    total = rem(a) + rem(b)
    if total == timedelta(0):
        if a is None:
            return _naive_utc(b) if b is not None else None
        if b is None:
            return _naive_utc(a) if a is not None else None
        return max(_naive_utc(a), _naive_utc(b))
    return now_n + total


def _max_subscription_end_dates(
    a: Optional[datetime], b: Optional[datetime], now: datetime
) -> Optional[datetime]:
    if a is None and b is None:
        return None

    def norm(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return _naive_utc(dt)
        return _naive_utc(dt.astimezone(timezone.utc).replace(tzinfo=None))

    vals: List[datetime] = []
    if a is not None:
        vals.append(norm(a))
    if b is not None:
        vals.append(norm(b))
    return max(vals)


def _payload_white_flag(payload: Optional[str]) -> bool:
    if not payload or not str(payload).strip():
        return False
    try:
        parts = dict(item.split(":", 1) for item in str(payload).split(","))
    except ValueError:
        return False
    return parts.get("white", "False") == "True"


async def _merge_user_paid_subscription_flags(session, user_id: int) -> Tuple[bool, bool]:
    has_pro = False
    has_white = False
    for model in _MERGE_PAYMENT_MODELS:
        if has_pro and has_white:
            break
        stmt = select(model.payload).where(
            model.user_id == user_id,
            model.is_gift.is_(False),
            model.status.in_(_BILLING_OK_STATUSES),
        )
        result = await session.execute(stmt)
        for (payload,) in result.all():
            if _payload_white_flag(payload):
                has_white = True
            else:
                has_pro = True
            if has_pro and has_white:
                break
    return has_pro, has_white


def _payload_duration_to_panel_days(raw: Optional[str]) -> Optional[int]:
    """Значение duration из payload платежа → число дней для панели (30secret → 30)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if s == "30secret":
        return 30
    if s == "5000sale":
        return 5000
    try:
        v = int(s)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _white_days_from_amount_fallback(amount: Any) -> Optional[int]:
    try:
        target = int(round(float(amount)))
    except (TypeError, ValueError):
        return None
    for key, price in dct_price.items():
        if "white" not in key:
            continue
        if int(price) != target:
            continue
        if key == "white_30":
            return 30
    return None


def _billing_duration_from_amount_fallback(amount: Any) -> Optional[int]:
    try:
        target = int(round(float(amount)))
    except (TypeError, ValueError):
        return None
    if target == 1:
        return None
    candidates: list[int] = []
    for key, price in dct_price.items():
        days = _billing_days_for_tariff_key(key)
        if days is None:
            continue
        if int(price) != target:
            continue
        candidates.append(days)
    from_tariffs = max(candidates) if candidates else None
    legacy_days = _LEGACY_BILLING_AMOUNT_TO_DAYS.get(target)
    if from_tariffs is not None and legacy_days is not None:
        return max(from_tariffs, legacy_days)
    if from_tariffs is not None:
        return from_tariffs
    return legacy_days


class AsyncSQL:
    def __init__(self):
        self.session_factory = AsyncSessionLocal

    async def get_user(self, user_id: int) -> Optional[Tuple]:
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user:
                return _user_tuple(user)
            return None

    async def get_user_by_internal_id(self, internal_id: int) -> Optional[Tuple]:
        async with self.session_factory() as session:
            user = await session.get(Users, internal_id)
            if user:
                return _user_tuple(user)
            return None

    async def get_user_by_email(self, email: str) -> Optional[Tuple]:
        em = _norm_email(email)
        async with self.session_factory() as session:
            stmt = select(Users).where(func.lower(Users.email) == em)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user:
                return _user_tuple(user)
            return None

    async def get_user_object_by_internal_id(self, internal_id: int) -> Optional[Users]:
        async with self.session_factory() as session:
            return await session.get(Users, internal_id)

    async def next_negative_user_id(self) -> int:
        async with self.session_factory() as session:
            stmt = select(func.min(Users.user_id)).where(Users.user_id < 0)
            result = await session.execute(stmt)
            m = result.scalar_one_or_none()
            if m is None:
                return -10
            nxt = int(m) - 1
            while nxt < 0 and len(str(nxt)) < 3:
                nxt -= 1
            return nxt

    async def register_email_user(
        self, email: str, password_hash: str, stamp: str = "email"
    ) -> int:
        em = _norm_email(email)
        uid = await self.next_negative_user_id()
        async with self.session_factory() as session:
            u = Users(
                user_id=uid,
                email=em,
                password_hash=password_hash,
                stamp=stamp,
                create_user=datetime.now(),
            )
            session.add(u)
            await session.commit()
            await session.refresh(u)
            return int(u.id)

    async def set_user_stamp_by_internal_id(self, internal_id: int, stamp: str) -> bool:
        """Обновляет stamp только если текущее значение пустое или 'email'."""
        async with self.session_factory() as session:
            result = await session.execute(select(Users).where(Users.id == internal_id))
            user = result.scalar_one_or_none()
            if user is None:
                return False
            current = (user.stamp or "").strip()
            if current and current != "email":
                return False
            user.stamp = stamp
            await session.commit()
            return True

    async def set_password_hash_by_internal_id(self, internal_id: int, password_hash: str) -> bool:
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.id == internal_id).values(password_hash=password_hash)
            r = await session.execute(stmt)
            await session.commit()
            return (r.rowcount or 0) > 0

    async def set_activation_pass_by_email(self, email: str, value) -> bool:
        em = _norm_email(email)
        async with self.session_factory() as session:
            stmt = update(Users).where(func.lower(Users.email) == em).values(activation_pass=value)
            r = await session.execute(stmt)
            await session.commit()
            return (r.rowcount or 0) > 0

    async def set_email_verified(self, internal_id: int, verified: bool) -> bool:
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.id == internal_id).values(field_bool_1=verified)
            r = await session.execute(stmt)
            await session.commit()
            return (r.rowcount or 0) > 0

    async def replace_password_reset_codes(self, email: str, code: str, expires_at: datetime) -> None:
        em = _norm_email(email)
        exp = _naive_utc(expires_at)
        async with self.session_factory() as session:
            await session.execute(delete(PasswordResetCodes).where(PasswordResetCodes.email == em))
            session.add(PasswordResetCodes(email=em, code=code, expires_at=exp))
            await session.commit()

    async def verify_password_reset_code(self, email: str, code: str) -> bool:
        em = _norm_email(email)
        now = _naive_utc(datetime.now(timezone.utc))
        async with self.session_factory() as session:
            stmt = select(PasswordResetCodes).where(
                PasswordResetCodes.email == em,
                PasswordResetCodes.code == code,
                PasswordResetCodes.expires_at > now,
            )
            row = (await session.execute(stmt)).scalar_one_or_none()
            return row is not None

    async def delete_password_reset_codes_for_email(self, email: str) -> None:
        em = _norm_email(email)
        async with self.session_factory() as session:
            await session.execute(delete(PasswordResetCodes).where(PasswordResetCodes.email == em))
            await session.commit()

    async def replace_linking_code(self, creator_internal_id: int, code: str, expires_at: datetime) -> None:
        exp = _naive_utc(expires_at)
        now = _naive_utc(datetime.now(timezone.utc))
        async with self.session_factory() as session:
            await session.execute(delete(LinkingCodes).where(LinkingCodes.user_id == creator_internal_id))
            session.add(
                LinkingCodes(
                    code=code,
                    user_id=creator_internal_id,
                    created_at=now,
                    expires_at=exp,
                )
            )
            await session.commit()

    async def get_valid_linking_code(self, code: str) -> Optional[Tuple[int, int]]:
        now = _naive_utc(datetime.now(timezone.utc))
        async with self.session_factory() as session:
            stmt = select(LinkingCodes).where(
                LinkingCodes.code == code,
                LinkingCodes.expires_at > now,
            )
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return None
            return (int(row.code_id), int(row.user_id))

    async def delete_linking_code_by_id(self, code_id: int) -> None:
        async with self.session_factory() as session:
            await session.execute(delete(LinkingCodes).where(LinkingCodes.code_id == code_id))
            await session.commit()

    async def merge_email_placeholder_into_telegram(
        self,
        email_row_internal_id: int,
        telegram_user_id: int,
    ) -> bool:
        async with self.session_factory() as session:
            e = await session.get(Users, email_row_internal_id)
            if e is None or e.user_id >= 0:
                return False
            stmt = select(Users).where(Users.user_id == telegram_user_id)
            t = (await session.execute(stmt)).scalar_one_or_none()
            if t is None or t.user_id <= 0:
                return False

            merge_now = datetime.now(timezone.utc)
            merged_email = e.email
            merged_password_hash = e.password_hash
            e.email = None
            e.password_hash = None
            await session.flush()

            t_paid_pro, t_paid_white = await _merge_user_paid_subscription_flags(session, t.user_id)
            e_paid_pro, e_paid_white = await _merge_user_paid_subscription_flags(session, e.user_id)

            if t_paid_pro and e_paid_pro:
                t.subscription_end_date = _sum_subscription_end_dates(
                    t.subscription_end_date, e.subscription_end_date, merge_now
                )
            else:
                t.subscription_end_date = _max_subscription_end_dates(
                    t.subscription_end_date, e.subscription_end_date, merge_now
                )

            if t_paid_white and e_paid_white:
                t.white_subscription_end_date = _sum_subscription_end_dates(
                    t.white_subscription_end_date, e.white_subscription_end_date, merge_now
                )
            else:
                t.white_subscription_end_date = _max_subscription_end_dates(
                    t.white_subscription_end_date, e.white_subscription_end_date, merge_now
                )

            t.in_panel = bool(t.in_panel or e.in_panel)
            t.in_chanel = bool(t.in_chanel or e.in_chanel)
            t.is_connect = bool(t.is_connect or e.is_connect)
            t.is_delete = bool(t.is_delete or e.is_delete)
            t.reserve_field = bool(t.reserve_field or e.reserve_field)
            if not (t.ref or "") and (e.ref or ""):
                t.ref = e.ref
            if merged_email:
                t.email = merged_email
            if merged_password_hash:
                t.password_hash = merged_password_hash
            if (e.stamp or "") and (e.stamp or "") != "email":
                if not (t.stamp or "") or (t.stamp or "") == "email":
                    t.stamp = e.stamp
            if not (t.ttclid or "") and (e.ttclid or ""):
                t.ttclid = e.ttclid
            if not (t.subscribtion or "") and (e.subscribtion or ""):
                t.subscribtion = e.subscribtion
            if not (t.white_subscription or "") and (e.white_subscription or ""):
                t.white_subscription = e.white_subscription
            t.field_bool_1 = bool(t.field_bool_1 or e.field_bool_1)
            t.field_bool_2 = bool(t.field_bool_2 or e.field_bool_2)
            t.field_bool_3 = bool(t.field_bool_3 or e.field_bool_3)

            old_uid = e.user_id
            await session.delete(e)
            await session.flush()

            await session.execute(
                update(Users).where(Users.ref == str(old_uid)).values(ref=str(telegram_user_id))
            )
            await session.execute(
                update(Gifts).where(Gifts.giver_id == old_uid).values(giver_id=telegram_user_id)
            )
            await session.execute(
                update(Gifts).where(Gifts.recepient_id == old_uid).values(recepient_id=telegram_user_id)
            )
            for model in (
                Payments, PaymentsCards, PaymentsPlategaCrypto,
                PaymentsWataSBP, PaymentsWataCard, PaymentsFkSBP,
                PaymentsStars, PaymentsCryptobot, WhiteCounter,
            ):
                await session.execute(
                    update(model).where(model.user_id == old_uid).values(user_id=telegram_user_id)
                )
            await session.execute(delete(LinkingCodes).where(LinkingCodes.user_id == email_row_internal_id))
            await session.commit()

        merged_email_kept = _norm_email(merged_email) if merged_email else None
        try:
            from bot import x3
            from X3 import panel_username_for_site_email, panel_username_for_site_user

            if merged_email_kept:
                await x3.delete_panel_user_by_username(panel_username_for_site_email(merged_email_kept, False))
                await x3.delete_panel_user_by_username(panel_username_for_site_email(merged_email_kept, True))
            await x3.delete_panel_user_by_username(panel_username_for_site_user(old_uid, False))
            await x3.delete_panel_user_by_username(panel_username_for_site_user(old_uid, True))

            row = await self.get_user(telegram_user_id)
            if row:
                def _aware_utc(dt: datetime) -> datetime:
                    if dt.tzinfo is None:
                        return dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)

                sub_end = row[9]
                w_end = row[10]
                tid = int(row[1])
                if sub_end:
                    await x3.set_expiration_date(str(tid), _aware_utc(sub_end), tid)
                if w_end:
                    await x3.set_expiration_date(str(tid) + "_white", _aware_utc(w_end), tid)
        except Exception as ex:
            logger.warning("post-merge panel cleanup/sync: {}", ex)

        return True

    async def get_user_object_by_user_id(self, user_id: int) -> Optional[Users]:
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def add_user(self, user_id: int, in_panel: bool, is_connect: bool = False,
                     ref: str = '', is_delete: bool = False, in_chanel: bool = False,
                     stamp='', partner: str = '') -> bool:
        """Возвращает True, если пользователь был вставлен; False если уже существовал (гонки /start)."""
        async with self.session_factory() as session:
            stmt = sqlite_insert(Users).values(
                user_id=user_id,
                ref=ref or None,
                partner=partner or None,
                is_delete=is_delete,
                in_panel=in_panel,
                is_connect=is_connect,
                in_chanel=in_chanel,
                stamp=stamp,
            ).on_conflict_do_nothing(index_elements=[Users.user_id])
            try:
                result = await session.execute(stmt)
                await session.commit()
                return (result.rowcount or 0) > 0
            except Exception as e:
                await session.rollback()
                logger.error(f"Error inserting user {user_id}: {e}")
                return False

    async def update_in_panel(self, user_id: int):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(in_panel=True)
            await session.execute(stmt)
            await session.commit()

    async def update_in_chanel(self, user_id: int, booly: bool):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(in_chanel=booly)
            await session.execute(stmt)
            await session.commit()

    async def update_is_connect(self, user_id: int, booly: bool):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(is_connect=booly)
            await session.execute(stmt)
            await session.commit()

    async def update_ttclid(self, user_id: int, ttclid: str):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(ttclid=ttclid)
            await session.execute(stmt)
            await session.commit()

    async def update_reserve_field(self, user_id: int):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(reserve_field=True)
            await session.execute(stmt)
            await session.commit()

    async def init_wl_trial_limits(self, user_id: int) -> None:
        """При первом триале (limit_wl=0): trafic_wl=0, limit_wl=2 GB. Иначе не трогаем."""
        from wl_traffic.constants import WL_TRIAL_LIMIT_GB

        async with self.session_factory() as session:
            stmt = (
                update(Users)
                .where(
                    Users.user_id == user_id,
                    or_(Users.limit_wl.is_(None), Users.limit_wl <= 0),
                )
                .values(trafic_wl=0.0, limit_wl=WL_TRIAL_LIMIT_GB)
            )
            await session.execute(stmt)
            await session.commit()

    async def add_wl_limit(self, user_id: int, gb: float) -> None:
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user is None:
                return
            current = float(user.limit_wl or 0.0)
            user.limit_wl = round(current + gb, 2)
            if gb > 0:
                user.field_bool_2 = False
            await session.commit()

    async def update_trafic_wl(self, user_id: int, gb: float) -> None:
        async with self.session_factory() as session:
            stmt = (
                update(Users)
                .where(Users.user_id == user_id)
                .values(trafic_wl=round(gb, 2))
            )
            await session.execute(stmt)
            await session.commit()

    async def add_trafic_wl(self, user_id: int, gb: float) -> None:
        """Прибавляет GB к накопленному trafic_wl (ежедневное накопление с панели)."""
        if gb <= 0:
            return
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user is None:
                return
            current = float(user.trafic_wl or 0.0)
            user.trafic_wl = round(current + gb, 2)
            await session.commit()

    async def get_wl_traffic_last_closed_date(self) -> Optional[date]:
        """Последний WL-день, закрытый accumulate (глобально для бота)."""
        async with self.session_factory() as session:
            stmt = select(WlTrafficMeta.last_closed_date).where(WlTrafficMeta.id == 1)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def set_wl_traffic_last_closed_date(self, day: date) -> None:
        """Помечает WL-день как закрытый (идемпотентность accumulate)."""
        async with self.session_factory() as session:
            stmt = select(WlTrafficMeta).where(WlTrafficMeta.id == 1)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if row is None:
                row = WlTrafficMeta(id=1, last_closed_date=day)
                session.add(row)
            else:
                row.last_closed_date = day
            await session.commit()

    async def get_wl_limits(self, user_id: int) -> tuple[float, float]:
        """Возвращает (trafic_wl, limit_wl) в GB."""
        async with self.session_factory() as session:
            stmt = select(Users.trafic_wl, Users.limit_wl).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            row = result.one_or_none()
            if row is None:
                return 0.0, 0.0
            return float(row[0] or 0.0), float(row[1] or 0.0)

    async def select_users_active_subscription(self) -> List[Tuple[int, float, float, bool]]:
        """Пользователи с активной PRO-подпиской: (user_id, trafic_wl, limit_wl, field_bool_2)."""
        async with self.session_factory() as session:
            now = datetime.now()
            stmt = select(
                Users.user_id,
                Users.trafic_wl,
                Users.limit_wl,
                Users.field_bool_2,
            ).where(
                Users.is_delete == False,
                Users.in_panel == True,
                Users.subscription_end_date.isnot(None),
                Users.subscription_end_date > now,
            )
            result = await session.execute(stmt)
            return [
                (int(r[0]), float(r[1] or 0.0), float(r[2] or 0.0), bool(r[3]))
                for r in result.all()
            ]

    async def add_wl_limit_subscribers_from_today(self, gb: float) -> list[int]:
        """
        +gb к limit_wl всем, у кого подписка до конца сегодня или позже (МСК).
        field_bool_2 сбрасывается через add_wl_limit.
        Возвращает список user_id.
        """
        from wl_traffic.constants import WL_TIMEZONE

        if gb <= 0:
            return []
        today_start = (
            datetime.now(WL_TIMEZONE)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .replace(tzinfo=None)
        )
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(
                Users.subscription_end_date.isnot(None),
                Users.subscription_end_date >= today_start,
            )
            result = await session.execute(stmt)
            user_ids = [int(r[0]) for r in result.all()]

        for user_id in user_ids:
            await self.add_wl_limit(user_id, gb)
        return user_ids

    async def update_delete(self, user_id: int, booly: bool):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(is_delete=booly)
            await session.execute(stmt)
            await session.commit()

    async def select_ref_count(self, user_id: int) -> int:
        async with self.session_factory() as session:
            stmt = select(func.count(Users.user_id)).where(Users.ref == str(user_id))
            result = await session.execute(stmt)
            return result.scalar() or 0

    async def select_ref_paid_count(self, user_id: int) -> int:
        async with self.session_factory() as session:
            stmt = select(func.count(Users.user_id)).where(
                Users.ref == str(user_id),
                Users.reserve_field == True,
            )
            result = await session.execute(stmt)
            return result.scalar() or 0

    async def select_partner_count(self, partner_id: int) -> int:
        async with self.session_factory() as session:
            stmt = select(func.count(Users.user_id)).where(Users.partner == str(partner_id))
            result = await session.execute(stmt)
            return result.scalar() or 0

    async def select_partner_referrals_payments_sum(self, partner_id: int) -> int:
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(Users.partner == str(partner_id))
            result = await session.execute(stmt)
            user_ids = [row[0] for row in result.all()]
            if not user_ids:
                return 0

            total = 0
            for i in range(0, len(user_ids), _STAT_IN_CHUNK):
                chunk = user_ids[i : i + _STAT_IN_CHUNK]
                for model in _MERGE_PAYMENT_MODELS:
                    stmt_sum = select(func.coalesce(func.sum(model.amount), 0)).where(
                        model.user_id.in_(chunk),
                        model.status.in_(_BILLING_OK_STATUSES),
                    )
                    val = (await session.execute(stmt_sum)).scalar() or 0
                    total += int(val)
            return total

    async def update_partner_flag(self, user_id: int, flag: bool = True) -> None:
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(partner_flag=flag)
            await session.execute(stmt)
            await session.commit()

    async def add_partner_balance(self, partner_user_id: int, amount: int) -> bool:
        if amount <= 0:
            return False
        async with self.session_factory() as session:
            stmt = (
                update(Users)
                .where(Users.user_id == partner_user_id)
                .values(partner_balance=func.coalesce(Users.partner_balance, 0) + amount)
            )
            result = await session.execute(stmt)
            await session.commit()
            return (result.rowcount or 0) > 0

    async def partner_record_payout(self, partner_user_id: int, amount: int) -> Tuple[bool, str]:
        if amount <= 0:
            return False, "Сумма должна быть больше 0"
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == partner_user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user is None:
                return False, "Пользователь не найден"
            balance = user.partner_balance or 0
            if balance < amount:
                return False, f"Недостаточно на балансе: {balance} ₽, запрошено {amount} ₽"
            user.partner_balance = balance - amount
            user.partner_pay = (user.partner_pay or 0) + amount
            await session.commit()
            return True, ""

    async def update_subscription_end_date(self, user_id: int, end_date: datetime):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(subscription_end_date=end_date)
            await session.execute(stmt)
            await session.commit()

    async def update_white_subscription_end_date(self, user_id: int, end_date: datetime):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(white_subscription_end_date=end_date)
            await session.execute(stmt)
            await session.commit()

    async def update_subscribtion(self, user_id: int, subscribtion: Optional[str]):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(subscribtion=subscribtion)
            await session.execute(stmt)
            await session.commit()

    async def update_white_subscription(self, user_id: int, white_subscription: Optional[str]):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(white_subscription=white_subscription)
            await session.execute(stmt)
            await session.commit()

    async def get_subscription_end_date(self, user_id: int) -> Optional[datetime]:
        async with self.session_factory() as session:
            stmt = select(Users.subscription_end_date).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def notification_sent_today(self, user_id: int) -> bool:
        async with self.session_factory() as session:
            stmt = select(Users.last_notification_date).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            last = result.scalar_one_or_none()
            today = date.today()
            if last:
                if isinstance(last, datetime):
                    last = last.date()
                return last == today
            return False

    async def mark_notification_as_sent(self, user_id: int):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(last_notification_date=date.today())
            await session.execute(stmt)
            await session.commit()

    async def update_field_str_1(self, user_id: int, value: Optional[str]):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(field_str_1=value)
            await session.execute(stmt)
            await session.commit()

    async def update_field_bool_2(self, user_id: int, value: bool):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(field_bool_2=value)
            await session.execute(stmt)
            await session.commit()

    async def update_field_bool_3(self, user_id: int, value: bool):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(field_bool_3=value)
            await session.execute(stmt)
            await session.commit()

    async def reset_field_bool_2_all(self) -> int:
        """Всем строкам users: field_bool_2 = False. Возвращает число обновлённых записей."""
        async with self.session_factory() as session:
            result = await session.execute(update(Users).values(field_bool_2=False))
            await session.commit()
            return int(result.rowcount or 0)

    async def reset_field_bool_3_all(self) -> int:
        """Всем строкам users: field_bool_3 = False. Возвращает число обновлённых записей."""
        async with self.session_factory() as session:
            result = await session.execute(update(Users).values(field_bool_3=False))
            await session.commit()
            return int(result.rowcount or 0)

    async def get_last_notification_date(self, user_id: int) -> Optional[date]:
        async with self.session_factory() as session:
            stmt = select(Users.last_notification_date).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            val = result.scalar_one_or_none()
            if isinstance(val, datetime):
                return val.date()
            return val

    async def update_broadcast_status(self, user_id: int, status: str):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(
                last_broadcast_status=status,
                last_broadcast_date=datetime.now()
            )
            await session.execute(stmt)
            await session.commit()

    async def select_all_users(self) -> List[int]:
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(Users.is_delete == False)
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_USER_IDS_PANEL_EXPIRED_REGULAR_SUBSCRIPTION(self) -> List[int]:
        """В панели, не удалены, обычная подписка по календарю UTC истекла или даты нет."""
        today_utc = datetime.now(timezone.utc).date()
        expired = or_(
            Users.subscription_end_date.is_(None),
            cast(Users.subscription_end_date, Date) < today_utc,
        )
        async with self.session_factory() as session:
            stmt = (
                select(Users.user_id)
                .where(
                    Users.is_delete == False,
                    Users.in_panel == True,
                    expired,
                )
                .order_by(Users.user_id)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_USER_IDS_NO_ACTIVE_PRO_SUBSCRIPTION(self) -> List[int]:
        """
        Не удалены; для /add_7_to_all: subscription_end_date пусто
        или календарный день окончания (UTC) не позже чем 2 дня назад (UTC).
        """
        today_utc = datetime.now(timezone.utc).date()
        expired_at_least_2_days_ago = today_utc - timedelta(days=2)
        eligible = or_(
            Users.subscription_end_date.is_(None),
            cast(Users.subscription_end_date, Date) <= expired_at_least_2_days_ago,
        )
        async with self.session_factory() as session:
            stmt = (
                select(Users.user_id)
                .where(
                    Users.is_delete == False,
                    eligible,
                )
                .order_by(Users.user_id)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def select_rows_for_subscription_expiry_push(
        self, now_utc_naive: datetime, window: timedelta
    ) -> List[Tuple[int, datetime, bool, Optional[str], Optional[str]]]:
        """
        Строки для sheduler.time_mes без N× get_user: user_id, subscription_end_date,
        in_panel (оплачивал / клава тарифа), ttclid, field_str_1 (JSON состояния push).

        Фильтр по времени (как _in_send_window в Python, moment <= now < moment + window):
        — Подписка активна: end попадает в одно из окон «за 7 / 3 / 1 день» или «за 1 час».
        — Подписка истекла: end попадает в окно second_chance (+7 дн) или post-expiry p1..p200 (+3n дн).
        """
        w = window
        now = now_utc_naive

        active_7 = and_(
            Users.subscription_end_date > now,
            Users.subscription_end_date > now + timedelta(days=7) - w,
            Users.subscription_end_date <= now + timedelta(days=7),
        )
        active_3 = and_(
            Users.subscription_end_date > now,
            Users.subscription_end_date > now + timedelta(days=3) - w,
            Users.subscription_end_date <= now + timedelta(days=3),
        )
        active_1 = and_(
            Users.subscription_end_date > now,
            Users.subscription_end_date > now + timedelta(days=1) - w,
            Users.subscription_end_date <= now + timedelta(days=1),
        )
        active_h = and_(
            Users.subscription_end_date > now,
            Users.subscription_end_date > now + timedelta(hours=1) - w,
            Users.subscription_end_date <= now + timedelta(hours=1),
        )
        active_cond = or_(active_7, active_3, active_1, active_h)

        post_second = and_(
            Users.subscription_end_date <= now,
            Users.subscription_end_date > now - timedelta(days=7) - w,
            Users.subscription_end_date <= now - timedelta(days=7),
        )
        post_pn = []
        for n in range(1, 201):
            d = timedelta(days=3 * n)
            post_pn.append(
                and_(
                    Users.subscription_end_date <= now,
                    Users.subscription_end_date > now - d - w,
                    Users.subscription_end_date <= now - d,
                )
            )
        expired_cond = or_(post_second, *post_pn)

        async with self.session_factory() as session:
            stmt = (
                select(
                    Users.user_id,
                    Users.subscription_end_date,
                    Users.in_panel,
                    Users.ttclid,
                    Users.field_str_1,
                )
                .where(
                    Users.is_delete == False,
                    Users.subscription_end_date.isnot(None),
                    or_(active_cond, expired_cond),
                )
                .order_by(Users.user_id)
            )
            result = await session.execute(stmt)
            rows = result.all()
            return [
                (r[0], r[1], bool(r[2]), r[3], r[4])
                for r in rows
            ]

    async def select_not_connected_subscribe_yes(self) -> List[int]:
        async with self.session_factory() as session:
            current_time = datetime.now()
            today = date.today()
            stmt = select(Users.user_id).where(
                Users.in_panel == True,
                Users.is_connect == False,
                Users.is_delete == False,
                Users.subscription_end_date > current_time
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def select_not_connected_subscribe_off(self):
        async with self.session_factory() as session:
            current_time = datetime.now()
            stmt = select(Users.user_id).where(
                Users.in_panel == True,
                Users.is_connect == False,
                Users.is_delete == False,
                (Users.subscription_end_date < current_time) |
                (Users.subscription_end_date.is_(None))
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def select_connected_subscribe_off(self):
        async with self.session_factory() as session:
            current_time = datetime.now()
            stmt = select(Users.user_id).where(
                Users.in_panel == True,
                Users.is_connect == True,
                Users.is_delete == False,
                (Users.subscription_end_date < current_time) |
                (Users.subscription_end_date.is_(None))
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def select_connected_subscribe_yes(self):
        async with self.session_factory() as session:
            current_time = datetime.now()
            stmt = select(Users.user_id).where(
                Users.in_panel == True,
                Users.is_connect == True,
                Users.is_delete == False,
                Users.subscription_end_date > current_time
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def select_subscribe_off(self):
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(
                Users.in_panel == False,
                Users.is_connect == False,
                Users.is_delete == False
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]


    async def select_subscribe_yes(self):
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(
                Users.in_panel == True,
                Users.is_delete == False
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]


    async def select_connected_never_paid(self) -> List[int]:
        """
        Возвращает список user_id, у которых is_tarif=True, is_delete=False,
        и нет ни одной успешной оплаты (статус 'confirmed' в Payments или PaymentsStars,
        или статус 'paid' в PaymentsCryptobot).
        """
        async with self.session_factory() as session:
            # Подзапрос: все пользователи с успешными платежами
            today = datetime.now().date()
            paid_subq = (
                select(Payments.user_id)
                .where(Payments.status == 'confirmed')
                .union(
                    select(PaymentsStars.user_id).where(PaymentsStars.status == 'confirmed'),
                    select(PaymentsCryptobot.user_id).where(PaymentsCryptobot.status == 'paid'),
                    select(PaymentsCards.user_id).where(PaymentsCards.status == 'confirmed'),
                    select(PaymentsPlategaCrypto.user_id).where(PaymentsPlategaCrypto.status == 'confirmed'),
                    select(PaymentsWataSBP.user_id).where(PaymentsWataSBP.status == 'confirmed'),
                    select(PaymentsWataCard.user_id).where(PaymentsWataCard.status == 'confirmed'),
                    select(PaymentsFkSBP.user_id).where(PaymentsFkSBP.status == 'confirmed'),
                )
                .subquery()
            )
            stmt = select(Users.user_id).where(
                Users.is_connect == True,
                Users.is_delete == False,
                Users.user_id.notin_(paid_subq)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    _FOREVER_PAYMENT_AMOUNTS = (4990, 2790)

    @staticmethod
    def _forever_payment_cond(table):
        """Платёж за тариф «Навсегда» (duration:5000 / 5000sale или типичные суммы)."""
        return or_(
            table.payload.like("%duration:5000%"),
            table.amount.in_(AsyncSQL._FOREVER_PAYMENT_AMOUNTS),
        )

    def _forever_payers_subquery(self):
        """user_id, хотя бы раз оплативших тариф «Навсегда»."""
        fc = self._forever_payment_cond
        return (
            select(Payments.user_id)
            .where(Payments.status == "confirmed", fc(Payments))
            .union(
                select(PaymentsStars.user_id).where(
                    PaymentsStars.status == "confirmed", fc(PaymentsStars)
                ),
                select(PaymentsCryptobot.user_id).where(
                    PaymentsCryptobot.status == "paid", fc(PaymentsCryptobot)
                ),
                select(PaymentsCards.user_id).where(
                    PaymentsCards.status == "confirmed", fc(PaymentsCards)
                ),
                select(PaymentsPlategaCrypto.user_id).where(
                    PaymentsPlategaCrypto.status == "confirmed", fc(PaymentsPlategaCrypto)
                ),
                select(PaymentsWataSBP.user_id).where(
                    PaymentsWataSBP.status == "confirmed", fc(PaymentsWataSBP)
                ),
                select(PaymentsWataCard.user_id).where(
                    PaymentsWataCard.status == "confirmed", fc(PaymentsWataCard)
                ),
                select(PaymentsFkSBP.user_id).where(
                    PaymentsFkSBP.status == "confirmed", fc(PaymentsFkSBP)
                ),
            )
            .subquery()
        )

    async def select_forever_active_users(self) -> List[int]:
        """Активные пользователи тарифа Навсегда (end_date >= 2030-01-01 и ещё не истекла)."""
        from wl_traffic.constants import FOREVER_END_CUTOFF

        async with self.session_factory() as session:
            now = datetime.now()
            stmt = select(Users.user_id).where(
                Users.is_delete == False,
                Users.in_panel == True,
                Users.subscription_end_date.isnot(None),
                Users.subscription_end_date > now,
                Users.subscription_end_date >= FOREVER_END_CUTOFF,
            )
            result = await session.execute(stmt)
            return [int(r[0]) for r in result.all()]

    _SHORT_SUBSCRIPTION_AMOUNTS = (99, 149, 249, 299)

    @staticmethod
    def _subscription_payment_cond(table):
        """Успешная оплата своей подписки: не подарок и не докупка трафика Антиглушилка."""
        return and_(
            table.status.in_(_BILLING_OK_STATUSES),
            or_(table.is_gift.is_(False), table.is_gift.is_(None)),
            or_(
                table.payload.is_(None),
                ~table.payload.like("%duration:traffic%"),
            ),
        )

    @staticmethod
    def _short_subscription_duration_cond(table):
        """Платёж за подписку на 7 или 30 дней (duration:7 / 30 / 30secret / 30old, иначе сумма)."""
        payload_short = or_(
            table.payload.like("%duration:7,%"),
            table.payload.like("%duration:7"),
            table.payload.like("%duration:30,%"),
            table.payload.like("%duration:30"),
            table.payload.like("%duration:30secret%"),
            table.payload.like("%duration:30old%"),
        )
        amount_short = and_(
            or_(
                table.payload.is_(None),
                ~table.payload.like("%duration:%"),
            ),
            table.amount.in_(AsyncSQL._SHORT_SUBSCRIPTION_AMOUNTS),
        )
        return or_(payload_short, amount_short)

    def _users_with_multiple_subscription_pays_subquery(self):
        """user_id с двумя и более успешными оплатами подписки (все платёжные таблицы)."""
        cond = self._subscription_payment_cond
        parts = [
            select(model.user_id).where(cond(model))
            for model in _MERGE_PAYMENT_MODELS
        ]
        rows = union_all(*parts).subquery()
        return (
            select(rows.c.user_id)
            .group_by(rows.c.user_id)
            .having(func.count() > 1)
            .subquery()
        )

    def _users_with_non_short_subscription_pays_subquery(self):
        """user_id с хотя бы одной успешной оплатой подписки не на 7 или 30 дней."""
        cond = self._subscription_payment_cond
        short = self._short_subscription_duration_cond
        parts = [
            select(model.user_id).where(cond(model), ~short(model))
            for model in _MERGE_PAYMENT_MODELS
        ]
        return union_all(*parts).subquery()

    async def user_has_promo_120_payment(self, user_id: int) -> bool:
        """Была ли успешная оплата акции 3+1 (duration:120 в payload)."""
        promo_cond = lambda table: and_(
            self._subscription_payment_cond(table),
            or_(
                table.payload.like("%duration:120,%"),
                table.payload.like("%duration:120"),
            ),
        )
        async with self.session_factory() as session:
            for model in _MERGE_PAYMENT_MODELS:
                stmt = select(func.count()).select_from(model).where(
                    model.user_id == user_id,
                    promo_cond(model),
                )
                if int((await session.execute(stmt)).scalar_one()) > 0:
                    return True
        return False

    def _build_broadcast_where(self, category: str, exclude_today: bool):
        """
        Условие выборки пользователей для рассылки.
        exclude_today: только те, у кого last_broadcast_date пусто или дата (UTC) не сегодня.
        """
        current_time = datetime.now()
        today_d = datetime.now(timezone.utc).date()
        skip_today_cond = or_(
            Users.last_broadcast_date.is_(None),
            func.date(Users.last_broadcast_date) != today_d,
        )

        def wrap(base):
            return and_(base, skip_today_cond) if exclude_today else base

        if category == "all_users":
            return wrap(Users.is_delete == False)
        if category == "not_connected_subscribe_yes":
            return wrap(
                and_(
                    Users.in_panel == True,
                    Users.is_connect == False,
                    Users.is_delete == False,
                    Users.subscription_end_date > current_time,
                )
            )
        if category == "not_connected_subscribe_off":
            return wrap(
                and_(
                    Users.in_panel == True,
                    Users.is_connect == False,
                    Users.is_delete == False,
                    or_(
                        Users.subscription_end_date < current_time,
                        Users.subscription_end_date.is_(None),
                    ),
                )
            )
        if category == "connected_subscribe_off":
            return wrap(
                and_(
                    Users.in_panel == True,
                    Users.is_connect == True,
                    Users.is_delete == False,
                    or_(
                        Users.subscription_end_date < current_time,
                        Users.subscription_end_date.is_(None),
                    ),
                )
            )
        if category == "connected_subscribe_yes":
            return wrap(
                and_(
                    Users.in_panel == True,
                    Users.is_connect == True,
                    Users.is_delete == False,
                    Users.subscription_end_date > current_time,
                )
            )
        if category == "not_subscribed":
            return wrap(
                and_(
                    Users.in_panel == False,
                    Users.is_connect == False,
                    Users.is_delete == False,
                )
            )
        if category == "connected_never_paid":
            paid_subq = (
                select(Payments.user_id)
                .where(Payments.status == "confirmed")
                .union(
                    select(PaymentsStars.user_id).where(PaymentsStars.status == "confirmed"),
                    select(PaymentsCryptobot.user_id).where(PaymentsCryptobot.status == "paid"),
                    select(PaymentsCards.user_id).where(PaymentsCards.status == "confirmed"),
                    select(PaymentsPlategaCrypto.user_id).where(PaymentsPlategaCrypto.status == "confirmed"),
                    select(PaymentsWataSBP.user_id).where(PaymentsWataSBP.status == "confirmed"),
                    select(PaymentsWataCard.user_id).where(PaymentsWataCard.status == "confirmed"),
                    select(PaymentsFkSBP.user_id).where(PaymentsFkSBP.status == "confirmed"),
                )
                .subquery()
            )
            return wrap(
                and_(
                    Users.is_connect == True,
                    Users.is_delete == False,
                    Users.user_id.notin_(paid_subq),
                )
            )
        if category == "subscribed_all":
            return wrap(
                and_(
                    Users.in_panel == True,
                    Users.subscription_end_date != None,
                    Users.is_delete == False,
                )
            )
        if category == "never_bought_forever":
            from wl_traffic.constants import FOREVER_END_CUTOFF

            forever_paid = self._forever_payers_subquery()
            return wrap(
                and_(
                    Users.is_delete == False,
                    Users.user_id.notin_(forever_paid),
                    or_(
                        Users.subscription_end_date.is_(None),
                        Users.subscription_end_date < FOREVER_END_CUTOFF,
                    ),
                )
            )
        if category == "paid_at_most_once":
            multi_paid = self._users_with_multiple_subscription_pays_subquery()
            non_short_paid = self._users_with_non_short_subscription_pays_subquery()
            return wrap(
                and_(
                    Users.is_delete == False,
                    Users.user_id.notin_(multi_paid),
                    Users.user_id.notin_(non_short_paid),
                )
            )
        return None

    async def count_users_for_broadcast(self, category: str, exclude_today: bool) -> int:
        where_clause = self._build_broadcast_where(category, exclude_today)
        if where_clause is None:
            return 0
        async with self.session_factory() as session:
            stmt = select(func.count()).select_from(Users).where(where_clause)
            return int((await session.execute(stmt)).scalar_one())

    async def select_user_ids_for_broadcast(self, category: str, exclude_today: bool) -> List[int]:
        where_clause = self._build_broadcast_where(category, exclude_today)
        if where_clause is None:
            return []
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(where_clause)
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def select_subscribed_not_in_chanel(self):
        async with self.session_factory() as session:
            # Подзапрос: все пользователи с успешными платежами
            stmt = select(Users.user_id).where(
                Users.in_panel == True,
                Users.subscription_end_date == None,
                Users.is_delete == False
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def select_user_by_parameter(self, parameter: str, value: str) -> List[int]:
        """
        Возвращает список user_id, у которых значение указанного параметра равно value.
        Допустимые параметры: 'Ref', 'Is_pay_null', 'stamp'.
        """
        # Маппинг имён параметров на атрибуты модели
        param_map = {
            'ref': Users.ref,
            'in_panel': Users.in_panel,
            'stamp': Users.stamp,
        }
        if parameter not in param_map:
            logger.info(f"Invalid parameter: {parameter}")
            return []

        attr = param_map[parameter]

        # Преобразование значения для булевых полей
        if parameter == 'in_panel':
            try:
                val = bool(int(value))
            except ValueError:
                logger.error(f"Invalid value type for parameter {parameter}: {value}")
                return []
        else:
            val = value

        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(attr == val)
            result = await session.execute(stmt)
            rows = result.all()
            logger.info(f"Query result for parameter '{parameter}' with value '{value}': {len(rows)}")
            return [row[0] for row in rows]

    async def get_stat_by_ref_or_stamp(self, arg: str) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int], Optional[int], Optional[str]]:
        """
        Возвращает статистику по пользователям, у которых Ref == arg,
        если таких нет – по пользователям с stamp == arg.
        Возвращает (total, with_sub, with_tarif, with_tarif_not_blocked, total_payments, source)
        или (None, ...) если нет совпадений.
        """
        # 1. Ищем по Ref
        users = await self.select_user_by_parameter('ref', arg)
        source = 'ref'
        if not users:
            # 2. Ищем по stamp
            users = await self.select_user_by_parameter('stamp', arg)
            source = 'stamp'

        if not users:
            return None, None, None, None, None, None

        total = len(users)
        with_sub = 0
        with_tarif = 0
        with_tarif_not_blocked = 0
        total_payments = 0

        async with self.session_factory() as session:
            for i in range(0, len(users), _STAT_IN_CHUNK):
                chunk = users[i : i + _STAT_IN_CHUNK]
                stmt_users = select(
                    Users.subscription_end_date,
                    Users.is_connect,
                    Users.is_delete,
                ).where(Users.user_id.in_(chunk))
                result = await session.execute(stmt_users)
                for sub_end, is_connect, is_delete in result.all():
                    if sub_end is not None:
                        with_sub += 1
                    if is_connect:
                        with_tarif += 1
                    if is_connect and not is_delete:
                        with_tarif_not_blocked += 1

            with_tarif //= 2
            with_tarif_not_blocked //= 2

            for i in range(0, len(users), _STAT_IN_CHUNK):
                chunk = users[i : i + _STAT_IN_CHUNK]
                stmt_pay = select(func.coalesce(func.sum(Payments.amount), 0)).where(
                    Payments.user_id.in_(chunk),
                    Payments.status == 'confirmed',
                )
                total_payments += (await session.execute(stmt_pay)).scalar() or 0

                stmt_fk = select(func.coalesce(func.sum(PaymentsFkSBP.amount), 0)).where(
                    PaymentsFkSBP.user_id.in_(chunk),
                    PaymentsFkSBP.status == 'confirmed',
                )
                total_payments += (await session.execute(stmt_fk)).scalar() or 0

                stmt_wata_sbp = select(func.coalesce(func.sum(PaymentsWataSBP.amount), 0)).where(
                    PaymentsWataSBP.user_id.in_(chunk),
                    PaymentsWataSBP.status == 'confirmed',
                )
                total_payments += (await session.execute(stmt_wata_sbp)).scalar() or 0

                stmt_wata_card = select(func.coalesce(func.sum(PaymentsWataCard.amount), 0)).where(
                    PaymentsWataCard.user_id.in_(chunk),
                    PaymentsWataCard.status == 'confirmed',
                )
                total_payments += (await session.execute(stmt_wata_card)).scalar() or 0

                stmt_stars = select(PaymentsStars.amount).where(
                    PaymentsStars.user_id.in_(chunk),
                    PaymentsStars.status == "confirmed",
                )
                for (amt,) in (await session.execute(stmt_stars)).all():
                    total_payments += amt

                stmt_cryptobot = select(func.coalesce(func.sum(PaymentsCryptobot.amount), 0)).where(
                    PaymentsCryptobot.user_id.in_(chunk),
                    PaymentsCryptobot.status == "paid",
                    PaymentsCryptobot.amount > 0.02,
                )
                cb_sum = (await session.execute(stmt_cryptobot)).scalar() or 0
                total_payments += int(round(float(cb_sum)))

        return total, with_sub, with_tarif, with_tarif_not_blocked, total_payments, source

    def get_parameters(self) -> List[str]:
        """Возвращает список доступных параметров для фильтрации пользователей."""
        return [
            'not_connected_subscribe_yes',
            'not_connected_subscribe_off',
            'connected_subscribe_off',
            'connected_subscribe_yes',
            'not_subscribed',
            'subscribed',
            'connected_never_paid',
            'connected_never_paid',
            'all_users'
        ]

    async def delete_from_db(self, user_id: int) -> bool:
        """Полностью удаляет пользователя из БД по User_id."""
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if not user:
                logger.warning(f"User {user_id} not found for deletion")
                return False
            await session.delete(user)
            await session.commit()
            logger.info(f"✅ Удалено пользователей: 1 (User_id: {user_id})")
            return True

    async def reset_all_delete_flag(self) -> int:
        """Устанавливает Is_delete = False для всех записей в таблице users."""
        async with self.session_factory() as session:
            stmt = update(Users).values(is_delete=False)
            result = await session.execute(stmt)
            await session.commit()
            updated = result.rowcount
            logger.info(f"✅ Сброшен флаг Is_delete для {updated} пользователей")
            return updated

    async def get_users_with_confirmed_payments(self, user_ids: Optional[List[int]] = None) -> List[int]:
        """
        Возвращает список user_id, у которых есть хотя бы один платёж со статусом 'confirmed'.
        Если передан список user_ids, возвращаются только те, кто есть в этом списке.
        """
        async with self.session_factory() as session:
            stmt = select(Payments.user_id).where(Payments.status == 'confirmed').distinct()
            if user_ids:
                stmt = stmt.where(Payments.user_id.in_(user_ids))
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def get_payment_stats_by_period(self, start_date: datetime, end_date: datetime) -> Tuple[Dict[str, int], Dict[str, int]]:
        """
        Возвращает статистику платежей за период по группам ref и stamp.
        Для каждого платежа с суммой != 1, статус 'confirmed', дата между start_date и end_date включительно,
        находим пользователя и добавляем сумму в группы ref и stamp (если они заданы).
        Возвращает два словаря: ref_totals, stamp_totals.
        """
        # Приводим даты к началу и концу суток для включительности
        start = datetime.combine(start_date.date(), datetime.min.time())
        end = datetime.combine(end_date.date(), datetime.max.time())

        async with self.session_factory() as session:
            # Получаем платежи за период, исключая сумму 1
            stmt_payments = select(
                Payments.user_id,
                Payments.amount
            ).where(
                Payments.status == 'confirmed',
                Payments.amount != 1,
                Payments.time_created.between(start, end)
            )
            payments_result = await session.execute(stmt_payments)
            payments_data = payments_result.all()

            if not payments_data:
                return {}, {}

            # Собираем уникальные user_id из платежей
            user_ids = list(set(p[0] for p in payments_data))

            # Получаем данные всех этих пользователей одним запросом
            stmt_users = select(
                Users.user_id,
                Users.ref,
                Users.stamp
            ).where(Users.user_id.in_(user_ids))
            users_result = await session.execute(stmt_users)
            users_data = users_result.all()

        # Словарь для быстрого поиска ref и stamp по user_id
        user_map = {u[0]: (u[1], u[2]) for u in users_data}

        ref_totals = {}
        stamp_totals = {}

        for user_id, amount in payments_data:
            ref, stamp = user_map.get(user_id, (None, None))
            if ref:
                ref_totals[ref] = ref_totals.get(ref, 0) + amount
            if stamp:
                stamp_totals[stamp] = stamp_totals.get(stamp, 0) + amount

        return ref_totals, stamp_totals

    async def update_broadcast_status(self, user_id: int, status: str) -> None:
        """
        Обновляет статус последней рассылки и дату для указанного пользователя.
        """
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(
                last_broadcast_status=status,
                last_broadcast_date=datetime.now()  # сохраняем полную дату и время
            )
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Error updating broadcast status for user {user_id}: {e}")

    async def get_gift(self, gift_id: str) -> Optional[Gifts]:
        """Возвращает запись подарка или None."""
        async with self.session_factory() as session:
            result = await session.execute(select(Gifts).where(Gifts.gift_id == gift_id))
            return result.scalar_one_or_none()

    async def next_gift_number(self) -> int:
        """Следующий порядковый номер для panel username gift_N."""
        async with self.session_factory() as session:
            stmt = select(func.count()).select_from(Users).where(Users.stamp == "gift")
            result = await session.execute(stmt)
            return int(result.scalar_one() or 0) + 1

    async def create_gift_web_user(self, gift_id: str, gift_num: int) -> int:
        """Создаёт пользователя для веб-активации подарка. Возвращает user_id (отрицательный)."""
        panel_username = f"gift_{gift_num}"
        uid = await self.next_negative_user_id()
        async with self.session_factory() as session:
            u = Users(
                user_id=uid,
                stamp="gift",
                field_str_1=gift_id,
                field_str_2=panel_username,
                in_panel=False,
                create_user=_naive_utc(datetime.now(timezone.utc)),
            )
            session.add(u)
            await session.commit()
        return uid

    async def activate_gift(self, gift_id: str, recipient_id: int) -> Tuple[bool, Optional[int], Optional[bool]]:
        """
        Активирует подарок по gift_id для указанного получателя.
        Возвращает (успех, duration, white_flag) или (False, None, None) если подарок не найден или уже активирован.
        """
        async with self.session_factory() as session:
            # Проверяем существование и статус подарка
            stmt = select(Gifts).where(
                Gifts.gift_id == gift_id,
                Gifts.flag == False,
                Gifts.recepient_id == None
            )
            result = await session.execute(stmt)
            gift = result.scalar_one_or_none()

            if not gift:
                logger.warning(f"Gift {gift_id} not found or already activated")
                return False, None, None

            # Активируем подарок
            gift.flag = True
            gift.recepient_id = recipient_id
            try:
                await session.commit()
                logger.info(f"Gift {gift_id} activated for user {recipient_id}")
                return True, gift.duration, gift.white_flag
            except Exception as e:
                await session.rollback()
                logger.error(f"Error activating gift {gift_id} for user {recipient_id}: {e}")
                return False, None, None

    async def get_pending_platega_payments(self) -> List[Payments]:
        """Возвращает все платежи из таблицы payments со статусом 'pending'."""
        async with self.session_factory() as session:
            stmt = select(Payments).where(Payments.status == 'pending')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_pending_platega_card_payments(self) -> List[PaymentsCards]:
        """Возвращает все платежи из таблицы payments со статусом 'pending'."""
        async with self.session_factory() as session:
            stmt = select(PaymentsCards).where(PaymentsCards.status == 'pending')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_pending_platega_crypto_payments(self) -> List[PaymentsPlategaCrypto]:
        """Возвращает все платежи из таблицы payments со статусом 'pending'."""
        async with self.session_factory() as session:
            stmt = select(PaymentsPlategaCrypto).where(PaymentsPlategaCrypto.status == 'pending')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def update_payment_status(self, transaction_id: str, new_status: str) -> None:
        """Обновляет статус платежа по transaction_id."""
        async with self.session_factory() as session:
            stmt = update(Payments).where(Payments.transaction_id == transaction_id).values(status=new_status)
            await session.execute(stmt)
            await session.commit()

    async def update_payment_card_status(self, transaction_id: str, new_status: str) -> None:
        """Обновляет статус платежа по transaction_id."""
        async with self.session_factory() as session:
            stmt = update(PaymentsCards).where(PaymentsCards.transaction_id == transaction_id).values(status=new_status)
            await session.execute(stmt)
            await session.commit()

    async def update_payment_platega_crypto_status(self, transaction_id: str, new_status: str) -> None:
        """Обновляет статус платежа по transaction_id."""
        async with self.session_factory() as session:
            stmt = update(PaymentsPlategaCrypto).where(PaymentsPlategaCrypto.transaction_id == transaction_id).values(status=new_status)
            await session.execute(stmt)
            await session.commit()

    async def alloc_fk_api_nonce(self) -> int:
        """Уникальный растущий nonce для FreeKassa API без отдельной таблицы."""
        return time.time_ns() // 1000

    async def get_pending_fk_sbp_payments(self) -> List[PaymentsFkSBP]:
        async with self.session_factory() as session:
            stmt = select(PaymentsFkSBP).where(PaymentsFkSBP.status == 'pending')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_fk_sbp_payments_for_date(self, day: date) -> List[PaymentsFkSBP]:
        day_start = datetime.combine(day, datetime.min.time())
        day_end = day_start + timedelta(days=1)
        async with self.session_factory() as session:
            stmt = (
                select(PaymentsFkSBP)
                .where(
                    PaymentsFkSBP.time_created >= day_start,
                    PaymentsFkSBP.time_created < day_end,
                )
                .order_by(PaymentsFkSBP.time_created)
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def update_fk_sbp_payment_status(self, transaction_id: str, new_status: str) -> None:
        async with self.session_factory() as session:
            stmt = update(PaymentsFkSBP).where(
                PaymentsFkSBP.transaction_id == transaction_id
            ).values(status=new_status)
            await session.execute(stmt)
            await session.commit()

    async def add_fk_sbp_payment(
            self,
            user_id: int,
            amount: int,
            status: str,
            transaction_id: str,
            fk_order_id: Optional[int],
            payload: str,
            nonce: int,
            signature: str,
            is_gift: bool = False,
            method: str = 'fk_qr_card',
    ) -> None:
        async with self.session_factory() as session:
            payment = PaymentsFkSBP(
                user_id=user_id,
                amount=amount,
                status=status,
                transaction_id=transaction_id,
                fk_order_id=fk_order_id,
                payload=payload,
                nonce=nonce,
                signature=signature,
                method=method,
                is_gift=is_gift,
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(
                    f"💰 Платёж FreeKassa записан: user_id={user_id}, amount={amount}, is_gift={is_gift}, method={method}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа FreeKassa: {e}")
                raise

    async def get_pending_wata_sbp_payments(self) -> List[PaymentsWataSBP]:
        async with self.session_factory() as session:
            stmt = select(PaymentsWataSBP).where(PaymentsWataSBP.status == 'pending')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def count_pending_wata_sbp(self) -> int:
        async with self.session_factory() as session:
            stmt = select(func.count()).select_from(PaymentsWataSBP).where(PaymentsWataSBP.status == "pending")
            return int((await session.execute(stmt)).scalar_one())

    async def count_pending_wata_card(self) -> int:
        async with self.session_factory() as session:
            stmt = select(func.count()).select_from(PaymentsWataCard).where(PaymentsWataCard.status == "pending")
            return int((await session.execute(stmt)).scalar_one())

    async def get_pending_wata_card_payments(self) -> List[PaymentsWataCard]:
        async with self.session_factory() as session:
            stmt = select(PaymentsWataCard).where(PaymentsWataCard.status == 'pending')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_pending_wata_sbp_payments_polled(
        self,
        recent_hours: int = 72,
        recent_limit: int = 100,
        stale_limit: int = 50,
    ) -> List[PaymentsWataSBP]:
        cutoff = datetime.now() - timedelta(hours=recent_hours)
        async with self.session_factory() as session:
            q_recent = (
                select(PaymentsWataSBP)
                .where(PaymentsWataSBP.status == "pending", PaymentsWataSBP.time_created >= cutoff)
                .order_by(PaymentsWataSBP.time_created.desc())
                .limit(recent_limit)
            )
            q_stale = (
                select(PaymentsWataSBP)
                .where(PaymentsWataSBP.status == "pending", PaymentsWataSBP.time_created < cutoff)
                .order_by(PaymentsWataSBP.time_created.asc())
                .limit(stale_limit)
            )
            r1 = (await session.execute(q_recent)).scalars().all()
            r2 = (await session.execute(q_stale)).scalars().all()
        seen: set[int] = set()
        out: List[PaymentsWataSBP] = []
        for p in (*r1, *r2):
            if p.id in seen:
                continue
            seen.add(p.id)
            out.append(p)
        return out

    async def get_pending_wata_card_payments_polled(
        self,
        recent_hours: int = 72,
        recent_limit: int = 100,
        stale_limit: int = 50,
    ) -> List[PaymentsWataCard]:
        cutoff = datetime.now() - timedelta(hours=recent_hours)
        async with self.session_factory() as session:
            q_recent = (
                select(PaymentsWataCard)
                .where(PaymentsWataCard.status == "pending", PaymentsWataCard.time_created >= cutoff)
                .order_by(PaymentsWataCard.time_created.desc())
                .limit(recent_limit)
            )
            q_stale = (
                select(PaymentsWataCard)
                .where(PaymentsWataCard.status == "pending", PaymentsWataCard.time_created < cutoff)
                .order_by(PaymentsWataCard.time_created.asc())
                .limit(stale_limit)
            )
            r1 = (await session.execute(q_recent)).scalars().all()
            r2 = (await session.execute(q_stale)).scalars().all()
        seen: set[int] = set()
        out: List[PaymentsWataCard] = []
        for p in (*r1, *r2):
            if p.id in seen:
                continue
            seen.add(p.id)
            out.append(p)
        return out

    async def update_wata_sbp_status(self, transaction_id: str, new_status: str) -> None:
        async with self.session_factory() as session:
            stmt = update(PaymentsWataSBP).where(PaymentsWataSBP.transaction_id == transaction_id).values(status=new_status)
            await session.execute(stmt)
            await session.commit()

    async def update_wata_card_status(self, transaction_id: str, new_status: str) -> None:
        async with self.session_factory() as session:
            stmt = update(PaymentsWataCard).where(PaymentsWataCard.transaction_id == transaction_id).values(status=new_status)
            await session.execute(stmt)
            await session.commit()

    async def add_wata_sbp_payment(
        self, user_id: int, amount: int, status: str, transaction_id: str, payload: str, is_gift: bool = False
    ) -> None:
        async with self.session_factory() as session:
            payment = PaymentsWataSBP(
                user_id=user_id,
                amount=amount,
                status=status,
                transaction_id=transaction_id,
                payload=payload,
                is_gift=is_gift,
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(f"💰 Платёж WATA СБП записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа WATA СБП: {e}")
                raise

    async def add_wata_card_payment(
        self, user_id: int, amount: int, status: str, transaction_id: str, payload: str, is_gift: bool = False
    ) -> None:
        async with self.session_factory() as session:
            payment = PaymentsWataCard(
                user_id=user_id,
                amount=amount,
                status=status,
                transaction_id=transaction_id,
                payload=payload,
                is_gift=is_gift,
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(f"💰 Платёж WATA Карта записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа WATA Карта: {e}")
                raise

    async def count_open_payment_slots_for_user(self, user_id: int) -> int:
        uid = int(user_id)
        async with self.session_factory() as session:
            total = 0
            pairs = (
                (PaymentsWataSBP, and_(PaymentsWataSBP.user_id == uid, PaymentsWataSBP.status == "pending")),
                (PaymentsWataCard, and_(PaymentsWataCard.user_id == uid, PaymentsWataCard.status == "pending")),
                (
                    PaymentsCryptobot,
                    and_(
                        PaymentsCryptobot.user_id == uid,
                        or_(
                            PaymentsCryptobot.status == "active",
                            PaymentsCryptobot.status == "pending",
                        ),
                    ),
                ),
                (Payments, and_(Payments.user_id == uid, Payments.status == "pending")),
                (PaymentsCards, and_(PaymentsCards.user_id == uid, PaymentsCards.status == "pending")),
                (PaymentsPlategaCrypto, and_(PaymentsPlategaCrypto.user_id == uid, PaymentsPlategaCrypto.status == "pending")),
                (PaymentsFkSBP, and_(PaymentsFkSBP.user_id == uid, PaymentsFkSBP.status == "pending")),
            )
            for model, cond in pairs:
                q = select(func.count()).select_from(model).where(cond)
                total += int((await session.execute(q)).scalar_one())
        return total

    async def get_payment_by_transaction_id(self, transaction_id: str, user_id: int) -> Optional[str]:
        async with self.session_factory() as session:
            for model in (Payments, PaymentsCards, PaymentsPlategaCrypto, PaymentsWataSBP, PaymentsWataCard, PaymentsFkSBP):
                stmt = select(model).where(
                    model.transaction_id == transaction_id,
                    model.user_id == user_id,
                )
                row = (await session.execute(stmt)).scalar_one_or_none()
                if row is not None:
                    return row.status
        return None

    async def get_active_cryptobot_payments(self) -> List[PaymentsCryptobot]:
        """
        Возвращает все платежи Cryptobot со статусом 'active'.
        """
        async with self.session_factory() as session:
            stmt = select(PaymentsCryptobot).where(PaymentsCryptobot.status == 'active')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def update_cryptobot_payment_status(self, payment_id: int, status: str) -> None:
        """
        Обновляет статус платежа Cryptobot.
        """
        async with self.session_factory() as session:
            stmt = update(PaymentsCryptobot).where(PaymentsCryptobot.id == payment_id).values(status=status)
            await session.execute(stmt)
            await session.commit()

    async def add_payment_stars(self, user_id: int, amount: int, is_gift: bool, payload: str) -> None:
        """Добавляет запись в таблицу payments_stars."""
        async with self.session_factory() as session:
            payment = PaymentsStars(
                user_id=user_id,
                amount=amount,
                is_gift=is_gift,
                status='confirmed',
                payload=payload
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(
                    f"💰 Платёж Telegram Stars записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа Telegram Stars: {e}")

    async def create_gift(self, giver_id: int, duration: int, white_flag: bool) -> str:
        """Создаёт запись о подарке и возвращает gift_id."""
        gift_id = str(uuid.uuid4())
        async with self.session_factory() as session:
            gift = Gifts(
                gift_id=gift_id,
                giver_id=giver_id,
                duration=duration,
                recepient_id=None,
                white_flag=white_flag,
                flag=False
            )
            session.add(gift)
            try:
                await session.commit()
                logger.info(f"✅ Запись о подарке создана: gift_id={gift_id}")
                return gift_id
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка создания подарка: {e}")
                raise

    async def count_users_with_active_subscription(self) -> int:
        """Число людей с хотя бы одной активной подпиской (end_date > now)."""
        async with self.session_factory() as session:
            now = datetime.now()
            active_any = or_(
                and_(Users.subscription_end_date.isnot(None), Users.subscription_end_date > now),
                and_(Users.white_subscription_end_date.isnot(None), Users.white_subscription_end_date > now),
            )
            stmt = select(func.count()).select_from(Users).where(active_any)
            result = await session.execute(stmt)
            return int(result.scalar() or 0)

    async def add_online_stats(
        self,
        users_panel: int,
        users_active: int,
        users_pay: int,
        users_trial: int,
        users_subscribed: int,
    ) -> None:
        """
        Сохраняет ежедневную статистику онлайн-активности.
        """
        async with self.session_factory() as session:
            online_record = Online(
                users_panel=users_panel,
                users_active=users_active,
                users_pay=users_pay,
                users_trial=users_trial,
                users_subscribed=users_subscribed,
            )
            session.add(online_record)
            await session.commit()

    async def add_platega_payment(self, user_id: int, amount: int, status: str, transaction_id: str, payload: str,
                                  is_gift: bool = False) -> None:
        """
        Записывает платёж Platega в таблицу payments.
        """
        async with self.session_factory() as session:
            payment = Payments(
                user_id=user_id,
                amount=amount,
                status=status,
                transaction_id=transaction_id,
                is_gift=is_gift,
                payload=payload
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(f"💰 Платёж Platega SBP записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа Platega: {e}")
                raise

    async def add_platega_card_payment(self, user_id: int, amount: int, status: str, transaction_id: str, payload: str,
                                       is_gift: bool = False) -> None:
        """
        Записывает платёж PlategaCard в таблицу payments.
        """
        async with self.session_factory() as session:
            payment = PaymentsCards(
                user_id=user_id,
                amount=amount,
                status=status,
                transaction_id=transaction_id,
                payload=payload,
                is_gift=is_gift
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(f"💰 Платёж Platega Card записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа Platega: {e}")
                raise

    async def add_platega_crypto_payment(self, user_id: int, amount: int, status: str, transaction_id: str, payload: str,
                                       is_gift: bool = False) -> None:
        """
        Записывает платёж PlategaCard в таблицу payments.
        """
        async with self.session_factory() as session:
            payment = PaymentsPlategaCrypto(
                user_id=user_id,
                amount=amount,
                status=status,
                transaction_id=transaction_id,
                payload=payload,
                is_gift=is_gift
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(f"💰 Платёж Platega Crypto записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа Platega: {e}")
                raise

    async def add_cryptobot_payment(self, user_id: int, amount: float, currency: str, is_gift: bool, invoice_id: str,
                                    payload: str) -> None:
        """
        Запись платежа Cryptobot в таблицу payments_cryptobot.
        """
        async with self.session_factory() as session:
            payment = PaymentsCryptobot(
                user_id=user_id,
                amount=amount,
                currency=currency,
                is_gift=is_gift,
                status='active',
                invoice_id=invoice_id,
                payload=payload
            )
            session.add(payment)
            await session.commit()
            logger.info(f"Cryptobot invoice created: {invoice_id} for user {user_id}")

    async def get_all_users(self) -> List[Users]:
        """Возвращает список всех пользователей."""
        async with self.session_factory() as session:
            result = await session.execute(select(Users))
            return result.scalars().all()

    async def select_users_subscription_after_cutoff(
        self, cutoff: datetime
    ) -> List[Users]:
        """Для /add_new_users: is_delete=False, subscription_end_date > cutoff."""
        async with self.session_factory() as session:
            stmt = (
                select(Users)
                .where(
                    Users.subscription_end_date.isnot(None),
                    Users.subscription_end_date > cutoff,
                )
                .order_by(Users.user_id)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def select_users_subscription_on_or_before_cutoff(
        self, cutoff: datetime
    ) -> List[Users]:
        """Для /add_new_users: is_delete=False, subscription_end_date не пусто и <= cutoff."""
        async with self.session_factory() as session:
            stmt = (
                select(Users)
                .where(
                    Users.subscription_end_date.isnot(None),
                    Users.subscription_end_date <= cutoff,
                )
                .order_by(Users.user_id)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def select_user_ids_main_sub_on_or_after(
        self, cutoff: datetime
    ) -> List[int]:
        """Для /add_2_bonus: is_delete=False, обычная подписка (subscription_end_date) >= cutoff."""
        async with self.session_factory() as session:
            stmt = (
                select(Users.user_id)
                .where(
                    Users.subscription_end_date.isnot(None),
                    Users.subscription_end_date >= cutoff,
                )
                .order_by(Users.user_id)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def get_all_payments(self) -> List[Payments]:
        """Возвращает список всех платежей Platega."""
        async with self.session_factory() as session:
            result = await session.execute(select(Payments))
            return result.scalars().all()

    async def get_all_payments_cards(self) -> List[PaymentsCards]:
        """Возвращает список всех платежей по картам (PaymentsCards)."""
        async with self.session_factory() as session:
            result = await session.execute(select(PaymentsCards))
            return result.scalars().all()

    async def get_all_payments_platega_crypto(self) -> List[PaymentsPlategaCrypto]:
        async with self.session_factory() as session:
            result = await session.execute(select(PaymentsPlategaCrypto))
            return result.scalars().all()

    async def get_all_payments_stars(self) -> List[PaymentsStars]:
        """Возвращает список всех платежей Telegram Stars."""
        async with self.session_factory() as session:
            result = await session.execute(select(PaymentsStars))
            return result.scalars().all()

    async def get_all_payments_cryptobot(self) -> List[PaymentsCryptobot]:
        """Возвращает список всех крипто-платежей."""
        async with self.session_factory() as session:
            result = await session.execute(select(PaymentsCryptobot))
            return result.scalars().all()

    async def get_all_gifts(self) -> List[Gifts]:
        """Возвращает список всех подарков."""
        async with self.session_factory() as session:
            result = await session.execute(select(Gifts))
            return result.scalars().all()

    async def get_all_online(self) -> List[Online]:
        """Возвращает список всех записей онлайн-статистики."""
        async with self.session_factory() as session:
            result = await session.execute(select(Online))
            return result.scalars().all()

    async def get_all_white_counter(self) -> List[WhiteCounter]:
        """Возвращает список всех записей white_counter."""
        async with self.session_factory() as session:
            result = await session.execute(select(WhiteCounter))
            return result.scalars().all()

    async def get_export_snapshot(self) -> Dict[str, List[Any]]:
        """
        Одна сессия БД: все SELECT для /export подряд.
        Меньше открытий соединения и накладных расходов, чем десять отдельных get_all_*.
        """
        async with self.session_factory() as session:
            users_list = (await session.execute(select(Users))).scalars().all()
            payments_list = (await session.execute(select(Payments))).scalars().all()
            payments_cards_list = (await session.execute(select(PaymentsCards))).scalars().all()
            payments_platega_crypto_list = (await session.execute(select(PaymentsPlategaCrypto))).scalars().all()
            payments_fk_sbp_list = (await session.execute(select(PaymentsFkSBP))).scalars().all()
            payments_wata_sbp_list = (await session.execute(select(PaymentsWataSBP))).scalars().all()
            payments_wata_card_list = (await session.execute(select(PaymentsWataCard))).scalars().all()
            payments_stars_list = (await session.execute(select(PaymentsStars))).scalars().all()
            payments_cryptobot_list = (await session.execute(select(PaymentsCryptobot))).scalars().all()
            gifts_list = (await session.execute(select(Gifts))).scalars().all()
            online_list = (await session.execute(select(Online))).scalars().all()
            white_counter_list = (await session.execute(select(WhiteCounter))).scalars().all()
            platega_autopay_list = (await session.execute(select(PlategaAutopaySubscription))).scalars().all()
            platega_recurent_list = (await session.execute(select(PlategaRecurent))).scalars().all()
        return {
            "users": users_list,
            "payments": payments_list,
            "payments_cards": payments_cards_list,
            "payments_platega_crypto": payments_platega_crypto_list,
            "payments_fk_sbp": payments_fk_sbp_list,
            "payments_wata_sbp": payments_wata_sbp_list,
            "payments_wata_card": payments_wata_card_list,
            "payments_stars": payments_stars_list,
            "payments_cryptobot": payments_cryptobot_list,
            "gifts": gifts_list,
            "online": online_list,
            "white_counter": white_counter_list,
            "platega_autopay_subscriptions": platega_autopay_list,
            "platega_recurent": platega_recurent_list,
        }

    async def get_trafic_stat_source(
        self,
    ) -> Tuple[
        List[Tuple[int, Optional[int], Optional[datetime], float, float]],
        List[Tuple[int, datetime, Any, Optional[str], bool, str, Optional[str]]],
    ]:
        """
        Данные для /trafic_stat.
        users: (user_id, linked_telegram_id, subscription_end_date, trafic_wl, limit_wl)
        payments: (user_id, time_created, amount, payload, is_gift, channel, currency)
        channel: rub | stars | cryptobot. Успешные платежи (confirmed/paid/CONFIRMED).
        Активная подписка сейчас, trafic_wl > 7 ГБ, без тарифа «Навсегда».
        """
        from wl_traffic.constants import FOREVER_END_CUTOFF

        users_rows: List[Tuple[int, Optional[int], Optional[datetime], float, float]] = []
        pay_rows: List[Tuple[int, datetime, Any, Optional[str], bool, str, Optional[str]]] = []
        now = datetime.now()

        async with self.session_factory() as session:
            uq = select(
                Users.user_id,
                Users.linked_telegram_id,
                Users.subscription_end_date,
                Users.trafic_wl,
                Users.limit_wl,
            ).where(
                Users.is_delete == False,
                Users.subscription_end_date.isnot(None),
                Users.subscription_end_date > now,
                Users.subscription_end_date < FOREVER_END_CUTOFF,
                Users.trafic_wl > 7,
            )
            for uid, linked, end_dt, trafic, limit_wl in (await session.execute(uq)).all():
                users_rows.append((
                    int(uid),
                    int(linked) if linked is not None else None,
                    end_dt,
                    float(trafic or 0.0),
                    float(limit_wl or 0.0),
                ))

            rub_queries = (
                select(
                    Payments.user_id, Payments.time_created, Payments.amount,
                    Payments.payload, Payments.is_gift,
                ).where(Payments.status.in_(_BILLING_OK_STATUSES)),
                select(
                    PaymentsCards.user_id, PaymentsCards.time_created, PaymentsCards.amount,
                    PaymentsCards.payload, PaymentsCards.is_gift,
                ).where(PaymentsCards.status.in_(_BILLING_OK_STATUSES)),
                select(
                    PaymentsPlategaCrypto.user_id, PaymentsPlategaCrypto.time_created,
                    PaymentsPlategaCrypto.amount, PaymentsPlategaCrypto.payload,
                    PaymentsPlategaCrypto.is_gift,
                ).where(PaymentsPlategaCrypto.status.in_(_BILLING_OK_STATUSES)),
                select(
                    PaymentsWataSBP.user_id, PaymentsWataSBP.time_created, PaymentsWataSBP.amount,
                    PaymentsWataSBP.payload, PaymentsWataSBP.is_gift,
                ).where(PaymentsWataSBP.status.in_(_BILLING_OK_STATUSES)),
                select(
                    PaymentsWataCard.user_id, PaymentsWataCard.time_created, PaymentsWataCard.amount,
                    PaymentsWataCard.payload, PaymentsWataCard.is_gift,
                ).where(PaymentsWataCard.status.in_(_BILLING_OK_STATUSES)),
                select(
                    PaymentsFkSBP.user_id, PaymentsFkSBP.time_created, PaymentsFkSBP.amount,
                    PaymentsFkSBP.payload, PaymentsFkSBP.is_gift,
                ).where(PaymentsFkSBP.status.in_(_BILLING_OK_STATUSES)),
            )
            for q in rub_queries:
                for uid, tc, amt, pl, ig in (await session.execute(q)).all():
                    pay_rows.append((int(uid), tc, amt, pl, bool(ig), "rub", None))

            q_stars = select(
                PaymentsStars.user_id, PaymentsStars.time_created, PaymentsStars.amount,
                PaymentsStars.payload, PaymentsStars.is_gift,
            ).where(PaymentsStars.status.in_(_BILLING_OK_STATUSES))
            for uid, tc, amt, pl, ig in (await session.execute(q_stars)).all():
                pay_rows.append((int(uid), tc, amt, pl, bool(ig), "stars", None))

            q_crypto = select(
                PaymentsCryptobot.user_id, PaymentsCryptobot.time_created, PaymentsCryptobot.amount,
                PaymentsCryptobot.payload, PaymentsCryptobot.is_gift, PaymentsCryptobot.currency,
            ).where(PaymentsCryptobot.status.in_(_BILLING_OK_STATUSES))
            for uid, tc, amt, pl, ig, cur in (await session.execute(q_crypto)).all():
                pay_rows.append((
                    int(uid), tc, amt, pl, bool(ig), "cryptobot",
                    str(cur) if cur else None,
                ))

            q_rec = select(
                PlategaRecurent.user_id, PlategaRecurent.time_created, PlategaRecurent.amount,
                PlategaRecurent.payload,
            ).where(func.lower(PlategaRecurent.status).in_(_BILLING_OK_STATUSES))
            for uid, tc, amt, pl in (await session.execute(q_rec)).all():
                pay_rows.append((int(uid), tc, amt, pl, False, "rub", None))

        return users_rows, pay_rows

    async def add_white_counter_if_not_exists(self, user_id: int) -> None:
        """
        Добавляет запись в white_counter, если её ещё нет для данного пользователя.
        """
        async with self.session_factory() as session:
            stmt = select(WhiteCounter).where(WhiteCounter.user_id == user_id)
            result = await session.execute(stmt)
            if not result.scalar_one_or_none():
                session.add(WhiteCounter(user_id=user_id))
                await session.commit()
                logger.info(f"✅ Добавлена запись в white_counter для пользователя {user_id}")

    async def set_reserve_field_for_paid_users(self) -> int:
        """
        Устанавливает reserve_field = True для всех пользователей,
        у которых есть хотя бы один подтверждённый платёж в любой из таблиц.
        Возвращает количество обновлённых записей.
        """
        async with self.session_factory() as session:
            # Подзапросы для каждой таблицы с нужным статусом
            from sqlalchemy import union, select, update

            subq_payments = select(Payments.user_id).where(Payments.status == 'confirmed')
            subq_cards = select(PaymentsCards.user_id).where(PaymentsCards.status == 'confirmed')
            subq_platega_crypto = select(PaymentsPlategaCrypto.user_id).where(
                PaymentsPlategaCrypto.status == 'confirmed')
            subq_fk_sbp = select(PaymentsFkSBP.user_id).where(PaymentsFkSBP.status == 'confirmed')
            subq_wata_sbp = select(PaymentsWataSBP.user_id).where(PaymentsWataSBP.status == 'confirmed')
            subq_wata_card = select(PaymentsWataCard.user_id).where(PaymentsWataCard.status == 'confirmed')
            subq_stars = select(PaymentsStars.user_id).where(PaymentsStars.status == 'confirmed')
            subq_cryptobot = select(PaymentsCryptobot.user_id).where(PaymentsCryptobot.status == 'paid')

            union_query = union(
                subq_payments,
                subq_cards,
                subq_platega_crypto,
                subq_fk_sbp,
                subq_wata_sbp,
                subq_wata_card,
                subq_stars,
                subq_cryptobot
            ).subquery()

            stmt = (
                update(Users)
                .where(Users.user_id.in_(union_query))
                .values(reserve_field=True)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get_users_with_payment(self) -> List[int]:
        """Возвращает список user_id пользователей с has_discount=True и is_delete=False."""
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(
                Users.reserve_field == True
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def get_user_subscription_payment_report(
        self, user_id: int
    ) -> List[Tuple[datetime, str, str, str, str]]:
        """
        Успешные платежи пользователя (confirmed/paid) по всем таблицам оплат,
        включая подтверждённые рекуррентные списания Platega (CONFIRMED).
        Возвращает список (time_created, тип, способ оплаты, детали, transaction_id).
        transaction_id заполнен только у автосписаний Platega.
        """
        rows_acc: List[Tuple[datetime, str, str, str, str]] = []

        def _parse_map(payload: Optional[str]) -> Dict[str, str]:
            if not payload:
                return {}
            out: Dict[str, str] = {}
            for part in payload.split(","):
                if ":" not in part:
                    continue
                k, _, v = part.partition(":")
                out[k.strip()] = v.strip()
            return out

        def _row_report_fields(
            payload: Optional[str], is_gift: bool, amount: Any
        ) -> Tuple[str, str, str]:
            from payments.tariff_gate import tariff_period_label

            m = _parse_map(payload)
            method = _payment_method_label(m.get("method"))
            raw_duration = m.get("duration")

            traffic_gb = _parse_traffic_duration(raw_duration)
            if traffic_gb is not None:
                return "Трафик", method, f"{traffic_gb} GB"

            white = m.get("white", "False").lower() == "true"
            gift = bool(is_gift) or m.get("gift", "False").lower() == "true"
            dur = _payload_duration_to_panel_days(raw_duration)
            if dur is None:
                try:
                    amt_f = float(amount)
                except (TypeError, ValueError):
                    amt_f = None
                if amt_f is not None:
                    if white:
                        dur = _white_days_from_amount_fallback(amt_f)
                    else:
                        dur = _billing_duration_from_amount_fallback(amt_f)

            if gift and white:
                label = "Подарок, вайт (mobile)"
            elif gift:
                label = "Подарок, обычная"
            elif white:
                label = "Вайт (mobile)"
            else:
                label = "Обычная"

            days_s = tariff_period_label(dur) if dur is not None else "—"
            return label, method, days_s

        async with self.session_factory() as session:
            queries: List[Any] = [
                select(
                    Payments.user_id,
                    Payments.time_created,
                    Payments.amount,
                    Payments.payload,
                    Payments.is_gift,
                ).where(
                    Payments.user_id == user_id,
                    Payments.status.in_(_BILLING_OK_STATUSES),
                ),
                select(
                    PaymentsCards.user_id,
                    PaymentsCards.time_created,
                    PaymentsCards.amount,
                    PaymentsCards.payload,
                    PaymentsCards.is_gift,
                ).where(
                    PaymentsCards.user_id == user_id,
                    PaymentsCards.status.in_(_BILLING_OK_STATUSES),
                ),
                select(
                    PaymentsPlategaCrypto.user_id,
                    PaymentsPlategaCrypto.time_created,
                    PaymentsPlategaCrypto.amount,
                    PaymentsPlategaCrypto.payload,
                    PaymentsPlategaCrypto.is_gift,
                ).where(
                    PaymentsPlategaCrypto.user_id == user_id,
                    PaymentsPlategaCrypto.status.in_(_BILLING_OK_STATUSES),
                ),
                select(
                    PaymentsStars.user_id,
                    PaymentsStars.time_created,
                    PaymentsStars.amount,
                    PaymentsStars.payload,
                    PaymentsStars.is_gift,
                ).where(
                    PaymentsStars.user_id == user_id,
                    PaymentsStars.status.in_(_BILLING_OK_STATUSES),
                ),
                select(
                    PaymentsCryptobot.user_id,
                    PaymentsCryptobot.time_created,
                    PaymentsCryptobot.amount,
                    PaymentsCryptobot.payload,
                    PaymentsCryptobot.is_gift,
                ).where(
                    PaymentsCryptobot.user_id == user_id,
                    PaymentsCryptobot.status.in_(_BILLING_OK_STATUSES),
                ),
                select(
                    PaymentsFkSBP.user_id,
                    PaymentsFkSBP.time_created,
                    PaymentsFkSBP.amount,
                    PaymentsFkSBP.payload,
                    PaymentsFkSBP.is_gift,
                ).where(
                    PaymentsFkSBP.user_id == user_id,
                    PaymentsFkSBP.status.in_(_BILLING_OK_STATUSES),
                ),
                select(
                    PaymentsWataSBP.user_id,
                    PaymentsWataSBP.time_created,
                    PaymentsWataSBP.amount,
                    PaymentsWataSBP.payload,
                    PaymentsWataSBP.is_gift,
                ).where(
                    PaymentsWataSBP.user_id == user_id,
                    PaymentsWataSBP.status.in_(_BILLING_OK_STATUSES),
                ),
                select(
                    PaymentsWataCard.user_id,
                    PaymentsWataCard.time_created,
                    PaymentsWataCard.amount,
                    PaymentsWataCard.payload,
                    PaymentsWataCard.is_gift,
                ).where(
                    PaymentsWataCard.user_id == user_id,
                    PaymentsWataCard.status.in_(_BILLING_OK_STATUSES),
                ),
            ]
            for q in queries:
                for _uid, tc, amt, pl, ig in (await session.execute(q)).all():
                    kind, method, detail = _row_report_fields(pl, bool(ig), amt)
                    rows_acc.append((tc, kind, method, detail, ""))

            rec_q = select(
                PlategaRecurent.time_created,
                PlategaRecurent.amount,
                PlategaRecurent.payload,
                PlategaRecurent.transaction_id,
            ).where(
                PlategaRecurent.user_id == user_id,
                PlategaRecurent.status == "CONFIRMED",
            )
            for tc, amt, pl, tx_id in (await session.execute(rec_q)).all():
                kind, method, detail = _row_report_fields(pl, False, amt)
                rows_acc.append((tc, kind, method, detail, str(tx_id or "").strip()))

        rows_acc.sort(key=lambda x: (x[0], x[1]))
        return rows_acc

    # ── Platega recurrent autopay ─────────────────────────────────────

    _PLATEGA_AUTOPAY_ACTIVE = ('pending', 'active', 'past_due')

    async def get_active_platega_autopay(self, user_id: int) -> Optional[PlategaAutopaySubscription]:
        rows = await self.list_active_platega_autopays(user_id)
        return rows[0] if rows else None

    async def get_status_active_platega_autopay(self, user_id: int) -> Optional[PlategaAutopaySubscription]:
        async with AsyncSessionLocal() as session:
            stmt = (
                select(PlategaAutopaySubscription)
                .where(
                    PlategaAutopaySubscription.user_id == user_id,
                    PlategaAutopaySubscription.status == 'active',
                )
                .order_by(PlategaAutopaySubscription.id.desc())
                .limit(1)
            )
            return (await session.execute(stmt)).scalar_one_or_none()

    async def list_active_platega_autopays(self, user_id: int) -> List[PlategaAutopaySubscription]:
        async with AsyncSessionLocal() as session:
            stmt = (
                select(PlategaAutopaySubscription)
                .where(
                    PlategaAutopaySubscription.user_id == user_id,
                    PlategaAutopaySubscription.status.in_(self._PLATEGA_AUTOPAY_ACTIVE),
                )
                .order_by(PlategaAutopaySubscription.id.desc())
            )
            return list((await session.execute(stmt)).scalars().all())

    async def platega_recurent_has_confirmed(self, subscription_id: str) -> bool:
        async with AsyncSessionLocal() as session:
            stmt = select(PlategaRecurent.id).where(
                PlategaRecurent.subscription_id == subscription_id,
                PlategaRecurent.status == 'CONFIRMED',
                PlategaRecurent.processed.is_(True),
            ).limit(1)
            return (await session.execute(stmt)).scalar_one_or_none() is not None

    async def get_platega_autopay_by_subscription_id(
        self, subscription_id: str
    ) -> Optional[PlategaAutopaySubscription]:
        async with AsyncSessionLocal() as session:
            stmt = select(PlategaAutopaySubscription).where(
                PlategaAutopaySubscription.subscription_id == subscription_id
            )
            return (await session.execute(stmt)).scalar_one_or_none()

    async def add_platega_autopay_pending(
        self,
        user_id: int,
        subscription_id: str,
        duration: str,
        amount: int,
        payload: str,
        *,
        white: bool = False,
        source: Optional[str] = None,
    ) -> None:
        async with AsyncSessionLocal() as session:
            row = PlategaAutopaySubscription(
                user_id=user_id,
                subscription_id=subscription_id,
                duration=duration,
                amount=amount,
                status='pending',
                payload=payload,
                white=white,
                source=source,
            )
            session.add(row)
            await session.commit()
            logger.success(
                "Platega autopay pending: user_id={} subscription_id={} duration={}",
                user_id, subscription_id, duration,
            )

    async def update_platega_autopay_status(
        self,
        subscription_id: str,
        status: str,
        *,
        next_charge_at: Optional[datetime] = None,
        cancel_reason: Optional[str] = None,
    ) -> None:
        async with AsyncSessionLocal() as session:
            values: Dict[str, Any] = {'status': status}
            if next_charge_at is not None:
                values['next_charge_at'] = next_charge_at
            if cancel_reason is not None:
                values['cancel_reason'] = cancel_reason
                values['time_cancelled'] = datetime.now()
            stmt = (
                update(PlategaAutopaySubscription)
                .where(PlategaAutopaySubscription.subscription_id == subscription_id)
            )
            if status != 'cancelled':
                stmt = stmt.where(PlategaAutopaySubscription.status != 'cancelled')
            stmt = stmt.values(**values)
            await session.execute(stmt)
            await session.commit()

    async def add_platega_recurent_payment(
        self,
        user_id: int,
        subscription_id: str,
        transaction_id: str,
        amount: int,
        currency: str,
        status: str,
        payment_method: Optional[int],
        payload: Optional[str],
        next_charge_at: Optional[datetime],
    ) -> Optional[PlategaRecurent]:
        async with AsyncSessionLocal() as session:
            existing = (
                await session.execute(
                    select(PlategaRecurent).where(PlategaRecurent.transaction_id == transaction_id)
                )
            ).scalar_one_or_none()
            if existing:
                if payload and not (existing.payload or "").strip():
                    existing.payload = payload
                    await session.commit()
                    await session.refresh(existing)
                return existing
            row = PlategaRecurent(
                user_id=user_id,
                subscription_id=subscription_id,
                transaction_id=transaction_id,
                amount=amount,
                currency=currency or 'RUB',
                status=status,
                payment_method=payment_method,
                payload=payload,
                next_charge_at=next_charge_at,
                processed=False,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row

    async def mark_platega_recurent_processed(self, transaction_id: str) -> None:
        async with AsyncSessionLocal() as session:
            stmt = (
                update(PlategaRecurent)
                .where(PlategaRecurent.transaction_id == transaction_id)
                .values(processed=True)
            )
            await session.execute(stmt)
            await session.commit()
