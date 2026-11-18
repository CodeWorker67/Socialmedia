"""Методы AsyncSQL для landing API (/api/landing). Подключается при импорте."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Tuple

from sqlalchemy import func, select, update

from config_bd.models import SecondSite, Users
from config_bd.utils import AsyncSQL, _norm_email, _MERGE_PAYMENT_MODELS, _BILLING_OK_STATUSES


class _LandingSQL:
    LANDING_USER_ID_START = -50001

    @staticmethod
    async def _second_site_for_tg_id(session, tg_id: int) -> Optional[SecondSite]:
        stmt = select(SecondSite).where(SecondSite.tg_id == tg_id)
        return (await session.execute(stmt)).scalar_one_or_none()

    async def get_landing_site_by_tg_id(self, tg_id: int) -> Optional[SecondSite]:
        async with self.session_factory() as session:
            return await self._second_site_for_tg_id(session, tg_id)

    async def get_landing_user_by_email(self, email: str) -> Optional[Tuple[Users, SecondSite]]:
        em = _norm_email(email)
        async with self.session_factory() as session:
            stmt = select(SecondSite).where(func.lower(SecondSite.email) == em)
            site = (await session.execute(stmt)).scalar_one_or_none()
            if site is None:
                return None
            stmt = select(Users).where(Users.user_id == site.tg_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user is None:
                return None
            return user, site

    async def get_landing_user_by_google_sub(self, google_sub: str) -> Optional[Tuple[Users, SecondSite]]:
        async with self.session_factory() as session:
            stmt = select(SecondSite).where(SecondSite.google_sub == google_sub)
            site = (await session.execute(stmt)).scalar_one_or_none()
            if site is None:
                return None
            stmt = select(Users).where(Users.user_id == site.tg_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user is None:
                return None
            return user, site

    async def get_landing_user_by_phone(self, phone: str) -> Optional[Tuple[Users, SecondSite]]:
        normalized = str(phone).strip()
        async with self.session_factory() as session:
            stmt = select(SecondSite).where(SecondSite.phone == normalized)
            site = (await session.execute(stmt)).scalar_one_or_none()
            if site is None:
                return None
            stmt = select(Users).where(Users.user_id == site.tg_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user is None:
                return None
            return user, site

    async def get_landing_user_by_whatsapp_id(self, whatsapp_id: str) -> Optional[Tuple[Users, SecondSite]]:
        wa = str(whatsapp_id).strip()
        async with self.session_factory() as session:
            stmt = select(SecondSite).where(SecondSite.whatsapp_id == wa)
            site = (await session.execute(stmt)).scalar_one_or_none()
            if site is None:
                return None
            stmt = select(Users).where(Users.user_id == site.tg_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user is None:
                return None
            return user, site

    async def get_landing_user_by_internal_id(self, internal_id: int) -> Optional[Tuple[Users, SecondSite]]:
        async with self.session_factory() as session:
            user = await session.get(Users, internal_id)
            if user is None:
                return None
            site = await self._second_site_for_tg_id(session, user.user_id)
            if site is None:
                return None
            return user, site

    async def next_landing_user_id(self) -> int:
        start = self.LANDING_USER_ID_START
        async with self.session_factory() as session:
            stmt = select(func.min(Users.user_id)).where(Users.user_id <= start)
            result = await session.execute(stmt)
            m = result.scalar_one_or_none()
            if m is None:
                return start
            return int(m) - 1

    async def register_landing_email_user(
        self,
        email: str,
        stamp: str = "",
        site_url: Optional[str] = None,
        partner: str = "",
        yandex_id: Optional[str] = None,
    ) -> int:
        em = _norm_email(email)
        uid = await self.next_landing_user_id()
        partner_val = partner or None
        async with self.session_factory() as session:
            u = Users(
                user_id=uid,
                stamp=stamp or "email",
                partner=partner_val,
                yandex_id=yandex_id or None,
                create_user=datetime.now(),
            )
            session.add(u)
            await session.flush()
            session.add(
                SecondSite(
                    tg_id=uid,
                    email=em,
                    site_url=site_url,
                    verified=False,
                )
            )
            await session.commit()
            await session.refresh(u)
            return int(u.id)

    async def register_landing_phone_user(
        self,
        phone: str,
        stamp: str = "",
        site_url: Optional[str] = None,
        partner: str = "",
        yandex_id: Optional[str] = None,
    ) -> int:
        normalized = str(phone).strip()
        uid = await self.next_landing_user_id()
        partner_val = partner or None
        async with self.session_factory() as session:
            u = Users(
                user_id=uid,
                stamp=stamp or "phone",
                partner=partner_val,
                yandex_id=yandex_id or None,
                create_user=datetime.now(),
            )
            session.add(u)
            await session.flush()
            session.add(
                SecondSite(
                    tg_id=uid,
                    phone=normalized,
                    site_url=site_url,
                    verified=True,
                )
            )
            await session.commit()
            await session.refresh(u)
            return int(u.id)

    async def register_landing_whatsapp_user(
        self,
        whatsapp_id: str,
        stamp: str = "",
        site_url: Optional[str] = None,
        partner: str = "",
        phone: Optional[str] = None,
        yandex_id: Optional[str] = None,
    ) -> int:
        wa = str(whatsapp_id).strip()
        uid = await self.next_landing_user_id()
        partner_val = partner or None
        phone_val = str(phone).strip() if phone else None
        async with self.session_factory() as session:
            if phone_val:
                existing_phone = (
                    await session.execute(
                        select(SecondSite.id).where(SecondSite.phone == phone_val)
                    )
                ).scalar_one_or_none()
                if existing_phone is not None:
                    phone_val = None
            u = Users(
                user_id=uid,
                stamp=stamp or "whatsapp",
                partner=partner_val,
                yandex_id=yandex_id or None,
                create_user=datetime.now(),
            )
            session.add(u)
            await session.flush()
            session.add(
                SecondSite(
                    tg_id=uid,
                    whatsapp_id=wa,
                    phone=phone_val,
                    site_url=site_url,
                    verified=True,
                )
            )
            await session.commit()
            await session.refresh(u)
            return int(u.id)

    async def register_landing_google_user(
        self,
        email: str,
        google_sub: str,
        stamp: str = "",
        site_url: Optional[str] = None,
        partner: str = "",
        yandex_id: Optional[str] = None,
    ) -> int:
        em = _norm_email(email)
        uid = await self.next_landing_user_id()
        partner_val = partner or None
        async with self.session_factory() as session:
            u = Users(
                user_id=uid,
                stamp=stamp or "google",
                partner=partner_val,
                yandex_id=yandex_id or None,
                create_user=datetime.now(),
            )
            session.add(u)
            await session.flush()
            session.add(
                SecondSite(
                    tg_id=uid,
                    email=em,
                    google_sub=google_sub,
                    site_url=site_url,
                    verified=True,
                )
            )
            await session.commit()
            await session.refresh(u)
            return int(u.id)

    async def set_landing_activation_pass_by_email(self, email: str, value) -> bool:
        em = _norm_email(email)
        async with self.session_factory() as session:
            stmt = (
                update(SecondSite)
                .where(func.lower(SecondSite.email) == em)
                .values(activation_pass=value)
            )
            r = await session.execute(stmt)
            await session.commit()
            return (r.rowcount or 0) > 0

    async def set_landing_email_verified(self, internal_id: int, verified: bool) -> bool:
        async with self.session_factory() as session:
            user = await session.get(Users, internal_id)
            if user is None:
                return False
            stmt = (
                update(SecondSite)
                .where(SecondSite.tg_id == user.user_id)
                .values(verified=verified)
            )
            r = await session.execute(stmt)
            await session.commit()
            return (r.rowcount or 0) > 0

    async def set_landing_password_by_internal_id(self, internal_id: int, password_hash: str) -> bool:
        async with self.session_factory() as session:
            user = await session.get(Users, internal_id)
            if user is None:
                return False
            stmt = (
                update(SecondSite)
                .where(SecondSite.tg_id == user.user_id)
                .values(password=password_hash)
            )
            r = await session.execute(stmt)
            await session.commit()
            return (r.rowcount or 0) > 0

    async def clear_landing_password_by_internal_id(self, internal_id: int) -> bool:
        async with self.session_factory() as session:
            user = await session.get(Users, internal_id)
            if user is None:
                return False
            stmt = (
                update(SecondSite)
                .where(SecondSite.tg_id == user.user_id)
                .values(password=None)
            )
            r = await session.execute(stmt)
            await session.commit()
            return (r.rowcount or 0) > 0

    async def select_partner_referrals_list(self, partner_id: int, limit: int = 50) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
            stmt = (
                select(Users.user_id, Users.create_user, Users.in_panel, Users.email)
                .where(Users.partner == str(partner_id))
                .order_by(Users.create_user.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.all()
            out: list[dict[str, Any]] = []
            for uid, created, in_panel, email in rows:
                paid_sum = 0
                for model in _MERGE_PAYMENT_MODELS:
                    stmt_sum = select(func.coalesce(func.sum(model.amount), 0)).where(
                        model.user_id == uid,
                        model.status.in_(_BILLING_OK_STATUSES),
                    )
                    paid_sum += int((await session.execute(stmt_sum)).scalar() or 0)
                out.append(
                    {
                        "user_id": int(uid),
                        "email": email,
                        "registered_at": created.isoformat() if created else None,
                        "has_subscription": bool(in_panel),
                        "payments_sum": paid_sum,
                    }
                )
            return out


def _apply_landing_sql_patch() -> None:
    for name, attr in _LandingSQL.__dict__.items():
        if name.startswith("__"):
            continue
        setattr(AsyncSQL, name, attr)


_apply_landing_sql_patch()