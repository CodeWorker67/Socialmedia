"""Landing site API — /api/landing/* (OTP email, Google, tariffs, payments)."""
from __future__ import annotations

import re
import secrets
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional
from urllib.parse import urlparse

import aiohttp
import bcrypt
import jwt
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr, Field

from bot import sql, x3
from lead_tracker import post_user_registered, post_user_trial
from X3 import panel_username_for_site_user
from config import (
    ADMIN_IDS,
    API_FREEKASSA,
    BOT_URL,
    GOOGLE_CLIENT_ID,
    JWT_SECRET,
    LANDING_SITE_URL,
    LOGINBOT_API_KEY,
    PARTNER_MIN,
    PARTNER_PROCENT,
    PAYMENT_MAX_PENDING_PER_USER,
    PLATEGA_API_KEY,
    PLATEGA_MERCHANT_ID,
    SHOP_ID_FREEKASSA,
    SUPPORT_URL,
    WEB_API_PUBLIC_URL,
    WHATSAPP_AUTH_CODE_TTL_SEC,
    WHATSAPP_VERIFY_TOKEN,
    WHATSAPP_WA_ME_NUMBER,
)
from config_bd.utils import _norm_email
from lexicon import dct_desc, dct_price, lexicon
from logging_config import logger
from payments.payload_source import SITE
from payments.pay_freekassa import pay_site
from payments.platega_recurrent import create_recurrent_payment, is_recurrent_tariff
from unisender_go import send_transactional_email
from wl_traffic.constants import WL_TRAFFIC_TARIFFS
from wl_traffic.service import get_wl_used_gb_for_user
from services.loginbot import LOGINBOT_DEFAULT_TIMEOUT, LoginBotError, start_call_auth
from services.yandex_metrica import report_trial_to_metrica
from services.whatsapp_cloud import (
    WhatsAppCloudError,
    parse_incoming_text_messages,
    send_text_message,
    verify_webhook_signature,
    whatsapp_configured,
)


async def post_user_registered_from_internal_id(internal_id: int, *, username: Optional[str] = None) -> None:
    user = await sql.get_user_object_by_internal_id(internal_id)
    if user is None:
        return
    await post_user_registered(
        int(user.user_id),
        username or user.email or user.username,
        user.fullname,
        (user.stamp or "").strip() or None,
    )

landing_router = APIRouter(prefix="/api/landing", tags=["landing"])

LANDING_JWT_MAX_AGE = 365 * 24 * 3600  # 1 year
LANDING_COOKIE = "landing_auth"

# Публичный список landing — раздел «Купить» (без триала и без «Навсегда»).
TARIFF_PUBLIC = [
    ("365", "1 год", 5, False),
    ("180", "6 месяцев", 5, False),
    ("90", "3 месяца", 5, False),
    ("30", "1 месяц", 5, False),
    ("7", "7 дней", 5, False),
]


async def _create_landing_sbp_checkout(
    *,
    billing_user_id: int,
    duration_str: str,
    price: int,
    description: str,
    white: bool,
    is_gift: bool,
    telegram_username: Optional[str] = None,
) -> dict[str, Any]:
    if (
        not is_gift
        and is_recurrent_tariff(duration_str)
        and PLATEGA_API_KEY
        and PLATEGA_MERCHANT_ID
    ):
        return await create_recurrent_payment(
            user_id=billing_user_id,
            duration=duration_str,
            amount=int(price),
            description=description,
            white=white,
            source=SITE,
        )
    if not API_FREEKASSA or SHOP_ID_FREEKASSA is None:
        return {"status": "error", "url": "", "id": ""}
    return await pay_site(
        val=str(price),
        des=description,
        billing_user_id=billing_user_id,
        duration=duration_str,
        white=white,
        is_gift=is_gift,
        kind="sbp",
        telegram_username=telegram_username,
        payload_source=SITE,
    )


def _wl_traffic_site_price(gb: str, billing_user_id: int) -> int:
    price = WL_TRAFFIC_TARIFFS.get(gb, 0)
    if billing_user_id in ADMIN_IDS:
        return 10
    return price

_rate_limits: dict[str, list[float]] = {}
_phone_auth_sessions: dict[str, dict[str, Any]] = {}
_whatsapp_auth_codes: dict[str, dict[str, Any]] = {}
_whatsapp_wa_id_active_code: dict[str, str] = {}
bearer_scheme = HTTPBearer(auto_error=False)
PHONE_SESSION_TTL = LOGINBOT_DEFAULT_TIMEOUT + 120
WHATSAPP_CODE_TTL = WHATSAPP_AUTH_CODE_TTL_SEC


def _rate_check(key: str, max_requests: int, window_sec: int) -> bool:
    now = time.time()
    timestamps = [t for t in _rate_limits.get(key, []) if now - t < window_sec]
    if len(timestamps) >= max_requests:
        _rate_limits[key] = timestamps
        return False
    timestamps.append(now)
    _rate_limits[key] = timestamps
    return True


def _rate_limit_or_raise(request_ip: str, action: str, max_req: int = 5, window: int = 300):
    key = f"landing:{action}:{request_ip}"
    if not _rate_check(key, max_req, window):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Слишком много попыток. Подождите 5 минут.")


def _site_url_from_request(request: Request) -> Optional[str]:
    origin = (request.headers.get("origin") or "").strip()
    if origin:
        return origin.rstrip("/")
    referer = (request.headers.get("referer") or "").strip()
    if referer:
        parsed = urlparse(referer)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return None


def _client_ip(request: Request) -> str:
    x_real = (request.headers.get("x-real-ip") or "").strip()
    if x_real:
        return x_real
    xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if xff:
        return xff
    return request.client.host if request.client else ""


def _client_is_https(request: Request) -> bool:
    proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
    if proto == "https":
        return True
    return request.url.scheme == "https"


def _cookie_params(request: Request) -> tuple[Literal["lax", "none"], bool]:
    if _client_is_https(request):
        return "none", True
    return "lax", False


def _require_jwt_secret() -> str:
    if not JWT_SECRET:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "JWT_SECRET is not configured")
    return JWT_SECRET


def _issue_landing_jwt(*, user_id: int, auth: str, username: Optional[str]) -> str:
    secret = _require_jwt_secret()
    exp = datetime.now(timezone.utc) + timedelta(seconds=LANDING_JWT_MAX_AGE)
    payload: dict[str, Any] = {
        "user_id": user_id,
        "auth": auth,
        "site": "landing",
        "exp": exp,
    }
    if username is not None:
        payload["username"] = username
    token = jwt.encode(payload, secret, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def _set_landing_cookie(request: Request, response, token: str) -> None:
    samesite, secure = _cookie_params(request)
    response.set_cookie(
        key=LANDING_COOKIE,
        value=token,
        httponly=True,
        secure=secure,
        samesite=samesite,
        max_age=LANDING_JWT_MAX_AGE,
        path="/",
    )


def _clear_landing_cookie(request: Request, response) -> None:
    samesite, secure = _cookie_params(request)
    response.delete_cookie(key=LANDING_COOKIE, path="/", secure=secure, httponly=True, samesite=samesite)


def _auth_response(request: Request, token: str, user: dict, **extra) -> JSONResponse:
    body = {"token": token, "user": user, **extra}
    resp = JSONResponse(content=body)
    resp.headers["X-Auth-Token"] = token
    _set_landing_cookie(request, resp, token)
    return resp


async def get_landing_jwt_context(
    request: Request,
    cred: Annotated[Optional[HTTPAuthorizationCredentials], Depends(bearer_scheme)],
) -> dict[str, Any]:
    raw_token = None
    if cred and cred.credentials:
        raw_token = cred.credentials
    else:
        raw_token = request.cookies.get(LANDING_COOKIE)
    if not raw_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    secret = _require_jwt_secret()
    try:
        payload = jwt.decode(raw_token, secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    if payload.get("site") != "landing":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    uid = payload.get("user_id")
    if isinstance(uid, (int, float)):
        uid = int(uid)
    elif isinstance(uid, str) and uid.isdigit():
        uid = int(uid)
    else:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    auth = payload.get("auth") or "email"
    if auth not in ("email", "google", "phone", "whatsapp"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    return {"user_id": uid, "username": payload.get("username"), "auth": auth}


LandingCtx = Annotated[dict[str, Any], Depends(get_landing_jwt_context)]


def _random_otp_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


async def _send_landing_otp(email: str) -> None:
    code = _random_otp_code()
    expires = datetime.now(timezone.utc) + timedelta(minutes=15)
    activation_value = f"{code}:{int(expires.timestamp())}"
    await sql.set_landing_activation_pass_by_email(email, activation_value)
    body = f"Ваш код для входа: {code}\n\nКод действителен 15 минут."
    try:
        await send_transactional_email(
            to_email=email,
            subject="Код входа — Social VPN",
            plaintext=body,
        )
    except Exception as e:
        logger.warning("Landing OTP email failed: {}", e)
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            "Не удалось отправить код на почту. Попробуйте позже.",
        ) from e


def _landing_auth_type(site) -> str:
    if site.google_sub:
        return "google"
    if getattr(site, "whatsapp_id", None):
        return "whatsapp"
    if getattr(site, "phone", None):
        return "phone"
    return "email"


def _landing_raffle_tickets(user) -> int:
    raw = getattr(user, "tickets", None)
    try:
        n = int(raw or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, n)


def _landing_subscription_plan_name(user, panel_active: bool) -> str:
    """«Пробный» только при активном доступе без успешных оплат (reserve_field)."""
    if not panel_active:
        return "Подписка"
    if bool(getattr(user, "reserve_field", False)):
        return "Подписка"
    return "Пробный"


def _landing_user_dict(user, site) -> dict[str, Any]:
    tickets = _landing_raffle_tickets(user)
    return {
        "id": int(user.id),
        "email": site.email,
        "phone": getattr(site, "phone", None),
        "whatsapp_id": getattr(site, "whatsapp_id", None),
        "auth": _landing_auth_type(site),
        "billing_user_id": int(user.user_id),
        "has_password": bool(site.password),
        "tickets": tickets,
        "raffle_tickets": tickets,
        "has_paid_subscription": bool(getattr(user, "reserve_field", False)),
        "in_panel": bool(getattr(user, "in_panel", False)),
        "stamp": (getattr(user, "stamp", None) or "").strip() or None,
        "trial_available": not bool(getattr(user, "in_panel", False)),
    }


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) != 11 or not digits.startswith("7"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный номер телефона")
    return digits


def _format_phone_display(phone: str) -> str:
    if len(phone) == 11 and phone.startswith("7"):
        return f"+7 ({phone[1:4]}) {phone[4:7]}-{phone[7:9]}-{phone[9:11]}"
    return phone


def _loginbot_webhook_url(request: Request) -> str:
    base = WEB_API_PUBLIC_URL or str(request.base_url).rstrip("/")
    return f"{base}/api/landing/auth/phone/webhook"


def _cleanup_phone_sessions() -> None:
    now = time.time()
    expired = [
        rid
        for rid, session in _phone_auth_sessions.items()
        if now - float(session.get("created_at", 0)) > PHONE_SESSION_TTL
    ]
    for rid in expired:
        _phone_auth_sessions.pop(rid, None)


def _whatsapp_bot_url() -> Optional[str]:
    if not WHATSAPP_WA_ME_NUMBER:
        return None
    digits = re.sub(r"\D", "", WHATSAPP_WA_ME_NUMBER)
    if not digits:
        return None
    return f"https://wa.me/{digits}?text=START"


def _cleanup_whatsapp_codes() -> None:
    now = time.time()
    expired_codes = [
        code
        for code, data in _whatsapp_auth_codes.items()
        if now > float(data.get("expires_at", 0))
    ]
    for code in expired_codes:
        data = _whatsapp_auth_codes.pop(code, None)
        if data:
            wa_id = str(data.get("wa_id") or "")
            if _whatsapp_wa_id_active_code.get(wa_id) == code:
                _whatsapp_wa_id_active_code.pop(wa_id, None)


def _whatsapp_phone_from_wa_id(wa_id: str) -> Optional[str]:
    digits = re.sub(r"\D", "", wa_id or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return digits
    return None


async def _issue_whatsapp_login_code(
    *,
    wa_id: str,
    profile_name: str,
    partner: str = "",
    site_url: Optional[str] = None,
) -> str:
    _cleanup_whatsapp_codes()
    old_code = _whatsapp_wa_id_active_code.get(wa_id)
    if old_code:
        _whatsapp_auth_codes.pop(old_code, None)
    code = _random_otp_code()
    while code in _whatsapp_auth_codes:
        code = _random_otp_code()
    expires_at = time.time() + WHATSAPP_CODE_TTL
    _whatsapp_auth_codes[code] = {
        "wa_id": wa_id,
        "profile_name": profile_name,
        "partner": partner,
        "site_url": site_url,
        "expires_at": expires_at,
    }
    _whatsapp_wa_id_active_code[wa_id] = code
    return code


async def _ensure_landing_whatsapp_user(
    wa_id: str,
    *,
    profile_name: str,
    site_url: Optional[str],
    partner: str,
    landing_stamp: str = "",
    yandex_id: Optional[str] = None,
) -> tuple[Any, Any]:
    pair = await sql.get_landing_user_by_whatsapp_id(wa_id)
    if pair is not None:
        user, site = pair
        await sql.set_landing_email_verified(int(user.id), True)
        return user, site
    phone = _whatsapp_phone_from_wa_id(wa_id)
    if landing_stamp:
        stamp = landing_stamp[:100]
    else:
        stamp = profile_name[:100] if profile_name else "whatsapp"
    internal_id = await sql.register_landing_whatsapp_user(
        wa_id,
        stamp=stamp,
        site_url=site_url,
        partner=partner,
        phone=phone,
        yandex_id=yandex_id,
    )
    await post_user_registered_from_internal_id(
        internal_id,
        username=profile_name or wa_id,
    )
    pair = await sql.get_landing_user_by_internal_id(internal_id)
    if pair is None:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "User creation failed")
    return pair


async def _handle_whatsapp_incoming_message(
    *,
    wa_id: str,
    profile_name: str,
    text: str,
) -> None:
    code = await _issue_whatsapp_login_code(
        wa_id=wa_id,
        profile_name=profile_name,
        partner="",
        site_url=None,
    )
    minutes = max(1, WHATSAPP_CODE_TTL // 60)
    reply = (
        f"Ваш код для входа на сайт Social VPN: {code}\n\n"
        f"Введите его на странице входа. Код действует {minutes} мин."
    )
    await send_text_message(wa_id, reply)


async def _ensure_landing_phone_user(
    phone: str,
    *,
    site_url: Optional[str],
    partner: str,
    stamp: str = "",
    yandex_id: Optional[str] = None,
) -> tuple[Any, Any]:
    pair = await sql.get_landing_user_by_phone(phone)
    if pair is not None:
        user, site = pair
        await sql.set_landing_email_verified(int(user.id), True)
        return user, site
    internal_id = await sql.register_landing_phone_user(
        phone,
        site_url=site_url,
        partner=partner,
        stamp=stamp,
        yandex_id=yandex_id,
    )
    await post_user_registered_from_internal_id(internal_id, username=phone)
    pair = await sql.get_landing_user_by_internal_id(internal_id)
    if pair is None:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "User creation failed")
    return pair


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, stored: Optional[str]) -> bool:
    if not stored:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
    except ValueError:
        return False


def _validate_password_pair(password: str, confirm: str) -> None:
    if len(password) < 4:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль должен быть не короче 4 символов")
    if password != confirm:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароли не совпадают")


def _tariff_parts(tariff_id: str) -> tuple[str, str, bool]:
    white = "white" in tariff_id
    desc_key = tariff_id.replace("_white", "")
    return desc_key, desc_key, white


def _reject_mobile_purchase(tariff_id: str) -> None:
    if _tariff_parts(tariff_id)[2]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Мобильный тариф недоступен на сайте")


def _activ_block(result: dict) -> tuple[bool, Optional[str]]:
    active = str(result.get("activ", "")).startswith("✅")
    t = result.get("time") or "-"
    expires = t if active and t != "-" else None
    return active, expires


async def _landing_user_pair(ctx: LandingCtx):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    return pair


async def _landing_panel_username(ctx: LandingCtx) -> str:
    user, _site = await _landing_user_pair(ctx)
    return panel_username_for_site_user(int(user.user_id), False)


def _parse_partner_ref(raw: Optional[str]) -> Optional[str]:
    """Парсит partner id из ?start=partner_{id} или голого id."""
    if not raw:
        return None
    value = str(raw).strip()
    if value.startswith("partner_"):
        value = value.replace("partner_", "", 1)
    try:
        pid = int(value)
    except ValueError:
        return None
    if pid == 0:
        return None
    return str(pid)


def _parse_landing_stamp(raw: Optional[str]) -> str:
    """Метка из ?start=, если это не partner id."""
    if not raw:
        return ""
    value = str(raw).strip()
    if not value or value.startswith("partner_"):
        return ""
    if value.startswith("YD"):
        from utils.yd_start import parse_yd_start_arg

        parsed = parse_yd_start_arg(value)
        return parsed[0] if parsed else ""
    if _parse_partner_ref(value):
        return ""
    return value[:100]


def _parse_landing_yandex_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    value = str(raw).strip()
    if not value:
        return None
    return value[:100]


def _landing_yandex_id(
    explicit: Optional[str],
    *,
    stamp_raw: Optional[str] = None,
    start_query: Optional[str] = None,
) -> Optional[str]:
    """ClientID Метрики из поля yandex_id или из YD{stamp}_{yandex_id} в start/stamp."""
    yd = _parse_landing_yandex_id(explicit)
    if yd:
        return yd
    from utils.yd_start import parse_yd_start_arg

    for raw in (stamp_raw, start_query):
        if not raw:
            continue
        value = str(raw).strip()
        if not value.startswith("YD"):
            continue
        parsed = parse_yd_start_arg(value)
        if parsed:
            return parsed[1]
    return None


def _landing_attribution(
    *,
    partner_raw: Optional[str],
    stamp_raw: Optional[str],
    start_query: Optional[str] = None,
) -> tuple[str, str]:
    partner = _parse_partner_ref(partner_raw) or ""
    stamp = _parse_landing_stamp(stamp_raw)
    if start_query:
        if not partner:
            partner = _parse_partner_ref(start_query) or ""
        if not stamp:
            stamp = _parse_landing_stamp(start_query)
    return partner, stamp


class EmailIn(BaseModel):
    email: EmailStr
    partner: Optional[str] = None
    stamp: Optional[str] = None
    yandex_id: Optional[str] = None


class VerifyCodeIn(BaseModel):
    email: EmailStr
    code: str = Field(min_length=6, max_length=6)


class GoogleAuthIn(BaseModel):
    credential: str
    partner: Optional[str] = None
    stamp: Optional[str] = None
    yandex_id: Optional[str] = None


class CreatePaymentIn(BaseModel):
    tariff_id: str
    method: Literal["sbp", "card"]
    is_gift: bool = False


class CreateTrafficPaymentIn(BaseModel):
    gb: str = Field(..., description="Размер пакета в GB (ключ из WL_TRAFFIC_TARIFFS)")
    method: Literal["sbp", "card"]


class PasswordLoginIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=4)


class SetPasswordIn(BaseModel):
    password: str = Field(min_length=4)
    password_confirm: str = Field(min_length=4)


class ChangePasswordIn(BaseModel):
    current_password: str = Field(min_length=4)
    new_password: str = Field(min_length=4)
    password_confirm: str = Field(min_length=4)


class RemovePasswordIn(BaseModel):
    current_password: str = Field(min_length=4)


class PhoneStartIn(BaseModel):
    phone: str = Field(min_length=10, max_length=32)
    partner: Optional[str] = None
    stamp: Optional[str] = None
    yandex_id: Optional[str] = None


class WhatsAppVerifyIn(BaseModel):
    code: str = Field(min_length=6, max_length=6)
    partner: Optional[str] = None
    stamp: Optional[str] = None
    yandex_id: Optional[str] = None


@landing_router.post("/auth/check-email")
async def landing_check_email(body: EmailIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "check-email", max_req=20, window=300)
    em = str(body.email).strip().lower()
    pair = await sql.get_landing_user_by_email(em)
    has_password = bool(pair and pair[1].password)
    return {"email": em, "has_password": has_password}


@landing_router.post("/auth/password-login")
async def landing_password_login(body: PasswordLoginIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "password-login", max_req=10, window=300)
    em = str(body.email).strip().lower()
    pair = await sql.get_landing_user_by_email(em)
    if pair is None or not _verify_password(body.password, pair[1].password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Неверный email или пароль")
    user, site = pair
    internal_id = int(user.id)
    await sql.set_landing_email_verified(internal_id, True)
    token = _issue_landing_jwt(user_id=internal_id, auth="email", username=em)
    return _auth_response(request, token, _landing_user_dict(user, site), success=True)


@landing_router.post("/auth/send-code")
async def landing_send_code(body: EmailIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "send-code", max_req=5, window=300)
    em = str(body.email).strip().lower()
    existing = await sql.get_landing_user_by_email(em)
    if existing is None:
        partner, stamp = _landing_attribution(
            partner_raw=body.partner,
            stamp_raw=body.stamp,
            start_query=request.query_params.get("start"),
        )
        start_q = request.query_params.get("start")
        internal_id = await sql.register_landing_email_user(
            em,
            site_url=_site_url_from_request(request),
            partner=partner,
            stamp=stamp,
            yandex_id=_landing_yandex_id(
                body.yandex_id,
                stamp_raw=body.stamp,
                start_query=start_q,
            ),
        )
        await post_user_registered_from_internal_id(internal_id, username=em)
    await _send_landing_otp(em)
    return {"success": True, "email": em}


@landing_router.post("/auth/verify-code")
async def landing_verify_code(body: VerifyCodeIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "verify-code", max_req=10, window=300)
    if not body.code.isdigit() or len(body.code) != 6:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код")
    pair = await sql.get_landing_user_by_email(str(body.email))
    if pair is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пользователь не найден")
    user, site = pair
    activation = site.activation_pass
    if not activation or ":" not in str(activation):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Код не был отправлен")
    stored_code, expires_ts = str(activation).rsplit(":", 1)
    try:
        if int(time.time()) > int(expires_ts):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Код истёк, запросите новый")
    except ValueError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код")
    if stored_code != body.code:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код")
    internal_id = int(user.id)
    await sql.set_landing_email_verified(internal_id, True)
    await sql.set_landing_activation_pass_by_email(str(body.email), None)
    em = site.email or str(body.email).strip().lower()
    token = _issue_landing_jwt(user_id=internal_id, auth="email", username=em)
    return _auth_response(request, token, _landing_user_dict(user, site), success=True)


@landing_router.post("/auth/resend-code")
async def landing_resend_code(body: EmailIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "resend-code", max_req=3, window=300)
    em = str(body.email).strip().lower()
    pair = await sql.get_landing_user_by_email(em)
    if pair is None:
        return {"success": True}
    await _send_landing_otp(em)
    return {"success": True}


@landing_router.post("/auth/google")
async def landing_google(body: GoogleAuthIn, request: Request):
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Google login not configured")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://oauth2.googleapis.com/tokeninfo?id_token={body.credential}"
        ) as resp:
            if resp.status != 200:
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid Google token")
            payload = await resp.json()
    if payload.get("aud") != GOOGLE_CLIENT_ID:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid Google token audience")
    google_email = payload.get("email")
    google_sub = payload.get("sub")
    if not google_email or not payload.get("email_verified") or not google_sub:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Google email not verified")
    em = google_email.strip().lower()
    pair = await sql.get_landing_user_by_google_sub(google_sub)
    if pair is None:
        by_email = await sql.get_landing_user_by_email(em)
        if by_email:
            user, site = by_email
            internal_id = int(user.id)
        else:
            partner, stamp = _landing_attribution(
                partner_raw=body.partner,
                stamp_raw=body.stamp,
                start_query=request.query_params.get("start"),
            )
            start_q = request.query_params.get("start")
            internal_id = await sql.register_landing_google_user(
                em,
                google_sub,
                site_url=_site_url_from_request(request),
                partner=partner,
                stamp=stamp,
                yandex_id=_landing_yandex_id(
                    body.yandex_id,
                    stamp_raw=body.stamp,
                    start_query=start_q,
                ),
            )
            await post_user_registered_from_internal_id(
                internal_id,
                username=em,
                full_name=payload.get("name") or payload.get("given_name"),
            )
            pair = await sql.get_landing_user_by_internal_id(internal_id)
            if pair is None:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "User creation failed")
            user, site = pair
    else:
        user, site = pair
    internal_id = int(user.id)
    token = _issue_landing_jwt(user_id=internal_id, auth="google", username=em)
    return _auth_response(request, token, _landing_user_dict(user, site), success=True)


@landing_router.post("/auth/phone/start")
async def landing_phone_start(body: PhoneStartIn, request: Request):
    if not LOGINBOT_API_KEY:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Phone login not configured")
    _rate_limit_or_raise(_client_ip(request), "phone-start", max_req=5, window=300)
    _cleanup_phone_sessions()
    phone = _normalize_phone(body.phone)
    partner, stamp = _landing_attribution(
        partner_raw=body.partner,
        stamp_raw=body.stamp,
        start_query=request.query_params.get("start"),
    )
    site_url = _site_url_from_request(request)
    webhook = _loginbot_webhook_url(request)
    try:
        auth_data = await start_call_auth(
            phone,
            webhook=webhook,
            payload=phone,
            timeout=LOGINBOT_DEFAULT_TIMEOUT,
        )
    except LoginBotError as e:
        logger.warning("LoginBot auth start failed: {}", e)
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            "Не удалось начать авторизацию по телефону. Попробуйте позже.",
        ) from e
    request_id = str(auth_data.get("requestId") or "")
    call_to_phone = str(auth_data.get("callToPhone") or "")
    if not request_id or not call_to_phone:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Некорректный ответ LoginBot")
    start_q = request.query_params.get("start")
    _phone_auth_sessions[request_id] = {
        "request_id": request_id,
        "phone": phone,
        "status": "pending",
        "created_at": time.time(),
        "partner": partner,
        "stamp": stamp,
        "yandex_id": _landing_yandex_id(
            body.yandex_id,
            stamp_raw=body.stamp,
            start_query=start_q,
        ),
        "site_url": site_url,
        "client_ip": _client_ip(request),
        "internal_id": None,
    }
    return {
        "request_id": request_id,
        "call_to_phone": call_to_phone,
        "call_to_phone_display": _format_phone_display(call_to_phone),
        "timeout": int(auth_data.get("timeout") or LOGINBOT_DEFAULT_TIMEOUT),
        "valid_till": auth_data.get("validTill"),
    }


@landing_router.post("/auth/phone/webhook")
async def landing_phone_webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid JSON")
    request_id = str(body.get("requestId") or "")
    status_value = str(body.get("status") or "")
    phone = str(body.get("phone") or "")
    if not request_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing requestId")
    session = _phone_auth_sessions.get(request_id)
    if session is None:
        logger.info("LoginBot webhook for unknown requestId={}", request_id)
        return {"ok": True}
    session["status"] = status_value
    if phone:
        session["phone"] = _normalize_phone(phone)
    if status_value == "accepted":
        try:
            user, _site = await _ensure_landing_phone_user(
                session["phone"],
                site_url=session.get("site_url"),
                partner=session.get("partner") or "",
                stamp=session.get("stamp") or "",
                yandex_id=session.get("yandex_id"),
            )
            session["internal_id"] = int(user.id)
        except Exception as e:
            logger.exception("Phone webhook user provisioning failed: {}", e)
            session["status"] = "rejected"
    return {"ok": True}


@landing_router.get("/auth/phone/status/{request_id}")
async def landing_phone_status(request_id: str, request: Request):
    _rate_limit_or_raise(_client_ip(request), "phone-status", max_req=40, window=300)
    _cleanup_phone_sessions()
    session = _phone_auth_sessions.get(request_id)
    if session is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Сессия авторизации не найдена")
    if session.get("client_ip") and session["client_ip"] != _client_ip(request):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Доступ запрещён")
    status_value = str(session.get("status") or "pending")
    if status_value == "pending":
        return {"status": "pending"}
    if status_value == "accepted":
        internal_id = session.get("internal_id")
        if internal_id is None:
            user, site = await _ensure_landing_phone_user(
                session["phone"],
                site_url=session.get("site_url"),
                partner=session.get("partner") or "",
                stamp=session.get("stamp") or "",
                yandex_id=session.get("yandex_id"),
            )
            internal_id = int(user.id)
            session["internal_id"] = internal_id
        else:
            pair = await sql.get_landing_user_by_internal_id(int(internal_id))
            if pair is None:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "User not found")
            user, site = pair
        phone = session["phone"]
        token = _issue_landing_jwt(user_id=int(user.id), auth="phone", username=phone)
        _phone_auth_sessions.pop(request_id, None)
        return _auth_response(
            request,
            token,
            _landing_user_dict(user, site),
            success=True,
            status="accepted",
        )
    _phone_auth_sessions.pop(request_id, None)
    messages = {
        "rejected": "Время ожидания звонка истекло. Попробуйте снова.",
        "canceled": "Авторизация отменена. Попробуйте снова.",
    }
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"status": status_value, "detail": messages.get(status_value, "Авторизация не удалась")},
    )


@landing_router.get("/auth/whatsapp/config")
async def landing_whatsapp_config():
    if not whatsapp_configured():
        return {"enabled": False, "bot_url": None, "code_ttl_seconds": WHATSAPP_CODE_TTL}
    return {
        "enabled": True,
        "bot_url": _whatsapp_bot_url(),
        "code_ttl_seconds": WHATSAPP_CODE_TTL,
    }


@landing_router.post("/auth/whatsapp/verify-code")
async def landing_whatsapp_verify_code(body: WhatsAppVerifyIn, request: Request):
    if not whatsapp_configured():
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "WhatsApp login not configured")
    _rate_limit_or_raise(_client_ip(request), "whatsapp-verify", max_req=15, window=300)
    if not body.code.isdigit() or len(body.code) != 6:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код")
    _cleanup_whatsapp_codes()
    session = _whatsapp_auth_codes.get(body.code)
    if session is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный или истёкший код")
    if time.time() > float(session.get("expires_at", 0)):
        _whatsapp_auth_codes.pop(body.code, None)
        wa_id = str(session.get("wa_id") or "")
        if _whatsapp_wa_id_active_code.get(wa_id) == body.code:
            _whatsapp_wa_id_active_code.pop(wa_id, None)
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Код истёк, запросите новый в WhatsApp")
    wa_id = str(session.get("wa_id") or "")
    partner, stamp = _landing_attribution(
        partner_raw=body.partner or session.get("partner"),
        stamp_raw=body.stamp or session.get("stamp"),
        start_query=request.query_params.get("start"),
    )
    site_url = session.get("site_url") or _site_url_from_request(request)
    user, site = await _ensure_landing_whatsapp_user(
        wa_id,
        profile_name=str(session.get("profile_name") or ""),
        site_url=site_url,
        partner=str(partner),
        landing_stamp=stamp,
        yandex_id=_landing_yandex_id(
            body.yandex_id or session.get("yandex_id"),
            stamp_raw=body.stamp or session.get("stamp"),
            start_query=request.query_params.get("start"),
        ),
    )
    _whatsapp_auth_codes.pop(body.code, None)
    _whatsapp_wa_id_active_code.pop(wa_id, None)
    username = getattr(site, "whatsapp_id", None) or wa_id
    token = _issue_landing_jwt(user_id=int(user.id), auth="whatsapp", username=str(username))
    return _auth_response(request, token, _landing_user_dict(user, site), success=True)


@landing_router.get("/auth/whatsapp/webhook")
async def landing_whatsapp_webhook_verify(
    hub_mode: str = Query(alias="hub.mode", default=""),
    hub_verify_token: str = Query(alias="hub.verify_token", default=""),
    hub_challenge: str = Query(alias="hub.challenge", default=""),
):
    if hub_mode != "subscribe":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid mode")
    if not WHATSAPP_VERIFY_TOKEN or hub_verify_token != WHATSAPP_VERIFY_TOKEN:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid verify token")
    return PlainTextResponse(content=hub_challenge)


@landing_router.post("/auth/whatsapp/webhook")
async def landing_whatsapp_webhook(request: Request):
    raw = await request.body()
    signature = request.headers.get("x-hub-signature-256")
    if not verify_webhook_signature(raw, signature):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid signature")
    try:
        body = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid JSON")
    messages = parse_incoming_text_messages(body)
    for msg in messages:
        wa_id = msg["wa_id"]
        try:
            await _handle_whatsapp_incoming_message(
                wa_id=wa_id,
                profile_name=msg.get("profile_name") or "",
                text=msg.get("text") or "",
            )
        except WhatsAppCloudError as e:
            logger.warning("WhatsApp reply failed wa_id={}: {}", wa_id, e)
        except Exception:
            logger.exception("WhatsApp incoming handler failed wa_id={}", wa_id)
    return {"ok": True}


@landing_router.post("/auth/logout")
async def landing_logout(request: Request):
    resp = JSONResponse(content={"success": True})
    _clear_landing_cookie(request, resp)
    return resp


@landing_router.get("/auth/me")
async def landing_me(ctx: LandingCtx):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    user, site = pair
    return _landing_user_dict(user, site)


@landing_router.get("/config/tariffs")
async def landing_tariffs():
    out: list[dict[str, Any]] = []
    for tid, label, devices, first_only in TARIFF_PUBLIC:
        if tid not in dct_price:
            continue
        item: dict[str, Any] = {
            "id": tid,
            "label": label,
            "price": dct_price[tid],
            "devices": devices,
        }
        if first_only:
            item["first_payment_only"] = True
        out.append(item)
    return out


@landing_router.get("/config/traffic-packages")
async def landing_traffic_packages():
    out: list[dict[str, Any]] = []
    for gb, price in WL_TRAFFIC_TARIFFS.items():
        out.append({"gb": gb, "price": price})
    return out


@landing_router.get("/user/wl-traffic")
async def landing_user_wl_traffic(ctx: LandingCtx):
    user, _site = await _landing_user_pair(ctx)
    billing_uid = int(user.user_id)
    trafic_wl, limit_wl = await sql.get_wl_limits(billing_uid)
    used_gb = await get_wl_used_gb_for_user(x3, billing_uid, trafic_wl)
    limit_gb = round(float(limit_wl or 0.0), 2)
    used_gb = round(float(used_gb or 0.0), 2)
    remaining_gb = max(0.0, round(limit_gb - used_gb, 2))
    return {
        "limit_gb": limit_gb,
        "used_gb": used_gb,
        "remaining_gb": remaining_gb,
        "limit_exhausted": limit_gb > 0 and used_gb >= limit_gb,
    }


@landing_router.post("/payments/create")
async def landing_payments_create(ctx: LandingCtx, body: CreatePaymentIn, request: Request):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    user, _site = pair
    billing_user_id = int(user.user_id)
    tariff_id = body.tariff_id
    if tariff_id not in dct_price:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unknown tariff")
    _reject_mobile_purchase(tariff_id)
    desc_key, duration_str, white = _tariff_parts(tariff_id)
    price = dct_price[tariff_id]
    if billing_user_id in ADMIN_IDS:
        price = 1
    description = (
        f"Подписка в подарок {dct_desc[desc_key]}" if body.is_gift else dct_desc[desc_key]
    )
    site_uname = ctx.get("username") if isinstance(ctx.get("username"), str) else None

    if body.method == "sbp":
        result = await _create_landing_sbp_checkout(
            billing_user_id=billing_user_id,
            duration_str=duration_str,
            price=int(price),
            description=description,
            white=white,
            is_gift=body.is_gift,
            telegram_username=site_uname,
        )
    elif body.method == "card":
        if not API_FREEKASSA or SHOP_ID_FREEKASSA is None:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "FreeKassa is not configured")
        result = await pay_site(
            val=str(price),
            des=description,
            billing_user_id=billing_user_id,
            duration=duration_str,
            white=white,
            is_gift=body.is_gift,
            kind="card",
            telegram_username=site_uname,
            payload_source=SITE,
        )
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unknown payment method")

    if result.get("status") == "rate_limited":
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            lexicon["payment_too_many_pending"].format(PAYMENT_MAX_PENDING_PER_USER),
        )
    ok = result.get("status") in ("pending", "PENDING") and bool(result.get("url"))
    if not ok:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Не удалось создать платёж")
    return {
        "payment_url": result.get("url") or "",
        "payment_id": result.get("id") or "",
    }


@landing_router.post("/payments/create-traffic")
async def landing_payments_create_traffic(
    ctx: LandingCtx, body: CreateTrafficPaymentIn, request: Request
):
    user, _site = await _landing_user_pair(ctx)
    billing_user_id = int(user.user_id)
    gb = body.gb.strip()
    if gb not in WL_TRAFFIC_TARIFFS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unknown traffic package")

    price = _wl_traffic_site_price(gb, billing_user_id)
    duration_str = f"traffic{gb}"
    description = f"Пакет трафика {gb} GB"
    site_uname = ctx.get("username") if isinstance(ctx.get("username"), str) else None

    if body.method == "sbp":
        result = await _create_landing_sbp_checkout(
            billing_user_id=billing_user_id,
            duration_str=duration_str,
            price=int(price),
            description=description,
            white=False,
            is_gift=False,
            telegram_username=site_uname,
        )
    elif body.method == "card":
        if not API_FREEKASSA or SHOP_ID_FREEKASSA is None:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "FreeKassa is not configured")
        result = await pay_site(
            val=str(price),
            des=description,
            billing_user_id=billing_user_id,
            duration=duration_str,
            white=False,
            is_gift=False,
            kind="card",
            telegram_username=site_uname,
            payload_source=SITE,
        )
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unknown payment method")

    if result.get("status") == "rate_limited":
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            lexicon["payment_too_many_pending"].format(PAYMENT_MAX_PENDING_PER_USER),
        )
    ok = result.get("status") in ("pending", "PENDING") and bool(result.get("url"))
    if not ok:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Не удалось создать платёж")
    return {
        "payment_url": result.get("url") or "",
        "payment_id": result.get("id") or "",
    }


@landing_router.get("/user/password-status")
async def landing_password_status(ctx: LandingCtx):
    _user, site = await _landing_user_pair(ctx)
    return {"has_password": bool(site.password)}


@landing_router.post("/user/set-password")
async def landing_set_password(ctx: LandingCtx, body: SetPasswordIn):
    _validate_password_pair(body.password, body.password_confirm)
    user, site = await _landing_user_pair(ctx)
    if site.password:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль уже установлен")
    ok = await sql.set_landing_password_by_internal_id(int(user.id), _hash_password(body.password))
    if not ok:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось сохранить пароль")
    return {"success": True, "has_password": True}


@landing_router.post("/user/change-password")
async def landing_change_password(ctx: LandingCtx, body: ChangePasswordIn):
    user, site = await _landing_user_pair(ctx)
    if not site.password:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль не установлен")
    if not _verify_password(body.current_password, site.password):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный текущий пароль")
    _validate_password_pair(body.new_password, body.password_confirm)
    ok = await sql.set_landing_password_by_internal_id(int(user.id), _hash_password(body.new_password))
    if not ok:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось сохранить пароль")
    return {"success": True, "has_password": True}


@landing_router.post("/user/remove-password")
async def landing_remove_password(ctx: LandingCtx, body: RemovePasswordIn):
    user, site = await _landing_user_pair(ctx)
    if not site.password:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль не установлен")
    if not _verify_password(body.current_password, site.password):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный текущий пароль")
    ok = await sql.clear_landing_password_by_internal_id(int(user.id))
    if not ok:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось удалить пароль")
    return {"success": True, "has_password": False}


@landing_router.get("/user/subscription")
async def landing_user_subscription(ctx: LandingCtx):
    user, _site = await _landing_user_pair(ctx)
    panel_un = await _landing_panel_username(ctx)
    result_pro = await x3.activ(panel_un)
    active, expires = _activ_block(result_pro)
    tickets = _landing_raffle_tickets(user)
    plan_name = _landing_subscription_plan_name(user, active)
    has_paid = bool(getattr(user, "reserve_field", False))
    return {
        "active": active,
        "expires": expires,
        "plan_name": plan_name,
        "has_paid_subscription": has_paid,
        "in_panel": bool(getattr(user, "in_panel", False)),
        "stamp": (getattr(user, "stamp", None) or "").strip() or None,
        "trial_available": not bool(getattr(user, "in_panel", False)),
        "tickets": tickets,
        "raffle_tickets": tickets,
        "pro": {
            "active": active,
            "expires": expires,
            "plan_name": plan_name,
        },
    }


@landing_router.get("/user/keys")
async def landing_user_keys(ctx: LandingCtx):
    panel_un = await _landing_panel_username(ctx)
    sub_url = await x3.sublink(panel_un)
    return {
        "subscription_url": sub_url or None,
        "pro_url": sub_url or None,
    }


async def activate_landing_free_trial(ctx: LandingCtx) -> dict[str, Any]:
    """Бесплатный триал 3 дня для любого пользователя (без привязки к метке)."""
    user, site = await _landing_user_pair(ctx)
    billing_uid = int(user.user_id)
    if user.in_panel:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Триал уже был активирован"},
        )
    panel_un = panel_username_for_site_user(billing_uid, False)
    existing_panel = await x3.get_user_by_username(panel_un)
    if existing_panel and existing_panel.get("response"):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Триал уже был активирован"},
        )
    em = site.email or ctx.get("username") or site.phone
    if not em:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Нет контакта в профиле")
    day = 3
    ok = await x3.add_client_site(day, str(em).strip().lower(), False, billing_uid)
    if not ok:
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Не удалось активировать триал",
        )
    logger.info("landing trial 3d user {} panel username={}", billing_uid, panel_un)
    result_active = await x3.activ(panel_un)
    time_str = result_active["time"]
    if await sql.get_user(billing_uid) is not None:
        await sql.update_in_panel(billing_uid)
    else:
        await sql.add_user(billing_uid, True)
    await sql.init_wl_trial_limits(billing_uid)
    sub_url = await x3.sublink(panel_un)
    await post_user_trial(billing_uid)
    try:
        await report_trial_to_metrica(billing_uid)
    except Exception as e:
        logger.warning("Yandex Metrica trial user {}: {}", billing_uid, e)
    return {
        "success": True,
        "expires": time_str,
        "subscription_url": sub_url or None,
    }


@landing_router.post("/trial/activate")
async def landing_trial_activate(ctx: LandingCtx, request: Request):
    del request
    return await activate_landing_free_trial(ctx)


@landing_router.get("/payments/{transaction_id}/status")
async def landing_payment_status(ctx: LandingCtx, transaction_id: str):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    billing_uid = int(pair[0].user_id)
    st = await sql.get_payment_by_transaction_id(transaction_id, billing_uid)
    if st is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return {"status": st}


@landing_router.get("/user/partner")
async def landing_user_partner(ctx: LandingCtx, request: Request):
    user, _site = await _landing_user_pair(ctx)
    billing_uid = int(user.user_id)
    referrals = await sql.select_partner_count(billing_uid)
    payments_sum = await sql.select_partner_referrals_payments_sum(billing_uid)
    balance = user.partner_balance or 0
    paid_out = user.partner_pay or 0
    total_earned = balance + paid_out
    site_base = (_site_url_from_request(request) or LANDING_SITE_URL).rstrip("/")
    partner_code = str(billing_uid)
    site_link = f"{site_base}/?start=partner_{partner_code}"
    telegram_link = f"{BOT_URL}?start=partner_{partner_code}"
    referrals_list = await sql.select_partner_referrals_list(billing_uid)
    return {
        "referrals": referrals,
        "payments_sum": payments_sum,
        "balance": balance,
        "paid_out": paid_out,
        "total_earned": total_earned,
        "percent": PARTNER_PROCENT,
        "min_withdraw": PARTNER_MIN,
        "can_withdraw": balance >= PARTNER_MIN,
        "partner_code": partner_code,
        "site_link": site_link,
        "telegram_link": telegram_link,
        "support_url": SUPPORT_URL,
        "referrals_list": referrals_list,
    }
