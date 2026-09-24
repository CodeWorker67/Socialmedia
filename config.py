from dotenv import load_dotenv
import os
from typing import Set, Optional

# Загрузка переменных окружения из .env файла
load_dotenv()

TG_TOKEN: Optional[str] = os.environ.get("TG_TOKEN")
ADMIN_IDS: Set[int] = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(', ')} if os.environ.get("ADMIN_IDS") else set()
_cid = os.environ.get("CHECKER_ID")
CHECKER_ID: Optional[int] = int(_cid) if _cid else None
CHECKER_IDS: Set[int] = {int(x) for x in os.environ.get("CHECKER_IDS", "").split(', ')} if os.environ.get("CHECKER_IDS") else set()
PLATEGA_API_KEY: Optional[str] = os.environ.get("PLATEGA_API_KEY")
PLATEGA_MERCHANT_ID: Optional[str] = os.environ.get("PLATEGA_MERCHANT_ID")
WATA_API_SBP_KEY: Optional[str] = os.environ.get("WATA_API_SBP_KEY")
WATA_API_CARD_KEY: Optional[str] = os.environ.get("WATA_API_CARD_KEY")
WATA_API_BASE: str = os.environ.get("WATA_API_BASE", "https://api.wata.pro/api/h2h").rstrip("/")
API_FREEKASSA: Optional[str] = (os.environ.get("API_FREEKASSA") or "").strip() or None
SHOP_ID_FREEKASSA: Optional[int] = (
    int(os.environ["SHOP_ID_FREEKASSA"]) if os.environ.get("SHOP_ID_FREEKASSA") else None
)
FREEKASSA_SERVER_IP: str = os.environ.get("FREEKASSA_SERVER_IP", "72.56.14.94")
CHANEL_ID: Optional[int] = int(os.environ.get("CHANEL_ID"))
CRYPTOBOT_API_TOKEN: Optional[str] = os.environ.get("CRYPTOBOT_API_TOKEN")
PANEL_URL: Optional[str] = os.environ.get("PANEL_URL")
PANEL_API_TOKEN: Optional[str] = os.environ.get("PANEL_API_TOKEN")
BOT_URL: Optional[str] = os.environ.get("BOT_URL")
CHANEL_URL: Optional[str] = os.environ.get("CHANEL_URL")
SUPPORT_URL: Optional[str] = os.environ.get("SUPPORT_URL")
PARTNER_PROCENT: int = int(os.environ.get("PARTNER_PROCENT", "20"))
PARTNER_MIN: int = int(os.environ.get("PARTNER_MIN", "500"))
PARTNER_SUPPORT_URL: str = (
    os.environ.get("PARTNER_SUPPORT_URL") or os.environ.get("SUPPORT_URL") or ""
).strip()
DOCUMENT_URL_1: str = (
    os.environ.get("DOCUMENT_URL_1")
    or "https://telegra.ph/Polzovatelskoe-soglashenie-04-01-19"
).strip()
DOCUMENT_URL_2: str = (
    os.environ.get("DOCUMENT_URL_2")
    or "https://telegra.ph/Politika-konfidencialnosti-08-01-83"
).strip()
TRUE_SUB_LINK: Optional[str] = os.environ.get("TRUE_SUB_LINK")
MIRROR_SUB_LINK: Optional[str] = os.environ.get("MIRROR_SUB_LINK")
SHORT_UUID_SECRET: Optional[str] = os.environ.get("SHORT_UUID_SECRET")

# Lead Tracker (POST /users/, /users/trial, /users/connected, /payments/)
LEAD_TRACKER_BASE: Optional[str] = (os.environ.get("LEAD_TRACKER_BASE") or "").strip() or None
LEAD_TRACKER_API_KEY: Optional[str] = (os.environ.get("LEAD_TRACKER_API_KEY") or "").strip() or None
LEAD_TRACKER_STAR_RUB_PER_STAR: str = os.environ.get("LEAD_TRACKER_STAR_RUB_PER_STAR", "1.0")

# Web API (web_api.py): сайт + страница подписки + uvicorn в main
WEB_API_PORT: int = int(os.environ.get("WEB_API_PORT", "8080"))
SUB_PAGE_API_KEY: Optional[str] = (os.environ.get("SUB_PAGE_API_KEY") or "").strip() or None
SUB_PAGE_CORS_ORIGINS: Optional[str] = os.environ.get("SUB_PAGE_CORS_ORIGINS")
PUBLIC_SITE_URL: str = (os.environ.get("PUBLIC_SITE_URL") or "").strip().rstrip("/")
# Публичный URL web_api (для callback Platega). Пример: https://bt.gosocial.run
WEB_API_PUBLIC_URL: str = (
    os.environ.get("WEB_API_PUBLIC_URL") or PUBLIC_SITE_URL or ""
).strip().rstrip("/")
JWT_SECRET: Optional[str] = os.environ.get("JWT_SECRET")
GOOGLE_CLIENT_ID: Optional[str] = os.environ.get("GOOGLE_CLIENT_ID")
PAYMENT_MAX_PENDING_PER_USER: int = int(os.environ.get("PAYMENT_MAX_PENDING_PER_USER", "8"))
THROTTLE_MAX_UPDATES: int = int(os.environ.get("THROTTLE_MAX_UPDATES", "25"))
THROTTLE_WINDOW_SEC: float = float(os.environ.get("THROTTLE_WINDOW_SEC", "8"))
# Тестовая цена оплаты с сайта для указанного email (FreeKassa СБП от 10 ₽).
SITE_TEST_EMAIL: str = (os.environ.get("SITE_TEST_EMAIL") or "open21vpn@gmail.com").strip().lower()
SITE_TEST_PRICE_RUB: int = int(os.environ.get("SITE_TEST_PRICE_RUB", "10"))
SMTP_HOST: Optional[str] = (os.environ.get("SMTP_HOST") or "").strip() or None
SMTP_PORT: int = int((os.environ.get("SMTP_PORT") or "587").strip())
SMTP_USER: Optional[str] = (os.environ.get("SMTP_USER") or "").strip() or None
SMTP_PASSWORD: Optional[str] = (os.environ.get("SMTP_PASSWORD") or "").strip() or None
SMTP_FROM: Optional[str] = (os.environ.get("SMTP_FROM") or "").strip() or None

# Unisender Go (HTTPS вместо SMTP): https://godocs.unisender.ru/web-api-ref
UNISENDER_GO_API_KEY: Optional[str] = (os.environ.get("UNISENDER_GO_API_KEY") or "").strip() or None
UNISENDER_GO_API_URL: str = (
    os.environ.get("UNISENDER_GO_API_URL") or "https://go1.unisender.ru/ru/transactional/api/v1"
).strip().rstrip("/")
UNISENDER_GO_FROM_NAME: str = (os.environ.get("UNISENDER_GO_FROM_NAME") or "SocialmediaVPN").strip()

# file_id картинки для рассылки /discount_push
DISCOUNT_PUSH_PHOTO_ID: str = (os.environ.get("DISCOUNT_PUSH_PHOTO_ID") or "AgACAgQAAxkBAAGLsp1qTOJ_gvKWp9to4yEw-h_u6MnQ2wACrg5rG0k0aFLtvGXwmsbjjQEAAwIAA3kAAzwE").strip()
