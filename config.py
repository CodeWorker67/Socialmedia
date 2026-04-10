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
CHANEL_ID: Optional[int] = int(os.environ.get("CHANEL_ID"))
CRYPTOBOT_API_TOKEN: Optional[str] = os.environ.get("CRYPTOBOT_API_TOKEN")
PANEL_URL: Optional[str] = os.environ.get("PANEL_URL")
PANEL_API_TOKEN: Optional[str] = os.environ.get("PANEL_API_TOKEN")
BOT_URL: Optional[str] = os.environ.get("BOT_URL")
CHANEL_URL: Optional[str] = os.environ.get("CHANEL_URL")
SUPPORT_URL: Optional[str] = os.environ.get("SUPPORT_URL")
DOCUMENT_URL_1: Optional[str] = os.environ.get("DOCUMENT_URL_1")
DOCUMENT_URL_2: Optional[str] = os.environ.get("DOCUMENT_URL_2")
TRUE_SUB_LINK: Optional[str] = os.environ.get("TRUE_SUB_LINK")
MIRROR_SUB_LINK: Optional[str] = os.environ.get("MIRROR_SUB_LINK")
SHORT_UUID_SECRET: Optional[str] = os.environ.get("SHORT_UUID_SECRET")

