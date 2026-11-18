"""WhatsApp Cloud API (Meta): отправка сообщений и разбор webhook."""
from __future__ import annotations

import hashlib
import hmac
from typing import Any, Optional

import aiohttp

from config import (
    WHATSAPP_ACCESS_TOKEN,
    WHATSAPP_API_VERSION,
    WHATSAPP_APP_SECRET,
    WHATSAPP_PHONE_NUMBER_ID,
)
from logging_config import logger


class WhatsAppCloudError(Exception):
    pass


def whatsapp_configured() -> bool:
    return bool(WHATSAPP_ACCESS_TOKEN and WHATSAPP_PHONE_NUMBER_ID)


def verify_webhook_signature(raw_body: bytes, signature_header: Optional[str]) -> bool:
    if not WHATSAPP_APP_SECRET:
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(
        WHATSAPP_APP_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature_header)


async def send_text_message(to_wa_id: str, text: str) -> None:
    if not whatsapp_configured():
        raise WhatsAppCloudError("WhatsApp API is not configured")
    url = (
        f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/"
        f"{WHATSAPP_PHONE_NUMBER_ID}/messages"
    )
    payload = {
        "messaging_product": "whatsapp",
        "to": str(to_wa_id),
        "type": "text",
        "text": {"body": text},
    }
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            if resp.status >= 400:
                body = await resp.text()
                logger.warning("WhatsApp send failed status={} body={}", resp.status, body[:500])
                raise WhatsAppCloudError(f"WhatsApp API error {resp.status}")


def parse_incoming_text_messages(body: dict[str, Any]) -> list[dict[str, Any]]:
    """Из payload Meta webhook извлекает входящие текстовые сообщения."""
    out: list[dict[str, Any]] = []
    for entry in body.get("entry") or []:
        for change in entry.get("changes") or []:
            value = change.get("value") or {}
            contacts = {c.get("wa_id"): c for c in (value.get("contacts") or []) if c.get("wa_id")}
            for msg in value.get("messages") or []:
                if msg.get("type") != "text":
                    continue
                wa_id = str(msg.get("from") or "")
                if not wa_id:
                    continue
                contact = contacts.get(wa_id) or {}
                profile = contact.get("profile") or {}
                text_obj = msg.get("text") or {}
                text = str(text_obj.get("body") or "").strip()
                out.append(
                    {
                        "wa_id": wa_id,
                        "profile_name": str(profile.get("name") or "").strip(),
                        "text": text,
                        "message_id": str(msg.get("id") or ""),
                    }
                )
    return out
