"""
Telegram notifier — sends messages via Bot API using httpx (outbound only).
"""

import logging

import httpx

from config import settings

logger = logging.getLogger(__name__)

_BASE = "https://api.telegram.org/bot{token}"


def send_message(text: str, parse_mode: str = "HTML") -> bool:
    url = f"{_BASE.format(token=settings.telegram_bot_token)}/sendMessage"
    payload = {
        "chat_id": settings.telegram_chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        response = httpx.post(url, json=payload, timeout=15)
        response.raise_for_status()
        logger.info("Telegram message sent successfully.")
        return True
    except httpx.HTTPStatusError as exc:
        logger.error("Telegram API error: %s — %s", exc.response.status_code, exc.response.text)
    except Exception as exc:
        logger.error("Telegram send failed: %s", exc)
    return False
