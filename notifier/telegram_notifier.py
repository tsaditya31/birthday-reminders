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
        # Retry without HTML parse mode if the markup was rejected
        if exc.response.status_code == 400 and parse_mode:
            logger.info("Retrying without parse_mode...")
            payload.pop("parse_mode", None)
            try:
                response = httpx.post(url, json=payload, timeout=15)
                response.raise_for_status()
                logger.info("Telegram message sent (plain text fallback).")
                return True
            except Exception as retry_exc:
                logger.error("Plain text retry also failed: %s", retry_exc)
    except Exception as exc:
        logger.error("Telegram send failed: %s", exc)
    return False
