"""
Telegram bot — long-polling loop for inbound messages.
Only responds to messages from the configured chat_id (security: ignores all others).
Checks for due reminders every poll cycle.
"""

import logging
import time

import httpx

from config import settings
from core.chat_handler import handle_message
from db.store import get_due_reminders, mark_reminder_sent
from notifier.telegram_notifier import send_message

logger = logging.getLogger(__name__)

_BASE = f"https://api.telegram.org/bot{settings.telegram_bot_token}"
_POLL_INTERVAL = settings.bot_poll_interval
_TIMEOUT = 30       # long-poll timeout (seconds)


def _get_updates(offset: int) -> list[dict]:
    url = f"{_BASE}/getUpdates"
    try:
        resp = httpx.get(
            url,
            params={"offset": offset, "timeout": _TIMEOUT},
            timeout=_TIMEOUT + 5,
        )
        resp.raise_for_status()
        return resp.json().get("result", [])
    except Exception as exc:
        logger.warning("getUpdates error: %s", exc)
        return []


def _delete_webhook():
    """Remove any existing webhook so getUpdates (long-polling) works."""
    url = f"{_BASE}/deleteWebhook"
    try:
        resp = httpx.post(url, timeout=10)
        resp.raise_for_status()
        logger.info("deleteWebhook: %s", resp.json())
    except Exception as exc:
        logger.warning("deleteWebhook failed: %s", exc)


def _check_reminders():
    """Send any due reminders and mark them as sent."""
    try:
        due = get_due_reminders()
        for r in due:
            text = f"Reminder: {r['reminder_text']}"
            send_message(text, parse_mode="")
            mark_reminder_sent(r["id"])
            logger.info("Sent reminder #%s: %s", r["id"], r["reminder_text"][:60])
    except Exception as exc:
        logger.warning("Reminder check error: %s", exc)


def run_polling_loop():
    """Block forever, polling Telegram for new messages and replying."""
    _delete_webhook()
    logger.info("Telegram bot polling started (chat_id=%s)", settings.telegram_chat_id)
    offset = 0

    # Use the configured chat_id as the user_id for the agent loop
    user_id = str(settings.telegram_chat_id)

    while True:
        updates = _get_updates(offset)

        for update in updates:
            offset = update["update_id"] + 1
            msg = update.get("message")
            if not msg:
                continue

            chat_id = str(msg.get("chat", {}).get("id", ""))
            if chat_id != str(settings.telegram_chat_id):
                logger.warning("Ignoring message from unknown chat_id: %s", chat_id)
                continue

            text = msg.get("text", "").strip()
            if not text:
                continue

            logger.info("Received message: %s", text[:100])
            try:
                reply = handle_message(user_id, text)
            except Exception as exc:
                logger.error("chat_handler error: %s", exc)
                reply = "Sorry, something went wrong processing your request."

            send_message(reply, parse_mode="")

        # Check for due reminders every cycle
        _check_reminders()

        if not updates:
            time.sleep(_POLL_INTERVAL)
