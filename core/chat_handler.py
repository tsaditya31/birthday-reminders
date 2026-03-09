"""
Chat handler — parses user intent via Claude and queries the DB to build a response.
"""

import json
import logging
from datetime import date
from html import escape

import anthropic

from config import settings
from db.store import get_upcoming_action_items, get_upcoming_birthdays

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

SYSTEM_PROMPT = (
    "You are a personal assistant. Parse the user's question about their calendar "
    "and return JSON with intent and parameters. "
    "Supported intents: action_items, birthdays, help, unknown. "
    'Return ONLY valid JSON, e.g. {"intent": "action_items", "days_ahead": 7}.'
)


def _parse_intent(user_message: str) -> dict:
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=256,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    raw = message.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError:
        logger.warning("Claude returned non-JSON intent: %s", raw[:200])
        return {"intent": "unknown"}


def _format_action_items(items: list[dict]) -> str:
    if not items:
        return "No upcoming action items found."
    lines = ["<b>Upcoming Action Items</b>"]
    for item in items:
        due = item.get("due_date") or "no date"
        time_part = f" {item['due_time']}" if item.get("due_time") else ""
        title = escape(item["title"])
        item_type = escape(item["type"])
        lines.append(f"• <b>{title}</b> [{item_type}] — {due}{time_part}")
        if item.get("description"):
            lines.append(f"  <i>{escape(item['description'])}</i>")
    return "\n".join(lines)


def _format_birthdays(items: list[dict]) -> str:
    if not items:
        return "No upcoming birthdays found."
    today = date.today()
    lines = ["<b>Upcoming Birthdays</b>"]
    for b in items:
        month, day = b["birth_month"], b["birth_day"]
        # Figure out days away
        try:
            this_year = date(today.year, month, day)
        except ValueError:
            this_year = date(today.year, month, min(day, 28))
        delta = (this_year - today).days
        if delta < 0:
            try:
                this_year = date(today.year + 1, month, day)
            except ValueError:
                this_year = date(today.year + 1, month, min(day, 28))
            delta = (this_year - today).days

        if delta == 0:
            when = "TODAY"
        elif delta == 1:
            when = "tomorrow"
        else:
            when = f"in {delta} days"

        year_info = f", turning {today.year - b['birth_year']}" if b.get("birth_year") else ""
        lines.append(f"• <b>{escape(b['name'])}</b> — {month}/{day} ({when}{year_info})")
    return "\n".join(lines)


_HELP_TEXT = (
    "<b>What I can help with:</b>\n"
    "• <i>action items</i> — ask about upcoming tasks, appointments, deadlines\n"
    "• <i>birthdays</i> — ask about upcoming birthdays\n\n"
    "Examples:\n"
    "  \"what are my action items this week?\"\n"
    "  \"any birthdays coming up?\"\n"
    "  \"show me birthdays in the next 30 days\""
)


def handle_message(user_message: str) -> str:
    """Parse user intent and return an HTML-formatted reply string."""
    logger.info("Handling message: %s", user_message[:100])
    parsed = _parse_intent(user_message)
    intent = parsed.get("intent", "unknown")

    if intent == "action_items":
        days_ahead = int(parsed.get("days_ahead", 7))
        items = get_upcoming_action_items(days_ahead=days_ahead)
        return _format_action_items(list(items))

    if intent == "birthdays":
        days_ahead = int(parsed.get("days_ahead", 14))
        items = get_upcoming_birthdays(days_ahead=days_ahead)
        return _format_birthdays(list(items))

    if intent == "help":
        return _HELP_TEXT

    return (
        "Sorry, I didn\u2019t understand that. Try asking about your action items or upcoming birthdays. "
        "Send /help for examples."
    )
