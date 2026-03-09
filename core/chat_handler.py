"""
Chat handler — parses user intent via Claude and queries the DB to build a response.
"""

import json
import logging
import random
from datetime import date, timedelta
from html import escape

import anthropic

from config import settings
from db.store import (
    add_preference,
    deactivate_preference,
    dismiss_action_item,
    dismiss_birthday,
    find_action_item_by_title,
    find_birthday_by_name,
    get_action_items_between,
    get_active_preferences,
    get_upcoming_action_items,
    get_upcoming_birthdays,
)

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

SYSTEM_PROMPT = (
    "You are a personal assistant. Parse the user's message and return JSON with intent and parameters.\n"
    "Supported intents:\n"
    "  action_items, birthdays, help, unknown,\n"
    "  delete_birthday, dismiss_action_item, add_preference, list_preferences\n\n"
    "For action_items and birthdays, return EITHER:\n"
    '  - "days_ahead": <int> for relative queries like "this week", "next 30 days"\n'
    '  - "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD" for specific periods '
    'like "in March", "next month", "in April 2026"\n\n'
    'For delete_birthday: {"intent": "delete_birthday", "name": "<person name>"}\n'
    'For dismiss_action_item: {"intent": "dismiss_action_item", "title": "<search term>"}\n'
    'For add_preference: {"intent": "add_preference", "category": "extraction_rule"|"sender_filter", '
    '"rule_text": "<the rule>"}\n'
    '  - Use "extraction_rule" for rules about what to extract or ignore (e.g. "stop flagging shipping confirmations")\n'
    '  - Use "sender_filter" for blocking emails from specific senders (e.g. "ignore emails from noreply@amazon.com")\n'
    'For list_preferences: {"intent": "list_preferences"}\n\n'
    f"Today's date is {date.today().isoformat()}.\n"
    "Return ONLY valid JSON.\n"
    "Examples:\n"
    '  "action items this week" -> {"intent": "action_items", "days_ahead": 7}\n'
    '  "birthdays in March" -> {"intent": "birthdays", "start_date": "2026-03-01", "end_date": "2026-03-31"}\n'
    '  "remove John\'s birthday" -> {"intent": "delete_birthday", "name": "John"}\n'
    '  "dismiss the dentist appointment" -> {"intent": "dismiss_action_item", "title": "dentist"}\n'
    '  "stop flagging shipping confirmations" -> {"intent": "add_preference", "category": "extraction_rule", "rule_text": "Do not extract action items from shipping confirmation emails"}\n'
    '  "ignore emails from noreply@amazon.com" -> {"intent": "add_preference", "category": "sender_filter", "rule_text": "noreply@amazon.com"}\n'
    '  "what rules have I set?" -> {"intent": "list_preferences"}'
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


def _birthday_in_range(birth_month: int, birth_day: int, start: date, end: date) -> bool:
    """Check if a birthday (month/day) falls within the given date range."""
    year = start.year
    try:
        bday = date(year, birth_month, birth_day)
    except ValueError:
        bday = date(year, birth_month, min(birth_day, 28))
    if bday < start:
        try:
            bday = date(year + 1, birth_month, birth_day)
        except ValueError:
            bday = date(year + 1, birth_month, min(birth_day, 28))
    return start <= bday <= end


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


def _conversational(templates: list[str], **kwargs) -> str:
    return random.choice(templates).format(**kwargs)


_HELP_TEXT = (
    "<b>What I can help with:</b>\n"
    "• <i>action items</i> — ask about upcoming tasks, appointments, deadlines\n"
    "• <i>birthdays</i> — ask about upcoming birthdays\n"
    "• <i>remove a birthday</i> — e.g. \"remove John's birthday\"\n"
    "• <i>dismiss an action item</i> — e.g. \"dismiss the dentist appointment\"\n"
    "• <i>set preferences</i> — e.g. \"stop flagging shipping confirmations\"\n"
    "• <i>list preferences</i> — see your active rules\n\n"
    "Examples:\n"
    '  "what are my action items this week?"\n'
    '  "action items for March"\n'
    '  "any birthdays coming up?"\n'
    '  "remove Sarah\'s birthday"\n'
    '  "ignore emails from noreply@amazon.com"\n'
    '  "what rules have I set?"'
)


def handle_message(user_message: str) -> str:
    """Parse user intent and return an HTML-formatted reply string."""
    logger.info("Handling message: %s", user_message[:100])
    parsed = _parse_intent(user_message)
    intent = parsed.get("intent", "unknown")

    if intent == "action_items":
        if "start_date" in parsed and "end_date" in parsed:
            items = get_action_items_between(parsed["start_date"], parsed["end_date"])
        else:
            days_ahead = int(parsed.get("days_ahead", 7))
            items = get_action_items_between(
                date.today().isoformat(),
                (date.today() + timedelta(days=days_ahead)).isoformat(),
            )
        return _format_action_items(list(items))

    if intent == "birthdays":
        if "start_date" in parsed and "end_date" in parsed:
            # Convert date range to days_ahead from today
            end = date.fromisoformat(parsed["end_date"])
            start = date.fromisoformat(parsed["start_date"])
            today = date.today()
            days_ahead = max((end - today).days, 0)
            items = get_upcoming_birthdays(days_ahead=days_ahead)
            # Filter to only include birthdays within the requested range
            items = [
                b for b in items
                if _birthday_in_range(b["birth_month"], b["birth_day"], start, end)
            ]
        else:
            days_ahead = int(parsed.get("days_ahead", 14))
            items = get_upcoming_birthdays(days_ahead=days_ahead)
        return _format_birthdays(list(items))

    if intent == "delete_birthday":
        name = parsed.get("name", "")
        if not name:
            return "Which birthday should I remove? Please include the person's name."
        match = find_birthday_by_name(name)
        if not match:
            return f"I couldn't find a birthday matching \"{escape(name)}\"."
        dismiss_birthday(match["id"])
        return _conversational(
            [
                "Done! I've removed <b>{name}</b>'s birthday ({m}/{d}).",
                "Got it — <b>{name}</b>'s birthday ({m}/{d}) has been removed.",
                "Removed <b>{name}</b>'s birthday ({m}/{d}). It won't show up anymore.",
            ],
            name=escape(match["name"]),
            m=match["birth_month"],
            d=match["birth_day"],
        )

    if intent == "dismiss_action_item":
        title = parsed.get("title", "")
        if not title:
            return "Which action item should I dismiss? Please describe it."
        match = find_action_item_by_title(title)
        if not match:
            return f"I couldn't find an action item matching \"{escape(title)}\"."
        dismiss_action_item(match["id"])
        return _conversational(
            [
                "Done! I've dismissed \"<b>{title}</b>\".",
                "Got it — \"<b>{title}</b>\" is dismissed. Won't bother you about it again.",
                "Dismissed \"<b>{title}</b>\". Out of sight, out of mind!",
            ],
            title=escape(match["title"]),
        )

    if intent == "add_preference":
        category = parsed.get("category", "extraction_rule")
        rule_text = parsed.get("rule_text", "")
        if not rule_text:
            return "What rule would you like me to follow? Please describe it."
        pref_id = add_preference(category, rule_text, source_msg=user_message)
        cat_label = "extraction rule" if category == "extraction_rule" else "sender filter"
        return _conversational(
            [
                "Saved! New {cat}: \"<i>{rule}</i>\". I'll follow this going forward.",
                "Got it — I've added a {cat}: \"<i>{rule}</i>\".",
                "Rule stored! {cat}: \"<i>{rule}</i>\". This will apply to future email processing.",
            ],
            cat=cat_label,
            rule=escape(rule_text),
        )

    if intent == "list_preferences":
        prefs = get_active_preferences()
        if not prefs:
            return "You don't have any active preferences set. You can tell me things like \"stop flagging shipping confirmations\" to add one."
        lines = ["<b>Your active preferences:</b>"]
        for p in prefs:
            cat_label = "Extraction rule" if p["category"] == "extraction_rule" else "Sender filter"
            lines.append(f"• [{cat_label}] <i>{escape(p['rule_text'])}</i> (#{p['id']})")
        return "\n".join(lines)

    if intent == "help":
        return _HELP_TEXT

    return (
        "Sorry, I didn't understand that. Try asking about your action items or upcoming birthdays. "
        "Send /help for examples."
    )
