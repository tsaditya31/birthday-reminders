"""
Chat handler — parses user intent via Claude and queries the DB to build a response.
"""

import calendar
import json
import logging
import random
from collections import deque
from datetime import date, timedelta
from html import escape

import anthropic

from config import settings
from core.utils import strip_json_markdown
from db.store import (
    add_learned_query,
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
    insert_feedback,
    upsert_action_item,
)

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

# Rolling conversation history for context (user msg + assistant JSON response)
_history: deque[dict] = deque(maxlen=10)

# Track the last query range so "expand" / "show more" can widen it
_last_query: dict = {}  # {"intent": ..., "start_date": ..., "end_date": ...}


def _end_of_month(d: date) -> date:
    """Return the last day of the month for a given date."""
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def _next_month_end(d: date) -> date:
    """Return the last day of the month after the given date."""
    if d.month == 12:
        return date(d.year + 1, 1, 31)
    nm = d.month + 1
    return date(d.year, nm, calendar.monthrange(d.year, nm)[1])


SYSTEM_PROMPT = (
    "You are a personal assistant. Parse the user's message and return JSON with intent and parameters.\n"
    "You may see prior conversation messages for context. Use them to resolve references like "
    "\"expand that\", \"show me the whole month\", \"more details\", etc.\n\n"
    "Supported intents:\n"
    "  action_items, birthdays, expand, help, unknown,\n"
    "  delete_birthday, dismiss_action_item, add_preference, list_preferences,\n"
    "  feedback_useful, feedback_not_useful, feedback_missed, feedback_correct\n\n"
    'Use "expand" when the user wants to see MORE of what was just shown — e.g. "expand", '
    '"show more", "show the whole month", "what about next month too", "wider range".\n\n'
    "For action_items and birthdays, return EITHER:\n"
    '  - "days_ahead": <int> for relative queries like "this week", "next 30 days"\n'
    '  - "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD" for specific periods '
    'like "in March", "next month", "in April 2026"\n'
    "Prefer start_date/end_date over days_ahead when the user mentions a specific month or period.\n\n"
    'For delete_birthday: {"intent": "delete_birthday", "name": "<person name>"}\n'
    'For dismiss_action_item: {"intent": "dismiss_action_item", "title": "<search term>"}\n'
    'For add_preference: {"intent": "add_preference", "category": "extraction_rule"|"sender_filter", '
    '"rule_text": "<the rule>"}\n'
    '  - Use "extraction_rule" for rules about what to extract or ignore (e.g. "stop flagging shipping confirmations")\n'
    '  - Use "sender_filter" for blocking emails from specific senders (e.g. "ignore emails from noreply@amazon.com")\n'
    'For list_preferences: {"intent": "list_preferences"}\n\n'
    "Feedback intents:\n"
    'For feedback_useful: {"intent": "feedback_useful", "title": "<search term>"}\n'
    '  — user says something like "that dentist appointment was helpful"\n'
    'For feedback_not_useful: {"intent": "feedback_not_useful", "title": "<search term>"}\n'
    '  — user says something like "the Amazon shipping one was irrelevant"\n'
    'For feedback_missed: {"intent": "feedback_missed", "description": "<what was missed>"}\n'
    '  — user says something like "you missed an email from my lawyer about a filing deadline"\n'
    'For feedback_correct: {"intent": "feedback_correct", "title": "<search term>", '
    '"corrected_type": "<new type>", "corrected_date": "<YYYY-MM-DD or null>"}\n'
    '  — user says something like "the passport item should be renewal not deadline"\n\n'
    f"Today's date is {date.today().isoformat()}.\n"
    "Return ONLY valid JSON.\n"
    "Examples:\n"
    '  "action items this week" -> {"intent": "action_items", "days_ahead": 7}\n'
    '  "birthdays in March" -> {"intent": "birthdays", "start_date": "2026-03-01", "end_date": "2026-03-31"}\n'
    '  "remove John\'s birthday" -> {"intent": "delete_birthday", "name": "John"}\n'
    '  "dismiss the dentist appointment" -> {"intent": "dismiss_action_item", "title": "dentist"}\n'
    '  "stop flagging shipping confirmations" -> {"intent": "add_preference", "category": "extraction_rule", "rule_text": "Do not extract action items from shipping confirmation emails"}\n'
    '  "ignore emails from noreply@amazon.com" -> {"intent": "add_preference", "category": "sender_filter", "rule_text": "noreply@amazon.com"}\n'
    '  "what rules have I set?" -> {"intent": "list_preferences"}\n'
    '  "expand" / "show more" / "show the whole month" -> {"intent": "expand"}\n'
    '  "what about next month" (after seeing action items) -> {"intent": "expand"}\n'
    '  "that dentist reminder was helpful" -> {"intent": "feedback_useful", "title": "dentist"}\n'
    '  "the Amazon one was irrelevant" -> {"intent": "feedback_not_useful", "title": "Amazon"}\n'
    '  "you missed my lawyer email about a filing" -> {"intent": "feedback_missed", "description": "lawyer email about filing deadline"}\n'
    '  "the passport item should be renewal not deadline" -> {"intent": "feedback_correct", "title": "passport", "corrected_type": "renewal"}'
)


def _parse_intent(user_message: str) -> dict:
    messages = list(_history) + [{"role": "user", "content": user_message}]
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=256,
        system=SYSTEM_PROMPT,
        messages=messages,
    )
    raw = strip_json_markdown(message.content[0].text)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Claude returned non-JSON intent: %s", raw[:200])
        parsed = {"intent": "unknown"}

    # Record the exchange so follow-ups have context
    _history.append({"role": "user", "content": user_message})
    _history.append({"role": "assistant", "content": raw.strip()})
    return parsed


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


def _format_action_items(items: list[dict], start: str = "", end: str = "") -> str:
    if not items:
        if start and end:
            return f"No action items found for {start} to {end}."
        return "No upcoming action items found."
    header = "<b>Action Items"
    if start and end:
        header += f" ({start} to {end})"
    header += "</b>"
    lines = [header]
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
    "• <i>list preferences</i> — see your active rules\n"
    "• <i>feedback</i> — help me improve:\n"
    "  - \"that dentist reminder was helpful\"\n"
    "  - \"the Amazon one was irrelevant\"\n"
    "  - \"you missed my lawyer email about a filing\"\n"
    "  - \"the passport item should be renewal not deadline\"\n\n"
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
        today = date.today()
        if "start_date" in parsed and "end_date" in parsed:
            start_s, end_s = parsed["start_date"], parsed["end_date"]
        elif "days_ahead" in parsed:
            start_s = today.isoformat()
            end_s = (today + timedelta(days=int(parsed["days_ahead"]))).isoformat()
        else:
            # Default: rest of current month
            start_s = today.isoformat()
            end_s = _end_of_month(today).isoformat()
        items = get_action_items_between(start_s, end_s)
        _last_query.update(intent="action_items", start_date=start_s, end_date=end_s)
        return _format_action_items(list(items), start=start_s, end=end_s)

    if intent == "birthdays":
        today = date.today()
        if "start_date" in parsed and "end_date" in parsed:
            start = date.fromisoformat(parsed["start_date"])
            end = date.fromisoformat(parsed["end_date"])
        elif "days_ahead" in parsed:
            start = today
            end = today + timedelta(days=int(parsed["days_ahead"]))
        else:
            # Default: rest of current month
            start = today
            end = _end_of_month(today)
        days_ahead = max((end - today).days, 0)
        items = get_upcoming_birthdays(days_ahead=days_ahead)
        items = [
            b for b in items
            if _birthday_in_range(b["birth_month"], b["birth_day"], start, end)
        ]
        _last_query.update(intent="birthdays", start_date=start.isoformat(), end_date=end.isoformat())
        return _format_birthdays(list(items))

    if intent == "expand":
        if not _last_query:
            return "Nothing to expand yet. Try asking about your action items or birthdays first."
        # Widen: extend end_date to end of next month from the previous end_date
        prev_end = date.fromisoformat(_last_query["end_date"])
        new_end = _next_month_end(prev_end)
        start_s = _last_query.get("start_date", date.today().isoformat())
        end_s = new_end.isoformat()
        if _last_query["intent"] == "action_items":
            items = get_action_items_between(start_s, end_s)
            _last_query.update(end_date=end_s)
            return _format_action_items(list(items), start=start_s, end=end_s)
        elif _last_query["intent"] == "birthdays":
            start = date.fromisoformat(start_s)
            days_ahead = max((new_end - date.today()).days, 0)
            items = get_upcoming_birthdays(days_ahead=days_ahead)
            items = [
                b for b in items
                if _birthday_in_range(b["birth_month"], b["birth_day"], start, new_end)
            ]
            _last_query.update(end_date=end_s)
            return _format_birthdays(list(items))
        return "I can only expand action items or birthday queries."

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

    if intent == "feedback_useful":
        title = parsed.get("title", "")
        if not title:
            return "Which action item was helpful? Please describe it."
        match = find_action_item_by_title(title)
        if not match:
            return f"I couldn't find an action item matching \"{escape(title)}\"."
        insert_feedback(
            feedback_type="useful",
            action_item_id=match["id"],
            email_message_id=match.get("email_message_id"),
            original_type=match.get("type"),
        )
        return _conversational(
            [
                "Thanks! Noted that \"<b>{title}</b>\" was useful. I'll prioritize similar items.",
                "Good to know \"<b>{title}</b>\" was helpful — I'll learn from this.",
            ],
            title=escape(match["title"]),
        )

    if intent == "feedback_not_useful":
        title = parsed.get("title", "")
        if not title:
            return "Which action item was not useful? Please describe it."
        match = find_action_item_by_title(title)
        if not match:
            return f"I couldn't find an action item matching \"{escape(title)}\"."
        insert_feedback(
            feedback_type="not_useful",
            action_item_id=match["id"],
            email_message_id=match.get("email_message_id"),
            original_type=match.get("type"),
            user_comment=user_message,
        )
        return _conversational(
            [
                "Got it — \"<b>{title}</b>\" wasn't useful. I'll avoid flagging similar items.",
                "Noted! I'll be less aggressive about items like \"<b>{title}</b>\".",
            ],
            title=escape(match["title"]),
        )

    if intent == "feedback_missed":
        description = parsed.get("description", "")
        if not description:
            return "What did I miss? Please describe the email or action item."
        # Use Claude to generate a Gmail search query from the description
        query_msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=100,
            system="Generate a single Gmail search query to find the described email. Return ONLY the query string, nothing else.",
            messages=[{"role": "user", "content": f"Find an email about: {description}"}],
        )
        gmail_query = query_msg.content[0].text.strip().strip('"')
        # Store the learned query
        add_learned_query(gmail_query, source=f"feedback: {description}")
        # Record feedback
        insert_feedback(
            feedback_type="missed",
            user_comment=description,
        )
        # Run immediate targeted crawl
        from crawler.gmail_crawler import crawl_action_emails
        from core.action_extractor import extract_action_items
        from core.preferences import get_blocked_senders
        raw_emails = crawl_action_emails(days_back=60, max_per_query=50)
        found_count = 0
        if raw_emails:
            blocked = get_blocked_senders()
            if blocked:
                raw_emails = [e for e in raw_emails if not any(b in e.sender.lower() for b in blocked)]
            actions = extract_action_items(raw_emails)
            for item in actions:
                upsert_action_item(
                    type=item.type,
                    title=item.title,
                    email_message_id=item.email_message_id,
                    description=item.description,
                    due_date=item.due_date,
                    due_time=item.due_time,
                    email_subject=item.email_subject,
                    email_from=item.email_from,
                    priority=item.priority,
                    category=item.category,
                    confidence=item.confidence,
                    source_snippet=item.source_snippet,
                )
                found_count += 1
        if found_count > 0:
            return (
                f"Thanks for letting me know! I've added the search query \"{escape(gmail_query)}\" "
                f"and re-scanned your emails. Found <b>{found_count}</b> new action item(s). "
                "I'll use this query in future crawls too."
            )
        return (
            f"Thanks for the feedback! I've added the search query \"{escape(gmail_query)}\" "
            "for future crawls. No new items found right now, but I'll catch them next time."
        )

    if intent == "feedback_correct":
        title = parsed.get("title", "")
        if not title:
            return "Which action item needs correction? Please describe it."
        match = find_action_item_by_title(title)
        if not match:
            return f"I couldn't find an action item matching \"{escape(title)}\"."
        corrected_type = parsed.get("corrected_type")
        corrected_date = parsed.get("corrected_date")
        insert_feedback(
            feedback_type="wrong_type" if corrected_type else "wrong_date",
            action_item_id=match["id"],
            email_message_id=match.get("email_message_id"),
            original_type=match.get("type"),
            corrected_type=corrected_type,
            original_date=match.get("due_date"),
            corrected_date=corrected_date,
        )
        # Apply correction directly to the item
        updates = {}
        if corrected_type:
            updates["type"] = corrected_type
        if corrected_date:
            updates["due_date"] = corrected_date
        if updates:
            from db.store import get_db
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            with get_db() as conn:
                with conn.cursor() as cur:
                    set_clauses = ", ".join(f"{k} = %s" for k in updates)
                    values = list(updates.values()) + [now, match["id"]]
                    cur.execute(
                        f"UPDATE action_items SET {set_clauses}, updated_at = %s WHERE id = %s",
                        values,
                    )
        parts = []
        if corrected_type:
            parts.append(f"type → <b>{escape(corrected_type)}</b>")
        if corrected_date:
            parts.append(f"date → <b>{escape(corrected_date)}</b>")
        return (
            f"Updated \"<b>{escape(match['title'])}</b>\": {', '.join(parts)}. "
            "I'll remember this for future extractions."
        )

    if intent == "help":
        return _HELP_TEXT

    return (
        "Sorry, I didn't understand that. Try asking about your action items or upcoming birthdays. "
        "Send /help for examples."
    )
