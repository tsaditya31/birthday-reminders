"""
Tool handler functions for the agent loop.
Each function wraps existing db/store.py + crawler code and returns a dict.
"""

import logging
from datetime import date, timedelta
from html import escape

from config import settings
from db.store import (
    add_learned_query,
    add_preference,
    dismiss_action_item,
    dismiss_birthday,
    find_action_item_by_title,
    find_birthday_by_name,
    get_action_items_between,
    get_active_preferences,
    get_upcoming_birthdays,
    insert_feedback,
    upsert_action_item,
)

logger = logging.getLogger(__name__)

# Pending calendar event awaiting user confirmation
_pending_calendar_event: dict | None = None


def tool_get_upcoming_birthdays(days_ahead: int = 30) -> dict:
    """Query birthdays within the next N days."""
    items = get_upcoming_birthdays(days_ahead=days_ahead)
    today = date.today()
    results = []
    for b in items:
        try:
            this_year = date(today.year, b["birth_month"], b["birth_day"])
        except ValueError:
            this_year = date(today.year, b["birth_month"], min(b["birth_day"], 28))
        delta = (this_year - today).days
        if delta < 0:
            try:
                this_year = date(today.year + 1, b["birth_month"], b["birth_day"])
            except ValueError:
                this_year = date(today.year + 1, b["birth_month"], min(b["birth_day"], 28))
            delta = (this_year - today).days
        age_info = None
        if b.get("birth_year"):
            age_info = today.year - b["birth_year"]
        results.append({
            "name": b["name"],
            "birth_month": b["birth_month"],
            "birth_day": b["birth_day"],
            "days_away": delta,
            "turning_age": age_info,
            "classification": b.get("classification"),
        })
    return {"birthdays": results, "count": len(results)}


def tool_get_action_items(start_date: str | None = None, end_date: str | None = None) -> dict:
    """Query action items within a date range."""
    today = date.today()
    if not start_date:
        start_date = today.isoformat()
    if not end_date:
        end_date = (today + timedelta(days=30)).isoformat()
    items = get_action_items_between(start_date, end_date)
    results = []
    for item in items:
        results.append({
            "id": item["id"],
            "type": item["type"],
            "title": item["title"],
            "description": item.get("description"),
            "due_date": item.get("due_date"),
            "due_time": item.get("due_time"),
            "priority": item.get("priority"),
            "category": item.get("category"),
        })
    return {"action_items": results, "count": len(results), "start_date": start_date, "end_date": end_date}


def tool_list_preferences(category: str | None = None) -> dict:
    """Show active user preferences/rules."""
    prefs = get_active_preferences(category=category)
    results = []
    for p in prefs:
        results.append({
            "id": p["id"],
            "category": p["category"],
            "rule_text": p["rule_text"],
            "created_at": str(p.get("created_at", "")),
        })
    return {"preferences": results, "count": len(results)}


def tool_dismiss_birthday(name: str) -> dict:
    """Remove a birthday by person name."""
    match = find_birthday_by_name(name)
    if not match:
        return {"success": False, "error": f"No birthday found matching '{name}'."}
    dismiss_birthday(match["id"])
    return {
        "success": True,
        "dismissed": {"name": match["name"], "birth_month": match["birth_month"], "birth_day": match["birth_day"]},
    }


def tool_dismiss_action_item(title: str) -> dict:
    """Dismiss an action item by title search."""
    match = find_action_item_by_title(title)
    if not match:
        return {"success": False, "error": f"No action item found matching '{title}'."}
    dismiss_action_item(match["id"])
    return {"success": True, "dismissed": {"title": match["title"], "type": match.get("type")}}


def tool_add_preference(category: str, rule_text: str) -> dict:
    """Add an extraction rule or sender filter."""
    pref_id = add_preference(category, rule_text)
    return {"success": True, "preference_id": pref_id, "category": category, "rule_text": rule_text}


def tool_submit_feedback(
    feedback_type: str,
    title: str | None = None,
    description: str | None = None,
    corrected_type: str | None = None,
    corrected_date: str | None = None,
) -> dict:
    """Record feedback: useful, not_useful, missed, or correct."""
    if feedback_type in ("useful", "not_useful"):
        if not title:
            return {"success": False, "error": "Please specify which item (title)."}
        match = find_action_item_by_title(title)
        if not match:
            return {"success": False, "error": f"No action item found matching '{title}'."}
        insert_feedback(
            feedback_type=feedback_type,
            action_item_id=match["id"],
            email_message_id=match.get("email_message_id"),
            original_type=match.get("type"),
            user_comment=description,
        )
        return {"success": True, "feedback_type": feedback_type, "item_title": match["title"]}

    if feedback_type == "missed":
        if not description:
            return {"success": False, "error": "Please describe what was missed."}
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        query_msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=100,
            system="Generate a single Gmail search query to find the described email. Return ONLY the query string.",
            messages=[{"role": "user", "content": f"Find an email about: {description}"}],
        )
        gmail_query = query_msg.content[0].text.strip().strip('"')
        add_learned_query(gmail_query, source=f"feedback: {description}")
        insert_feedback(feedback_type="missed", user_comment=description)
        return {"success": True, "feedback_type": "missed", "learned_query": gmail_query}

    if feedback_type == "correct":
        if not title:
            return {"success": False, "error": "Please specify which item to correct."}
        match = find_action_item_by_title(title)
        if not match:
            return {"success": False, "error": f"No action item found matching '{title}'."}
        fb_type = "wrong_type" if corrected_type else "wrong_date"
        insert_feedback(
            feedback_type=fb_type,
            action_item_id=match["id"],
            email_message_id=match.get("email_message_id"),
            original_type=match.get("type"),
            corrected_type=corrected_type,
            original_date=match.get("due_date"),
            corrected_date=corrected_date,
        )
        # Apply correction directly
        if corrected_type or corrected_date:
            from datetime import datetime, timezone
            from db.store import get_db
            updates = {}
            if corrected_type:
                updates["type"] = corrected_type
            if corrected_date:
                updates["due_date"] = corrected_date
            now = datetime.now(timezone.utc).isoformat()
            with get_db() as conn:
                with conn.cursor() as cur:
                    set_clauses = ", ".join(f"{k} = %s" for k in updates)
                    values = list(updates.values()) + [now, match["id"]]
                    cur.execute(
                        f"UPDATE action_items SET {set_clauses}, updated_at = %s WHERE id = %s",
                        values,
                    )
        return {
            "success": True,
            "feedback_type": "correct",
            "item_title": match["title"],
            "corrected_type": corrected_type,
            "corrected_date": corrected_date,
        }

    return {"success": False, "error": f"Unknown feedback type: {feedback_type}"}


def tool_crawl_emails_now(days_back: int = 60) -> dict:
    """On-demand Gmail crawl + extraction."""
    from crawler.gmail_crawler import crawl_action_emails
    from core.action_extractor import extract_action_items
    from core.preferences import get_blocked_senders

    logger.info("On-demand crawl triggered (days_back=%d)", days_back)
    raw_emails = crawl_action_emails(days_back=days_back, max_per_query=50)
    if not raw_emails:
        return {"emails_found": 0, "items_extracted": 0, "message": "No new emails found."}

    blocked = get_blocked_senders()
    if blocked:
        raw_emails = [e for e in raw_emails if not any(b in e.sender.lower() for b in blocked)]

    actions = extract_action_items(raw_emails)
    saved = 0
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
        saved += 1
    return {"emails_found": len(raw_emails), "items_extracted": saved}


def tool_search_email(query: str, max_results: int = 5) -> dict:
    """Search Gmail for emails matching a query and return their contents."""
    from crawler.gmail_crawler import search_emails

    raw_emails = search_emails(query=query, max_results=max_results)
    if not raw_emails:
        return {"emails": [], "count": 0, "message": f"No emails found for '{query}'."}

    results = []
    for e in raw_emails:
        # Truncate body to keep tool result manageable for Claude
        body = e.full_body[:3000] if e.full_body else e.snippet
        results.append({
            "id": e.id,
            "subject": e.subject,
            "sender": e.sender,
            "date": e.date,
            "snippet": e.snippet,
            "body": body,
        })
    return {"emails": results, "count": len(results)}


def tool_get_calendar_events(start_date: str | None = None, end_date: str | None = None, query: str | None = None) -> dict:
    """Read events from Google Calendar."""
    from core.calendar_helper import list_events
    today = date.today()
    if not start_date:
        start_date = today.isoformat()
    if not end_date:
        end_date = (today + timedelta(days=7)).isoformat()
    events = list_events(start_date, end_date, query=query)
    return {"events": events, "count": len(events), "start_date": start_date, "end_date": end_date}


def tool_suggest_calendar_event(
    summary: str,
    date_str: str,
    time_str: str | None = None,
    duration_minutes: int = 60,
    description: str | None = None,
    all_day: bool = False,
) -> dict:
    """Format a calendar event preview and store it pending confirmation."""
    global _pending_calendar_event
    _pending_calendar_event = {
        "summary": summary,
        "date": date_str,
        "time": time_str,
        "duration_minutes": duration_minutes,
        "description": description,
        "all_day": all_day,
    }
    return {
        "pending": True,
        "preview": _pending_calendar_event,
        "message": "Event preview ready. Ask the user to confirm before creating.",
    }


def get_pending_calendar_event() -> dict | None:
    return _pending_calendar_event


def clear_pending_calendar_event():
    global _pending_calendar_event
    _pending_calendar_event = None


def confirm_calendar_event() -> dict:
    """Create the pending calendar event."""
    global _pending_calendar_event
    if not _pending_calendar_event:
        return {"success": False, "error": "No pending calendar event."}
    from core.calendar_helper import create_event
    evt = _pending_calendar_event
    result = create_event(
        summary=evt["summary"],
        date_str=evt["date"],
        time_str=evt.get("time"),
        duration_minutes=evt.get("duration_minutes", 60),
        description=evt.get("description"),
        all_day=evt.get("all_day", False),
    )
    _pending_calendar_event = None
    return result


# Registry for the agent to dispatch tool calls
TOOL_HANDLERS = {
    "get_upcoming_birthdays": tool_get_upcoming_birthdays,
    "get_action_items": tool_get_action_items,
    "list_preferences": tool_list_preferences,
    "dismiss_birthday": tool_dismiss_birthday,
    "dismiss_action_item": tool_dismiss_action_item,
    "add_preference": tool_add_preference,
    "submit_feedback": tool_submit_feedback,
    "crawl_emails_now": tool_crawl_emails_now,
    "search_email": tool_search_email,
    "get_calendar_events": tool_get_calendar_events,
    "suggest_calendar_event": tool_suggest_calendar_event,
}


def execute_tool(name: str, arguments: dict) -> dict:
    """Dispatch a tool call by name. Returns the result dict."""
    handler = TOOL_HANDLERS.get(name)
    if not handler:
        return {"error": f"Unknown tool: {name}"}
    try:
        return handler(**arguments)
    except Exception as exc:
        logger.error("Tool %s failed: %s", name, exc)
        return {"error": str(exc)}
