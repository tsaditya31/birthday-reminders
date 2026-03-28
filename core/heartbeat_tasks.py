"""
Heartbeat tasks — each function runs once per heartbeat tick.
All are fire-and-forget; exceptions are caught by the heartbeat loop.
"""

import logging
from datetime import date, datetime, timezone
from html import escape

import anthropic

from config import settings
from db.store import (
    get_ambiguous_items,
    get_calendar_suggestable_items,
    get_heartbeat_state,
    get_items_due_soon,
    get_new_urgent_items,
    insert_pending_clarification,
    is_proactive_sent,
    mark_proactive_sent,
    set_heartbeat_state,
    upsert_action_item,
    upsert_birthday,
)
from notifier.telegram_notifier import send_message

logger = logging.getLogger(__name__)


# ── Task 1: Auto-crawl ──────────────────────────────────────────────────────

def task_auto_crawl():
    """Crawl action emails (1 day back). Crawl birthday emails once per day."""
    from core.action_extractor import extract_action_items
    from core.birthday_extractor import extract_birthdays
    from core.preferences import get_blocked_senders
    from crawler.gmail_crawler import crawl_action_emails, crawl_emails

    # Always crawl action emails (1 day back for freshness)
    raw_actions = crawl_action_emails(days_back=1, max_per_query=50)
    if raw_actions:
        blocked = get_blocked_senders()
        if blocked:
            raw_actions = [e for e in raw_actions if not any(b in e.sender.lower() for b in blocked)]
        if raw_actions:
            actions = extract_action_items(raw_actions)
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
            logger.info("Auto-crawl: %d action emails → %d items", len(raw_actions), len(actions))

    # Crawl birthday emails once per day
    today_str = date.today().isoformat()
    last_birthday_crawl = get_heartbeat_state("last_birthday_crawl_date")
    if last_birthday_crawl != today_str:
        raw_birthdays = crawl_emails(max_per_query=200)
        if raw_birthdays:
            birthdays = extract_birthdays(raw_birthdays)
            for b in birthdays:
                upsert_birthday(
                    name=b.name,
                    birth_month=b.birth_month,
                    birth_day=b.birth_day,
                    classification=b.classification,
                    birth_year=b.birth_year,
                    email_source=b.email_source,
                    age_at_extraction=b.age_at_extraction,
                )
            logger.info("Auto-crawl: %d birthday emails → %d birthdays", len(raw_birthdays), len(birthdays))
        set_heartbeat_state("last_birthday_crawl_date", today_str)


# ── Task 2: Proactive alerts ────────────────────────────────────────────────

def task_proactive_alerts():
    """Send alerts for new urgent/high items and items due within 24h."""
    if not settings.proactive_alerts_enabled:
        return

    # New urgent/high items
    new_urgent = get_new_urgent_items(since_hours=int(settings.heartbeat_interval_hours * 2) or 4)
    for item in new_urgent:
        priority_emoji = "🚨" if item["priority"] == "urgent" else "⚡"
        msg = (
            f"{priority_emoji} <b>New {item['priority']} item</b>\n"
            f"• {escape(item['title'])}"
        )
        if item.get("due_date"):
            msg += f"\n📅 Due: {item['due_date']}"
        if item.get("description"):
            desc = item["description"][:200]
            msg += f"\n<i>{escape(desc)}</i>"
        send_message(msg)
        mark_proactive_sent("urgent_alert", action_item_id=item["id"])

    # Items due within 24h
    due_soon = get_items_due_soon(hours=24)
    for item in due_soon:
        msg = (
            f"⏰ <b>Due soon</b>\n"
            f"• {escape(item['title'])}"
        )
        if item.get("due_date"):
            msg += f" — {item['due_date']}"
        if item.get("due_time"):
            msg += f" at {item['due_time']}"
        send_message(msg)
        mark_proactive_sent("due_soon", action_item_id=item["id"])


# ── Task 3: Clarifications ──────────────────────────────────────────────────

def task_clarifications():
    """Ask about ambiguous items (low confidence or missing due dates). Max 2 per tick."""
    ambiguous = get_ambiguous_items()
    asked = 0

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    for item in ambiguous:
        if asked >= settings.max_clarifications_per_tick:
            break

        # Build context for Claude to generate a natural question
        issues = []
        if item.get("confidence") and item["confidence"] < 0.65:
            issues.append(f"low confidence ({item['confidence']:.0%})")
        if not item.get("due_date"):
            issues.append("missing due date")

        prompt = (
            f"Generate a short, friendly Telegram message asking the user to clarify "
            f"an action item. Issues: {', '.join(issues)}.\n\n"
            f"Item: {item['title']}\n"
            f"Type: {item.get('type', 'unknown')}\n"
            f"Description: {(item.get('description') or 'none')[:300]}\n\n"
            f"Keep it under 3 sentences. Use Telegram HTML formatting. "
            f"Ask specifically about what's ambiguous."
        )

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=200,
            system="You generate clarification questions for a personal assistant bot. Be concise and friendly.",
            messages=[{"role": "user", "content": prompt}],
        )
        question = response.content[0].text.strip()

        question_type = "missing_date" if not item.get("due_date") else "low_confidence"
        insert_pending_clarification(
            action_item_id=item["id"],
            question_type=question_type,
            question_text=question,
        )
        send_message(question)
        asked += 1
        logger.info("Clarification sent for item %d: %s", item["id"], question_type)


# ── Task 4: Calendar suggestions ────────────────────────────────────────────

def task_calendar_suggestions():
    """Suggest calendar events for appointment/meeting items with dates."""
    items = get_calendar_suggestable_items()
    if not items:
        return

    item = items[0]  # Max 1 per tick
    time_str = f" at {item['due_time']}" if item.get("due_time") else ""
    msg = (
        f"📅 <b>Calendar suggestion</b>\n"
        f"I found an upcoming {escape(item.get('type', 'event'))}:\n"
        f"• <b>{escape(item['title'])}</b>\n"
        f"• Date: {item['due_date']}{time_str}\n\n"
        f"Would you like me to add this to your Google Calendar? "
        f"Reply with <b>yes</b> or <b>no</b>."
    )

    insert_pending_clarification(
        action_item_id=item["id"],
        question_type="calendar_suggestion",
        question_text=msg,
    )
    send_message(msg)
    mark_proactive_sent("calendar_suggestion", action_item_id=item["id"])
    logger.info("Calendar suggestion sent for item %d: %s", item["id"], item["title"])


# ── Task 5: Daily digest ────────────────────────────────────────────────────

def task_daily_digest():
    """Send daily digest if current hour is within digest window and not yet sent today."""
    now = datetime.now(timezone.utc)
    # Use a simple hour check (UTC). User can adjust digest_hour in config.
    current_hour = now.hour
    if not (settings.digest_hour_start <= current_hour <= settings.digest_hour_end):
        return

    today_str = date.today().isoformat()
    last_digest = get_heartbeat_state("last_digest_date")
    if last_digest == today_str:
        return

    from core.digest_engine import build_daily_digest

    digest = build_daily_digest(dry_run=False)
    if digest:
        send_message(digest)
        logger.info("Daily digest sent.")
    else:
        logger.info("Daily digest: nothing actionable today.")

    set_heartbeat_state("last_digest_date", today_str)
