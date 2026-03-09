"""
Digest engine — orchestrates action items and birthdays into a single daily
Telegram digest. Returns None when there is nothing actionable to send.
"""

import logging
from datetime import date, timedelta
from typing import Optional

from crawler.gmail_crawler import crawl_action_emails
from core.action_extractor import extract_action_items
from core.reminder_engine import get_birthday_alerts
from db.store import (
    get_upcoming_action_items,
    get_unnotified_urgent,
    mark_action_notified,
    mark_notified,
    upsert_action_item,
)

logger = logging.getLogger(__name__)


def _days_until_date(due_date_str: str) -> int:
    """Days from today to the given YYYY-MM-DD date string."""
    today = date.today()
    due = date.fromisoformat(due_date_str)
    return (due - today).days


def _format_appointment_line(row) -> str:
    days = _days_until_date(row["due_date"])
    time_str = f", {row['due_time']}" if row["due_time"] else ""
    if days == 0:
        timing = "Today"
    elif days == 1:
        timing = "Tomorrow"
    else:
        timing = f"In {days} days"
    return f"• {timing}: {row['title']}{time_str}"


def _format_deadline_line(row) -> str:
    days = _days_until_date(row["due_date"])
    if days == 0:
        timing = "Today"
    elif days == 1:
        timing = "Tomorrow"
    else:
        timing = f"In {days} days"
    sender = f" ({row['email_from'].split('<')[0].strip()})" if row["email_from"] else ""
    return f"• {timing}: {row['title']}{sender}"


def _format_urgent_line(row) -> str:
    sender = row["email_from"].split("<")[0].strip() if row["email_from"] else "Unknown"
    subject = row["email_subject"] or row["title"]
    return f'• {sender} — "{subject}"'


def _format_birthday_line(msg: str) -> str:
    # Strip HTML tags to get a plain one-liner for the digest section
    # Birthday messages are multi-line for annas_friend; use first line only
    first_line = msg.split("\n")[0]
    return f"• {first_line}"


def build_daily_digest(dry_run: bool = False) -> Optional[str]:
    """
    Full pipeline: crawl → extract → store → query → build HTML digest.
    Returns the digest string, or None if nothing is actionable today.
    """
    today = date.today()

    # ── Step 1: Crawl and extract new action emails ──────────────────────────
    logger.info("Crawling action emails…")
    raw_emails = crawl_action_emails(days_back=14)
    if raw_emails:
        logger.info("Extracting action items from %d emails…", len(raw_emails))
        action_items = extract_action_items(raw_emails)
        for item in action_items:
            upsert_action_item(
                type=item.type,
                title=item.title,
                email_message_id=item.email_message_id,
                description=item.description,
                due_date=item.due_date,
                due_time=item.due_time,
                email_subject=item.email_subject,
                email_from=item.email_from,
            )
    else:
        logger.info("No new action emails found.")

    # ── Step 2: Query DB for items to notify ─────────────────────────────────
    upcoming = get_upcoming_action_items(days_ahead=3)
    urgent_items = get_unnotified_urgent()

    # ── Step 3: Apply timing rules ────────────────────────────────────────────
    appointments_to_send: list = []
    deadlines_to_send: list = []
    appointments_to_mark: list[tuple] = []  # (id, flag)
    deadlines_to_mark: list[tuple] = []

    for row in upcoming:
        days = _days_until_date(row["due_date"])
        item_type = row["type"]

        if item_type == "appointment":
            if days == 0 and not row["notified_day"]:
                appointments_to_send.append(row)
                appointments_to_mark.append((row["id"], "day"))
            elif days == 2 and not row["notified_early"]:
                appointments_to_send.append(row)
                appointments_to_mark.append((row["id"], "early"))

        elif item_type == "deadline":
            if days == 0 and not row["notified_day"]:
                deadlines_to_send.append(row)
                deadlines_to_mark.append((row["id"], "day"))
            elif days == 3 and not row["notified_early"]:
                deadlines_to_send.append(row)
                deadlines_to_mark.append((row["id"], "early"))

    # ── Step 4: Birthday alerts ───────────────────────────────────────────────
    birthday_alerts = get_birthday_alerts()  # list of (msg, bid, flag)

    # ── Step 5: Bail if nothing to send ──────────────────────────────────────
    if not appointments_to_send and not deadlines_to_send and not urgent_items and not birthday_alerts:
        logger.info("Nothing actionable today.")
        return None

    # ── Step 6: Build HTML digest ─────────────────────────────────────────────
    day_label = today.strftime("%a %b %-d")
    lines = [f"📅 <b>Daily Digest — {day_label}</b>"]

    if appointments_to_send:
        lines.append("")
        lines.append("🏥 <b>Appointments</b>")
        for row in appointments_to_send:
            lines.append(_format_appointment_line(row))

    if deadlines_to_send:
        lines.append("")
        lines.append("⏰ <b>Deadlines</b>")
        for row in deadlines_to_send:
            lines.append(_format_deadline_line(row))

    if urgent_items:
        lines.append("")
        lines.append("📬 <b>Reply Needed</b>")
        for row in urgent_items:
            lines.append(_format_urgent_line(row))

    if birthday_alerts:
        lines.append("")
        lines.append("🎂 <b>Birthdays</b>")
        for msg, _bid, _flag in birthday_alerts:
            # For annas_friend the message may have Amazon links — include in full
            # For simple reminders, embed inline
            lines.append(_format_birthday_line(msg))
            # If message has more than one line (e.g. Amazon links), append rest indented
            extra_lines = msg.split("\n")[1:]
            for extra in extra_lines:
                if extra.strip():
                    lines.append(f"  {extra}")

    digest = "\n".join(lines)

    # ── Step 7: Mark notifications (skip in dry_run) ──────────────────────────
    if not dry_run:
        for item_id, flag in appointments_to_mark:
            mark_action_notified(item_id, flag)
        for item_id, flag in deadlines_to_mark:
            mark_action_notified(item_id, flag)
        for row in urgent_items:
            mark_action_notified(row["id"], "day")
        for _msg, bid, flag in birthday_alerts:
            mark_notified(bid, flag)

    return digest
