"""
Digest engine — orchestrates action items and birthdays into a single daily
Telegram digest. Returns None when there is nothing actionable to send.
"""

import logging
from datetime import date, timedelta
from html import escape
from typing import Optional

from core.utils import local_today

from core.reminder_engine import get_birthday_alerts
from db.store import (
    get_upcoming_action_items,
    get_unnotified_urgent,
    mark_action_notified,
    mark_notified,
)

logger = logging.getLogger(__name__)


def _days_until_date(due_date_str: str) -> int:
    """Days from today to the given YYYY-MM-DD date string."""
    today = local_today()
    due = date.fromisoformat(due_date_str)
    return (due - today).days


def _format_item_line(row) -> str:
    """Format a single action item line for the digest."""
    if row.get("due_date"):
        days = _days_until_date(row["due_date"])
        time_str = f", {row['due_time']}" if row.get("due_time") else ""
        if days == 0:
            timing = "Today"
        elif days == 1:
            timing = "Tomorrow"
        else:
            timing = f"In {days} days"
        line = f"• {timing}: {escape(row['title'])}{time_str}"
    else:
        line = f"• {escape(row['title'])}"
    item_type = row.get("type", "")
    category = row.get("category", "")
    tag = category or item_type
    if tag:
        line += f" [{escape(tag)}]"
    return line


def _format_birthday_line(msg: str) -> str:
    # Strip HTML tags to get a plain one-liner for the digest section
    # Birthday messages are multi-line for annas_friend; use first line only
    first_line = msg.split("\n")[0]
    return f"• {first_line}"


def _priority_early_days(priority: str) -> int:
    """How many days before due_date to send an early notification."""
    return {"urgent": 0, "high": 3, "normal": 2, "low": 0}.get(priority, 2)


def _should_notify(row: dict) -> Optional[str]:
    """Determine if a row should be notified now. Returns flag ('early'|'day') or None."""
    priority = row.get("priority") or "normal"
    if not row.get("due_date"):
        # No due date — notify urgent/high immediately if not yet notified
        if priority in ("urgent", "high") and not row["notified_day"]:
            return "day"
        return None
    days = _days_until_date(row["due_date"])
    if days == 0 and not row["notified_day"]:
        return "day"
    early_days = _priority_early_days(priority)
    if early_days > 0 and days == early_days and not row["notified_early"]:
        return "early"
    return None


def build_daily_digest(dry_run: bool = False) -> Optional[str]:
    """
    Query DB → apply priority rules → build HTML digest.
    Returns the digest string, or None if nothing is actionable today.
    Crawling is now on-demand via the agent's crawl_emails_now tool.
    """
    today = local_today()

    # ── Step 1: Query DB for items to notify ─────────────────────────────────
    # Widen to 3 days for early notifications
    upcoming = get_upcoming_action_items(days_ahead=3)
    urgent_items = get_unnotified_urgent()

    # ── Step 2: Apply priority-based timing rules ─────────────────────────────
    urgent_to_send: list = []
    high_to_send: list = []
    normal_to_send: list = []
    items_to_mark: list[tuple] = []  # (id, flag)

    for row in upcoming:
        flag = _should_notify(row)
        if flag:
            priority = row.get("priority") or "normal"
            if priority == "urgent":
                urgent_to_send.append(row)
            elif priority == "high":
                high_to_send.append(row)
            else:
                normal_to_send.append(row)
            items_to_mark.append((row["id"], flag))

    # Add unnotified urgent_reply items (legacy type) to urgent bucket
    for row in urgent_items:
        if row["id"] not in {r["id"] for r in urgent_to_send}:
            urgent_to_send.append(row)
            items_to_mark.append((row["id"], "day"))

    # ── Step 3: Birthday alerts ───────────────────────────────────────────────
    birthday_alerts = get_birthday_alerts()  # list of (msg, bid, flag)

    # ── Step 4: Bail if nothing to send ──────────────────────────────────────
    if not urgent_to_send and not high_to_send and not normal_to_send and not birthday_alerts:
        logger.info("Nothing actionable today.")
        return None

    # ── Step 5: Build HTML digest (priority-based) ────────────────────────────
    day_label = today.strftime("%a %b %-d")
    lines = [f"📅 <b>Daily Digest — {day_label}</b>"]

    if urgent_to_send:
        lines.append("")
        lines.append("🚨 <b>Urgent</b>")
        for row in urgent_to_send:
            lines.append(_format_item_line(row))

    if high_to_send:
        lines.append("")
        lines.append("⚡ <b>Important</b>")
        for row in high_to_send:
            lines.append(_format_item_line(row))

    if normal_to_send:
        lines.append("")
        lines.append("📋 <b>Coming Up</b>")
        for row in normal_to_send:
            lines.append(_format_item_line(row))

    if birthday_alerts:
        lines.append("")
        lines.append("🎂 <b>Birthdays</b>")
        for msg, _bid, _flag in birthday_alerts:
            lines.append(_format_birthday_line(msg))
            extra_lines = msg.split("\n")[1:]
            for extra in extra_lines:
                if extra.strip():
                    lines.append(f"  {extra}")

    digest = "\n".join(lines)

    # ── Step 6: Mark notifications (skip in dry_run) ──────────────────────────
    if not dry_run:
        for item_id, flag in items_to_mark:
            mark_action_notified(item_id, flag)
        for _msg, bid, flag in birthday_alerts:
            mark_notified(bid, flag)

    return digest
