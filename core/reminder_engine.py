"""
Reminder engine — checks upcoming birthdays and dispatches Telegram notifications.
Respects per-year notification flags to avoid duplicate sends.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from db.store import get_upcoming_birthdays, mark_notified, reset_annual_flags
from core.amazon_helper import build_amazon_message
from notifier.telegram_notifier import send_message

logger = logging.getLogger(__name__)

# Each alert tuple: (message_str, birthday_id, flag)
BirthdayAlert = tuple[str, int, str]


def _days_until(month: int, day: int) -> int:
    today = date.today()
    this_year = today.year
    target = date(this_year, month, day)
    if target < today:
        target = date(this_year + 1, month, day)
    return (target - today).days


def _compute_turning_age(birth_year: int | None, birth_month: int, birth_day: int) -> int | None:
    """Return the age this person is turning on their next upcoming birthday."""
    if birth_year is None:
        return None
    today = date.today()
    # Find the next occurrence of this birthday
    try:
        this_year_bday = date(today.year, birth_month, birth_day)
    except ValueError:
        this_year_bday = date(today.year, birth_month, min(birth_day, 28))
    if this_year_bday >= today:
        return today.year - birth_year
    else:
        return today.year + 1 - birth_year


def _format_basic_reminder(name: str, classification: str, days_until: int, age: int | None) -> str:
    if days_until == 0:
        timing = "is <b>today</b> 🎉"
    elif days_until == 1:
        timing = "is <b>tomorrow</b>!"
    else:
        timing = f"is in <b>{days_until} days</b>"

    emoji = {"annas_friend": "🧒", "family": "👨‍👩‍👧", "friend": "🎂"}.get(classification, "🎂")
    age_str = f" (turning {age})" if age else ""
    return f"{emoji} <b>{name}</b>'s birthday{age_str} {timing}"


def get_birthday_alerts() -> list[BirthdayAlert]:
    """
    Collect birthday alerts that are due to be sent today.
    Returns a list of (message_str, birthday_id, flag) tuples — does NOT send or mark.
    """
    today = date.today()

    if today.month == 1 and today.day == 1:
        logger.info("New year detected — resetting notification flags.")
        reset_annual_flags()

    upcoming = get_upcoming_birthdays(days_ahead=14)
    logger.info("Found %d birthdays in the next 14 days.", len(upcoming))

    alerts: list[BirthdayAlert] = []

    for row in upcoming:
        bid = row["id"]
        name = row["name"]
        classification = row["classification"]
        birth_month = row["birth_month"]
        birth_day = row["birth_day"]
        birth_year = row["birth_year"]

        days = _days_until(birth_month, birth_day)
        age = _compute_turning_age(birth_year, birth_month, birth_day)

        logger.info("%s: %d days away (classification=%s)", name, days, classification)

        if days == 0 and not row["notified_day"]:
            msg = _format_basic_reminder(name, classification, 0, age)
            if classification == "annas_friend":
                msg += "\n\n" + build_amazon_message(name, age, 0)
            alerts.append((msg, bid, "day"))

        elif days == 7 and not row["notified_1wk"]:
            msg = _format_basic_reminder(name, classification, 7, age)
            alerts.append((msg, bid, "1wk"))

        elif days == 14 and not row["notified_2wk"]:
            if classification == "annas_friend":
                msg = build_amazon_message(name, age, 14)
            else:
                msg = _format_basic_reminder(name, classification, 14, age)
            alerts.append((msg, bid, "2wk"))

    return alerts


def run_reminders(dry_run: bool = False):
    """Main daily reminder logic (standalone — sends directly)."""
    alerts = get_birthday_alerts()
    sent_count = 0

    for msg, bid, flag in alerts:
        if not dry_run:
            send_message(msg)
            mark_notified(bid, flag)
        else:
            logger.info("[DRY RUN] Would send (%s): %s", flag, msg)
        sent_count += 1

    logger.info("Reminders sent: %d", sent_count)
    return sent_count
