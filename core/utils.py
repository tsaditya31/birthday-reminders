"""Shared utilities for the core package."""

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from config import settings


def local_now() -> datetime:
    """Return the current datetime in the user's configured timezone."""
    return datetime.now(ZoneInfo(settings.user_timezone))


def local_today() -> date:
    """Return today's date in the user's configured timezone."""
    return local_now().date()


def strip_json_markdown(text: str) -> str:
    """Strip markdown code fences from a JSON response string."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return text.strip()
