"""
Google Calendar integration — read events and create new ones.
Reuses the same OAuth client_id/client_secret as the Gmail crawler.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from config import settings

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/calendar"]


def _build_calendar_service():
    """Build an authenticated Google Calendar API service."""
    token = settings.google_calendar_refresh_token
    if not token:
        raise RuntimeError(
            "Google Calendar not configured. Set GOOGLE_CALENDAR_REFRESH_TOKEN in your environment."
        )
    creds = Credentials(
        token=None,
        refresh_token=token,
        client_id=settings.gmail_client_id,
        client_secret=settings.gmail_client_secret,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def list_events(start_date: str, end_date: str, query: Optional[str] = None) -> list[dict]:
    """
    Query Google Calendar events between start_date and end_date (YYYY-MM-DD).
    Returns a list of simplified event dicts.
    """
    try:
        service = _build_calendar_service()
    except Exception as exc:
        logger.error("Calendar service unavailable: %s", exc)
        return []
    time_min = f"{start_date}T00:00:00Z"
    time_max = f"{end_date}T23:59:59Z"

    params = {
        "calendarId": "primary",
        "timeMin": time_min,
        "timeMax": time_max,
        "singleEvents": True,
        "orderBy": "startTime",
        "maxResults": 50,
    }
    if query:
        params["q"] = query

    results = service.events().list(**params).execute()
    events = []
    for item in results.get("items", []):
        start = item.get("start", {})
        end = item.get("end", {})
        events.append({
            "id": item.get("id"),
            "summary": item.get("summary", "(no title)"),
            "start": start.get("dateTime") or start.get("date"),
            "end": end.get("dateTime") or end.get("date"),
            "location": item.get("location"),
            "description": item.get("description"),
            "all_day": "date" in start and "dateTime" not in start,
        })
    return events


def create_event(
    summary: str,
    date_str: str,
    time_str: Optional[str] = None,
    duration_minutes: int = 60,
    description: Optional[str] = None,
    all_day: bool = False,
) -> dict:
    """Create a Google Calendar event. Returns success status and event details."""
    try:
        service = _build_calendar_service()
    except Exception as exc:
        logger.error("Calendar service unavailable: %s", exc)
        return {"success": False, "error": str(exc)}

    if all_day or not time_str:
        event_body = {
            "summary": summary,
            "start": {"date": date_str},
            "end": {"date": date_str},
        }
    else:
        start_dt = datetime.fromisoformat(f"{date_str}T{time_str}:00")
        end_dt = start_dt + timedelta(minutes=duration_minutes)
        # Use local timezone format (no Z suffix — Calendar API treats as local)
        event_body = {
            "summary": summary,
            "start": {"dateTime": start_dt.isoformat()},
            "end": {"dateTime": end_dt.isoformat()},
        }

    if description:
        event_body["description"] = description

    try:
        created = service.events().insert(calendarId="primary", body=event_body).execute()
        return {
            "success": True,
            "event_id": created.get("id"),
            "summary": created.get("summary"),
            "html_link": created.get("htmlLink"),
        }
    except Exception as exc:
        logger.error("Failed to create calendar event: %s", exc)
        return {"success": False, "error": str(exc)}
