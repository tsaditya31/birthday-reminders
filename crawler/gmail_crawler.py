"""
Gmail crawler — searches for birthday-related emails going back 2 years.
Returns RawEmail objects; deduplicates against processed_emails table.
"""

import base64
import email as email_lib
import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta

from core.utils import local_today
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from config import settings
from db.store import is_email_processed, mark_email_processed

BIRTHDAY_LOOKBACK_DAYS = settings.birthday_lookback_days
MAX_RETRIES = settings.gmail_max_retries
RETRY_BASE_DELAY = settings.gmail_retry_base_delay


def _retry_api_call(func, description: str):
    """Execute a Gmail API call with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            return func()
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                logger.error("%s failed after %d retries: %s", description, MAX_RETRIES, exc)
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning("%s failed (attempt %d/%d): %s — retrying in %.1fs",
                           description, attempt + 1, MAX_RETRIES, exc, delay)
            time.sleep(delay)

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

SEARCH_QUERIES = [
    "subject:birthday",
    "birthday party invitation",
    '"is turning"',
    '"happy birthday"',
    '"birthday reminder"',
    '"Birthday of"',
]

ACTION_QUERIES = [
    # Appointments & meetings
    "subject:appointment",
    "subject:reminder",
    '"your appointment"',
    '"your meeting"',
    "subject:scheduled",
    "subject:booking",
    "subject:confirmation",
    # Deadlines & responses
    '"RSVP by"',
    '"deadline"',
    '"due by"',
    '"respond by"',
    '"action required"',
    '"please respond"',
    '"time sensitive"',
    '"please complete"',
    '"next steps"',
    '"follow up"',
    '"follow-up"',
    "urgent",
    # Payments & renewals
    '"payment due"',
    '"invoice"',
    '"renewal notice"',
    '"expires on"',
    '"expiring soon"',
    # Legal & official
    '"filing deadline"',
    '"court date"',
    '"notarize"',
    '"sign by"',
]


def get_all_action_queries() -> list[str]:
    """Merge base ACTION_QUERIES with learned queries from the database."""
    from db.store import get_learned_queries
    learned = get_learned_queries()
    base_set = set(q.lower() for q in ACTION_QUERIES)
    merged = list(ACTION_QUERIES)
    for q in learned:
        if q.lower() not in base_set:
            merged.append(q)
    return merged


@dataclass
class RawEmail:
    id: str
    date: str
    subject: str
    sender: str
    snippet: str
    full_body: str


def _build_service():
    creds = Credentials(
        token=None,
        refresh_token=settings.gmail_refresh_token,
        client_id=settings.gmail_client_id,
        client_secret=settings.gmail_client_secret,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _after_date() -> str:
    cutoff = local_today() - timedelta(days=BIRTHDAY_LOOKBACK_DAYS)
    return cutoff.strftime("%Y/%m/%d")


def _after_date_days_back(days_back: int) -> str:
    cutoff = local_today() - timedelta(days=days_back)
    return cutoff.strftime("%Y/%m/%d")


def _decode_body(payload: dict) -> str:
    """Recursively extract plain-text body from a Gmail message payload."""
    mime_type = payload.get("mimeType", "")
    if mime_type == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
    if mime_type.startswith("multipart/"):
        for part in payload.get("parts", []):
            text = _decode_body(part)
            if text:
                return text
    return ""


def _get_header(headers: list[dict], name: str) -> str:
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def _fetch_message(service, msg_id: str) -> Optional[RawEmail]:
    try:
        request = service.users().messages().get(userId="me", id=msg_id, format="full")
        msg = _retry_api_call(request.execute, f"fetch message {msg_id}")
    except Exception as exc:
        logger.warning("Failed to fetch message %s: %s", msg_id, exc)
        return None

    headers = msg.get("payload", {}).get("headers", [])
    raw_email = RawEmail(
        id=msg_id,
        date=_get_header(headers, "Date"),
        subject=_get_header(headers, "Subject"),
        sender=_get_header(headers, "From"),
        snippet=msg.get("snippet", ""),
        full_body=_decode_body(msg.get("payload", {})) or msg.get("snippet", ""),
    )
    return raw_email


def crawl_emails(max_per_query: int = 200) -> list[RawEmail]:
    """
    Search Gmail for birthday-related emails across all queries.
    Skips already-processed messages. Returns new RawEmail objects.
    """
    service = _build_service()
    after = _after_date()
    seen_ids: set[str] = set()
    results: list[RawEmail] = []

    for query in SEARCH_QUERIES:
        full_query = f"{query} after:{after}"
        logger.info("Running Gmail query: %s", full_query)

        page_token: Optional[str] = None
        fetched = 0

        while fetched < max_per_query:
            params = {
                "userId": "me",
                "q": full_query,
                "maxResults": min(100, max_per_query - fetched),
            }
            if page_token:
                params["pageToken"] = page_token

            try:
                request = service.users().messages().list(**params)
                response = _retry_api_call(request.execute, f"list messages for '{query}'")
            except Exception as exc:
                logger.error("Gmail list failed for query '%s': %s", query, exc)
                break

            messages = response.get("messages", [])
            if not messages:
                break

            for msg_ref in messages:
                msg_id = msg_ref["id"]
                if msg_id in seen_ids:
                    continue
                seen_ids.add(msg_id)

                if is_email_processed(msg_id, processing_type="birthday"):
                    logger.debug("Skipping already-processed message %s", msg_id)
                    continue

                raw = _fetch_message(service, msg_id)
                if raw:
                    results.append(raw)
                    mark_email_processed(msg_id, processing_type="birthday")

            fetched += len(messages)
            page_token = response.get("nextPageToken")
            if not page_token:
                break

    logger.info("Crawl complete. %d new emails fetched.", len(results))
    return results


def crawl_action_emails(days_back: int = 3, max_per_query: int = 50) -> list[RawEmail]:
    """
    Search Gmail for action-related emails from the last days_back days.
    Skips already-processed messages. Returns new RawEmail objects.
    """
    service = _build_service()
    after = _after_date_days_back(days_back)
    seen_ids: set[str] = set()
    results: list[RawEmail] = []

    all_queries = get_all_action_queries()
    for query in all_queries:
        full_query = f"{query} after:{after}"
        logger.info("Running action Gmail query: %s", full_query)

        page_token: Optional[str] = None
        fetched = 0

        while fetched < max_per_query:
            params = {
                "userId": "me",
                "q": full_query,
                "maxResults": min(100, max_per_query - fetched),
            }
            if page_token:
                params["pageToken"] = page_token

            try:
                request = service.users().messages().list(**params)
                response = _retry_api_call(request.execute, f"list actions for '{query}'")
            except Exception as exc:
                logger.error("Gmail list failed for query '%s': %s", query, exc)
                break

            messages = response.get("messages", [])
            if not messages:
                break

            for msg_ref in messages:
                msg_id = msg_ref["id"]
                if msg_id in seen_ids:
                    continue
                seen_ids.add(msg_id)

                if is_email_processed(msg_id, processing_type="action"):
                    logger.debug("Skipping already-processed message %s", msg_id)
                    continue

                raw = _fetch_message(service, msg_id)
                if raw:
                    results.append(raw)
                    mark_email_processed(msg_id, processing_type="action")

            fetched += len(messages)
            page_token = response.get("nextPageToken")
            if not page_token:
                break

    logger.info("Action crawl complete. %d new emails fetched.", len(results))
    return results


def search_emails(query: str, max_results: int = 5) -> list[RawEmail]:
    """
    Search Gmail with an arbitrary query and return matching emails.
    Unlike crawl functions, this does NOT mark emails as processed
    and does NOT skip already-processed emails — it's a read-only lookup.
    """
    service = _build_service()
    results: list[RawEmail] = []

    logger.info("Searching Gmail: %s", query)
    try:
        request = service.users().messages().list(
            userId="me", q=query, maxResults=max_results,
        )
        response = _retry_api_call(request.execute, f"search '{query}'")
    except Exception as exc:
        logger.error("Gmail search failed for '%s': %s", query, exc)
        return results

    for msg_ref in response.get("messages", []):
        raw = _fetch_message(service, msg_ref["id"])
        if raw:
            results.append(raw)

    logger.info("Search returned %d emails.", len(results))
    return results
