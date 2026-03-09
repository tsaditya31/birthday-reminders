"""
Action extractor — uses Claude to parse raw emails and extract appointments,
deadlines, and urgent reply requests. Processes emails in batches of 20.
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

import anthropic

from config import settings
from crawler.gmail_crawler import RawEmail

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

BATCH_SIZE = 20
MIN_CONFIDENCE = 0.65

SYSTEM_PROMPT = """You are an assistant that extracts actionable items from emails.
For each email you receive, identify whether it contains an appointment, a deadline, or a request
requiring the user's reply. Return valid JSON only. Be conservative — skip marketing, newsletters,
automated shipping confirmations, and anything that requires no human action.
"""

USER_PROMPT_TEMPLATE = """Below are {n} emails. For each one, extract actionable information.

Classify each email as one of:
- "appointment" — a scheduled event with a specific date/time (doctor, dentist, school meeting,
  sports practice, any calendar event the user needs to attend)
- "deadline" — the user must take an action by a specific date (RSVP, payment, form submission,
  registration, renewal)
- "urgent_reply" — the email is clearly waiting on the user's personal reply; judge from the full
  context and tone, not just keywords
- null — marketing, newsletter, automated notification, or no clear human action needed

For each email, extract the title (short summary, max 80 chars), description (one sentence of
context), due_date (YYYY-MM-DD) and due_time (HH:MM, 24h) when present.
For urgent_reply items, due_date and due_time may be null.

Return a JSON array (one object per email, in the same order). Each object must have:
{{
  "email_index": <int, 0-based>,
  "type": "appointment" | "deadline" | "urgent_reply" | null,
  "title": <string or null>,
  "description": <string or null>,
  "due_date": <"YYYY-MM-DD" or null>,
  "due_time": <"HH:MM" or null>,
  "confidence": <float 0.0-1.0>,
  "notes": <brief reason string>
}}

If an email has no actionable item, set type to null and confidence < 0.3.

Today's date for reference: {today}

Emails:
{emails}"""


@dataclass
class ExtractedActionItem:
    type: str                   # 'appointment' | 'deadline' | 'urgent_reply'
    title: str
    description: str
    due_date: Optional[str]     # YYYY-MM-DD
    due_time: Optional[str]     # HH:MM
    email_message_id: str
    email_subject: str
    email_from: str
    confidence: float


def _format_emails_for_prompt(batch: list[RawEmail]) -> str:
    parts = []
    for i, email in enumerate(batch):
        body_preview = email.full_body[:800].replace("\n", " ")
        parts.append(
            f"[{i}] Subject: {email.subject}\n"
            f"    From: {email.sender}\n"
            f"    Date: {email.date}\n"
            f"    Body: {body_preview}"
        )
    return "\n\n".join(parts)


def _call_claude(batch: list[RawEmail]) -> list[dict]:
    from datetime import date
    from core.preferences import get_extraction_rules_block

    emails_text = _format_emails_for_prompt(batch)
    prompt = USER_PROMPT_TEMPLATE.format(
        n=len(batch),
        today=date.today().isoformat(),
        emails=emails_text,
    )

    system = SYSTEM_PROMPT + get_extraction_rules_block()
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_text = message.content[0].text.strip()

    if raw_text.startswith("```"):
        raw_text = raw_text.split("```")[1]
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]
    raw_text = raw_text.strip()

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        logger.error("Claude returned invalid JSON: %s\n%s", exc, raw_text[:500])
        return []


def extract_action_items(emails: list[RawEmail]) -> list[ExtractedActionItem]:
    """Process all raw emails in batches; return high-confidence action items."""
    extracted: list[ExtractedActionItem] = []

    for batch_start in range(0, len(emails), BATCH_SIZE):
        batch = emails[batch_start : batch_start + BATCH_SIZE]
        logger.info(
            "Extracting action batch %d–%d of %d",
            batch_start,
            batch_start + len(batch) - 1,
            len(emails),
        )

        raw_results = _call_claude(batch)

        for item in raw_results:
            confidence = item.get("confidence", 0.0)
            if confidence < MIN_CONFIDENCE:
                continue

            item_type = item.get("type")
            title = item.get("title")

            if not item_type or not title:
                continue

            idx = item.get("email_index", 0)
            source_email = batch[idx] if idx < len(batch) else batch[0]

            extracted.append(
                ExtractedActionItem(
                    type=item_type,
                    title=title,
                    description=item.get("description") or "",
                    due_date=item.get("due_date"),
                    due_time=item.get("due_time"),
                    email_message_id=source_email.id,
                    email_subject=source_email.subject,
                    email_from=source_email.sender,
                    confidence=confidence,
                )
            )
            logger.info(
                "Extracted action: [%s] %s (due=%s) conf=%.2f",
                item_type,
                title,
                item.get("due_date"),
                confidence,
            )

    logger.info("Total action items above threshold: %d", len(extracted))
    return extracted
