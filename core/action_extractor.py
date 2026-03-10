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
from core.utils import strip_json_markdown
from crawler.gmail_crawler import RawEmail

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

BATCH_SIZE = settings.extraction_batch_size
MIN_CONFIDENCE = settings.action_confidence_threshold

SYSTEM_PROMPT = """You are an assistant that extracts actionable items from emails.
For each email you receive, identify whether it contains an actionable item the user needs to
act on. Return valid JSON only. Be conservative — skip marketing, newsletters,
automated shipping confirmations, and anything that requires no human action.
"""

USER_PROMPT_TEMPLATE = """Below are {n} emails. For each one, extract actionable information.

Classify each email with an appropriate type — use any of these or create your own if needed:
- "appointment" — a scheduled event with a specific date/time (doctor, dentist, meeting, etc.)
- "deadline" — the user must take an action by a specific date (RSVP, payment, form, etc.)
- "meeting" — a meeting request or scheduled call
- "task" — a to-do item or action the user needs to complete
- "follow_up" — needs a follow-up response or check-in
- "payment" — a bill, invoice, or payment due
- "renewal" — subscription, license, passport, or membership renewal
- "booking" — a reservation or booking confirmation requiring action
- "legal" — legal filing, court date, notarization, or legal deadline
- "rsvp" — event invitation requiring RSVP
- "urgent_reply" — the email is clearly waiting on the user's personal reply
- null — marketing, newsletter, automated notification, or no clear human action needed

Assign a priority:
- "urgent" — needs immediate attention, time-critical
- "high" — important, should be handled soon
- "normal" — standard priority
- "low" — nice-to-know, no rush

Assign a category:
- "medical", "legal", "financial", "work", "personal", "travel", "education", "government", or
  any other appropriate category

For each email, extract the title (short summary, max 80 chars), description (one sentence of
context), due_date (YYYY-MM-DD) and due_time (HH:MM, 24h) when present.
For urgent_reply items, due_date and due_time may be null.

Return a JSON array (one object per email, in the same order). Each object must have:
{{
  "email_index": <int, 0-based>,
  "type": <string or null>,
  "title": <string or null>,
  "description": <string or null>,
  "due_date": <"YYYY-MM-DD" or null>,
  "due_time": <"HH:MM" or null>,
  "priority": "urgent" | "high" | "normal" | "low",
  "category": <string or null>,
  "confidence": <float 0.0-1.0>,
  "notes": <brief reason string>
}}

If an email has no actionable item, set type to null and confidence < 0.3.
{feedback_block}
Today's date for reference: {today}

Emails:
{emails}"""


@dataclass
class ExtractedActionItem:
    type: str
    title: str
    description: str
    due_date: Optional[str]     # YYYY-MM-DD
    due_time: Optional[str]     # HH:MM
    priority: str               # urgent | high | normal | low
    category: Optional[str]
    email_message_id: str
    email_subject: str
    email_from: str
    confidence: float
    source_snippet: Optional[str] = None


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


def _get_adaptive_threshold() -> float:
    """Adjust confidence threshold based on feedback ratio."""
    if not settings.adaptive_threshold_enabled:
        return MIN_CONFIDENCE
    from db.store import get_feedback_stats
    stats = get_feedback_stats(days=30)
    useful = stats.get("useful", 0)
    not_useful = stats.get("not_useful", 0)
    total = useful + not_useful
    if total < 5:
        return MIN_CONFIDENCE
    noise_ratio = not_useful / total
    if noise_ratio > 0.5:
        return min(MIN_CONFIDENCE + 0.1, 0.9)
    elif noise_ratio < 0.2 and useful > 5:
        return max(MIN_CONFIDENCE - 0.1, 0.3)
    return MIN_CONFIDENCE


def _call_claude(batch: list[RawEmail]) -> list[dict]:
    from datetime import date
    from core.preferences import get_extraction_rules_block, get_feedback_examples_block

    emails_text = _format_emails_for_prompt(batch)
    feedback_block = get_feedback_examples_block()
    prompt = USER_PROMPT_TEMPLATE.format(
        n=len(batch),
        today=date.today().isoformat(),
        emails=emails_text,
        feedback_block=feedback_block,
    )

    system = SYSTEM_PROMPT + get_extraction_rules_block()
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_text = strip_json_markdown(message.content[0].text)

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        logger.error("Claude returned invalid JSON: %s\n%s", exc, raw_text[:500])
        return []


def extract_action_items(emails: list[RawEmail]) -> list[ExtractedActionItem]:
    """Process all raw emails in batches; return high-confidence action items."""
    extracted: list[ExtractedActionItem] = []
    threshold = _get_adaptive_threshold()
    logger.info("Using confidence threshold: %.2f", threshold)

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
            if confidence < threshold:
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
                    priority=item.get("priority", "normal"),
                    category=item.get("category"),
                    email_message_id=source_email.id,
                    email_subject=source_email.subject,
                    email_from=source_email.sender,
                    confidence=confidence,
                    source_snippet=source_email.full_body[:500],
                )
            )
            logger.info(
                "Extracted action: [%s/%s] %s (due=%s, priority=%s) conf=%.2f",
                item_type,
                item.get("category", "?"),
                title,
                item.get("due_date"),
                item.get("priority", "normal"),
                confidence,
            )

    logger.info("Total action items above threshold: %d", len(extracted))
    return extracted
