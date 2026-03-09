"""
Birthday extractor — uses Claude to parse raw emails and extract structured birthday data.
Processes emails in batches of 20 to minimise API calls.
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
MIN_CONFIDENCE = 0.6

SYSTEM_PROMPT = """You are an assistant that extracts birthday information from emails.
For each email you receive, extract structured data and return valid JSON.
Be conservative: only extract when you are reasonably confident a real person's birthday is mentioned.
"""

USER_PROMPT_TEMPLATE = """Below are {n} emails. For each one, extract birthday information.

Classify each person as:
- "annas_friend" — if the email is about a child's birthday party, school-age kid, or references "Anna" as an invitee
- "family" — if it references family relationships (mom, dad, grandma, uncle, aunt, cousin, grandpa, etc.)
- "friend" — all other adults

Also extract age_at_event if the email says things like "turning 7", "8th birthday", "celebrating her 6th", etc.

Return a JSON array (one object per email, in the same order). Each object must have:
{{
  "email_index": <int, 0-based>,
  "name": <string or null>,
  "birth_month": <int 1-12 or null>,
  "birth_day": <int 1-31 or null>,
  "birth_year": <int or null>,
  "age_at_event": <int or null>,
  "classification": "annas_friend" | "family" | "friend" | null,
  "confidence": <float 0.0-1.0>,
  "notes": <brief reason string>
}}

If no birthday is clearly present in an email, set name and birth_month to null and confidence < 0.3.

Emails:
{emails}"""


@dataclass
class ExtractedBirthday:
    name: str
    birth_month: int
    birth_day: int
    birth_year: Optional[int]
    age_at_event: Optional[int]
    classification: str
    email_source: str
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
    from core.preferences import get_extraction_rules_block

    emails_text = _format_emails_for_prompt(batch)
    prompt = USER_PROMPT_TEMPLATE.format(n=len(batch), emails=emails_text)

    system = SYSTEM_PROMPT + get_extraction_rules_block()
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_text = message.content[0].text.strip()

    # Strip markdown code fences if present
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


def extract_birthdays(emails: list[RawEmail]) -> list[ExtractedBirthday]:
    """Process all raw emails in batches; return high-confidence extractions."""
    extracted: list[ExtractedBirthday] = []

    for batch_start in range(0, len(emails), BATCH_SIZE):
        batch = emails[batch_start : batch_start + BATCH_SIZE]
        logger.info(
            "Extracting batch %d–%d of %d",
            batch_start,
            batch_start + len(batch) - 1,
            len(emails),
        )

        raw_results = _call_claude(batch)

        for item in raw_results:
            confidence = item.get("confidence", 0.0)
            if confidence < MIN_CONFIDENCE:
                continue

            name = item.get("name")
            birth_month = item.get("birth_month")
            birth_day = item.get("birth_day")
            classification = item.get("classification")

            if not all([name, birth_month, birth_day, classification]):
                continue

            idx = item.get("email_index", 0)
            source_email = batch[idx] if idx < len(batch) else batch[0]
            email_source = f"{source_email.subject[:80]} ({source_email.date[:16]})"

            extracted.append(
                ExtractedBirthday(
                    name=name,
                    birth_month=int(birth_month),
                    birth_day=int(birth_day),
                    birth_year=item.get("birth_year"),
                    age_at_event=item.get("age_at_event"),
                    classification=classification,
                    email_source=email_source,
                    confidence=confidence,
                )
            )
            logger.info(
                "Extracted: %s (%s/%s) [%s] conf=%.2f",
                name,
                birth_month,
                birth_day,
                classification,
                confidence,
            )

    logger.info("Total extractions above threshold: %d", len(extracted))
    return extracted
