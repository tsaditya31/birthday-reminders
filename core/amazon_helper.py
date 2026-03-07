"""
Amazon helper — generates contextual Amazon search URLs for birthday gifts.
Uses Claude to produce age-appropriate search queries; no API key required.
"""

import json
import logging
from typing import Optional

import anthropic

from config import settings

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

BASE_URL = "https://www.amazon.com/s?k="


def _encode_query(query: str) -> str:
    return query.strip().replace(" ", "+")


def _call_claude_for_queries(name: str, age: Optional[int]) -> list[str]:
    if age:
        age_context = f"{name} is turning {age} years old."
        default_range = f"age {age}"
    else:
        age_context = f"{name}'s age is unknown, likely between 6–10 years old."
        default_range = "ages 6-10"

    prompt = f"""Generate 4 Amazon toy/gift search queries for a child's birthday.
{age_context}

Return a JSON array of 4 short search query strings (no URLs, just the search terms).
Make them specific and appealing — mix categories like LEGO/building, creative/art, games, and outdoor/active.
Tailor to the age. Examples for age 7: ["lego sets age 7 girls", "arts crafts kit age 7", "board games kids age 7", "outdoor toys age 7"].

Respond with only the JSON array."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as exc:
        logger.warning("Claude query generation failed: %s — using defaults", exc)
        return [
            f"toy gifts {default_range}",
            f"lego sets {default_range}",
            f"board games kids {default_range}",
            f"arts crafts kit {default_range}",
        ]


def build_amazon_message(name: str, age: Optional[int], days_until: int) -> str:
    """Build a Telegram-formatted message with Amazon gift search links."""
    queries = _call_claude_for_queries(name, age)
    links = [f'• <a href="{BASE_URL}{_encode_query(q)}">{q.replace("+", " ").title()}</a>' for q in queries]

    if age:
        age_str = f"turns {age}"
    else:
        age_str = "has a birthday"

    timing = f"in {days_until} days" if days_until > 0 else "today"

    lines = [
        f"🎁 Anna's friend <b>{name}</b> {age_str} <b>{timing}</b>!",
        "",
        "Amazon gift ideas:",
    ] + links

    return "\n".join(lines)
