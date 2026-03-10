"""Shared utilities for the core package."""


def strip_json_markdown(text: str) -> str:
    """Strip markdown code fences from a JSON response string."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return text.strip()
