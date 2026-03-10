"""
Preference helpers — loads user preferences and formats them for injection
into extraction prompts and sender pre-filtering.
"""

import re

from config import settings
from db.store import get_active_preferences, get_recent_feedback


def get_extraction_rules_block() -> str:
    """Load active extraction_rule preferences and format as a prompt block."""
    rules = get_active_preferences(category="extraction_rule")
    if not rules:
        return ""
    lines = ["\n\nUser-specified extraction rules (follow these strictly):"]
    for r in rules:
        lines.append(f"- {r['rule_text']}")
    return "\n".join(lines)


def get_feedback_examples_block() -> str:
    """Load recent feedback and format as few-shot examples for the extraction prompt."""
    feedback = get_recent_feedback(limit=settings.feedback_example_count)
    if not feedback:
        return ""
    lines = ["\n\nLearn from this user feedback on past extractions:"]
    for fb in feedback:
        title = fb.get("item_title") or fb.get("email_subject") or "unknown"
        ft = fb["feedback_type"]
        if ft == "useful":
            lines.append(f"- GOOD: \"{title}\" (type={fb.get('item_type')}) — user found this helpful")
        elif ft == "not_useful":
            lines.append(f"- BAD: \"{title}\" (type={fb.get('item_type')}) — user dismissed this, it was not useful")
        elif ft == "missed":
            comment = fb.get("user_comment", "")
            lines.append(f"- MISSED: user said you missed: \"{comment}\" — be more aggressive catching items like this")
        elif ft == "wrong_type":
            lines.append(
                f"- CORRECTION: \"{title}\" should be type={fb.get('corrected_type')} "
                f"not type={fb.get('original_type')}"
            )
        elif ft == "wrong_date":
            lines.append(
                f"- CORRECTION: \"{title}\" should have date={fb.get('corrected_date')} "
                f"not date={fb.get('original_date')}"
            )
    return "\n".join(lines)


def get_blocked_senders() -> set[str]:
    """Extract email addresses from sender_filter preferences."""
    filters = get_active_preferences(category="sender_filter")
    blocked = set()
    for f in filters:
        # Try to find email addresses in the rule text
        emails = re.findall(r"[\w.+-]+@[\w-]+\.[\w.-]+", f["rule_text"])
        if emails:
            blocked.update(e.lower() for e in emails)
        else:
            # Store the raw rule text lowered for substring matching on sender field
            blocked.add(f["rule_text"].lower())
    return blocked
