"""
Preference helpers — loads user preferences and formats them for injection
into extraction prompts and sender pre-filtering.
"""

import re

from db.store import get_active_preferences


def get_extraction_rules_block() -> str:
    """Load active extraction_rule preferences and format as a prompt block."""
    rules = get_active_preferences(category="extraction_rule")
    if not rules:
        return ""
    lines = ["\n\nUser-specified extraction rules (follow these strictly):"]
    for r in rules:
        lines.append(f"- {r['rule_text']}")
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
