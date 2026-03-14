"""
Item name normalizer — canonicalize item names for matching across
receipts and pantry snapshots. Copied from pantry-pilot.
"""

import re

_BRAND_NOISE = {
    "organic", "natural", "fresh", "premium", "grade", "fancy",
    "select", "choice", "value", "great", "best", "classic",
}

_SIZE_RE = re.compile(
    r"\b\d+(\.\d+)?\s*(oz|fl\s*oz|lb|lbs|kg|g|ml|l|ct|pk|pack|count|ea|gal|qt|pt)\b",
    re.IGNORECASE,
)

_ABBREVS = {
    "chkn": "chicken", "brst": "breast", "brsts": "breasts",
    "org": "organic", "whl": "whole", "grn": "green",
    "blk": "black", "wht": "white", "brn": "brown",
    "bana": "banana", "straw": "strawberry", "tom": "tomato",
    "pot": "potato", "sm": "small", "md": "medium", "lg": "large",
    "xl": "extra large", "pnt": "pint", "btl": "bottle",
    "pkt": "packet", "doz": "dozen", "veg": "vegetable", "vegs": "vegetables",
}

_PLURAL_SUFFIXES = [
    ("ies", "y"), ("ves", "f"), ("oes", "o"),
    ("ses", "s"), ("es", "e"), ("s", ""),
]

_PLURAL_EXCEPTIONS = {
    "hummus", "couscous", "asparagus", "citrus", "plus",
    "swiss", "molasses", "lettuce", "rice", "cheese", "juice",
    "sauce", "grapes", "peas", "oats", "herbs", "spices",
}


def normalize(name: str) -> str:
    """Normalize an item name to a canonical form for matching."""
    if not name:
        return ""

    s = name.lower().strip()
    s = _SIZE_RE.sub("", s)
    s = re.sub(r"[^\w\s-]", "", s)

    words = s.split()
    words = [_ABBREVS.get(w, w) for w in words]
    words = [w for w in words if w not in _BRAND_NOISE]

    s = " ".join(words).strip()
    s = re.sub(r"\s+", " ", s)

    if s and s not in _PLURAL_EXCEPTIONS:
        for suffix, replacement in _PLURAL_SUFFIXES:
            if s.endswith(suffix) and len(s) > len(suffix) + 1:
                candidate = s[: -len(suffix)] + replacement
                if len(candidate) >= 3:
                    s = candidate
                    break

    return s
