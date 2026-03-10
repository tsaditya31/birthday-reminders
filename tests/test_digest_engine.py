"""Tests for digest engine formatting and HTML escaping."""

from unittest.mock import patch
from datetime import date

from core.digest_engine import _format_item_line


@patch("core.digest_engine.date")
def test_appointment_today(mock_date):
    mock_date.today.return_value = date(2026, 3, 10)
    mock_date.fromisoformat = date.fromisoformat
    row = {"due_date": "2026-03-10", "due_time": "14:00", "title": "Dentist", "type": "appointment", "category": "medical"}
    result = _format_item_line(row)
    assert "Today" in result
    assert "Dentist" in result
    assert "14:00" in result


@patch("core.digest_engine.date")
def test_item_escapes_html(mock_date):
    mock_date.today.return_value = date(2026, 3, 10)
    mock_date.fromisoformat = date.fromisoformat
    row = {
        "due_date": "2026-03-10",
        "title": "RSVP <script>alert('xss')</script>",
        "type": "deadline",
        "category": None,
    }
    result = _format_item_line(row)
    assert "<script>" not in result
    assert "&lt;script&gt;" in result


@patch("core.digest_engine.date")
def test_item_no_due_date(mock_date):
    mock_date.today.return_value = date(2026, 3, 10)
    mock_date.fromisoformat = date.fromisoformat
    row = {
        "due_date": None,
        "title": "Reply needed",
        "type": "urgent_reply",
        "category": "work",
    }
    result = _format_item_line(row)
    assert "Reply needed" in result
    assert "[work]" in result
