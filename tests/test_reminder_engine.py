"""Tests for reminder engine logic."""

from datetime import date
from unittest.mock import patch

from core.reminder_engine import _compute_turning_age, _days_until, _format_basic_reminder


class TestDaysUntil:
    def test_birthday_today(self):
        today = date(2026, 3, 10)
        with patch("core.reminder_engine.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            assert _days_until(3, 10) == 0

    def test_birthday_tomorrow(self):
        today = date(2026, 3, 10)
        with patch("core.reminder_engine.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            assert _days_until(3, 11) == 1

    def test_birthday_next_year(self):
        today = date(2026, 3, 10)
        with patch("core.reminder_engine.date") as mock_date:
            mock_date.today.return_value = today
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            # March 1 already passed, so next occurrence is next year
            result = _days_until(3, 1)
            assert result > 300  # Should wrap to next year


class TestComputeTurningAge:
    def test_no_birth_year(self):
        assert _compute_turning_age(None, 5, 15) is None

    def test_birthday_already_passed(self):
        # Today is March 10, 2026. Birthday Jan 15 already passed.
        # Born 2019 → next birthday is Jan 15, 2027 → turning 8.
        with patch("core.reminder_engine.date") as mock_date:
            mock_date.today.return_value = date(2026, 3, 10)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            age = _compute_turning_age(2019, 1, 15)
            assert age == 8

    def test_birthday_not_yet(self):
        # Today is March 10, 2026. Birthday Dec 25 hasn't happened yet.
        # Born 2019 → turning 7 on Dec 25, 2026.
        with patch("core.reminder_engine.date") as mock_date:
            mock_date.today.return_value = date(2026, 3, 10)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            age = _compute_turning_age(2019, 12, 25)
            assert age == 7

    def test_birthday_today(self):
        # Today is March 10, 2026. Born 2020. Birthday is today → turning 6.
        with patch("core.reminder_engine.date") as mock_date:
            mock_date.today.return_value = date(2026, 3, 10)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            age = _compute_turning_age(2020, 3, 10)
            assert age == 6


class TestFormatBasicReminder:
    def test_today(self):
        msg = _format_basic_reminder("John", "friend", 0, 30)
        assert "today" in msg.lower()
        assert "John" in msg
        assert "turning 30" in msg

    def test_tomorrow(self):
        msg = _format_basic_reminder("Jane", "family", 1, None)
        assert "tomorrow" in msg.lower()
        assert "Jane" in msg
        assert "turning" not in msg

    def test_days_away(self):
        msg = _format_basic_reminder("Kid", "annas_friend", 7, 8)
        assert "7 days" in msg
        assert "turning 8" in msg
