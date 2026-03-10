"""Tests for Gmail crawler retry logic and processed-email helpers."""

import pytest
from unittest.mock import MagicMock, patch, call

from crawler.gmail_crawler import _retry_api_call


@patch("crawler.gmail_crawler.time.sleep")
def test_retry_succeeds_on_second_attempt(mock_sleep):
    func = MagicMock(side_effect=[Exception("transient"), {"result": "ok"}])
    result = _retry_api_call(func, "test call")
    assert result == {"result": "ok"}
    assert func.call_count == 2
    mock_sleep.assert_called_once()


@patch("crawler.gmail_crawler.time.sleep")
def test_retry_raises_after_max_retries(mock_sleep):
    func = MagicMock(side_effect=Exception("persistent failure"))
    with pytest.raises(Exception, match="persistent failure"):
        _retry_api_call(func, "test call")
    assert func.call_count == 3  # MAX_RETRIES default


def test_retry_succeeds_first_try():
    func = MagicMock(return_value={"messages": []})
    result = _retry_api_call(func, "test call")
    assert result == {"messages": []}
    assert func.call_count == 1


@patch("crawler.gmail_crawler.time.sleep")
def test_retry_exponential_backoff(mock_sleep):
    func = MagicMock(side_effect=[Exception("e1"), Exception("e2"), "ok"])
    result = _retry_api_call(func, "test call")
    assert result == "ok"
    # First retry: base_delay * 2^0 = 1.0, second: base_delay * 2^1 = 2.0
    calls = mock_sleep.call_args_list
    assert len(calls) == 2
    assert calls[0][0][0] == pytest.approx(1.0)
    assert calls[1][0][0] == pytest.approx(2.0)


# ── Processed-email helper tests (mocked DB) ────────────────────────────────

def test_is_email_processed_passes_processing_type():
    """is_email_processed should query with processing_type and extraction_version."""
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("db.store.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        with patch("db.store.settings") as mock_settings:
            mock_settings.extraction_version = 1

            from db.store import is_email_processed
            result = is_email_processed("msg_123", processing_type="action")

    assert result is False
    sql = mock_cur.execute.call_args[0][0]
    assert "processing_type" in sql
    assert "extraction_version" in sql
    params = mock_cur.execute.call_args[0][1]
    assert params == ("msg_123", "action", 1)


def test_mark_email_processed_passes_processing_type():
    """mark_email_processed should insert with processing_type and extraction_version."""
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("db.store.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)
        with patch("db.store.settings") as mock_settings:
            mock_settings.extraction_version = 1

            from db.store import mark_email_processed
            mark_email_processed("msg_456", processing_type="birthday")

    sql = mock_cur.execute.call_args[0][0]
    assert "processing_type" in sql
    assert "extraction_version" in sql
    params = mock_cur.execute.call_args[0][1]
    assert params[0] == "msg_456"
    assert params[1] == "birthday"
    assert params[2] == 1  # extraction_version


def test_clear_processed_emails_deletes_by_type():
    """clear_processed_emails should only delete rows for the given type."""
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("db.store.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)

        from db.store import clear_processed_emails
        clear_processed_emails("action")

    sql = mock_cur.execute.call_args[0][0]
    assert "DELETE" in sql
    assert "processing_type" in sql
    params = mock_cur.execute.call_args[0][1]
    assert params == ("action",)
