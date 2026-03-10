"""Tests for Gmail crawler retry logic."""

import pytest
from unittest.mock import MagicMock, patch

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
