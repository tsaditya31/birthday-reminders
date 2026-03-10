"""Tests for shared utility functions."""

from core.utils import strip_json_markdown


def test_plain_json_passthrough():
    assert strip_json_markdown('[{"a": 1}]') == '[{"a": 1}]'


def test_strips_json_code_fence():
    raw = '```json\n[{"a": 1}]\n```'
    assert strip_json_markdown(raw) == '[{"a": 1}]'


def test_strips_bare_code_fence():
    raw = '```\n{"key": "value"}\n```'
    assert strip_json_markdown(raw) == '{"key": "value"}'


def test_strips_surrounding_whitespace():
    raw = '  \n  {"key": "value"}  \n  '
    assert strip_json_markdown(raw) == '{"key": "value"}'
