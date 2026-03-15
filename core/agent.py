"""
Tool-use agent loop — replaces intent-parsing chat_handler.
Claude receives tool definitions and decides what to call.
"""

import json
import logging
from collections import deque
from datetime import date

import anthropic

from config import settings
from core.tools import (
    clear_pending_calendar_event,
    confirm_calendar_event,
    execute_tool,
    get_pending_calendar_event,
)

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

# Rolling conversation history (user + assistant messages)
_history: deque[dict] = deque(maxlen=20)

TOOL_DEFINITIONS = [
    {
        "name": "get_upcoming_birthdays",
        "description": "Query birthdays within the next N days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days_ahead": {"type": "integer", "description": "Number of days to look ahead. Default 30.", "default": 30},
            },
        },
    },
    {
        "name": "get_action_items",
        "description": "Query action items (appointments, deadlines, tasks, etc.) within a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date in YYYY-MM-DD format. Defaults to today."},
                "end_date": {"type": "string", "description": "End date in YYYY-MM-DD format. Defaults to 30 days from today."},
            },
        },
    },
    {
        "name": "list_preferences",
        "description": "Show the user's active extraction rules and sender filters.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Optional filter: 'extraction_rule' or 'sender_filter'.",
                    "enum": ["extraction_rule", "sender_filter"],
                },
            },
        },
    },
    {
        "name": "dismiss_birthday",
        "description": "Remove a birthday by the person's name. Use when the user wants to delete/remove a birthday.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "The person's name to search for."},
            },
            "required": ["name"],
        },
    },
    {
        "name": "dismiss_action_item",
        "description": "Dismiss an action item by title keyword. Use when the user wants to dismiss/remove/delete an action item.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Keyword to match the action item title."},
            },
            "required": ["title"],
        },
    },
    {
        "name": "add_preference",
        "description": "Add an extraction rule (what to extract or ignore) or sender filter (block emails from a sender).",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "enum": ["extraction_rule", "sender_filter"],
                    "description": "Type of preference.",
                },
                "rule_text": {"type": "string", "description": "The rule text or sender email to filter."},
            },
            "required": ["category", "rule_text"],
        },
    },
    {
        "name": "submit_feedback",
        "description": "Record feedback about action items. Types: 'useful' (item was helpful), 'not_useful' (item was irrelevant), 'missed' (something was not extracted), 'correct' (fix type or date).",
        "input_schema": {
            "type": "object",
            "properties": {
                "feedback_type": {
                    "type": "string",
                    "enum": ["useful", "not_useful", "missed", "correct"],
                    "description": "The type of feedback.",
                },
                "title": {"type": "string", "description": "Title keyword of the action item (for useful/not_useful/correct)."},
                "description": {"type": "string", "description": "Description of what was missed (for missed feedback)."},
                "corrected_type": {"type": "string", "description": "Corrected item type (for correct feedback)."},
                "corrected_date": {"type": "string", "description": "Corrected due date YYYY-MM-DD (for correct feedback)."},
            },
            "required": ["feedback_type"],
        },
    },
    {
        "name": "crawl_emails_now",
        "description": "Trigger an on-demand Gmail crawl to find and extract new action items from recent emails.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days_back": {"type": "integer", "description": "How many days back to search. Default 60.", "default": 60},
            },
        },
    },
    {
        "name": "search_email",
        "description": "Search Gmail for emails matching a query and return their full contents (subject, sender, date, body). Use this to read specific emails the user asks about.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Gmail search query (e.g. 'subject:Clara birthday', 'from:john@example.com')."},
                "max_results": {"type": "integer", "description": "Max emails to return. Default 5.", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_calendar_events",
        "description": "Read events from Google Calendar within a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD. Defaults to today."},
                "end_date": {"type": "string", "description": "End date YYYY-MM-DD. Defaults to 7 days from today."},
                "query": {"type": "string", "description": "Optional text search filter."},
            },
        },
    },
    {
        "name": "suggest_calendar_event",
        "description": "Suggest a new calendar event for the user to confirm. Shows a preview before creating.",
        "input_schema": {
            "type": "object",
            "properties": {
                "summary": {"type": "string", "description": "Event title."},
                "date_str": {"type": "string", "description": "Event date YYYY-MM-DD."},
                "time_str": {"type": "string", "description": "Event start time HH:MM (24h). Omit for all-day."},
                "duration_minutes": {"type": "integer", "description": "Duration in minutes. Default 60.", "default": 60},
                "description": {"type": "string", "description": "Event description."},
                "all_day": {"type": "boolean", "description": "Whether this is an all-day event.", "default": False},
            },
            "required": ["summary", "date_str"],
        },
    },
]

SYSTEM_PROMPT = f"""\
You are a personal assistant bot for managing birthdays, action items, and calendar events.
Today's date is {date.today().isoformat()}.

You help the user by calling the available tools to query their data and take actions.
You can call multiple tools if needed, and use results to give helpful responses.

Formatting rules (Telegram HTML):
- Use <b>bold</b> for emphasis and headers
- Use <i>italic</i> for secondary info
- Use bullet points (•) for lists
- Keep responses concise and friendly
- When showing dates, include how many days away they are

When suggesting calendar events, always show a preview and ask for confirmation before creating.
When the user confirms a suggested event (says yes/confirm/go ahead), tell them the event has been created.
"""


def _handle_calendar_confirmation(text: str) -> str | None:
    """Check if user is confirming/rejecting a pending calendar event."""
    pending = get_pending_calendar_event()
    if not pending:
        return None
    lower = text.lower().strip()
    if lower in ("yes", "y", "confirm", "go ahead", "do it", "ok", "sure", "yep", "yeah"):
        try:
            result = confirm_calendar_event()
        except Exception as exc:
            logger.error("Calendar confirmation failed: %s", exc)
            clear_pending_calendar_event()
            return f"Failed to create the event: {exc}"
        if result.get("success"):
            return f"Done! I've created <b>{result['summary']}</b> on your calendar."
        clear_pending_calendar_event()
        return f"Failed to create the event: {result.get('error', 'unknown error')}"
    if lower in ("no", "n", "cancel", "nah", "nope", "never mind"):
        clear_pending_calendar_event()
        return "No problem, I've cancelled the event suggestion."
    return None


def handle_message(text: str) -> str:
    """
    Agent loop: send user message + tools to Claude, execute tool calls,
    loop until Claude returns a text response.
    """
    # Check for pending calendar confirmation first
    cal_reply = _handle_calendar_confirmation(text)
    if cal_reply:
        return cal_reply

    # Build messages from history + new user message
    messages = list(_history) + [{"role": "user", "content": text}]

    max_iterations = 10
    for _ in range(max_iterations):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=messages,
        )

        # Collect tool uses and text from response
        tool_uses = []
        text_parts = []
        for block in response.content:
            if block.type == "tool_use":
                tool_uses.append(block)
            elif block.type == "text":
                text_parts.append(block.text)

        if not tool_uses:
            # No more tool calls — return the text response
            reply = "\n".join(text_parts).strip()
            if not reply:
                reply = "I'm not sure how to help with that. Try asking about your action items, birthdays, or calendar."
            # Update conversation history
            _history.append({"role": "user", "content": text})
            _history.append({"role": "assistant", "content": reply})
            return reply

        # Append the assistant message with tool_use blocks
        messages.append({"role": "assistant", "content": response.content})

        # Execute each tool and build tool_result messages
        tool_results = []
        for tool_use in tool_uses:
            logger.info("Calling tool: %s(%s)", tool_use.name, json.dumps(tool_use.input)[:200])
            result = execute_tool(tool_use.name, tool_use.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_use.id,
                "content": json.dumps(result),
            })

        messages.append({"role": "user", "content": tool_results})

    # Safety: if we hit max iterations, return what we have
    _history.append({"role": "user", "content": text})
    reply = "I had trouble processing that request. Please try again."
    _history.append({"role": "assistant", "content": reply})
    return reply
