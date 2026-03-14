"""
Chat handler — tool-use agent loop.

Claude gets conversation history + tools to query/modify the user's data,
add pantry items, and set reminders — all via natural language.
"""

import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

import anthropic

from config import settings
from core.item_normalizer import normalize as _normalize_item
from db.store import (
    insert_chat_message,
    get_recent_chat_messages,
    get_current_pantry_items,
    get_recent_purchases,
    get_purchase_history,
    add_manual_pantry_item,
    remove_pantry_item,
    insert_reminder,
    get_pending_reminders,
    get_user_timezone,
)

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

# ── Tool definitions ─────────────────────────────────────────────────────────

_TOOLS = [
    {
        "name": "get_pantry_inventory",
        "description": (
            "Get the user's current pantry/fridge/freezer inventory. "
            "Returns all items currently tracked with their location, quantity, and condition."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_purchase_history",
        "description": (
            "Get the user's recent purchase history from scanned receipts. "
            "Shows items bought, store, date, and price."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back. Default 30.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_shopping_suggestions",
        "description": (
            "Generate smart shopping suggestions by comparing purchase history "
            "against current pantry inventory. Shows what to buy and why."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "add_pantry_item",
        "description": (
            "Add an item to the user's pantry, fridge, or freezer inventory manually."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "item_name": {
                    "type": "string",
                    "description": "Name of the item (e.g. 'whole milk', 'chicken breast').",
                },
                "location": {
                    "type": "string",
                    "enum": ["pantry", "fridge", "freezer"],
                    "description": "Where the item is stored.",
                },
                "category": {
                    "type": "string",
                    "description": "Optional category (e.g. 'dairy', 'produce', 'meat').",
                },
            },
            "required": ["item_name", "location"],
        },
    },
    {
        "name": "remove_pantry_item",
        "description": (
            "Remove an item from the user's pantry/fridge/freezer inventory."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "item_name": {
                    "type": "string",
                    "description": "Name of the item to remove.",
                },
            },
            "required": ["item_name"],
        },
    },
    {
        "name": "set_reminder",
        "description": (
            "Schedule a reminder for the user. The reminder will be sent as a "
            "Telegram message when it's due."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "reminder_text": {
                    "type": "string",
                    "description": "What to remind the user about.",
                },
                "due_at": {
                    "type": "string",
                    "description": (
                        "When to send the reminder, as an ISO 8601 datetime string "
                        "(e.g. '2026-03-15T09:00:00'). Interpreted in the user's timezone."
                    ),
                },
            },
            "required": ["reminder_text", "due_at"],
        },
    },
    {
        "name": "list_reminders",
        "description": "Show the user's pending (unsent) reminders.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


# ── System prompt ────────────────────────────────────────────────────────────

def _build_system_prompt(user_tz: str) -> str:
    now = datetime.now(ZoneInfo(user_tz))
    return (
        "You are a helpful personal assistant integrated with the user's pantry, "
        "purchase history, and reminder system. You can look up their inventory, "
        "suggest what to buy, add or remove items, and set reminders.\n\n"
        "Use the provided tools to answer questions — do NOT guess about inventory "
        "or purchases; always call the relevant tool first.\n\n"
        "When setting reminders, convert relative times (like 'tomorrow morning') "
        "to absolute ISO datetimes based on the current time.\n\n"
        "Keep responses concise and conversational. Use plain text (no HTML, no markdown).\n\n"
        f"Current date/time: {now.strftime('%Y-%m-%d %H:%M %Z')}\n"
        f"User timezone: {user_tz}\n"
    )


# ── Shopping suggestions (inline, avoids cross-project import) ───────────────

def _generate_shopping_suggestions(user_id: int) -> str:
    """Generate shopping suggestions by comparing purchases vs pantry."""
    history = get_purchase_history(user_id, days=90)
    pantry = get_current_pantry_items(user_id)

    pantry_lookup = {item["normalized_name"]: item for item in pantry}
    suggestions = []
    today = date.today()

    for purchase in history:
        norm_name = purchase["normalized_name"]
        purchase_count = purchase["purchase_count"]
        last_purchased = purchase["last_purchased"]
        first_purchased = purchase["first_purchased"]

        if purchase_count < 2:
            continue

        if last_purchased and first_purchased and last_purchased != first_purchased:
            span_days = (last_purchased - first_purchased).days
            avg_interval = span_days / (purchase_count - 1)
        else:
            avg_interval = None

        pantry_item = pantry_lookup.get(norm_name)
        days_since = (today - last_purchased).days if last_purchased else None

        if pantry_item is None:
            suggestions.append(
                f"  [!!] {norm_name.title()} — Bought {purchase_count}x but not in pantry"
            )
        elif pantry_item.get("condition") == "nearly_empty":
            qty = pantry_item.get("estimated_qty", "nearly empty")
            suggestions.append(
                f"  [!] {norm_name.title()} — Running low ({qty})"
            )
        elif avg_interval and days_since and days_since > avg_interval * 1.2:
            suggestions.append(
                f"  [?] {norm_name.title()} — Usually buy every ~{avg_interval:.0f} days, "
                f"last bought {days_since} days ago"
            )

    if not suggestions:
        return (
            "No shopping suggestions yet.\n"
            "Send receipt photos to build purchase history, "
            "then pantry photos so I know what you have!"
        )

    return "Shopping Suggestions:\n" + "\n".join(suggestions)


# ── Tool execution ───────────────────────────────────────────────────────────

def _execute_tool(user_id: str, tool_name: str, tool_input: dict) -> str:
    """Dispatch a tool call to the appropriate function, return a string result."""
    uid = int(user_id)
    try:
        if tool_name == "get_pantry_inventory":
            items = get_current_pantry_items(uid)
            if not items:
                return "Pantry is empty. No items tracked yet."
            lines = []
            current_loc = None
            for item in items:
                loc = item.get("snapshot_type", "unknown")
                if loc != current_loc:
                    current_loc = loc
                    lines.append(f"\n[{loc.upper()}]")
                qty = item.get("estimated_qty", "")
                cond = item.get("condition", "")
                extra = f" ({qty})" if qty else ""
                extra += f" - {cond}" if cond and cond != "good" else ""
                lines.append(f"  - {item['item_name']}{extra}")
            return "\n".join(lines)

        if tool_name == "get_purchase_history":
            days = tool_input.get("days", 30)
            items = get_recent_purchases(uid, days=days)
            if not items:
                return f"No purchases found in the last {days} days."
            lines = [f"Purchases (last {days} days):"]
            for item in items:
                price = f" ${item['price']}" if item.get("price") else ""
                store = f" @ {item['store_name']}" if item.get("store_name") else ""
                dt = f" ({item['purchase_date']})" if item.get("purchase_date") else ""
                lines.append(f"  - {item['item_name']}{price}{store}{dt}")
            return "\n".join(lines)

        if tool_name == "get_shopping_suggestions":
            return _generate_shopping_suggestions(uid)

        if tool_name == "add_pantry_item":
            item_name = tool_input["item_name"]
            location = tool_input["location"]
            category = tool_input.get("category")
            normalized = _normalize_item(item_name)
            add_manual_pantry_item(uid, item_name, normalized, location, category)
            return f"Added '{item_name}' to {location}."

        if tool_name == "remove_pantry_item":
            item_name = tool_input["item_name"]
            normalized = _normalize_item(item_name)
            count = remove_pantry_item(uid, normalized)
            if count:
                return f"Removed '{item_name}' from inventory ({count} item(s))."
            return f"No current item matching '{item_name}' found in inventory."

        if tool_name == "set_reminder":
            reminder_text = tool_input["reminder_text"]
            due_at_str = tool_input["due_at"]
            user_tz = get_user_timezone(uid)
            tz = ZoneInfo(user_tz)
            naive_dt = datetime.fromisoformat(due_at_str)
            if naive_dt.tzinfo is None:
                local_dt = naive_dt.replace(tzinfo=tz)
            else:
                local_dt = naive_dt
            utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
            insert_reminder(user_id, reminder_text, utc_dt.isoformat())
            local_str = local_dt.strftime("%b %d at %I:%M %p %Z")
            return f"Reminder set: '{reminder_text}' — {local_str}"

        if tool_name == "list_reminders":
            reminders = get_pending_reminders(user_id)
            if not reminders:
                return "No pending reminders."
            user_tz = get_user_timezone(uid)
            tz = ZoneInfo(user_tz)
            lines = ["Pending reminders:"]
            for r in reminders:
                due = r["due_at"]
                if isinstance(due, str):
                    due = datetime.fromisoformat(due)
                local_due = due.astimezone(tz)
                lines.append(
                    f"  - {r['reminder_text']} (due {local_due.strftime('%b %d at %I:%M %p')})"
                )
            return "\n".join(lines)

        return f"Unknown tool: {tool_name}"

    except Exception as exc:
        logger.error("Tool execution error (%s): %s", tool_name, exc)
        return f"Error executing {tool_name}: {exc}"


# ── Agent loop ───────────────────────────────────────────────────────────────

_MAX_TOOL_ROUNDS = 5


def handle_message(user_id: str, text: str) -> str:
    """Run the tool-use agent loop and return a plain-text reply."""
    logger.info("Handling message from user %s: %s", user_id, text[:100])

    # Load conversation history
    history = get_recent_chat_messages(user_id, limit=20)
    messages = [{"role": h["role"], "content": h["content"]} for h in history]
    messages.append({"role": "user", "content": text})

    user_tz = get_user_timezone(int(user_id))
    system_prompt = _build_system_prompt(user_tz)

    # Agent loop: call Claude, execute tools, repeat until text response
    for _round in range(_MAX_TOOL_ROUNDS):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=system_prompt,
            tools=_TOOLS,
            messages=messages,
        )

        # If Claude returned a final text response (no tool use)
        if response.stop_reason == "end_turn":
            reply_parts = [
                block.text for block in response.content if block.type == "text"
            ]
            reply = "\n".join(reply_parts) if reply_parts else "I'm not sure how to help with that."
            insert_chat_message(user_id, "user", text)
            insert_chat_message(user_id, "assistant", reply)
            return reply

        # Tool use — execute each tool call and feed results back
        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result_str = _execute_tool(user_id, block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_str,
                    })

            messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason
        reply_parts = [
            block.text for block in response.content if block.type == "text"
        ]
        reply = "\n".join(reply_parts) if reply_parts else "Sorry, something went wrong."
        insert_chat_message(user_id, "user", text)
        insert_chat_message(user_id, "assistant", reply)
        return reply

    # Exhausted max rounds
    reply = "I ran into a loop trying to answer. Could you rephrase your question?"
    insert_chat_message(user_id, "user", text)
    insert_chat_message(user_id, "assistant", reply)
    return reply
