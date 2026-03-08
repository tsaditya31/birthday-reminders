import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from config import settings


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.database_path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS birthdays (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                name                TEXT NOT NULL,
                birth_month         INTEGER NOT NULL,
                birth_day           INTEGER NOT NULL,
                birth_year          INTEGER,
                classification      TEXT NOT NULL,
                email_source        TEXT,
                age_at_extraction   INTEGER,
                notified_2wk        INTEGER DEFAULT 0,
                notified_1wk        INTEGER DEFAULT 0,
                notified_day        INTEGER DEFAULT 0,
                created_at          TEXT,
                updated_at          TEXT
            );

            CREATE TABLE IF NOT EXISTS processed_emails (
                message_id  TEXT PRIMARY KEY,
                processed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS action_items (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                type             TEXT NOT NULL,
                title            TEXT NOT NULL,
                description      TEXT,
                due_date         TEXT,
                due_time         TEXT,
                email_message_id TEXT,
                email_subject    TEXT,
                email_from       TEXT,
                notified_early   INTEGER DEFAULT 0,
                notified_day     INTEGER DEFAULT 0,
                created_at       TEXT,
                updated_at       TEXT
            );
        """)


# ── Birthday helpers ──────────────────────────────────────────────────────────

def upsert_birthday(
    name: str,
    birth_month: int,
    birth_day: int,
    classification: str,
    birth_year: Optional[int] = None,
    email_source: Optional[str] = None,
    age_at_extraction: Optional[int] = None,
) -> int:
    """Insert or update a birthday record. Dedup on (name, birth_month, birth_day)."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM birthdays WHERE name = ? AND birth_month = ? AND birth_day = ?",
            (name, birth_month, birth_day),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE birthdays SET
                    birth_year = COALESCE(?, birth_year),
                    classification = ?,
                    email_source = COALESCE(?, email_source),
                    age_at_extraction = COALESCE(?, age_at_extraction),
                    updated_at = ?
                WHERE id = ?""",
                (birth_year, classification, email_source, age_at_extraction, now, existing["id"]),
            )
            return existing["id"]
        else:
            cur = conn.execute(
                """INSERT INTO birthdays
                    (name, birth_month, birth_day, birth_year, classification, email_source, age_at_extraction, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (name, birth_month, birth_day, birth_year, classification, email_source, age_at_extraction, now, now),
            )
            return cur.lastrowid


def get_upcoming_birthdays(days_ahead: int = 14) -> list[sqlite3.Row]:
    """Return birthdays whose (month, day) falls within the next `days_ahead` days."""
    from datetime import date, timedelta

    today = date.today()
    targets = [(today + timedelta(days=i)) for i in range(days_ahead + 1)]
    month_days = [(d.month, d.day) for d in targets]

    with get_db() as conn:
        rows = conn.execute("SELECT * FROM birthdays").fetchall()

    upcoming = []
    for row in rows:
        if (row["birth_month"], row["birth_day"]) in month_days:
            upcoming.append(row)
    return upcoming


def mark_notified(birthday_id: int, flag: str):
    """flag: '2wk' | '1wk' | 'day'"""
    col = {"2wk": "notified_2wk", "1wk": "notified_1wk", "day": "notified_day"}[flag]
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(f"UPDATE birthdays SET {col} = 1, updated_at = ? WHERE id = ?", (now, birthday_id))


def reset_annual_flags():
    """Call on Jan 1 to reset notification flags for a new year."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute("UPDATE birthdays SET notified_2wk=0, notified_1wk=0, notified_day=0, updated_at=?", (now,))


# ── Action-item helpers ───────────────────────────────────────────────────────

def upsert_action_item(
    type: str,
    title: str,
    email_message_id: str,
    description: Optional[str] = None,
    due_date: Optional[str] = None,
    due_time: Optional[str] = None,
    email_subject: Optional[str] = None,
    email_from: Optional[str] = None,
) -> int:
    """Insert or update an action item. Dedup on (email_message_id, type)."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM action_items WHERE email_message_id = ? AND type = ?",
            (email_message_id, type),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE action_items SET
                    title = ?,
                    description = COALESCE(?, description),
                    due_date = COALESCE(?, due_date),
                    due_time = COALESCE(?, due_time),
                    updated_at = ?
                WHERE id = ?""",
                (title, description, due_date, due_time, now, existing["id"]),
            )
            return existing["id"]
        else:
            cur = conn.execute(
                """INSERT INTO action_items
                    (type, title, description, due_date, due_time,
                     email_message_id, email_subject, email_from, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (type, title, description, due_date, due_time,
                 email_message_id, email_subject, email_from, now, now),
            )
            return cur.lastrowid


def get_upcoming_action_items(days_ahead: int = 3) -> list[sqlite3.Row]:
    """Return action items with due_date within the next days_ahead days."""
    from datetime import date, timedelta

    today = date.today()
    date_max = today + timedelta(days=days_ahead)
    with get_db() as conn:
        return conn.execute(
            """SELECT * FROM action_items
               WHERE due_date IS NOT NULL
                 AND due_date >= ?
                 AND due_date <= ?""",
            (today.isoformat(), date_max.isoformat()),
        ).fetchall()


def get_unnotified_urgent() -> list[sqlite3.Row]:
    """Return urgent_reply items that have not yet been notified."""
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM action_items WHERE type = 'urgent_reply' AND notified_day = 0"
        ).fetchall()


def mark_action_notified(item_id: int, flag: str):
    """flag: 'early' | 'day'"""
    col = {"early": "notified_early", "day": "notified_day"}[flag]
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            f"UPDATE action_items SET {col} = 1, updated_at = ? WHERE id = ?",
            (now, item_id),
        )


# ── Processed-email helpers ───────────────────────────────────────────────────

def is_email_processed(message_id: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM processed_emails WHERE message_id = ?", (message_id,)
        ).fetchone()
    return row is not None


def mark_email_processed(message_id: str):
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_emails (message_id, processed_at) VALUES (?, ?)",
            (message_id, now),
        )
