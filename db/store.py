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
