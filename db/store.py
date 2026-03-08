from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

from config import settings


@contextmanager
def get_db():
    conn = psycopg2.connect(settings.database_url, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS birthdays (
                    id                  SERIAL PRIMARY KEY,
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
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_emails (
                    message_id   TEXT PRIMARY KEY,
                    processed_at TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS action_items (
                    id               SERIAL PRIMARY KEY,
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
                )
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
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM birthdays WHERE name = %s AND birth_month = %s AND birth_day = %s",
                (name, birth_month, birth_day),
            )
            existing = cur.fetchone()

            if existing:
                cur.execute(
                    """UPDATE birthdays SET
                        birth_year = COALESCE(%s, birth_year),
                        classification = %s,
                        email_source = COALESCE(%s, email_source),
                        age_at_extraction = COALESCE(%s, age_at_extraction),
                        updated_at = %s
                    WHERE id = %s""",
                    (birth_year, classification, email_source, age_at_extraction, now, existing["id"]),
                )
                return existing["id"]
            else:
                cur.execute(
                    """INSERT INTO birthdays
                        (name, birth_month, birth_day, birth_year, classification,
                         email_source, age_at_extraction, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (name, birth_month, birth_day, birth_year, classification,
                     email_source, age_at_extraction, now, now),
                )
                return cur.fetchone()["id"]


def get_upcoming_birthdays(days_ahead: int = 14) -> list[dict]:
    """Return birthdays whose (month, day) falls within the next `days_ahead` days."""
    from datetime import date, timedelta

    today = date.today()
    targets = [(today + timedelta(days=i)) for i in range(days_ahead + 1)]
    month_days = set((d.month, d.day) for d in targets)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM birthdays")
            rows = cur.fetchall()

    return [row for row in rows if (row["birth_month"], row["birth_day"]) in month_days]


def mark_notified(birthday_id: int, flag: str):
    """flag: '2wk' | '1wk' | 'day'"""
    col = {"2wk": "notified_2wk", "1wk": "notified_1wk", "day": "notified_day"}[flag]
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE birthdays SET {col} = 1, updated_at = %s WHERE id = %s",
                (now, birthday_id),
            )


def reset_annual_flags():
    """Call on Jan 1 to reset notification flags for a new year."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE birthdays SET notified_2wk=0, notified_1wk=0, notified_day=0, updated_at=%s",
                (now,),
            )


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
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM action_items WHERE email_message_id = %s AND type = %s",
                (email_message_id, type),
            )
            existing = cur.fetchone()

            if existing:
                cur.execute(
                    """UPDATE action_items SET
                        title = %s,
                        description = COALESCE(%s, description),
                        due_date = COALESCE(%s, due_date),
                        due_time = COALESCE(%s, due_time),
                        updated_at = %s
                    WHERE id = %s""",
                    (title, description, due_date, due_time, now, existing["id"]),
                )
                return existing["id"]
            else:
                cur.execute(
                    """INSERT INTO action_items
                        (type, title, description, due_date, due_time,
                         email_message_id, email_subject, email_from, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (type, title, description, due_date, due_time,
                     email_message_id, email_subject, email_from, now, now),
                )
                return cur.fetchone()["id"]


def get_upcoming_action_items(days_ahead: int = 3) -> list[dict]:
    """Return action items with due_date within the next days_ahead days."""
    from datetime import date, timedelta

    today = date.today()
    date_max = today + timedelta(days=days_ahead)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT * FROM action_items
                   WHERE due_date IS NOT NULL
                     AND due_date >= %s
                     AND due_date <= %s""",
                (today.isoformat(), date_max.isoformat()),
            )
            return cur.fetchall()


def get_unnotified_urgent() -> list[dict]:
    """Return urgent_reply items that have not yet been notified."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM action_items WHERE type = 'urgent_reply' AND notified_day = 0"
            )
            return cur.fetchall()


def mark_action_notified(item_id: int, flag: str):
    """flag: 'early' | 'day'"""
    col = {"early": "notified_early", "day": "notified_day"}[flag]
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE action_items SET {col} = 1, updated_at = %s WHERE id = %s",
                (now, item_id),
            )


# ── Processed-email helpers ───────────────────────────────────────────────────

def is_email_processed(message_id: str) -> bool:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM processed_emails WHERE message_id = %s", (message_id,)
            )
            return cur.fetchone() is not None


def mark_email_processed(message_id: str):
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_emails (message_id, processed_at) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (message_id, now),
            )
