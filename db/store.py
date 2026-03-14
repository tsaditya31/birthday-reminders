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


def _add_column_if_missing(cur, table: str, column: str, col_type: str):
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    if not cur.fetchone():
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


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
                    dismissed           BOOLEAN DEFAULT FALSE,
                    notified_2wk        INTEGER DEFAULT 0,
                    notified_1wk        INTEGER DEFAULT 0,
                    notified_day        INTEGER DEFAULT 0,
                    created_at          TEXT,
                    updated_at          TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_emails (
                    message_id          TEXT NOT NULL,
                    processing_type     TEXT NOT NULL DEFAULT 'birthday',
                    extraction_version  INTEGER DEFAULT 1,
                    processed_at        TEXT NOT NULL,
                    PRIMARY KEY (message_id, processing_type)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_preferences (
                    id          SERIAL PRIMARY KEY,
                    category    TEXT NOT NULL,
                    rule_text   TEXT NOT NULL,
                    source_msg  TEXT,
                    active      BOOLEAN DEFAULT TRUE,
                    created_at  TEXT,
                    updated_at  TEXT
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
                    priority         TEXT DEFAULT 'normal',
                    category         TEXT,
                    confidence       REAL,
                    source_snippet   TEXT,
                    email_message_id TEXT,
                    email_subject    TEXT,
                    email_from       TEXT,
                    dismissed        BOOLEAN DEFAULT FALSE,
                    notified_early   INTEGER DEFAULT 0,
                    notified_day     INTEGER DEFAULT 0,
                    created_at       TEXT,
                    updated_at       TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS action_feedback (
                    id                SERIAL PRIMARY KEY,
                    feedback_type     TEXT NOT NULL,
                    action_item_id    INTEGER,
                    email_message_id  TEXT,
                    user_comment      TEXT,
                    original_type     TEXT,
                    corrected_type    TEXT,
                    original_date     TEXT,
                    corrected_date    TEXT,
                    created_at        TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS learned_queries (
                    id          SERIAL PRIMARY KEY,
                    query       TEXT NOT NULL UNIQUE,
                    source      TEXT,
                    created_at  TEXT
                )
            """)

            # Migrations for existing tables
            _add_column_if_missing(cur, "birthdays", "dismissed", "BOOLEAN DEFAULT FALSE")
            _add_column_if_missing(cur, "action_items", "dismissed", "BOOLEAN DEFAULT FALSE")
            _add_column_if_missing(cur, "action_items", "priority", "TEXT DEFAULT 'normal'")
            _add_column_if_missing(cur, "action_items", "category", "TEXT")
            _add_column_if_missing(cur, "action_items", "confidence", "REAL")
            _add_column_if_missing(cur, "action_items", "source_snippet", "TEXT")

            # Migrate processed_emails: add processing_type + extraction_version columns
            # and convert from single-column PK to composite PK
            _add_column_if_missing(cur, "processed_emails", "processing_type", "TEXT NOT NULL DEFAULT 'birthday'")
            _add_column_if_missing(cur, "processed_emails", "extraction_version", "INTEGER DEFAULT 1")
            # Migrate PK: if old single-column PK exists, recreate as composite
            cur.execute("""
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = 'processed_emails'
                  AND constraint_type = 'PRIMARY KEY'
                  AND constraint_name = 'processed_emails_pkey'
            """)
            if cur.fetchone():
                # Check if the PK already includes processing_type
                cur.execute("""
                    SELECT COUNT(*) as col_count
                    FROM information_schema.key_column_usage
                    WHERE table_name = 'processed_emails'
                      AND constraint_name = 'processed_emails_pkey'
                """)
                col_count = cur.fetchone()["col_count"]
                if col_count == 1:
                    cur.execute("ALTER TABLE processed_emails DROP CONSTRAINT processed_emails_pkey")
                    cur.execute("ALTER TABLE processed_emails ADD PRIMARY KEY (message_id, processing_type)")


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
            cur.execute("SELECT * FROM birthdays WHERE dismissed = FALSE")
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
    priority: Optional[str] = None,
    category: Optional[str] = None,
    confidence: Optional[float] = None,
    source_snippet: Optional[str] = None,
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
                        priority = COALESCE(%s, priority),
                        category = COALESCE(%s, category),
                        confidence = COALESCE(%s, confidence),
                        source_snippet = COALESCE(%s, source_snippet),
                        updated_at = %s
                    WHERE id = %s""",
                    (title, description, due_date, due_time, priority, category,
                     confidence, source_snippet, now, existing["id"]),
                )
                return existing["id"]
            else:
                cur.execute(
                    """INSERT INTO action_items
                        (type, title, description, due_date, due_time, priority, category,
                         confidence, source_snippet,
                         email_message_id, email_subject, email_from, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (type, title, description, due_date, due_time, priority, category,
                     confidence, source_snippet,
                     email_message_id, email_subject, email_from, now, now),
                )
                return cur.fetchone()["id"]


def get_upcoming_action_items(days_ahead: int = 3) -> list[dict]:
    """Return action items with due_date within the next days_ahead days."""
    from datetime import date, timedelta

    today = date.today()
    date_max = today + timedelta(days=days_ahead)
    return get_action_items_between(today.isoformat(), date_max.isoformat())


def get_action_items_between(start_date: str, end_date: str) -> list[dict]:
    """Return action items with due_date between start_date and end_date (inclusive)."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT * FROM action_items
                   WHERE due_date IS NOT NULL
                     AND due_date >= %s
                     AND due_date <= %s
                     AND dismissed = FALSE""",
                (start_date, end_date),
            )
            return cur.fetchall()


def get_unnotified_urgent() -> list[dict]:
    """Return urgent_reply items that have not yet been notified."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM action_items WHERE type = 'urgent_reply' AND notified_day = 0 AND dismissed = FALSE"
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

def is_email_processed(message_id: str, processing_type: str = "birthday") -> bool:
    current_version = settings.extraction_version
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT 1 FROM processed_emails
                   WHERE message_id = %s
                     AND processing_type = %s
                     AND extraction_version >= %s""",
                (message_id, processing_type, current_version),
            )
            return cur.fetchone() is not None


def mark_email_processed(message_id: str, processing_type: str = "birthday"):
    now = datetime.now(timezone.utc).isoformat()
    current_version = settings.extraction_version
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO processed_emails (message_id, processing_type, extraction_version, processed_at)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (message_id, processing_type)
                   DO UPDATE SET extraction_version = %s, processed_at = %s""",
                (message_id, processing_type, current_version, now, current_version, now),
            )


def clear_processed_emails(processing_type: str):
    """Delete all processed-email records for a given type, allowing full re-crawl."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_emails WHERE processing_type = %s",
                (processing_type,),
            )


# ── Preference helpers ───────────────────────────────────────────────────────

def add_preference(category: str, rule_text: str, source_msg: Optional[str] = None) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO user_preferences (category, rule_text, source_msg, created_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (category, rule_text, source_msg, now, now),
            )
            return cur.fetchone()["id"]


def get_active_preferences(category: Optional[str] = None) -> list[dict]:
    with get_db() as conn:
        with conn.cursor() as cur:
            if category:
                cur.execute(
                    "SELECT * FROM user_preferences WHERE active = TRUE AND category = %s ORDER BY created_at",
                    (category,),
                )
            else:
                cur.execute("SELECT * FROM user_preferences WHERE active = TRUE ORDER BY created_at")
            return cur.fetchall()


def deactivate_preference(pref_id: int):
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE user_preferences SET active = FALSE, updated_at = %s WHERE id = %s",
                (now, pref_id),
            )


# ── Dismiss helpers ──────────────────────────────────────────────────────────

def dismiss_birthday(birthday_id: int):
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE birthdays SET dismissed = TRUE, updated_at = %s WHERE id = %s",
                (now, birthday_id),
            )


def dismiss_action_item(item_id: int):
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            # Get item details for implicit feedback
            cur.execute("SELECT type, email_message_id FROM action_items WHERE id = %s", (item_id,))
            item = cur.fetchone()
            cur.execute(
                "UPDATE action_items SET dismissed = TRUE, updated_at = %s WHERE id = %s",
                (now, item_id),
            )
            # Record implicit not_useful feedback
            if item:
                cur.execute(
                    """INSERT INTO action_feedback
                        (feedback_type, action_item_id, email_message_id, original_type, created_at)
                       VALUES (%s, %s, %s, %s, %s)""",
                    ("not_useful", item_id, item.get("email_message_id"), item.get("type"), now),
                )


def find_birthday_by_name(name: str) -> Optional[dict]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM birthdays WHERE LOWER(name) LIKE %s AND dismissed = FALSE ORDER BY id LIMIT 1",
                (f"%{name.lower()}%",),
            )
            return cur.fetchone()


def find_action_item_by_title(title: str) -> Optional[dict]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM action_items WHERE LOWER(title) LIKE %s AND dismissed = FALSE ORDER BY id LIMIT 1",
                (f"%{title.lower()}%",),
            )
            return cur.fetchone()


# ── Feedback helpers ────────────────────────────────────────────────────────

def insert_feedback(
    feedback_type: str,
    action_item_id: Optional[int] = None,
    email_message_id: Optional[str] = None,
    user_comment: Optional[str] = None,
    original_type: Optional[str] = None,
    corrected_type: Optional[str] = None,
    original_date: Optional[str] = None,
    corrected_date: Optional[str] = None,
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO action_feedback
                    (feedback_type, action_item_id, email_message_id, user_comment,
                     original_type, corrected_type, original_date, corrected_date, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (feedback_type, action_item_id, email_message_id, user_comment,
                 original_type, corrected_type, original_date, corrected_date, now),
            )
            return cur.fetchone()["id"]


def get_recent_feedback(limit: int = 10) -> list[dict]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT af.*, ai.title as item_title, ai.type as item_type,
                          ai.email_subject, ai.description as item_description
                   FROM action_feedback af
                   LEFT JOIN action_items ai ON af.action_item_id = ai.id
                   ORDER BY af.created_at DESC LIMIT %s""",
                (limit,),
            )
            return cur.fetchall()


def get_feedback_stats(days: int = 30) -> dict:
    """Return counts of useful vs not_useful feedback over the last N days."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT feedback_type, COUNT(*) as cnt
                   FROM action_feedback
                   WHERE created_at >= %s
                   GROUP BY feedback_type""",
                (cutoff,),
            )
            rows = cur.fetchall()
    stats = {r["feedback_type"]: r["cnt"] for r in rows}
    return stats


# ── Learned query helpers ───────────────────────────────────────────────────

def add_learned_query(query: str, source: Optional[str] = None) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO learned_queries (query, source, created_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (query) DO NOTHING
                   RETURNING id""",
                (query, source, now),
            )
            row = cur.fetchone()
            return row["id"] if row else 0


def get_learned_queries() -> list[str]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT query FROM learned_queries ORDER BY created_at")
            return [r["query"] for r in cur.fetchall()]
