"""
Personal Email Intelligence Agent

Usage:
  python main.py crawl           # Crawl Gmail for birthdays, populate DB (2-year lookback)
  python main.py remind          # Build & send daily digest (birthdays + action items)
  python main.py remind --dry-run  # Simulate digest without sending
  python main.py bot             # Start interactive Telegram chatbot (long-polling)
"""

import argparse
import logging
import sys

from db.store import clear_processed_emails, init_db, upsert_action_item, upsert_birthday
from crawler.gmail_crawler import crawl_action_emails, crawl_emails
from config import settings
from core.action_extractor import extract_action_items
from core.birthday_extractor import extract_birthdays
from core.preferences import get_blocked_senders
from core.digest_engine import build_daily_digest
from notifier.telegram_notifier import send_message
from notifier.telegram_bot import run_polling_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _filter_blocked(emails, blocked):
    """Remove emails from blocked senders."""
    if not blocked:
        return emails
    before = len(emails)
    emails = [e for e in emails if not any(b in e.sender.lower() for b in blocked)]
    filtered = before - len(emails)
    if filtered:
        logger.info("Filtered out %d emails from blocked senders.", filtered)
    return emails


def cmd_crawl():
    """Crawl Gmail for birthday + action-item emails, extract and store."""
    logger.info("=== Starting Gmail crawl ===")
    init_db()
    blocked = get_blocked_senders()

    # ── Birthdays (2-year lookback) ──────────────────────────────────────────
    birthday_emails = crawl_emails()
    if birthday_emails:
        birthday_emails = _filter_blocked(birthday_emails, blocked)
        logger.info("Extracting birthdays from %d emails...", len(birthday_emails))
        birthdays = extract_birthdays(birthday_emails)
        saved = 0
        for b in birthdays:
            upsert_birthday(
                name=b.name,
                birth_month=b.birth_month,
                birth_day=b.birth_day,
                classification=b.classification,
                birth_year=b.birth_year,
                email_source=b.email_source,
                age_at_extraction=b.age_at_event,
            )
            saved += 1
        logger.info("%d birthdays saved/updated.", saved)
    else:
        logger.info("No new birthday emails to process.")

    # ── Action items (180-day lookback) ──────────────────────────────────────
    action_emails = crawl_action_emails(days_back=settings.action_lookback_days, max_per_query=200)
    if action_emails:
        action_emails = _filter_blocked(action_emails, blocked)
        logger.info("Extracting action items from %d emails...", len(action_emails))
        actions = extract_action_items(action_emails)
        saved = 0
        for item in actions:
            upsert_action_item(
                type=item.type,
                title=item.title,
                email_message_id=item.email_message_id,
                description=item.description,
                due_date=item.due_date,
                due_time=item.due_time,
                email_subject=item.email_subject,
                email_from=item.email_from,
                priority=item.priority,
                category=item.category,
                confidence=item.confidence,
                source_snippet=item.source_snippet,
            )
            saved += 1
        logger.info("%d action items saved/updated.", saved)
    else:
        logger.info("No new action emails to process.")

    logger.info("=== Crawl complete. ===")


def cmd_recrawl():
    """Clear action processing state and re-crawl with full lookback."""
    logger.info("=== Starting action item re-crawl ===")
    init_db()
    blocked = get_blocked_senders()

    clear_processed_emails("action")
    logger.info("Cleared action processing state. Re-crawling with %d-day lookback...", settings.action_lookback_days)

    action_emails = crawl_action_emails(days_back=settings.action_lookback_days, max_per_query=200)
    if action_emails:
        action_emails = _filter_blocked(action_emails, blocked)
        logger.info("Extracting action items from %d emails...", len(action_emails))
        actions = extract_action_items(action_emails)
        saved = 0
        for item in actions:
            upsert_action_item(
                type=item.type,
                title=item.title,
                email_message_id=item.email_message_id,
                description=item.description,
                due_date=item.due_date,
                due_time=item.due_time,
                email_subject=item.email_subject,
                email_from=item.email_from,
                priority=item.priority,
                category=item.category,
                confidence=item.confidence,
                source_snippet=item.source_snippet,
            )
            saved += 1
        logger.info("%d action items saved/updated.", saved)
    else:
        logger.info("No action emails found.")

    logger.info("=== Re-crawl complete. ===")


def cmd_bot():
    """Start the interactive Telegram chatbot (long-polling loop)."""
    logger.info("=== Starting Telegram bot ===")
    init_db()
    run_polling_loop()


def cmd_remind(dry_run: bool = False):
    """Build the daily digest (action items + birthdays) and send via Telegram."""
    logger.info("=== Building daily digest (dry_run=%s) ===", dry_run)
    init_db()

    digest = build_daily_digest(dry_run=dry_run)
    if digest:
        if not dry_run:
            send_message(digest)
            logger.info("=== Digest sent. ===")
        else:
            logger.info("[DRY RUN]\n%s", digest)
    else:
        logger.info("Nothing actionable today. No message sent.")


def main():
    parser = argparse.ArgumentParser(description="Personal Email Intelligence Agent")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("crawl", help="Crawl Gmail and extract birthdays (2-year lookback)")
    subparsers.add_parser("recrawl", help="Clear action state and re-crawl (180-day lookback)")
    subparsers.add_parser("bot", help="Start interactive Telegram chatbot (long-polling)")

    remind_parser = subparsers.add_parser(
        "remind", help="Build and send daily digest (action items + birthdays)"
    )
    remind_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log digest to console without sending to Telegram",
    )

    args = parser.parse_args()

    if args.command == "crawl":
        cmd_crawl()
    elif args.command == "recrawl":
        cmd_recrawl()
    elif args.command == "remind":
        cmd_remind(dry_run=args.dry_run)
    elif args.command == "bot":
        cmd_bot()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
