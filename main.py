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

from db.store import init_db, upsert_birthday
from crawler.gmail_crawler import crawl_emails
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


def cmd_crawl():
    """Crawl Gmail for birthday emails (2-year lookback), extract and store birthdays."""
    logger.info("=== Starting Gmail birthday crawl ===")
    init_db()

    emails = crawl_emails()
    if not emails:
        logger.info("No new emails to process.")
        return

    # Filter out emails from blocked senders
    blocked = get_blocked_senders()
    if blocked:
        before = len(emails)
        emails = [
            e for e in emails
            if not any(b in e.sender.lower() for b in blocked)
        ]
        filtered = before - len(emails)
        if filtered:
            logger.info("Filtered out %d emails from blocked senders.", filtered)

    logger.info("Extracting birthdays from %d emails...", len(emails))
    birthdays = extract_birthdays(emails)

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

    logger.info("=== Crawl complete. %d birthdays saved/updated. ===", saved)


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
    elif args.command == "remind":
        cmd_remind(dry_run=args.dry_run)
    elif args.command == "bot":
        cmd_bot()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
