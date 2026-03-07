"""
Birthday Reminders Agent

Usage:
  python main.py crawl    # Crawl Gmail, extract birthdays, populate DB
  python main.py remind   # Check upcoming birthdays, send Telegram notifications
  python main.py remind --dry-run  # Simulate notifications without sending
"""

import argparse
import logging
import sys

from db.store import init_db, upsert_birthday
from crawler.gmail_crawler import crawl_emails
from core.birthday_extractor import extract_birthdays
from core.reminder_engine import run_reminders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def cmd_crawl():
    logger.info("=== Starting Gmail crawl ===")
    init_db()

    emails = crawl_emails()
    if not emails:
        logger.info("No new emails to process.")
        return

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


def cmd_remind(dry_run: bool = False):
    logger.info("=== Running reminder check (dry_run=%s) ===", dry_run)
    init_db()
    count = run_reminders(dry_run=dry_run)
    logger.info("=== Reminder run complete. %d notifications dispatched. ===", count)


def main():
    parser = argparse.ArgumentParser(description="Birthday Reminders Agent")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("crawl", help="Crawl Gmail and extract birthdays")

    remind_parser = subparsers.add_parser("remind", help="Send upcoming birthday reminders")
    remind_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log notifications without actually sending them",
    )

    args = parser.parse_args()

    if args.command == "crawl":
        cmd_crawl()
    elif args.command == "remind":
        cmd_remind(dry_run=args.dry_run)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
