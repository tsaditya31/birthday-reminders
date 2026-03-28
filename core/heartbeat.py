"""
Background heartbeat thread — runs periodic tasks (auto-crawl, proactive alerts,
daily digest, clarifications, calendar suggestions) alongside the Telegram bot.
"""

import logging
import threading

from config import settings

logger = logging.getLogger(__name__)


class Heartbeat:
    """Daemon thread that runs heartbeat tasks on a configurable interval."""

    def __init__(self):
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="heartbeat")
        self._interval_seconds = settings.heartbeat_interval_hours * 3600

    def start(self):
        logger.info(
            "Heartbeat starting (interval=%.1f hours)",
            settings.heartbeat_interval_hours,
        )
        self._thread.start()

    def stop(self):
        logger.info("Heartbeat stopping...")
        self._stop_event.set()
        self._thread.join(timeout=10)

    def _loop(self):
        from core.heartbeat_tasks import (
            task_auto_crawl,
            task_proactive_alerts,
            task_daily_digest,
            task_clarifications,
            task_calendar_suggestions,
        )

        tasks = [
            ("auto_crawl", task_auto_crawl),
            ("proactive_alerts", task_proactive_alerts),
            ("daily_digest", task_daily_digest),
            ("clarifications", task_clarifications),
            ("calendar_suggestions", task_calendar_suggestions),
        ]

        # Run immediately on first start, then wait
        while not self._stop_event.is_set():
            for name, func in tasks:
                if self._stop_event.is_set():
                    break
                try:
                    logger.info("Heartbeat task: %s", name)
                    func()
                except Exception:
                    logger.exception("Heartbeat task %s failed", name)

            # Interruptible sleep
            self._stop_event.wait(timeout=self._interval_seconds)
