from __future__ import annotations

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from zoneinfo import ZoneInfo

from visascraper.bot.notification import (
    check_birthdays,
    check_visa_expirations,
    notify_approved_stay_permits,
    notify_approved_users,
)
from visascraper.config import settings
from visascraper.utils.logger import logger


def start_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=ZoneInfo(settings.app_timezone))
    scheduler.add_job(notify_approved_users, "interval", minutes=1, coalesce=True)
    scheduler.add_job(notify_approved_stay_permits, "interval", minutes=1, coalesce=True)
    scheduler.add_job(check_birthdays, "cron", hour=5, minute=0)
    scheduler.add_job(check_visa_expirations, "cron", hour=5, minute=0)
    scheduler.start()
    logger.info("AsyncIOScheduler запущен")
    return scheduler
