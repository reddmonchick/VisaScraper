from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bot.notification import notify_approved_users, check_birthdays, check_visa_expirations
import asyncio
from zoneinfo import ZoneInfo


def run_async(func):
    """Обёртка для запуска асинхронных функций в существующем event loop"""
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(func(*args, **kwargs))
    return wrapper


async def start_scheduler():
    scheduler = AsyncIOScheduler(timezone=ZoneInfo("Europe/Moscow"))

    scheduler.add_job(notify_approved_users, 'interval', minutes=1, coalesce=True, misfire_grace_time=60 * 5)
    #scheduler.add_job(run_async(check_birthdays), 'cron', hour=4, minute=0)
    #scheduler.add_job(run_async(check_visa_expirations), 'cron', hour=10, minute=0)

    scheduler.add_job(check_birthdays, 'cron', hour=5, minute=5, coalesce=True, misfire_grace_time=60 * 5)
    scheduler.add_job(check_visa_expirations, 'cron',hour=5, minute=5, coalesce=True, misfire_grace_time=60 * 5)


    scheduler.start()
    print("APScheduler запущен...")