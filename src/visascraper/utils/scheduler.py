from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bot.notification import notify_approved_users, check_birthdays, check_visa_expirations, notify_approved_stay_permits
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
    
    # Все функции должны быть async — или обёрнуты в asyncio.to_thread
    scheduler.add_job(notify_approved_users, 'interval', minutes=1, coalesce=True)
    scheduler.add_job(notify_approved_stay_permits, 'interval', minutes=1, coalesce=True)
    
    # Если check_birthdays и check_visa_expirations — async:
    scheduler.add_job(check_birthdays, 'cron', hour=5, minute=0)
    scheduler.add_job(check_visa_expirations, 'cron', hour=5, minute=0)
    
    # Если они sync — оборачиваем так:
    # scheduler.add_job(lambda: asyncio.to_thread(check_birthdays), 'cron', hour=5, minute=0)
    
    scheduler.start()
    print("APScheduler (AsyncIO) запущен...")