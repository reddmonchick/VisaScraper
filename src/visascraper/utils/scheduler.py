from apscheduler.schedulers.background import BackgroundScheduler
from src.visascraper.bot.notification import notify_approved_users, check_birthdays, check_visa_expirations
import asyncio
from zoneinfo import ZoneInfo


def run_async(func):
    """Обёртка для запуска асинхронных функций в APScheduler"""
    def wrapper(*args, **kwargs):
        asyncio.run(func(*args, **kwargs))
    return wrapper


def start_scheduler():
    scheduler = BackgroundScheduler(timezone=ZoneInfo("Europe/Moscow"))

    scheduler.add_job(run_async(notify_approved_users), 'interval', minutes=10)
    scheduler.add_job(run_async(check_birthdays), 'cron', hour=4, minute=0)
    scheduler.add_job(run_async(check_visa_expirations), 'cron', hour=10, minute=0)

    scheduler.start()
    print("APScheduler запущен...")