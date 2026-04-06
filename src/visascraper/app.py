from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from visascraper.bot.handler import bot_router
from visascraper.bot.notification import start_notification_service, stop_notification_service
from visascraper.config import settings
from visascraper.database.db import init_db
from visascraper.jobs import JobScheduler
from visascraper.services.scraper import DataParser
from visascraper.services.sheets import GoogleSheetsManager
from visascraper.services.storage import PDFManager, SessionManager, YandexDiskUploader
from visascraper.utils.logger import logger
from visascraper.utils.scheduler import start_scheduler


class BotRunner:
    def __init__(self, app: "Application"):
        if not settings.telegram_bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")

        self.bot = Bot(token=settings.telegram_bot_token)
        self.dp = Dispatcher(storage=MemoryStorage(), app=app)
        self.dp.include_router(bot_router)

    async def run(self) -> None:
        logger.info("Telegram-бот запущен и готов к работе")
        await self.dp.start_polling(self.bot)


class Application:
    def __init__(self):
        session_manager = SessionManager(settings.proxy)
        pdf_manager = PDFManager(session_manager, YandexDiskUploader(settings.yandex_token))

        self.gs_manager = GoogleSheetsManager()
        self.data_parser = DataParser(session_manager=session_manager, pdf_manager=pdf_manager)
        self.job_scheduler = JobScheduler(self.gs_manager, self.data_parser)
        self.bot_runner = BotRunner(self)
        self.async_scheduler = None

    async def run(self) -> None:
        init_db()
        self.data_parser.main_loop = asyncio.get_running_loop()
        self.job_scheduler.start_scheduler()
        self.async_scheduler = start_scheduler()
        await start_notification_service()

        try:
            await self.bot_runner.run()
        finally:
            await stop_notification_service()
            if self.async_scheduler and self.async_scheduler.running:
                self.async_scheduler.shutdown(wait=False)
            self.job_scheduler.stop_scheduler()
            await self.bot_runner.bot.session.close()
