import asyncio
import os
import signal
import threading
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

# --- Local Infrastructure and Service Imports ---
from database.db import init_db
from infrastructure.event_bus import EventBus
from parser_service.service import ParserService
from bot.handler import bot_router, setup_bot_event_listeners
from utils.logger import logger

class BotRunner:
    """A simple class to manage the bot's lifecycle."""
    def __init__(self, bot: Bot, dispatcher: Dispatcher):
        self.bot = bot
        self.dp = dispatcher

    async def run(self):
        """Starts the bot polling."""
        logger.info("Запуск Telegram бота...")
        await self.dp.start_polling(self.bot)

class Application:
    """
    The main application class that initializes and connects all components.
    Follows a decoupled, event-driven architecture.
    """
    def __init__(self):
        load_dotenv()
        self.event_bus = EventBus()
        self.parser_service = ParserService(self.event_bus)

        # Initialize Bot and Dispatcher
        bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        # Pass the event_bus to the dispatcher so handlers can access it
        storage = MemoryStorage()
        dp = Dispatcher(storage=storage, event_bus=self.event_bus)
        dp.include_router(bot_router)

        self.bot_runner = BotRunner(bot, dp)
        self.bot = bot # Keep a reference to the bot instance

        self.scheduler_thread = None
        self.stop_event = threading.Event()

    def _start_parser_scheduler(self):
        """Runs the parser's internal scheduler in a background thread."""
        logger.info("Запуск фонового потока для планировщика парсера...")
        self.parser_service.run_scheduled_jobs()
        while not self.stop_event.is_set():
            self.stop_event.wait(1)

        scheduler = getattr(self.parser_service.job_scheduler, 'scheduler', None)
        if scheduler and scheduler.running:
             scheduler.shutdown()
        logger.info("Планировщик парсера остановлен.")

    def setup_signal_handlers(self):
        """Sets up handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)

    def shutdown_handler(self, signum, frame):
        """Handles shutdown signals."""
        logger.info("Получен сигнал завершения. Начинаем остановку...")
        self.stop_event.set()
        if self.scheduler_thread:
            self.scheduler_thread.join()

        # Other shutdown logic can be added here
        logger.info("Приложение успешно остановлено.")

    async def run(self):
        """The main entry point to run the application."""
        init_db()
        self.setup_signal_handlers()

        # Get the current asyncio event loop
        loop = asyncio.get_running_loop()

        # Connect the bot and the parser service via the event bus
        setup_bot_event_listeners(self.event_bus, self.bot, loop)
        self.parser_service.setup_subscriptions()

        # Start the background scheduler for the parser
        self.scheduler_thread = threading.Thread(target=self._start_parser_scheduler, daemon=True)
        self.scheduler_thread.start()

        # Run the bot
        await self.bot_runner.run()

if __name__ == "__main__":
    app = Application()
    try:
        asyncio.run(app.run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Приложение остановлено вручную.")
