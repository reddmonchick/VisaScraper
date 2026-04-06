from __future__ import annotations

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from visascraper.config import settings

if not settings.telegram_bot_token:
    raise ValueError("TELEGRAM_BOT_TOKEN не найден")

bot = Bot(token=settings.telegram_bot_token)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
