from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import FSInputFile

from visascraper.config import settings
from visascraper.database.db import SessionLocal
from visascraper.database.models import BatchApplication, StayPermit
from visascraper.utils.logger import logger

DELAY_BETWEEN_MESSAGES = 1


@dataclass(slots=True)
class NotificationJob:
    text: str
    chat_id: str
    document_path: Path | None = None


class NotificationService:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[NotificationJob | None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._bot: Bot | None = None

    @property
    def is_running(self) -> bool:
        return self._worker_task is not None and not self._worker_task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        if not settings.telegram_bot_token:
            logger.warning("Сервис уведомлений не запущен: TELEGRAM_BOT_TOKEN не задан")
            return

        self._queue = asyncio.Queue()
        self._bot = Bot(token=settings.telegram_bot_token)
        self._worker_task = asyncio.create_task(self._worker(), name="notification-worker")
        logger.info("Сервис уведомлений Telegram запущен")

    async def stop(self) -> None:
        if self._queue and self._worker_task:
            await self._queue.put(None)
            await self._worker_task

        if self._bot:
            await self._bot.session.close()

        self._queue = None
        self._worker_task = None
        self._bot = None

    async def enqueue(
        self,
        text: str,
        document_path: Path | None = None,
        chat_id: str | None = None,
    ) -> None:
        recipient = chat_id or settings.telegram_channel_id
        if not recipient:
            logger.warning("Уведомление пропущено: TELEGRAM_CHANNEL_ID не задан")
            return

        if not self.is_running:
            await self.start()

        if not self._queue:
            logger.warning("Уведомление пропущено: сервис уведомлений не инициализирован")
            return

        await self._queue.put(
            NotificationJob(
                text=text,
                chat_id=recipient,
                document_path=document_path,
            )
        )

    async def _worker(self) -> None:
        if not self._queue or not self._bot:
            return

        while True:
            job = await self._queue.get()
            try:
                if job is None:
                    break
                await self._deliver(job)
            finally:
                self._queue.task_done()

    async def _deliver(self, job: NotificationJob) -> None:
        if not self._bot:
            return

        while True:
            try:
                document = None
                if job.document_path and job.document_path.exists():
                    document = FSInputFile(job.document_path)

                if document:
                    await self._bot.send_document(
                        chat_id=job.chat_id,
                        document=document,
                        caption=job.text,
                    )
                else:
                    await self._bot.send_message(chat_id=job.chat_id, text=job.text)

                await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                return
            except TelegramRetryAfter as exc:
                logger.warning("Telegram rate limit. Повтор через %s секунд", exc.retry_after)
                await asyncio.sleep(exc.retry_after)
            except Exception as exc:
                logger.error("Ошибка отправки Telegram сообщения: %s", exc)
                return


notification_service = NotificationService()


async def start_notification_service() -> None:
    await notification_service.start()


async def stop_notification_service() -> None:
    await notification_service.stop()


async def send_telegram_message(
    text: str,
    document_path: Path | None = None,
    chat_id: str | None = None,
) -> None:
    await notification_service.enqueue(text=text, document_path=document_path, chat_id=chat_id)


async def notify_approved_users() -> None:
    outgoing_messages: list[tuple[str, Path | None]] = []
    with SessionLocal() as db:
        users = db.query(BatchApplication).all()
        for user in users:
            if user.status == "Approved" and user.last_status not in {"Approved", None}:
                file_path = settings.temp_dir / f"{user.register_number}_batch_application.pdf"
                text = (
                    "Виза одобрена!\n"
                    f"Имя: {user.full_name}\n"
                    f"Статус: {user.status}\n"
                    f"Номер паспорта: {user.passport_number}"
                )
                #outgoing_messages.append((text, file_path if file_path.exists() else None))
                user.last_status = "Approved"
            elif user.status != "Approved" and user.last_status == "Approved":
                user.last_status = user.status
        db.commit()

    for text, document_path in outgoing_messages:
        await send_telegram_message(text, document_path=document_path)


async def notify_approved_stay_permits() -> None:
    outgoing_messages: list[tuple[str, Path | None]] = []
    with SessionLocal() as db:
        permits = db.query(StayPermit).all()
        for permit in permits:
            if permit.status == "Approved" and permit.last_status != "Approved":
                file_path = settings.temp_dir / f"{permit.reg_number}_stay_permit.pdf"
                text = (
                    "ITK (Stay Permit) одобрен!\n"
                    f"Имя: {permit.name}\n"
                    f"Статус: {permit.status}\n"
                    f"Номер паспорта: {permit.passport_number}\n"
                    f"Тип разрешения: {permit.type_of_staypermit}"
                )
                #outgoing_messages.append((text, file_path if file_path.exists() else None))
                permit.last_status = "Approved"
            elif permit.status != "Approved" and permit.last_status == "Approved":
                permit.last_status = permit.status
        db.commit()

    for text, document_path in outgoing_messages:
        await send_telegram_message(text, document_path=document_path)


async def check_birthdays() -> None:
    today = date.today()
    outgoing_messages: list[str] = []
    with SessionLocal() as db:
        users = db.query(BatchApplication).filter(
            BatchApplication.birth_date.is_not(None),
            BatchApplication.birth_date.like(f"{today.strftime('%d/%m')}/%"),
        ).all()
        for user in users:
            outgoing_messages.append(
                (
                    f"Сегодня день рождения у {user.full_name}!\n"
                    f"Тип визы: {user.visa_type}\n"
                    f"Дата рождения: {user.birth_date}"
                )
            )

    for text in outgoing_messages:
        await send_telegram_message(text)


async def check_visa_expirations() -> None:
    today = date.today()
    windows = [40, 5]
    outgoing_messages: list[tuple[str, Path | None]] = []
    with SessionLocal() as db:
        for days_before in windows:
            target_date = (today + timedelta(days=days_before)).strftime("%Y-%m-%d")
            users = db.query(StayPermit).filter(
                StayPermit.expired_date.is_not(None),
                StayPermit.expired_date == target_date,
            ).all()
            for user in users:
                file_path = settings.temp_dir / f"{user.reg_number}_stay_permit.pdf"
                text = (
                    f"ВНИМАНИЕ: у пользователя с паспортом {user.passport_number} виза заканчивается через {days_before} дней!\n"
                    f"Дата окончания: {user.expired_date}\n"
                    f"Тип визы: {user.type_of_staypermit}"
                )
                outgoing_messages.append((text, file_path if file_path.exists() else None))

    for text, document_path in outgoing_messages:
        await send_telegram_message(text, document_path=document_path)
