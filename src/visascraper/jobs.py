from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
import time

from gspread.exceptions import APIError

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from apscheduler.schedulers.background import BackgroundScheduler

from visascraper.config import settings
from visascraper.services.scraper import DataParser
from visascraper.services.sheets import GoogleSheetsManager
from visascraper.utils.logger import logger

ProgressCallback = Callable[[int, int, str, int, int, int], None]


class AccountsReadError(RuntimeError):
    """Не удалось получить список аккаунтов из Google Sheets после повторных попыток."""


class TelegramProgressReporter:
    def __init__(self, title: str, loop: asyncio.AbstractEventLoop | None):
        self.title = title
        self.loop = loop
        self.recipient_ids = tuple(dict.fromkeys(settings.admin_user_ids))
        self.bot: Bot | None = None
        self.message_ids: dict[str, int] = {}
        self.last_text: str | None = None
        self._last_error_by_chat: dict[str, str] = {}

        if settings.telegram_bot_token and self.recipient_ids and self.loop and self.loop.is_running():
            self.bot = Bot(token=settings.telegram_bot_token)

    @property
    def enabled(self) -> bool:
        return self.bot is not None and self.loop is not None

    def _log_chat_issue_once(self, chat_id: str, message: str) -> None:
        if self._last_error_by_chat.get(chat_id) == message:
            return
        self._last_error_by_chat[chat_id] = message
        logger.warning(message)

    def _clear_chat_issue(self, chat_id: str) -> None:
        self._last_error_by_chat.pop(chat_id, None)

    async def _send_fresh_message(self, chat_id: str, text: str) -> bool:
        if not self.bot:
            return False

        try:
            message = await self.bot.send_message(chat_id=chat_id, text=text)
            self.message_ids[chat_id] = message.message_id
            self._clear_chat_issue(chat_id)
            return True
        except TelegramBadRequest as exc:
            if "chat not found" in str(exc).lower():
                self._log_chat_issue_once(
                    chat_id,
                    f"Админу {chat_id} пока нельзя отправить progress-сообщение: чат с ботом не найден. Пользователь сможет получать уведомления после /start.",
                )
            else:
                self._log_chat_issue_once(
                    chat_id,
                    f"Не удалось отправить progress-сообщение для {chat_id}: {exc}",
                )
        except TelegramForbiddenError as exc:
            self._log_chat_issue_once(
                chat_id,
                f"Админ {chat_id} временно недоступен для progress-сообщений: {exc}",
            )
        except Exception as exc:
            self._log_chat_issue_once(
                chat_id,
                f"Не удалось отправить progress-сообщение для {chat_id}: {exc}",
            )
        return False

    async def _send_or_edit(self, text: str) -> None:
        if not self.bot:
            return

        for chat_id in self.recipient_ids:
            message_id = self.message_ids.get(chat_id)
            if message_id is None:
                await self._send_fresh_message(chat_id, text)
                continue

            try:
                await self.bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id)
                self._clear_chat_issue(chat_id)
            except TelegramBadRequest as exc:
                error_text = str(exc).lower()
                if "message is not modified" in error_text:
                    continue
                if "chat not found" in error_text:
                    self._log_chat_issue_once(
                        chat_id,
                        f"Админу {chat_id} пока нельзя отправить progress-сообщение: чат с ботом не найден. Пользователь сможет получать уведомления после /start.",
                    )
                    continue
                if "message to edit not found" in error_text or "message can't be edited" in error_text:
                    self.message_ids.pop(chat_id, None)
                    await self._send_fresh_message(chat_id, text)
                    continue
                self._log_chat_issue_once(
                    chat_id,
                    f"Не удалось обновить progress-сообщение для {chat_id}: {exc}",
                )
            except TelegramForbiddenError as exc:
                self._log_chat_issue_once(
                    chat_id,
                    f"Админ {chat_id} временно недоступен для progress-сообщений: {exc}",
                )
            except Exception as exc:
                self._log_chat_issue_once(
                    chat_id,
                    f"Не удалось обновить progress-сообщение для {chat_id}: {exc}",
                )

    def publish(self, text: str) -> None:
        if not self.enabled or text == self.last_text:
            return
        self.last_text = text
        asyncio.run_coroutine_threadsafe(self._send_or_edit(text), self.loop)

    def start(self, total: int) -> None:
        if not self.enabled:
            return
        if total <= 0:
            self.publish(
                f"ℹ️ {self.title}\n"
                "Аккаунты для парсинга не найдены."
            )
            return
        self.publish(
            f"🔄 {self.title}\n"
            "Последний аккаунт: подготовка\n"
            f"Прогресс: 0/{total}\n"
            "Спарсилось: 0\n"
            f"Осталось: {total}\n"
            "Batch записей: 0\n"
            "Stay Permit записей: 0"
        )

    def update(
        self,
        processed: int,
        total: int,
        current_account: str,
        remaining: int,
        batch_count: int,
        stay_count: int,
    ) -> None:
        if not self.enabled:
            return
        self.publish(
            f"🔄 {self.title}\n"
            f"Последний аккаунт: {current_account}\n"
            f"Прогресс: {processed}/{total}\n"
            f"Спарсилось: {processed}\n"
            f"Осталось: {remaining}\n"
            f"Batch записей: {batch_count}\n"
            f"Stay Permit записей: {stay_count}"
        )

    def finish(self, success: bool, error: Exception | None = None) -> None:
        if not self.enabled:
            return

        if success:
            base_text = self.last_text or f"🔄 {self.title}"
            if "\n" in base_text:
                final_text = f"✅ Завершено\n{base_text.split(chr(10), 1)[1]}"
            else:
                final_text = f"✅ {self.title} завершён"
        else:
            final_text = f"❌ {self.title}: ошибка\n{error}" if error else f"❌ {self.title}: ошибка"

        future = asyncio.run_coroutine_threadsafe(self._send_or_edit(final_text), self.loop)
        with suppress(Exception):
            future.result(timeout=15)
        self.last_text = final_text

        future = asyncio.run_coroutine_threadsafe(self._close_bot(), self.loop)
        with suppress(Exception):
            future.result(timeout=15)

    async def _close_bot(self) -> None:
        if self.bot:
            await self.bot.session.close()


class JobScheduler:
    def __init__(self, gs_manager: GoogleSheetsManager, data_parser: DataParser):
        self.gs_manager = gs_manager
        self.data_parser = data_parser
        self.scheduler = BackgroundScheduler(timezone=settings.app_timezone)

    @staticmethod
    def _is_retryable_accounts_error(exc: Exception) -> bool:
        if isinstance(exc, APIError):
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code in {429, 500, 502, 503, 504}:
                return True

        error_text = str(exc).lower()
        retryable_fragments = (
            "remote end closed connection without response",
            "connection aborted",
            "connection reset",
            "temporarily unavailable",
            "timed out",
            "timeout",
            "503",
            "502",
            "504",
            "429",
        )
        return any(fragment in error_text for fragment in retryable_fragments)

    def _get_accounts(self) -> list[tuple[str, str]]:
        max_attempts = 4
        last_exc: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                accounts = self.gs_manager.get_account_credentials()
                if not accounts:
                    logger.warning("В таблице аккаунтов не найдено ни одной пары логин/пароль")
                return accounts
            except Exception as exc:
                last_exc = exc
                if attempt < max_attempts and self._is_retryable_accounts_error(exc):
                    delay = attempt * 5
                    logger.warning(
                        "Не удалось прочитать таблицу аккаунтов (попытка %s/%s): %s. Повтор через %s сек.",
                        attempt,
                        max_attempts,
                        exc,
                        delay,
                    )
                    try:
                        self.gs_manager = GoogleSheetsManager()
                        logger.info("Клиент Google Sheets переинициализирован перед повторной попыткой %s/%s", attempt + 1, max_attempts)
                    except Exception as reinit_exc:
                        logger.warning("Не удалось переинициализировать клиент Google Sheets перед повторной попыткой: %s", reinit_exc)
                    time.sleep(delay)
                    logger.info("Повторяем чтение таблицы аккаунтов: попытка %s/%s", attempt + 1, max_attempts)
                    continue

                logger.error("Не удалось прочитать таблицу аккаунтов: %s", exc)
                raise AccountsReadError(str(exc)) from exc

        raise AccountsReadError(str(last_exc) if last_exc else "Неизвестная ошибка чтения таблицы аккаунтов")

    def _run_accounts(
        self,
        accounts: list[tuple[str, str]],
        label: str,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        if not accounts:
            logger.warning("Для задачи '%s' нет аккаунтов", label)
            return

        names = [name for name, _ in accounts]
        passwords = [password for _, password in accounts]
        batch_rows, manager_rows, stay_rows = self.data_parser.parse_accounts(
            names,
            passwords,
            progress_callback=progress_callback,
        )
        self.gs_manager.write_to_sheet(batch_rows, manager_rows, stay_rows)
        logger.info("Задача '%s' успешно завершена", label)

    def _run_with_telegram_progress(
        self,
        accounts: list[tuple[str, str]],
        label: str,
        title: str,
        reporter: TelegramProgressReporter | None = None,
    ) -> None:
        reporter = reporter or TelegramProgressReporter(title=title, loop=self.data_parser.main_loop)
        try:
            reporter.start(total=len(accounts))
            self._run_accounts(accounts, label, progress_callback=reporter.update)
        except Exception as exc:
            reporter.finish(success=False, error=exc)
            raise
        else:
            reporter.finish(success=True)

    def job_first_two(self) -> None:
        title = "Парсинг приоритетных аккаунтов по расписанию"
        reporter = TelegramProgressReporter(title=title, loop=self.data_parser.main_loop)
        try:
            accounts = self._get_accounts()[:2]
            self._run_with_telegram_progress(
                accounts[::-1],
                "priority_accounts",
                title,
                reporter=reporter,
            )
        except AccountsReadError as exc:
            logger.error("Задача 'priority_accounts' остановлена: не удалось получить аккаунты")
            reporter.finish(success=False, error=exc)
        except Exception:
            raise

    def job_others(self, progress_callback: ProgressCallback | None = None) -> None:
        accounts = self._get_accounts()[2:]
        self._run_accounts(accounts[::-1], "secondary_accounts", progress_callback=progress_callback)

    def start_scheduler(self) -> None:
        self.scheduler.add_job(
            self.job_first_two,
            "date",
            run_date=datetime.now(),
            id="priority_accounts_initial",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.job_first_two,
            "interval",
            minutes=settings.batch_parse_interval_minutes,
            id="priority_accounts_interval",
            replace_existing=True,
            misfire_grace_time=300,
            max_instances=1,
        )
        self.scheduler.start()
        logger.info(
            "Планировщик запущен: первые два аккаунта сразу и каждые %s минут",
            settings.batch_parse_interval_minutes,
        )
    def stop_scheduler(self) -> None:
        if not self.scheduler.running:
            return
        self.scheduler.shutdown(wait=False)
        logger.info("Планировщик JobScheduler остановлен")
