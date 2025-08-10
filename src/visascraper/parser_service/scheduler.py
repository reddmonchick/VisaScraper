import os
from typing import List
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from utils.logger import logger as custom_logger
from .google_sheets_manager import GoogleSheetsManager
from .data_parser import DataParser

# Constants for the scheduler
BATCH_PARSE_INTERVAL_MINUTES = int(os.getenv("BATCH_PARSE_INTERVAL_MINUTES", 10))
STAY_PARSE_HOUR = int(os.getenv("STAY_PARSE_HOUR", 7))
STAY_PARSE_MINUTE = int(os.getenv("STAY_PARSE_MINUTE", 0))
GS_BATCH_SHEET_ID = os.getenv("GOOGLE_SHEET_BATCH_ID")

class JobScheduler:
    """Управление задачами планировщика."""
    def __init__(self, gs_manager: GoogleSheetsManager, data_parser: DataParser):
        self.gs_manager = gs_manager
        self.data_parser = data_parser
        self.scheduler = None
        self.cached_batch_application_data: List[List[str]] = []
        self.cached_manager_data: List[List[str]] = []
        self.cached_stay_permit_data: List[List[str]] = []

    def job_first_two(self):
        """Задача для парсинга первых двух аккаунтов."""
        custom_logger.info("Запуск задачи для первых двух аккаунтов")
        try:
            if not self.gs_manager.gc:
                 self.gs_manager._init_client()

            spreadsheet_batch = self.gs_manager.gc.open_by_key(GS_BATCH_SHEET_ID)
            worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
            # Get all values at once to reduce API calls
            all_accounts = worksheet_account.get_all_values()
            if len(all_accounts) < 2: # Check if there's more than a header
                custom_logger.warning("Недостаточно аккаунтов для выполнения задачи (нужно хотя бы 2).")
                return

            names = [row[0] for row in all_accounts[1:]] # col 1
            passwords = [row[1] for row in all_accounts[1:]] # col 2

            if len(names) < 2:
                custom_logger.warning("Недостаточно аккаунтов для выполнения задачи первых двух")
                return

            first_two_names = names[:2]
            first_two_passwords = passwords[:2]

            batch_app, batch_mgr, stay = self.data_parser.parse_accounts(first_two_names, first_two_passwords)

            self.cached_batch_application_data = batch_app
            self.cached_manager_data = batch_mgr
            self.cached_stay_permit_data = stay

            self.gs_manager.write_to_sheet(GS_BATCH_SHEET_ID, batch_app, batch_mgr, stay)

        except Exception as e:
            custom_logger.error(f"[job_first_two] Критическая ошибка: {e}", exc_info=True)

    def job_others(self):
        """Задача для парсинга остальных аккаунтов."""
        custom_logger.info("Запуск задачи для остальных аккаунтов")
        try:
            if not self.gs_manager.gc:
                 self.gs_manager._init_client()

            spreadsheet_batch = self.gs_manager.gc.open_by_key(GS_BATCH_SHEET_ID)
            worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
            all_accounts = worksheet_account.get_all_values()

            if len(all_accounts) <= 2:
                 custom_logger.info("Нет 'остальных' аккаунтов для обработки.")
                 if self.cached_batch_application_data or self.cached_manager_data or self.cached_stay_permit_data:
                      self.gs_manager.write_to_sheet(
                          GS_BATCH_SHEET_ID,
                          self.cached_batch_application_data,
                          self.cached_manager_data,
                          self.cached_stay_permit_data
                      )
                 return

            names = [row[0] for row in all_accounts[1:]] # col 1
            passwords = [row[1] for row in all_accounts[1:]] # col 2

            remaining_names = names[2:]
            remaining_passwords = passwords[2:]

            if not remaining_names:
                custom_logger.info("Нет 'остальных' аккаунтов для обработки.")
                return

            batch_app_new, batch_mgr_new, stay_new = self.data_parser.parse_accounts(remaining_names, remaining_passwords)

            full_batch = self.cached_batch_application_data + batch_app_new
            full_manager = self.cached_manager_data + batch_mgr_new
            full_stay = self.cached_stay_permit_data + stay_new

            self.gs_manager.write_to_sheet(GS_BATCH_SHEET_ID, full_batch, full_manager, full_stay)

        except Exception as e:
            custom_logger.error(f"[job_others] Критическая ошибка: {e}", exc_info=True)

    def start_scheduler(self):
        """Запускает планировщик задач парсинга."""
        self.scheduler = BackgroundScheduler(
            timezone=ZoneInfo("Europe/Moscow"),
            executors={'default': ThreadPoolExecutor(2)}
        )
        self.scheduler.add_job(
            self.job_first_two,
            'interval',
            minutes=BATCH_PARSE_INTERVAL_MINUTES,
            coalesce=True,
            misfire_grace_time=60 * 5
        )
        # NOTE: The job_others is now triggered manually via the event bus
        self.scheduler.start()
        custom_logger.info("Планировщик парсинга (для job_first_two) запущен")
        return self.scheduler
