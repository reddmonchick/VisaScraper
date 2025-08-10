import os
import json
import time
from datetime import datetime, date
from typing import List

import gspread
from utils.logger import logger as custom_logger

# Constants used by this manager
IDX_BA_ACCOUNT = 10
IDX_MGR_ACCOUNT = 5
IDX_MGR_PAYMENT_DATE = 2
IDX_SP_ACCOUNT = 9
PAYMENT_DATE_FORMAT = "%d-%m-%Y"


class GoogleSheetsManager:
    """Логика работы с Google Sheets."""
    def __init__(self, credentials_path: str):
        self.credentials_path = credentials_path
        self.gc = None
        self._init_client()

    def _init_client(self):
        """Инициализирует клиент gspread."""
        try:
            if not os.path.exists(self.credentials_path):
                 custom_logger.critical(f"Файл учетных данных Google Sheets не найден: {self.credentials_path}")
                 raise FileNotFoundError(f"Файл учетных данных Google Sheets не найден: {self.credentials_path}")

            credentials = json.load(open(self.credentials_path))
            self.gc = gspread.service_account_from_dict(credentials)
        except Exception as e:
            custom_logger.critical(f"Не удалось инициализировать клиент Google Sheets: {e}")
            raise

    def _parse_date_for_sorting(self, date_str: str) -> date:
        """Преобразует строку даты 'DD-MM-YYYY' в datetime.date для сортировки."""
        if not date_str:
            return date.min
        try:
            return datetime.strptime(date_str, PAYMENT_DATE_FORMAT).date()
        except ValueError:
            custom_logger.warning(f"Не удалось преобразовать дату '{date_str}' для сортировки.")
            return date.min

    def write_to_sheet(self,
        spreadsheet_key: str,
        batch_app_data: List[List[str]],
        manager_data: List[List[str]],
        stay_data: List[List[str]]):
        """
        Записывает данные в соответствующие листы Google Sheets.
        """
        max_retries = 3
        base_delay = 5
        chunk_size = 500

        for attempt in range(1, max_retries + 1):
            try:
                if not self.gc:
                    self._init_client()

                spreadsheet = self.gc.open_by_key(spreadsheet_key)

                first_two_accounts = list(set(row[IDX_BA_ACCOUNT] for row in batch_app_data)) if batch_app_data else []

                # --- Batch Application ---
                worksheet_batch = spreadsheet.worksheet('Batch Application')
                all_batch_data = worksheet_batch.get_all_values()
                header_batch = all_batch_data[0] if all_batch_data else []
                existing_batch_data = all_batch_data[1:] if len(all_batch_data) > 1 else []
                filtered_batch_data = [
                    row for row in existing_batch_data if len(row) > IDX_BA_ACCOUNT and row[IDX_BA_ACCOUNT] not in first_two_accounts
                ]
                try:
                    sorted_new_batch_data = sorted(batch_app_data, key=lambda row: self._parse_date_for_sorting(row[6]), reverse=True)
                except Exception:
                    sorted_new_batch_data = batch_app_data

                updated_batch_data = [header_batch] + filtered_batch_data + sorted_new_batch_data
                worksheet_batch.clear()
                if updated_batch_data and any(updated_batch_data):
                    self._append_rows_in_chunks(worksheet_batch, updated_batch_data, chunk_size)
                custom_logger.info("✅ Данные Batch Application обновлены в Google Sheets")

                # --- Manager Worksheet ---
                worksheet_manager = spreadsheet.worksheet('Batch Application(Manager)')
                all_mgr_data = worksheet_manager.get_all_values()
                header_mgr = all_mgr_data[0] if all_mgr_data else []
                existing_mgr_data = all_mgr_data[1:] if len(all_mgr_data) > 1 else []
                filtered_mgr_data = [
                    row for row in existing_mgr_data if len(row) > IDX_MGR_ACCOUNT and row[IDX_MGR_ACCOUNT] not in first_two_accounts
                ]
                try:
                    sorted_new_mgr_data = sorted(manager_data, key=lambda row: self._parse_date_for_sorting(row[IDX_MGR_PAYMENT_DATE]), reverse=True)
                except Exception:
                    sorted_new_mgr_data = manager_data

                updated_mgr_data = [header_mgr] + filtered_mgr_data + sorted_new_mgr_data
                worksheet_manager.clear()
                if updated_mgr_data and any(updated_mgr_data):
                    self._append_rows_in_chunks(worksheet_manager, updated_mgr_data, chunk_size)
                custom_logger.info("✅ Данные Batch Application(Manager) обновлены в Google Sheets")

                # --- Stay Permit ---
                worksheet_stay = spreadsheet.worksheet('StayPermit')
                all_stay_data = worksheet_stay.get_all_values()
                header_stay = all_stay_data[0] if all_stay_data else []
                existing_stay_data = all_stay_data[1:] if len(all_stay_data) > 1 else []
                filtered_stay_data = [
                    row for row in existing_stay_data if len(row) > IDX_SP_ACCOUNT and row[IDX_SP_ACCOUNT] not in first_two_accounts
                ]

                updated_stay_data = [header_stay] + filtered_stay_data + stay_data
                worksheet_stay.clear()
                if updated_stay_data and any(updated_stay_data):
                    self._append_rows_in_chunks(worksheet_stay, updated_stay_data, chunk_size)
                custom_logger.info("✅ Данные Stay Permit обновлены в Google Sheets")

                return

            except Exception as e:
                custom_logger.warning(f"⚠️ Попытка {attempt}/{max_retries} записи в Google Sheets не удалась: {e}")
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    custom_logger.info(f"Повтор через {delay} секунд...")
                    time.sleep(delay)
                else:
                    custom_logger.error(f"❌ Все {max_retries} попытки записи в Google Sheets не удались.")
                    raise

    def _append_rows_in_chunks(self, worksheet, data, chunk_size: int):
        """
        Добавляет данные в лист Google Sheets порциями.
        """
        custom_logger.info(f"Начинаем запись данных в лист '{worksheet.title}' по частям. Всего строк: {len(data)}")
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            if i > 0:
                time.sleep(1)
            try:
                worksheet.append_rows(chunk)
            except Exception as e:
                custom_logger.error(f"❌ Ошибка при записи порции в лист '{worksheet.title}': {e}")
                raise
        custom_logger.info(f"✅ Запись данных в лист '{worksheet.title}' завершена.")
