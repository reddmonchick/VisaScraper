from __future__ import annotations

import json
import time
from datetime import date, datetime
from typing import Iterable

import gspread

from visascraper.config import settings
from visascraper.dto import (
    BATCH_APPLICATION_HEADERS,
    BATCH_MANAGER_HEADERS,
    IDX_BA_ACCOUNT,
    IDX_MGR_ACCOUNT,
    IDX_MGR_PAYMENT_DATE,
    IDX_SP_ACCOUNT,
    PAYMENT_DATE_FORMAT,
    STAY_PERMIT_HEADERS,
)
from visascraper.utils.logger import logger
from visascraper.utils.sheets_rotator import ExistingSpreadsheetRequiredError, ensure_valid_spreadsheet


class GoogleSheetsManager:
    def __init__(self):
        self.gc = self._init_client()

    def _init_client(self) -> gspread.Client:
        if settings.google_service_account_json:
            credentials = json.loads(settings.google_service_account_json)
            logger.info("Google Sheets инициализирован через GOOGLE_SERVICE_ACCOUNT_JSON")
            return gspread.service_account_from_dict(credentials)

        credentials_file = settings.google_service_account_file
        if not credentials_file.exists():
            raise FileNotFoundError(
                "Не найден файл сервисного аккаунта Google. "
                f"Ожидаемый путь: {credentials_file}. "
                "Укажите GOOGLE_SERVICE_ACCOUNT_FILE или GOOGLE_SERVICE_ACCOUNT_JSON."
            )

        logger.info("Google Sheets инициализирован через сервисный аккаунт: %s", credentials_file)
        return gspread.service_account(filename=str(credentials_file))

    @staticmethod
    def _parse_date_for_sorting(date_str: str) -> date:
        if not date_str:
            return date.min
        try:
            return datetime.strptime(date_str, PAYMENT_DATE_FORMAT).date()
        except ValueError:
            return date.min

    @staticmethod
    def _accounts_from_rows(*collections: Iterable[list[str]], account_index: int) -> set[str]:
        accounts: set[str] = set()
        for rows in collections:
            for row in rows:
                if len(row) > account_index and row[account_index]:
                    accounts.add(row[account_index])
        return accounts

    @staticmethod
    def _normalize_rows(rows: list[list[str]]) -> list[list[str]]:
        return [[value if value is not None else "" for value in row] for row in rows]

    @classmethod
    def _compose_final_rows(
        cls,
        existing_rows: list[list[str]],
        header: list[str],
        incoming_rows: list[list[str]],
        preserve_account_index: int,
        accounts_to_replace: set[str],
    ) -> list[list[str]]:
        preserved_rows = [
            row
            for row in existing_rows[1:]
            if row and len(row) > preserve_account_index and row[preserve_account_index] not in accounts_to_replace
        ]
        return cls._normalize_rows([header] + preserved_rows + incoming_rows)

    def get_account_credentials(self) -> list[tuple[str, str]]:
        if not settings.google_accounts_sheet_id:
            raise RuntimeError("Не задан GOOGLE_ACCOUNTS_SHEET_ID")

        spreadsheet = self.gc.open_by_key(settings.google_accounts_sheet_id)
        worksheet = spreadsheet.worksheet("Аккаунты")
        values = worksheet.get_all_values()
        credentials: list[tuple[str, str]] = []
        for row in values[1:]:
            if len(row) >= 2 and row[0] and row[1]:
                credentials.append((row[0].strip(), row[1].strip()))
        return credentials

    def write_to_sheet(
        self,
        batch_app_data: list[list[str]],
        manager_data: list[list[str]],
        stay_data: list[list[str]],
    ) -> None:
        max_retries = 3

        try:
            spreadsheet_key = ensure_valid_spreadsheet(self.gc)
        except ExistingSpreadsheetRequiredError as exc:
            logger.error("Запись в Google Sheets пропущена: %s", exc)
            return

        for attempt in range(1, max_retries + 1):
            try:
                spreadsheet = self.gc.open_by_key(spreadsheet_key)
                logger.info("Записываем данные в таблицу %s", spreadsheet_key)

                batch_accounts = self._accounts_from_rows(batch_app_data, account_index=IDX_BA_ACCOUNT)
                manager_accounts = self._accounts_from_rows(manager_data, account_index=IDX_MGR_ACCOUNT)
                stay_accounts = self._accounts_from_rows(stay_data, account_index=IDX_SP_ACCOUNT)
                accounts_to_process = batch_accounts | manager_accounts | stay_accounts
                if not accounts_to_process:
                    logger.info("Нет аккаунтов для обновления Google Sheets")
                    return

                logger.info("Аккаунты на обновление: %s", sorted(accounts_to_process))

                self._rewrite_worksheet(
                    worksheet=spreadsheet.worksheet("Batch Application"),
                    header=BATCH_APPLICATION_HEADERS,
                    incoming_rows=sorted(
                        self._normalize_rows(batch_app_data),
                        key=lambda row: self._parse_date_for_sorting(row[6]),
                        reverse=True,
                    ),
                    preserve_account_index=IDX_BA_ACCOUNT,
                    accounts_to_replace=accounts_to_process,
                )
                self._rewrite_worksheet(
                    worksheet=spreadsheet.worksheet("Batch Application(Manager)"),
                    header=BATCH_MANAGER_HEADERS,
                    incoming_rows=sorted(
                        self._normalize_rows(manager_data),
                        key=lambda row: self._parse_date_for_sorting(row[IDX_MGR_PAYMENT_DATE]),
                        reverse=True,
                    ),
                    preserve_account_index=IDX_MGR_ACCOUNT,
                    accounts_to_replace=accounts_to_process,
                )
                self._rewrite_worksheet(
                    worksheet=spreadsheet.worksheet("StayPermit"),
                    header=STAY_PERMIT_HEADERS,
                    incoming_rows=self._normalize_rows(stay_data),
                    preserve_account_index=IDX_SP_ACCOUNT,
                    accounts_to_replace=accounts_to_process,
                )
                logger.info("Google Sheets успешно обновлён")
                return
            except Exception as exc:
                logger.error("Попытка %s/%s записи в Google Sheets завершилась ошибкой: %s", attempt, max_retries, exc)
                if attempt == max_retries:
                    raise
                time.sleep(10 * attempt)

    def _rewrite_worksheet(
        self,
        worksheet,
        header: list[str],
        incoming_rows: list[list[str]],
        preserve_account_index: int,
        accounts_to_replace: set[str],
    ) -> None:
        existing_rows = self._normalize_rows(worksheet.get_all_values())
        final_rows = self._compose_final_rows(
            existing_rows=existing_rows,
            header=header,
            incoming_rows=incoming_rows,
            preserve_account_index=preserve_account_index,
            accounts_to_replace=accounts_to_replace,
        )

        if final_rows == existing_rows:
            logger.info("Лист %s не изменился, запись пропущена", worksheet.title)
            return

        worksheet.clear()
        worksheet.update(final_rows, "A1", value_input_option="USER_ENTERED")
        logger.info("Лист %s обновлён: %s строк", worksheet.title, len(final_rows))
