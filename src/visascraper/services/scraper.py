from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import date, datetime
from typing import Optional

from bs4 import BeautifulSoup
from curl_cffi import requests

from visascraper.database.crud import (
    notify_new_batch_applications,
    save_or_update_batch_data,
    save_or_update_stay_permit_data,
    save_or_update_stay_permit_data_async,
)
from visascraper.database.db import SessionLocal
from visascraper.dto import BatchApplicationData, PAYMENT_DATE_FORMAT, StayPermitData
from visascraper.services.storage import PDFManager, SessionManager
from visascraper.session_manager import check_session, load_session, login
from visascraper.utils.logger import logger
from visascraper.utils.parser import (
    extract_action_link,
    extract_detail,
    extract_status,
    extract_status_batch,
    extract_visa,
    safe_get,
)

BATCH_DATA_URL = "https://evisa.imigrasi.go.id/web/applications/batch/data"
STAY_PERMIT_DATA_URL = "https://evisa.imigrasi.go.id/front/applications/stay-permit/data"
BASE_URL = "https://evisa.imigrasi.go.id"


ProgressCallback = Callable[[int, int, str, int, int, int], None]


class DataParser:
    def __init__(self, session_manager: SessionManager, pdf_manager: PDFManager):
        self.session_manager = session_manager
        self.pdf_manager = pdf_manager
        self.main_loop: Optional[asyncio.AbstractEventLoop] = None

    @staticmethod
    def _parse_date_for_sorting(date_str: str) -> date:
        if not date_str:
            return date.min
        try:
            return datetime.strptime(date_str, PAYMENT_DATE_FORMAT).date()
        except ValueError:
            logger.warning("Не удалось разобрать дату для сортировки: %s", date_str)
            return date.min

    def _fetch_birth_date(
        self,
        session: requests.Session,
        detail_link: str,
        headers: dict[str, str],
        cookies: dict[str, str],
    ) -> str:
        if not detail_link:
            return ""
        try:
            response = session.get(detail_link, headers=headers, cookies=cookies)
            soup = BeautifulSoup(response.text, "html.parser")
            birth_label = soup.find(string="Date of Birth")
            if not birth_label:
                return ""
            next_small = birth_label.find_next("small")
            birth_date = next_small.text.strip() if next_small else ""
            return birth_date if birth_date.count("/") == 2 else ""
        except Exception as exc:
            logger.error("Ошибка при парсинге даты рождения из %s: %s", detail_link, exc)
            return ""

    def _store_batch_items(
        self,
        account_name: str,
        parsed_items: list[BatchApplicationData],
    ) -> tuple[list[list[str]], list[list[str]]]:
        payload = [item.to_db_dict() for item in parsed_items]
        with SessionLocal() as db:
            save_or_update_batch_data(db, payload)
        logger.info("Batch Application для %s сохранены в БД: %s записей", account_name, len(payload))

        if payload and self.main_loop and self.main_loop.is_running():
            asyncio.run_coroutine_threadsafe(notify_new_batch_applications(payload), self.main_loop)

        return [item.to_client_table_row() for item in parsed_items], [item.to_manager_row() for item in parsed_items]

    def _store_stay_items(self, account_name: str, parsed_items: list[StayPermitData]) -> list[list[str]]:
        payload = [item.to_db_dict() for item in parsed_items]
        with SessionLocal() as db:
            save_or_update_stay_permit_data(db, payload)
        logger.info("Stay Permit для %s сохранены в БД: %s записей", account_name, len(payload))

        if payload and self.main_loop and self.main_loop.is_running():
            asyncio.run_coroutine_threadsafe(save_or_update_stay_permit_data_async(payload), self.main_loop)

        return [item.to_sheet_row() for item in parsed_items]

    def fetch_and_update_batch(
        self,
        session: requests.Session,
        account_name: str,
        session_id: str,
    ) -> tuple[list[list[str]], list[list[str]]]:
        logger.info("Начинаем парсинг Batch Application для аккаунта %s", account_name)
        for attempt in range(1, 4):
            parsed_items: list[BatchApplicationData] = []
            offset = 0
            try:
                while True:
                    cookies = {"PHPSESSID": session_id}
                    headers = {
                        "Host": "evisa.imigrasi.go.id",
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "X-Requested-With": "XMLHttpRequest",
                    }
                    payload = {
                        "draw": "1",
                        "columns[0][data]": "no",
                        "columns[0][searchable]": "true",
                        "columns[0][orderable]": "true",
                        "columns[0][search][value]": "",
                        "columns[0][search][regex]": "false",
                        "columns[1][data]": "header_code",
                        "columns[1][searchable]": "true",
                        "columns[1][orderable]": "true",
                        "columns[1][search][value]": "",
                        "columns[1][search][regex]": "false",
                        "start": str(offset),
                        "length": "100000",
                        "search[value]": "",
                        "search[regex]": "false",
                    }
                    response = session.post(
                        BATCH_DATA_URL,
                        headers=headers,
                        data=payload,
                        cookies=cookies,
                    )
                    if response.status_code != 200:
                        raise RuntimeError(f"Ошибка получения Batch Application: {response.status_code}")

                    result_data = response.json().get("data", [])[:10]
                    if not result_data:
                        break

                    for item_data in result_data:
                        try:
                            action_link_relative = extract_visa(safe_get(item_data, "actions"))
                            action_link_original = (
                                f"{BASE_URL}{action_link_relative}"
                                if action_link_relative and action_link_relative.split("/")[-1] == "print"
                                else ""
                            )
                            detail_link_relative = extract_detail(safe_get(item_data, "actions"))
                            detail_link = f"{BASE_URL}{detail_link_relative}" if detail_link_relative else ""

                            batch_item = BatchApplicationData(
                                batch_no=safe_get(item_data, "header_code").strip().replace("\n", ""),
                                register_number=safe_get(item_data, "register_number"),
                                full_name=safe_get(item_data, "full_name"),
                                visitor_visa_number=safe_get(item_data, "request_code"),
                                passport_number=safe_get(item_data, "passport_number"),
                                payment_date=(safe_get(item_data, "paid_date") or "").replace("-", "").strip(),
                                visa_type=safe_get(item_data, "visa_type"),
                                status=extract_status_batch(safe_get(item_data, "status")),
                                action_link="",
                                account=account_name,
                                birth_date=self._fetch_birth_date(
                                    session=session,
                                    detail_link=detail_link,
                                    headers=headers,
                                    cookies=cookies,
                                ),
                            )
                            batch_item.action_link = self.pdf_manager.upload_batch_pdf(
                                session=session,
                                session_id=session_id,
                                action_link_original=action_link_original,
                                reg_number=batch_item.register_number,
                                full_name=batch_item.full_name,
                            )
                            parsed_items.append(batch_item)
                        except Exception as exc:
                            logger.error("Ошибка обработки Batch Application элемента для %s: %s", account_name, exc)

                    logger.info(
                        "Получен пакет Batch Application для %s: offset=%s, элементов=%s",
                        account_name,
                        offset,
                        len(result_data),
                    )
                    offset += len(result_data)
                return self._store_batch_items(account_name, parsed_items)
            except Exception as exc:
                logger.error(
                    "Ошибка в fetch_and_update_batch для %s, попытка %s/3: %s",
                    account_name,
                    attempt,
                    exc,
                )
                if attempt == 3:
                    return [], []
                time.sleep(10)
        return [], []

    def fetch_and_update_stay(
        self,
        session: requests.Session,
        account_name: str,
        session_id: str,
    ) -> list[list[str]]:
        logger.info("Начинаем парсинг Stay Permit для аккаунта %s", account_name)
        collected_items: dict[str, StayPermitData] = {}
        next_offset = 0

        for attempt in range(1, 4):
            offset = next_offset
            try:
                while True:
                    cookies = {"PHPSESSID": session_id}
                    headers = {
                        "Host": "evisa.imigrasi.go.id",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "X-Requested-With": "XMLHttpRequest",
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Site": "same-origin",
                    }
                    params = {
                        "draw": "1",
                        "columns[11][data]": "action",
                        "columns[11][searchable]": "true",
                        "columns[11][orderable]": "true",
                        "columns[11][search][value]": "",
                        "columns[11][search][regex]": "false",
                        "start": str(offset),
                        "length": "100000000",
                        "search[value]": "",
                        "search[regex]": "false",
                        "_": str(int(time.time() * 1000)),
                    }
                    response = session.get(
                        STAY_PERMIT_DATA_URL,
                        headers=headers,
                        cookies=cookies,
                        params=params,
                        verify=False,
                    )
                    if response.status_code != 200:
                        raise RuntimeError(f"Ошибка получения Stay Permit: {response.status_code}")

                    result_data = response.json().get("data", [])[:10]
                    if not result_data:
                        break

                    for item_data in result_data:
                        try:
                            reg_number_raw = safe_get(item_data, "register_number")
                            if not reg_number_raw:
                                continue
                            reg_number = reg_number_raw.split("'>")[-1].split("</a>")[0]
                            pdf_relative_url = extract_action_link(safe_get(item_data, "action"))
                            stay_item = StayPermitData(
                                reg_number=reg_number,
                                name=safe_get(item_data, "full_name", "No name"),
                                type_of_staypermit=safe_get(item_data, "type_of_staypermit", ""),
                                visa_type=safe_get(item_data, "type_of_visa", ""),
                                passport_number=safe_get(item_data, "passport_number", ""),
                                arrival_date=safe_get(item_data, "start_date", ""),
                                issue_date=safe_get(item_data, "issue_date", ""),
                                expired_date=safe_get(item_data, "expired_date", ""),
                                status=extract_status(safe_get(item_data, "status", "")),
                                action_link="",
                                account=account_name,
                            )
                            stay_item.action_link = self.pdf_manager.upload_stay_pdf(
                                session=session,
                                session_id=session_id,
                                pdf_relative_url=pdf_relative_url,
                                reg_number=stay_item.reg_number,
                            )
                            collected_items[stay_item.reg_number] = stay_item
                        except Exception as exc:
                            logger.error("Ошибка обработки Stay Permit элемента для %s: %s", account_name, exc)

                    logger.info(
                        "Получен пакет Stay Permit для %s: offset=%s, элементов=%s",
                        account_name,
                        offset,
                        len(result_data),
                    )
                    offset += len(result_data)
                    next_offset = offset

                return self._store_stay_items(account_name, list(collected_items.values()))
            except Exception as exc:
                logger.error(
                    "Ошибка в fetch_and_update_stay для %s, попытка %s/3: %s",
                    account_name,
                    attempt,
                    exc,
                )
                if attempt == 3:
                    if collected_items:
                        logger.warning(
                            "Сохраняем частично собранные Stay Permit для %s после ошибки: %s записей",
                            account_name,
                            len(collected_items),
                        )
                        return self._store_stay_items(account_name, list(collected_items.values()))
                    return []
                time.sleep(10)
        return []

    def parse_accounts(
        self,
        account_names: list[str],
        account_passwords: list[str],
        progress_callback: ProgressCallback | None = None,
    ) -> tuple[list[list[str]], list[list[str]], list[list[str]]]:
        total_accounts = min(len(account_names), len(account_passwords))
        logger.info("Начинаем парсинг для %s аккаунтов", total_accounts)
        if total_accounts == 0:
            logger.warning("Список аккаунтов пуст")
            return [], [], []

        batch_app_rows: list[list[str]] = []
        manager_rows: list[list[str]] = []
        stay_rows: list[list[str]] = []

        if progress_callback:
            progress_callback(0, total_accounts, "подготовка", total_accounts, 0, 0)

        for index, (name, password) in enumerate(zip(account_names, account_passwords), start=1):
            logger.info("Обрабатываем аккаунт %s (%s/%s)", name, index, total_accounts)
            session_id = load_session(name)
            session = self.session_manager.create_session()
            if not check_session(session, session_id):
                session_id = login(session, name, password)
                if not session_id:
                    logger.warning("Не удалось залогиниться под аккаунтом %s", name)
                    processed = index
                    remaining = total_accounts - processed
                    logger.info(
                        "Прогресс парсинга: обработано %s/%s аккаунтов, осталось %s",
                        processed,
                        total_accounts,
                        remaining,
                    )
                    if progress_callback:
                        progress_callback(processed, total_accounts, name, remaining, len(batch_app_rows), len(stay_rows))
                    self.session_manager.close_session(session)
                    continue

            stay_rows.extend(self.fetch_and_update_stay(session, name, session_id))
            batch_rows, manager_batch_rows = self.fetch_and_update_batch(session, name, session_id)
            batch_app_rows.extend(batch_rows)
            manager_rows.extend(manager_batch_rows)
            self.session_manager.close_session(session)

            processed = index
            remaining = total_accounts - processed
            logger.info(
                "Прогресс парсинга: обработано %s/%s аккаунтов, осталось %s",
                processed,
                total_accounts,
                remaining,
            )
            if progress_callback:
                progress_callback(processed, total_accounts, name, remaining, len(batch_app_rows), len(stay_rows))

        return batch_app_rows, manager_rows, stay_rows
