
"""
Основной скрипт для парсинга данных с evisa.imigrasi.go.id,
сохранения их в БД и записи в Google Sheets.
Также включает логику уведомлений через Telegram бота.
"""
import os
import sys
import json
import time
import signal
import threading
import asyncio
import logging as py_logging
from traceback import format_exc
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import List, Tuple, Dict, Any, Optional, NamedTuple
from dataclasses import dataclass, asdict

# === Импорты внешних библиотек ===
import gspread
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from curl_cffi import requests
from bs4 import BeautifulSoup
import yadisk 
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

# === Импорты внутренних модулей ===
from database.db import init_db, SessionLocal
from database.models import BatchApplication, StayPermit
from database.crud import save_batch_data, save_stay_permit_data
from session_manager import login, check_session, load_session 
from utils.driver_uploader import upload_to_yandex_disk, download_pdf 
from utils.logger import logger as custom_logger # Используем логгер
from utils.parser import (
    safe_get, extract_status_batch, extract_status,
    extract_action_link as extract_action_link_parser, # Переименовано во избежание конфликта
    extract_reg_number, extract_visa, extract_detail
)
from bot.handler import bot_router
from utils.scheduler import start_scheduler as start_notification_scheduler

# === Конфигурация ===
from dotenv import load_dotenv
load_dotenv()

# === Константы ===
MAX_ERRORS_BEFORE_RESTART = 5
RESTART_DELAY_AFTER_ERRORS = 60 # секунд
RETRY_DELAY_AFTER_ERROR = 30 # секунд
BATCH_PARSE_INTERVAL_MINUTES = 10
STAY_PARSE_HOUR = 7
STAY_PARSE_MINUTE = 0
GS_BATCH_SHEET_ID = os.getenv("GOOGLE_SHEET_BATCH_ID")
GS_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")
YANDEX_TOKEN = os.getenv('YANDEX_TOKEN')

# Константы для индексов столбцов в данных (для сортировки и фильтрации)
IDX_BA_ACCOUNT = 10 # Индекс столбца 'account' в client_data_table (Batch)
IDX_MGR_ACCOUNT = 5  # Индекс столбца 'account' в manager_data (Batch Manager)
IDX_MGR_PAYMENT_DATE = 2 # Индекс столбца 'payment_date' в manager_data
IDX_SP_ACCOUNT = 9   # Индекс столбца 'account' в stay_data (Stay Permit)
IDX_SP_ACTION_LINK = 7 # Индекс столбца 'action_link' в stay_data

PAYMENT_DATE_FORMAT = "%d-%m-%Y" # Формат даты в строке, например, '28-03-2025'
SP_TEMP_DIR = "src/temp"
os.makedirs(SP_TEMP_DIR, exist_ok=True) 

# === Глобальные переменные ===
scheduler_jobs: Optional[BackgroundScheduler] = None
error_counter = 0

# === Классы данных ===

@dataclass
class BatchApplicationData:
    """Класс для представления распарсенных данных Batch Application."""
    batch_no: str
    register_number: str
    full_name: str
    visitor_visa_number: str
    passport_number: str
    payment_date: str
    visa_type: str
    status: str
    action_link: str # Это будет публичная ссылка с Яндекс.Диска
    account: str
    birth_date: str

    def to_client_table_row(self) -> list:
        """Преобразует объект в список для записи в Google Sheets (Batch Application)."""
        return [
            self.batch_no, self.register_number, self.full_name, self.birth_date,
            self.visitor_visa_number, self.passport_number, self.payment_date,
            self.visa_type, self.status, self.action_link, self.account
        ]

    def to_manager_row(self) -> list:
        """Преобразует объект в список для записи в Google Sheets (Batch Application Manager)."""
        return [
            self.full_name, self.visa_type, self.payment_date,
            self.status, self.action_link, self.account
        ]

    def to_db_dict(self) -> dict:
        """Преобразует объект в словарь для сохранения в БД."""
        return asdict(self)

@dataclass
class StayPermitData:
    """Класс для представления распарсенных данных Stay Permit."""
    reg_number: str
    full_name: str
    type_of_staypermit: str
    visa_type: str
    passport_number: str
    arrival_date: str
    issue_date: str
    expired_date: str
    status: str
    action_link: str # Это будет публичная ссылка с Яндекс.Диска
    account: str

    def to_sheet_row(self) -> list:
        """Преобразует объект в список для записи в Google Sheets (Stay Permit)."""
        return [
            self.full_name, self.type_of_staypermit, self.visa_type,
            self.arrival_date, self.issue_date, self.expired_date,
            self.status, self.action_link, self.passport_number, self.account
        ]

    def to_db_dict(self) -> dict:
        """Преобразует объект в словарь для сохранения в БД."""
        return asdict(self)

# === Классы логики ===

class SessionManager:
    """Управление HTTP-сессией."""
    def __init__(self):
        self.session = requests.Session()

    def get_session(self) -> requests.Session:
        return self.session

class YandexDiskUploader:
    """Логика работы с Яндекс.Диском."""
    def __init__(self, token: str):
        self.token = token
        self.ya_disk_client = yadisk.YaDisk(token=self.token) if self.token else None
        self._check_token()

    def _check_token(self):
        """Проверяет валидность токена."""
        if not self.ya_disk_client or not self.ya_disk_client.check_token():
            custom_logger.error("Недействительный токен Яндекс.Диска")
            raise Exception("Недействительный токен Яндекс.Диска. Проверьте YANDEX_TOKEN.")

    def upload_pdf(self, pdf_content: bytes, filename: str) -> str:
        """
        Загружает PDF-файл в папку /Visa на Яндекс.Диске и возвращает публичную ссылку.
        """
        if not self.ya_disk_client:
            custom_logger.error("Клиент Яндекс.Диска не инициализирован")
            return ''

        file_path = f"/Visa/{filename}"
        try:
            # Проверка существования файла
            if self.ya_disk_client.exists(file_path):
                meta = self.ya_disk_client.get_meta(file_path, fields=["public_url"])
                public_url = meta.public_url
                if public_url:
                    #custom_logger.info(f"Файл уже существует, возвращаем публичную ссылку: {public_url}")
                    return public_url
        except yadisk.exceptions.PathNotFoundError:
            custom_logger.info(f"Файл {file_path} не существует, приступаем к загрузке")
        except Exception as e:
            custom_logger.error(f"Ошибка при проверке файла {file_path}: {e}")

        try:
            # Создание папки /Visa, если не существует
            try:
                self.ya_disk_client.mkdir("/Visa")
                custom_logger.info("Папка /Visa создана")
            except yadisk.exceptions.PathExistsError:
                pass

            # Загрузка файла напрямую из байтов
            from io import BytesIO
            file_like_object = BytesIO(pdf_content)
            self.ya_disk_client.upload(file_like_object, file_path, overwrite=True)

            # Публикация файла
            self.ya_disk_client.publish(file_path)

            # Получение публичной ссылки
            meta = self.ya_disk_client.get_meta(file_path, fields=["public_url"])
            public_url = meta.public_url
            if not public_url:
                custom_logger.error("Не удалось получить публичную ссылку")
                raise Exception("Не удалось получить публичную ссылку после публикации")
            custom_logger.info(f"Файл успешно загружен, публичная ссылка: {public_url}")
            return public_url

        except yadisk.exceptions.YaDiskError as e:
            custom_logger.error(f"Ошибка Яндекс.Диска: {e}")
        except Exception as e:
            custom_logger.error(f"Общая ошибка при загрузке файла: {e}")
        return ''

class PDFManager:
    """Управление загрузкой и хранением PDF."""
    def __init__(self, session_manager: SessionManager, yandex_uploader: YandexDiskUploader):
        self.session_manager = session_manager
        self.yandex_uploader = yandex_uploader

    def download_pdf(self, session_id: str, pdf_url: str) -> Optional[bytes]:
        """Скачивает PDF по ссылке, используя сессию."""
        cookies = {'PHPSESSID': session_id}
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://evisa.imigrasi.go.id/ ',
        }
        try:
            response = self.session_manager.get_session().get(pdf_url, cookies=cookies, headers=headers)
            if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
                return response.content
            else:
                custom_logger.error(f"Ошибка загрузки PDF: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")
                return None
        except Exception as e:
            custom_logger.error(f"Ошибка при загрузке PDF: {e}")
            return None

    def upload_batch_pdf(self, session_id: str, action_link_original: str, reg_number: str, full_name: str) -> str:
        """
        Загружает PDF Batch Application на Яндекс.Диск.
        Проверяет наличие локальной копии, чтобы избежать повторной загрузки.
        """
        action_link_yandex = ''
        if not action_link_original:
            return action_link_yandex

        try:
            # Формируем путь к локальному файлу (аналогично Stay Permit)
            temp_path = os.path.join(SP_TEMP_DIR, f"{reg_number}_batch_application.pdf")

            # Проверяем, есть ли локальная копия (аналогично Stay Permit)
            if not os.path.exists(temp_path):
                pdf_content = self.download_pdf(session_id, action_link_original)
                if pdf_content:
                    # Сохраняем локально (аналогично Stay Permit)
                    with open(temp_path, "wb") as f:
                        f.write(pdf_content)
                    custom_logger.info(f"✅ PDF для Batch {full_name} ({reg_number}) скачан и сохранён локально.")
                else:
                    custom_logger.warning(f"⚠️ Не удалось скачать PDF для Batch {full_name} ({reg_number}) по ссылке {action_link_original}")
                    return action_link_yandex # Возвращаем пустую строку
            else:
                # Используем существующую локальную копию (аналогично Stay Permit)
                with open(temp_path, "rb") as f:
                    pdf_content = f.read()
                custom_logger.info(f"✅ Используем локальную копию PDF для Batch {full_name} ({reg_number}).")

            # Загружаем на Яндекс.Диск (общая логика)
            file_name_for_yandex = f"{reg_number}_batch_application.pdf" # Имя файла для Яндекс.Диска
            action_link_yandex = self.yandex_uploader.upload_pdf(pdf_content, file_name_for_yandex)
            if action_link_yandex:
                custom_logger.info(f"✅ PDF для Batch {full_name} ({reg_number}) загружен. Ссылка: {action_link_yandex}")
            else:
                custom_logger.error(f"❌ Не удалось получить публичную ссылку для Batch {full_name} ({reg_number}).")

        except Exception as e:
            custom_logger.error(f"❌ Ошибка при обработке/загрузке PDF Batch на Яндекс.Диск для {full_name} ({reg_number}): {e}")

        return action_link_yandex # Возвращаем ссылку или пустую строку

    def upload_stay_pdf(self, session_id: str, pdf_relative_url: str, reg_number: str) -> str:
        """Загружает PDF Stay Permit на Яндекс.Диск."""
        action_link_yandex = ''
        if not pdf_relative_url:
            return action_link_yandex

        try:
            pdf_full_url = f"https://evisa.imigrasi.go.id{pdf_relative_url}"
            temp_path = os.path.join(SP_TEMP_DIR, f"{reg_number}_stay_permit.pdf")

            if not os.path.exists(temp_path):
                pdf_content = self.download_pdf(session_id, pdf_full_url)
                if pdf_content:
                    with open(temp_path, "wb") as f:
                        f.write(pdf_content)
                    custom_logger.info(f"✅ PDF для Stay Permit {reg_number} скачан и сохранён локально.")
                else:
                    custom_logger.warning(f"⚠️ Не удалось скачать PDF для Stay Permit {reg_number} по ссылке {pdf_full_url}")
                    return action_link_yandex
            else:
                with open(temp_path, "rb") as f:
                    pdf_content = f.read()
                #custom_logger.info(f"✅ Используем локальную копию PDF для Stay Permit {reg_number}.")

            public_link = self.yandex_uploader.upload_pdf(pdf_content, f"{reg_number}_stay_permit.pdf")
            if public_link:
                action_link_yandex = public_link
                custom_logger.info(f"✅ PDF для Stay Permit {reg_number} загружен. Ссылка: {action_link_yandex}")
            else:
                custom_logger.error(f"❌ Не удалось получить публичную ссылку для Stay Permit {reg_number}.")

        except Exception as e:
            custom_logger.error(f"❌ Ошибка при обработке/загрузке PDF Stay Permit {reg_number}: {e}")

        return action_link_yandex

class DataParser:
    """Логика парсинга данных с сайта."""
    def __init__(self, session_manager: SessionManager, pdf_manager: PDFManager):
        self.session_manager = session_manager
        self.pdf_manager = pdf_manager

    def _parse_date_for_sorting(self, date_str: str) -> date:
        """Преобразует строку даты в datetime.date для сортировки."""
        if not date_str:
            return date.min
        try:
            return datetime.strptime(date_str, PAYMENT_DATE_FORMAT).date()
        except ValueError:
            custom_logger.warning(f"Не удалось преобразовать дату '{date_str}' для сортировки.")
            return date.min

    def fetch_and_update_batch(self, name: str, session_id: str) -> Tuple[List[List[str]], List[List[str]]]:
        """Парсит данные Batch Application для аккаунта."""
        custom_logger.info(f"Начинаем парсинг Batch Application для аккаунта: {name}")
        offset = 0
        parsed_data_list: List[BatchApplicationData] = []
        attempt = 0
        max_attempts = 3
        items_parsed_total = 0

        while attempt < max_attempts:
            try:
                while True: # Пагинация
                    cookies = {'PHPSESSID': session_id}
                    headers = {
                        'Host': 'evisa.imigrasi.go.id',
                        'User-Agent': 'Mozilla/5.0',
                        'Accept': 'application/json',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'X-Requested-With': 'XMLHttpRequest'
                    }
                    data = {
                        'draw': '1',
                        'columns[0][data]': 'no',
                        'columns[0][searchable]': 'true',
                        'columns[0][orderable]': 'true',
                        'columns[0][search][value]': '',
                        'columns[0][search][regex]': 'false',
                        'columns[1][data]': 'header_code',
                        'columns[1][searchable]': 'true',
                        'columns[1][orderable]': 'true',
                        'columns[1][search][value]': '',
                        'columns[1][search][regex]': 'false',
                        'start': str(offset),
                        'length': '100000',
                        'search[value]': '',
                        'search[regex]': 'false'
                    }
                    response = self.session_manager.get_session().post(
                        'https://evisa.imigrasi.go.id/web/applications/batch/data',
                        headers=headers,
                        data=data,
                        cookies=cookies
                    )

                    if response.status_code != 200:
                        custom_logger.error(f"Ошибка получения данных Batch для {name}: {response.status_code}")
                        break

                    result_data = response.json().get('data', [])
                    if not result_data:
                        custom_logger.info(f"Данные Batch для {name} закончились (offset={offset}).")
                        break

                    items_in_batch = 0
                    for item_data in result_data:
                        try:
                            batch_no = safe_get(item_data, 'header_code').strip().replace('\n', '')
                            reg_number = safe_get(item_data, 'register_number')
                            full_name = safe_get(item_data, 'full_name')
                            visitor_visa_number = safe_get(item_data, 'request_code')
                            passport_number = safe_get(item_data, 'passport_number')
                            payment_date = safe_get(item_data, 'paid_date')
                            visa_type = safe_get(item_data, 'visa_type')
                            status = extract_status_batch(safe_get(item_data, 'status'))
                            action = extract_visa(safe_get(item_data, 'actions'))
                            action_link_original = f"https://evisa.imigrasi.go.id{action}" if action and action.split('/')[-1] == 'print' else ''
                            
                            detail_link = f"https://evisa.imigrasi.go.id{extract_detail(safe_get(item_data, 'actions'))}"
                            date_birth = ''
                            try:
                                detail_response = self.session_manager.get_session().get(detail_link)
                                detail_result = detail_response.text
                                date_birth = detail_result.split('Date of Birth')[-1].split('</small')[0].split('<small>')[-1]
                            except Exception as ex:
                                custom_logger.error(f'Ошибка при парсинге дня рождения клиента {detail_link}: {ex}')

                            # Создаем объект данных
                            batch_obj = BatchApplicationData(
                                batch_no=batch_no,
                                register_number=reg_number,
                                full_name=full_name,
                                visitor_visa_number=visitor_visa_number,
                                passport_number=passport_number,
                                payment_date=payment_date,
                                visa_type=visa_type,
                                status=status,
                                action_link='', # Будет заполнено позже
                                account=name,
                                birth_date=date_birth
                            )

                            # --- НОВАЯ ЛОГИКА: Загрузка PDF на Яндекс.Диск ---
                            final_action_link = self.pdf_manager.upload_batch_pdf(
                                session_id, action_link_original, batch_obj.register_number, batch_obj.full_name
                            )
                            batch_obj.action_link = final_action_link
                            # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

                            parsed_data_list.append(batch_obj)
                            items_in_batch += 1
                            items_parsed_total += 1

                            if items_parsed_total % 10 == 0:
                                custom_logger.info(f'Спарсили {items_parsed_total} Batch Application (аккаунт {name})')

                        except Exception as item_e:
                            custom_logger.error(f"Ошибка при обработке одного элемента Batch для {name}: {item_e}")

                    custom_logger.info(f"Обработан пакет Batch для {name}, offset={offset}, items={items_in_batch}")
                    offset += 850 # Предполагаемый шаг пагинации

                # Сохранение в БД
                db_dicts = [obj.to_db_dict() for obj in parsed_data_list]
                with SessionLocal() as db:
                    save_batch_data(db, db_dicts)
                    custom_logger.info(f"✅ Данные Batch Application для {name} сохранены в БД (всего {len(db_dicts)} записей)")

                # Подготовка данных для Google Sheets
                client_data_table = [obj.to_client_table_row() for obj in parsed_data_list]
                manager_data = [obj.to_manager_row() for obj in parsed_data_list]
                return client_data_table, manager_data

            except Exception as exc:
                attempt += 1
                custom_logger.error(f"Ошибка в fetch_and_update_batch (аккаунт {name}, попытка {attempt}/{max_attempts}): {exc}")
                if attempt >= max_attempts:
                    custom_logger.warning(f"Не удалось спарсить аккаунт {name} после {max_attempts} попыток.")
                    return [], []
                time.sleep(10)

        return [], [] # Дублируем return для безопасности типов

    def fetch_and_update_stay(self, name: str, session_id: str) -> List[List[str]]:
        """Парсит данные Stay Permit для аккаунта."""
        custom_logger.info(f"Начинаем парсинг Stay Permit для аккаунта: {name}")
        offset = 0
        parsed_data_list: List[StayPermitData] = []
        attempt = 0
        max_attempts = 3
        items_parsed_total = 0

        while attempt < max_attempts:
            try:
                while True: # Пагинация
                    cookies = {'PHPSESSID': session_id}
                    headers = {
                        'Host': 'evisa.imigrasi.go.id',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Sec-Fetch-Dest': 'empty',
                        'Sec-Fetch-Mode': 'cors',
                        'Sec-Fetch-Site': 'same-origin'
                    }
                    # Note: params are often dynamically generated. You might need to adjust them.
                    params = {
                        'draw': '1',
                        # ... (остальные параметры columns остаются без изменений) ...
                        'columns[11][data]': 'action',
                        'columns[11][searchable]': 'true',
                        'columns[11][orderable]': 'true',
                        'columns[11][search][value]': '',
                        'columns[11][search][regex]': 'false',
                        'start': str(offset),
                        'length': '100000000',
                        'search[value]': '',
                        'search[regex]': 'false',
                        '_': str(int(time.time() * 1000)),
                    }
                    
                    response = self.session_manager.get_session().get(
                        'https://evisa.imigrasi.go.id/front/applications/stay-permit/data',
                        headers=headers,
                        cookies=cookies,
                        params=params,
                        verify=False
                    )

                    if response.status_code != 200:
                        custom_logger.error(f"Ошибка получения данных Stay Permit для {name}: {response.status_code}")
                        break

                    result_data = response.json().get('data', [])
                    if not result_data:
                        custom_logger.info(f"Данные Stay Permit для {name} закончились (offset={offset}).")
                        break

                    items_in_batch = 0
                    for item_data in result_data:
                        try:
                            reg_number_raw = safe_get(item_data, 'register_number')
                            if not reg_number_raw:
                                custom_logger.warning("Пропущен элемент Stay Permit без регистрационного номера.")
                                continue
                            reg_number = reg_number_raw.split("'>")[-1].split("</a>")[0]

                            full_name = safe_get(item_data, 'full_name', '')
                            type_permit = safe_get(item_data, 'type_of_staypermit', '')
                            type_visa = safe_get(item_data, 'type_of_visa', '')
                            start_date = safe_get(item_data, 'start_date', '')
                            issue_data = safe_get(item_data, 'issue_date', '')
                            expired_data = safe_get(item_data, 'expired_date', '')
                            passport_number = safe_get(item_data, 'passport_number', '')
                            status_raw = safe_get(item_data, 'status', '')
                            status = extract_status(status_raw)

                            # Создаем объект данных
                            stay_obj = StayPermitData(
                                reg_number=reg_number,
                                full_name=full_name,
                                type_of_staypermit=type_permit,
                                visa_type=type_visa,
                                passport_number=passport_number,
                                arrival_date=start_date,
                                issue_date=issue_data,
                                expired_date=expired_data,
                                status=status,
                                action_link='', # Будет заполнено позже
                                account=name
                            )

                            # --- НОВАЯ ЛОГИКА: Загрузка PDF на Яндекс.Диск ---
                            action_html = safe_get(item_data, 'action')
                            pdf_relative_url = ''
                            if action_html:
                                pdf_relative_url = extract_action_link_parser(action_html) # Используем переименованную функцию
                            
                            public_link = self.pdf_manager.upload_stay_pdf(session_id, pdf_relative_url, stay_obj.reg_number)
                            stay_obj.action_link = public_link
                            # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

                            parsed_data_list.append(stay_obj)
                            items_in_batch += 1
                            items_parsed_total += 1

                            if items_parsed_total % 10 == 0:
                                custom_logger.info(f'Спарсили {items_parsed_total} Stay Permit (аккаунт {name})')

                        except Exception as item_e:
                            custom_logger.error(f"Ошибка при обработке одного элемента Stay Permit для {name}: {item_e}")

                    custom_logger.info(f"Обработан пакет Stay Permit для {name}, offset={offset}, items={items_in_batch}")
                    offset += 1250 # Предполагаемый шаг пагинации

                # Сохранение в БД
                db_dicts = [obj.to_db_dict() for obj in parsed_data_list]
                with SessionLocal() as db:
                    save_stay_permit_data(db, db_dicts)
                    custom_logger.info(f"✅ Данные Stay Permit для {name} сохранены в БД (всего {len(db_dicts)} записей)")

                # Подготовка данных для Google Sheets
                sheet_data = [obj.to_sheet_row() for obj in parsed_data_list]
                return sheet_data

            except Exception as exc:
                attempt += 1
                custom_logger.error(f"Ошибка в fetch_and_update_stay (аккаунт {name}, попытка {attempt}/{max_attempts}): {exc}")
                if attempt >= max_attempts:
                    custom_logger.warning(f"Не удалось спарсить Stay Permit для {name}.")
                    return []
                time.sleep(10)

        return [] # Дублируем return

    def parse_accounts(self, account_names: List[str], account_passwords: List[str]) -> Tuple[List[List[str]], List[List[str]], List[List[str]]]:
        """Парсит данные для списка аккаунтов."""
        custom_logger.info(f"Начинаем парсинг для {len(account_names)} аккаунтов.")
        if not account_names or not account_passwords:
            custom_logger.warning("Нет аккаунтов для парсинга")
            return [], [], []

        batch_app_table = []
        batch_mgr_table = []
        stay_data_table = []
        temp_counter = 0

        for name, password in zip(account_names, account_passwords):
            custom_logger.info(f"Обрабатываем аккаунт {name} ({temp_counter + 1}/{len(account_names)})")
            session_id = load_session(name)
            if not check_session(session_id):
                session_id = login(name, password)
                if not session_id:
                    custom_logger.warning(f"Не удалось залогиниться под {name}")
                    continue # Пропускаем аккаунт, если логин не удался

            stay_data = self.fetch_and_update_stay(name, session_id)
            batch_app, batch_mgr = self.fetch_and_update_batch(name, session_id)

            batch_app_table.extend(batch_app)
            batch_mgr_table.extend(batch_mgr)
            stay_data_table.extend(stay_data)

            temp_counter += 1
            custom_logger.info(f'Обработан аккаунт {name} ({temp_counter}/{len(account_names)})')

        return batch_app_table, batch_mgr_table, stay_data_table

class GoogleSheetsManager:
    """Логика работы с Google Sheets."""
    def __init__(self, credentials_path: str):
        self.credentials_path = credentials_path
        self.gc = None
        self._init_client()

    def _init_client(self):
        """Инициализирует клиент gspread."""
        try:
            # Проверка наличия файла учетных данных
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
            return date.max # Помещаем пустые даты в конец
        try:
            return datetime.strptime(date_str, PAYMENT_DATE_FORMAT).date()
        except ValueError:
            custom_logger.warning(f"Не удалось преобразовать дату '{date_str}' для сортировки.")
            return date.max

    def write_to_sheet(self,
        spreadsheet_key: str,
        batch_app_data: List[List[str]],
        manager_data: List[List[str]],
        stay_data: List[List[str]]):
        """
        Записывает данные в соответствующие листы Google Sheets.
        Включает фильтрацию по аккаунтам и сортировку Batch Application по payment_date.
        Добавлена обработка больших объемов данных и повторные попытки.
        """
        max_retries = 3
        base_delay = 5 # Базовая задержка в секундах
        chunk_size = 500 # Максимальное количество строк для отправки за раз

        for attempt in range(1, max_retries + 1):
            try:
                if not self.gc:
                    self._init_client()

                spreadsheet = self.gc.open_by_key(spreadsheet_key)
                
                # Получаем список аккаунтов из новых данных Batch Application
                first_two_accounts = list(set(row[IDX_BA_ACCOUNT] for row in batch_app_data)) if batch_app_data else []

                # --- Batch Application ---
                worksheet_batch = spreadsheet.worksheet('Batch Application')
                all_batch_data = worksheet_batch.get_all_values()
                header_batch = all_batch_data[0] if all_batch_data else []
                existing_batch_data = all_batch_data[1:] if len(all_batch_data) > 1 else []
                filtered_batch_data = [
                    row for row in existing_batch_data if row[IDX_BA_ACCOUNT] not in first_two_accounts
                ]
                # Сортировка новых данных Batch Application по payment_date (индекс 6)
                try:
                    sorted_new_batch_data = sorted(batch_app_data, key=lambda row: self._parse_date_for_sorting(row[6]), reverse=True)
                    custom_logger.info("✅ BatchApplication данные отсортированы по payment_date.")
                except Exception as sort_error:
                    custom_logger.warning(f"⚠️ Ошибка при сортировке BatchApplication: {sort_error}. Данные не отсортированы.")
                    sorted_new_batch_data = batch_app_data

                updated_batch_data = [header_batch] + filtered_batch_data + sorted_new_batch_data
                
                # Очищаем лист
                worksheet_batch.clear()
                
                # Записываем данные по частям
                if updated_batch_data and any(updated_batch_data):
                    self._append_rows_in_chunks(worksheet_batch, updated_batch_data, chunk_size)
                custom_logger.info("✅ Данные Batch Application обновлены в Google Sheets")

                # --- Manager Worksheet ---
                worksheet_manager = spreadsheet.worksheet('Batch Application(Manager)')
                all_mgr_data = worksheet_manager.get_all_values()
                header_mgr = all_mgr_data[0] if all_mgr_data else []
                existing_mgr_data = all_mgr_data[1:] if len(all_mgr_data) > 1 else []
                filtered_mgr_data = [
                    row for row in existing_mgr_data if row[IDX_MGR_ACCOUNT] not in first_two_accounts
                ]
                # Сортировка новых данных Manager по payment_date (индекс 2)
                try:
                    sorted_new_mgr_data = sorted(manager_data, key=lambda row: self._parse_date_for_sorting(row[IDX_MGR_PAYMENT_DATE]), reverse=True)
                    custom_logger.info("✅ BatchApplication(Manager) данные отсортированы по payment_date.")
                except Exception as sort_error:
                    custom_logger.warning(f"⚠️ Ошибка при сортировке BatchApplication(Manager): {sort_error}. Данные не отсортированы.")
                    sorted_new_mgr_data = manager_data

                updated_mgr_data = [header_mgr] + filtered_mgr_data + sorted_new_mgr_data
                
                # Очищаем лист
                worksheet_manager.clear()
                
                # Записываем данные по частям
                if updated_mgr_data and any(updated_mgr_data):
                    self._append_rows_in_chunks(worksheet_manager, updated_mgr_data, chunk_size)
                custom_logger.info("✅ Данные Batch Application(Manager) обновлены в Google Sheets")

                # --- Stay Permit ---
                worksheet_stay = spreadsheet.worksheet('StayPermit')
                all_stay_data = worksheet_stay.get_all_values()
                header_stay = all_stay_data[0] if all_stay_data else []
                existing_stay_data = all_stay_data[1:] if len(all_stay_data) > 1 else []
                filtered_stay_data = [
                    row for row in existing_stay_data if row[IDX_SP_ACCOUNT] not in first_two_accounts
                ]

                updated_stay_data = [header_stay] + filtered_stay_data + stay_data
                
                # Очищаем лист
                worksheet_stay.clear()
                
                # Записываем данные по частям
                if updated_stay_data and any(updated_stay_data):
                    self._append_rows_in_chunks(worksheet_stay, updated_stay_data, chunk_size)
                custom_logger.info("✅ Данные Stay Permit обновлены в Google Sheets")
                
                # Если все прошло успешно, выходим из цикла попыток
                return 

            #except (requests.exceptions.ConnectionError, gspread.exceptions.APIError) as e:
            except Exception as e:
                custom_logger.warning(f"⚠️ Попытка {attempt}/{max_retries} записи в Google Sheets не удалась из-за ошибки: {e}")
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1)) # Экспоненциальная задержка
                    custom_logger.info(f"Повтор через {delay} секунд...")
                    time.sleep(delay)
                else:
                    custom_logger.error(f"❌ Все {max_retries} попытки записи в Google Sheets не удались.")
                    raise # Пробрасываем ошибку после исчерпания попыток
           # except Exception as e:
                # Для других неожиданных ошибок, сразу пробрасываем
                #custom_logger.error(f"❌ Неожиданная ошибка при записи в Google Sheets: {e}")
                #raise

    def _append_rows_in_chunks(self, worksheet:  List[List[str]],data, chunk_size: int):
        """
        Добавляет данные в лист Google Sheets порциями, чтобы избежать ошибок из-за большого объема.
        """
        custom_logger.info(f"Начинаем запись данных в лист '{worksheet.title}' по частям. Всего строк: {len(data)}")
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            # Небольшая задержка между запросами может помочь
            if i > 0: 
                time.sleep(1) 
            try:
                worksheet.append_rows(chunk)
                custom_logger.debug(f"Записана порция {i//chunk_size + 1}: строки {i+1}-{min(i+chunk_size, len(data))}")
            except (requests.exceptions.ConnectionError, gspread.exceptions.APIError) as e:
                # Если ошибка произошла при записи части, пробрасываем её выше для обработки retry в write_to_sheet
                custom_logger.error(f"❌ Ошибка при записи порции {i//chunk_size + 1} в лист '{worksheet.title}': {e}")
                raise 
            except Exception as e:
                custom_logger.error(f"❌ Неожиданная ошибка при записи порции {i//chunk_size + 1} в лист '{worksheet.title}': {e}")
                raise
        custom_logger.info(f"✅ Запись данных в лист '{worksheet.title}' завершена.") 

class JobScheduler:
    """Управление задачами планировщика."""
    def __init__(self, gs_manager: GoogleSheetsManager, data_parser: DataParser): # Добавлен data_parser
        self.gs_manager = gs_manager
        self.data_parser = data_parser # Сохраняем экземпляр DataParser
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
            names = worksheet_account.col_values(1) # Пропускаем заголовок
            passwords = worksheet_account.col_values(2)

            if len(names) < 2:
                custom_logger.warning("Недостаточно аккаунтов для выполнения задачи первых двух")
                return # Выходим, если аккаунтов меньше 2

            first_two_names = names[:2]
            first_two_passwords = passwords[:2]

            # --- ИНТЕГРАЦИЯ: Вызов парсинга через DataParser ---
            batch_app, batch_mgr, stay = self.data_parser.parse_accounts(first_two_names, first_two_passwords)
            # --- КОНЕЦ ИНТЕГРАЦИИ ---

            # Кэшируем данные
            self.cached_batch_application_data = batch_app
            self.cached_manager_data = batch_mgr
            self.cached_stay_permit_data = stay

            # Записываем в Google Sheets
            self.gs_manager.write_to_sheet(GS_BATCH_SHEET_ID, batch_app, batch_mgr, stay)

        except Exception as e:
            custom_logger.error(f"[job_first_two] Критическая ошибка: {e}", exc_info=True)
            # Не вызываем raise здесь, чтобы не останавливать планировщик из-за одной задачи
            # Но можно залогировать и/или отправить уведомление

    def job_others(self):
        """Задача для парсинга остальных аккаунтов."""
        custom_logger.info("Запуск задачи для остальных аккаунтов")
        try:
             if not self.gs_manager.gc:
                 self.gs_manager._init_client()
                 
             spreadsheet_batch = self.gs_manager.gc.open_by_key(GS_BATCH_SHEET_ID)
             worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
             names = worksheet_account.col_values(1) # Пропускаем заголовок
             passwords = worksheet_account.col_values(2)

             if len(names) <= 2:
                 custom_logger.info("Недостаточно аккаунтов для выполнения полного цикла")
                 # Все данные уже обработаны job_first_two
                 # Записываем кэшированные данные, если они есть
                 if self.cached_batch_application_data or self.cached_manager_data or self.cached_stay_permit_data:
                      self.gs_manager.write_to_sheet(
                          GS_BATCH_SHEET_ID,
                          self.cached_batch_application_data,
                          self.cached_manager_data,
                          self.cached_stay_permit_data
                      )
                 return # Выходим, если аккаунтов <= 2

             remaining_names = names[2:]
             remaining_passwords = passwords[2:]

             # --- ИНТЕГРАЦИЯ: Вызов парсинга через DataParser ---
             batch_app_new, batch_mgr_new, stay_new = self.data_parser.parse_accounts(remaining_names, remaining_passwords)
             # --- КОНЕЦ ИНТЕГРАЦИИ ---

             # Объединяем кэшированные данные с новыми
             full_batch = self.cached_batch_application_data + batch_app_new
             full_manager = self.cached_manager_data + batch_mgr_new
             full_stay = self.cached_stay_permit_data + stay_new

             # Записываем полный набор данных в Google Sheets
             self.gs_manager.write_to_sheet(GS_BATCH_SHEET_ID, full_batch, full_manager, full_stay)

        except Exception as e:
            custom_logger.error(f"[job_others] Критическая ошибка: {e}", exc_info=True)
            # Не вызываем raise здесь, чтобы не останавливать планировщик из-за одной задачи
            # Но можно залогировать и/или отправить уведомление

    def start_scheduler(self):
        """Запускает планировщик задач парсинга."""
        global scheduler_jobs # Для совместимости с shutdown_handler
        self.scheduler = BackgroundScheduler(
            timezone=ZoneInfo("Europe/Moscow"),
            executors={'default': ThreadPoolExecutor(2)}
        )
        # Передача методов экземпляра
        self.scheduler.add_job(
            self.job_first_two,
            'interval',
            minutes=BATCH_PARSE_INTERVAL_MINUTES,
            coalesce=True,
            misfire_grace_time=60 * 5
        )
        self.scheduler.add_job(
            self.job_others,
            'cron',
            hour=STAY_PARSE_HOUR,
            minute=STAY_PARSE_MINUTE,
            coalesce=True,
            misfire_grace_time=60 * 5
        )
        self.scheduler.start()
        scheduler_jobs = self.scheduler # Для совместимости с shutdown_handler
        custom_logger.info("Планировщик парсинга запущен")

class BotRunner:
    """Запуск и управление Telegram ботом."""
    def __init__(self):
        self.bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        self.dp = Dispatcher(storage=MemoryStorage())
        self.dp.include_router(bot_router)

    async def run(self):
        """Асинхронный запуск бота."""
        await start_notification_scheduler() # запуск асинхронного планировщика уведомлений
        await self.dp.start_polling(self.bot)

class Application:
    """Основной класс приложения, объединяющий все компоненты."""
    def __init__(self):
        self.session_manager = SessionManager()
        self.yandex_uploader = YandexDiskUploader(YANDEX_TOKEN)
        self.pdf_manager = PDFManager(self.session_manager, self.yandex_uploader)
        self.data_parser = DataParser(self.session_manager, self.pdf_manager)
        self.gs_manager = GoogleSheetsManager(GS_CREDENTIALS_PATH)
        # Передаем data_parser в JobScheduler
        self.job_scheduler = JobScheduler(self.gs_manager, self.data_parser) 
        self.bot_runner = BotRunner()

    def shutdown_handler(self, signum, frame):
        """Обработчик сигналов завершения."""
        custom_logger.info("Получен сигнал завершения. Останавливаем планировщики...")
        if self.job_scheduler.scheduler:
            self.job_scheduler.scheduler.shutdown()
        sys.exit(0)

    def setup_signal_handlers(self):
        """Настраивает обработчики сигналов."""
        signal.signal(signal.SIGINT, self.shutdown_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)

    def run_parsing_cycle(self):
        """Выполняет один цикл парсинга (для инициализации)."""
        custom_logger.info("Запуск начального парсинга...")
        # --- ИНТЕГРАЦИЯ: Вызов парсинга через JobScheduler ---
        # Выполняем задачи напрямую, а не через планировщик, для инициализации
        self.job_scheduler.job_first_two()
        self.job_scheduler.job_others()
        # --- КОНЕЦ ИНТЕГРАЦИИ ---

    async def run(self):
        """Асинхронная точка входа для запуска всего приложения."""
        global error_counter
        init_db()
        
        # --- Установка обработчиков сигналов в основном потоке ---
        self.setup_signal_handlers() 
        # ---------------------------------------------------------

        # Запуск основного цикла парсинга в отдельном потоке
        parser_thread = threading.Thread(target=self.main_loop, daemon=True)
        parser_thread.start()
        
        # Ждём, пока main инициализирует планировщики и запустит парсинг
        await asyncio.sleep(5)
        
        # Запускаем бота
        await self.bot_runner.run()

# И соответственно, уберите или закомментируйте вызов setup_signal_handlers из main_loop:
    def main_loop(self):
        """Основной цикл запуска приложения."""
        global error_counter
        # init_db() # Можно убрать отсюда, если он уже вызван в run()
        # self.setup_signal_handlers() # <-- УБРАТЬ ЭТУ СТРОКУ
        started = False
        while not started:
            try:
                self.run_parsing_cycle()
                self.job_scheduler.start_scheduler()
                custom_logger.info("Основной поток работает")
                started = True
                error_counter = 0 # Сброс счётчика после успешного запуска
            except Exception as e:
                error_counter += 1
                custom_logger.error(f"Ошибка при запуске (попытка {error_counter}/{MAX_ERRORS_BEFORE_RESTART}): {e}")
                # traceback.print_exc() # Раскомментируйте для подробного лога ошибок
                if self.job_scheduler.scheduler:
                    self.job_scheduler.scheduler.shutdown()
                if error_counter >= MAX_ERRORS_BEFORE_RESTART:
                    custom_logger.critical("Превышено количество попыток. Перезапуск программы через 60 секунд...")
                    time.sleep(RESTART_DELAY_AFTER_ERRORS)
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                else:
                    custom_logger.warning(f"Перезапуск через {RETRY_DELAY_AFTER_ERROR} секунд...")
                    time.sleep(RETRY_DELAY_AFTER_ERROR)

# === Точка входа ===

if __name__ == "__main__":
    app = Application()
    asyncio.run(app.run())
