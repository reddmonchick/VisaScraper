
import os
import sys
import json
import time
import signal
import threading
import asyncio
from traceback import format_exc
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import List, Tuple, Dict, Any, Optional, NamedTuple
from dataclasses import dataclass, asdict
import gspread
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from curl_cffi import requests
import asyncio
from bs4 import BeautifulSoup
import yadisk
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from database.db import init_db, SessionLocal
from database.crud import save_or_update_batch_data, save_or_update_stay_permit_data, save_or_update_stay_permit_data_async
from session_manager import login, check_session, load_session
from utils.logger import logger as custom_logger
from utils.parser import (
    safe_get, extract_status_batch, extract_status,
    extract_action_link as extract_action_link_parser,
    extract_reg_number, extract_visa, extract_detail
)
from utils.sheets_rotator import ensure_valid_spreadsheet
from bot.handler import bot_router
from utils.scheduler import start_scheduler as start_notification_scheduler
from bot.notification import notification_queue, notification_worker
from dotenv import load_dotenv

import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import os

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

load_dotenv()

# === Конфиги ===
MAX_ERRORS_BEFORE_RESTART = 5
RESTART_DELAY_AFTER_ERRORS = 60
RETRY_DELAY_AFTER_ERROR = 30
BATCH_PARSE_INTERVAL_MINUTES = 10

# Пути и токены
GS_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")
YANDEX_TOKEN = os.getenv('YANDEX_TOKEN')
PROXY = os.getenv('PROXY')

# Вечные таблицы
GOOGLE_ACCOUNTS_SHEET_ID = os.getenv("GOOGLE_ACCOUNTS_SHEET_ID")  # ← аккаунты
GOOGLE_ARCHIVE_INDEX_ID = os.getenv("GOOGLE_ARCHIVE_INDEX_ID")   # ← оглавление

# Индексы в таблицах
IDX_BA_ACCOUNT = 10
IDX_MGR_ACCOUNT = 5
IDX_MGR_PAYMENT_DATE = 2
IDX_SP_ACCOUNT = 9

PAYMENT_DATE_FORMAT = "%d-%m-%Y"
SP_TEMP_DIR = "src/temp"
os.makedirs(SP_TEMP_DIR, exist_ok=True)

scheduler_jobs: Optional[BackgroundScheduler] = None
error_counter = 0

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
    name: str
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
            self.name, self.type_of_staypermit, self.visa_type,
            self.arrival_date, self.issue_date, self.expired_date,
            self.status, self.action_link, self.passport_number, self.account
        ]

    def to_db_dict(self) -> dict:
        """Преобразует объект в словарь для сохранения в БД."""
        return asdict(self)

# === Классы логики ===

class SessionManager:
    """Управление HTTP-сессией с поддержкой прокси."""
    def __init__(self, proxies: str = None):
        self.proxies = proxies

    def get_session(self) -> requests.Session:
        self.session = requests.Session()
        if self.proxies:
            proxies = {
                'http': f'http://{self.proxies}',
                'https': f'http://{self.proxies}'
            }
            self.session.proxies.update(proxies)
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
                #custom_logger.error("Не удалось получить публичную ссылку")
                raise Exception("Не удалось получить публичную ссылку после публикации")
            #custom_logger.info(f"Файл успешно загружен, публичная ссылка: {public_url}")
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
                    #custom_logger.info(f"✅ PDF для Batch {full_name} ({reg_number}) скачан и сохранён локально.")
                else:
                    custom_logger.warning(f"⚠️ Не удалось скачать PDF для Batch {full_name} ({reg_number}) по ссылке {action_link_original}")
                    return action_link_yandex # Возвращаем пустую строку
            else:
                # Используем существующую локальную копию (аналогично Stay Permit)
                with open(temp_path, "rb") as f:
                    pdf_content = f.read()
                #custom_logger.info(f"✅ Используем локальную копию PDF для Batch {full_name} ({reg_number}).")

            # Загружаем на Яндекс.Диск (общая логика)
            file_name_for_yandex = f"{reg_number}_batch_application.pdf" # Имя файла для Яндекс.Диска
            action_link_yandex = self.yandex_uploader.upload_pdf(pdf_content, file_name_for_yandex)
            if action_link_yandex:
                pass
                #custom_logger.info(f"✅ PDF для Batch {full_name} ({reg_number}) загружен. Ссылка: {action_link_yandex}")
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
                    #custom_logger.info(f"✅ PDF для Stay Permit {reg_number} скачан и сохранён локально.")
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
                #custom_logger.info(f"✅ PDF для Stay Permit {reg_number} загружен. Ссылка: {action_link_yandex}")
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
                    for item_data in result_data[:1]:
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
                                soup = BeautifulSoup(detail_result, 'html.parser')
                                birth_label = soup.find(text='Date of Birth')  # Находим текст лейбла
                                if birth_label:
                                    next_small = birth_label.find_next('small')  # Следующий <small>
                                    if next_small:
                                        date_birth = next_small.text.strip()
                                    else:
                                        date_birth = ''
                                else:
                                    date_birth = ''
                                if date_birth.count('/') != 2:
                                    date_birth = ''
                            except Exception as ex:
                                custom_logger.error(f'Ошибка при парсинге дня рождения клиента {detail_link}: {ex}')
                                date_birth = ''

                            # Создаем объект данных
                            batch_obj = BatchApplicationData(
                                batch_no=batch_no,
                                register_number=reg_number,
                                full_name=full_name,
                                visitor_visa_number=visitor_visa_number,
                                passport_number=passport_number,
                                payment_date='' if payment_date.strip() == '-' else payment_date ,
                                visa_type=visa_type,
                                status=status,
                                action_link='', # Будет заполнено позже
                                account=name,
                                birth_date=date_birth
                            )

                            #Загрузка PDF на Яндекс.Диск ---
                            final_action_link = self.pdf_manager.upload_batch_pdf(
                                session_id, action_link_original, batch_obj.register_number, batch_obj.full_name
                            )
                            batch_obj.action_link = final_action_link
                            parsed_data_list.append(batch_obj)
                            items_in_batch += 1
                            items_parsed_total += 1

                            if items_parsed_total % 10 == 0:
                                custom_logger.info(f'Спарсили {items_parsed_total} Batch Application (аккаунт {name})')

                        except Exception as item_e:
                            custom_logger.error(f"Ошибка при обработке одного элемента Batch для {name}: {item_e}")

                    custom_logger.info(f"Обработан пакет Batch для {name}, offset={offset}, items={items_in_batch}")
                    offset += 850

                # Сохранение в БД
                db_dicts = [obj.to_db_dict() for obj in parsed_data_list]
                with SessionLocal() as db:
                    save_or_update_batch_data(db, db_dicts)
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

        return [], [] 

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
                    params = {
                        'draw': '1',
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
                        custom_logger.info(f"Данные Stay Permit для {name} закончились (offset={offset})")
                        break

                    items_in_batch = 0
                    for item_data in result_data:
                        try:
                            reg_number_raw = safe_get(item_data, 'register_number')
                            if not reg_number_raw:
                                custom_logger.warning("Пропущен элемент Stay Permit без регистрационного номера.")
                                continue
                            reg_number = reg_number_raw.split("'>")[-1].split("</a>")[0]

                            full_name = safe_get(item_data, 'full_name', 'No name')
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
                                name=full_name,
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

                            # Загрузка PDF на Яндекс.Диск ---
                            action_html = safe_get(item_data, 'action')
                            pdf_relative_url = ''
                            if action_html:
                                pdf_relative_url = extract_action_link_parser(action_html) # Используем переименованную функцию
                            
                            public_link = self.pdf_manager.upload_stay_pdf(session_id, pdf_relative_url, stay_obj.reg_number)
                            stay_obj.action_link = public_link

                            parsed_data_list.append(stay_obj)
                            items_in_batch += 1
                            items_parsed_total += 1

                            if items_parsed_total % 10 == 0:
                                custom_logger.info(f'Спарсили {items_parsed_total} Stay Permit (аккаунт {name})')

                        except Exception as item_e:
                            custom_logger.error(f"Ошибка при обработке одного элемента Stay Permit для {name}: {item_e}")

                    custom_logger.info(f"Обработан пакет Stay Permit для {name}, offset={offset}, items={items_in_batch}")
                    offset += 1250

                db_dicts = [obj.to_db_dict() for obj in parsed_data_list]

                if db_dicts:
                    with SessionLocal() as db:
                        save_or_update_stay_permit_data(db, db_dicts)
                    
                    custom_logger.info(f"Данные Stay Permit для {name} сохранены в БД (всего {len(db_dicts)} записей)")

                    # ←←← А это — уведомления (в очередь)
                    for item in db_dicts:
                        notification_queue.put({
                            "type": "new_stay_permit",
                            "data": item
                        })
                    custom_logger.info(f"Добавлено {len(db_dicts)} ITK в очередь уведомлений (аккаунт {name})")
                else:
                    custom_logger.info(f"Нет данных StayPermit для аккаунта {name}")

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
            if not check_session(self.session_manager.get_session(), session_id):
                session_id = login(self.session_manager.get_session(), name, password)
                if not session_id:
                    custom_logger.warning(f"Не удалось залогиниться под {name}")
                    continue # Пропускаем аккаунт, если логин не удался

            custom_logger.info(f'[NEXT STEP] переходим к следующему этапу')

            stay_data = self.fetch_and_update_stay(name, session_id)
            batch_app, batch_mgr = self.fetch_and_update_batch(name, session_id)

            batch_app_table.extend(batch_app)
            batch_mgr_table.extend(batch_mgr)
            stay_data_table.extend(stay_data)

            temp_counter += 1
            custom_logger.info(f'Обработан аккаунт {name} ({temp_counter}/{len(account_names)})')

        return batch_app_table, batch_mgr_table, stay_data_table



class GoogleSheetsManager:
    def __init__(self):
        self.gc = None
        self.token_path = "token.json"                    # ← будет создан автоматически
        self.creds_path = "src/credentials_oauth.json"     # ← твой скачанный файл
        self._init_client()

    def _init_client(self):
        creds = None

        # Пробуем загрузить существующий токен
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)

        # Если токена нет или он просрочен — запускаем авторизацию
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                custom_logger.info("Запускаем авторизацию Google (откроется браузер)...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.creds_path, SCOPES
                )
                creds = flow.run_local_server(port=0)  # ← откроет браузер на ПК

            # Сохраняем токен для будущего использования
            with open(self.token_path, "w") as token_file:
                token_file.write(creds.to_json())
            custom_logger.info(f"Токен успешно сохранён в {self.token_path}")

        self.gc = gspread.authorize(creds)
        custom_logger.info("Google Sheets подключен через твой личный аккаунт — лимиты бесконечные!")

    def _parse_date_for_sorting(self, date_str: str) -> date:
        if not date_str:
            return date.min
        try:
            return datetime.strptime(date_str, PAYMENT_DATE_FORMAT).date()
        except ValueError:
            return date.min

    def write_to_sheet(self, batch_app_data: List[List[str]], manager_data: List[List[str]], stay_data: List[List[str]]):
        max_retries = 3
        chunk_size = 500

        for attempt in range(1, max_retries + 1):
            try:
                if not self.gc:
                    self._init_client()

                # ← ВОЛШЕБНАЯ СТРОКА — всегда актуальная таблица
                spreadsheet_key = ensure_valid_spreadsheet(self.gc)
                custom_logger.info(f"Запись данных → таблица: {spreadsheet_key}")
                spreadsheet = self.gc.open_by_key(spreadsheet_key)

                accounts_to_process = list(set(row[IDX_BA_ACCOUNT] for row in batch_app_data)) if batch_app_data else []
                custom_logger.info(f"Обновляем аккаунты: {accounts_to_process}")

                # === Batch Application ===
                header_batch = ['Batch No', 'Register Number', 'Full Name', 'Date of Birth', 'Visitor Visa Number',
                                'Passport Number', 'Payment Date', 'Visa Type', 'Status', 'Action Link', 'Account']
                ws_batch = spreadsheet.worksheet('Batch Application')
                all_batch = ws_batch.get_all_values()
                existing = [r for r in all_batch[1:] if r and len(r) >= 11 and r[IDX_BA_ACCOUNT] not in accounts_to_process]
                new_sorted = sorted(batch_app_data, key=lambda x: self._parse_date_for_sorting(x[6]), reverse=True)
                ws_batch.clear()
                self._append_rows_in_chunks(ws_batch, [header_batch] + existing + new_sorted, chunk_size)

                # === Manager ===
                header_mgr = ['Full Name', 'Visa Type', 'Payment Date', 'Status', 'Action Link', 'Account']
                ws_mgr = spreadsheet.worksheet('Batch Application(Manager)')
                all_mgr = ws_mgr.get_all_values()
                existing_mgr = [r for r in all_mgr[1:] if r and len(r) >= 6 and r[IDX_MGR_ACCOUNT] not in accounts_to_process]
                new_mgr_sorted = sorted(manager_data, key=lambda x: self._parse_date_for_sorting(x[IDX_MGR_PAYMENT_DATE]), reverse=True)
                ws_mgr.clear()
                self._append_rows_in_chunks(ws_mgr, [header_mgr] + existing_mgr + new_mgr_sorted, chunk_size)

                # === Stay Permit ===
                header_stay = ['Name', 'Type of Stay Permit', 'Visa Type', 'Arrival Date', 'Issue Date',
                               'Expired Date', 'Status', 'Action Link', 'Passport Number', 'Account']
                ws_stay = spreadsheet.worksheet('StayPermit')
                all_stay = ws_stay.get_all_values()
                existing_stay = [r for r in all_stay[1:] if r and len(r) >= 10 and r[IDX_SP_ACCOUNT] not in accounts_to_process]
                ws_stay.clear()
                self._append_rows_in_chunks(ws_stay, [header_stay] + existing_stay + stay_data, chunk_size)

                custom_logger.info("ВСЕ ДАННЫЕ УСПЕШНО ЗАПИСАНЫ В GOOGLE SHEETS")
                return

            except Exception as e:
                custom_logger.error(f"Попытка {attempt}/3 записи в Sheets: {e}\n{format_exc()}")
                if attempt == max_retries:
                    custom_logger.critical("Запись в Google Sheets провалилась окончательно!")
                    raise
                time.sleep(10 * attempt)

    def _append_rows_in_chunks(self, worksheet, data, chunk_size: int):
        if not data:
            return
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            worksheet.append_rows(chunk, value_input_option='USER_ENTERED')
            if i > 0:
                time.sleep(1)


class JobScheduler:
    def __init__(self, gs_manager: GoogleSheetsManager, data_parser):
        self.gs_manager = gs_manager
        self.data_parser = data_parser
        self.scheduler = None

    def _get_accounts_from_permanent_sheet(self) -> tuple[List[str], List[str]]:
        """Читает аккаунты из ВЕЧНОЙ таблицы GOOGLE_ACCOUNTS_SHEET_ID"""
        try:
            sheet = self.gs_manager.gc.open_by_key(GOOGLE_ACCOUNTS_SHEET_ID)
            ws = sheet.worksheet("Аккаунты")
            values = ws.get_all_values()
            if len(values) < 2:
                custom_logger.warning("В таблице аккаунтов меньше 2 строк!")
                return [], []
            names = [row[0] for row in values[1:]]
            passwords = [row[1] for row in values[1:]]
            return names, passwords
        except Exception as e:
            custom_logger.error(f"Ошибка чтения таблицы аккаунтов: {e}")
            return [], []

    def job_first_two(self):
        custom_logger.info("Запуск задачи: первые два аккаунта")
        try:
            names, passwords = self._get_accounts_from_permanent_sheet()
            if len(names) < 2:
                custom_logger.warning("Недостаточно аккаунтов")
                return
            batch_app, batch_mgr, stay = self.data_parser.parse_accounts(names[:2], passwords[:2])
            self.gs_manager.write_to_sheet(batch_app, batch_mgr, stay)
            custom_logger.info("Задача 'первые два' выполнена")
        except Exception as e:
            custom_logger.error(f"[job_first_two] Ошибка: {e}\n{format_exc()}")

    def job_others(self):
        custom_logger.info("Запуск задачи: остальные аккаунты")
        try:
            names, passwords = self._get_accounts_from_permanent_sheet()
            if len(names) <= 2:
                custom_logger.info("Нет остальных аккаунтов")
                return
            batch_app, batch_mgr, stay = self.data_parser.parse_accounts(names[2:], passwords[2:])
            self.gs_manager.write_to_sheet(batch_app, batch_mgr, stay)
            custom_logger.info("Задача 'остальные' выполнена")
        except Exception as e:
            custom_logger.error(f"[job_others] Ошибка: {e}\n{format_exc()}")

    def start_scheduler(self):
        global scheduler_jobs
        self.scheduler = BackgroundScheduler(timezone=ZoneInfo("Europe/Moscow"))
        self.scheduler.add_job(
            self.job_first_two,
            'interval',
            minutes=BATCH_PARSE_INTERVAL_MINUTES,
            coalesce=True,
            max_instances=1
        )
        self.scheduler.start()
        scheduler_jobs = self.scheduler
        custom_logger.info("Планировщик запущен (только первые два аккаунта)")


# === Основное приложение ===
class Application:
    def __init__(self):
        self.session_manager = SessionManager(PROXY)
        self.yandex_uploader = YandexDiskUploader(YANDEX_TOKEN)
        self.pdf_manager = PDFManager(self.session_manager, self.yandex_uploader)
        self.data_parser = DataParser(self.session_manager, self.pdf_manager)
        self.gs_manager = GoogleSheetsManager()
        self.job_scheduler = JobScheduler(self.gs_manager, self.data_parser)
        self.bot_runner = BotRunner(self)

    def run_parsing_cycle(self):
        custom_logger.info("Запуск начального парсинга...")
        self.job_scheduler.job_first_two()

    async def run(self):
        global error_counter
        init_db()
        self.run_parsing_cycle()
        self.job_scheduler.start_scheduler()

        # Запуск бота
        await self.bot_runner.run()


class BotRunner:
    def __init__(self, app):
        self.bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        self.dp = Dispatcher(storage=MemoryStorage(), app=app)
        self.dp.include_router(bot_router)

    async def run(self):
        await start_notification_scheduler()

        asyncio.create_task(notification_worker())


        await self.dp.start_polling(self.bot)


if __name__ == "__main__":
    app = Application()
    asyncio.run(app.run())