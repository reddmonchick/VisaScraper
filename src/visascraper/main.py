
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


import gspread
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from curl_cffi import requests
from bs4 import BeautifulSoup
import yadisk 
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage


from database.db import init_db, SessionLocal
from database.models import BatchApplication, StayPermit
from database.crud import save_or_update_batch_data, save_or_update_stay_permit_data
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


from dotenv import load_dotenv
load_dotenv()


MAX_ERRORS_BEFORE_RESTART = 5
RESTART_DELAY_AFTER_ERRORS = 60 # секунд
RETRY_DELAY_AFTER_ERROR = 30 # секунд
BATCH_PARSE_INTERVAL_MINUTES = 10
STAY_PARSE_HOUR = 7
STAY_PARSE_MINUTE = 0
GS_BATCH_SHEET_ID = os.getenv("GOOGLE_SHEET_BATCH_ID")
GS_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")
YANDEX_TOKEN = os.getenv('YANDEX_TOKEN')
PROXY = os.getenv('PROXY')


IDX_BA_ACCOUNT = 10 # Индекс столбца 'account' в client_data_table (Batch)
IDX_MGR_ACCOUNT = 5  # Индекс столбца 'account' в manager_data (Batch Manager)
IDX_MGR_PAYMENT_DATE = 2 # Индекс столбца 'payment_date' в manager_data
IDX_SP_ACCOUNT = 9   # Индекс столбца 'account' в stay_data (Stay Permit)
IDX_SP_ACTION_LINK = 7 # Индекс столбца 'action_link' в stay_data

PAYMENT_DATE_FORMAT = "%d-%m-%Y" # Формат даты в строке, например, '28-03-2025'
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
                                if date_birth.count('/') != 2:
                                    date_birth = ''
                            except Exception as ex:
                                custom_logger.error(f'Ошибка при парсинге дня рождения клиента {detail_link}: {ex}')

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

                # Сохранение в БД
                db_dicts = [obj.to_db_dict() for obj in parsed_data_list]
                with SessionLocal() as db:
                    save_or_update_stay_permit_data(db, db_dicts)
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

    def write_to_sheet(self, spreadsheet_key: str, batch_app_data: List[List[str]], manager_data: List[List[str]], stay_data: List[List[str]]):
        max_retries = 3
        base_delay = 5
        chunk_size = 500

        for attempt in range(1, max_retries + 1):
            try:
                if not self.gc:
                    self._init_client()

                spreadsheet = self.gc.open_by_key(spreadsheet_key)
                
                accounts_to_process = list(set(row[IDX_BA_ACCOUNT] for row in batch_app_data)) if batch_app_data else []
                custom_logger.info(f"Обновляем данные для аккаунтов: {accounts_to_process}")

                # --- Batch Application ---
                header_batch = ['Batch No', 'Register Number', 'Full Name', 'Date of Birth', 'Visitor Visa Number', 'Passport Number', 'Payment Date', 'Visa Type', 'Status', 'Action Link', 'Account']  # Фиксированный заголовок
                num_columns_batch = len(header_batch)

                worksheet_batch = spreadsheet.worksheet('Batch Application')
                all_batch_data = worksheet_batch.get_all_values()
                existing_batch_data = all_batch_data[1:] if len(all_batch_data) > 1 else []  # Игнорируем старый заголовок

                # Нормализуем старые строки: удаляем ведущие '', обрезаем/паддим до num_columns_batch
                normalized_existing = []
                for row in existing_batch_data:
                    # Удаляем ведущие ''
                    while row and row[0] == '':
                        row.pop(0)
                    # Обрезаем если длиннее, паддим '' если короче
                    if len(row) > num_columns_batch:
                        row = row[:num_columns_batch]
                    elif len(row) < num_columns_batch:
                        row += [''] * (num_columns_batch - len(row))
                    normalized_existing.append(row)

                # Фильтруем (теперь len(row) == num_columns_batch, но для safety)
                filtered_batch_data = [
                    row for row in normalized_existing if len(row) == num_columns_batch and row[IDX_BA_ACCOUNT] not in accounts_to_process
                ]
                
                sorted_new_batch_data = sorted(batch_app_data, key=lambda row: self._parse_date_for_sorting(row[6]), reverse=True)
                
                updated_batch_data = filtered_batch_data + sorted_new_batch_data
                
                worksheet_batch.clear()
                self._append_rows_in_chunks(worksheet_batch, [header_batch] + updated_batch_data, chunk_size)
                custom_logger.info("✅ Данные Batch Application обновлены в Google Sheets")

                # --- Batch Application(Manager) --- (аналогично, фиксированный заголовок)
                header_mgr = ['Full Name', 'Visa Type', 'Payment Date', 'Status', 'Action Link', 'Account']
                num_columns_mgr = len(header_mgr)

                worksheet_manager = spreadsheet.worksheet('Batch Application(Manager)')
                all_mgr_data = worksheet_manager.get_all_values()
                existing_mgr_data = all_mgr_data[1:] if len(all_mgr_data) > 1 else []

                normalized_mgr = []  # Аналогичная нормализация
                for row in existing_mgr_data:
                    while row and row[0] == '':
                        row.pop(0)
                    if len(row) > num_columns_mgr:
                        row = row[:num_columns_mgr]
                    elif len(row) < num_columns_mgr:
                        row += [''] * (num_columns_mgr - len(row))
                    normalized_mgr.append(row)

                filtered_mgr_data = [
                    row for row in normalized_mgr if len(row) == num_columns_mgr and row[IDX_MGR_ACCOUNT] not in accounts_to_process
                ]
                
                sorted_new_mgr_data = sorted(manager_data, key=lambda row: self._parse_date_for_sorting(row[IDX_MGR_PAYMENT_DATE]), reverse=True)
                
                updated_mgr_data = filtered_mgr_data + sorted_new_mgr_data
                
                worksheet_manager.clear()
                self._append_rows_in_chunks(worksheet_manager, [header_mgr] + updated_mgr_data, chunk_size)
                custom_logger.info("✅ Данные Batch Application(Manager) обновлены в Google Sheets")

                # --- Stay Permit --- (аналогично)
                header_stay = ['Name', 'Type of Stay Permit', 'Visa Type', 'Arrival Date', 'Issue Date', 'Expired Date', 'Status', 'Action Link', 'Passport Number', 'Account']
                num_columns_stay = len(header_stay)

                worksheet_stay = spreadsheet.worksheet('StayPermit')
                all_stay_data = worksheet_stay.get_all_values()
                existing_stay_data = all_stay_data[1:] if len(all_stay_data) > 1 else []

                normalized_stay = []
                for row in existing_stay_data:
                    while row and row[0] == '':
                        row.pop(0)
                    if len(row) > num_columns_stay:
                        row = row[:num_columns_stay]
                    elif len(row) < num_columns_stay:
                        row += [''] * (num_columns_stay - len(row))
                    normalized_stay.append(row)

                filtered_stay_data = [
                    row for row in normalized_stay if len(row) == num_columns_stay and row[IDX_SP_ACCOUNT] not in accounts_to_process
                ]

                updated_stay_data = filtered_stay_data + stay_data
                
                worksheet_stay.clear()
                self._append_rows_in_chunks(worksheet_stay, [header_stay] + updated_stay_data, chunk_size)
                custom_logger.info("✅ Данные Stay Permit обновлены в Google Sheets")
                
                return 

            except Exception as e:
                custom_logger.warning(f"⚠️ Попытка {attempt}/{max_retries} записи в Google Sheets не удалась: {e}\n{format_exc()}")
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    custom_logger.info(f"Повтор через {delay} секунд...")
                    time.sleep(delay)
                else:
                    custom_logger.error(f"❌ Все {max_retries} попытки записи в Google Sheets не удались.")
                    raise

    def _append_rows_in_chunks(self, worksheet, data, chunk_size: int):
        """Добавляет данные в лист Google Sheets порциями."""
        if not data or not any(data):
            custom_logger.warning(f"Нет данных для записи в лист '{worksheet.title}'.")
            return

        custom_logger.info(f"Начинаем запись {len(data)} строк в лист '{worksheet.title}'...")
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            if i > 0: 
                time.sleep(1) 
            try:
                worksheet.append_rows(chunk, value_input_option='USER_ENTERED')
                custom_logger.debug(f"Записана порция {i//chunk_size + 1} в '{worksheet.title}'")
            except Exception as e:
                custom_logger.error(f"❌ Ошибка при записи порции в лист '{worksheet.title}': {e}")
                raise
        custom_logger.info(f"✅ Запись данных в лист '{worksheet.title}' завершена.") 

class JobScheduler:
    """Управление задачами планировщика."""
    def __init__(self, gs_manager: GoogleSheetsManager, data_parser: DataParser):
        self.gs_manager = gs_manager
        self.data_parser = data_parser
        self.scheduler = None
        # УБРАЛИ ВЕСЬ КЭШ, ОН БЫЛ ИСТОЧНИКОМ ОШИБКИ
        # self.cached_batch_application_data ... и т.д.

    def job_first_two(self):
        """Задача для парсинга ПЕРВЫХ ДВУХ аккаунтов (для планировщика)."""
        custom_logger.info("Запуск задачи для первых двух аккаунтов")
        try:
            if not self.gs_manager.gc:
                self.gs_manager._init_client()
                
            spreadsheet_batch = self.gs_manager.gc.open_by_key(GS_BATCH_SHEET_ID)
            worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
            all_values = worksheet_account.get_all_values()
            if len(all_values) < 2:
                custom_logger.warning("Недостаточно аккаунтов для выполнения задачи 'первых двух'")
                return

            names = [row[0] for row in all_values[0:2]] # Берем только 2 и 3 строки (индексы 1 и 2)
            passwords = [row[1] for row in all_values[0:2]]

            # Парсим ТОЛЬКО эти аккаунты
            batch_app, batch_mgr, stay = self.data_parser.parse_accounts(names, passwords)

            # Записываем в Google Sheets ТОЛЬКО их данные
            self.gs_manager.write_to_sheet(GS_BATCH_SHEET_ID, batch_app, batch_mgr, stay)
            custom_logger.info("✅ Задача для первых двух аккаунтов успешно выполнена.")

        except Exception as e:
            custom_logger.error(f"[job_first_two] Критическая ошибка: {e}\n{format_exc()}")

    def job_others(self):
        """Задача для парсинга ОСТАЛЬНЫХ аккаунтов (для вызова из бота)."""
        custom_logger.info("Запуск задачи для остальных аккаунтов")
        try:
            if not self.gs_manager.gc:
                self.gs_manager._init_client()
                
            spreadsheet_batch = self.gs_manager.gc.open_by_key(GS_BATCH_SHEET_ID)
            worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
            all_values = worksheet_account.get_all_values()
            
            if len(all_values) <= 3: # Если аккаунтов 2 или меньше (1 заголовок + 2 акка)
                custom_logger.info("Нет 'остальных' аккаунтов для обработки.")
                return

            # Берем все, начиная с 4-й строки (индекс 3)
            remaining_accounts = all_values[2:]
            names = [row[0] for row in remaining_accounts]
            passwords = [row[1] for row in remaining_accounts]

            if not names:
                custom_logger.info("Не найдено 'остальных' аккаунтов для парсинга.")
                return

            # Парсим ТОЛЬКО эти аккаунты
            batch_app, batch_mgr, stay = self.data_parser.parse_accounts(names, passwords)

            # Записываем в Google Sheets ТОЛЬКО их данные
            self.gs_manager.write_to_sheet(GS_BATCH_SHEET_ID, batch_app, batch_mgr, stay)
            custom_logger.info("✅ Задача для остальных аккаунтов успешно выполнена.")

        except Exception as e:
            custom_logger.error(f"[job_others] Критическая ошибка: {e}\n{format_exc()}")


    def start_scheduler(self):
        """Запускает планировщик задач парсинга (только для первых двух)."""
        global scheduler_jobs
        self.scheduler = BackgroundScheduler(
            timezone=ZoneInfo("Europe/Moscow"),
            executors={'default': ThreadPoolExecutor(2)}
        )
        self.scheduler.add_job(
            self.job_first_two, # В расписании только эта задача
            'interval',
            minutes=BATCH_PARSE_INTERVAL_MINUTES,
            coalesce=True,
            misfire_grace_time=60 * 5
        )
        self.scheduler.start()
        scheduler_jobs = self.scheduler
        custom_logger.info("Планировщик парсинга запущен (только для первых двух аккаунтов)")

class BotRunner:
    """Запуск и управление Telegram ботом."""
    def __init__(self, app):
        self.bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        # Передаем экземпляр 'app' в Dispatcher, чтобы он был доступен в хендлерах
        self.dp = Dispatcher(storage=MemoryStorage(), app=app)
        self.dp.include_router(bot_router)

    async def run(self):
        """Асинхронный запуск бота."""
        await start_notification_scheduler() # запуск асинхронного планировщика уведомлений
        await self.dp.start_polling(self.bot)

class Application:
    """Основной класс приложения, объединяющий все компоненты."""
    def __init__(self):
        self.session_manager = SessionManager(PROXY)
        self.yandex_uploader = YandexDiskUploader(YANDEX_TOKEN)
        self.pdf_manager = PDFManager(self.session_manager, self.yandex_uploader)
        self.data_parser = DataParser(self.session_manager, self.pdf_manager)
        self.gs_manager = GoogleSheetsManager(GS_CREDENTIALS_PATH)
        # Передаем data_parser в JobScheduler
        self.job_scheduler = JobScheduler(self.gs_manager, self.data_parser) 
        # Передаем 'self' (экземпляр Application) в BotRunner
        self.bot_runner = BotRunner(self)

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

        self.job_scheduler.job_first_two()
        #self.job_scheduler.job_others()

    async def run(self):
        """Асинхронная точка входа для запуска всего приложения."""
        global error_counter
        init_db()
        
        self.setup_signal_handlers() 

        parser_thread = threading.Thread(target=self.main_loop, daemon=True)
        parser_thread.start()
        

        await asyncio.sleep(5)
        

        await self.bot_runner.run()

    def main_loop(self):
        """Основной цикл запуска приложения."""
        global error_counter
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
                if self.job_scheduler.scheduler:
                    self.job_scheduler.scheduler.shutdown()
                if error_counter >= MAX_ERRORS_BEFORE_RESTART:
                    custom_logger.critical("Превышено количество попыток. Перезапуск программы через 60 секунд...")
                    time.sleep(RESTART_DELAY_AFTER_ERRORS)
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                else:
                    custom_logger.warning(f"Перезапуск через {RETRY_DELAY_AFTER_ERROR} секунд...")
                    time.sleep(RETRY_DELAY_AFTER_ERROR)



if __name__ == "__main__":
    app = Application()
    asyncio.run(app.run())
