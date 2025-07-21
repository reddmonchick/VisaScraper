from database.db import init_db
from database.models import BatchApplication, StayPermit
from infrasctructure.google_sheets import setup_google_sheet, prepare_worksheet
from session_manager import login, check_session, load_session, save_value
from utils.driver_uploader import upload_to_yandex_disk
from utils.logger import logger as logging
from utils.parser import safe_get, extract_status_batch, extract_status, extract_action_link, extract_reg_number, extract_visa, extract_detail
from bot.bot import dp, bot
from bot.handler import bot_router
from utils.scheduler import start_scheduler as start_notification_scheduler
from database.crud import save_batch_data, save_stay_permit_data
from apscheduler.schedulers.background import BackgroundScheduler
from database.db import SessionLocal
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
import gspread
from apscheduler.executors.pool import ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv
from curl_cffi import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

from traceback import format_exc

# Загрузка переменных из .env
import os
import sys
import threading
import asyncio
import time
import json
import signal
import sys
load_dotenv()

# Подключение роутера
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(bot_router)
error_counter = 0
MAX_ERRORS_BEFORE_RESTART = 5

# Функция сохранения данных в БД
session = requests.Session()

# === Глобальные переменные для хранения временных данных ===
cached_batch_application_data = []
cached_manager_data = []
cached_stay_permit_data = []

def shutdown_handler(signal, frame):
    logging.info("Получен сигнал завершения. Останавливаем планировщики...")
    if scheduler_jobs:
        scheduler_jobs.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def download_pdf(session_id: str, pdf_url: str) -> bytes | None:
    cookies = {'PHPSESSID': session_id}
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://evisa.imigrasi.go.id/ ',
    }
    try:
        response = session.get(pdf_url, cookies=cookies, headers=headers)
        if response.status_code == 200 and 'application/pdf' in response.headers['Content-Type']:
            return response.content
        else:
            print(f"Ошибка загрузки PDF: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")
            return None
    except Exception as e:
        print(f"Ошибка при загрузке PDF: {e}")
        return None


def fetch_and_update_batch(name, session_id):

    offset = 0
    client_data = []
    manager_data = []
    client_data_table = []

    attempt = 0
    max_attempts = 3
    while attempt < max_attempts:
        temp_counter = 0
        try:
            while True:
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

                response = session.post(
                    'https://evisa.imigrasi.go.id/web/applications/batch/data', 
                    headers=headers,
                    data=data,
                    cookies=cookies
                )
                logging.info(f'Запрос на Batch Application {response}')

                if response.status_code != 200:
                    print(f"Ошибка получения данных для Batch от {name} {response.status_code}")
                    break

                result = response.json().get('data', [])
                if not result:
                    break

                for res in result:
                    batch_no = safe_get(res, 'header_code').strip().replace('\n', '')
                    reg_number = safe_get(res, 'register_number')
                    full_name = safe_get(res, 'full_name')
                    visitor_visa_number = safe_get(res, 'request_code')
                    passport_number = safe_get(res, 'passport_number')
                    payment_date = safe_get(res, 'paid_date')
                    visa_type = safe_get(res, 'visa_type')
                    status = extract_status_batch(safe_get(res, 'status'))
                    action = extract_visa(safe_get(res, 'actions'))
                    action_link = f"https://evisa.imigrasi.go.id{action}"  if action.split('/')[-1] == 'print' else ''

                    detail_link = f"https://evisa.imigrasi.go.id{extract_detail(safe_get(res, 'actions'))}"
                    try:
                        response = session.get(detail_link)
                        result = response.text
                        date_birth = result.split('Date of Birth')[-1].split('</small')[0].split('<small>')[-1]
                    except Exception as ex:
                        logging.error(f'Ошибка при парсинге дня рождения клиента {detail_link} {ex}')
                        date_birth = ''

                    client_data.append({
                        "batch_no": batch_no,
                        "register_number": reg_number,
                        "full_name": full_name,
                        "visitor_visa_number": visitor_visa_number,
                        "passport_number": passport_number,
                        "payment_date": payment_date,
                        "visa_type": visa_type,
                        "status": status,
                        "action_link": action_link,
                        "account": name,
                        "birth_date": date_birth
                    })
                    client_data_table.append([
                        batch_no,
                        reg_number,
                        full_name,
                        date_birth,
                        visitor_visa_number,
                        passport_number,
                        payment_date,
                        visa_type,
                        status,
                        action_link,
                        name
                    ])
                    manager_data.append([
                        full_name, visa_type, payment_date, status, action_link, name
                    ])
                    temp_counter += 1
                    if temp_counter % 10 == 0:
                        logging.info(f'Спарсили {temp_counter} Batch Application')

                offset += 850


            with SessionLocal() as db:
                save_batch_data(db, client_data)
                logging.info("Данные Batch Application сохранены в БД")

            return client_data_table, manager_data
        
        except Exception as exc:
            attempt += 1
            logging.error(f"Ошибка в fetch_and_update_batch (аккаунт {name}, попытка {attempt}/{max_attempts}): {exc}")
            if attempt >= max_attempts:
                logging.warning(f"Не удалось спарсить аккаунт {name} после {max_attempts} попыток.")
                return [], []
            time.sleep(10)


def fetch_and_update_stay(name, session_id):
    """Обрабатывает данные Stay Permit и загружает их в Google Sheets."""

    offset = 0
    client_data = []
    client_write_data = []

    attempt = 0
    max_attempts = 3

    while attempt < max_attempts:
        try:
            while True:
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
                    'columns[0][data]': 'register_number',
                    'columns[0][searchable]': 'true',
                    'columns[0][orderable]': 'true',
                    'columns[0][search][value]': '',
                    'columns[0][search][regex]': 'false',
                    'columns[1][data]': 'full_name',
                    'columns[1][searchable]': 'true',
                    'columns[1][orderable]': 'true',
                    'columns[1][search][value]': '',
                    'columns[1][search][regex]': 'false',
                    'columns[2][data]': 'permit_number',
                    'columns[2][searchable]': 'true',
                    'columns[2][orderable]': 'true',
                    'columns[2][search][value]': '',
                    'columns[2][search][regex]': 'false',
                    'columns[3][data]': 'type_of_staypermit',
                    'columns[3][searchable]': 'true',
                    'columns[3][orderable]': 'true',
                    'columns[3][search][value]': '',
                    'columns[3][search][regex]': 'false',
                    'columns[4][data]': 'visa_number',
                    'columns[4][searchable]': 'true',
                    'columns[4][orderable]': 'true',
                    'columns[4][search][value]': '',
                    'columns[4][search][regex]': 'false',
                    'columns[5][data]': 'type_of_visa',
                    'columns[5][searchable]': 'true',
                    'columns[5][orderable]': 'true',
                    'columns[5][search][value]': '',
                    'columns[5][search][regex]': 'false',
                    'columns[6][data]': 'passport_number',
                    'columns[6][searchable]': 'true',
                    'columns[6][orderable]': 'true',
                    'columns[6][search][value]': '',
                    'columns[6][search][regex]': 'false',
                    'columns[7][data]': 'start_date',
                    'columns[7][searchable]': 'true',
                    'columns[7][orderable]': 'true',
                    'columns[7][search][value]': '',
                    'columns[7][search][regex]': 'false',
                    'columns[8][data]': 'issue_date',
                    'columns[8][searchable]': 'true',
                    'columns[8][orderable]': 'true',
                    'columns[8][search][value]': '',
                    'columns[8][search][regex]': 'false',
                    'columns[9][data]': 'expired_date',
                    'columns[9][searchable]': 'true',
                    'columns[9][orderable]': 'true',
                    'columns[9][search][value]': '',
                    'columns[9][search][regex]': 'false',
                    'columns[10][data]': 'status',
                    'columns[10][searchable]': 'true',
                    'columns[10][orderable]': 'true',
                    'columns[10][search][value]': '',
                    'columns[10][search][regex]': 'false',
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

                response = session.get(
                    'https://evisa.imigrasi.go.id/front/applications/stay-permit/data', 
                    headers=headers,
                    cookies=cookies,
                    params=params,
                    verify=False
                )
                logging.info(f'Запрос на Stay Permit {response}')

                if response.status_code != 200:
                    logging.info(f"Ошибка получения данных для Stay Permit от {name}")
                    break

                result = response.json().get('data', [])
                if not result:
                    break

                temp_counter = 0

                for res in result:
                    reg_number = res.get('register_number').split("'>")[-1].split("</a>")[0]
                    full_name = res.get('full_name', '')
                    type_permit = res.get('type_of_staypermit', '')
                    type_visa = res.get('type_of_visa', '')
                    start_date = res.get('start_date', '')
                    issue_data = res.get('issue_date', '')
                    expired_data = res.get('expired_date', '')
                    passport_number = res.get('passport_number', '')
                    status = extract_status(res.get('status', ''))
                    action_result = ''
                    try:
                        action_html = safe_get(res, 'action')
                        if action_html:
                            pdf_relative_url = extract_action_link(action_html)
                            if pdf_relative_url:
                                temp_path = f"src/temp/{reg_number}_stay_permit.pdf"
                                if not os.path.exists(temp_path):
                                    #print(f"Файл {reg_number} ещё не скачан. Скачиваем...")
                                    pdf_content = download_pdf(session_id, pdf_relative_url)
                                    if pdf_content:
                                        # Сохраняем временно
                                        os.makedirs("src/temp", exist_ok=True)
                                        with open(temp_path, "wb") as f:
                                            f.write(pdf_content)
                                        #print(f"Файл {reg_number} успешно сохранён локально.")
                                    else:
                                        #print(f"Не удалось скачать файл для {reg_number}")
                                        action_result = ''
                                        continue
                                else:
                                    pass
                                    #print(f"Файл {reg_number} уже скачан, используем локальную копию.")

                                # Читаем содержимое файла
                                with open(temp_path, "rb") as f:
                                    pdf_content = f.read()

                                # Загружаем на Google Drivef
                                temp_counter += 1
                                public_link = upload_to_yandex_disk(pdf_content, f"{reg_number}_stay_permit.pdf")
                                action_result = public_link
                                if temp_counter % 10 == 0:
                                    logging.info(f'Скачали и вставили {temp_counter} ссылок(Stay Permit)')
                        else:
                            action_result = ''
                    except Exception as e:
                        print(f"Ошибка при обработке действия: {e}")
                        action_result = ''

                    client_data.append([
                        full_name, type_permit, type_visa, start_date, issue_data,
                        expired_data, status, action_result,passport_number, name
                    ])
                    client_write_data.append({
                        'reg_number': reg_number,
                        "name": full_name,
                        "type_of_staypermit": type_permit,
                        "visa_type": type_visa,
                        "passport_number": passport_number,
                        "arrival_date": start_date,
                        "issue_date": issue_data,
                        "expired_date": expired_data,
                        "status": status,
                        "action_link": action_result,
                        "account": name
                    })
                offset += 1250
            with SessionLocal() as db:
                save_stay_permit_data(db, client_write_data)
                print("✅ Данные Stay Permit сохранены в БД")
            logging.info(f"Данные Stay Permit для {name} успешно обновлены.")
            return client_data
        
        except Exception as exc:
            attempt += 1
            logging.error(f"Ошибка в fetch_and_update_stay (аккаунт {name}, попытка {attempt}/{max_attempts}): {exc}")
            if attempt >= max_attempts:
                logging.warning(f"Не удалось спарсить Stay Permit для {name}.")
                return []
            time.sleep(10)


def parse_accounts(account_names, account_passwords):
    attempt = 0
    max_attempts = 3

    while attempt < max_attempts:
        try:
            credentials = json.load(open(os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")))
            gc = gspread.service_account_from_dict(credentials)
            spreadsheet_batch = gc.open_by_key(os.getenv("GOOGLE_SHEET_BATCH_ID"))

            worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
            names = worksheet_account.col_values(1)[1:]
            passwords = worksheet_account.col_values(2)[1:]
            if not names or not passwords:
                logging.warning("Нет аккаунтов для парсинга")
                return [], [], []

            batch_app_table = []
            batch_mgr_table = []
            stay_data_table = []

            for name, password in zip(account_names, account_passwords):
                session_id = load_session(name)
                if not check_session(session_id):
                    session_id = login(name, password)
                    if not session_id:
                        continue
                stay_data = fetch_and_update_stay(name, session_id)
                batch_app, batch_mgr = fetch_and_update_batch(name, session_id)
                batch_app_table.extend(batch_app)
                batch_mgr_table.extend(batch_mgr)
                stay_data_table.extend(stay_data)

            return batch_app_table, batch_mgr_table, stay_data_table

        except Exception as e:
            attempt += 1
            logging.error(f"Ошибка в parse_accounts (попытка {attempt}/{max_attempts}): {e}")
            if attempt >= max_attempts:
                logging.critical("Критическая ошибка в парсинге. Передаём управление в main для перезапуска.")
                raise
            time.sleep(10)


def write_to_sheet(gc, spreadsheet_key, batch_app_data, manager_data, stay_data):
    try:
        credentials = json.load(open(os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")))
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(spreadsheet_key)

        worksheet_batch = spreadsheet.worksheet('Batch Application')
        worksheet_manager = spreadsheet.worksheet('Batch Application(Manager)')
        worksheet_stay = spreadsheet.worksheet('StayPermit')

        # Получаем список аккаунтов, которые относятся к job_first_two
        first_two_accounts = [row[10] for row in batch_app_data]  # Account находится в 10-м столбце
        first_two_accounts = list(set(first_two_accounts))  # Уникальные аккаунты

        # --- Batch Application ---
        # Получаем все данные
        all_batch_data = worksheet_batch.get_all_values()
        header = all_batch_data[0]
        existing_data = all_batch_data[1:]

        # Фильтруем существующие данные, удаляя старые данные по первым двум аккаунтам
        filtered_data = [
            row for row in existing_data if row[10] not in first_two_accounts
        ]

        # Добавляем новые данные
        updated_batch_data = [header] + filtered_data + batch_app_data

        # Перезаписываем
        worksheet_batch.clear()
        worksheet_batch.append_rows(updated_batch_data)

        # --- Manager Worksheet ---
        all_mgr_data = worksheet_manager.get_all_values()
        existing_mgr_data = all_mgr_data[1:]
        filtered_mgr_data = [
            row for row in existing_mgr_data if row[5] not in first_two_accounts
        ]
        updated_mgr_data = [all_mgr_data[0]] + filtered_mgr_data + manager_data
        worksheet_manager.clear()
        worksheet_manager.append_rows(updated_mgr_data)

        # --- Stay Permit ---
        all_stay_data = worksheet_stay.get_all_values()
        existing_stay_data = all_stay_data[1:]
        filtered_stay_data = [
            row for row in existing_stay_data if row[9] not in first_two_accounts
        ]
        updated_stay_data = [all_stay_data[0]] + filtered_stay_data + stay_data
        worksheet_stay.clear()
        worksheet_stay.append_rows(updated_stay_data)

        logging.info("✅ Все данные успешно обновлены в Google Sheets")

    except Exception as e:
        logging.error(f"❌ Ошибка при записи в Google Sheets: {e}")

def job_first_two():
    global cached_batch_application_data, cached_manager_data, cached_stay_permit_data
    logging.info("Запуск задачи для первых двух аккаунтов")

    try:
        credentials = json.load(open(os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")))
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet_batch = gc.open_by_key(os.getenv("GOOGLE_SHEET_BATCH_ID"))
        worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
        names = worksheet_account.col_values(1)
        passwords = worksheet_account.col_values(2)
        if len(names) < 2:
            logging.warning("Недостаточно аккаунтов для выполнения задачи первых двух")
            return

        first_two_names = names[:2]
        first_two_passwords = passwords[:2]

        batch_app, batch_mgr, stay = parse_accounts(first_two_names, first_two_passwords)

        cached_batch_application_data = batch_app
        cached_manager_data = batch_mgr
        cached_stay_permit_data = stay

        write_to_sheet(gc, os.getenv("GOOGLE_SHEET_BATCH_ID"), batch_app, batch_mgr, stay)

    except Exception as e:
        logging.error(f"[job_first_two] Критическая ошибка: {e}", exc_info=True)
        raise  # передаём ошибку выше, чтобы main() мог перезапустить


def job_others():
    logging.info("Запуск задачи для остальных аккаунтов")

    try:
        credentials = json.load(open(os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")))
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet_batch = gc.open_by_key(os.getenv("GOOGLE_SHEET_BATCH_ID"))
        worksheet_account = spreadsheet_batch.worksheet('Аккаунты')
        names = worksheet_account.col_values(1)
        passwords = worksheet_account.col_values(2)
        if len(names) <= 2:
            logging.info("Недостаточно аккаунтов для выполнения полного цикла")
            return

        remaining_names = names[2:]
        remaining_passwords = passwords[2:]

        batch_app_new, batch_mgr_new, stay_new = parse_accounts(remaining_names, remaining_passwords)

        full_batch = cached_batch_application_data + batch_app_new
        full_manager = cached_manager_data + batch_mgr_new
        full_stay = cached_stay_permit_data + stay_new

        write_to_sheet(gc, os.getenv("GOOGLE_SHEET_BATCH_ID"), full_batch, full_manager, full_stay)

    except Exception as e:
        logging.error(f"[job_others] Критическая ошибка: {e}", exc_info=True)
        raise


# === Функция запуска планировщика ===
scheduler_jobs = None  # Для APScheduler парсинга

def start_parser_scheduler():
    global scheduler_jobs
    scheduler_jobs = BackgroundScheduler(timezone=ZoneInfo("Europe/Moscow"),
                                         executors={'default': ThreadPoolExecutor(2)})
    scheduler_jobs.add_job(job_first_two, 'interval', minutes=10,coalesce=True,misfire_grace_time=60 * 5)
    scheduler_jobs.add_job(job_others,'cron', hour=7, minute=0,coalesce=True,misfire_grace_time=60 * 5)
    scheduler_jobs.start()
    logging.info("Планировщик парсинга запущен")


def main():
    global error_counter
    init_db()

    # Переменная для отслеживания успешного старта
    started = False

    while not started:
        try:
            # Запуск бота в отдельном потоке

            # Парсинг и планировщики
            logging.info("Запуск начального парсинга...")
            job_first_two()
            job_others()
            start_parser_scheduler()
            logging.info("Основной поток работает")
            started = True
            error_counter = 0  # Сброс счётчика после успешного запуска
        except Exception as e:
            error_counter += 1
            logging.error(f"Ошибка при запуске (попытка {error_counter}/{MAX_ERRORS_BEFORE_RESTART}): {e}")
            if scheduler_jobs:
                scheduler_jobs.shutdown()
            if error_counter >= MAX_ERRORS_BEFORE_RESTART:
                logging.critical("Превышено количество попыток. Перезапуск программы через 60 секунд...")
                time.sleep(60)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            else:
                logging.warning(f"Перезапуск через 30 секунд...")
                time.sleep(30)


async def run_all():
    # Запускаем main в отдельном потоке
    parser_thread = threading.Thread(target=main)
    parser_thread.start()

    # Ждём, пока main инициализирует планировщики и запустит парсинг
    #await asyncio.sleep(5)

    # Запускаем бота
    #await start_notification_scheduler()  # запуск асинхронного планировщика
    #await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(run_all())