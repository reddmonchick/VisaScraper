import time
from datetime import datetime, date
from typing import List, Tuple

from database.db import SessionLocal
from database.crud import save_or_update_batch_data, save_or_update_stay_permit_data
from utils.logger import logger as custom_logger
from utils.parser import (
    safe_get, extract_status_batch, extract_status,
    extract_action_link as extract_action_link_parser,
    extract_reg_number, extract_visa, extract_detail
)

from .data_models import BatchApplicationData, StayPermitData
from .session_manager import SessionManager
from .pdf_manager import PDFManager

PAYMENT_DATE_FORMAT = "%d-%m-%Y"

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

    def fetch_and_update_batch(self, name: str, session_id: str) -> Tuple[List, List]:
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

                            batch_obj = BatchApplicationData(
                                batch_no=batch_no,
                                register_number=reg_number,
                                full_name=full_name,
                                visitor_visa_number=visitor_visa_number,
                                passport_number=passport_number,
                                payment_date='' if payment_date.strip() == '-' else payment_date ,
                                visa_type=visa_type,
                                status=status,
                                action_link='',
                                account=name,
                                birth_date=date_birth
                            )

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

                db_dicts = [obj.to_db_dict() for obj in parsed_data_list]
                with SessionLocal() as db:
                    save_or_update_batch_data(db, db_dicts)
                    custom_logger.info(f"✅ Данные Batch Application для {name} сохранены в БД (всего {len(db_dicts)} записей)")

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
                                action_link='',
                                account=name
                            )

                            action_html = safe_get(item_data, 'action')
                            pdf_relative_url = ''
                            if action_html:
                                pdf_relative_url = extract_action_link_parser(action_html)

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
                with SessionLocal() as db:
                    save_or_update_stay_permit_data(db, db_dicts)
                    custom_logger.info(f"✅ Данные Stay Permit для {name} сохранены в БД (всего {len(db_dicts)} записей)")

                sheet_data = [obj.to_sheet_row() for obj in parsed_data_list]
                return sheet_data

            except Exception as exc:
                attempt += 1
                custom_logger.error(f"Ошибка в fetch_and_update_stay (аккаунт {name}, попытка {attempt}/{max_attempts}): {exc}")
                if attempt >= max_attempts:
                    custom_logger.warning(f"Не удалось спарсить Stay Permit для {name}.")
                    return []
                time.sleep(10)

        return []

    def parse_accounts(self, account_names: List[str], account_passwords: List[str]) -> Tuple[List, List, List]:
        """Парсит данные для списка аккаунтов."""
        custom_logger.info(f"Начинаем парсинг для {len(account_names)} аккаунтов.")
        if not account_names or not account_passwords:
            custom_logger.warning("Нет аккаунтов для парсинга")
            return [], [], []

        # These are defined in the original main.py, need to find where they come from
        # For now, I'll assume they are available or I'll move them.
        # It seems they are related to session management logic that is not in DataParser
        from .session_manager import login, check_session, load_session

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
                    continue

            stay_data = self.fetch_and_update_stay(name, session_id)
            batch_app, batch_mgr = self.fetch_and_update_batch(name, session_id)

            batch_app_table.extend(batch_app)
            batch_mgr_table.extend(batch_mgr)
            stay_data_table.extend(stay_data)

            temp_counter += 1
            custom_logger.info(f'Обработан аккаунт {name} ({temp_counter}/{len(account_names)})')

        return batch_app_table, batch_mgr_table, stay_data_table
