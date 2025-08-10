import os
from typing import Optional

from curl_cffi import requests

# Local imports from the same service
from .session_manager import SessionManager
from .yandex_uploader import YandexDiskUploader
from utils.logger import logger as custom_logger

# Define constants used by this manager
SP_TEMP_DIR = "src/temp"
os.makedirs(SP_TEMP_DIR, exist_ok=True)


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
            session = self.session_manager.get_session()
            if isinstance(session, requests.Session):
                 response = session.get(pdf_url, cookies=cookies, headers=headers)
            else: # Assuming it's a curl_cffi session if not standard requests
                 response = session.get(pdf_url, cookies=cookies, headers=headers, impersonate="chrome110")

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
            temp_path = os.path.join(SP_TEMP_DIR, f"{reg_number}_batch_application.pdf")

            if not os.path.exists(temp_path):
                pdf_content = self.download_pdf(session_id, action_link_original)
                if pdf_content:
                    with open(temp_path, "wb") as f:
                        f.write(pdf_content)
                    custom_logger.info(f"✅ PDF для Batch {full_name} ({reg_number}) скачан и сохранён локально.")
                else:
                    custom_logger.warning(f"⚠️ Не удалось скачать PDF для Batch {full_name} ({reg_number}) по ссылке {action_link_original}")
                    return action_link_yandex
            else:
                with open(temp_path, "rb") as f:
                    pdf_content = f.read()
                custom_logger.info(f"✅ Используем локальную копию PDF для Batch {full_name} ({reg_number}).")

            file_name_for_yandex = f"{reg_number}_batch_application.pdf"
            action_link_yandex = self.yandex_uploader.upload_pdf(pdf_content, file_name_for_yandex)
            if action_link_yandex:
                custom_logger.info(f"✅ PDF для Batch {full_name} ({reg_number}) загружен. Ссылка: {action_link_yandex}")
            else:
                custom_logger.error(f"❌ Не удалось получить публичную ссылку для Batch {full_name} ({reg_number}).")

        except Exception as e:
            custom_logger.error(f"❌ Ошибка при обработке/загрузке PDF Batch на Яндекс.Диск для {full_name} ({reg_number}): {e}")

        return action_link_yandex

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
