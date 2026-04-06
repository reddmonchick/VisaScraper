from __future__ import annotations

import os
from io import BytesIO
from typing import Optional

import yadisk
from curl_cffi import requests

from visascraper.config import ensure_runtime_dirs, settings
from visascraper.utils.logger import logger

ensure_runtime_dirs()


class SessionManager:
    """Factory for HTTP sessions with optional proxy support."""

    def __init__(self, proxies: str | None = None):
        self.proxies = proxies

    def create_session(self) -> requests.Session:
        session = requests.Session()
        if self.proxies:
            proxy_url = f"http://{self.proxies}"
            session.proxies.update({"http": proxy_url, "https": proxy_url})
        return session

    @staticmethod
    def close_session(session: requests.Session | None) -> None:
        if session is None:
            return
        try:
            session.close()
        except Exception as exc:
            logger.warning("Не удалось корректно закрыть HTTP-сессию: %s", exc)


class YandexDiskUploader:
    def __init__(self, token: str | None):
        self.token = token
        self.client = yadisk.YaDisk(token=token) if token else None
        if self.client:
            self._check_token()
        else:
            logger.warning("YANDEX_TOKEN не задан — загрузка PDF в Яндекс.Диск отключена")

    def _check_token(self) -> None:
        if not self.client:
            return
        if not self.client.check_token():
            raise RuntimeError("Недействительный токен Яндекс.Диска. Проверьте YANDEX_TOKEN.")

    def upload_pdf(self, pdf_content: bytes, filename: str) -> str:
        if not self.client:
            return ""

        file_path = f"/Visa/{filename}"
        try:
            if self.client.exists(file_path):
                meta = self.client.get_meta(file_path, fields=["public_url"])
                if meta.public_url:
                    return meta.public_url
        except yadisk.exceptions.PathNotFoundError:
            logger.info("Файл %s отсутствует на Яндекс.Диске, загружаем", file_path)
        except Exception as exc:
            logger.warning("Ошибка проверки файла %s на Яндекс.Диске: %s", file_path, exc)

        try:
            try:
                self.client.mkdir("/Visa")
            except yadisk.exceptions.PathExistsError:
                pass

            self.client.upload(BytesIO(pdf_content), file_path, overwrite=True)
            self.client.publish(file_path)
            meta = self.client.get_meta(file_path, fields=["public_url"])
            return meta.public_url or ""
        except Exception as exc:
            logger.error("Не удалось загрузить PDF %s на Яндекс.Диск: %s", filename, exc)
            return ""


class PDFManager:
    def __init__(self, session_manager: SessionManager, yandex_uploader: YandexDiskUploader):
        self.session_manager = session_manager
        self.yandex_uploader = yandex_uploader
        self.temp_dir = settings.temp_dir

    def download_pdf(self, session: requests.Session, session_id: str, pdf_url: str) -> Optional[bytes]:
        cookies = {"PHPSESSID": session_id}
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://evisa.imigrasi.go.id/",
        }
        try:
            response = session.get(pdf_url, cookies=cookies, headers=headers)
            if response.status_code == 200 and "application/pdf" in response.headers.get("Content-Type", ""):
                return response.content
            logger.error(
                "Ошибка загрузки PDF %s: status=%s content-type=%s",
                pdf_url,
                response.status_code,
                response.headers.get("Content-Type"),
            )
        except Exception as exc:
            logger.error("Ошибка при загрузке PDF %s: %s", pdf_url, exc)
        return None

    def _get_or_cache_pdf(
        self,
        local_name: str,
        session: requests.Session,
        session_id: str,
        pdf_url: str,
    ) -> Optional[bytes]:
        path = self.temp_dir / local_name
        if path.exists():
            return path.read_bytes()

        pdf_content = self.download_pdf(session, session_id, pdf_url)
        if pdf_content:
            path.write_bytes(pdf_content)
        return pdf_content

    def upload_batch_pdf(
        self,
        session: requests.Session,
        session_id: str,
        action_link_original: str,
        reg_number: str,
        full_name: str,
    ) -> str:
        if not action_link_original:
            return ""
        file_name = f"{reg_number}_batch_application.pdf"
        pdf_content = self._get_or_cache_pdf(file_name, session, session_id, action_link_original)
        if not pdf_content:
            logger.warning("Не удалось подготовить PDF Batch для %s (%s)", full_name, reg_number)
            return ""
        return self.yandex_uploader.upload_pdf(pdf_content, file_name)

    def upload_stay_pdf(
        self,
        session: requests.Session,
        session_id: str,
        pdf_relative_url: str,
        reg_number: str,
    ) -> str:
        if not pdf_relative_url:
            return ""
        pdf_url = pdf_relative_url
        if pdf_relative_url.startswith("/"):
            pdf_url = f"https://evisa.imigrasi.go.id{pdf_relative_url}"

        file_name = f"{reg_number}_stay_permit.pdf"
        pdf_content = self._get_or_cache_pdf(file_name, session, session_id, pdf_url)
        if not pdf_content:
            logger.warning("Не удалось подготовить PDF Stay Permit для %s", reg_number)
            return ""
        return self.yandex_uploader.upload_pdf(pdf_content, file_name)
