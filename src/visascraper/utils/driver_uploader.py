from __future__ import annotations

import os
from pathlib import Path

import requests
import yadisk

from visascraper.config import settings
from visascraper.utils.logger import logger


def upload_driver(driver_path: str | os.PathLike[str], remote_path: str = "/driver/chromedriver") -> str | None:
    if not settings.yandex_token:
        logger.warning("YANDEX_TOKEN не задан — upload_driver пропущен")
        return None

    client = yadisk.YaDisk(token=settings.yandex_token)
    if not client.check_token():
        raise RuntimeError("Недействительный токен Яндекс.Диска")

    local_path = Path(driver_path)
    if not local_path.exists():
        raise FileNotFoundError(f"Файл драйвера не найден: {local_path}")

    try:
        client.upload(str(local_path), remote_path, overwrite=True)
        client.publish(remote_path)
        meta = client.get_meta(remote_path, fields=["public_url"])
        logger.info("Драйвер %s загружен на Яндекс.Диск", local_path)
        return meta.public_url
    except Exception as exc:
        logger.error("Не удалось загрузить драйвер %s: %s", local_path, exc)
        return None


def download_file(url: str, output_path: str | os.PathLike[str]) -> None:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    Path(output_path).write_bytes(response.content)
    logger.info("Файл скачан: %s", output_path)
