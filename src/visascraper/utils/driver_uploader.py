import yadisk
import os
from typing import Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from utils.logger import logger as logging
import time

load_dotenv()

# Настройка логирования

def upload_to_yandex_disk(pdf_content: bytes, filename: str) -> str:
    """
    Загружает PDF-файл в папку /Visa на Яндекс.Диске и возвращает публичную ссылку.
    
    Args:
        pdf_content: Байтовое содержимое PDF-файла.
        filename: Имя файла.
    
    Returns:
        str: Публичная ссылка на загруженный файл.
    """
    # Вставьте ваш токен (получите на https://oauth.yandex.ru/authorize?response_type=token&client_id=ВАШ_CLIENT_ID)
    YANDEX_TOKEN = os.getenv('YANDEX_TOKEN')

    # Инициализация клиента Яндекс.Диска
    try:
        y = yadisk.YaDisk(token=YANDEX_TOKEN)
    except Exception as e:
        logging.error(f"Ошибка инициализации YaDisk: {str(e)}")
        raise Exception(f"Не удалось подключиться к Яндекс.Диску: {str(e)}")

    # Проверка валидности токена
    try:
        if not y.check_token():
            logging.error("Недействительный токен Яндекс.Диска")
            raise Exception("Недействительный токен. Получите новый на https://oauth.yandex.ru/authorize?response_type=token&client_id=ВАШ_CLIENT_ID")
    except yadisk.exceptions.UnauthorizedError as e:
        logging.error(f"Ошибка авторизации: {str(e)}")
        raise Exception("Ошибка авторизации. Проверьте YANDEX_TOKEN или получите новый на https://oauth.yandex.ru")

    # Путь к файлу в папке /Visa
    file_path = f"/Visa/{filename}"
    logging.info(f"Проверка файла: {file_path}")

    # Проверка существования файла
    try:
        if y.exists(file_path):
            meta = y.get_meta(file_path, fields=["public_url"])
            public_url = meta.public_url
            if public_url:
                logging.info(f"Файл уже существует, возвращаем публичную ссылку: {public_url}")
                return public_url
    except yadisk.exceptions.PathNotFoundError:
        logging.info(f"Файл {file_path} не существует, приступаем к загрузке")
    except Exception as e:
        logging.error(f"Ошибка при проверке файла {file_path}: {str(e)}")
        raise Exception(f"Ошибка при проверке файла: {str(e)}")

    # Сохраняем PDF временно
    local_file_path = f"src/temp/{filename}"

    try:

        # Создание папки /Visa, если не существует
        try:
            y.mkdir("/Visa")
            logging.info("Папка /Visa создана")
        except yadisk.exceptions.PathExistsError:
            logging.info("Папка /Visa уже существует")

        # Загрузка файла
        logging.info(f"Загрузка файла на Яндекс.Диск: {file_path}")
        y.upload(local_file_path, file_path, overwrite=True)

        # Публикация файла
        logging.info(f"Публикация файла: {file_path}")
        y.publish(file_path)

        # Получение публичной ссылки
        meta = y.get_meta(file_path, fields=["public_url"])
        public_url = meta.public_url
        if not public_url:
            logging.error("Не удалось получить публичную ссылку")
            raise Exception("Не удалось получить публичную ссылку после публикации")
        logging.info(f"Файл успешно загружен, публичная ссылка: {public_url}")
        return public_url

    except yadisk.exceptions.YaDiskError as e:
        logging.error(f"Ошибка Яндекс.Диска: {str(e)}")
        raise Exception(f"Ошибка Яндекс.Диска: {str(e)}")
    except Exception as e:
        logging.error(f"Общая ошибка: {str(e)}")
        raise Exception(f"Ошибка при загрузке файла: {str(e)}")
    finally:
        pass