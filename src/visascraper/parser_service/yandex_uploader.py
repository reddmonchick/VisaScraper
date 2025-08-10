import yadisk
from io import BytesIO
from utils.logger import logger as custom_logger

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
            # custom_logger.info(f"Файл {file_path} не существует, приступаем к загрузке")
            pass
        except Exception as e:
            custom_logger.error(f"Ошибка при проверке файла {file_path}: {e}")

        try:
            # Создание папки /Visa, если не существует
            try:
                self.ya_disk_client.mkdir("/Visa")
                # custom_logger.info("Папка /Visa создана")
            except yadisk.exceptions.PathExistsError:
                pass

            # Загрузка файла напрямую из байтов
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
            # custom_logger.info(f"Файл успешно загружен, публичная ссылка: {public_url}")
            return public_url

        except yadisk.exceptions.YaDiskError as e:
            custom_logger.error(f"Ошибка Яндекс.Диска: {e}")
        except Exception as e:
            custom_logger.error(f"Общая ошибка при загрузке файла: {e}")
        return ''
