import os
import threading
from utils.logger import logger as custom_logger
from infrastructure.event_bus import EventBus

# Import all the service components
from .data_parser import DataParser
from .google_sheets_manager import GoogleSheetsManager
from .pdf_manager import PDFManager
from .scheduler import JobScheduler
from .session_manager import SessionManager
from .yandex_uploader import YandexDiskUploader

class ParserService:
    """
    Инкапсулирует всю логику, связанную с парсингом.
    Работает как отдельный сервис, который общается с остальной частью
    приложения через шину событий (EventBus).
    """
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._initialize_components()
        self.job_scheduler = JobScheduler(self.gs_manager, self.data_parser)
        self.is_manual_parsing_running = False

    def _initialize_components(self):
        """Инициализирует и связывает все компоненты парсера."""
        custom_logger.info("Инициализация компонентов сервиса парсинга...")
        # Получаем токены и пути из переменных окружения
        yandex_token = os.getenv('YANDEX_TOKEN')
        gs_credentials_path = os.getenv("GOOGLE_CREDENTIALS_JSON_PATH")

        # Создаем экземпляры
        self.session_manager = SessionManager()
        self.yandex_uploader = YandexDiskUploader(token=yandex_token)
        self.pdf_manager = PDFManager(self.session_manager, self.yandex_uploader)
        self.data_parser = DataParser(self.session_manager, self.pdf_manager)
        self.gs_manager = GoogleSheetsManager(credentials_path=gs_credentials_path)
        custom_logger.info("Компоненты сервиса парсинга успешно инициализированы.")

    def setup_subscriptions(self):
        """Подписывает обработчики сервиса на события из шины."""
        self.event_bus.subscribe('parsing:start_others', self.handle_start_parsing_others)
        custom_logger.info("Сервис парсинга подписался на события.")

    def handle_start_parsing_others(self, user_id: str):
        """
        Обработчик события для запуска парсинга "остальных" аккаунтов.
        """
        if self.is_manual_parsing_running:
            custom_logger.warning(f"Попытка запустить парсинг, когда он уже выполняется. Запрошено пользователем {user_id}.")
            self.event_bus.publish('parsing:already_running', user_id=user_id)
            return

        custom_logger.info(f"Получено событие 'parsing:start_others' от пользователя {user_id}. Запускаем парсинг в отдельном потоке.")
        self.is_manual_parsing_running = True

        # Запускаем тяжелую задачу в отдельном потоке, чтобы не блокировать
        parser_thread = threading.Thread(target=self._run_parsing_job, args=(user_id,))
        parser_thread.start()

    def _run_parsing_job(self, user_id: str):
        """Выполняет саму задачу парсинга и публикует результат."""
        try:
            self.job_scheduler.job_others()
            custom_logger.info(f"Парсинг, запущенный пользователем {user_id}, успешно завершен.")
            self.event_bus.publish('parsing:finished', user_id=user_id, success=True)
        except Exception as e:
            custom_logger.error(f"Ошибка в процессе парсинга, запущенного пользователем {user_id}: {e}", exc_info=True)
            self.event_bus.publish('parsing:finished', user_id=user_id, success=False, error_message=str(e))
        finally:
            self.is_manual_parsing_running = False

    def run_scheduled_jobs(self):
        """Запускает фоновый планировщик для регулярных задач (например, job_first_two)."""
        custom_logger.info("Запуск фонового планировщика для регулярных задач...")
        self.job_scheduler.start_scheduler()

    def run_initial_parsing(self):
        """Выполняет один цикл парсинга при старте."""
        custom_logger.info("Запуск начального цикла парсинга...")
        try:
            self.job_scheduler.job_first_two()
            self.job_scheduler.job_others()
        except Exception as e:
            custom_logger.error(f"Ошибка во время начального парсинга: {e}", exc_info=True)
            # В случае ошибки при инициализации, приложение может не захотеть продолжать работу
            # Здесь можно добавить логику для корректного завершения работы
            raise
