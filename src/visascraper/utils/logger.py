import logging
import os

# Создаем папку logs, если её нет
os.makedirs("logs", exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s] %(message)s',
    handlers=[
        logging.StreamHandler(),  # Логи в консоль
        logging.FileHandler("logs/app.log", encoding='utf-8')  # Логи в файл
    ]
)

# Получаем логгер
logger = logging.getLogger(__name__)

# Отключаем подробные логи от библиотек
logging.getLogger("google.auth.transport").setLevel(logging.WARNING)
logging.getLogger("google.auth.client").setLevel(logging.WARNING)
logging.getLogger("oauth2client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)