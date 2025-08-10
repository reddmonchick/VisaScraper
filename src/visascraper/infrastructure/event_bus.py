import logging
from collections import defaultdict
from typing import Callable, Any, DefaultDict, List

logger = logging.getLogger(__name__)

class EventBus:
    """
    Простая реализация шины событий для имитации архитектуры,
    управляемой событиями (например, NATS).
    Это позволяет различным компонентам системы (например, боту и парсеру)
    общаться друг с другом, не имея прямых зависимостей.
    """
    def __init__(self):
        # Словарь для хранения подписчиков.
        # Ключ - название события (строка).
        # Значение - список функций-обработчиков (коллбэков).
        self.subscribers: DefaultDict[str, List[Callable]] = defaultdict(list)

    def subscribe(self, event_type: str, callback: Callable):
        """
        Подписывает функцию-обработчик на определенный тип события.
        """
        self.subscribers[event_type].append(callback)
        logger.info(f"Новый подписчик для события '{event_type}': {callback.__name__}")

    def publish(self, event_type: str, *args: Any, **kwargs: Any):
        """
        Публикует событие.
        Все подписчики этого события будут вызваны с переданными аргументами.
        """
        if event_type not in self.subscribers:
            logger.warning(f"Публикация события '{event_type}', на которое нет подписчиков.")
            return

        logger.info(f"Публикация события '{event_type}' с аргументами: args={args}, kwargs={kwargs}")
        for callback in self.subscribers[event_type]:
            try:
                # В реальной системе это было бы асинхронно,
                # но для имитации достаточно простого вызова.
                callback(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Ошибка при вызове обработчика {callback.__name__} для события '{event_type}': {e}",
                    exc_info=True
                )
