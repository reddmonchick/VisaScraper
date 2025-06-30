FROM python:3.12-slim

# Установка Poetry
RUN pip install poetry

# Рабочая директория внутри контейнера
WORKDIR /app

# Копируем pyproject.toml и poetry.lock
COPY pyproject.toml .
COPY poetry.lock .

# Устанавливаем зависимости
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --no-interaction --no-ansi

# Копируем исходный код
COPY src/ ./src/

# Запускаем приложение
CMD ["poetry", "run", "python", "src/visascraper/main.py"]