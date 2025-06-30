FROM python:3.12-alpine

WORKDIR /app

COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-interaction --no-ansi

COPY src/ ./src/

CMD ["poetry", "run", "python", "src/visascraper/main.py"]