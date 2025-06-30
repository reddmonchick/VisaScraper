
FROM python:3.12-slim

WORKDIR /app


COPY pyproject.toml .


RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false


RUN poetry install --no-root --no-interaction --no-ansi

COPY src/ src/

CMD ["poetry", "run", "python", "src/visascraper/main.py"]