FROM  duffn/python-poetry:3.9.18-slim-1.6.1-2023-10-06

WORKDIR /app

COPY pyproject.toml .
COPY poetry.lock .
 

COPY src/ ./src/

CMD ["poetry", "run", "python", "src/visascraper/main.py"]