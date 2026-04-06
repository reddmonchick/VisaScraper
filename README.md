# VisaScraper

Сервис для парсинга статусов виз и Stay Permit, сохранения данных в SQLite/Google Sheets и отправки уведомлений в Telegram.

## Что изменено

- `main.py` стал тонкой точкой входа, основная логика вынесена по модулям.
- Google Sheets переведён на **сервисный аккаунт**, без еженедельной ручной переавторизации.
- Исправлены импорты пакета, чтобы проект нормально запускался через Poetry и Docker.
- Добавлены отдельные слои: `services`, `jobs`, `bot`, `database`, `utils`.
- Исправлены явные баги: отсутствие `job_others`, неверные пути к файлам, поиск по паспорту, логирование и ряд дублирующихся кусков кода.

## Структура проекта

```text
.
├── logs/
├── src/
│   ├── service_account.json        # сервисный аккаунт Google (локально, не коммитить)
│   └── visascraper/
│       ├── app.py                  # сборка приложения
│       ├── config.py               # настройки и пути
│       ├── dto.py                  # dataclass-модели и заголовки таблиц
│       ├── jobs.py                 # расписание парсинга аккаунтов
│       ├── main.py                 # entrypoint
│       ├── services/
│       │   ├── scraper.py          # парсинг batch/stay permit
│       │   ├── sheets.py           # работа с Google Sheets
│       │   └── storage.py          # HTTP-сессии, PDF, Yandex Disk
│       ├── bot/
│       ├── database/
│       ├── utils/
│       ├── data/
│       └── temp/
├── pyproject.toml
└── docker-compose.yml
```

## Установка

```bash
poetry install
```

## Настройка Google Sheets через сервисный аккаунт

1. Создайте сервисный аккаунт в Google Cloud.
2. Скачайте JSON-ключ и сохраните его локально, например в `src/service_account.json`.
3. Дайте email сервисного аккаунта доступ:
   - к таблице с аккаунтами;
   - к таблице-оглавлению архива;
   - к папке Google Drive, где создаются новые таблицы.
4. Укажите путь в `.env` через `GOOGLE_SERVICE_ACCOUNT_FILE`.

Поддерживается и второй вариант: передать JSON целиком через `GOOGLE_SERVICE_ACCOUNT_JSON`.

## Основные переменные окружения

Смотрите `.env.example`.

Ключевые:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_BOT_PASSWORD`
- `TELEGRAM_CHANNEL_ID`
- `ADMIN_USER_IDS`
- `YANDEX_TOKEN`
- `PROXY`
- `GOOGLE_ACCOUNTS_SHEET_ID`
- `GOOGLE_ARCHIVE_INDEX_ID`
- `GOOGLE_DRIVE_FOLDER_ID`
- `GOOGLE_TEMPLATE_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_FILE`
- `BATCH_PARSE_INTERVAL_MINUTES`
- `APP_TIMEZONE`

## Запуск

```bash
poetry run run
```

или

```bash
python -m visascraper.main
```

## Docker

```bash
docker compose up --build -d
```

## Примечание

Старые файлы OAuth (`token.json`, `credentials_oauth.json`) больше не нужны для работы Google Sheets, если проект полностью переведён на сервисный аккаунт.
