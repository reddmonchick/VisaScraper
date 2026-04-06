from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
PACKAGE_ROOT = Path(__file__).resolve().parent


@dataclass(slots=True, frozen=True)
class Settings:
    proxy: str | None
    yandex_token: str | None
    telegram_bot_token: str | None
    telegram_bot_password: str | None
    telegram_channel_id: str | None
    admin_user_ids: tuple[str, ...]
    google_accounts_sheet_id: str | None
    google_archive_index_id: str | None
    google_template_sheet_id: str | None
    google_drive_folder_id: str | None
    google_service_account_file: Path
    google_service_account_json: str | None
    batch_parse_interval_minutes: int
    app_timezone: str
    temp_dir: Path
    logs_dir: Path
    database_path: Path
    session_store_path: Path


settings = Settings(
    proxy=os.getenv("PROXY") or None,
    yandex_token=os.getenv("YANDEX_TOKEN") or None,
    telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN") or None,
    telegram_bot_password=os.getenv("TELEGRAM_BOT_PASSWORD") or None,
    telegram_channel_id=os.getenv("TELEGRAM_CHANNEL_ID") or None,
    admin_user_ids=tuple(uid.strip() for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()),
    google_accounts_sheet_id=os.getenv("GOOGLE_ACCOUNTS_SHEET_ID") or None,
    google_archive_index_id=os.getenv("GOOGLE_ARCHIVE_INDEX_ID") or None,
    google_template_sheet_id=os.getenv("GOOGLE_TEMPLATE_SHEET_ID") or None,
    google_drive_folder_id=os.getenv("GOOGLE_DRIVE_FOLDER_ID") or None,
    google_service_account_file=Path(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(SRC_ROOT / "service_account.json"))
    ),
    google_service_account_json=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or None,
    batch_parse_interval_minutes=int(os.getenv("BATCH_PARSE_INTERVAL_MINUTES", "10")),
    app_timezone=os.getenv("APP_TIMEZONE", "Europe/Moscow"),
    temp_dir=PACKAGE_ROOT / "temp",
    logs_dir=PROJECT_ROOT / "logs",
    database_path=PACKAGE_ROOT / "data" / "visascraper.db",
    session_store_path=SRC_ROOT / "data.json",
)


def ensure_runtime_dirs() -> None:
    settings.temp_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    settings.session_store_path.parent.mkdir(parents=True, exist_ok=True)
