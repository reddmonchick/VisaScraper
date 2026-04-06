from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import sessionmaker

from visascraper.config import ensure_runtime_dirs, settings
from visascraper.database.models import Base

ensure_runtime_dirs()
DATABASE_URL = f"sqlite:///{settings.database_path.as_posix()}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def _table_columns(conn: Connection, table_name: str) -> set[str]:
    rows = conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def _ensure_column(conn: Connection, table_name: str, column_name: str, definition: str) -> None:
    if column_name in _table_columns(conn, table_name):
        return
    conn.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _migrate_users(conn: Connection) -> None:
    _ensure_column(conn, "users", "is_authorized", "INTEGER NOT NULL DEFAULT 0")
    if settings.telegram_bot_password:
        conn.execute(
            text(
                """
                UPDATE users
                SET is_authorized = 1
                WHERE password = :password
                """
            ),
            {"password": settings.telegram_bot_password},
        )


def _create_runtime_indexes(conn: Connection) -> None:
    statements = (
        "CREATE INDEX IF NOT EXISTS ix_batch_applications_register_number ON batch_applications (register_number)",
        "CREATE INDEX IF NOT EXISTS ix_batch_applications_passport_number ON batch_applications (passport_number)",
        "CREATE INDEX IF NOT EXISTS ix_batch_applications_full_name ON batch_applications (full_name)",
        "CREATE INDEX IF NOT EXISTS ix_batch_applications_account ON batch_applications (account)",
        "CREATE INDEX IF NOT EXISTS ix_stay_permits_passport_number ON stay_permits (passport_number)",
        "CREATE INDEX IF NOT EXISTS ix_stay_permits_account ON stay_permits (account)",
    )
    for statement in statements:
        conn.exec_driver_sql(statement)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        _migrate_users(conn)
        _create_runtime_indexes(conn)
