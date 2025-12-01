# database/db.py — УНИВЕРСАЛЬНЫЙ ВАРИАНТ 2025 ГОДА
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base
import os

# УМНЫЙ ПУТЬ — работает и в Docker, и на Windows, и на Linux
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # корень проекта
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "visascraper.db")

# Создаём папку data, если её нет
os.makedirs(DATA_DIR, exist_ok=True)

# Формируем URL — с правильными слешами для Windows
DATABASE_URL = f"sqlite:///{DB_PATH.replace(os.sep, '/')}"

# Создаём движок
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # нужно для SQLite
    echo=False  # включи echo=True если хочешь видеть SQL-запросы
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Создаёт таблицы при первом запуске"""
    Base.metadata.create_all(bind=engine)