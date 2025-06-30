# database/db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base

# Подключение к SQLite
DATABASE_URL = "sqlite:///./visadata.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)