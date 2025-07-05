from sqlalchemy.orm import Session
from sqlalchemy import or_
from utils.logger import logger
from .models import User, BatchApplication, StayPermit
from typing import Optional
import re

def init_db(engine):
    Base.metadata.create_all(bind=engine)

def clear_old_users_if_password_changed(db: Session, current_password: str):
    users = db.query(User).all()
    for user in users:
        if user.password != current_password:
            db.delete(user)
    db.commit()

def get_db():
    from database.db import SessionLocal
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def save_batch_data(db: Session, data_list: list):
    for item in data_list:
        batch_no = item.get("batch_no")
        if not is_batch_exists(db, batch_no):
            db.execute(BatchApplication.__table__.insert(), [item])
    db.commit()


def save_stay_permit_data(db: Session, data_list: list):
    for item in data_list:
        stay_id = item.get("reg_number")
        if not is_stay_permit_exists(db, stay_id):
            db.execute(StayPermit.__table__.insert(), [item])
    db.commit()


def is_batch_exists(db: Session, batch_no: str) -> bool:
    return db.query(BatchApplication).filter(BatchApplication.batch_no == batch_no).first() is not None


def is_stay_permit_exists(db: Session, stay_id: str) -> bool:
    return db.query(StayPermit).filter(StayPermit.reg_number == stay_id).first() is not None

def get_user_by_telegram_id(db: Session, telegram_id: str):
    return db.query(User).filter(User.telegram_id == telegram_id).first()

def create_user(db: Session, telegram_id: str, account_name: str, password: str):
    existing = db.query(User).filter(User.telegram_id == telegram_id).first()
    if existing:
        existing.account_name = account_name
        existing.password = password
        db.commit()
        return existing

    new_user = User(telegram_id=telegram_id, account_name=account_name, password=password)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

def delete_user(db: Session):
    db.query(User).delete()
    db.commit()


def search_by_passport(db: Session, search_input: str):
    search_input = search_input.strip()
    logger.info(f"Входной запрос для поиска по паспорту: '{search_input}'")

    # Ищем ПЕРВУЮ последовательность, содержащую хотя бы одну цифру — скорее всего это номер паспорта
    passport_match = re.search(r'\b(?=\w*\d)[A-Z0-9-]{5,}\b', search_input)

    passport_number = passport_match.group(0) if passport_match else None
    logger.info(f"Извлеченный номер паспорта: '{passport_number}'")

    # Убираем номер паспорта из строки, чтобы осталось только имя
    if passport_number:
        name_part = re.sub(r'\b' + re.escape(passport_number) + r'\b\s*', '', search_input).strip()
    else:
        name_part = search_input
    logger.info(f"Извлеченная часть имени: '{name_part}'")

    # Разделяем имя на части
    name_parts = list(set(filter(None, re.split(r'\s+', name_part.upper()))))
    logger.info(f"Части имени: {name_parts}")

    # Формируем SQL-запрос
    query = db.query(BatchApplication)

    # Фильтруем по имени, если есть части имени
    if name_parts:
        name_filters = [BatchApplication.full_name.ilike(f"%{part}%") for part in name_parts]
        query = query.filter(or_(*name_filters))
    
    # Фильтруем по номеру паспорта, если он есть
    if passport_number:
        query = query.filter(or_(
            BatchApplication.passport_number == passport_number,
            BatchApplication.passport_number.ilike(f"%{passport_number}%")
        ))

    results = query.all()
    logger.info(f"Найдено записей: {len(results)}")
    for result in results:
        logger.info(f"Результат: {result.__dict__}")
    
    return results

def search_by_stay_permit(db: Session, search_input: str):
    search_input = search_input.strip()
    logger.info(f"Входной запрос для поиска по месту жительства: '{search_input}'")

    # Ищем ПЕРВУЮ последовательность, содержащую хотя бы одну цифру — скорее всего это номер паспорта
    passport_match = re.search(r'\b(?=\w*\d)[A-Z0-9-]{5,}\b', search_input)

    passport_number = passport_match.group(0) if passport_match else None
    logger.info(f"Извлеченный номер паспорта: '{passport_number}'")

    # Убираем номер паспорта из строки, чтобы осталось только имя
    if passport_number:
        name_part = re.sub(r'\b' + re.escape(passport_number) + r'\b\s*', '', search_input).strip()
    else:
        name_part = search_input
    logger.info(f"Извлеченная часть имени: '{name_part}'")

    # Разделяем имя на части
    name_parts = list(set(filter(None, re.split(r'\s+', name_part.upper()))))
    logger.info(f"Части имени: {name_parts}")

    # Формируем SQL-запрос
    query = db.query(StayPermit)

    # Фильтруем по имени, если есть части имени
    if name_parts:
        name_filters = [StayPermit.name.ilike(f"%{part}%") for part in name_parts]
        query = query.filter(or_(*name_filters))
    
    # Фильтруем по номеру паспорта, если он есть
    if passport_number:
        query = query.filter(or_(
            StayPermit.passport_number == passport_number,
            StayPermit.passport_number.ilike(f"%{passport_number}%")
        ))

    results = query.all()
    logger.info(f"Найдено записей: {len(results)}")
    for result in results:
        logger.info(f"Результат: {result.__dict__}")
    
    return results