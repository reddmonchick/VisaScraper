
from database.db import init_db, SessionLocal
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_
from utils.logger import logger as custom_logger
from .models import User, BatchApplication, StayPermit
from typing import Optional, List
import re
import asyncio
from aiogram.types import FSInputFile
import os
from bot.notification import send_telegram_message 



def save_or_update_batch_data(db: Session, data_list: list):
    """Сохраняет новые заявления BatchApplication или обновляет существующие."""
    for item_data in data_list:
        reg_number = item_data.get("register_number")
        if not reg_number:
            continue

        existing_app = db.query(BatchApplication).filter(BatchApplication.register_number == reg_number).first()
        if existing_app:
            new_status = item_data.get("status")
            if existing_app.status != new_status:
                existing_app.last_status = existing_app.status
                existing_app.status = new_status

            for key, value in item_data.items():
                if hasattr(existing_app, key):
                    setattr(existing_app, key, value)
        else:
            new_app = BatchApplication(**item_data)
            new_app.last_status = item_data.get("status")
            db.add(new_app)
    db.commit()


def save_or_update_stay_permit_data(db: Session, data_list: List[dict]):
    """
    Исправленный upsert:
    1. Убирает дубликаты из входящего списка.
    2. Загружает существующие записи пачкой (оптимизация).
    3. Обновляет или вставляет без конфликтов.
    """
    if not data_list:
        return

    # 1. Убираем дубликаты внутри самого списка data_list.
    # Используем словарь: если reg_number повторяется, останется последний (самый свежий).
    unique_data_map = {
        item['reg_number']: item 
        for item in data_list 
        if item.get('reg_number')
    }

    if not unique_data_map:
        return

    # 2. Получаем список всех reg_number, которые мы хотим обработать
    reg_numbers = list(unique_data_map.keys())

    # 3. ОПТИМИЗАЦИЯ: Загружаем все существующие записи одним запросом
    # вместо того, чтобы делать запрос в цикле для каждой записи.
    existing_records = db.query(StayPermit).filter(StayPermit.reg_number.in_(reg_numbers)).all()
    
    # Создаем словарь существующих записей для быстрого поиска {reg_number: объект}
    existing_dict = {rec.reg_number: rec for rec in existing_records}

    for reg_number, data in unique_data_map.items():
        existing = existing_dict.get(reg_number)

        if existing:
            # Обновляем существующую запись
            for key, value in data.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
            custom_logger.info(f"Обновлена запись Stay Permit: {reg_number}")
        else:
            # Создаем новую запись
            new_record = StayPermit(**data)
            db.add(new_record)
            custom_logger.info(f"Добавлена новая запись Stay Permit: {reg_number}")

    # 4. Коммит с обработкой ошибок
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        custom_logger.error(f"Ошибка при сохранении Stay Permits: {e}")
        # Можно повторно выбросить ошибку, если нужно прервать выполнение
        # raise e

async def save_or_update_stay_permit_data_async(data_list: List[dict]):
    if not data_list:
        return

    # 1. Сохраняем в БД (оборачиваем создание сессии внутрь функции или используем контекстный менеджер)
    # Лучше создать обертку, которая сама создаст и закроет сессию
    def _sync_wrapper(data):
        with SessionLocal() as db:
            save_or_update_stay_permit_data(db, data)
    
    try:
        await asyncio.to_thread(_sync_wrapper, data_list)
    except Exception as e:
        custom_logger.error(f"Критическая ошибка при сохранении в БД: {e}")
        return  # Прерываем выполнение, если сохранение не удалось

    # 2. Отправка уведомлений (логика осталась прежней, но сессия в `with`)
    try:
        # Используем with для автоматического закрытия сессии
        with SessionLocal() as db:
            for item in data_list:
                reg_number = item.get("reg_number")
                if not reg_number:
                    continue

                permit = db.query(StayPermit).filter(StayPermit.reg_number == reg_number).first()
                
                # Проверяем, что пермит существует и не был уведомлен
                if not permit or getattr(permit, "notified_as_new", False):
                    continue

                file_path = f"src/temp/{reg_number}_stay_permit.pdf"
                document = FSInputFile(file_path) if os.path.exists(file_path) else None

                text = (
                    f"Новый ITK добавлен в систему!\n\n"
                    f"ФИО: {item.get('name', '—')}\n"
                    f"Паспорт: {item.get('passport_number', '—')}\n"
                    f"Тип разрешения: {item.get('type_of_staypermit', '—')}\n"
                    f"Дата выдачи: {item.get('issue_date', '—')}\n"
                    f"Действует до: {item.get('expired_date', '—')}\n"
                    f"Рег. номер: {reg_number}\n"
                    f"Статус: {item.get('status', 'не указан')}"
                )

                # Отправляем в фоне
                asyncio.create_task(send_telegram_message(text, document=document))

                # Помечаем как отправленное
                permit.notified_as_new = True
                
                # Коммитим флаг уведомления сразу или пачками - здесь пачкой надежнее внутри цикла, 
                # но можно и один раз в конце, если уверены в надежности
            
            db.commit()
            # custom_logger.info(...) можно добавить тут

    except Exception as e:
        custom_logger.error(f"Ошибка при отправке уведомлений о новых ITK: {e}")



def init_db(engine):
    from .models import Base
    Base.metadata.create_all(bind=engine)


def clear_old_users_if_password_changed(db: Session, current_password: str):
    users = db.query(User).all()
    for user in users:
        if user.password != current_password:
            db.delete(user)
    db.commit()


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
    custom_logger.info(f"Входной запрос для поиска по паспорту: '{search_input}'")
    passport_match = re.search(r'\b(?=\w*\d)[A-Z0-9-]{5,}\b', search_input)
    passport_number = passport_match.group(0) if passport_match else None
    custom_logger.info(f"Извлеченный номер паспорта: '{passport_number}'")

    if passport_number:
        name_part = re.sub(r'\b' + re.escape(passport_number) + r'\b\s*', '', search_input).strip()
    else:
        name_part = search_input

    name_parts = list(set(filter(None, re.split(r'\s+', name_part.upper()))))
    custom_logger.info(f"Части имени: {name_parts}")

    query = db.query(BatchApplication)
    if name_parts and passport_number:
        name_filters = [BatchApplication.full_name.ilike(f"%{part}%") for part in name_parts]
        query = query.filter(or_(*name_filters))
        query = query.filter(or_(
            BatchApplication.passport_number == passport_number,
            BatchApplication.passport_number.ilike(f"%{passport_number}%")
        ))
    results = query.all()
    custom_logger.info(f"Найдено записей BatchApplication: {len(results)}")
    return results


def search_by_stay_permit(db: Session, passport_number_input: str):
    passport_number = passport_number_input.strip().upper()
    custom_logger.info(f"Поиск StayPermit по паспорту: '{passport_number}'")
    if not passport_number:
        return []
    results = db.query(StayPermit).filter(StayPermit.passport_number == passport_number).all()
    custom_logger.info(f"Найдено StayPermit: {len(results)}")
    return results