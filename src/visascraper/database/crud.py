from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy import or_
from sqlalchemy.orm import Session

from visascraper.bot.notification import send_telegram_message
from visascraper.config import settings
from visascraper.database.db import SessionLocal
from visascraper.database.models import BatchApplication, StayPermit, User
from visascraper.utils.logger import logger


def _batch_pdf_path(register_number: str) -> Path:
    return settings.temp_dir / f"{register_number}_batch_application.pdf"


def _stay_pdf_path(reg_number: str) -> Path:
    return settings.temp_dir / f"{reg_number}_stay_permit.pdf"


async def notify_new_batch_applications(data_list: list[dict]) -> None:
    try:
        outgoing_messages: list[tuple[str, Path | None]] = []
        with SessionLocal() as db:
            for item in data_list:
                reg_number = item.get("register_number")
                if not reg_number:
                    continue

                app = db.query(BatchApplication).filter(BatchApplication.register_number == reg_number).first()
                if not app or app.notified_as_new:
                    continue

                text = (
                    "Новое заявление Batch Application!\n\n"
                    f"ФИО: {item.get('full_name', '—')}\n"
                    f"Паспорт: {item.get('passport_number', '—')}\n"
                    f"Рег. номер: {reg_number}\n"
                    f"Статус: {item.get('status', 'не указан')}"
                )
                file_path = _batch_pdf_path(reg_number)
                outgoing_messages.append((text, file_path if file_path.exists() else None))
                app.notified_as_new = True

            db.commit()
            logger.info("Уведомления по новым Batch Application подготовлены: %s", len(outgoing_messages))

        for text, document_path in outgoing_messages:
            await send_telegram_message(text, document_path=document_path)
    except Exception as exc:
        logger.error("Ошибка отправки уведомлений Batch Application: %s", exc)


def save_or_update_batch_data(db: Session, data_list: list[dict]) -> None:
    if not data_list:
        return

    unique_map = {item["register_number"]: item for item in data_list if item.get("register_number")}
    existing_records = db.query(BatchApplication).filter(BatchApplication.register_number.in_(unique_map.keys())).all()
    existing_map = {item.register_number: item for item in existing_records}

    for reg_number, payload in unique_map.items():
        existing = existing_map.get(reg_number)
        if existing:
            new_status = payload.get("status")
            if existing.status != new_status:
                existing.last_status = existing.status
                existing.status = new_status
            for key, value in payload.items():
                if key in {"id", "notified_as_new", "last_status"}:
                    continue
                if hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            record = BatchApplication(**payload)
            record.last_status = payload.get("status")
            db.add(record)

    db.commit()


def save_or_update_stay_permit_data(db: Session, data_list: list[dict]) -> None:
    if not data_list:
        return

    unique_map = {item["reg_number"]: item for item in data_list if item.get("reg_number")}
    if not unique_map:
        return

    existing_records = db.query(StayPermit).filter(StayPermit.reg_number.in_(unique_map.keys())).all()
    existing_map = {record.reg_number: record for record in existing_records}

    for reg_number, payload in unique_map.items():
        existing = existing_map.get(reg_number)
        if existing:
            new_status = payload.get("status")
            if existing.status != new_status:
                existing.last_status = existing.status
                existing.status = new_status
            for key, value in payload.items():
                if key in {"id", "notified_as_new", "last_status"}:
                    continue
                if hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            record = StayPermit(**payload)
            record.last_status = payload.get("status")
            db.add(record)

    db.commit()


async def save_or_update_stay_permit_data_async(data_list: list[dict]) -> None:
    if not data_list:
        return

    try:
        outgoing_messages: list[tuple[str, Path | None]] = []
        with SessionLocal() as db:
            for item in data_list:
                reg_number = item.get("reg_number")
                if not reg_number:
                    continue

                permit = db.query(StayPermit).filter(StayPermit.reg_number == reg_number).first()
                if not permit or permit.notified_as_new:
                    continue

                text = (
                    "Новый ITK добавлен в систему!\n\n"
                    f"ФИО: {item.get('name', '—')}\n"
                    f"Паспорт: {item.get('passport_number', '—')}\n"
                    f"Тип разрешения: {item.get('type_of_staypermit', '—')}\n"
                    f"Дата выдачи: {item.get('issue_date', '—')}\n"
                    f"Действует до: {item.get('expired_date', '—')}\n"
                    f"Рег. номер: {reg_number}\n"
                    f"Статус: {item.get('status', 'не указан')}"
                )
                file_path = _stay_pdf_path(reg_number)
                outgoing_messages.append((text, file_path if file_path.exists() else None))
                permit.notified_as_new = True

            db.commit()

        for text, document_path in outgoing_messages:
            await send_telegram_message(text, document_path=document_path)
    except Exception as exc:
        logger.error("Ошибка отправки уведомлений о новых ITK: %s", exc)


def get_user_by_telegram_id(db: Session, telegram_id: str):
    return db.query(User).filter(User.telegram_id == telegram_id).first()


def search_by_passport(db: Session, search_input: str):
    search_input = search_input.strip().upper()
    passport_match = re.search(r"\b(?=\w*\d)[A-Z0-9-]{5,}\b", search_input)
    passport_number = passport_match.group(0) if passport_match else None
    name_query = re.sub(rf"\b{re.escape(passport_number)}\b\s*", "", search_input).strip() if passport_number else search_input
    name_parts = list(dict.fromkeys(filter(None, re.split(r"\s+", name_query))))

    query = db.query(BatchApplication)
    if passport_number:
        query = query.filter(
            or_(
                BatchApplication.passport_number == passport_number,
                BatchApplication.passport_number.ilike(f"%{passport_number}%"),
            )
        )
    if name_parts:
        query = query.filter(or_(*[BatchApplication.full_name.ilike(f"%{part}%") for part in name_parts]))

    if not passport_number and not name_parts:
        return []

    results = query.all()
    logger.info("Найдено записей BatchApplication: %s", len(results))
    return results


def search_by_stay_permit(db: Session, passport_number_input: str):
    passport_number = passport_number_input.strip().upper()
    if not passport_number:
        return []
    results = db.query(StayPermit).filter(StayPermit.passport_number == passport_number).all()
    logger.info("Найдено StayPermit: %s", len(results))
    return results
