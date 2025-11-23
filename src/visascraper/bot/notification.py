from aiogram import Bot
from datetime import date, timedelta
import os
from database.db import SessionLocal
from database.models import BatchApplication, StayPermit
from utils.logger import logger as custom_logger
from sqlalchemy import func
from aiogram.exceptions import TelegramRetryAfter
from dotenv import load_dotenv
import asyncio
from aiogram.types import FSInputFile
import queue
import asyncio
from aiogram.types import FSInputFile
import os

load_dotenv()


# === Инициализация бота ===
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
bot = Bot(token=BOT_TOKEN)


# === Асинхронная отправка сообщения ===
DELAY_BETWEEN_MESSAGES = 1  # 1 секунда между сообщениями

# Глобальная очередь — безопасная для многопоточной записи
notification_queue = queue.Queue()   # ←←← ВОТ ЭТА СТРОКА ИЗМЕНИЛАСЬ

async def send_telegram_message(text: str, document: FSInputFile = None):
    bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    try:
        if document:
            await bot.send_document(
                    chat_id=os.getenv("TELEGRAM_CHANNEL_ID"),
                    document=document,
                    caption=text
                )
        else:
            await bot.send_message(chat_id=os.getenv("TELEGRAM_CHANNEL_ID"), text=text)
            print("✅ Сообщение отправлено в Telegram")
            await asyncio.sleep(DELAY_BETWEEN_MESSAGES)  # Задержка после успешной отправки
    except TelegramRetryAfter as e:
        print(f"⚠️ Слишком много запросов. Ждём {e.retry_after} секунд...")
        await asyncio.sleep(e.retry_after)
        await send_telegram_message(text)  # Повторяем отправку после ожидания
    except Exception as e:
        print(f"❌ Ошибка при отправке сообщения: {e} {os.getenv("TELEGRAM_CHANNEL_ID")}")
    finally:
        await bot.session.close()

# Обработчик очереди — запускается в основном event loop
async def notification_worker():
    custom_logger.info("notification_worker запущен и ждёт задачи...")
    while True:
        try:
            # ←←← .get() блокирует, пока не придёт задача
            item = notification_queue.get(timeout=1)
        except queue.Empty:
            continue

        if item["type"] == "new_stay_permit":
            data = item["data"]
            reg_number = data.get("reg_number")
            if not reg_number:
                notification_queue.task_done()
                continue

            # Проверка, не отправляли ли уже
            db = SessionLocal()
            try:
                permit = db.query(StayPermit).filter(StayPermit.reg_number == reg_number).first()
                if permit and getattr(permit, "notified_as_new", False):
                    notification_queue.task_done()
                    continue
            finally:
                db.close()

            file_path = f"src/temp/{reg_number}_stay_permit.pdf"
            document = FSInputFile(file_path) if os.path.exists(file_path) else None

            text = (
                    "🗒️Новый ITK в системе!\n\n"
                    f"ФИО: {data.get('name') or '—'}\n"
                    f"Паспорт: {data.get('passport_number') or '—'}\n"
                    f"Тип: {data.get('type_of_staypermit') or '—'}\n"
                    f"Выдан: {data.get('issue_date') or '—'}\n"
                    f"До: {data.get('expired_date') or '—'}\n"
                    f"Рег.номер: {reg_number}"
                )

            await send_telegram_message(text, document=document)

            # Помечаем как отправленное
            db = SessionLocal()
            try:
                permit = db.query(StayPermit).filter(StayPermit.reg_number == reg_number).first()
                if permit:
                    permit.notified_as_new = True
                    db.commit()
            except:
                db.rollback()
            finally:
                db.close()

        notification_queue.task_done()


# === 1. Уведомление о статусе "Approved" ===
async def notify_approved_users():
    db = SessionLocal()
    print('Запустили крон: notify_approved_users')
    try:
        users = db.query(BatchApplication).all()
        for user in users:
            if user.status == "Approved" and user.last_status != "Approved" and user.last_status != None:

                file_path = f"src/temp/{user.register_number}_batch_application.pdf"
                document = FSInputFile(file_path)

                text = (
                    f"🎉 Виза одобрена!\n"
                    f"Имя: {user.full_name}\n"
                    f"Статус: {user.status}\n"
                    f"Номер паспорта: {user.passport_number}\n"
                  #  f"Ссылка: {user.action_link}"
                )
                await send_telegram_message(text, document=document)
                user.last_status = "Approved"
                db.commit()
            elif user.status != "Approved" and user.last_status == "Approved":
                user.last_status = user.status
                db.commit()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        db.rollback()
    finally:
        db.close()

async def notify_approved_stay_permits():
    db = SessionLocal()
    print('Запустили крон: notify_approved_stay_permits')
    try:
        # Запрашиваем все разрешения на пребывание (StayPermit)
        permits = db.query(StayPermit).all()
        for permit in permits:
            # Основное условие: статус стал "Approved", а предыдущий статус был другим (или не был установлен)
            if permit.status == "Approved" and permit.last_status != "Approved":
                
                # Формируем путь к локальному PDF файлу
                file_path = f"src/temp/{permit.reg_number}_stay_permit.pdf"
                document = FSInputFile(file_path)

                # Формируем текст уведомления
                text = (
                    f"🎉 ITK (Stay Permit) одобрен!\n"
                    f"Имя: {permit.name}\n"
                    f"Статус: {permit.status}\n"
                    f"Номер паспорта: {permit.passport_number}\n"
                    f"Тип разрешения: {permit.type_of_staypermit}"
                )
                
                # Отправляем сообщение с документом
                await send_telegram_message(text, document=document)
                
                # Обновляем last_status, чтобы не отправлять повторно
                permit.last_status = "Approved"
                db.commit()

            # Условие сброса: если статус изменился с "Approved" на любой другой
            elif permit.status != "Approved" and permit.last_status == "Approved":
                permit.last_status = permit.status
                db.commit()

    except Exception as e:
        print(f"❌ Ошибка в notify_approved_stay_permits: {e}")
        db.rollback()
    finally:
        db.close()


# === 2. Проверка дней рождения ===
async def check_birthdays():
    db = SessionLocal()
    today = date.today()
    print('Запустили крон: check_birthdays')
    try:
        users = db.query(BatchApplication).filter(
            BatchApplication.birth_date.is_not(None),
            BatchApplication.birth_date.like(f"{today.strftime('%d/%m')}/%")
        ).all()

        for user in users:
            text = (
                f"🎂 Сегодня день рождения у {user.full_name}!\n"
                f"Тип визы: {user.visa_type}\n"
                f"Дата рождения: {user.birth_date}"
            )
            await send_telegram_message(text)
    except Exception as e:
        print(f"❌ Ошибка при проверке дней рождения: {e}")
    finally:
        db.close()


# === 3. Проверка истечения срока действия визы ===
async def check_visa_expirations():
    db = SessionLocal()
    today = date.today()
    target_date = (today + timedelta(days=40)).strftime("%Y-%m-%d")
    two_target_date = (today + timedelta(days=5)).strftime("%Y-%m-%d")

    print('Запустили крон: check_visa_expirations')

    try:
        users = db.query(StayPermit).filter(
            StayPermit.expired_date.is_not(None),
            StayPermit.expired_date == target_date
        ).all()

        for user in users:
            file_path = f"src/temp/{user.reg_number}_stay_permit.pdf"
            document = FSInputFile(file_path)

            text = (
                f"⚠️ ВНИМАНИЕ: У пользователя c номером паспорта {user.passport_number} виза заканчивается через 40 дней!\n"
                f"Дата окончания: {user.expired_date}\n"
                f"Тип визы: {user.type_of_staypermit}\n"
               # f"Ссылка: {user.action_link}"
            )
            await send_telegram_message(text, document)

        users = db.query(StayPermit).filter(
            StayPermit.expired_date.is_not(None),
            StayPermit.expired_date == two_target_date
        ).all()

        for user in users:
            file_path = f"src/temp/{user.reg_number}_stay_permit.pdf"
            document = FSInputFile(file_path)

            text = (
                f"⚠️ ВНИМАНИЕ: У пользователя c номером паспорта {user.passport_number} виза заканчивается через 5 дней!\n"
                f"Дата окончания: {user.expired_date}\n"
                f"Тип визы: {user.type_of_staypermit}\n"
               # f"Ссылка: {user.action_link}"
            )
            await send_telegram_message(text, document=document)
    except Exception as e:
        print(f"❌ Ошибка при проверке истечения визы: {e}")
    finally:
        db.close()