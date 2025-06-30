from aiogram import Bot
from datetime import date, timedelta
import os
from visascraper.database.db import SessionLocal
from visascraper.database.models import BatchApplication, StayPermit

# === Инициализация бота ===
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
bot = Bot(token=BOT_TOKEN)


# === Асинхронная отправка сообщения ===
async def send_telegram_message(text: str):
    try:
        await bot.send_message(chat_id=CHANNEL_ID, text=text)
        print("✅ Сообщение отправлено в Telegram")
    except Exception as e:
        print(f"❌ Ошибка при отправке сообщения: {e}")


# === 1. Уведомление о статусе "Approved" ===
async def notify_approved_users():
    db = SessionLocal()
    print('Запустили крон: notify_approved_users')
    try:
        users = db.query(BatchApplication).all()
        for user in users:
            if user.status == "Approved" and user.last_status != "Approved" and user.last_status != None:
                text = (
                    f"🎉 Виза одобрена!\n"
                    f"Имя: {user.full_name}\n"
                    f"Статус: {user.status}\n"
                    f"Номер паспорта: {user.passport_number}\n"
                    f"Ссылка: {user.action_link}"
                )
                await send_telegram_message(text)
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
    threshold = today + timedelta(days=30)  # За 30 дней до истечения
    print('Запустили крон: check_visa_expirations')
    try:
        users = db.query(StayPermit).filter(
            StayPermit.expired_date.is_not(None),
            StayPermit.expired_date == threshold.strftime("%d/%m/%Y")
        ).all()

        for user in users:
            text = (
                f"⚠️ ВНИМАНИЕ: У пользователя {user.name} виза заканчивается через 30 дней!\n"
                f"Дата окончания: {user.expired_date}\n"
                f"Тип визы: {user.type_of_staypermit}"
            )
            await send_telegram_message(text)
    except Exception as e:
        print(f"❌ Ошибка при проверке истечения визы: {e}")
    finally:
        db.close()