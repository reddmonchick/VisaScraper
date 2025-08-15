import logging
import asyncio
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from sqlalchemy.orm import Session
from aiogram.types import FSInputFile

from database.db import SessionLocal
from database.models import User
from database.crud import search_by_passport, search_by_stay_permit
from bot.keyboards import main_menu, admin_menu

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BOT_PASSWORD = os.getenv("TELEGRAM_BOT_PASSWORD")
ADMIN_USER_IDS = [uid.strip() for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()]

if not BOT_TOKEN or not BOT_PASSWORD:
    raise ValueError("Не заданы необходимые переменные окружения")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

bot_router = Router()

# Глобальный флаг для парсинга
is_parsing_running = False

# Состояния для поиска
class PassportSearch(StatesGroup):
    waiting_for_passport = State()

class StayPermitSearch(StatesGroup):
    waiting_for_stay_permit = State()

# Функции работы с БД
def get_user_by_telegram_id(db: Session, telegram_id: str):
    return db.query(User).filter(User.telegram_id == telegram_id).first()

def create_or_update_user(db: Session, telegram_id: str):
    try:
        user = get_user_by_telegram_id(db, telegram_id)
        if not user:
            user = User(telegram_id=telegram_id, password=BOT_PASSWORD)
            db.add(user)
            logger.info(f"Создан новый пользователь: {telegram_id}")
        else:
            user.password = BOT_PASSWORD
            logger.info(f"Обновлен пароль пользователя: {telegram_id}")
        db.commit()
        return user
    except Exception as e:
        logger.error(f"Ошибка при создании/обновлении пользователя: {e}")
        db.rollback()
        return None

def is_authorized(db: Session, telegram_id: str) -> bool:
    user = get_user_by_telegram_id(db, telegram_id)
    return user is not None and user.password == BOT_PASSWORD


# Обработчики команд
@bot_router.message(F.text == "/start")
async def cmd_start(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    logger.info(f"Команда /start от {user_id}")
 
    with SessionLocal() as db:
        if is_authorized(db, user_id):
            await message.answer("✅ Вы уже авторизованы!", reply_markup=main_menu(user_id))
        else:
            await message.answer("🔐 Введите пароль:")
    await state.clear()


# Обработчики административных функций
@bot_router.callback_query(F.data == "admin_panel")
async def callback_admin(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    
    if user_id not in ADMIN_USER_IDS:
        await callback.answer("⚠️ У вас нет доступа!", show_alert=True)
        return
    
    await callback.message.answer("Админ-панель:", reply_markup=admin_menu())
    await callback.answer()

@bot_router.callback_query(F.data == "start_parsing_others")
async def start_parsing_others(callback: CallbackQuery, bot: Bot, app):
    global is_parsing_running
    user_id = str(callback.from_user.id)
    
    if user_id not in ADMIN_USER_IDS:
        await callback.answer("⚠️ Доступ запрещен!", show_alert=True)
        return

    if is_parsing_running:
        await callback.answer("Парсинг уже запущен!", show_alert=True)
        return

    is_parsing_running = True
    await callback.message.answer("🚀 Запуск парсинга...")
    await callback.answer()
    
    try:
        await asyncio.to_thread(app.job_scheduler.job_others)
        await bot.send_message(user_id, "✅ Парсинг завершен!")
    except Exception as e:
        await bot.send_message(user_id, f"❌ Ошибка: {e}")
        logger.error(f"Ошибка парсинга: {e}", exc_info=True)
    finally:
        is_parsing_running = False

# Обработчики поиска
@bot_router.callback_query(F.data == "search_passport")
async def start_search(callback: CallbackQuery, state: FSMContext):
    user_id = str(callback.from_user.id)
    
    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            await callback.message.answer("⚠️ Вы не авторизованы!")
            await callback.answer()
            return

    await state.set_state(PassportSearch.waiting_for_passport)
    await callback.message.answer(
        "Введите имя и фамилию на латинице и номер паспорта\n"
        "Пример: ROMAN DUDUKALOV 4729312290"
    )
    await callback.answer()

@bot_router.message(PassportSearch.waiting_for_passport)
async def process_passport_input(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    search_input = message.text.strip().upper()
    
    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            await message.answer("⚠️ Вы не авторизованы!")
            await state.clear()
            return

        try:
            results = search_by_passport(db, search_input)
        except Exception as e:
            logger.error(f"Ошибка поиска: {e}")
            await message.answer("❌ Ошибка при поиске.")
            await state.clear()
            return

    if not results:
        await message.answer("❌ Ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu(user_id))
        await state.clear()
        return

    for result in results:
        info = f"""
Батч номер: {result.batch_no}
Рег. номер: {result.register_number}
Полное имя: {result.full_name}
Номер визы: {result.visitor_visa_number}
Тип визы: {result.visa_type}
Номер паспорта: {result.passport_number}
Дата оплаты: {result.payment_date}
День рождения: {result.birth_date}
Статус: {result.status}
Аккаунт: {result.account}
        """.strip()
        
        file_path = f"src/temp/{result.register_number}_batch_application.pdf"
        
        if os.path.exists(file_path):
            try:
                await message.answer_document(
                    document=FSInputFile(file_path),
                    caption=f"Результат:\n\n{info}"
                )
            except Exception as e:
                logger.error(f"Ошибка отправки файла: {e}")
                await message.answer(f"❌ Ошибка отправки файла для {result.register_number}")
        else:
            await message.answer(f"{info}\n\n⚠️ Файл отсутствует")

    await message.answer("Выберите действие:", reply_markup=main_menu(user_id))
    await state.clear()

@bot_router.callback_query(F.data == "search_stay_permit")
async def start_search_stay(callback: CallbackQuery, state: FSMContext):
    user_id = str(callback.from_user.id)
    
    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            await callback.message.answer("⚠️ Вы не авторизованы!")
            await callback.answer()
            return

    await state.set_state(StayPermitSearch.waiting_for_stay_permit)
    await callback.message.answer(
        "Введите имя и фамилию на латинице и номер паспорта\n"
        "Пример: ROMAN DUDUKALOV 4729312290"
    )
    await callback.answer()

@bot_router.message(StayPermitSearch.waiting_for_stay_permit)
async def process_stay_permit_input(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    search_input = message.text.strip().upper()
    
    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            await message.answer("⚠️ Вы не авторизованы!")
            await state.clear()
            return

        try:
            results = search_by_stay_permit(db, search_input)
        except Exception as e:
            logger.error(f"Ошибка поиска: {e}")
            await message.answer("❌ Ошибка при поиске.")
            await state.clear()
            return

    if not results:
        await message.answer("❌ Ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu(user_id))
        await state.clear()
        return

    for result in results:
        info = f"""
Рег. номер: {result.reg_number}
Полное имя: {result.name}
Тип разрешения: {result.type_of_staypermit}
Тип визы: {result.visa_type}
Номер паспорта: {result.passport_number}
Дата прибытия: {result.arrival_date}
Дата выдачи: {result.issue_date}
Срок действия: {result.expired_date}
Статус: {result.status}
Аккаунт: {result.account}
        """.strip()
        
        file_path = f"src/temp/{result.reg_number}_stay_permit.pdf"
        
        if os.path.exists(file_path):
            try:
                await message.answer_document(
                    document=FSInputFile(file_path),
                    caption=f"🏠 Результат:\n\n{info}"
                )
            except Exception as e:
                logger.error(f"Ошибка отправки файла: {e}")
                await message.answer(f"❌ Ошибка отправки файла для {result.reg_number}")
        else:
            await message.answer(f"🏠 Результат:\n\n{info}\n\n⚠️ Файл отсутствует")

    await message.answer("Выберите действие:", reply_markup=main_menu(user_id))
    await state.clear()

@bot_router.message(F.text)
async def check_password_or_other_text(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    entered_password = message.text.strip()
    
    # Автоматическая авторизация для админов
    if user_id in ADMIN_USER_IDS:
        with SessionLocal() as db:
            user = create_or_update_user(db, user_id)
            if user:
                await message.answer("✅ Автоматическая авторизация админа!", reply_markup=main_menu(user_id))
                return
    
    # Стандартная проверка пароля для обычных пользователей
    with SessionLocal() as db:
        if is_authorized(db, user_id):
            await message.answer("Вы уже авторизованы!", reply_markup=main_menu(user_id))
            return

    if entered_password != BOT_PASSWORD:
        await message.answer("❌ Неверный пароль.")
        return

    with SessionLocal() as db:
        user = create_or_update_user(db, user_id)
        if user:
            await message.answer("✅ Авторизация успешна!", reply_markup=main_menu(user_id))
        else:
            await message.answer("❌ Ошибка при авторизации.")