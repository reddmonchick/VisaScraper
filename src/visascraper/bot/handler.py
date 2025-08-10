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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BOT_PASSWORD = os.getenv("TELEGRAM_BOT_PASSWORD")

if not BOT_TOKEN or not BOT_PASSWORD:
    logger.error("TELEGRAM_BOT_TOKEN или TELEGRAM_BOT_PASSWORD не заданы в .env")
    raise ValueError("Не заданы необходимые переменные окружения")

bot_router = Router()

class PassportSearch(StatesGroup):
    waiting_for_passport = State()

class StayPermitSearch(StatesGroup):
    waiting_for_stay_permit = State()

def get_user_by_telegram_id(db: Session, telegram_id: str):
    try:
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        logger.info(f"Поиск пользователя: telegram_id={telegram_id}, найден={user is not None}")
        return user
    except Exception as e:
        logger.error(f"Ошибка при запросе пользователя: {e}")
        return None

def create_or_update_user(db: Session, telegram_id: str):
    try:
        user = get_user_by_telegram_id(db, telegram_id)
        if not user:
            user = User(telegram_id=telegram_id, password=BOT_PASSWORD)
            db.add(user)
            logger.info(f"Создан новый пользователь: telegram_id={telegram_id}")
        else:
            user.password = BOT_PASSWORD
            logger.info(f"Обновлен пароль пользователя: telegram_id={telegram_id}")
        db.commit()
        db.refresh(user)
        return user
    except Exception as e:
        logger.error(f"Ошибка при создании/обновлении пользователя: {e}")
        db.rollback()
        return None

def is_authorized(db: Session, telegram_id: str) -> bool:
    user = get_user_by_telegram_id(db, telegram_id)
    authorized = user is not None and user.password == BOT_PASSWORD
    logger.info(f"Проверка авторизации: telegram_id={telegram_id}, authorized={authorized}")
    return authorized

def main_menu():
    kb = [
        [InlineKeyboardButton(text="🔍 Нажми чтобы узнать готовность визы", callback_data="search_passport")],
        [InlineKeyboardButton(text="🏠 Получить ITK", callback_data="search_stay_permit")]
    ]
    logger.info("Создана клавиатура главного меню")
    return InlineKeyboardMarkup(inline_keyboard=kb)

@bot_router.message(F.text == "/start")
async def cmd_start(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    logger.info(f"Команда /start от пользователя: telegram_id={user_id}")
 
    with SessionLocal() as db:
        if is_authorized(db, user_id):
            await message.answer("✅ Вы уже авторизованы!", reply_markup=main_menu())
        else:
            await message.answer("🔐 Введите пароль:")
    current_state = await state.get_state()
    logger.info(f"Текущее состояние после /start: {current_state}")

@bot_router.callback_query(lambda c: c.data == "search_passport")
async def start_search(callback: CallbackQuery, state: FSMContext):
    user_id = str(callback.from_user.id)
    logger.info(f"Callback search_passport от пользователя: user_id={user_id}")

    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            logger.warning(f"Пользователь {user_id} не авторизован")
            await callback.message.answer("⚠️ Вы не авторизованы.")
            await callback.answer()
            return

    await state.clear()
    await state.set_state(PassportSearch.waiting_for_passport)
    current_state = await state.get_state()
    logger.info(f"Установлено состояние: {current_state} для user_id={user_id}")
    #await callback.message.answer(f"Текущее состояние: {current_state}")
    await callback.message.answer("Введите имя и фамилию на латинице и номер паспорта")
    await callback.message.answer("Пример: ROMAN DUDUKALOV 4729312290")
    await callback.answer()

@bot_router.message(PassportSearch.waiting_for_passport)
async def process_passport_input(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    current_state = await state.get_state()
    logger.info(f"Обработка ввода в состоянии PassportSearch.waiting_for_passport: text={message.text}, user_id={user_id}, state={current_state}")

    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            logger.warning(f"Пользователь {user_id} не авторизован")
            await message.answer("⚠️ Вы не авторизованы.")
            await state.clear()
            return

    search_input = message.text.strip().upper()
    logger.info(f"Поиск по паспорту: input={search_input}, user_id={user_id}")

    with SessionLocal() as db:
        try:
            results = search_by_passport(db, search_input)
            logger.info(f"Результат поиска по паспорту: найдено={len(results) if results else 0} записей")
        except Exception as e:
            logger.error(f"Ошибка при поиске по паспорту: {e}")
            await message.answer("❌ Ошибка при поиске.")
            await state.clear()
            return

    if not results:
        logger.info(f"По запросу '{search_input}' ничего не найдено")
        await message.answer("❌ По вашему запросу ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu())
        await state.clear()
        return

    for result in results:
        info_data = {
            "Батч номер": result.batch_no,
            "Рег. номер": result.register_number,
            "Полное имя": result.full_name,
            "Номер визы": result.visitor_visa_number,
            "Тип визы": result.visa_type,
            "Номер паспорта": result.passport_number,
            "Дата оплаты": result.payment_date,
            "День рождения": result.birth_date,
            "Статус": result.status,
            "Аккаунт": result.account
        }
        info = "\n".join([f"{key}: {value}" for key, value in info_data.items()])
        

        file_path = f"src/temp/{result.register_number}_batch_application.pdf"
        

        if os.path.exists(file_path):
            try:
                document = FSInputFile(file_path)
                await message.answer_document(
                    document=document,
                    caption=f" Результат по готовности визы:\n\n{info}"
                )
            except Exception as e:
                logger.error(f"Ошибка отправки файла {file_path}: {e}")
                await message.answer(f"❌ Ошибка отправки файла для рег. номера {result.reg_number}")
        else:
            logger.warning(f"Файл не найден: {file_path}")
            await message.answer(f" Результат:\n\n{info}\n\n⚠️ Файл разрешения отсутствует")

    await message.answer("Выберите действие:", reply_markup=main_menu())
    await state.clear()
    logger.info(f"Состояние очищено для user_id={user_id}")

@bot_router.callback_query(lambda c: c.data == "search_stay_permit")
async def start_search_stay(callback: CallbackQuery, state: FSMContext):
    user_id = str(callback.from_user.id)
    logger.info(f"Callback search_stay_permit от пользователя: user_id={user_id}")

    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            logger.warning(f"Пользователь {user_id} не авторизован")
            await callback.message.answer("⚠️ Вы не авторизованы.")
            await callback.answer()
            return

    await state.clear()
    await state.set_state(StayPermitSearch.waiting_for_stay_permit)
    current_state = await state.get_state()
    logger.info(f"Установлено состояние: {current_state} для user_id={user_id}")
    #await callback.message.answer(f"Текущее состояние: {current_state}")
    await callback.message.answer("Введите имя и фамилию на латинице и номер паспорта")
    await callback.message.answer("Пример: ROMAN DUDUKALOV 4729312290")
    await callback.answer()

@bot_router.message(StayPermitSearch.waiting_for_stay_permit)
async def process_stay_permit_input(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    current_state = await state.get_state()
    logger.info(f"Обработка ввода в состоянии StayPermitSearch.waiting_for_stay_permit: text={message.text}, user_id={user_id}, state={current_state}")

    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            logger.warning(f"Пользователь {user_id} не авторизован")
            await message.answer("⚠️ Вы не авторизованы.")
            await state.clear()
            return

    search_input = message.text.strip().upper()
    logger.info(f"Поиск по разрешению на проживание: input={search_input}, user_id={user_id}")

    with SessionLocal() as db:
        try:
            results = search_by_stay_permit(db, search_input)
            logger.info(f"Результат поиска по разрешению: найдено={len(results) if results else 0} записей")
        except Exception as e:
            logger.error(f"Ошибка при поиске по разрешению: {e}")
            await message.answer("❌ Ошибка при поиске.")
            await state.clear()
            return

    if not results:
        logger.info(f"По запросу '{search_input}' ничего не найдено")
        await message.answer("❌ По вашему запросу ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu())
        await state.clear()
        return

    for result in results:
        info_data = {
            "Рег. номер": result.reg_number,
            "Полное имя": result.name,
            "Тип разрешения": result.type_of_staypermit,
            "Тип визы": result.visa_type,
            "Номер паспорта": result.passport_number,
            "Дата прибытия": result.arrival_date,
            "Дата выдачи": result.issue_date,
            "Срок действия": result.expired_date,
            "Статус": result.status,
            "Аккаунт": result.account
        }
        info = "\n".join([f"{key}: {value}" for key, value in info_data.items()])
        

        file_path = f"src/temp/{result.reg_number}_stay_permit.pdf"
        

        if os.path.exists(file_path):
            try:
                document = FSInputFile(file_path)
                await message.answer_document(
                    document=document,
                    caption=f"🏠 Результат по разрешению на проживание:\n\n{info}"
                )
            except Exception as e:
                logger.error(f"Ошибка отправки файла {file_path}: {e}")
                await message.answer(f"❌ Ошибка отправки файла для рег. номера {result.reg_number}")
        else:
            logger.warning(f"Файл не найден: {file_path}")
            await message.answer(f"🏠 Результат:\n\n{info}\n\n⚠️ Файл разрешения отсутствует")

    await message.answer("Выберите действие:", reply_markup=main_menu())
    await state.clear()
    logger.info(f"Состояние очищено для user_id={user_id}")

@bot_router.message(F.text)
async def check_password_or_other_text(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    current_state = await state.get_state()
    logger.info(f"Получено текстовое сообщение: text={message.text}, user_id={user_id}, state={current_state}")

    with SessionLocal() as db:
        if is_authorized(db, user_id):
            logger.info(f"Пользователь {user_id} уже авторизован")
            await message.answer("Вы уже авторизованы!", reply_markup=main_menu())
            return

    entered_password = message.text.strip()
    if entered_password != BOT_PASSWORD:
        logger.warning(f"Неверный пароль от пользователя {user_id}: {entered_password}")
        await message.answer("❌ Неверный пароль.")
        return

    with SessionLocal() as db:
        user = create_or_update_user(db, user_id)
        if user:
            db.commit()
            await message.answer("✅ Авторизация успешна!", reply_markup=main_menu())
        else:
            await message.answer("❌ Ошибка при авторизации.")