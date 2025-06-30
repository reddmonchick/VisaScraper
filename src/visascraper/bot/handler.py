from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
import os
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from visascraper.database.db import SessionLocal
from visascraper.database.models import User
from visascraper.database.crud import search_by_passport

load_dotenv()

router = Router()
BOT_PASSWORD = os.getenv("TELEGRAM_BOT_PASSWORD")

# === Модель состояния для ввода паспорта ===
class PassportSearch(StatesGroup):
    waiting_for_passport = State()
    waiting_for_stay_permit = State()  # Новое состояние

# === Функции работы с БД ===
def get_user_by_telegram_id(db: Session, telegram_id: str):
    return db.query(User).filter(User.telegram_id == telegram_id).first()

def create_or_update_user(db: Session, telegram_id: str):
    user = get_user_by_telegram_id(db, telegram_id)
    if not user:
        user = User(telegram_id=telegram_id, password=BOT_PASSWORD)
        db.add(user)
    else:
        user.password = BOT_PASSWORD
    db.commit()
    db.refresh(user)
    return user

def is_authorized(db: Session, telegram_id: str) -> bool:
    user = db.query(User).filter(User.telegram_id == telegram_id).first()
    return user is not None and user.password == BOT_PASSWORD

# === Клавиатуры ===
def main_menu():
    kb = [
        [InlineKeyboardButton(text="🔍 Найти статус визы", callback_data="search_passport")],
        [InlineKeyboardButton(text="🏠 Найти место жительства", callback_data="search_stay_permit")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

# === Обработчики команд ===
@router.message(F.text == "/start")
async def cmd_start(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)

    with SessionLocal() as db:
        if is_authorized(db, user_id):
            await message.answer("✅ Вы уже авторизованы!", reply_markup=main_menu())
        else:
            await message.answer("🔐 Введите пароль:")


@router.message(PassportSearch.waiting_for_passport)
async def process_passport_input(message: Message, state: FSMContext):
    search_input = message.text.strip().upper()
    
    with SessionLocal() as db:
        results = search_by_passport(db, search_input)

    if not results:
        await message.answer("❌ По вашему запросу ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu())
        await state.clear()
        return

    for result in results:
        info = "\n".join([f"{key}: {value}" for key, value in result.__dict__.items() if not key.startswith("_")])
        await message.answer(f"🔍 Результат:\n\n{info}")

    await message.answer("Выберите действие:", reply_markup=main_menu())
    await state.clear()


@router.message(F.text)
async def check_password_or_other_text(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)

    with SessionLocal() as db:
        if is_authorized(db, user_id):
            # Если уже авторизован — показываем меню
            return await message.answer("Вы уже авторизованы!", reply_markup=main_menu())

    entered_password = message.text.strip()
    if entered_password != BOT_PASSWORD:
        await message.answer("❌ Неверный пароль.")
        return

    with SessionLocal() as db:
        create_or_update_user(db, user_id)
        db.commit()
        await message.answer("✅ Авторизация успешна!", reply_markup=main_menu())


@router.callback_query(lambda c: c.data == "search_passport")
async def start_search(callback: CallbackQuery, state: FSMContext):
    user_id = str(callback.from_user.id)

    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            return await callback.message.answer("⚠️ Вы не авторизованы.")

    await state.set_state(PassportSearch.waiting_for_passport)
    await callback.message.answer("Введите имя и фамилию на латинице и номер паспорта")
    await callback.message.answer("Пример: ROMAN DUDUKALOV 4729312290")

@router.message(PassportSearch.waiting_for_stay_permit)
async def process_stay_permit_input(message: Message, state: FSMContext):
    search_input = message.text.strip().upper()

    with SessionLocal() as db:
        results = search_by_stay_permit(db, search_input)

    if not results:
        await message.answer("❌ По вашему запросу ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu())
        await state.clear()
        return

    for result in results:
        info = "\n".join([f"{key}: {value}" for key, value in result.__dict__.items() if not key.startswith("_")])
        await message.answer(f"🏠 Результат:\n\n{info}")

    await message.answer("Выберите действие:", reply_markup=main_menu())
    await state.clear()

@router.callback_query(lambda c: c.data == "search_stay_permit")
async def start_search_stay(callback: CallbackQuery, state: FSMContext):
    user_id = str(callback.from_user.id)

    with SessionLocal() as db:
        if not is_authorized(db, user_id):
            return await callback.message.answer("⚠️ Вы не авторизованы.")

    await state.set_state(PassportSearch.waiting_for_stay_permit)
    await callback.message.answer("Введите имя и фамилию на латинице и номер паспорта")
    await callback.message.answer("Пример: ROMAN DUDUKALOV 4729312290")