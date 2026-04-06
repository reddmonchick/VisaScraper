from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, Message
from sqlalchemy.orm import Session

from visascraper.bot.keyboards import admin_menu, main_menu
from visascraper.config import settings
from visascraper.database.crud import search_by_passport, search_by_stay_permit
from visascraper.database.db import SessionLocal
from visascraper.database.models import User

if not settings.telegram_bot_token or not settings.telegram_bot_password:
    raise ValueError("Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_BOT_PASSWORD")

logger = logging.getLogger(__name__)
bot_router = Router()
is_parsing_running = False


class PassportSearch(StatesGroup):
    waiting_for_passport = State()


class StayPermitSearch(StatesGroup):
    waiting_for_stay_permit = State()


def get_user_by_telegram_id(db: Session, telegram_id: str):
    return db.query(User).filter(User.telegram_id == telegram_id).first()


def create_or_update_user(db: Session, telegram_id: str):
    user = get_user_by_telegram_id(db, telegram_id)
    if not user:
        user = User(telegram_id=telegram_id, password=settings.telegram_bot_password)
        db.add(user)
        logger.info("Создан новый пользователь %s", telegram_id)
    else:
        user.password = settings.telegram_bot_password
        logger.info("Обновлён пароль пользователя %s", telegram_id)
    db.commit()
    return user


def is_authorized(db: Session, telegram_id: str) -> bool:
    user = get_user_by_telegram_id(db, telegram_id)
    return user is not None and user.password == settings.telegram_bot_password


def authorize_user(db: Session, telegram_id: str):
    user = get_user_by_telegram_id(db, telegram_id)
    if not user:
        user = User(telegram_id=telegram_id, is_authorized=True)
        db.add(user)
        logger.info("Создан новый пользователь %s", telegram_id)
    else:
        logger.info("Обновлён доступ пользователя %s", telegram_id)

    user.password = None
    user.is_authorized = True
    db.commit()
    return user


def is_authorized(db: Session, telegram_id: str) -> bool:
    user = get_user_by_telegram_id(db, telegram_id)
    return user is not None and bool(user.is_authorized)


@bot_router.message(F.text == "/start")
async def cmd_start(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    with SessionLocal() as db:
        if is_authorized(db, user_id):
            await message.answer("✅ Вы уже авторизованы!", reply_markup=main_menu(user_id))
        else:
            await message.answer("🔐 Введите пароль:")
    await state.clear()


@bot_router.callback_query(F.data == "admin_panel")
async def callback_admin(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    if user_id not in settings.admin_user_ids:
        await callback.answer("⚠️ У вас нет доступа!", show_alert=True)
        return

    await callback.message.answer("Админ-панель:", reply_markup=admin_menu())
    await callback.answer()


@bot_router.callback_query(F.data == "start_parsing_others")
async def start_parsing_others(callback: CallbackQuery, bot: Bot, app):
    global is_parsing_running

    user_id = str(callback.from_user.id)
    if user_id not in settings.admin_user_ids:
        await callback.answer("⚠️ Доступ запрещён!", show_alert=True)
        return
    if is_parsing_running:
        await callback.answer("Парсинг уже запущен!", show_alert=True)
        return

    is_parsing_running = True
    loop = asyncio.get_running_loop()
    status_message = await callback.message.answer(
        "⏳ Подготовка к парсингу второстепенных аккаунтов..."
    )
    await callback.answer()

    last_progress_text = status_message.text or ""

    async def update_progress_message(text: str) -> None:
        nonlocal last_progress_text
        if text == last_progress_text:
            return
        with suppress(Exception):
            await status_message.edit_text(text)
            last_progress_text = text

    def progress_callback(processed: int, total: int, current_account: str, remaining: int, batch_count: int, stay_count: int) -> None:
        progress_text = (
            "🔄 Парсинг второстепенных аккаунтов...\n"
            f"Последний аккаунт: {current_account}\n"
            f"Прогресс: {processed}/{total}\n"
            f"Спарсилось: {processed}\n"
            f"Осталось: {remaining}\n"
            f"Batch записей: {batch_count}\n"
            f"Stay Permit записей: {stay_count}"
        )
        asyncio.run_coroutine_threadsafe(update_progress_message(progress_text), loop)

    try:
        await asyncio.to_thread(app.job_scheduler.job_others, progress_callback)
        final_text = last_progress_text
        if "Прогресс:" not in final_text:
            final_text = "✅ Парсинг завершён!"
        else:
            final_text = f"✅ Завершено\n{final_text.split('\n', 1)[1]}"
        await update_progress_message(final_text)
        await bot.send_message(user_id, "✅ Парсинг завершён!")
    except Exception as exc:
        logger.exception("Ошибка парсинга второстепенных аккаунтов")
        await update_progress_message(f"❌ Ошибка парсинга: {exc}")
        await bot.send_message(user_id, f"❌ Ошибка: {exc}")
    finally:
        is_parsing_running = False


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
        results = search_by_passport(db, search_input)

    if not results:
        await message.answer("❌ Ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu(user_id))
        await state.clear()
        return

    for result in results:
        info = (
            f"Батч номер: {result.batch_no}\n"
            f"Рег. номер: {result.register_number}\n"
            f"Полное имя: {result.full_name}\n"
            f"Номер визы: {result.visitor_visa_number}\n"
            f"Тип визы: {result.visa_type}\n"
            f"Номер паспорта: {result.passport_number}\n"
            f"Дата оплаты: {result.payment_date}\n"
            f"День рождения: {result.birth_date}\n"
            f"Статус: {result.status}\n"
            f"Аккаунт: {result.account}"
        )
        file_path = settings.temp_dir / f"{result.register_number}_batch_application.pdf"
        if file_path.exists():
            await message.answer_document(document=FSInputFile(file_path), caption=f"Результат:\n\n{info}")
        else:
            await message.answer(f"{info}\n\n⚠️ PDF-файл отсутствует")

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
    await callback.message.answer("Введите номер паспорта\nПример: 4729312290")
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
        results = search_by_stay_permit(db, search_input)

    if not results:
        await message.answer("❌ Ничего не найдено.")
        await message.answer("Выберите действие:", reply_markup=main_menu(user_id))
        await state.clear()
        return

    for result in results:
        info = (
            f"Рег. номер: {result.reg_number}\n"
            f"Полное имя: {result.name}\n"
            f"Тип разрешения: {result.type_of_staypermit}\n"
            f"Тип визы: {result.visa_type}\n"
            f"Номер паспорта: {result.passport_number}\n"
            f"Дата прибытия: {result.arrival_date}\n"
            f"Дата выдачи: {result.issue_date}\n"
            f"Срок действия: {result.expired_date}\n"
            f"Статус: {result.status}\n"
            f"Аккаунт: {result.account}"
        )
        file_path = settings.temp_dir / f"{result.reg_number}_stay_permit.pdf"
        if file_path.exists():
            await message.answer_document(document=FSInputFile(file_path), caption=f"🏠 Результат:\n\n{info}")
        else:
            await message.answer(f"🏠 Результат:\n\n{info}\n\n⚠️ PDF-файл отсутствует")

    await message.answer("Выберите действие:", reply_markup=main_menu(user_id))
    await state.clear()


@bot_router.message(F.text)
async def check_password_or_other_text(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    entered_password = message.text.strip()

    if user_id in settings.admin_user_ids:
        with SessionLocal() as db:
            authorize_user(db, user_id)
        await message.answer("✅ Автоматическая авторизация админа!", reply_markup=main_menu(user_id))
        return

    with SessionLocal() as db:
        if is_authorized(db, user_id):
            await message.answer("Вы уже авторизованы!", reply_markup=main_menu(user_id))
            return

    if entered_password != settings.telegram_bot_password:
        await message.answer("❌ Неверный пароль.")
        return

    with SessionLocal() as db:
        authorize_user(db, user_id)
    await message.answer("✅ Авторизация успешна!", reply_markup=main_menu(user_id))
