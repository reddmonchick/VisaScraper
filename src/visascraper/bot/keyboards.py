from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import os
from dotenv import load_dotenv
load_dotenv()




ADMIN_USER_IDS=os.getenv("ADMIN_USER_IDS")  # ID админов через запятую

ADMIN_USER_IDS = [uid.strip() for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()]

# 3. Модифицируем функцию создания главного меню
# Функции для клавиатур
def main_menu(user_id: str) -> InlineKeyboardMarkup:
    """Создает главное меню с учетом прав администратора"""
    buttons = [
        [InlineKeyboardButton(text="🔍 Узнать готовность визы", callback_data="search_passport")],
        [InlineKeyboardButton(text="🏠 Получить ITK", callback_data="search_stay_permit")],
    ]
    
    # Добавляем кнопку админ-панели только для админов
    if user_id in ADMIN_USER_IDS:
        buttons.append([InlineKeyboardButton(text="🔑 Админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_menu() -> InlineKeyboardMarkup:
    """Клавиатура администратора"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Запустить парсинг второстепенных аккаунтов", callback_data="start_parsing_others")]
    ])