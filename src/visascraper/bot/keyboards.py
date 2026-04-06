from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from visascraper.config import settings


def main_menu(user_id: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🔍 Узнать готовность визы", callback_data="search_passport")],
        [InlineKeyboardButton(text="🏠 Получить ITK", callback_data="search_stay_permit")],
    ]
    if user_id in settings.admin_user_ids:
        buttons.append([InlineKeyboardButton(text="🔑 Админ-панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Запустить парсинг второстепенных аккаунтов", callback_data="start_parsing_others")]
        ]
    )
