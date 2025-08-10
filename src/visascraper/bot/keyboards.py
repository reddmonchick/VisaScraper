from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def main_menu():
    buttons = [
        [InlineKeyboardButton(text="🔍 Узнать готовность визы", callback_data="search_passport")],
        [InlineKeyboardButton(text="🏠 Получить ITK", callback_data="search_stay_permit")],
        [InlineKeyboardButton(text="🔑 Админ-панель", callback_data="admin_panel")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_menu():
    buttons = [
        [InlineKeyboardButton(text="Запустить парсинг второстепенных аккаунтов", callback_data="start_parsing_others")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)