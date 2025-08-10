from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def main_menu():
    buttons = [
        [InlineKeyboardButton(text="🔍 Нажми чтобы узнать готовность визы", callback_data="search_passport")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_menu():
    buttons = [
        [InlineKeyboardButton(text="Запустить парсинг второстепенных аккаунтов", callback_data="start_parsing_others")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)