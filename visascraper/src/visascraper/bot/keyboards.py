from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def main_menu():
    buttons = [
        [InlineKeyboardButton(text="🔍 Нажми чтобы узнать готовность визы", callback_data="search_passport")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)