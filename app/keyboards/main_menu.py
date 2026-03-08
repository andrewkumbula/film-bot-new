from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """
    Главное меню /start: фильм, сериал, избранное, настройки, помощь.
    """
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🎬 Подобрать фильм"),
                KeyboardButton(text="📺 Подобрать сериал"),
            ],
            [
                KeyboardButton(text="⭐️ Избранное"),
                KeyboardButton(text="⚙️ Настройки"),
            ],
            [KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выбери действие…",
    )

