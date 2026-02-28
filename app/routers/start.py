from __future__ import annotations

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message

from ..config import Settings
from ..keyboards.main_menu import main_menu_keyboard


def get_router(settings: Settings) -> Router:
    router = Router(name="start")

    @router.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        text = (
            "👋 Привет! Я бот <b>«Фильм на вечер»</b>.\n\n"
            "Помогу за пару минут подобрать крутой фильм под твоё настроение. "
            "Используй кнопки ниже, чтобы начать 🚀"
        )
        await message.answer(text, reply_markup=main_menu_keyboard())

    @router.message(F.text.endswith("Настройки"))
    async def cmd_settings(message: Message) -> None:
        text = (
            "⚙️ <b>Настройки</b>\n\n"
            "Сейчас доступно:\n"
            "• Рекомендации только с рейтингом Кинопоиска ≥ 6.0\n"
            "• Возраст и рейтинг подгружаются из API Кинопоиска (если указан <code>KINOPOISK_API_KEY</code> в .env)\n\n"
            "Дополнительные опции появятся в следующих версиях."
        )
        await message.answer(text)

    @router.message(F.text.endswith("Помощь"))
    async def cmd_help(message: Message) -> None:
        text = (
            "ℹ️ <b>Помощь</b>\n\n"
            "🎬 <b>Подобрать фильм</b> — пройди короткую анкету (настроение, жанры, длительность, возраст, с кем смотришь, чего избегать), и бот предложит 5 фильмов.\n\n"
            "⭐️ <b>Избранное</b> — здесь сохраняются фильмы, которые ты добавил кнопкой «В избранное» в рекомендациях.\n\n"
            "🔞 В каждой карточке отображаются возрастное ограничение и рейтинг Кинопоиска (если настроен API). Фильмы с рейтингом ниже 6.0 не показываются.\n\n"
            "Есть вопросы? Напиши разработчику или нажми /start для возврата в меню."
        )
        await message.answer(text)

    return router

