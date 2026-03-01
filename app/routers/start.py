from __future__ import annotations

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message

from ..config import Settings
from ..keyboards.main_menu import main_menu_keyboard
from ..services.users import ensure_user


def get_router(settings: Settings) -> Router:
    router = Router(name="start")

    @router.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        if message.from_user:
            await ensure_user(
                message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
            )
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
            "🎬 <b>Подобрать фильм</b>\n"
            "При нажатии можно выбрать источник:\n"
            "• <b>Обычный подбор</b> — анкета (настроение, жанры, длительность, возраст, с кем смотришь). "
            "Можно отметить несколько пунктов «чего избегать» (жестокость, тяжёлые драмы, старое кино, грустный финал), затем нажать «Готово». ИИ предложит 5 фильмов.\n"
            "• <b>Кинопоиск Топ 250</b> — подбор только из списка Топ 250: выбери настроение, жанр (в т.ч. аниме), эпоху (новое / 90–00-е / классика). ИИ выберет 5 фильмов под твои предпочтения.\n\n"
            "⭐️ <b>Избранное</b>\n"
            "Фильмы, добавленные кнопкой «В избранное» в карточке рекомендации. У каждого фильма есть кнопка «🗑 Удалить из избранного».\n\n"
            "⚙️ <b>Настройки</b>\n"
            "Кратко о том, как формируются рекомендации и откуда берутся рейтинги.\n\n"
            "🔞 В карточках показываются возрастной рейтинг и рейтинг Кинопоиска (если настроен API). Фильмы с рейтингом ниже 6.0 не показываются. К карточкам по возможности прикрепляются постеры.\n\n"
            "Вопросы? Напиши разработчику или нажми /start для возврата в меню."
        )
        await message.answer(text)

    return router

