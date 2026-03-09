from __future__ import annotations

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ..config import Settings
from ..keyboards.main_menu import main_menu_keyboard
from ..services.user_settings import get_min_rating_filter_enabled, set_min_rating_filter
from ..services.users import ensure_user


def _settings_message_text(show_low_rated: bool) -> str:
    """show_low_rated = показывать фильмы с рейтингом ниже 6.0 (обратно фильтру в БД)."""
    line = "✅ Вкл" if show_low_rated else "❌ Выкл"
    return (
        "⚙️ <b>Настройки</b>\n\n"
        "Показывать фильмы с рейтингом Кинопоиска ниже 6.0 — " + line + "\n\n"
        "По умолчанию выключено (ниже 6.0 не показываем). Если рейтинга нет — фильм показывается."
    )


def _settings_keyboard(show_low_rated: bool) -> InlineKeyboardMarkup:
    label = "✅ Показывать ниже 6.0 (вкл)" if show_low_rated else "❌ Показывать ниже 6.0 (выкл)"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data="settings:min_rating_toggle")],
        ]
    )


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
            "Помогу за пару минут подобрать фильм или сериал под настроение. "
            "Используй кнопки ниже 🚀"
        )
        await message.answer(text, reply_markup=main_menu_keyboard())

    @router.message(F.text.endswith("Настройки"))
    async def cmd_settings(message: Message) -> None:
        if not message.from_user:
            return
        user_id = message.from_user.id
        filter_on = await get_min_rating_filter_enabled(user_id, settings)
        show_low_rated = not filter_on  # обратно: фильтр вкл = не показываем ниже 6
        text = _settings_message_text(show_low_rated)
        await message.answer(text, reply_markup=_settings_keyboard(show_low_rated))

    @router.callback_query(F.data == "settings:min_rating_toggle")
    async def settings_min_rating_toggle(callback: CallbackQuery) -> None:
        if not callback.from_user:
            await callback.answer()
            return
        await callback.answer()
        user_id = callback.from_user.id
        filter_on = await get_min_rating_filter_enabled(user_id, settings)
        show_low_rated = not filter_on
        # переключаем: новый show_low_rated = не текущий
        new_show_low_rated = not show_low_rated
        await set_min_rating_filter(user_id, not new_show_low_rated, settings)
        text = _settings_message_text(new_show_low_rated)
        try:
            await callback.message.edit_text(text, reply_markup=_settings_keyboard(new_show_low_rated))
        except Exception:
            pass

    @router.message(F.text.endswith("Помощь"))
    async def cmd_help(message: Message) -> None:
        text = (
            "ℹ️ <b>Помощь</b>\n\n"
            "🎬 <b>Подобрать фильм</b>\n"
            "При нажатии можно выбрать источник:\n"
            "• <b>Обычный подбор</b> — анкета (настроение, жанры, возраст, с кем смотришь). "
            "Можно отметить несколько пунктов «чего избегать» (жестокость, тяжёлые драмы, старое кино, грустный финал), затем нажать «Готово». ИИ предложит 5 фильмов.\n"
            "• <b>Кинопоиск Топ 250</b> — подбор только из списка Топ 250: выбери настроение, жанр (в т.ч. аниме), эпоху (новое / 90–00-е / классика). ИИ выберет 5 фильмов под твои предпочтения.\n\n"
            "📺 <b>Подобрать сериал</b>\n"
            "Укажи время (1–2 ч / 2–4 ч / несколько вечеров), формат (мини-сериал / 1 сезон / несколько), настроение и ограничения — бот покажет 3 сериала.\n\n"
            "⭐️ <b>Избранное</b>\n"
            "Фильмы, добавленные кнопкой «В избранное» в карточке рекомендации. У каждого фильма есть кнопка «🗑 Удалить из избранного».\n\n"
            "⚙️ <b>Настройки</b>\n"
            "В Настройках можно включить «показывать фильмы с рейтингом ниже 6.0» (по умолчанию выключено). Если рейтинга нет — фильм показывается.\n\n"
            "🔞 В карточках показываются возрастной рейтинг и рейтинг Кинопоиска (если настроен API). К карточкам по возможности прикрепляются постеры.\n\n"
            "Вопросы? Напиши разработчику или нажми /start для возврата в меню."
        )
        await message.answer(text)

    return router

