from __future__ import annotations

import logging

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from ..config import Settings, load_settings
from ..keyboards.main_menu import main_menu_keyboard
from ..services.favorites import list_favorites_for_user, remove_favorite_for_user
from .flow_movie import _send_movie_card

logger = logging.getLogger(__name__)


def get_router(settings: Settings) -> Router:
    router = Router(name="favorites")

    @router.message(F.text.contains("Избранное"))
    async def show_favorites(message: Message) -> None:
        s = settings  # явная привязка из замыкания, чтобы не было UnboundLocalError при любом порядке выполнения
        if not message.from_user:
            await message.answer("Не удалось определить пользователя.")
            return
        try:
            favorites = await list_favorites_for_user(s, message.from_user.id, limit=10)
        except Exception as e:
            logger.exception("list_favorites_for_user failed: %s", e)
            err_short = str(e).strip()[:200]
            await message.answer(
                "Не удалось загрузить избранное. Попробуй позже или напиши в поддержку.\n\n"
                f"Ошибка для отладки: {err_short}",
                reply_markup=main_menu_keyboard(),
            )
            return

        if not favorites:
            await message.answer(
                "Пока в избранном пусто 😌\n\n"
                "Когда добавишь фильмы через кнопку «⭐️ В избранное», они появятся здесь.",
                reply_markup=main_menu_keyboard(),
            )
            return

        await message.answer("Твои сохранённые фильмы ⭐️:")

        for idx, rec in enumerate(favorites, start=1):
            parts = []
            title_line = f"{idx}. <b>{rec['title']}</b>"
            if rec.get("year"):
                title_line += f" ({rec['year']})"
            parts.append(title_line)
            # Возрастное ограничение и рейтинг Кинопоиска
            line_parts = []
            if rec.get("age_rating"):
                line_parts.append(f"🔞 {rec['age_rating']}+")
            if rec.get("rating_kp") is not None:
                try:
                    line_parts.append(f"⭐ Кинопоиск: {float(rec['rating_kp']):.1f}")
                except (TypeError, ValueError):
                    pass
            if line_parts:
                parts.append("  ".join(line_parts))

            if rec.get("genres"):
                parts.append("🎭 Жанры: " + ", ".join(rec["genres"]))
            if rec.get("mood_tags"):
                parts.append("🔖 Настроение: " + " ".join(rec["mood_tags"]))
            if rec.get("why"):
                parts.append("💡 Почему подходит: " + rec["why"])
            if rec.get("warnings"):
                parts.append("⚠️ Предупреждения: " + "; ".join(rec["warnings"]))
            if rec.get("similar_if_liked"):
                parts.append("🎞 Понравится, если любишь: " + ", ".join(rec["similar_if_liked"]))

            movie_id = rec.get("movie_id")
            kb = None
            if movie_id is not None:
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🗑 Удалить из избранного", callback_data=f"fav_remove:{movie_id}")],
                    ]
                )
            urls = rec.get("poster_urls") or ([rec["poster_url"]] if rec.get("poster_url") else [])
            try:
                await _send_movie_card(message, urls, "\n".join(parts), kb or InlineKeyboardMarkup(inline_keyboard=[]), s)
            except Exception:
                await message.answer("\n".join(parts), reply_markup=kb)

    @router.callback_query(F.data.startswith("fav_remove:"))
    async def remove_from_favorites(callback: CallbackQuery) -> None:
        try:
            movie_id = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer()
            return
        removed = await remove_favorite_for_user(callback.from_user.id, movie_id)
        if removed:
            await callback.answer("Удалено из избранного")
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
                await callback.message.edit_text(
                    callback.message.text + "\n\n✅ Удалено из избранного.",
                )
            except Exception:
                pass
        else:
            await callback.answer("Уже удалён или не найден")

    return router

