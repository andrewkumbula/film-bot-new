from __future__ import annotations

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from ..config import Settings
from ..keyboards.main_menu import main_menu_keyboard
from ..services.favorites import list_favorites_for_user, remove_favorite_for_user


def get_router(settings: Settings) -> Router:
    router = Router(name="favorites")

    @router.message(F.text == "⭐️ Избранное")
    async def show_favorites(message: Message) -> None:
        favorites = await list_favorites_for_user(settings, message.from_user.id, limit=10)

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

