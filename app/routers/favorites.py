from __future__ import annotations

from aiogram import Router, F
from aiogram.types import Message

from ..config import Settings
from ..keyboards.main_menu import main_menu_keyboard
from ..services.favorites import list_favorites_for_user


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

            await message.answer("\n".join(parts))

    return router

