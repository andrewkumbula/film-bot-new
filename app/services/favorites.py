from __future__ import annotations

from typing import Any, Dict, List

import aiosqlite

from ..config import BASE_DIR
from ..config import Settings, load_settings


async def add_favorite_for_user(user_id: int, rec: Dict[str, Any]) -> None:
    """
    Сохраняет рекомендацию в таблицу избранного.
    Ожидает dict в формате Recommendation.model_dump().
    """
    # Для простоты получаем настройки здесь; можно прокидывать settings из контекста,
    # но это не критично для учебного проекта.
    settings = load_settings()

    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            """
            INSERT INTO favorites (user_id, title, year, genres, why, mood_tags, warnings, similar_if_liked)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                rec.get("title"),
                rec.get("year"),
                ",".join(rec.get("genres") or []),
                rec.get("why"),
                ",".join(rec.get("mood_tags") or []),
                ",".join(rec.get("warnings") or []),
                ",".join(rec.get("similar_if_liked") or []),
            ),
        )
        await db.commit()


async def list_favorites_for_user(settings: Settings, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Возвращает последние сохранённые избранные фильмы для пользователя.
    """
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            """
            SELECT title, year, genres, why, mood_tags, warnings, similar_if_liked
            FROM favorites
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        rows = await cursor.fetchall()

    favorites: List[Dict[str, Any]] = []
    for row in rows:
        title, year, genres, why, mood_tags, warnings, similar_if_liked = row
        favorites.append(
            {
                "title": title,
                "year": year,
                "genres": (genres or "").split(",") if genres else [],
                "why": why,
                "mood_tags": (mood_tags or "").split(",") if mood_tags else [],
                "warnings": (warnings or "").split(",") if warnings else [],
                "similar_if_liked": (similar_if_liked or "").split(",") if similar_if_liked else [],
            }
        )

    return favorites

