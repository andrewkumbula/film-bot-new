"""
Ночной бэкфилл: для фильмов с полным описанием, но без краткого — ИИ генерирует short_description.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import aiosqlite

from ..config import load_settings
from ..llm.service import shorten_description_for_card

if TYPE_CHECKING:
    from ..config import Settings

logger = logging.getLogger(__name__)

# За один запуск обрабатываем не больше (лимит запросов к LLM)
BACKFILL_LIMIT_PER_RUN = 50


async def backfill_short_descriptions(settings: "Settings | None" = None, limit: int = BACKFILL_LIMIT_PER_RUN) -> int:
    """
    Для всех фильмов, у которых есть description, но нет short_description,
    вызывает ИИ и сохраняет краткое описание. Возвращает число обновлённых записей.
    """
    if settings is None:
        settings = load_settings()
    updated = 0
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            """
            SELECT id, title, description FROM movies
            WHERE description IS NOT NULL AND TRIM(description) != ''
            AND (short_description IS NULL OR TRIM(short_description) = '')
            ORDER BY id LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
    for row in rows:
        movie_id, title, description = row[0], row[1] or "", row[2] or ""
        try:
            short = await shorten_description_for_card(settings, description, title=title)
        except Exception as e:
            logger.warning("short_description for movie id=%s: %s", movie_id, e)
            continue
        if not short:
            continue
        try:
            async with aiosqlite.connect(settings.db_path) as db:
                await db.execute(
                    "UPDATE movies SET short_description = ? WHERE id = ?",
                    (short[:500], movie_id),
                )
                await db.commit()
            updated += 1
        except Exception as e:
            logger.warning("save short_description movie id=%s: %s", movie_id, e)
    if updated:
        logger.info("short_descriptions backfill: updated %s movies", updated)
    return updated
