"""
Настройки пользователя: хранятся в таблице user_settings.
Настройка «показывать фильмы с рейтингом ниже 6.0» (выкл по умолчанию = не показываем ниже 6.0).
В БД: min_rating_filter_enabled = 1 значит «фильтровать» (не показывать ниже 6.0), 0 = показывать.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from ..config import load_settings

if TYPE_CHECKING:
    from ..config import Settings

MIN_RATING_THRESHOLD = 6.0


async def get_min_rating_filter_enabled(user_id: int, settings: "Settings | None" = None) -> bool:
    """
    True = фильтровать (не показывать фильмы с рейтингом ниже 6.0).
    Если записи нет — True по умолчанию (не показываем ниже 6.0).
    """
    if settings is None:
        settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT min_rating_filter_enabled FROM user_settings WHERE user_id = ?",
            (user_id,),
        )
        row = await cursor.fetchone()
    if row is None:
        return True  # по умолчанию фильтр включён (ниже 6.0 не показываем)
    return bool(row[0])


async def set_min_rating_filter(user_id: int, enabled: bool, settings: "Settings | None" = None) -> None:
    """enabled=True = фильтровать (не показывать ниже 6.0), False = показывать все."""
    if settings is None:
        settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            """
            INSERT INTO user_settings (user_id, min_rating_filter_enabled, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
                min_rating_filter_enabled = excluded.min_rating_filter_enabled,
                updated_at = datetime('now')
            """,
            (user_id, 1 if enabled else 0),
        )
        await db.commit()


def passes_min_rating_filter(rating_kp: float | None, min_filter_enabled: bool) -> bool:
    """
    True, если фильм проходит по настройке «рейтинг не ниже 6.0».
    Если рейтинга нет (None) — считаем, что проходит (показываем).
    """
    if not min_filter_enabled:
        return True
    if rating_kp is None:
        return True
    return rating_kp >= MIN_RATING_THRESHOLD
