"""
Сохранение данных пользователя (username, имя) для идентификации в отчётах.
"""
from __future__ import annotations

from typing import Optional

import aiosqlite

from ..config import load_settings


async def ensure_user(
    user_id: int,
    username: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
) -> None:
    """
    Создаёт или обновляет запись пользователя (при /start или при старте флоу).
    По этим данным в отчёте можно вывести имя и username вместо одного user_id.
    """
    settings = load_settings()
    username = (username or "").strip()[:128] if username else None
    first_name = (first_name or "").strip()[:128] if first_name else None
    last_name = (last_name or "").strip()[:128] if last_name else None
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, username, first_name, last_name),
            )
            await db.commit()
    except Exception:
        pass
