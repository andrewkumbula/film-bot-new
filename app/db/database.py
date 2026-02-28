from __future__ import annotations

import aiosqlite

from ..config import Settings


async def init_db(settings: Settings) -> None:
    """
    Создаёт файл БД и необходимые таблицы, если их ещё нет.
    """
    async with aiosqlite.connect(settings.db_path) as db:
        await db.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                year INTEGER,
                genres TEXT,
                why TEXT,
                mood_tags TEXT,
                warnings TEXT,
                similar_if_liked TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        await db.commit()

