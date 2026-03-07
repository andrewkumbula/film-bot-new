#!/usr/bin/env python3
"""
Добавляет таблицу oscar_nominations (если нет) и колонку year_from_source.
Запуск на проде, если бот ещё не перезапускали после обновления кода:
  python scripts/ensure_oscar_schema.py
Использует тот же DB_PATH, что и бот (.env).
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import aiosqlite

from app.config import load_settings


async def main() -> None:
    settings = load_settings()
    print(f"DB: {settings.db_path}")
    async with aiosqlite.connect(settings.db_path) as db:
        # Таблица (как в database.py)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS oscar_nominations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL DEFAULT 'best_picture',
                ceremony_year INTEGER NOT NULL,
                ceremony_label TEXT NOT NULL,
                title_from_source TEXT NOT NULL,
                is_winner INTEGER NOT NULL DEFAULT 0,
                movie_id INTEGER REFERENCES movies(id),
                year_from_source INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_oscar_nom_movie ON oscar_nominations(movie_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_oscar_nom_year ON oscar_nominations(ceremony_year)")
        # Колонка year_from_source
        cursor = await db.execute("PRAGMA table_info(oscar_nominations)")
        rows = await cursor.fetchall()
        columns = [r[1] for r in rows] if rows else []
        if "year_from_source" not in columns:
            await db.execute("ALTER TABLE oscar_nominations ADD COLUMN year_from_source INTEGER")
            print("Добавлена колонка year_from_source.")
        else:
            print("Колонка year_from_source уже есть.")
        await db.commit()
    print("Готово.")


if __name__ == "__main__":
    asyncio.run(main())
