#!/usr/bin/env python3
"""
Маппинг записей oscar_nominations (movie_id IS NULL) на Кинопоиск: поиск фильма, запись в movies, проставление movie_id и флагов.
Запуск: python scripts/map_oscar_to_kinopoisk.py
Рекомендуется после parse_oscar_wikipedia.py. Лимит Кинопоиска — учитывайте задержку между запросами.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import aiosqlite

from app.config import load_settings
from app.services.kinopoisk import get_movie_info
from app.services.oscar import update_movie_oscar_flags

DELAY_SEC = 2.0  # пауза между запросами к API


async def main() -> None:
    settings = load_settings()
    if not settings.kinopoisk_api_key:
        print("KINOPOISK_API_KEY не задан. Маппинг невозможен.")
        return

    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT id, title_from_source, year_from_source, ceremony_year FROM oscar_nominations WHERE movie_id IS NULL ORDER BY ceremony_year DESC"
        )
        rows = await cursor.fetchall()

    print(f"Записей без movie_id (уже привязанные при следующем запуске не трогаем): {len(rows)}")
    if not rows:
        print("Нечего обрабатывать. Запустите снова после parse_oscar_wikipedia.py или когда появятся новые номинации.")
        return
    mapped = 0
    for oscar_id, title_from_source, year_from_source, ceremony_year in rows:
        title = (title_from_source or "").strip()
        if not title:
            continue
        year = year_from_source
        if year is None and ceremony_year is not None:
            year = int(ceremony_year) - 1  # фильм обычно за предыдущий год
        elif year is None:
            year = None
        info = await get_movie_info(settings, title, year)
        if info is None and year is not None:
            info = await get_movie_info(settings, title, year - 1)
        if info is None and year is not None:
            info = await get_movie_info(settings, title, year + 1)
        if info is None or info.kinopoisk_id is None:
            await asyncio.sleep(DELAY_SEC)
            continue
        async with aiosqlite.connect(settings.db_path) as db:
            cur = await db.execute("SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1", (info.kinopoisk_id,))
            movie_row = await cur.fetchone()
        if not movie_row:
            await asyncio.sleep(DELAY_SEC)
            continue
        movie_id = movie_row[0]
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute("UPDATE oscar_nominations SET movie_id = ? WHERE id = ?", (movie_id, oscar_id))
            await db.commit()
        await update_movie_oscar_flags(settings, movie_id)
        mapped += 1
        print(f"  {title[:50]} -> movie_id={movie_id}")
        await asyncio.sleep(DELAY_SEC)

    print(f"Сопоставлено с Кинопоиском: {mapped}")


if __name__ == "__main__":
    asyncio.run(main())
