#!/usr/bin/env python3
"""
Добавляет несколько демо-сериалов в таблицу series для проверки флоу.
Запуск: python scripts/seed_series_demo.py
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import aiosqlite

from app.config import load_settings

DEMO_SERIES = [
    {
        "name": "Ход королевы",
        "original_name": "The Queen's Gambit",
        "year": 2020,
        "rating_kp": 8.5,
        "votes": 500000,
        "is_mini_series": 1,
        "seasons_total": 1,
        "episodes_total": 7,
        "runtime_episode_min": 62,
        "status": "ended",
        "countries": "США",
        "genres": "драма",
        "short_description": "Американка Бет Хармон с детства поражает всех мастерской игрой в шахматы.",
    },
    {
        "name": "Офис",
        "original_name": "The Office",
        "year": 2005,
        "rating_kp": 8.7,
        "votes": 400000,
        "is_mini_series": 0,
        "seasons_total": 9,
        "episodes_total": 188,
        "runtime_episode_min": 22,
        "status": "ended",
        "countries": "США",
        "genres": "комедия, ситком",
        "short_description": "Жизнь сотрудников офиса бумажной компании в Скрантоне.",
    },
    {
        "name": "Очень странные дела",
        "original_name": "Stranger Things",
        "year": 2016,
        "rating_kp": 8.6,
        "votes": 600000,
        "is_mini_series": 0,
        "seasons_total": 4,
        "episodes_total": 34,
        "runtime_episode_min": 50,
        "status": "ended",
        "countries": "США",
        "genres": "фантастика, ужасы, драма",
        "short_description": "Исчезновение мальчика в маленьком городе и появление девочки с необычными способностями.",
    },
]


async def main() -> None:
    settings = load_settings()
    names = [s["name"] for s in DEMO_SERIES]
    placeholders = ",".join("?" * len(names))
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            f"DELETE FROM series WHERE name IN ({placeholders}) AND kinopoisk_id IS NULL",
            names,
        )
        for s in DEMO_SERIES:
            await db.execute(
                """
                INSERT INTO series (
                    name, original_name, year, rating_kp, votes,
                    is_mini_series, seasons_total, episodes_total, runtime_episode_min,
                    status, countries, genres, short_description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    s["name"], s["original_name"], s["year"], s["rating_kp"], s.get("votes", 0),
                    s["is_mini_series"], s["seasons_total"], s["episodes_total"], s["runtime_episode_min"],
                    s["status"], s["countries"], s["genres"], s.get("short_description", ""),
                ),
            )
        await db.commit()
    print(f"Добавлено демо-сериалов: {len(DEMO_SERIES)}. Запустите бота и нажмите «Подобрать сериал».")


if __name__ == "__main__":
    asyncio.run(main())
