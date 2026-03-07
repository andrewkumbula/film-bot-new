#!/usr/bin/env python3
"""
Выгрузка состояния таблицы oscar_nominations для проверки (тот же DB_PATH, что и у приложения).
Запуск из корня проекта: python scripts/dump_oscar_table.py

Помогает убедиться:
- что БД существует и к ней есть доступ;
- сколько записей в oscar_nominations и сколько с привязкой к movies;
- какой путь к БД использует конфиг (на проде должен совпадать с тем, что у бота).
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
    db_path = settings.db_path
    print(f"DB_PATH: {db_path.resolve()}")
    print(f"Файл существует: {db_path.exists()}")
    if not db_path.exists():
        print("БД не найдена. Запусти parse_oscar_wikipedia.py (и при необходимости бота один раз для создания таблиц).")
        return

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        # Общее число записей
        cur = await db.execute("SELECT COUNT(*) FROM oscar_nominations")
        total = (await cur.fetchone())[0]
        # С привязкой к фильму
        cur = await db.execute("SELECT COUNT(*) FROM oscar_nominations WHERE movie_id IS NOT NULL")
        with_movie = (await cur.fetchone())[0]
        # Без привязки
        without_movie = total - with_movie

    print(f"\noscar_nominations:")
    print(f"  всего записей:     {total}")
    print(f"  с movie_id:        {with_movie}")
    print(f"  без movie_id:      {without_movie}")

    if total == 0:
        print("\nТаблица пуста. Выполни: python scripts/parse_oscar_wikipedia.py")
        return

    # Несколько примеров (последние по году церемонии)
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, ceremony_year, title_from_source, is_winner, movie_id
            FROM oscar_nominations
            ORDER BY ceremony_year DESC
            LIMIT 10
            """
        )
        rows = await cur.fetchall()

    print("\nПример записей (последние 10 по году церемонии):")
    for r in rows:
        mid = "—" if r["movie_id"] is None else r["movie_id"]
        win = "🏆" if r["is_winner"] else "  "
        title = (r["title_from_source"] or "")[:50]
        print(f"  {win} {r['ceremony_year']} | movie_id={mid} | {title}")

    print("\nНа проде запускай из корня проекта с тем же .env, что и бот — тогда DB_PATH совпадет с приложением.")


if __name__ == "__main__":
    asyncio.run(main())
