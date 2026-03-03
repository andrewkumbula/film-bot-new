#!/usr/bin/env python3
"""
Очищает все короткие описания (short_description) в таблице movies.
После запуска ночной бэкфилл (03:00) заново сгенерирует их с актуальным промптом.

Запуск из корня проекта:
  python scripts/clear_short_descriptions.py

Использует DB_PATH из .env (та же БД, что и бот). Подходит для продовой очистки.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiosqlite

from app.config import load_settings


async def main():
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "UPDATE movies SET short_description = NULL WHERE short_description IS NOT NULL"
        )
        await db.commit()
        n = cursor.rowcount
    print(f"Очищено коротких описаний: {n}")
    print("Ночной бэкфилл (03:00) заново сгенерирует их с текущим промптом.")


if __name__ == "__main__":
    asyncio.run(main())
