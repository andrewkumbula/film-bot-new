#!/usr/bin/env python3
"""
Запуск всех миграций БД в порядке номеров.
Использует DB_PATH из окружения или app_data/bot.db относительно корня проекта.
Запуск из корня: python scripts/run_migrations.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Корень проекта
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Загрузка .env
try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "app_data" / "bot.db"))
DB_PATH = Path(DB_PATH)
if not DB_PATH.is_absolute():
    DB_PATH = PROJECT_ROOT / DB_PATH


def main() -> int:
    if not DB_PATH.parent.exists():
        print(f"Папка БД не найдена: {DB_PATH.parent}")
        return 1

    import sqlite3
    from scripts.migrations import (
        m001_movies_extra_columns,
        m002_movies_unique_title_year,
        m003_top250_poster_url,
        m004_top250_movie_id,
    )

    migrations = [
        ("001_movies_extra_columns", m001_movies_extra_columns.run),
        ("002_movies_unique_title_year", m002_movies_unique_title_year.run),
        ("003_top250_poster_url", m003_top250_poster_url.run),
        ("004_top250_movie_id", m004_top250_movie_id.run),
    ]

    print(f"БД: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        for name, run_fn in migrations:
            print(f"  Выполняю {name}...")
            run_fn(conn)
        print("Миграции выполнены.")
    except Exception as e:
        print(f"Ошибка: {e}")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
