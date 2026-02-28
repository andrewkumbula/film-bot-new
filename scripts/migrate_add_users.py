#!/usr/bin/env python3
"""
Миграция: создаёт таблицу users на сервере (если её ещё нет).
Запуск с корня проекта: python scripts/migrate_add_users.py
Либо: cd /path/to/project && python scripts/migrate_add_users.py
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

# Корень проекта (родитель папки scripts)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None

    env_path = PROJECT_ROOT / ".env"
    if env_path.exists() and load_dotenv:
        load_dotenv(env_path)

    db_path = os.getenv("DB_PATH", str(PROJECT_ROOT / "app_data" / "bot.db"))
    db_path = Path(db_path)
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path

    if not db_path.parent.exists():
        print(f"Папка БД не найдена: {db_path.parent}")
        return

    print(f"Подключение к БД: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.commit()
        print("Таблица users создана или уже существует.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
