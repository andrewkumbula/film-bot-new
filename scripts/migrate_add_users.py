#!/usr/bin/env python3
"""
Начальная схема БД: создаёт все таблицы (movies, favorites, watched, users, flow_log, kinopoisk_top250),
если их ещё нет. Решает ошибки вида "no such table: flow_log".
После изменений схемы дополнительно запустите: python scripts/run_migrations.py
Запуск с корня: python scripts/migrate_add_users.py
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

# Корень проекта (родитель папки scripts)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS movies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kinopoisk_id INTEGER UNIQUE,
    title TEXT NOT NULL,
    year INTEGER,
    age_rating TEXT,
    rating_kp REAL,
    poster_url TEXT,
    description TEXT,
    genres TEXT,
    countries TEXT,
    votes INTEGER,
    raw_json TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_movies_kinopoisk ON movies(kinopoisk_id);
CREATE INDEX IF NOT EXISTS idx_movies_title_year ON movies(title, year);

CREATE TABLE IF NOT EXISTS favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    why TEXT,
    mood_tags TEXT,
    genres TEXT,
    warnings TEXT,
    similar_if_liked TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    UNIQUE(user_id, movie_id)
);
CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id);

CREATE TABLE IF NOT EXISTS watched (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    UNIQUE(user_id, movie_id)
);
CREATE INDEX IF NOT EXISTS idx_watched_user ON watched(user_id);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flow_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    step TEXT NOT NULL,
    value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_flow_log_user ON flow_log(user_id);
CREATE INDEX IF NOT EXISTS idx_flow_log_step ON flow_log(step);
CREATE INDEX IF NOT EXISTS idx_flow_log_session ON flow_log(session_id);

CREATE TABLE IF NOT EXISTS kinopoisk_top250 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER REFERENCES movies(id),
    kinopoisk_id INTEGER UNIQUE NOT NULL,
    title TEXT NOT NULL,
    year INTEGER,
    genres TEXT,
    rating_kp REAL,
    position INTEGER,
    age_rating TEXT,
    poster_url TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_top250_kinopoisk ON kinopoisk_top250(kinopoisk_id);
CREATE INDEX IF NOT EXISTS idx_top250_year ON kinopoisk_top250(year);
"""


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

    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Подключение к БД: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
        print("Таблицы созданы или уже существуют (movies, favorites, watched, users, flow_log, kinopoisk_top250).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
