#!/usr/bin/env python3
"""
Импорт Топ 250 из JSON-файла (созданного export_top250_for_prod.py) в прод-БД.
Запуск на проде: python scripts/import_top250_to_prod.py [путь к top250_export.json]
По умолчанию ищет top250_export.json в корне проекта.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    if (PROJECT_ROOT / ".env").exists():
        load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

DB_PATH = Path(os.getenv("DB_PATH", str(PROJECT_ROOT / "app_data" / "bot.db")))
if not DB_PATH.is_absolute():
    DB_PATH = PROJECT_ROOT / DB_PATH
DEFAULT_IMPORT_PATH = PROJECT_ROOT / "top250_export.json"


def main() -> int:
    import_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_IMPORT_PATH
    if not import_path.is_absolute():
        import_path = PROJECT_ROOT / import_path
    if not import_path.exists():
        print(f"Файл не найден: {import_path}")
        return 1
    if not DB_PATH.parent.exists():
        print(f"Папка БД не найдена: {DB_PATH.parent}")
        return 1

    with open(import_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    movies = data.get("movies") or []
    top250 = data.get("top250") or []

    conn = sqlite3.connect(DB_PATH)
    try:
        for m in movies:
            kp = m.get("kinopoisk_id")
            if kp is None:
                continue
            conn.execute(
                """INSERT INTO movies (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, description, genres, countries, votes, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(kinopoisk_id) DO UPDATE SET
                     title=excluded.title, year=excluded.year, age_rating=excluded.age_rating, rating_kp=excluded.rating_kp,
                     poster_url=excluded.poster_url, description=excluded.description, genres=excluded.genres,
                     countries=excluded.countries, votes=excluded.votes, updated_at=CURRENT_TIMESTAMP""",
                (
                    kp,
                    m.get("title") or "",
                    m.get("year"),
                    m.get("age_rating"),
                    m.get("rating_kp"),
                    m.get("poster_url"),
                    m.get("description"),
                    m.get("genres"),
                    m.get("countries"),
                    m.get("votes"),
                ),
            )
        conn.commit()

        cur = conn.execute("PRAGMA table_info(kinopoisk_top250)")
        columns = [r[1] for r in cur.fetchall()]
        has_movie_id = "movie_id" in columns

        conn.execute("DELETE FROM kinopoisk_top250")
        for item in top250:
            kp = item.get("kinopoisk_id")
            pos = item.get("position")
            if kp is None or pos is None:
                continue
            cur = conn.execute(
                "SELECT id, title, year, genres, rating_kp, age_rating, poster_url FROM movies WHERE kinopoisk_id = ? LIMIT 1",
                (kp,),
            )
            row = cur.fetchone()
            if not row:
                continue
            movie_id, title, year, genres, rating_kp, age_rating, poster_url = row
            if has_movie_id:
                conn.execute(
                    """INSERT INTO kinopoisk_top250 (movie_id, kinopoisk_id, title, year, genres, rating_kp, position, age_rating, poster_url, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                    (movie_id, kp, title or "", year, genres or "", rating_kp, pos, age_rating, poster_url),
                )
            else:
                conn.execute(
                    """INSERT INTO kinopoisk_top250 (kinopoisk_id, title, year, genres, rating_kp, position, age_rating, poster_url, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                    (kp, title or "", year, genres or "", rating_kp, pos, age_rating, poster_url),
                )
        conn.commit()
        print(f"Импорт: {len(movies)} фильмов, {len(top250)} позиций Топ 250 в {DB_PATH}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
