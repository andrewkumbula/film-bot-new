#!/usr/bin/env python3
"""
Экспорт Топ 250 и связанных записей movies в JSON-файл для переноса на прод.
Запуск на деве (или откуда есть заполненный Топ 250): python scripts/export_top250_for_prod.py
Создаёт файл top250_export.json в корне проекта (или путь из TOP250_EXPORT_PATH).
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
OUT_PATH = Path(os.getenv("TOP250_EXPORT_PATH", str(PROJECT_ROOT / "top250_export.json")))
if not OUT_PATH.is_absolute():
    OUT_PATH = PROJECT_ROOT / OUT_PATH


def main() -> int:
    if not DB_PATH.exists():
        print(f"БД не найдена: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("PRAGMA table_info(kinopoisk_top250)")
    columns = [r[1] for r in cur.fetchall()]
    if "movie_id" not in columns:
        cur = conn.execute(
            """SELECT t.kinopoisk_id, t.title, t.year, t.genres, t.rating_kp, t.position, t.age_rating, t.poster_url
               FROM kinopoisk_top250 t ORDER BY t.position"""
        )
        rows = cur.fetchall()
        movies_by_kp = {}
        top250 = []
        for row in rows:
            r = dict(row)
            kp = r["kinopoisk_id"]
            top250.append({"kinopoisk_id": kp, "position": r["position"]})
            if kp not in movies_by_kp:
                movies_by_kp[kp] = {
                    "kinopoisk_id": kp,
                    "title": r.get("title") or "",
                    "year": r.get("year"),
                    "age_rating": r.get("age_rating"),
                    "rating_kp": r.get("rating_kp"),
                    "poster_url": r.get("poster_url"),
                    "genres": r.get("genres"),
                }
        movies = list(movies_by_kp.values())
    else:
        cur = conn.execute(
            """SELECT t.kinopoisk_id, t.position, t.title, t.year, t.genres, t.rating_kp, t.age_rating, t.poster_url,
                      m.id as _m_id, m.kinopoisk_id as _m_kp, m.title as _m_title, m.year as _m_year, m.age_rating as _m_age_rating,
                      m.rating_kp as _m_rating_kp, m.poster_url as _m_poster_url, m.description as _m_description,
                      m.genres as _m_genres, m.countries as _m_countries, m.votes as _m_votes
               FROM kinopoisk_top250 t
               JOIN movies m ON t.movie_id = m.id
               ORDER BY t.position"""
        )
        rows = cur.fetchall()
        movies_by_kp = {}
        top250 = []
        for row in rows:
            r = dict(row)
            kp = r["kinopoisk_id"]
            top250.append({"kinopoisk_id": kp, "position": r["position"]})
            if kp not in movies_by_kp:
                movies_by_kp[kp] = {
                    "kinopoisk_id": kp,
                    "title": r.get("_m_title") or r.get("title") or "",
                    "year": r.get("_m_year") if r.get("_m_year") is not None else r.get("year"),
                    "age_rating": r.get("_m_age_rating") or r.get("age_rating"),
                    "rating_kp": r.get("_m_rating_kp") if r.get("_m_rating_kp") is not None else r.get("rating_kp"),
                    "poster_url": r.get("_m_poster_url") or r.get("poster_url"),
                    "description": r.get("_m_description"),
                    "genres": r.get("_m_genres") or r.get("genres"),
                    "countries": r.get("_m_countries"),
                    "votes": r.get("_m_votes"),
                }
        movies = list(movies_by_kp.values())
    conn.close()

    out = {"movies": movies, "top250": top250}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Экспорт: {len(movies)} фильмов, {len(top250)} позиций Топ 250 → {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
