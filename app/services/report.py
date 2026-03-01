"""
Формирование отчёта по логам флоу (flow_log) за сутки — CSV для отправки в Telegram.
Дозаполнение данных фильмов из API Кинопоиска.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime

import aiosqlite

from ..config import load_settings
from .kinopoisk import refresh_movie_from_api

logger = logging.getLogger(__name__)


async def build_flow_log_csv(hours: int = 24) -> tuple[bytes, str]:
    """
    Собирает из flow_log записи за последние hours часов, возвращает (csv_bytes, filename).
    """
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT f.user_id, u.username, u.first_name, u.last_name,
                   f.session_id, f.step, f.value, f.created_at
            FROM flow_log f
            LEFT JOIN users u ON f.user_id = u.user_id
            WHERE datetime(f.created_at) >= datetime('now', '-' || ? || ' hours')
            ORDER BY f.created_at
            """,
            (hours,),
        )
        rows = await cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "username", "first_name", "last_name", "session_id", "step", "value", "created_at"])
    for row in rows:
        writer.writerow([
            row["user_id"],
            row["username"] or "",
            row["first_name"] or "",
            row["last_name"] or "",
            row["session_id"],
            row["step"],
            row["value"] or "",
            row["created_at"] or "",
        ])

    csv_str = output.getvalue()
    # CSV в UTF-8 с BOM для корректного открытия в Excel
    bom = "\ufeff"
    csv_bytes = (bom + csv_str).encode("utf-8")
    date_label = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"flow_log_{date_label}.csv"
    return csv_bytes, filename


async def build_movies_csv() -> tuple[bytes, str]:
    """
    Выгружает все фильмы из таблицы movies в CSV.
    Возвращает (csv_bytes, filename). Поле raw_json не включается.
    """
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT id, kinopoisk_id, title, year, age_rating, rating_kp,
                   poster_url, description, genres, countries, votes, updated_at
            FROM movies
            ORDER BY title, year
            """
        )
        rows = await cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "kinopoisk_id", "title", "year", "age_rating", "rating_kp",
        "poster_url", "description", "genres", "countries", "votes", "updated_at",
    ])
    for row in rows:
        writer.writerow([
            row["id"],
            row["kinopoisk_id"] or "",
            row["title"] or "",
            row["year"] or "",
            row["age_rating"] or "",
            row["rating_kp"] if row["rating_kp"] is not None else "",
            row["poster_url"] or "",
            (row["description"] or "").replace("\n", " ").replace("\r", ""),
            row["genres"] or "",
            row["countries"] or "",
            row["votes"] or "",
            row["updated_at"] or "",
        ])

    csv_str = output.getvalue()
    bom = "\ufeff"
    csv_bytes = (bom + csv_str).encode("utf-8")
    date_label = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"movies_{date_label}.csv"
    return csv_bytes, filename


async def build_top250_csv() -> tuple[bytes, str, int]:
    """
    Выгружает таблицу Кинопоиск Топ 250 в CSV.
    Возвращает (csv_bytes, filename, count).
    """
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT kinopoisk_id, title, year, genres, rating_kp, position, age_rating, poster_url, updated_at
            FROM kinopoisk_top250
            ORDER BY position
            """
        )
        rows = await cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "kinopoisk_id", "title", "year", "genres", "rating_kp", "position", "age_rating", "poster_url", "updated_at",
    ])
    for row in rows:
        writer.writerow([
            row["kinopoisk_id"],
            row["title"] or "",
            row["year"] or "",
            row["genres"] or "",
            row["rating_kp"] if row["rating_kp"] is not None else "",
            row["position"] or "",
            row["age_rating"] or "",
            row["poster_url"] or "",
            row["updated_at"] or "",
        ])

    csv_str = output.getvalue()
    bom = "\ufeff"
    csv_bytes = (bom + csv_str).encode("utf-8")
    date_label = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"top250_{date_label}.csv"
    return csv_bytes, filename, len(rows)


async def run_movies_backfill(limit: int = 15) -> tuple[int, str]:
    """
    Дозаполняет записи в movies из API Кинопоиска: выбираются фильмы с пустым poster_url
    или description, для каждого запрашиваются данные и обновляется строка.
    Возвращает (количество обновлённых, сообщение об ошибке или "").
    Лимит — чтобы не превысить квоту API (например 200/день).
    """
    settings = load_settings()
    if not settings.kinopoisk_api_key:
        return 0, "Не задан KINOPOISK_API_KEY"

    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT id, kinopoisk_id, title, year
            FROM movies
            WHERE (poster_url IS NULL OR poster_url = '' OR description IS NULL OR description = '')
            ORDER BY id
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()

    updated = 0
    for row in rows:
        try:
            ok = await refresh_movie_from_api(
                settings,
                kinopoisk_id=row["kinopoisk_id"] if row["kinopoisk_id"] else None,
                title=row["title"],
                year=row["year"],
            )
            if ok:
                updated += 1
        except Exception as e:
            logger.warning("Backfill movie id=%s: %s", row["id"], e)
    return updated, ""
