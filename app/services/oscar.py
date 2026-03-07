"""
Оскар: номинации «Лучший фильм» и др. Данные в oscar_nominations; фильмы в movies.
Подбор по типу (победитель/номинант/все) и по году (эпоха).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

from ..config import Settings

logger = logging.getLogger(__name__)

CATEGORY_BEST_PICTURE = "best_picture"


def _oscar_year_era_filter(ceremony_year: int, year_era: str) -> bool:
    """Фильтр по эпохе для Оскара (год церемонии)."""
    if year_era == "any":
        return True
    if year_era == "2020":
        return ceremony_year >= 2020
    if year_era == "2010s":
        return 2010 <= ceremony_year <= 2019
    if year_era == "2000s":
        return 2000 <= ceremony_year <= 2009
    if year_era == "1990s":
        return 1990 <= ceremony_year <= 1999
    if year_era == "1980s":
        return 1980 <= ceremony_year <= 1989
    if year_era == "1970s":
        return 1970 <= ceremony_year <= 1979
    if year_era == "1960s":
        return 1960 <= ceremony_year <= 1969
    if year_era == "classic":
        return ceremony_year < 1960
    return True


async def get_filtered_oscar(
    settings: Settings,
    type_filter: str,
    year_era: str,
    category: str = CATEGORY_BEST_PICTURE,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Возвращает список фильмов Оскара: с привязкой к movies (если есть) и без.
    Без movie_id — только title_from_source, ceremony_year, is_winner, year_from_source.
    type_filter: "winner" | "nominee" | "all"
    year_era: "2020" | "2010s" | "2000s" | ... | "classic" | "any"
    """
    rows: List[Any] = []
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute(
                """
                SELECT
                    o.id AS oscar_id,
                    o.ceremony_year,
                    o.is_winner,
                    o.title_from_source,
                    o.year_from_source,
                    m.id AS movie_id,
                    m.kinopoisk_id,
                    COALESCE(NULLIF(TRIM(m.title), ''), o.title_from_source) AS title,
                    m.year,
                    m.genres,
                    m.rating_kp,
                    m.age_rating,
                    m.poster_url,
                    m.poster_urls,
                    m.short_description
                FROM oscar_nominations o
                LEFT JOIN movies m ON o.movie_id = m.id
                WHERE o.category = ?
                ORDER BY o.ceremony_year DESC, o.is_winner DESC
                """,
                (category,),
            )
            rows = await cursor.fetchall()
        except Exception as e:
            logger.warning("get_filtered_oscar query failed: %s", e)
            return []

    result = []
    for row in rows:
        try:
            ceremony_year = int(row["ceremony_year"]) if row["ceremony_year"] is not None else None
        except (TypeError, ValueError):
            ceremony_year = None
        if ceremony_year is None:
            continue
        if not _oscar_year_era_filter(ceremony_year, year_era):
            continue
        is_winner = bool(row["is_winner"])
        if type_filter == "winner" and not is_winner:
            continue
        if type_filter == "nominee" and is_winner:
            continue
        poster_urls = None
        if row.get("poster_urls"):
            try:
                raw = row["poster_urls"]
                if raw and isinstance(raw, str):
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        poster_urls = parsed
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
        title = (row["title"] or row["title_from_source"] or "").strip()
        result.append({
            "oscar_id": row["oscar_id"],
            "ceremony_year": ceremony_year,
            "is_winner": is_winner,
            "movie_id": row["movie_id"],
            "kinopoisk_id": row["kinopoisk_id"],
            "title": title,
            "year": row["year"],
            "year_from_source": row.get("year_from_source"),
            "genres": row["genres"],
            "rating_kp": row["rating_kp"],
            "age_rating": row["age_rating"],
            "poster_url": row["poster_url"],
            "poster_urls": poster_urls,
            "short_description": (row["short_description"] or "").strip() if row.get("short_description") else None,
        })
        if len(result) >= limit:
            break
    return result


async def get_oscar_flags(
    settings: Settings,
    *,
    kinopoisk_id: Optional[int] = None,
    movie_id: Optional[int] = None,
) -> Tuple[bool, bool]:
    """
    Возвращает (nominated_oscar, won_oscar) для фильма по kinopoisk_id или movie_id.
    """
    if kinopoisk_id is None and movie_id is None:
        return False, False
    async with aiosqlite.connect(settings.db_path) as db:
        if movie_id is not None:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(nominated_oscar), 0), COALESCE(MAX(won_oscar), 0) FROM movies WHERE id = ?",
                (movie_id,),
            )
        else:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(nominated_oscar), 0), COALESCE(MAX(won_oscar), 0) FROM movies WHERE kinopoisk_id = ?",
                (kinopoisk_id,),
            )
        row = await cursor.fetchone()
    if not row:
        return False, False
    return bool(row[0]), bool(row[1])


async def get_movie_id_by_kinopoisk(settings: Settings, kinopoisk_id: int) -> Optional[int]:
    """Возвращает id записи в movies по kinopoisk_id или None."""
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute("SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1", (kinopoisk_id,))
        row = await cursor.fetchone()
    return row[0] if row else None


async def link_oscar_to_movie(settings: Settings, oscar_id: int, movie_id: int) -> None:
    """Привязывает номинацию к фильму (после успешного поиска в Кинопоиске) и обновляет флаги в movies."""
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            "UPDATE oscar_nominations SET movie_id = ? WHERE id = ?",
            (movie_id, oscar_id),
        )
        await db.commit()
    await update_movie_oscar_flags(settings, movie_id)


async def update_movie_oscar_flags(settings: Settings, movie_id: int) -> None:
    """Обновляет в movies флаги nominated_oscar и won_oscar по данным из oscar_nominations."""
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT COUNT(*), SUM(CASE WHEN is_winner = 1 THEN 1 ELSE 0 END) FROM oscar_nominations WHERE movie_id = ?",
            (movie_id,),
        )
        row = await cursor.fetchone()
    if not row or (row[0] or 0) == 0:
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute(
                "UPDATE movies SET nominated_oscar = 0, won_oscar = 0 WHERE id = ?",
                (movie_id,),
            )
            await db.commit()
        return
    nominated = 1
    won = 1 if (row[1] and int(row[1]) > 0) else 0
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            "UPDATE movies SET nominated_oscar = ?, won_oscar = ? WHERE id = ?",
            (nominated, won, movie_id),
        )
        await db.commit()


async def get_oscar_count(settings: Settings, with_movie_only: bool = True) -> int:
    """Число записей в oscar_nominations (опционально только с movie_id)."""
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            if with_movie_only:
                cursor = await db.execute(
                    "SELECT COUNT(*) FROM oscar_nominations WHERE movie_id IS NOT NULL"
                )
            else:
                cursor = await db.execute("SELECT COUNT(*) FROM oscar_nominations")
            row = await cursor.fetchone()
            return row[0] if row else 0
    except Exception:
        return 0
