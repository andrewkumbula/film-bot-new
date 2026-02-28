from __future__ import annotations

from typing import Any, Dict, List, Optional

import aiosqlite

from ..config import Settings, load_settings


async def _get_movie_id(
    *,
    kinopoisk_id: Optional[int] = None,
    title: str,
    year: Optional[int] = None,
) -> Optional[int]:
    """Только поиск movie_id по kinopoisk_id или (title, year). Без создания."""
    settings = load_settings()
    title = (title or "").strip()
    if not title:
        return None
    async with aiosqlite.connect(settings.db_path) as db:
        if kinopoisk_id is not None:
            cursor = await db.execute("SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1", (kinopoisk_id,))
            row = await cursor.fetchone()
            if row:
                return row[0]
        cursor = await db.execute(
            "SELECT id FROM movies WHERE title = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
            (title, year, year),
        )
        row = await cursor.fetchone()
        return row[0] if row else None


async def is_favorite(user_id: int, rec: Dict[str, Any]) -> bool:
    """Проверяет, есть ли фильм уже в избранном у пользователя."""
    movie_id = await _get_movie_id(
        kinopoisk_id=rec.get("kinopoisk_id"),
        title=(rec.get("title") or "").strip(),
        year=rec.get("year"),
    )
    if movie_id is None:
        return False
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM favorites WHERE user_id = ? AND movie_id = ? LIMIT 1",
            (user_id, movie_id),
        )
        return (await cursor.fetchone()) is not None


async def is_watched(user_id: int, rec: Dict[str, Any]) -> bool:
    """Проверяет, есть ли фильм уже в списке «Посмотрел» у пользователя."""
    movie_id = await _get_movie_id(
        kinopoisk_id=rec.get("kinopoisk_id"),
        title=(rec.get("title") or "").strip(),
        year=rec.get("year"),
    )
    if movie_id is None:
        return False
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM watched WHERE user_id = ? AND movie_id = ? LIMIT 1",
            (user_id, movie_id),
        )
        return (await cursor.fetchone()) is not None


async def get_or_create_movie(
    *,
    kinopoisk_id: Optional[int] = None,
    title: str,
    year: Optional[int] = None,
    age_rating: Optional[str] = None,
    rating_kp: Optional[float] = None,
) -> Optional[int]:
    """
    Возвращает id записи в таблице movies. Ищет по kinopoisk_id (если есть),
    иначе по (title, year). Если не найдено — создаёт запись и возвращает id.
    """
    settings = load_settings()
    title = (title or "").strip()
    if not title:
        return None

    async with aiosqlite.connect(settings.db_path) as db:
        if kinopoisk_id is not None:
            cursor = await db.execute(
                "SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1",
                (kinopoisk_id,),
            )
            row = await cursor.fetchone()
            if row:
                return row[0]

        cursor = await db.execute(
            "SELECT id FROM movies WHERE title = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
            (title, year, year),
        )
        row = await cursor.fetchone()
        if row:
            if kinopoisk_id is not None:
                await db.execute(
                    "UPDATE movies SET kinopoisk_id = ?, age_rating = ?, rating_kp = ? WHERE id = ?",
                    (kinopoisk_id, age_rating, rating_kp, row[0]),
                )
                await db.commit()
            return row[0]

        await db.execute(
            """INSERT INTO movies (kinopoisk_id, title, year, age_rating, rating_kp)
               VALUES (?, ?, ?, ?, ?)""",
            (kinopoisk_id, title, year, age_rating, rating_kp),
        )
        cursor = await db.execute("SELECT last_insert_rowid()")
        movie_id = (await cursor.fetchone())[0]
        await db.commit()
        return movie_id


async def add_favorite_for_user(user_id: int, rec: Dict[str, Any]) -> bool:
    """
    Сохраняет рекомендацию в избранное. Использует таблицу movies (по id Кинопоиска или title+year).
    Возвращает True, если добавлено, False — если такой фильм уже в избранном.
    """
    settings = load_settings()
    title = (rec.get("title") or "").strip()
    if not title:
        return False

    kinopoisk_id = rec.get("kinopoisk_id")
    year = rec.get("year")
    age_rating = rec.get("age_rating")
    rating_kp = rec.get("rating_kp")

    movie_id = await get_or_create_movie(
        kinopoisk_id=kinopoisk_id,
        title=title,
        year=year,
        age_rating=age_rating,
        rating_kp=rating_kp,
    )
    if movie_id is None:
        return False

    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM favorites WHERE user_id = ? AND movie_id = ? LIMIT 1",
            (user_id, movie_id),
        )
        if await cursor.fetchone():
            return False

        await db.execute(
            """
            INSERT INTO favorites (user_id, movie_id, why, mood_tags, genres, warnings, similar_if_liked)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                movie_id,
                rec.get("why"),
                ",".join(rec.get("mood_tags") or []),
                ",".join(rec.get("genres") or []),
                ",".join(rec.get("warnings") or []),
                ",".join(rec.get("similar_if_liked") or []),
            ),
        )
        await db.commit()
    return True


async def list_favorites_for_user(settings: Settings, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Возвращает избранные фильмы пользователя (данные из movies + поля из favorites).
    """
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            """
            SELECT m.title, m.year, m.age_rating, m.rating_kp,
                   f.why, f.mood_tags, f.genres, f.warnings, f.similar_if_liked
            FROM favorites f
            JOIN movies m ON f.movie_id = m.id
            WHERE f.user_id = ?
            ORDER BY f.created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        rows = await cursor.fetchall()

    favorites: List[Dict[str, Any]] = []
    for row in rows:
        title, year, age_rating, rating_kp, why, mood_tags, genres, warnings, similar_if_liked = row
        favorites.append(
            {
                "title": title,
                "year": year,
                "age_rating": age_rating,
                "rating_kp": rating_kp,
                "why": why,
                "mood_tags": (mood_tags or "").split(",") if mood_tags else [],
                "genres": (genres or "").split(",") if genres else [],
                "warnings": (warnings or "").split(",") if warnings else [],
                "similar_if_liked": (similar_if_liked or "").split(",") if similar_if_liked else [],
            }
        )
    return favorites


async def add_watched_for_user(user_id: int, rec: Dict[str, Any]) -> bool:
    """
    Добавляет фильм в список «Посмотрел». Использует movies (get_or_create).
    Возвращает True, если добавлено, False — если уже был в списке.
    """
    title = (rec.get("title") or "").strip()
    if not title:
        return False

    movie_id = await get_or_create_movie(
        kinopoisk_id=rec.get("kinopoisk_id"),
        title=title,
        year=rec.get("year"),
        age_rating=rec.get("age_rating"),
        rating_kp=rec.get("rating_kp"),
    )
    if movie_id is None:
        return False

    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM watched WHERE user_id = ? AND movie_id = ? LIMIT 1",
            (user_id, movie_id),
        )
        if await cursor.fetchone():
            return False
        await db.execute(
            "INSERT INTO watched (user_id, movie_id) VALUES (?, ?)",
            (user_id, movie_id),
        )
        await db.commit()
    return True
