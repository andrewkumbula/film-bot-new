"""
Сервис «Не интересно»: сохранение фильмов, отмеченных пользователем как неинтересные,
и фильтрация рекомендаций по этому списку.
"""
from __future__ import annotations

from typing import Any, Dict, Set

import aiosqlite

from ..config import load_settings
from .favorites import get_or_create_movie
from .kinopoisk import get_movie_info


async def is_not_interested(user_id: int, rec: Dict[str, Any]) -> bool:
    """Проверяет, отметил ли пользователь фильм как «Не интересно»."""
    movie_id = await _get_movie_id_for_rec(rec)
    if movie_id is None:
        return False
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM not_interested WHERE user_id = ? AND movie_id = ? LIMIT 1",
            (user_id, movie_id),
        )
        return (await cursor.fetchone()) is not None


async def add_not_interested(user_id: int, rec: Dict[str, Any]) -> bool:
    """
    Добавляет фильм в список «Не интересно» для пользователя.
    Возвращает True, если добавлено, False — если уже было в списке.
    """
    settings = load_settings()
    title = (rec.get("title") or "").strip()
    if not title:
        return False

    kinopoisk_id = rec.get("kinopoisk_id")
    year = rec.get("year")

    if settings.kinopoisk_api_key:
        await get_movie_info(settings, title, year)

    movie_id = await get_or_create_movie(
        kinopoisk_id=kinopoisk_id,
        title=title,
        year=year,
        age_rating=rec.get("age_rating"),
        rating_kp=rec.get("rating_kp"),
    )
    if movie_id is None:
        return False

    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM not_interested WHERE user_id = ? AND movie_id = ? LIMIT 1",
            (user_id, movie_id),
        )
        if await cursor.fetchone():
            return False
        await db.execute(
            "INSERT INTO not_interested (user_id, movie_id) VALUES (?, ?)",
            (user_id, movie_id),
        )
        await db.commit()
    return True


async def add_not_interested_by_movie_id(user_id: int, movie_id: int) -> bool:
    """Добавляет в «Не интересно» по movie_id (для кнопок со старых карточек)."""
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute("SELECT 1 FROM movies WHERE id = ? LIMIT 1", (movie_id,))
        if not await cursor.fetchone():
            return False
        cursor = await db.execute(
            "SELECT 1 FROM not_interested WHERE user_id = ? AND movie_id = ? LIMIT 1",
            (user_id, movie_id),
        )
        if await cursor.fetchone():
            return False
        await db.execute(
            "INSERT INTO not_interested (user_id, movie_id) VALUES (?, ?)",
            (user_id, movie_id),
        )
        await db.commit()
    return True


async def get_not_interested_movie_ids(user_id: int) -> Set[int]:
    """Возвращает множество movie_id, отмеченных пользователем как «Не интересно»."""
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT movie_id FROM not_interested WHERE user_id = ?",
            (user_id,),
        )
        rows = await cursor.fetchall()
    return {row[0] for row in rows}


async def get_not_interested_kinopoisk_ids(user_id: int) -> Set[int]:
    """Возвращает множество kinopoisk_id из not_interested (через movies) для фильтрации Топ 250."""
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            """
            SELECT m.kinopoisk_id FROM not_interested n
            JOIN movies m ON n.movie_id = m.id
            WHERE n.user_id = ? AND m.kinopoisk_id IS NOT NULL
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()
    return {row[0] for row in rows}


async def rec_in_not_interested(
    user_id: int,
    rec: Dict[str, Any],
    *,
    ni_kinopoisk_ids: Set[int] | None = None,
    ni_movie_ids: Set[int] | None = None,
) -> bool:
    """
    Проверяет, входит ли рекомендация rec в список «Не интересно» пользователя.
    Можно передать заранее полученные множества ni_kinopoisk_ids и ni_movie_ids, чтобы не дергать БД в цикле.
    """
    if ni_kinopoisk_ids is None or ni_movie_ids is None:
        ni_kinopoisk_ids = await get_not_interested_kinopoisk_ids(user_id)
        ni_movie_ids = await get_not_interested_movie_ids(user_id)

    kp = rec.get("kinopoisk_id")
    if kp is not None and kp in ni_kinopoisk_ids:
        return True
    movie_id = await _get_movie_id_for_rec(rec)
    return movie_id is not None and movie_id in ni_movie_ids


async def _get_movie_id_for_rec(rec: Dict[str, Any]) -> int | None:
    """Возвращает movie_id по данным рекомендации (только поиск, без создания)."""
    from .favorites import _get_movie_id
    return await _get_movie_id(
        kinopoisk_id=rec.get("kinopoisk_id"),
        title=(rec.get("title") or "").strip(),
        year=rec.get("year"),
    )
