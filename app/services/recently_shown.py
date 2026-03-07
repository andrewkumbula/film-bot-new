"""
Недавно показанные фильмы: исключаем их из следующих N выдач (по умолчанию 20).
Одна «выдача» = один показ подборки (обычный подбор, Топ 250 или Оскар).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Set, Tuple

import aiosqlite

from ..config import Settings

logger = logging.getLogger(__name__)

RECENT_DELIVERIES_COUNT = 20


async def get_next_delivery_number(settings: Settings, user_id: int) -> int:
    """Возвращает номер следующей выдачи для пользователя (max + 1)."""
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT COALESCE(MAX(delivery_number), 0) FROM shown_recently WHERE user_id = ?",
            (user_id,),
        )
        row = await cursor.fetchone()
    return (row[0] or 0) + 1


async def get_recently_shown_ids(
    settings: Settings,
    user_id: int,
    last_n_deliveries: int = RECENT_DELIVERIES_COUNT,
) -> Tuple[Set[int], Set[int]]:
    """
    Возвращает (movie_ids, kinopoisk_ids) из последних last_n_deliveries выдач.
    Эти id нужно исключить из кандидатов.
    """
    movie_ids: Set[int] = set()
    kinopoisk_ids: Set[int] = set()
    async with aiosqlite.connect(settings.db_path) as db:
        # Номера последних N выдач
        cursor = await db.execute(
            """
            SELECT DISTINCT delivery_number FROM shown_recently
            WHERE user_id = ?
            ORDER BY delivery_number DESC
            LIMIT ?
            """,
            (user_id, last_n_deliveries),
        )
        rows = await cursor.fetchall()
        if not rows:
            return movie_ids, kinopoisk_ids
        delivery_numbers = [r[0] for r in rows]
        placeholders = ",".join("?" * len(delivery_numbers))
        cursor = await db.execute(
            f"""
            SELECT movie_id, kinopoisk_id FROM shown_recently
            WHERE user_id = ? AND delivery_number IN ({placeholders})
            """,
            (user_id, *delivery_numbers),
        )
        for row in await cursor.fetchall():
            if row[0] is not None:
                movie_ids.add(int(row[0]))
            if row[1] is not None:
                kinopoisk_ids.add(int(row[1]))
    return movie_ids, kinopoisk_ids


def _should_exclude_rec(
    rec: Dict[str, Any],
    exclude_movie_ids: Set[int],
    exclude_kinopoisk_ids: Set[int],
) -> bool:
    """True, если фильм нужно исключить (уже показывали недавно)."""
    mid = rec.get("movie_id")
    if mid is not None and int(mid) in exclude_movie_ids:
        return True
    kp = rec.get("kinopoisk_id")
    if kp is not None and int(kp) in exclude_kinopoisk_ids:
        return True
    return False


def filter_out_recently_shown(
    candidates: List[Dict[str, Any]],
    exclude_movie_ids: Set[int],
    exclude_kinopoisk_ids: Set[int],
) -> List[Dict[str, Any]]:
    """Отфильтровать кандидатов: убрать те, что в последних N выдачах."""
    return [
        c for c in candidates
        if not _should_exclude_rec(c, exclude_movie_ids, exclude_kinopoisk_ids)
    ]


async def record_shown(
    settings: Settings,
    user_id: int,
    delivery_number: int,
    items: List[Dict[str, Any]],
) -> None:
    """
    Записать показанные в этой выдаче фильмы.
    items: список dict с ключами movie_id (опционально) и kinopoisk_id (опционально).
    """
    if not items:
        return
    async with aiosqlite.connect(settings.db_path) as db:
        for rec in items:
            movie_id = rec.get("movie_id")
            kinopoisk_id = rec.get("kinopoisk_id")
            if movie_id is None and kinopoisk_id is None:
                continue
            if movie_id is not None:
                movie_id = int(movie_id)
            if kinopoisk_id is not None:
                kinopoisk_id = int(kinopoisk_id)
            await db.execute(
                """
                INSERT INTO shown_recently (user_id, delivery_number, movie_id, kinopoisk_id)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, delivery_number, movie_id, kinopoisk_id),
            )
        await db.commit()
    logger.debug("record_shown: user_id=%s delivery=%s count=%s", user_id, delivery_number, len(items))
