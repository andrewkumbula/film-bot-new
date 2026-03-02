"""
Ночной скрипт: маппинг записей movies без данных Кинопоиска на записи с данными.
Уровень 1: 100% совпадение названия + год ±1 → перенос ссылок и удаление пустой записи.
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import aiosqlite

if TYPE_CHECKING:
    from ..config import Settings

logger = logging.getLogger(__name__)

# Таблицы с FK на movies.id
MOVIE_REF_TABLES = ("favorites", "watched", "not_interested", "kinopoisk_top250")
# В этих таблицах есть UNIQUE(user_id, movie_id) — после UPDATE могут появиться дубликаты
TABLES_WITH_USER_MOVIE_UNIQUE = ("favorites", "watched", "not_interested")


def _normalize_title(title: Optional[str]) -> str:
    """Нормализация для сравнения: пробелы, регистр (для 100% совпадения)."""
    if not title or not isinstance(title, str):
        return ""
    t = title.strip()
    t = re.sub(r"\s+", " ", t)
    return t.lower()


def _year_in_range(empty_year: Optional[int], full_year: Optional[int]) -> bool:
    """Год полной записи в диапазоне [empty_year-1, empty_year+1]."""
    if empty_year is None:
        return full_year is None
    if full_year is None:
        return False
    return abs(empty_year - full_year) <= 1


async def get_empty_movies(settings: "Settings") -> List[Dict[str, Any]]:
    """Записи movies без данных Кинопоиска (нет kinopoisk_id)."""
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, title, year FROM movies WHERE kinopoisk_id IS NULL AND TRIM(COALESCE(title, '')) != ''"
        )
        rows = await cursor.fetchall()
    return [{"id": r["id"], "title": r["title"], "year": r["year"]} for r in rows]


async def get_full_movies(settings: "Settings") -> List[Dict[str, Any]]:
    """Записи movies с данными Кинопоиска (есть kinopoisk_id)."""
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, title, year FROM movies WHERE kinopoisk_id IS NOT NULL AND TRIM(COALESCE(title, '')) != ''"
        )
        rows = await cursor.fetchall()
    return [{"id": r["id"], "title": r["title"], "year": r["year"]} for r in rows]


def find_level1_match(
    empty: Dict[str, Any], full_list: List[Dict[str, Any]]
) -> Optional[int]:
    """
    Уровень 1: 100% совпадение названия (после нормализации) и год в диапазоне ±1.
    Возвращает id главной записи или None.
    """
    empty_title_norm = _normalize_title(empty.get("title"))
    empty_year = empty.get("year")
    if not empty_title_norm:
        return None
    for f in full_list:
        if _normalize_title(f.get("title")) != empty_title_norm:
            continue
        if not _year_in_range(empty_year, f.get("year")):
            continue
        return f["id"]
    return None


async def _remove_duplicate_user_movie(settings: "Settings", table: str) -> None:
    """Удаляет дубликаты (user_id, movie_id), оставляя строку с минимальным id."""
    # SQLite: оставить одну запись на (user_id, movie_id) — с минимальным id
    sql = f"""
    DELETE FROM {table}
    WHERE id NOT IN (
        SELECT MIN(id) FROM {table} GROUP BY user_id, movie_id
    )
    """
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(sql)
        await db.commit()


async def merge_movie_into(
    settings: "Settings", empty_id: int, main_id: int
) -> None:
    """
    Переносит все ссылки с пустой записи на главную, удаляет дубликаты по (user_id, movie_id),
    затем удаляет пустую запись из movies.
    """
    if empty_id == main_id:
        return
    async with aiosqlite.connect(settings.db_path, timeout=30.0) as db:
        for table in MOVIE_REF_TABLES:
            try:
                await db.execute(
                    f"UPDATE {table} SET movie_id = ? WHERE movie_id = ?",
                    (main_id, empty_id),
                )
            except Exception as e:
                logger.warning("movie_mapping_cleanup: UPDATE %s failed: %s", table, e)
                raise
        await db.commit()

    for table in TABLES_WITH_USER_MOVIE_UNIQUE:
        try:
            await _remove_duplicate_user_movie(settings, table)
        except Exception as e:
            logger.warning("movie_mapping_cleanup: dedup %s failed: %s", table, e)

    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute("DELETE FROM movies WHERE id = ?", (empty_id,))
        await db.commit()


async def run_cleanup_level1(settings: "Settings | None" = None) -> Dict[str, Any]:
    """
    Один проход уровня 1: пустые записи с 100% названием и годом ±1 маппятся на полные,
    ссылки переносятся, пустая запись удаляется.
    Возвращает {"merged": N, "errors": [...]}.
    """
    from ..config import load_settings
    if settings is None:
        settings = load_settings()

    empty_list = await get_empty_movies(settings)
    full_list = await get_full_movies(settings)
    merged = 0
    errors: List[str] = []

    for empty in empty_list:
        main_id = find_level1_match(empty, full_list)
        if main_id is None:
            continue
        try:
            await merge_movie_into(settings, empty["id"], main_id)
            merged += 1
        except Exception as e:
            errors.append(f"empty_id={empty['id']} main_id={main_id}: {e}")
            logger.exception("merge_movie_into failed: empty=%s main_id=%s", empty, main_id)

    if merged:
        logger.info("movie_mapping_cleanup level1: merged %s empty records", merged)
    if errors:
        logger.warning("movie_mapping_cleanup level1: %s errors", len(errors))
    return {"merged": merged, "errors": errors}
