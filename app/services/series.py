"""
Сериалы: подбор по времени, формату, настроению, ограничениям.
Данные из таблицы series; исключаем series_watched.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Set

import aiosqlite

from ..config import Settings

logger = logging.getLogger(__name__)

# Значения фильтров (совпадают с callback_data в клавиатурах)
TIME_ANY = "any"
FORMAT_ANY = "any"
MOOD_ANY = "any"


def _match_time(series: Dict[str, Any], time_slot: str) -> bool:
    """Подходит ли сериал под выбранное время (1–2 ч, 2–4 ч, несколько вечеров)."""
    if time_slot == TIME_ANY:
        return True
    runtime = series.get("runtime_episode_min") or 0
    episodes = series.get("episodes_total") or 0
    total_min = runtime * episodes
    if time_slot == "1-2h":
        return 30 <= total_min <= 150  # 1–2.5 часа суммарно
    if time_slot == "2-4h":
        return 120 <= total_min <= 300
    if time_slot == "several":
        return total_min > 240  # несколько вечеров
    return True


def _match_format(series: Dict[str, Any], format_type: str) -> bool:
    """Подходит ли под формат: мини, 1 сезон, несколько сезонов."""
    if format_type == FORMAT_ANY:
        return True
    is_mini = series.get("is_mini_series") or 0
    seasons = series.get("seasons_total") or 0
    if format_type == "mini":
        return is_mini == 1
    if format_type == "one_season":
        return seasons == 1
    if format_type == "several_seasons":
        return seasons and seasons > 1
    return True


def _match_mood(series: Dict[str, Any], mood: str) -> bool:
    """По жанрам и тегам определяем настроение. Упрощённо — по genres."""
    if mood == MOOD_ANY:
        return True
    genres = (series.get("genres") or "").lower()
    mood_map = {
        "light": ["комедия", "мелодрама", "семейный", "мультфильм"],
        "tense": ["триллер", "детектив", "криминал", "драма"],
        "funny": ["комедия", "ситком"],
        "atmospheric": ["драма", "мелодрама", "фэнтези", "фантастика"],
        "dark": ["триллер", "ужасы", "драма", "криминал"],
        "romance": ["мелодрама", "романтика", "драма"],
        "surprise": ["фантастика", "фэнтези", "триллер", "детектив"],
    }
    keywords = mood_map.get(mood, [])
    return any(k in genres for k in keywords)


def _match_restrictions(series: Dict[str, Any], restrictions: List[str]) -> bool:
    """Учитывает ограничения: только завершённые, без ужасов, рейтинг 7+ и т.д."""
    if not restrictions:
        return True
    for r in restrictions:
        if r == "completed_only":
            if (series.get("status") or "").lower() not in ("ended", "завершён", "completed"):
                return False
        if r == "no_horror":
            if "ужасы" in (series.get("genres") or "").lower() or "horror" in (series.get("genres") or "").lower():
                return False
        if r == "no_heavy_drama":
            genres = (series.get("genres") or "").lower()
            if "драма" in genres and "криминал" in genres:
                return False
        if r == "rating_7_plus":
            rating = series.get("rating_kp")
            if rating is not None and float(rating) < 7.0:
                return False
        if r == "no_russian":
            countries = (series.get("countries") or "").lower()
            if "россия" in countries or "ссср" in countries or "russia" in countries:
                return False
    return True


async def get_series_watched_ids(settings: Settings, user_id: int) -> Set[int]:
    """Множество series_id, которые пользователь отметил «Уже смотрел»."""
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT series_id FROM series_watched WHERE user_id = ?",
            (user_id,),
        )
        rows = await cursor.fetchall()
    return {r[0] for r in rows}


async def get_series_favorite_ids(settings: Settings, user_id: int) -> Set[int]:
    """Множество series_id в избранном."""
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT series_id FROM series_favorites WHERE user_id = ?",
            (user_id,),
        )
        rows = await cursor.fetchall()
    return {r[0] for r in rows}


async def is_series_in_favorites(settings: Settings, user_id: int, series_id: int) -> bool:
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM series_favorites WHERE user_id = ? AND series_id = ? LIMIT 1",
            (user_id, series_id),
        )
        row = await cursor.fetchone()
    return row is not None


async def add_series_favorite(settings: Settings, user_id: int, series_id: int) -> bool:
    """Добавить в избранное. Возвращает True если добавлено, False если уже было."""
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO series_favorites (user_id, series_id) VALUES (?, ?)",
                (user_id, series_id),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("add_series_favorite: %s", e)
        return False


async def remove_series_favorite(settings: Settings, user_id: int, series_id: int) -> bool:
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute(
                "DELETE FROM series_favorites WHERE user_id = ? AND series_id = ?",
                (user_id, series_id),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("remove_series_favorite: %s", e)
        return False


async def is_series_watched(settings: Settings, user_id: int, series_id: int) -> bool:
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT 1 FROM series_watched WHERE user_id = ? AND series_id = ? LIMIT 1",
            (user_id, series_id),
        )
        row = await cursor.fetchone()
    return row is not None


async def add_series_watched(settings: Settings, user_id: int, series_id: int) -> bool:
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO series_watched (user_id, series_id) VALUES (?, ?)",
                (user_id, series_id),
            )
            await db.commit()
        return True
    except Exception as e:
        logger.warning("add_series_watched: %s", e)
        return False


async def get_filtered_series(
    settings: Settings,
    user_id: int,
    time_slot: str = TIME_ANY,
    format_type: str = FORMAT_ANY,
    mood: str = MOOD_ANY,
    restrictions: Optional[List[str]] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Возвращает сериалы из БД, отфильтрованные по параметрам подбора.
    Исключает сериалы из «Уже смотрел».
    """
    restrictions = restrictions or []
    watched_ids = await get_series_watched_ids(settings, user_id)
    rows: List[Any] = []
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute(
                """
                SELECT id, kinopoisk_id, name, original_name, year, rating_kp, votes,
                       poster_url, poster_urls, description, short_description,
                       is_mini_series, seasons_total, episodes_total, runtime_episode_min,
                       status, countries, genres
                FROM series ORDER BY COALESCE(rating_kp, 0) DESC, COALESCE(year, 0) DESC
                """
            )
            rows = await cursor.fetchall()
        except Exception as e:
            logger.warning("get_filtered_series query failed: %s", e)
            return []

    result = []
    for row in rows:
        sid = row["id"]
        if sid in watched_ids:
            continue
        item = {
            "id": sid,
            "kinopoisk_id": row["kinopoisk_id"],
            "name": (row["name"] or "").strip(),
            "original_name": (row["original_name"] or "").strip(),
            "year": row["year"],
            "rating_kp": row["rating_kp"],
            "votes": row["votes"],
            "poster_url": row["poster_url"],
            "poster_urls": _parse_poster_urls(row["poster_urls"]),
            "description": (row["description"] or "").strip(),
            "short_description": (row["short_description"] or "").strip(),
            "is_mini_series": row["is_mini_series"] or 0,
            "seasons_total": row["seasons_total"],
            "episodes_total": row["episodes_total"],
            "runtime_episode_min": row["runtime_episode_min"],
            "status": (row["status"] or "").strip(),
            "countries": (row["countries"] or "").strip(),
            "genres": (row["genres"] or "").strip(),
        }
        if not _match_time(item, time_slot):
            continue
        if not _match_format(item, format_type):
            continue
        if not _match_mood(item, mood):
            continue
        if not _match_restrictions(item, restrictions):
            continue
        result.append(item)
        if len(result) >= limit:
            break
    return result


def _parse_poster_urls(raw: Any) -> Optional[List[str]]:
    if not raw:
        return None
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else None
        except (json.JSONDecodeError, TypeError):
            return None
    return None


async def get_series_by_id(settings: Settings, series_id: int) -> Optional[Dict[str, Any]]:
    """Один сериал по id."""
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls, "
            "description, short_description, is_mini_series, seasons_total, episodes_total, runtime_episode_min, "
            "status, countries, genres FROM series WHERE id = ? LIMIT 1",
            (series_id,),
        )
        row = await cursor.fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "kinopoisk_id": row["kinopoisk_id"],
        "name": (row["name"] or "").strip(),
        "original_name": (row["original_name"] or "").strip(),
        "year": row["year"],
        "rating_kp": row["rating_kp"],
        "votes": row["votes"],
        "poster_url": row["poster_url"],
        "poster_urls": _parse_poster_urls(row["poster_urls"]),
        "description": (row["description"] or "").strip(),
        "short_description": (row["short_description"] or "").strip(),
        "is_mini_series": row["is_mini_series"] or 0,
        "seasons_total": row["seasons_total"],
        "episodes_total": row["episodes_total"],
        "runtime_episode_min": row["runtime_episode_min"],
        "status": (row["status"] or "").strip(),
        "countries": (row["countries"] or "").strip(),
        "genres": (row["genres"] or "").strip(),
    }


async def get_series_count(settings: Settings) -> int:
    """Число сериалов в БД."""
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM series")
        row = await cursor.fetchone()
    return row[0] if row else 0
