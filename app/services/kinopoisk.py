"""
Сервис для получения данных о фильмах/сериалах через API Кинопоиска (poiskkino.dev).
Сначала проверяем таблицу movies; в API ходим только если фильма ещё нет. Все данные сохраняем.
Токен можно получить в Telegram-боте @poiskkinodev_bot.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiosqlite
import httpx

from ..config import Settings


@dataclass
class KinopoiskMovieInfo:
    """Данные по фильму/сериалу из API Кинопоиска или из кэша (movies)."""
    kinopoisk_id: Optional[int] = None
    age_rating: Optional[str] = None
    rating_kp: Optional[float] = None
    votes: Optional[int] = None
    poster_url: Optional[str] = None


async def get_movie_from_db(
    settings: Settings,
    *,
    kinopoisk_id: Optional[int] = None,
    title: Optional[str] = None,
    year: Optional[int] = None,
) -> Optional[KinopoiskMovieInfo]:
    """
    Возвращает данные из таблицы movies, если запись есть. Поиск по kinopoisk_id или по точному совпадению (title, year).
    В API не ходит. По названию — только точное совпадение, так что по запросу «Летающие ножи» не вернётся «Летающие звери».
    """
    title = (title or "").strip() if title else None
    if not title and kinopoisk_id is None:
        return None

    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        if kinopoisk_id is not None:
            cursor = await db.execute(
                "SELECT kinopoisk_id, age_rating, rating_kp, votes, poster_url FROM movies WHERE kinopoisk_id = ? LIMIT 1",
                (kinopoisk_id,),
            )
        else:
            cursor = await db.execute(
                "SELECT kinopoisk_id, age_rating, rating_kp, votes, poster_url FROM movies WHERE title = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (title, year, year),
            )
        row = await cursor.fetchone()
    if not row:
        return None
    return KinopoiskMovieInfo(
        kinopoisk_id=row["kinopoisk_id"],
        age_rating=row["age_rating"],
        rating_kp=row["rating_kp"],
        votes=row["votes"],
        poster_url=row["poster_url"] or None,
    )


def _parse_poster(doc: Dict[str, Any]) -> Optional[str]:
    poster = doc.get("poster")
    if isinstance(poster, dict):
        return poster.get("url") or poster.get("previewUrl") or poster.get("preview") or None
    if isinstance(poster, str) and poster.strip().startswith("http"):
        return poster.strip()
    return None


def _parse_doc_to_row(doc: Dict[str, Any]) -> tuple:
    """Из ответа API собирает кортеж для INSERT/UPDATE movies."""
    kinopoisk_id = doc.get("id")
    if kinopoisk_id is not None:
        try:
            kinopoisk_id = int(kinopoisk_id)
        except (TypeError, ValueError):
            kinopoisk_id = None
    name = (doc.get("name") or doc.get("alternativeName") or "").strip() or (str(kinopoisk_id) if kinopoisk_id else "")
    year = doc.get("year")
    if year is not None:
        try:
            year = int(year)
        except (TypeError, ValueError):
            year = None
    raw_age = doc.get("ageRating")
    age_rating = str(raw_age).strip() if raw_age is not None else None
    if not age_rating and doc.get("ratingMpaa"):
        age_rating = _mpaa_to_age(str(doc.get("ratingMpaa")))
    r = doc.get("rating")
    rating_kp = None
    if r is not None:
        if isinstance(r, (int, float)):
            rating_kp = float(r)
        elif isinstance(r, dict) and r.get("kp") is not None:
            try:
                rating_kp = float(r["kp"])
            except (TypeError, ValueError):
                pass
    votes = doc.get("votes")
    if votes is not None:
        try:
            votes = int(votes)
        except (TypeError, ValueError):
            votes = None
    poster_url = _parse_poster(doc)
    description = (doc.get("description") or "").strip() or None
    if description and len(description) > 5000:
        description = description[:5000]
    genres_raw = doc.get("genres") or []
    genre_names = [g.get("name") or "" for g in genres_raw if isinstance(g, dict)]
    genres = ",".join(g.strip() for g in genre_names if g.strip()) or None
    countries_raw = doc.get("countries") or []
    country_names = [c.get("name") or "" for c in countries_raw if isinstance(c, dict)]
    countries = ",".join(c.strip() for c in country_names if c.strip()) or None
    raw_json = json.dumps(doc, ensure_ascii=False)[:100000] if doc else None  # лимит размера
    return (kinopoisk_id, name, year, age_rating, rating_kp, poster_url, description, genres, countries, votes, raw_json)


async def save_movie_from_api_doc(settings: Settings, doc: Dict[str, Any]) -> None:
    """
    Сохраняет в movies все данные из ответа API Кинопоиска. Если запись есть (по kinopoisk_id или title+year) — обновляет.
    """
    row = _parse_doc_to_row(doc)
    kinopoisk_id, title, year, age_rating, rating_kp, poster_url, description, genres, countries, votes, raw_json = row
    if not title and not kinopoisk_id:
        return

    async with aiosqlite.connect(settings.db_path) as db:
        if kinopoisk_id is not None:
            cursor = await db.execute("SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1", (kinopoisk_id,))
            existing = await cursor.fetchone()
        else:
            cursor = await db.execute(
                "SELECT id FROM movies WHERE title = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (title, year, year),
            )
            existing = await cursor.fetchone()

        if existing:
            await db.execute(
                """UPDATE movies SET title = ?, year = ?, age_rating = ?, rating_kp = ?, poster_url = ?, description = ?, genres = ?, countries = ?, votes = ?, raw_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
                (title, year, age_rating, rating_kp, poster_url, description, genres, countries, votes, raw_json, existing[0]),
            )
            if kinopoisk_id is not None:
                await db.execute("UPDATE movies SET kinopoisk_id = ? WHERE id = ?", (kinopoisk_id, existing[0]))
        else:
            await db.execute(
                """INSERT INTO movies (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, description, genres, countries, votes, raw_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, description, genres, countries, votes, raw_json),
            )
        await db.commit()


async def _fetch_movie_doc_by_id(settings: Settings, kinopoisk_id: int) -> Optional[Dict[str, Any]]:
    """Запрос к API: полные данные по фильму по ID. Не пишет в БД."""
    if not settings.kinopoisk_api_key:
        return None
    url = f"{settings.kinopoisk_base_url.rstrip('/')}/v1.4/movie/{kinopoisk_id}"
    headers = {"X-API-KEY": settings.kinopoisk_api_key}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            if response.status_code != 200:
                return None
            return response.json()
    except (httpx.RequestError, ValueError):
        return None


def _normalize_title(s: str) -> str:
    """Нормализация названия для сравнения: нижний регистр, лишние пробелы."""
    return (s or "").strip().lower()


def _doc_title_match_score(doc: Dict[str, Any], requested_title: str, requested_year: Optional[int]) -> int:
    """
    Оценка совпадения doc с запрошенным названием и годом. Больше = лучше.
    Приоритет: точное совпадение названия > год совпадает > первое в списке.
    """
    doc_name = _normalize_title(doc.get("name") or doc.get("alternativeName") or "")
    doc_year = doc.get("year")
    if doc_year is not None:
        try:
            doc_year = int(doc_year)
        except (TypeError, ValueError):
            doc_year = None
    req_title = _normalize_title(requested_title)
    if not req_title:
        return 1 if (requested_year is None or doc_year == requested_year) else 0
    year_ok = requested_year is None or doc_year == requested_year
    # Точное совпадение названия (или запрос входит в название)
    if doc_name == req_title and year_ok:
        return 100
    if req_title in doc_name and year_ok:
        return 80
    if doc_name in req_title and year_ok:
        return 60
    # Совпадение по словам (например "летающие ножи" vs "ножи летающие")
    req_words = set(req_title.split())
    doc_words = set(doc_name.split())
    if req_words and req_words <= doc_words and year_ok:
        return 50
    if year_ok:
        return 10  # год совпал, название нет
    return 0


async def _fetch_movie_doc_by_search(
    settings: Settings, title: str, year: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Запрос к API: поиск по названию (и году).
    Возвращает doc с наилучшим совпадением названия и года, а не первый результат —
    иначе при похожих названиях (например «Летающие ножи» и «Летающие звери») можно взять не тот фильм.
    """
    if not settings.kinopoisk_api_key:
        return None
    url = f"{settings.kinopoisk_base_url.rstrip('/')}/v1.4/movie/search"
    headers = {"X-API-KEY": settings.kinopoisk_api_key}
    params = {"query": title.strip(), "limit": 10}
    if year:
        params["query"] = f"{title.strip()} {year}"
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.get(url, headers=headers, params=params)
            if response.status_code != 200:
                return None
            data = response.json()
    except (httpx.RequestError, ValueError):
        return None
    docs = data.get("docs") or []
    if not docs:
        return None
    requested_title = (title or "").strip()
    best = None
    best_score = -1
    for d in docs:
        score = _doc_title_match_score(d, requested_title, year)
        if score > best_score:
            best_score = score
            best = d
    return best if best_score > 0 else docs[0]


async def refresh_movie_from_api(
    settings: Settings,
    *,
    kinopoisk_id: Optional[int] = None,
    title: Optional[str] = None,
    year: Optional[int] = None,
) -> bool:
    """
    Дозаполняет запись в movies из API: по kinopoisk_id или по (title, year).
    Возвращает True, если данные успешно получены и сохранены.
    """
    doc = None
    if kinopoisk_id is not None:
        doc = await _fetch_movie_doc_by_id(settings, kinopoisk_id)
    elif title:
        title = (title or "").strip()
        if title:
            doc = await _fetch_movie_doc_by_search(settings, title, year)
    if not doc:
        return False
    await save_movie_from_api_doc(settings, doc)
    return True


# Минимальный балл совпадения названия, чтобы не подставлять постер от другого фильма (например «Летающие звери» при запросе «Летающие ножи»).
_MIN_TITLE_MATCH_SCORE_TO_USE = 40


async def get_movie_info(
    settings: Settings, title: str, year: Optional[int] = None
) -> Optional[KinopoiskMovieInfo]:
    """
    Возвращает данные по фильму. Сначала проверяет таблицу movies (точное совпадение title+year); если запись есть — из кэша.
    Если нет — запрашивает API, проверяет, что выбранный документ действительно совпадает с запросом, сохраняет и возвращает.
    """
    # Сначала из БД — только точное совпадение title и year, «Летающие звери» не вернётся по запросу «Летающие ножи»
    cached = await get_movie_from_db(settings, title=title, year=year)
    if cached is not None:
        return cached

    if not settings.kinopoisk_api_key:
        return None

    doc = await _fetch_movie_doc_by_search(settings, title, year)
    if not doc:
        return None

    # Защита: если API вернул документ с другим названием (например только год совпал) — не сохраняем и не подставляем его данные
    score = _doc_title_match_score(doc, (title or "").strip(), year)
    if score < _MIN_TITLE_MATCH_SCORE_TO_USE:
        return None  # карточка покажется без постера и рейтинга Кинопоиска, но не с данными другого фильма

    await save_movie_from_api_doc(settings, doc)

    row = _parse_doc_to_row(doc)
    kinopoisk_id, _title, _year, age_rating, rating_kp, poster_url, _d, _g, _c, votes, _raw = row
    return KinopoiskMovieInfo(
        kinopoisk_id=kinopoisk_id,
        age_rating=age_rating,
        rating_kp=rating_kp,
        votes=votes,
        poster_url=poster_url,
    )


async def get_age_rating(settings: Settings, title: str, year: Optional[int] = None) -> Optional[str]:
    """Удобная обёртка: возвращает только возрастной рейтинг (из кэша или API)."""
    info = await get_movie_info(settings, title, year)
    return info.age_rating if info else None


def _mpaa_to_age(mpaa: str) -> Optional[str]:
    """Преобразует рейтинг MPAA (например 'pg-13') в примерный возраст."""
    m = (mpaa or "").strip().upper()
    if not m:
        return None
    if "G" in m or "0" in m:
        return "0"
    if "PG" in m and "13" not in m:
        return "6"
    if "PG-13" in m or "PG13" in m:
        return "12"
    if "R" in m or "NC-17" in m:
        return "18"
    return None
