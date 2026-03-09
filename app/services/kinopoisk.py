"""
Сервис для получения данных о фильмах/сериалах через API Кинопоиска (poiskkino.dev).
Сначала проверяем таблицу movies; в API ходим только если фильма ещё нет. Все данные сохраняем.
Токен можно получить в Telegram-боте @poiskkinodev_bot.

Одновременные запросы к API ограничены семафором, чтобы не получать 429 / таймауты.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiosqlite
import httpx

from ..config import Settings
from ..llm.service import get_kinopoisk_title_from_search_results
from ..services.tavily import tavily_search

logger = logging.getLogger(__name__)

# Один запрос в момент + пауза после запроса, чтобы не получать 429 от API Кинопоиска.
# Семафор создаём при первом использовании (внутри running loop), иначе RuntimeError: attached to a different loop
_KINOPOISK_API_SEMAPHORE: Optional[asyncio.Semaphore] = None
_KINOPOISK_REQUEST_DELAY_SEC = 0.5


def _get_kinopoisk_semaphore() -> asyncio.Semaphore:
    global _KINOPOISK_API_SEMAPHORE
    if _KINOPOISK_API_SEMAPHORE is None:
        _KINOPOISK_API_SEMAPHORE = asyncio.Semaphore(1)
    return _KINOPOISK_API_SEMAPHORE


@dataclass
class KinopoiskMovieInfo:
    """Данные по фильму/сериалу из API Кинопоиска или из кэша (movies)."""
    kinopoisk_id: Optional[int] = None
    age_rating: Optional[str] = None
    rating_kp: Optional[float] = None
    votes: Optional[int] = None
    poster_url: Optional[str] = None
    poster_urls: Optional[List[str]] = None  # все URL постеров от API (1–3 и более)
    short_description: Optional[str] = None  # краткое описание для карточки (генерируется ИИ ночью)


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
                "SELECT kinopoisk_id, age_rating, rating_kp, votes, poster_url, poster_urls, short_description FROM movies WHERE kinopoisk_id = ? LIMIT 1",
                (kinopoisk_id,),
            )
        else:
            cursor = await db.execute(
                "SELECT kinopoisk_id, age_rating, rating_kp, votes, poster_url, poster_urls, short_description FROM movies WHERE title = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (title, year, year),
            )
        row = await cursor.fetchone()
    if not row:
        return None
    poster_urls = None
    try:
        raw = row["poster_urls"]
        if raw and isinstance(raw, str):
            poster_urls = json.loads(raw)
            if not isinstance(poster_urls, list):
                poster_urls = None
    except (json.JSONDecodeError, TypeError, KeyError, IndexError):
        pass
    short_desc = None
    try:
        short_desc = row["short_description"] if row["short_description"] else None
    except (KeyError, IndexError):
        pass
    return KinopoiskMovieInfo(
        kinopoisk_id=row["kinopoisk_id"],
        age_rating=row["age_rating"],
        rating_kp=row["rating_kp"],
        votes=row["votes"],
        poster_url=row["poster_url"] or None,
        poster_urls=poster_urls,
        short_description=short_desc,
    )


# Порядок полей постера в ответе API (приоритет для «главного» постера)
_POSTER_KEYS = ("url", "previewUrl", "preview")


def _parse_poster(doc: Dict[str, Any]) -> Optional[str]:
    """Возвращает один URL постера (приоритет: url → previewUrl → preview)."""
    urls = _parse_poster_urls(doc)
    return urls[0] if urls else None


def _parse_poster_urls(doc: Dict[str, Any]) -> List[str]:
    """Собирает все URL постеров из doc (без дублей, порядок: url, previewUrl, preview)."""
    seen: set = set()
    out: List[str] = []
    poster = doc.get("poster")
    if isinstance(poster, dict):
        for key in _POSTER_KEYS:
            val = poster.get(key)
            if isinstance(val, str) and val.strip().startswith("http"):
                u = val.strip()[:500]
                if u not in seen:
                    seen.add(u)
                    out.append(u)
        for key, val in poster.items():
            if key in _POSTER_KEYS:
                continue
            if isinstance(val, str) and val.strip().startswith("http"):
                u = val.strip()[:500]
                if u not in seen:
                    seen.add(u)
                    out.append(u)
    elif isinstance(poster, str) and poster.strip().startswith("http"):
        u = poster.strip()[:500]
        if u not in seen:
            out.append(u)
    return out


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
    poster_urls = _parse_poster_urls(doc)
    poster_urls_json = json.dumps(poster_urls, ensure_ascii=False)[:2000] if poster_urls else None
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
    return (kinopoisk_id, name, year, age_rating, rating_kp, poster_url, poster_urls_json, description, genres, countries, votes, raw_json)


async def save_movie_from_api_doc(settings: Settings, doc: Dict[str, Any]) -> None:
    """
    Сохраняет в movies все данные из ответа API Кинопоиска. Если запись есть (по kinopoisk_id или title+year) — обновляет.
    """
    row = _parse_doc_to_row(doc)
    kinopoisk_id, title, year, age_rating, rating_kp, poster_url, poster_urls_json, description, genres, countries, votes, raw_json = row
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
                """UPDATE movies SET title = ?, year = ?, age_rating = ?, rating_kp = ?, poster_url = ?, poster_urls = ?, description = ?, genres = ?, countries = ?, votes = ?, raw_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
                (title, year, age_rating, rating_kp, poster_url, poster_urls_json, description, genres, countries, votes, raw_json, existing[0]),
            )
            if kinopoisk_id is not None:
                await db.execute("UPDATE movies SET kinopoisk_id = ? WHERE id = ?", (kinopoisk_id, existing[0]))
        else:
            await db.execute(
                """INSERT INTO movies (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, poster_urls, description, genres, countries, votes, raw_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, poster_urls_json, description, genres, countries, votes, raw_json),
            )
        await db.commit()


async def update_movie_by_id_from_api_doc(
    settings: Settings, movie_id: int, doc: Dict[str, Any]
) -> bool:
    """
    Обновляет существующую запись movies по id данными из ответа API Кинопоиска.
    Используется при ночном дозаполнении: фильм был без kinopoisk_id, нашли по уточнённому названию.
    """
    row = _parse_doc_to_row(doc)
    (
        kinopoisk_id, title, year, age_rating, rating_kp, poster_url, poster_urls_json,
        description, genres, countries, votes, raw_json,
    ) = row
    if not title and not kinopoisk_id:
        return False
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute("SELECT 1 FROM movies WHERE id = ? LIMIT 1", (movie_id,))
        if not await cursor.fetchone():
            return False
        await db.execute(
            """UPDATE movies SET kinopoisk_id = ?, title = ?, year = ?, age_rating = ?, rating_kp = ?,
               poster_url = ?, poster_urls = ?, description = ?, genres = ?, countries = ?, votes = ?, raw_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
            (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, poster_urls_json,
             description, genres, countries, votes, raw_json, movie_id),
        )
        await db.commit()
    return True


async def _fetch_movie_doc_by_id(settings: Settings, kinopoisk_id: int) -> Optional[Dict[str, Any]]:
    """Запрос к API: полные данные по фильму по ID. Не пишет в БД."""
    if not settings.kinopoisk_api_key:
        return None
    url = f"{settings.kinopoisk_base_url.rstrip('/')}/v1.4/movie/{kinopoisk_id}"
    headers = {"X-API-KEY": settings.kinopoisk_api_key}
    async with _get_kinopoisk_semaphore():
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, headers=headers)
                if response.status_code != 200:
                    logger.warning(
                        "Kinopoisk API by_id: status %s for kinopoisk_id=%s",
                        response.status_code,
                        kinopoisk_id,
                    )
                    return None
                result = response.json()
                await asyncio.sleep(_KINOPOISK_REQUEST_DELAY_SEC)
                return result
        except (httpx.RequestError, ValueError) as e:
            logger.warning("Kinopoisk API by_id error for kinopoisk_id=%s: %s", kinopoisk_id, e)
            return None


async def ensure_movie_details_by_id(
    settings: Settings, kinopoisk_id: int
) -> Optional[Dict[str, Any]]:
    """
    Если по фильму нет возрастного рейтинга/описания — запрашивает полные данные с poiskkino по ID,
    сохраняет в movies и возвращает {age_rating, rating_kp, description} для подстановки в карточку.
    Возвращает None при ошибке или отсутствии API-ключа.
    """
    if not settings.kinopoisk_api_key:
        return None
    doc = await _fetch_movie_doc_by_id(settings, kinopoisk_id)
    if not doc:
        return None
    await save_movie_from_api_doc(settings, doc)
    row = _parse_doc_to_row(doc)
    _id, _title, _year, age_rating, rating_kp, _poster, _poster_urls, description, _g, _c, _v, _raw = row
    return {
        "age_rating": age_rating,
        "rating_kp": rating_kp,
        "description": description,
    }


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
    async with _get_kinopoisk_semaphore():
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logger.warning(
                        "Kinopoisk API search: status %s for query=%s",
                        response.status_code,
                        params.get("query", ""),
                    )
                    return None
                data = response.json()
            await asyncio.sleep(_KINOPOISK_REQUEST_DELAY_SEC)
        except (httpx.RequestError, ValueError) as e:
            logger.warning("Kinopoisk API search error for query=%s: %s", params.get("query", ""), e)
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


def _year_candidates(year: Optional[int]) -> list:
    """Для fallback ±1 год: [year, year-1, year+1] без дублей и без None."""
    if year is None:
        return []
    out = [year]
    if year - 1 not in out:
        out.append(year - 1)
    if year + 1 not in out:
        out.append(year + 1)
    return out


async def get_movie_info(
    settings: Settings, title: str, year: Optional[int] = None
) -> Optional[KinopoiskMovieInfo]:
    """
    Возвращает данные по фильму. Сначала проверяет таблицу movies (точное совпадение title+year); если запись есть — из кэша.
    Если нет — запрашивает API. При неудаче по точному году пробует ±1 год (ИИ может указать 2022, а в Кинопоиске 2021).
    """
    title = (title or "").strip()
    if not title:
        return None

    years_to_try = _year_candidates(year) if year is not None else [None]

    # 1) Кэш: точный год, затем ±1
    for y in years_to_try:
        cached = await get_movie_from_db(settings, title=title, year=y)
        if cached is not None:
            return cached

    if not settings.kinopoisk_api_key:
        return None

    # 2) API: точный год, затем ±1
    for y in years_to_try:
        doc = await _fetch_movie_doc_by_search(settings, title, y)
        if not doc:
            continue
        score = _doc_title_match_score(doc, title, y)
        if score < _MIN_TITLE_MATCH_SCORE_TO_USE:
            continue
        await save_movie_from_api_doc(settings, doc)
        row = _parse_doc_to_row(doc)
        kinopoisk_id, _title, _year, age_rating, rating_kp, poster_url, _poster_urls, _d, _g, _c, votes, _raw = row
        poster_urls = json.loads(_poster_urls) if _poster_urls and isinstance(_poster_urls, str) else None
        if not isinstance(poster_urls, list):
            poster_urls = None
        return KinopoiskMovieInfo(
            kinopoisk_id=kinopoisk_id,
            age_rating=age_rating,
            rating_kp=rating_kp,
            votes=votes,
            poster_url=poster_url,
            poster_urls=poster_urls,
        )

    return None


def _is_series_doc(doc: Dict[str, Any]) -> bool:
    """Проверяет, что doc из API — сериал (а не фильм)."""
    if not doc:
        return False
    t = (doc.get("type") or "").strip().lower()
    if t in ("tv-series", "tv_series", "сериал", "series"):
        return True
    if doc.get("isSeries") is True:
        return True
    return False


async def _fetch_series_doc_by_search(
    settings: Settings, title: str, year: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Поиск сериала по названию. Использует тот же search API, фильтрует только сериалы.
    """
    if not settings.kinopoisk_api_key:
        return None
    url = f"{settings.kinopoisk_base_url.rstrip('/')}/v1.4/movie/search"
    headers = {"X-API-KEY": settings.kinopoisk_api_key}
    params = {"query": title.strip(), "limit": 15}
    if year:
        params["query"] = f"{title.strip()} {year}"
    async with _get_kinopoisk_semaphore():
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    return None
                data = response.json()
            await asyncio.sleep(_KINOPOISK_REQUEST_DELAY_SEC)
        except (httpx.RequestError, ValueError):
            return None
    docs = [d for d in (data.get("docs") or []) if _is_series_doc(d)]
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


def _parse_doc_to_series_row(doc: Dict[str, Any]) -> tuple:
    """Из ответа API собирает кортеж для INSERT/UPDATE series."""
    kinopoisk_id = doc.get("id")
    if kinopoisk_id is not None:
        try:
            kinopoisk_id = int(kinopoisk_id)
        except (TypeError, ValueError):
            kinopoisk_id = None
    name = (doc.get("name") or doc.get("alternativeName") or "").strip() or (str(kinopoisk_id) if kinopoisk_id else "")
    original_name = (doc.get("alternativeName") or doc.get("name") or "").strip() or None
    year = doc.get("year")
    if year is not None:
        try:
            year = int(year)
        except (TypeError, ValueError):
            year = None
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
    poster_urls = _parse_poster_urls(doc)
    poster_urls_json = json.dumps(poster_urls, ensure_ascii=False)[:2000] if poster_urls else None
    description = (doc.get("description") or "").strip() or None
    if description and len(description) > 5000:
        description = description[:5000]
    genres_raw = doc.get("genres") or []
    genre_names = [g.get("name") or "" for g in genres_raw if isinstance(g, dict)]
    genres = ",".join(g.strip() for g in genre_names if g.strip()) or None
    countries_raw = doc.get("countries") or []
    country_names = [c.get("name") or "" for c in countries_raw if isinstance(c, dict)]
    countries = ",".join(c.strip() for c in country_names if c.strip()) or None
    # Сериал-специфичные поля (API может отдавать по-разному)
    movie_length = doc.get("movieLength")  # может быть длина серии в минутах
    if movie_length is not None:
        try:
            runtime_episode_min = int(movie_length)
        except (TypeError, ValueError):
            runtime_episode_min = None
    else:
        runtime_episode_min = None
    # Количество сезонов/серий — в некоторых API в полях seriesLength, numberOfEpisodes
    episodes_total = doc.get("numberOfEpisodes") or doc.get("episodesCount")
    if episodes_total is not None:
        try:
            episodes_total = int(episodes_total)
        except (TypeError, ValueError):
            episodes_total = None
    seasons_total = doc.get("seasonsCount") or doc.get("numberOfSeasons")
    if seasons_total is not None:
        try:
            seasons_total = int(seasons_total)
        except (TypeError, ValueError):
            seasons_total = None
    status = (doc.get("status") or "").strip() or None
    if status and isinstance(status, dict):
        status = (status.get("name") or "").strip() or None
    is_mini = 1 if (seasons_total == 1 and (episodes_total or 0) <= 10) else 0
    return (
        kinopoisk_id, name, original_name, year, rating_kp, votes,
        poster_url, poster_urls_json, description, None,  # short_description заполним отдельно
        is_mini, seasons_total, episodes_total, runtime_episode_min, status, countries, genres,
    )


async def save_series_from_api_doc(settings: Settings, doc: Dict[str, Any]) -> Optional[int]:
    """
    Сохраняет сериал из ответа API в таблицу series. Возвращает series.id или None.
    """
    row = _parse_doc_to_series_row(doc)
    (kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls_json,
     description, _, is_mini, seasons_total, episodes_total, runtime_episode_min, status, countries, genres) = row
    if not name and not kinopoisk_id:
        return None
    async with aiosqlite.connect(settings.db_path) as db:
        if kinopoisk_id is not None:
            cursor = await db.execute("SELECT id FROM series WHERE kinopoisk_id = ? LIMIT 1", (kinopoisk_id,))
            existing = await cursor.fetchone()
        else:
            cursor = await db.execute(
                "SELECT id FROM series WHERE name = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (name, year, year),
            )
            existing = await cursor.fetchone()
        if existing:
            await db.execute(
                """UPDATE series SET name=?, original_name=?, year=?, rating_kp=?, votes=?, poster_url=?, poster_urls=?,
                   description=?, short_description=?, is_mini_series=?, seasons_total=?, episodes_total=?,
                   runtime_episode_min=?, status=?, countries=?, genres=?, updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (name, original_name, year, rating_kp, votes, poster_url, poster_urls_json,
                 description, None, is_mini, seasons_total, episodes_total, runtime_episode_min, status, countries, genres, existing[0]),
            )
            await db.commit()
            return existing[0]
        cursor = await db.execute(
            """INSERT INTO series (kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls,
               description, short_description, is_mini_series, seasons_total, episodes_total, runtime_episode_min,
               status, countries, genres) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls_json,
             description, None, is_mini, seasons_total, episodes_total, runtime_episode_min, status, countries, genres),
        )
        await db.commit()
        return cursor.lastrowid


async def get_series_info(
    settings: Settings, title: str, year: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Возвращает данные по сериалу: сначала из таблицы series, иначе — поиск в API (только сериалы),
    сохранение в series и возврат словаря для карточки.
    """
    title = (title or "").strip()
    if not title:
        return None
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        for y in (year, year - 1, year + 1) if year is not None else (None,):
            if y is not None and y < 0:
                continue
            cursor = await db.execute(
                "SELECT id, kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls, "
                "description, short_description, is_mini_series, seasons_total, episodes_total, runtime_episode_min, "
                "status, countries, genres FROM series WHERE name = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (title, y, y),
            )
            row = await cursor.fetchone()
            if row:
                return _series_row_to_dict(row)
    if not settings.kinopoisk_api_key:
        return None
    years_to_try = [year, year - 1, year + 1, None] if year is not None else [None]
    for y in years_to_try:
        if y is not None and y < 0:
            continue
        doc = await _fetch_series_doc_by_search(settings, title, y)
        if not doc:
            continue
        if _doc_title_match_score(doc, title, y) < _MIN_TITLE_MATCH_SCORE_TO_USE:
            continue
        series_id = await save_series_from_api_doc(settings, doc)
        if not series_id:
            continue
        async with aiosqlite.connect(settings.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls, "
                "description, short_description, is_mini_series, seasons_total, episodes_total, runtime_episode_min, "
                "status, countries, genres FROM series WHERE id = ? LIMIT 1",
                (series_id,),
            )
            row = await cursor.fetchone()
        if row:
            return _series_row_to_dict(row)
    return None


async def get_or_create_series_from_llm(
    settings: Settings, title: str, year: Optional[int], why: str = ""
) -> Optional[Dict[str, Any]]:
    """
    Возвращает сериал из БД по названию и году (или ±1 год), либо создаёт запись-заглушку
    из ответа ИИ (name, year, short_description=why). Так мы всегда можем что-то показать;
    обогащение постером/рейтингом делается отдельно через enrich_series_from_kinopoisk.
    """
    title = (title or "").strip()
    if not title:
        return None
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        for y in (year, year - 1, year + 1, None) if year is not None else (None,):
            if y is not None and y < 0:
                continue
            cursor = await db.execute(
                "SELECT id, kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls, "
                "description, short_description, is_mini_series, seasons_total, episodes_total, runtime_episode_min, "
                "status, countries, genres FROM series WHERE name = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (title, y, y),
            )
            row = await cursor.fetchone()
            if row:
                return _series_row_to_dict(row)
        cursor = await db.execute(
            """INSERT INTO series (name, year, short_description) VALUES (?, ?, ?)""",
            (title, year, (why or "").strip() or None),
        )
        await db.commit()
        sid = cursor.lastrowid
        cursor = await db.execute(
            "SELECT id, kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls, "
            "description, short_description, is_mini_series, seasons_total, episodes_total, runtime_episode_min, "
            "status, countries, genres FROM series WHERE id = ? LIMIT 1",
            (sid,),
        )
        row = await cursor.fetchone()
        if row:
            return _series_row_to_dict(row)
    return None


async def update_series_from_api_doc(
    settings: Settings, series_id: int, doc: Dict[str, Any]
) -> None:
    """Обновляет запись series по id данными из ответа API Кинопоиска (постер, рейтинг и т.д.)."""
    row = _parse_doc_to_series_row(doc)
    (kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls_json,
     description, _, is_mini, seasons_total, episodes_total, runtime_episode_min, status, countries, genres) = row
    async with aiosqlite.connect(settings.db_path) as db:
        # short_description из API в _parse_doc_to_series_row не парсится (None); сохраняем текущий «почему» от ИИ
        await db.execute(
            """UPDATE series SET kinopoisk_id=?, name=?, original_name=?, year=?, rating_kp=?, votes=?,
               poster_url=?, poster_urls=?, description=?, short_description=COALESCE(NULLIF(?, ''), short_description),
               is_mini_series=?, seasons_total=?, episodes_total=?, runtime_episode_min=?,
               status=?, countries=?, genres=?, updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (kinopoisk_id, name, original_name, year, rating_kp, votes, poster_url, poster_urls_json,
             description, None, is_mini, seasons_total, episodes_total, runtime_episode_min,
             status, countries, genres, series_id),
        )
        await db.commit()


async def enrich_series_from_kinopoisk(
    settings: Settings, series_id: int
) -> bool:
    """
    Ищет сериал в API Кинопоиска по name/year из записи series и обновляет запись
    (постер, рейтинг, описание и т.д.). Возвращает True, если обогащение прошло.
    """
    if not settings.kinopoisk_api_key:
        return False
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT name, year FROM series WHERE id = ? LIMIT 1", (series_id,)
        )
        row = await cursor.fetchone()
    if not row:
        return False
    name, year = row[0], row[1]
    doc = await _fetch_series_doc_by_search(settings, name, year)
    if not doc:
        years_to_try = [year - 1, year + 1, None] if year is not None else []
        for y in years_to_try:
            if y is not None and y < 0:
                continue
            doc = await _fetch_series_doc_by_search(settings, name, y)
            if doc:
                break
    if not doc:
        return False
    if _doc_title_match_score(doc, (name or "").strip(), year) < _MIN_TITLE_MATCH_SCORE_TO_USE:
        return False
    await update_series_from_api_doc(settings, series_id, doc)
    return True


def _series_row_to_dict(row: Any) -> Dict[str, Any]:
    """Преобразует строку series (Row) в словарь для флоу."""
    poster_urls = None
    if row["poster_urls"]:
        try:
            poster_urls = json.loads(row["poster_urls"]) if isinstance(row["poster_urls"], str) else row["poster_urls"]
        except (json.JSONDecodeError, TypeError):
            pass
    return {
        "id": row["id"],
        "kinopoisk_id": row["kinopoisk_id"],
        "name": (row["name"] or "").strip(),
        "original_name": (row["original_name"] or "").strip(),
        "year": row["year"],
        "rating_kp": row["rating_kp"],
        "votes": row["votes"],
        "poster_url": row["poster_url"],
        "poster_urls": poster_urls,
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


def _is_kinopoisk_url(url: str) -> bool:
    """Проверяет, что URL ведёт на домен Кинопоиска (kinopoisk.ru и поддомены)."""
    if not url or not isinstance(url, str):
        return False
    return "kinopoisk.ru" in url.lower()


async def run_kinopoisk_id_backfill(
    settings: Settings, limit: int = 50
) -> Dict[str, Any]:
    """
    Ночной джоб: для всех фильмов без kinopoisk_id уточняет название через ИИ
    (например ё/е) и повторно ищет в Кинопоиске. Обновляет запись при успешном совпадении.
    Возвращает {"updated": N, "processed": M, "errors": [...]}.
    """
    updated = 0
    processed = 0
    errors: List[str] = []
    if not settings.kinopoisk_api_key:
        return {"updated": 0, "processed": 0, "errors": ["KINOPOISK_API_KEY not set"]}
    if not getattr(settings, "openrouter_api_key", None):
        return {"updated": 0, "processed": 0, "errors": ["OPENROUTER_API_KEY not set"]}
    has_tavily = bool(getattr(settings, "tavily_api_key", "") or "").strip()

    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT id, title, year FROM movies
               WHERE kinopoisk_id IS NULL AND title IS NOT NULL AND TRIM(title) != ''
               ORDER BY id LIMIT ?""",
            (limit,),
        )
        rows = await cursor.fetchall()

    for row in rows:
        movie_id = row["id"]
        title = (row["title"] or "").strip()
        year = row["year"]
        if not title:
            continue
        processed += 1
        try:
            search_title = title
            if has_tavily:
                query = f"{title} {year} кинопоиск" if year is not None else f"{title} кинопоиск"
                query = query.strip()
                search_results = await tavily_search(
                    settings,
                    query,
                    max_results=8,
                    log_context={"movie_id": movie_id, "title": title, "year": year},
                )
                # В ИИ отдаём только результаты с домена Кинопоиска — по ним извлекаем точное название
                kinopoisk_only = [r for r in search_results if _is_kinopoisk_url((r.get("url") or ""))]
                if kinopoisk_only:
                    corrected = await get_kinopoisk_title_from_search_results(
                        settings, title, year, kinopoisk_only
                    )
                    if corrected:
                        search_title = corrected.strip()
            doc = await _fetch_movie_doc_by_search(settings, search_title, year)
            if not doc:
                await asyncio.sleep(_KINOPOISK_REQUEST_DELAY_SEC * 2)
                continue
            score = _doc_title_match_score(doc, search_title, year)
            if score < _MIN_TITLE_MATCH_SCORE_TO_USE:
                await asyncio.sleep(_KINOPOISK_REQUEST_DELAY_SEC)
                continue
            ok = await update_movie_by_id_from_api_doc(settings, movie_id, doc)
            if ok:
                updated += 1
                logger.info(
                    "kinopoisk_backfill: movie_id=%s title=%r -> found (score=%s)",
                    movie_id, title, score,
                )
        except Exception as e:
            errors.append(f"id={movie_id}: {e}")
            logger.warning("kinopoisk_backfill error for movie_id=%s: %s", movie_id, e)
        await asyncio.sleep(_KINOPOISK_REQUEST_DELAY_SEC * 2)

    return {"updated": updated, "processed": processed, "errors": errors}
