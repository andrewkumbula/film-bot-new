"""
Сервис для получения данных о фильмах/сериалах через API Кинопоиска (poiskkino.dev).
Токен можно получить в Telegram-боте @poiskkinodev_bot.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import httpx

from ..config import Settings


@dataclass
class KinopoiskMovieInfo:
    """Данные по фильму/сериалу из API Кинопоиска."""
    kinopoisk_id: Optional[int] = None  # id в API Кинопоиска (уникальный идентификатор)
    age_rating: Optional[str] = None  # "0", "6", "12", "16", "18"
    rating_kp: Optional[float] = None  # рейтинг Кинопоиска (например 8.5)
    votes: Optional[int] = None


async def get_movie_info(
    settings: Settings, title: str, year: Optional[int] = None
) -> Optional[KinopoiskMovieInfo]:
    """
    Ищет фильм или сериал по названию (и опционально году) в API Кинопоиска
    и возвращает возрастной рейтинг, рейтинг КП и число голосов. Один запрос — все поля.
    Если KINOPOISK_API_KEY не задан, возвращает None.
    """
    if not settings.kinopoisk_api_key:
        return None

    url = f"{settings.kinopoisk_base_url.rstrip('/')}/v1.4/movie/search"
    headers = {"X-API-KEY": settings.kinopoisk_api_key}
    params = {"query": title.strip(), "limit": 5}
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

    # Выбираем лучший результат (по году при наличии)
    doc = None
    for d in docs:
        doc_year = d.get("year")
        if year and doc_year and int(doc_year) != year:
            continue
        doc = d
        break
    if doc is None:
        doc = docs[0]

    age_rating = None
    raw_age = doc.get("ageRating")
    if raw_age is not None:
        age_rating = str(raw_age).strip() or None
    elif doc.get("ratingMpaa"):
        age_rating = _mpaa_to_age(str(doc.get("ratingMpaa")))

    rating_kp = None
    r = doc.get("rating")
    if r is not None:
        if isinstance(r, (int, float)):
            rating_kp = float(r)
        elif isinstance(r, dict):
            kp = r.get("kp")
            if kp is not None:
                try:
                    rating_kp = float(kp)
                except (TypeError, ValueError):
                    pass
    votes = doc.get("votes")
    if votes is not None:
        try:
            votes = int(votes)
        except (TypeError, ValueError):
            votes = None

    kinopoisk_id = doc.get("id")
    if kinopoisk_id is not None:
        try:
            kinopoisk_id = int(kinopoisk_id)
        except (TypeError, ValueError):
            kinopoisk_id = None

    return KinopoiskMovieInfo(
        kinopoisk_id=kinopoisk_id,
        age_rating=age_rating,
        rating_kp=rating_kp,
        votes=votes,
    )


async def get_age_rating(settings: Settings, title: str, year: Optional[int] = None) -> Optional[str]:
    """
    Удобная обёртка: возвращает только возрастной рейтинг.
    """
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
