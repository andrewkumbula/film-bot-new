"""
Кинопоиск Топ 250: загрузка с API и фильтрация по жанру/году для ветки «Топ 250».
Таблица kinopoisk_top250 обновляется раз в месяц (scheduler).

Один полный цикл загрузки = 5 запросов к API (5 страниц по 50 фильмов). При лимите 200 запросов/день
это укладывается с большим запасом при обновлении раз в месяц.
"""
from __future__ import annotations

import logging
import random
from typing import Any, Dict, List, Optional, Set

import aiosqlite
import httpx

from ..config import Settings, load_settings

logger = logging.getLogger(__name__)

# Маппинг кодов жанров из клавиатуры на варианты в API/БД (названия на русском или английском)
GENRE_CODE_TO_NAMES = {
    "comedy": ["комедия", "comedy"],
    "detective": ["детектив", "detective"],
    "scifi": ["фантастика", "sci-fi", "фантастика"],
    "fantasy": ["фэнтези", "fantasy"],
    "romance": ["мелодрама", "романтика", "romance", "мелодрама"],
    "horror": ["ужасы", "horror"],
    "drama": ["драма", "drama"],
    "action": ["боевик", "action", "экшн"],
    "family": ["семейный", "family", "для всей семьи"],
    "arthouse": ["артхаус", "arthouse", "драма"],
    "animation": ["мультфильм", "анимация", "animation", "мультипликация"],
    "anime": ["аниме", "anime"],
}


async def fetch_top250_from_api(settings: Settings) -> List[tuple]:
    """
    Загружает список топ-фильмов с API Кинопоиска.
    Возвращает список пар (doc, position), где doc — сырой ответ API по фильму (для сохранения в movies).
    """
    if not settings.kinopoisk_api_key:
        logger.warning("KINOPOISK_API_KEY не задан, загрузка Топ 250 пропущена")
        return []

    base = settings.kinopoisk_base_url.rstrip("/")
    headers = {"X-API-KEY": settings.kinopoisk_api_key}
    result: List[tuple] = []
    for page in range(1, 6):  # 5 * 50 = 250
        url = f"{base}/v1.4/movie"
        params = {
            "limit": 50,
            "page": page,
            "sortField": "votes.kp",
            "sortType": "-1",
        }
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, headers=headers, params=params)
                if resp.status_code != 200:
                    logger.warning("Top250 API page %s: status %s", page, resp.status_code)
                    break
                data = resp.json()
        except (httpx.RequestError, ValueError) as e:
            logger.warning("Top250 API request failed: %s", e)
            break

        docs = data.get("docs") or []
        if not docs:
            break
        for i, doc in enumerate(docs):
            kinopoisk_id = doc.get("id")
            if not kinopoisk_id:
                continue
            try:
                int(kinopoisk_id)
            except (TypeError, ValueError):
                continue
            position = (page - 1) * 50 + i + 1
            result.append((doc, position))
        if len(docs) < 50:
            break
    return result[:250]


def _doc_to_top250_row(doc: Dict[str, Any], position: int) -> Optional[tuple]:
    """Из сырого doc API извлекает (kinopoisk_id, title, year, genres, rating_kp, age_rating, poster_url) для вставки в top250."""
    kinopoisk_id = doc.get("id")
    if kinopoisk_id is not None:
        try:
            kinopoisk_id = int(kinopoisk_id)
        except (TypeError, ValueError):
            return None
    else:
        return None
    name = (doc.get("name") or doc.get("alternativeName") or "").strip() or str(kinopoisk_id)
    year = doc.get("year")
    if year is not None:
        try:
            year = int(year)
        except (TypeError, ValueError):
            year = None
    genres_raw = doc.get("genres") or []
    genre_names = [g.get("name") or "" for g in genres_raw if isinstance(g, dict) and g.get("name")]
    genres_str = ",".join(g.strip().lower() for g in genre_names if g.strip())
    rating = doc.get("rating")
    if isinstance(rating, dict):
        rating = rating.get("kp")
    if rating is not None:
        try:
            rating = float(rating)
        except (TypeError, ValueError):
            rating = None
    age_rating = doc.get("ageRating")
    if age_rating is not None:
        age_rating = str(age_rating).strip() or None
    poster_url = None
    poster = doc.get("poster")
    if isinstance(poster, dict):
        poster_url = poster.get("url") or poster.get("previewUrl") or poster.get("preview")
    elif isinstance(poster, str) and poster.strip().startswith("http"):
        poster_url = poster.strip()
    return (kinopoisk_id, name, year, genres_str, rating, age_rating, (poster_url or "")[:500] or None, position)


async def save_top250_to_db(settings: Settings, items: List[tuple]) -> None:
    """
    Сохраняет Топ 250: для каждого фильма пишет полные данные в movies, затем ссылку в kinopoisk_top250.
    items — список пар (doc, position), где doc — сырой ответ API Кинопоиска.
    """
    if not items:
        return
    from .kinopoisk import save_movie_from_api_doc

    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute("DELETE FROM kinopoisk_top250")
        for doc, position in items:
            await save_movie_from_api_doc(settings, doc)
            row_data = _doc_to_top250_row(doc, position)
            if not row_data:
                continue
            kinopoisk_id, title, year, genres_str, rating_kp, age_rating, poster_url, pos = row_data
            cursor = await db.execute("SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1", (kinopoisk_id,))
            movie_row = await cursor.fetchone()
            movie_id = movie_row[0] if movie_row else None
            if movie_id is None:
                continue
            await db.execute(
                """
                INSERT INTO kinopoisk_top250 (movie_id, kinopoisk_id, title, year, genres, rating_kp, position, age_rating, poster_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (movie_id, kinopoisk_id, title or "", year, genres_str or "", rating_kp, pos, age_rating, poster_url),
            )
        await db.commit()
    logger.info("Top250: сохранено %s записей в movies и kinopoisk_top250", len(items))


async def get_top250_count(settings: Settings) -> int:
    """Возвращает число записей в таблице kinopoisk_top250 (0 если таблица пуста или не создана)."""
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM kinopoisk_top250")
            row = await cursor.fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


async def get_top250_kinopoisk_ids(settings: Settings) -> Set[int]:
    """Возвращает множество kinopoisk_id из таблицы Топ 250. Пустое множество, если таблица пуста."""
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            cursor = await db.execute("SELECT kinopoisk_id FROM kinopoisk_top250")
            rows = await cursor.fetchall()
            return {r[0] for r in rows} if rows else set()
    except Exception:
        return set()


async def get_top250_positions_map(settings: Settings) -> Dict[int, int]:
    """Возвращает словарь kinopoisk_id → место в Топ 250 (1–250). Пустой словарь, если таблица пуста."""
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            cursor = await db.execute("SELECT kinopoisk_id, position FROM kinopoisk_top250")
            rows = await cursor.fetchall()
            return {r[0]: r[1] for r in rows if r[0] is not None and r[1] is not None} if rows else {}
    except Exception:
        return {}


def filter_pairs_by_top250(
    pairs: List[tuple],
    top250_ids: Set[int],
) -> List[tuple]:
    """
    Оставляет только пары (rec, info), у которых info.kinopoisk_id есть в top250_ids.
    Если у фильма нет kinopoisk_id — исключаем. Если top250_ids пуст (таблица Топ 250 не загружена) — возвращаем все пары без фильтра.
    """
    if not top250_ids:
        return pairs
    result = []
    for rec, info in pairs:
        kid = info.kinopoisk_id if info else None
        if kid is None:
            continue
        if kid in top250_ids:
            result.append((rec, info))
    return result


async def refresh_top250(settings: Settings) -> None:
    """Загружает Топ 250 с API и сохраняет в БД. Вызывать раз в месяц (или при старте, если таблица пуста)."""
    items = await fetch_top250_from_api(settings)
    if items:
        await save_top250_to_db(settings, items)
    else:
        logger.warning("Top250: не удалось загрузить данные, таблица не обновлена")


def _year_era_filter(year: Optional[int], era: str) -> bool:
    if year is None:
        return era == "any"
    if era == "new":
        return year >= 2010
    if era == "90s00s":
        return 1990 <= year <= 2009
    if era == "classic":
        return year < 1990
    return True


def _genres_match(db_genres_str: str, selected_codes: List[str]) -> bool:
    if not selected_codes:
        return True
    db_genres_lower = (db_genres_str or "").lower()
    for code in selected_codes:
        for name in GENRE_CODE_TO_NAMES.get(code, [code]):
            if name.lower() in db_genres_lower:
                return True
    return False


def _row_get(row, *keys) -> Any:
    """Берёт значение из row по первому успешному ключу/индексу (Row может не поддерживать 'in')."""
    for k in keys:
        try:
            if isinstance(k, int):
                if len(row) > k:
                    return row[k]
            else:
                val = row[k]
                return val
        except (KeyError, TypeError, IndexError):
            continue
    return None


def _row_to_filtered_item_by_index(row, has_poster: bool) -> Dict[str, Any]:
    """Собирает словарь по индексам: 0=kinopoisk_id, 1=title, 2=year, 3=genres, 4=rating_kp, 5=position, 6=age_rating, 7=poster_url (если has_poster)."""
    try:
        y = row[2]
        year = int(y) if y is not None else None
    except (TypeError, ValueError, IndexError):
        year = None
    return {
        "kinopoisk_id": row[0] if len(row) > 0 else None,
        "title": (row[1] or "") if len(row) > 1 else "",
        "year": year,
        "genres": row[3] if len(row) > 3 else None,
        "rating_kp": row[4] if len(row) > 4 else None,
        "age_rating": row[6] if len(row) > 6 else None,
        "poster_url": (row[7] or None) if has_poster and len(row) > 7 else None,
        "position": row[5] if len(row) > 5 else None,
    }


def _row_to_filtered_item(row, has_poster: bool = True, use_index: bool = False) -> Dict[str, Any]:
    """Собирает словарь из строки БД. use_index=True — по индексам (для простого SELECT из top250)."""
    if use_index:
        return _row_to_filtered_item_by_index(row, has_poster)
    try:
        year_raw = _row_get(row, "year", "m.year", 2)
        year = int(year_raw) if year_raw is not None else None
    except (TypeError, ValueError):
        year = None
    return {
        "kinopoisk_id": _row_get(row, "kinopoisk_id", "m.kinopoisk_id", 0),
        "title": (_row_get(row, "title", "m.title", 1) or ""),
        "year": year,
        "genres": _row_get(row, "genres", "m.genres", 3),
        "rating_kp": _row_get(row, "rating_kp", "m.rating_kp", 4),
        "age_rating": _row_get(row, "age_rating", "m.age_rating", 6),
        "poster_url": (_row_get(row, "poster_url", "m.poster_url", 7) or None) if has_poster else None,
        "position": _row_get(row, "position", "t.position", 5),
    }


async def get_filtered_top250(
    settings: Settings,
    mood: str,
    genre_codes: List[str],
    year_era: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Возвращает фильмы Топ 250, отфильтрованные по жанру и году (эпохе).
    Данные берутся из movies (JOIN с kinopoisk_top250 по movie_id); при отсутствии movie_id — из самой top250.
    При ошибке БД возвращает пустой список и пишет в лог.
    """
    rows = []
    has_poster = True
    use_index = False  # для JOIN используем имена колонок (AS), для простого SELECT — индексы
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        for query_sql, with_poster, use_idx in [
            (
                """
                SELECT
                    m.kinopoisk_id AS kinopoisk_id,
                    COALESCE(NULLIF(TRIM(m.title), ''), t.title) AS title,
                    COALESCE(m.year, t.year) AS year,
                    COALESCE(NULLIF(TRIM(m.genres), ''), t.genres) AS genres,
                    COALESCE(m.rating_kp, t.rating_kp) AS rating_kp,
                    COALESCE(m.age_rating, t.age_rating) AS age_rating,
                    COALESCE(NULLIF(TRIM(m.poster_url), ''), t.poster_url) AS poster_url,
                    t.position AS position
                FROM kinopoisk_top250 t
                JOIN movies m ON t.movie_id = m.id
                ORDER BY t.position
                """,
                True,
                False,
            ),
            (
                "SELECT kinopoisk_id, title, year, genres, rating_kp, position, age_rating, poster_url FROM kinopoisk_top250 ORDER BY position",
                True,
                True,
            ),
            (
                "SELECT kinopoisk_id, title, year, genres, rating_kp, position, age_rating FROM kinopoisk_top250 ORDER BY position",
                False,
                True,
            ),
        ]:
            try:
                cursor = await db.execute(query_sql)
                rows = await cursor.fetchall()
                has_poster = with_poster
                use_index = use_idx
                break
            except Exception as e:
                logger.debug("get_filtered_top250 query failed: %s", e)
                continue
        if not rows:
            logger.warning("get_filtered_top250: no rows from any query variant")
            return []

    filtered = []
    for row in rows:
        item = _row_to_filtered_item(row, has_poster, use_index=use_index)
        if not _year_era_filter(item["year"], year_era):
            continue
        if not _genres_match(item["genres"] or "", genre_codes):
            continue
        filtered.append(item)
    if len(filtered) <= limit:
        return filtered
    return random.sample(filtered, limit)


def match_picks_to_candidates(
    picks: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Сопоставляет список выборов ИИ (title, year) с полными записями из candidates.
    Возвращает список полных записей в порядке picks; не найденные пропускаются.
    """
    result = []
    used = set()
    for pick in picks:
        pt = (pick.get("title") or "").strip()
        py = pick.get("year")
        for i, c in enumerate(candidates):
            if i in used:
                continue
            ct = (c.get("title") or "").strip()
            cy = c.get("year")
            if ct == pt and cy == py:
                result.append(c)
                used.add(i)
                break
    return result
