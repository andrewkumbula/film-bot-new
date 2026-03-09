"""
Поиск через Tavily API. Используется для ночного дозаполнения kinopoisk_id:
запрос «название фильма год кинопоиск» → результаты передаются в ИИ для извлечения точного названия.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import httpx

from ..config import Settings

logger = logging.getLogger(__name__)

TAVILY_SEARCH_URL = "https://api.tavily.com/search"


async def tavily_search(
    settings: Settings,
    query: str,
    max_results: int = 8,
    *,
    log_context: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Выполняет поиск через Tavily. Возвращает список словарей с ключами title, url, content.
    При отсутствии TAVILY_API_KEY или ошибке возвращает пустой список.
    log_context — опциональный контекст для логов (например movie_id, title, year).
    """
    api_key = getattr(settings, "tavily_api_key", None) or ""
    if not api_key.strip():
        return []
    query = (query or "").strip()
    if not query:
        return []

    logger.info(
        "tavily_search request query=%r context=%s",
        query,
        log_context or {},
    )

    payload = {
        "query": query,
        "max_results": min(max_results, 20),
        "search_depth": "basic",
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key.strip()}",
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(TAVILY_SEARCH_URL, json=payload, headers=headers)
    except httpx.RequestError as e:
        logger.warning(
            "tavily_search request error query=%r context=%s error=%s",
            query, log_context or {}, e,
        )
        return []
    if response.status_code != 200:
        logger.warning(
            "tavily_search response status=%s query=%r context=%s body=%s",
            response.status_code, query, log_context or {}, response.text[:200],
        )
        return []
    try:
        data = response.json()
    except ValueError:
        logger.warning("tavily_search response parse error query=%r context=%s", query, log_context or {})
        return []
    results = data.get("results") or []
    out = []
    for r in results:
        if isinstance(r, dict):
            out.append({
                "title": (r.get("title") or "").strip(),
                "url": (r.get("url") or "").strip(),
                "content": (r.get("content") or "").strip(),
            })

    urls = [x.get("url") or "" for x in out]
    titles = [x.get("title") or "" for x in out]
    logger.info(
        "tavily_search response query=%r context=%s results_count=%s urls=%s titles=%s",
        query, log_context or {}, len(out), urls, titles,
    )
    return out
