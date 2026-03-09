from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import httpx
from pydantic import ValidationError

from ..config import Settings
from .schemas import LlmResponse, Top250LlmResponse, Top250Pick, SeriesLlmResponse, SeriesPick


class LlmError(Exception):
    def __init__(self, user_message: str, *, debug_detail: str | None = None) -> None:
        super().__init__(user_message)
        self.user_message = user_message
        self.debug_detail = debug_detail or user_message


def _build_prompt(preferences: Dict[str, Any], negative: str) -> str:
    """
    Формирует человекочитаемое описание предпочтений для промпта.
    """
    mood_map = {
        "fun": "весёлое",
        "scary": "страшное",
        "touching": "трогательное",
        "smart": "умное",
        "mindblown": "с «взрывом мозга»",
        "light": "лёгкое",
    }
    duration_map = {
        "short": "до 90 минут",
        "medium": "90–120 минут",
        "long": "дольше 120 минут",
    }
    age_map = {
        "0": "0+",
        "6": "6+",
        "12": "12+",
        "16": "16+",
        "18": "18+",
    }
    company_map = {
        "couple": "вдвоём",
        "family": "с семьёй",
        "friends": "в компании друзей",
        "solo": "в одиночестве",
    }

    lines = []
    mood = preferences.get("mood")
    if mood and mood != "any":
        lines.append(f"- Настроение: {mood_map.get(mood, mood)}")

    genres = preferences.get("genres") or []
    if genres:
        lines.append(f"- Жанры: {', '.join(genres)}")

    duration = preferences.get("duration")
    if duration and duration != "any":
        lines.append(f"- Длительность: {duration_map.get(duration, duration)}")

    age = preferences.get("age")
    if age and age != "any":
        lines.append(f"- Возрастной рейтинг: {age_map.get(age, age)}")

    company = preferences.get("company")
    if company and company != "any":
        lines.append(f"- С кем смотрим: {company_map.get(company, company)}")
        if company == "family":
            lines.append("- Важно: смотрят с детьми — не предлагай фильмы с рейтингом 18+.")

    if negative:
        lines.append(f"- Чего избегать: {negative}")

    details = "\n".join(lines) if lines else "нет явных предпочтений, предложи популярные варианты"

    prompt = (
        "Ты — киноселекционер, помогающий подобрать <фильм на вечер>.\n"
        "У тебя есть предпочтения пользователя:\n"
        f"{details}\n\n"
        "Твоя задача — вернуть <ТОЛЬКО один JSON-объект> строго в формате:\n"
        "{\n"
        '  \"session_summary\": \"краткая сводка предпочтений\",\n'
        "  \"recommendations\": [\n"
        "    {\n"
        '      \"title\": \"Название\",\n'
        "      \"year\": 2021,\n"
        "      \"genres\": [\"...\"],\n"
        '      \"why\": \"Почему подходит\",\n'
        "      \"mood_tags\": [\"😌\", \"😂\"],\n"
        "      \"warnings\": [\"если есть триггеры\"],\n"
        "      \"similar_if_liked\": [\"...\", \"...\"]\n"
        "    }\n"
        "  ],\n"
        "  \"followup_questions\": [\"вопрос при необходимости\"]\n"
        "}\n\n"
        "Требования:\n"
        "- Дай от 12 до 15 фильмов за один раз (часть может отфильтроваться — «уже смотрел», «не интересно» и т.п.).\n"
        "- Используй реальные фильмы.\n"
        "- В поле title пиши название так, как оно указано на Кинопоиске (kinopoisk.ru): орфография (например ё, а не е), официальное написание — это нужно для поиска постера и рейтинга.\n"
        "- Не добавляй комментарии вне JSON, без пояснений, без Markdown, без ```.\n"
        "- Все поля должны быть заполнены корректными типами.\n"
        "- Если чего-то не знаешь точно (год, жанр) — оцени максимально правдоподобно.\n"
    )
    return prompt


async def _request_llm_raw(settings: Settings, prompt: str, *, timeout_sec: int = 30) -> str:
    """
    Делает один запрос к OpenRouter и возвращает текст ответа модели.
    """
    url = f"{settings.openrouter_base_url.rstrip('/')}/chat/completions"

    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type": "application/json",
        "X-Title": "Film na vecher bot",
    }

    payload = {
        "model": settings.model,
        "messages": [
            {
                "role": "system",
                "content": "Ты — дружелюбный эксперт по кино. Отвечай только валидным JSON, без пояснений.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        # Просим модель отдать именно JSON-объект
        "response_format": {"type": "json_object"},
    }

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        try:
            response = await client.post(url, headers=headers, json=payload)
        except httpx.RequestError as e:
            raise LlmError("Проблема с сетью или OpenRouter.") from e

    if response.status_code >= 400:
        raise LlmError(
            "OpenRouter вернул ошибку.",
            debug_detail=f"HTTP {response.status_code}: {response.text}",
        )

    data = response.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as e:
        raise LlmError("Неожиданный формат ответа от модели.") from e

    return content


def _strip_code_fences(text: str) -> str:
    """
    Убирает обрамление ```json ... ``` если модель всё-таки его добавила.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        # Отрезаем первую строку ```... и последнюю ```
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].startswith("```"):
            return "\n".join(lines[1:-1])
    return stripped


async def get_recommendations_from_llm(
    settings: Settings,
    *,
    user_id: int,
    preferences: Dict[str, Any],
    negative: str,
) -> LlmResponse:
    """
    Основная функция для получения и валидации JSON от модели.
    Делает 1 ретрай при таймауте/сетевой ошибке или невалидном JSON.
    """
    prompt = _build_prompt(preferences, negative)

    last_error: str | None = None
    for attempt in range(2):  # 1 попытка + 1 ретрай
        try:
            raw = await _request_llm_raw(settings, prompt)
        except LlmError as e:
            last_error = e.debug_detail
            # при первой ошибке — ещё одна попытка
            if attempt == 0:
                continue
            raise

        text = _strip_code_fences(raw)

        try:
            # Валидация через Pydantic
            return LlmResponse.model_validate_json(text)
        except ValidationError as e:
            last_error = str(e)
            # Попробуем ещё раз с более жёстким уточнением
            if attempt == 0:
                prompt = (
                    prompt
                    + "\n\nВ прошлый раз JSON был невалидным. "
                    "Сейчас СТРОГО верни только один корректный JSON-объект, без комментариев и пояснений."
                )
                continue
            raise LlmError(
                "Модель вернула некорректный ответ.",
                debug_detail=f"Validation error: {last_error}",
            )

    # Теоретически сюда не дойдём
    raise LlmError(
        "Не удалось получить рекомендации от модели.",
        debug_detail=last_error or "unknown error",
    )


def _build_top250_prompt(
    mood: str,
    genre_codes: list,
    year_era: str,
    candidates: list,
) -> str:
    """Промпт для ИИ: выбрать 5 фильмов из списка кандидатов по предпочтениям пользователя."""
    mood_map = {
        "fun": "весёлое",
        "scary": "страшное",
        "touching": "трогательное",
        "smart": "умное",
        "mindblown": "с «взрывом мозга»",
        "light": "лёгкое",
        "any": "любое",
    }
    era_map = {
        "new": "новое кино (2010+)",
        "90s00s": "90-е–00-е",
        "classic": "классика (до 1990)",
        "any": "любая эпоха",
    }
    mood_str = mood_map.get(mood, mood)
    era_str = era_map.get(year_era, year_era)
    genres_str = ", ".join(genre_codes) if genre_codes else "любые"

    lines = []
    for i, film in enumerate(candidates[:80], 1):  # не более 80 в промпте
        title = film.get("title", "?")
        year = film.get("year")
        year_s = f" ({year})" if year else ""
        lines.append(f"{i}. {title}{year_s}")

    list_text = "\n".join(lines)
    prompt = (
        "Пользователь выбирает фильм из Кинопоиск Топ 250. Его предпочтения:\n"
        f"- Настроение: {mood_str}\n"
        f"- Жанры: {genres_str}\n"
        f"- Эпоха: {era_str}\n\n"
        "Ниже список фильмов (из Топ 250), уже отфильтрованных по жанру и эпохе. "
        "Выбери ровно 5 фильмов, которые лучше всего подходят под настроение и предпочтения пользователя. "
        "Верни ТОЛЬКО один JSON-объект в формате:\n"
        "{\n"
        '  "recommendations": [\n'
        '    {"title": "Название фильма", "year": 2020},\n'
        "    ...\n"
        "  ]\n"
        "}\n\n"
        "Список фильмов на выбор:\n"
        f"{list_text}\n\n"
        "Требования: ровно 5 фильмов из этого списка; названия и годы должны совпадать с вариантами выше; без комментариев, без Markdown."
    )
    return prompt


async def shorten_description_for_card(
    settings: Settings, long_description: str, title: str = ""
) -> str | None:
    """
    Генерирует краткое яркое описание для карточки фильма (до 120 символов).
    Возвращает None при ошибке или пустом вводе.
    """
    long_description = (long_description or "").strip()
    if not long_description or len(long_description) < 20:
        return None
    prompt = (
        "Напиши одно короткое описание фильма для карточки в мессенджере — как цепляющий слоган или интригующий тизер, а не пересказ сюжета.\n\n"
        "Нужно: образность, атмосфера, настроение или один яркий образ. Не надо сухого перечисления типа «герой узнаёт, что… и становится…». "
        "Можно вопрос, метафору, контраст или намёк на главную идею. Язык живой, без кавычек. "
        "Можно добавить 1–2 смайла для настроения, но не перегружай. "
        "Весь текст строго на русском языке, без английских слов.\n\n"
        "Исходный текст:\n"
        f"{long_description[:1500]}\n\n"
        "Строго до 120 символов, одно предложение. Ответь только этим текстом, без пояснений."
    )
    if title:
        prompt = f"Фильм: {title}\n\n" + prompt
    url = f"{settings.openrouter_base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type": "application/json",
    }
    model = getattr(settings, "model_short_desc", None) or settings.model
    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt},
        ],
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(url, headers=headers, json=payload)
    except httpx.RequestError:
        return None
    if response.status_code >= 400:
        return None
    try:
        content = response.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None
    text = (content or "").strip().strip('"').strip("'")
    if not text:
        return None
    if len(text) > 120:
        text = text[:117].rstrip() + "…"
    return text


def _format_search_results_for_prompt(results: list) -> str:
    """Форматирует результаты поиска (Tavily) в текст для промпта."""
    lines = []
    for i, r in enumerate(results, 1):
        title = (r.get("title") or "").strip()
        url = (r.get("url") or "").strip()
        content = (r.get("content") or "").strip()
        if title or url or content:
            parts = [f"[{i}]"]
            if title:
                parts.append(f"Заголовок: {title}")
            if url:
                parts.append(f"URL: {url}")
            if content:
                parts.append(f"Содержание: {content[:800]}" + ("…" if len(content) > 800 else ""))
            lines.append("\n".join(parts))
    return "\n\n".join(lines) if lines else ""


async def get_kinopoisk_title_from_search_results(
    settings: Settings,
    title: str,
    year: Optional[int],
    search_results: list,
) -> Optional[str]:
    """
    По результатам поиска в интернете (Tavily: «название год кинопоиск») просит ИИ
    извлечь точное название фильма как на Кинопоиске (ё/е и т.д.). ИИ опирается только
    на переданные результаты, а не на свои знания.
    """
    if not (title or "").strip():
        return None
    text = _format_search_results_for_prompt(search_results)
    if not text.strip():
        return None
    year_part = f", год {year}" if year else ""
    prompt = (
        "Ниже — фрагменты страниц Кинопоиска (kinopoisk.ru) по запросу про фильм «"
        + (title or "").strip()
        + f"»{year_part}.\n\n"
        "Извлеки из этих фрагментов одно точное название фильма так, как оно записано на Кинопоиске: учти орфографию (например ё вместо е), официальное написание. "
        "Ориентируйся на заголовки страниц и текст с них.\n\n"
        "Важно: ответь только названием фильма, без года. Год мы передаём в поиск отдельно. "
        "Если в результатах название идёт с годом в скобках или через пробел (например «Довод (2020)» или «Довод 2020») — в ответ включи только название, без года. "
        "Год в ответ добавляй только если он часть официального названия (например «2001: Космическая одиссея»).\n\n"
        "Ответь одним названием, без кавычек, без пояснений.\n\n"
        "Фрагменты со страниц Кинопоиска:\n\n"
        f"{text}"
    )
    url = f"{settings.openrouter_base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type": "application/json",
    }
    model = getattr(settings, "model_short_desc", None) or settings.model
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(url, headers=headers, json=payload)
    except httpx.RequestError:
        return None
    if response.status_code >= 400:
        return None
    try:
        content = response.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None
    corrected = (content or "").strip().strip('"').strip("'").strip()
    if not corrected:
        return None
    # Убираем год в конце, если ИИ всё же вернул «Название 2020» или «Название (2020)» — год передаётся в поиск отдельно
    corrected = re.sub(r"\s*\(\s*(19|20)\d{2}\s*\)\s*$", "", corrected).strip()
    corrected = re.sub(r"\s+(19|20)\d{2}\s*$", "", corrected).strip()
    return corrected if corrected else None


async def get_top250_picks_from_llm(
    settings: Settings,
    mood: str,
    genre_codes: list,
    year_era: str,
    candidates: list,
) -> Top250LlmResponse:
    """
    Отправляет ИИ список кандидатов (из Топ 250) и предпочтения пользователя;
    возвращает 5 выбранных фильмов (title, year).
    """
    if len(candidates) <= 5:
        return Top250LlmResponse(
            recommendations=[
                Top250Pick(title=c.get("title", ""), year=c.get("year"))
                for c in candidates[:5]
            ],
        )
    prompt = _build_top250_prompt(mood, genre_codes, year_era, candidates)
    raw = await _request_llm_raw(settings, prompt, timeout_sec=25)
    text = _strip_code_fences(raw)
    try:
        return Top250LlmResponse.model_validate_json(text)
    except Exception as e:
        raise LlmError(
            "Не удалось выбрать фильмы из списка.",
            debug_detail=str(e),
        )


def _build_series_prompt(
    time_slot: str,
    format_type: str,
    mood: str,
    restrictions: List[str],
) -> str:
    """Промпт для подбора сериалов по времени, формату, настроению и ограничениям."""
    time_map = {
        "1-2h": "1–2 часа",
        "2-4h": "2–4 часа",
        "several": "несколько вечеров",
        "any": "не важно",
    }
    format_map = {
        "mini": "мини-сериал",
        "one_season": "1 сезон",
        "several_seasons": "несколько сезонов",
        "any": "не важно",
    }
    mood_map = {
        "light": "лёгкое / расслабиться",
        "tense": "напряжённое",
        "funny": "смешное",
        "atmospheric": "атмосферное",
        "dark": "мрачное",
        "romance": "романтика",
        "surprise": "удиви",
        "any": "не важно",
    }
    rest_map = {
        "completed_only": "только завершённые",
        "no_horror": "без ужасов",
        "no_heavy_drama": "без тяжёлой драмы",
        "rating_7_plus": "рейтинг 7+",
        "no_russian": "без российских сериалов",
    }
    lines = [
        f"- Время: {time_map.get(time_slot, time_slot)}",
        f"- Формат: {format_map.get(format_type, format_type)}",
        f"- Настроение: {mood_map.get(mood, mood)}",
    ]
    if restrictions:
        lines.append("- Ограничения: " + ", ".join(rest_map.get(r, r) for r in restrictions))
    details = "\n".join(lines)
    return (
        "Ты — эксперт по сериалам. Пользователь хочет подобрать сериал на вечер.\n"
        f"Предпочтения:\n{details}\n\n"
        "Верни ТОЛЬКО один JSON-объект в формате:\n"
        "{\n"
        '  "session_summary": "краткая сводка в 1 предложение",\n'
        '  "recommendations": [\n'
        '    {"title": "Название сериала", "year": 2020, "why": "Почему подходит"},\n'
        "    ...\n"
        "  ]\n"
        "}\n\n"
        "Требования:\n"
        "- Дай от 8 до 12 сериалов (реальных, с Кинопоиска/IMDb). Часть может отфильтроваться.\n"
        "- Используй только сериалы (не полнометражные фильмы).\n"
        "- title — оригинальное или русское название, как чаще ищут; year — год первого сезона.\n"
        "- Без комментариев вне JSON, без Markdown, без ```.\n"
    )


async def get_series_recommendations_from_llm(
    settings: Settings,
    time_slot: str,
    format_type: str,
    mood: str,
    restrictions: List[str],
) -> SeriesLlmResponse:
    """
    Запрос к ИИ: подбор сериалов по времени, формату, настроению и ограничениям.
    Возвращает список {title, year, why} для последующего обогащения из Кинопоиска.
    """
    prompt = _build_series_prompt(time_slot, format_type, mood, restrictions)
    raw = await _request_llm_raw(settings, prompt, timeout_sec=35)
    text = _strip_code_fences(raw)
    try:
        return SeriesLlmResponse.model_validate_json(text)
    except ValidationError as e:
        raise LlmError(
            "Модель вернула некорректный ответ по сериалам.",
            debug_detail=str(e),
        )


def _build_series_similar_prompt(series_title: str, series_year: Optional[int] = None) -> str:
    """Промпт для подбора сериалов, похожих на указанный."""
    year_part = f" ({series_year})" if series_year else ""
    return (
        f"Пользователю понравился сериал «{series_title}»{year_part}. "
        "Подбери 8–12 других сериалов, похожих по стилю, жанру или настроению.\n\n"
        "Верни ТОЛЬКО один JSON-объект:\n"
        "{\n"
        '  "session_summary": "краткая сводка в 1 предложение",\n'
        '  "recommendations": [\n'
        '    {"title": "Название", "year": 2020, "why": "Почему похож"},\n'
        "    ...\n"
        "  ]\n"
        "}\n\n"
        "Требования: только реальные сериалы (не фильмы); title и year как на Кинопоиске/IMDb; без комментариев и Markdown."
    )


def _build_series_similar_multi_prompt(titles_text: str) -> str:
    """Промпт для подбора сериалов, похожих на один или несколько указанных (через запятую)."""
    parts = [t.strip() for t in (titles_text or "").split(",") if t.strip()]
    if not parts:
        parts = [titles_text or "сериал"]
    list_str = ", ".join(f"«{p}»" for p in parts[:10])
    return (
        f"Пользователь хочет сериалы, похожие на: {list_str}. "
        "Подбери 8–12 других сериалов, похожих по стилю, жанру или настроению на любой из них.\n\n"
        "Верни ТОЛЬКО один JSON-объект:\n"
        "{\n"
        '  "session_summary": "краткая сводка в 1 предложение",\n'
        '  "recommendations": [\n'
        '    {"title": "Название", "year": 2020, "why": "Почему похож"},\n'
        "    ...\n"
        "  ]\n"
        "}\n\n"
        "Требования: только реальные сериалы (не фильмы); title и year как на Кинопоиске/IMDb; без комментариев и Markdown."
    )


def _build_series_by_description_prompt(description: str) -> str:
    """Промпт для подбора сериалов по свободному текстовому описанию пользователя."""
    return (
        f"Пользователь описал, какой сериал хочет:\n\n{description.strip()}\n\n"
        "По этому описанию подбери 8–12 подходящих сериалов (реальных, с Кинопоиска/IMDb).\n\n"
        "Верни ТОЛЬКО один JSON-объект:\n"
        "{\n"
        '  "session_summary": "краткая сводка в 1 предложение",\n'
        '  "recommendations": [\n'
        '    {"title": "Название", "year": 2020, "why": "Почему подходит"},\n'
        "    ...\n"
        "  ]\n"
        "}\n\n"
        "Требования: только сериалы (не полнометражные фильмы); title и year как при поиске; без комментариев и Markdown."
    )


async def get_series_similar_from_llm(
    settings: Settings, series_title: str, series_year: Optional[int] = None
) -> SeriesLlmResponse:
    """
    Запрос к ИИ: сериалы, похожие на указанный. Возвращает список {title, year, why}.
    """
    prompt = _build_series_similar_prompt(series_title, series_year)
    raw = await _request_llm_raw(settings, prompt, timeout_sec=30)
    text = _strip_code_fences(raw)
    try:
        return SeriesLlmResponse.model_validate_json(text)
    except ValidationError as e:
        raise LlmError(
            "Не удалось подобрать похожие сериалы.",
            debug_detail=str(e),
        )


async def get_series_similar_multi_from_llm(
    settings: Settings, titles_text: str
) -> SeriesLlmResponse:
    """
    Подбор сериалов, похожих на один или несколько (строка «название1, название2»).
    """
    prompt = _build_series_similar_multi_prompt(titles_text)
    raw = await _request_llm_raw(settings, prompt, timeout_sec=30)
    text = _strip_code_fences(raw)
    try:
        return SeriesLlmResponse.model_validate_json(text)
    except ValidationError as e:
        raise LlmError(
            "Не удалось подобрать похожие сериалы.",
            debug_detail=str(e),
        )


async def get_series_by_description_from_llm(
    settings: Settings, description: str
) -> SeriesLlmResponse:
    """Подбор сериалов по свободному текстовому описанию пользователя."""
    if not (description or "").strip():
        raise LlmError("Описание не задано.", debug_detail="empty")
    prompt = _build_series_by_description_prompt(description)
    raw = await _request_llm_raw(settings, prompt, timeout_sec=35)
    text = _strip_code_fences(raw)
    try:
        return SeriesLlmResponse.model_validate_json(text)
    except ValidationError as e:
        raise LlmError(
            "Не удалось подобрать сериалы по описанию.",
            debug_detail=str(e),
        )

