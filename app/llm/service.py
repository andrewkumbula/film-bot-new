from __future__ import annotations

import json
from typing import Any, Dict

import httpx
from pydantic import ValidationError

from ..config import Settings
from .schemas import LlmResponse


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
    if mood:
        lines.append(f"- Настроение: {mood_map.get(mood, mood)}")

    genres = preferences.get("genres") or []
    if genres:
        lines.append(f"- Жанры: {', '.join(genres)}")

    duration = preferences.get("duration")
    if duration:
        lines.append(f"- Длительность: {duration_map.get(duration, duration)}")

    age = preferences.get("age")
    if age:
        lines.append(f"- Возрастной рейтинг: {age_map.get(age, age)}")

    company = preferences.get("company")
    if company:
        lines.append(f"- С кем смотрим: {company_map.get(company, company)}")

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
        "- Дай от 3 до 7 фильмов.\n"
        "- Используй реальные фильмы.\n"
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

