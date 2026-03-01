from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class Recommendation(BaseModel):
    title: str = Field(..., description="Название фильма")
    year: Optional[int] = Field(None, description="Год выхода")
    genres: List[str] = Field(default_factory=list)
    why: str = Field("", description="Почему этот фильм подходит пользователю")
    mood_tags: List[str] = Field(default_factory=list, description="Эмодзи или короткие теги настроения")
    warnings: List[str] = Field(default_factory=list, description="Предупреждения, триггеры")
    similar_if_liked: List[str] = Field(default_factory=list, description="Примеры похожих фильмов")


class LlmResponse(BaseModel):
    session_summary: str
    recommendations: List[Recommendation]
    followup_questions: List[str] = Field(default_factory=list)


class Top250Pick(BaseModel):
    """Один фильм, выбранный ИИ из списка кандидатов Топ 250."""
    title: str
    year: Optional[int] = None


class Top250LlmResponse(BaseModel):
    """Ответ ИИ: 5 фильмов из предложенного списка по предпочтениям пользователя."""
    recommendations: List[Top250Pick]

