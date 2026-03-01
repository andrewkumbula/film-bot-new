"""
Тесты настроек пользователя: чтение/запись и логика фильтра рейтинга.
"""
from __future__ import annotations

import pytest

from app.services.user_settings import (
    get_min_rating_filter_enabled,
    set_min_rating_filter,
    passes_min_rating_filter,
)


@pytest.mark.asyncio
async def test_min_rating_default_when_no_row(db_initialized):
    """Если записи нет — по умолчанию фильтр включён (ниже 6.0 не показываем)."""
    settings = db_initialized
    # Новый пользователь без записи в user_settings
    enabled = await get_min_rating_filter_enabled(99999, settings)
    assert enabled is True


@pytest.mark.asyncio
async def test_set_and_get_min_rating_filter(db_initialized):
    """Включение/выключение фильтра сохраняется и читается."""
    settings = db_initialized
    user_id = 12345
    await set_min_rating_filter(user_id, True, settings)
    assert await get_min_rating_filter_enabled(user_id, settings) is True
    await set_min_rating_filter(user_id, False, settings)
    assert await get_min_rating_filter_enabled(user_id, settings) is False


@pytest.mark.asyncio
async def test_passes_min_rating_filter_logic():
    """Логика: без рейтинга — показываем; при включённом фильтре < 6 отсекаем."""
    assert passes_min_rating_filter(None, True) is True
    assert passes_min_rating_filter(7.0, True) is True
    assert passes_min_rating_filter(6.0, True) is True
    assert passes_min_rating_filter(5.9, True) is False
    assert passes_min_rating_filter(5.0, True) is False
    assert passes_min_rating_filter(5.0, False) is True
    assert passes_min_rating_filter(None, False) is True
