"""
Логирование пути пользователя по шагам флоу.
Нужно для анализа: какие шаги реально используются, какие пропускают.
"""
from __future__ import annotations

import aiosqlite

from ..config import load_settings


async def log_flow_step(user_id: int, session_id: str, step: str, value: str = "") -> None:
    """
    Записывает один шаг флоу: пользователь, сессия, название шага, выбранное значение.
    value — код выбора (например fun, comedy,scifi, neg:none) или "skip" при пропуске.
    """
    settings = load_settings()
    value_str = (value or "").strip()[:500]  # ограничиваем длину
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            await db.execute(
                "INSERT INTO flow_log (user_id, session_id, step, value) VALUES (?, ?, ?, ?)",
                (user_id, session_id, step, value_str),
            )
            await db.commit()
    except Exception:
        pass  # не ломаем флоу при ошибке записи лога
