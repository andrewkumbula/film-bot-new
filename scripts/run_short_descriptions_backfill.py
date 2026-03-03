#!/usr/bin/env python3
"""
Ручной запуск бэкфилла коротких описаний (short_description) для фильмов.
То же, что делает планировщик в 03:00, но можно запустить сейчас и задать лимит.

Запуск из корня проекта:
  python scripts/run_short_descriptions_backfill.py          # по умолчанию до 250 фильмов
  python scripts/run_short_descriptions_backfill.py 100       # обработать до 100

Нужны OPENROUTER_API_KEY и (для модели) OPENROUTER_MODEL_SHORT_DESC в .env.
"""
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# чтобы видеть прогресс в консоли
logging.basicConfig(level=logging.INFO, format="%(message)s")


async def main():
    try:
        limit = int(sys.argv[1]) if len(sys.argv) > 1 else 250
    except ValueError:
        limit = 250
    if limit < 1:
        limit = 250

    from app.config import load_settings
    from app.services.short_descriptions import backfill_short_descriptions

    settings = load_settings()
    print(f"Запуск бэкфилла коротких описаний (до {limit} фильмов)…")
    n = await backfill_short_descriptions(settings, limit=limit)
    print(f"Готово. Обновлено записей: {n}")


if __name__ == "__main__":
    asyncio.run(main())
