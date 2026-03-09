#!/usr/bin/env python3
"""
Ручной запуск дозаполнения kinopoisk_id для фильмов без него.
Поиск через Tavily по запросу «название год кинопоиск», результаты отдаются в ИИ
для извлечения точного названия, затем повторный поиск в Кинопоиске.
То же, что делает планировщик в 03:30.

Запуск из корня проекта:
  python scripts/run_kinopoisk_backfill.py        # по умолчанию до 50 фильмов
  python scripts/run_kinopoisk_backfill.py 100     # обработать до 100

Нужны OPENROUTER_API_KEY, KINOPOISK_API_KEY в .env. Для уточнения названия по поиску — TAVILY_API_KEY.
"""
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(message)s")


async def main():
    try:
        limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    except ValueError:
        limit = 50
    if limit < 1:
        limit = 50

    from app.config import load_settings
    from app.services.kinopoisk import run_kinopoisk_id_backfill

    settings = load_settings()
    print(f"Запуск дозаполнения kinopoisk_id (до {limit} фильмов)…")
    result = await run_kinopoisk_id_backfill(settings, limit=limit)
    print(f"Обработано: {result['processed']}, обновлено: {result['updated']}")
    if result["errors"]:
        print(f"Ошибки: {len(result['errors'])}")
        for e in result["errors"][:5]:
            print(f"  {e}")
        if len(result["errors"]) > 5:
            print(f"  ... и ещё {len(result['errors']) - 5}")


if __name__ == "__main__":
    asyncio.run(main())
