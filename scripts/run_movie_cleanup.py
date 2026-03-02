#!/usr/bin/env python3
"""
Ручной запуск маппинга пустых записей movies (уровень 1: 100% название + год ±1).
Использование: из корня проекта: python scripts/run_movie_cleanup.py
"""
import asyncio
import os
import sys

# корень проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import load_settings
from app.services.movie_mapping_cleanup import run_cleanup_level1


async def main():
    settings = load_settings()
    result = await run_cleanup_level1(settings)
    print(f"Слияний: {result['merged']}")
    if result["errors"]:
        print(f"Ошибки: {len(result['errors'])}")
        for e in result["errors"][:5]:
            print(f"  {e}")
        if len(result["errors"]) > 5:
            print(f"  ... и ещё {len(result['errors']) - 5}")


if __name__ == "__main__":
    asyncio.run(main())
