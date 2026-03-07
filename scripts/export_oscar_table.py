#!/usr/bin/env python3
"""
Выгрузка структуры и данных oscar_nominations для диагностики.
Запуск из корня проекта: python scripts/export_oscar_table.py
Результат — в stdout и в файл oscar_nominations_export.txt (если удалось записать).
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import aiosqlite

from app.config import load_settings


async def main() -> None:
    settings = load_settings()
    db_path = settings.db_path
    lines = []
    def out(s: str = "") -> None:
        print(s)
        lines.append(s)

    out("=== Диагностика oscar_nominations ===")
    out(f"DB_PATH: {db_path}")
    out(f"Файл существует: {db_path.exists()}")
    out()

    try:
        async with aiosqlite.connect(db_path) as db:
            # Есть ли таблица
            cursor = await db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='oscar_nominations'"
            )
            row = await cursor.fetchone()
            if not row:
                out("ОШИБКА: таблица oscar_nominations не найдена.")
                out("Запустите бота хотя бы раз (он создаст таблицы) или проверьте DB_PATH.")
                _save(lines)
                return
            out("Таблица oscar_nominations найдена.")

            # Структура
            cursor = await db.execute("PRAGMA table_info(oscar_nominations)")
            columns = await cursor.fetchall()
            out("Колонки (PRAGMA table_info):")
            for c in columns:
                out(f"  {c[1]} ({c[2]})")
            col_names = [c[1] for c in columns]
            out()

            # Количество записей
            cursor = await db.execute("SELECT COUNT(*) FROM oscar_nominations")
            count_row = await cursor.fetchone()
            count = count_row[0] if count_row else 0
            out(f"Всего записей: {count}")

            # С количеством привязанных
            cursor = await db.execute(
                "SELECT COUNT(*) FROM oscar_nominations WHERE movie_id IS NOT NULL"
            )
            mapped_row = await cursor.fetchone()
            mapped = mapped_row[0] if mapped_row else 0
            out(f"С movie_id (привязаны): {mapped}")
            out()

            # Примеры записей (первые 5) — по имеющимся колонкам
            try:
                cursor = await db.execute(
                    "SELECT id, category, ceremony_year, title_from_source, is_winner, movie_id FROM oscar_nominations ORDER BY ceremony_year DESC LIMIT 5"
                )
                rows = await cursor.fetchall()
                out("Примеры записей (первые 5 по году):")
                for r in rows:
                    title = (r[3] or "")[:50]
                    out(f"  id={r[0]} category={r[1]} ceremony_year={r[2]} title={title!r} is_winner={r[4]} movie_id={r[5]}")
            except Exception as e:
                out(f"Ошибка при выборке примеров: {e}")
            out()

            # Проверка запроса как в get_filtered_oscar (без фильтров)
            out("Проверка запроса get_filtered_oscar (SELECT с LEFT JOIN):")
            try:
                cursor = await db.execute(
                    """
                    SELECT
                        o.id AS oscar_id,
                        o.ceremony_year,
                        o.is_winner,
                        o.title_from_source,
                        o.year_from_source,
                        m.id AS movie_id,
                        m.kinopoisk_id,
                        COALESCE(NULLIF(TRIM(m.title), ''), o.title_from_source) AS title
                    FROM oscar_nominations o
                    LEFT JOIN movies m ON o.movie_id = m.id
                    WHERE o.category = ?
                    ORDER BY o.ceremony_year DESC
                    LIMIT 3
                    """,
                    ("best_picture",),
                )
                check_rows = await cursor.fetchall()
                out(f"  Запрос выполнен, строк: {len(check_rows)}")
            except Exception as e:
                out(f"  ОШИБКА запроса: {e}")
                if "year_from_source" in str(e):
                    out("  Подсказка: перезапустите бота — при старте он добавит колонку year_from_source.")
                if "no such column" in str(e).lower():
                    out("  Убедитесь, что бот и скрипт используют один и тот же DB_PATH (см. выше).")
    except Exception as e:
        out(f"ОШИБКА: {e}")
        import traceback
        out(traceback.format_exc())

    _save(lines)


def _save(lines: list) -> None:
    try:
        path = os.path.join(os.path.dirname(__file__), "..", "oscar_nominations_export.txt")
        path = os.path.abspath(path)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"\nВыгрузка сохранена: {path}")
    except Exception as e:
        print(f"\nНе удалось записать файл: {e}")


if __name__ == "__main__":
    asyncio.run(main())
