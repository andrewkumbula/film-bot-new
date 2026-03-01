"""
Миграция 002: уникальный индекс по (title, year) в movies.
Идемпотентна. При наличии дубликатов по (title, year) индекс не создаётся (логируется предупреждение).
"""
from __future__ import annotations

import sqlite3


def run(conn: sqlite3.Connection) -> None:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_movies_title_year_unique'"
    )
    if cursor.fetchone():
        return
    try:
        conn.execute("CREATE UNIQUE INDEX idx_movies_title_year_unique ON movies(title, year)")
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        print("    Пропуск: в movies есть дубликаты (title, year), индекс не создан. Устраните дубликаты и запустите миграции снова.")
