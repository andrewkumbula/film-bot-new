"""
Миграция 003: колонка poster_url в kinopoisk_top250.
Идемпотентна.
"""
from __future__ import annotations

import sqlite3


def run(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(kinopoisk_top250)")
    columns = [row[1] for row in cursor.fetchall()]
    if "poster_url" not in columns:
        conn.execute("ALTER TABLE kinopoisk_top250 ADD COLUMN poster_url TEXT")
        conn.commit()
