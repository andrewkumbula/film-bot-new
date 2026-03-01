"""
Миграция 001: добавление в movies колонок Кинопоиска (poster_url, description, genres, countries, votes, raw_json, updated_at).
Идемпотентна: добавляет только отсутствующие колонки.
"""
from __future__ import annotations

import sqlite3


def run(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(movies)")
    columns = [row[1] for row in cursor.fetchall()]
    extras = [
        ("poster_url", "TEXT"),
        ("description", "TEXT"),
        ("genres", "TEXT"),
        ("countries", "TEXT"),
        ("votes", "INTEGER"),
        ("raw_json", "TEXT"),
        ("updated_at", "TEXT"),
    ]
    for col, typ in extras:
        if col not in columns:
            conn.execute(f"ALTER TABLE movies ADD COLUMN {col} {typ}")
            conn.commit()
