"""
Миграция 004: колонка movie_id (FK → movies) в kinopoisk_top250 и обратное заполнение.
Сначала добавляется колонка, затем для каждой строки без movie_id находится или создаётся запись в movies.
Идемпотентна.
"""
from __future__ import annotations

import sqlite3


def run(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(kinopoisk_top250)")
    columns = [row[1] for row in cursor.fetchall()]
    if "movie_id" not in columns:
        conn.execute(
            "ALTER TABLE kinopoisk_top250 ADD COLUMN movie_id INTEGER REFERENCES movies(id)"
        )
        conn.commit()

    cursor = conn.execute(
        """SELECT id, kinopoisk_id, title, year, genres, rating_kp, age_rating, poster_url
           FROM kinopoisk_top250 WHERE movie_id IS NULL"""
    )
    rows = cursor.fetchall()
    for row in rows:
        t_id, kp_id, title, year, genres, rating_kp, age_rating, poster_url = row
        cur = conn.execute("SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1", (kp_id,))
        movie_row = cur.fetchone()
        if movie_row:
            movie_id = movie_row[0]
        else:
            cur = conn.execute(
                "SELECT id FROM movies WHERE title = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (title or "", year, year),
            )
            movie_row = cur.fetchone()
            if movie_row:
                movie_id = movie_row[0]
                conn.execute("UPDATE movies SET kinopoisk_id = ? WHERE id = ?", (kp_id, movie_id))
            else:
                conn.execute(
                    """INSERT INTO movies (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, genres, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                    (kp_id, title or "", year, age_rating, rating_kp, poster_url, genres or ""),
                )
                movie_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("UPDATE kinopoisk_top250 SET movie_id = ? WHERE id = ?", (movie_id, t_id))
    conn.commit()
