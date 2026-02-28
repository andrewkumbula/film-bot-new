from __future__ import annotations

import aiosqlite

from ..config import Settings


async def init_db(settings: Settings) -> None:
    """
    Создаёт файл БД и необходимые таблицы.
    Таблица movies — уникальные фильмы (id Кинопоиска, название, год, рейтинг).
    Таблица favorites — избранное пользователей по movie_id.
    """
    async with aiosqlite.connect(settings.db_path) as db:
        await db.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kinopoisk_id INTEGER UNIQUE,
                title TEXT NOT NULL,
                year INTEGER,
                age_rating TEXT,
                rating_kp REAL
            );
            CREATE INDEX IF NOT EXISTS idx_movies_kinopoisk ON movies(kinopoisk_id);
            CREATE INDEX IF NOT EXISTS idx_movies_title_year ON movies(title, year);

            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                movie_id INTEGER NOT NULL,
                why TEXT,
                mood_tags TEXT,
                genres TEXT,
                warnings TEXT,
                similar_if_liked TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (movie_id) REFERENCES movies(id),
                UNIQUE(user_id, movie_id)
            );
            CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id);

            CREATE TABLE IF NOT EXISTS watched (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                movie_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (movie_id) REFERENCES movies(id),
                UNIQUE(user_id, movie_id)
            );
            CREATE INDEX IF NOT EXISTS idx_watched_user ON watched(user_id);

            CREATE TABLE IF NOT EXISTS flow_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                step TEXT NOT NULL,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_flow_log_user ON flow_log(user_id);
            CREATE INDEX IF NOT EXISTS idx_flow_log_step ON flow_log(step);
            CREATE INDEX IF NOT EXISTS idx_flow_log_session ON flow_log(session_id);
            """
        )
        await db.commit()

    await _migrate_old_favorites_if_needed(settings)


async def _migrate_old_favorites_if_needed(settings: Settings) -> None:
    """Если есть старая таблица favorites с колонкой title — переносим данные в movies + новую favorites."""
    try:
        async with aiosqlite.connect(settings.db_path) as db:
            cursor = await db.execute("PRAGMA table_info(favorites)")
            rows = await cursor.fetchall()
    except Exception:
        return
    columns = [r[1] for r in rows] if rows else []
    if "title" not in columns or "movie_id" in columns:
        return

    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                movie_id INTEGER NOT NULL,
                why TEXT,
                mood_tags TEXT,
                genres TEXT,
                warnings TEXT,
                similar_if_liked TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (movie_id) REFERENCES movies(id),
                UNIQUE(user_id, movie_id)
            )
            """
        )
        cursor = await db.execute(
            "SELECT user_id, title, year, genres, why, mood_tags, warnings, similar_if_liked, age_rating, rating_kp FROM favorites ORDER BY id"
        )
        old_rows = await cursor.fetchall()

        for row in old_rows:
            (user_id, title, year, genres, why, mood_tags, warnings, similar_if_liked, age_rating, rating_kp) = row
            if not title:
                continue
            cursor = await db.execute(
                "SELECT id FROM movies WHERE title = ? AND (year IS NULL AND ? IS NULL OR year = ?) LIMIT 1",
                (title.strip(), year, year),
            )
            movie_row = await cursor.fetchone()
            if movie_row:
                movie_id = movie_row[0]
            else:
                await db.execute(
                    "INSERT INTO movies (kinopoisk_id, title, year, age_rating, rating_kp) VALUES (?, ?, ?, ?, ?)",
                    (None, title.strip(), year, age_rating, rating_kp),
                )
                cursor = await db.execute("SELECT last_insert_rowid()")
                movie_id = (await cursor.fetchone())[0]
            await db.execute(
                """INSERT OR IGNORE INTO favorites_new (user_id, movie_id, why, mood_tags, genres, warnings, similar_if_liked)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user_id, movie_id, why or "", mood_tags or "", genres or "", warnings or "", similar_if_liked or ""),
            )
        await db.execute("DROP TABLE favorites")
        await db.execute("ALTER TABLE favorites_new RENAME TO favorites")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id)")
        await db.commit()

