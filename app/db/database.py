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
                rating_kp REAL,
                poster_url TEXT,
                description TEXT,
                genres TEXT,
                countries TEXT,
                votes INTEGER,
                raw_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

            CREATE TABLE IF NOT EXISTS not_interested (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                movie_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (movie_id) REFERENCES movies(id),
                UNIQUE(user_id, movie_id)
            );
            CREATE INDEX IF NOT EXISTS idx_not_interested_user ON not_interested(user_id);

            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

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

            CREATE TABLE IF NOT EXISTS kinopoisk_top250 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kinopoisk_id INTEGER UNIQUE NOT NULL,
                title TEXT NOT NULL,
                year INTEGER,
                genres TEXT,
                rating_kp REAL,
                position INTEGER,
                age_rating TEXT,
                poster_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_top250_kinopoisk ON kinopoisk_top250(kinopoisk_id);
            CREATE INDEX IF NOT EXISTS idx_top250_year ON kinopoisk_top250(year);
            """
        )
        await db.commit()
        await _ensure_top250_poster_column(db)
        await _ensure_top250_movie_id(db)
        await _ensure_movies_extra_columns(db)
        await _ensure_movies_poster_urls(db)
        await _ensure_movies_short_description(db)
        await _ensure_movies_unique_title_year(db)
        await _ensure_not_interested_table(db)
        await _ensure_user_settings_table(db)
        await _ensure_oscar_tables(db)
        await _ensure_oscar_year_from_source(db)
        await _ensure_movies_oscar_flags(db)
        await _ensure_shown_recently_table(db)
        await _ensure_series_tables(db)

    await _migrate_old_favorites_if_needed(settings)


async def _ensure_oscar_tables(db: aiosqlite.Connection) -> None:
    """Создаёт таблицы Оскара: oscar_nominations и oscar_moments."""
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS oscar_nominations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL DEFAULT 'best_picture',
            ceremony_year INTEGER NOT NULL,
            ceremony_label TEXT NOT NULL,
            title_from_source TEXT NOT NULL,
            is_winner INTEGER NOT NULL DEFAULT 0,
            movie_id INTEGER REFERENCES movies(id),
            year_from_source INTEGER,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_oscar_nom_movie ON oscar_nominations(movie_id)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_oscar_nom_year ON oscar_nominations(ceremony_year)"
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS oscar_moments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ceremony_year INTEGER NOT NULL,
            category TEXT,
            kind TEXT NOT NULL,
            title TEXT,
            description TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    await db.commit()


async def _ensure_oscar_year_from_source(db: aiosqlite.Connection) -> None:
    """Добавляет колонку year_from_source в oscar_nominations, если её нет (старые БД)."""
    cursor = await db.execute("PRAGMA table_info(oscar_nominations)")
    rows = await cursor.fetchall()
    columns = [r[1] for r in rows] if rows else []
    if "year_from_source" not in columns:
        await db.execute("ALTER TABLE oscar_nominations ADD COLUMN year_from_source INTEGER")
        await db.commit()


async def _ensure_series_tables(db: aiosqlite.Connection) -> None:
    """Таблицы для сериалов: series, series_favorites, series_watched."""
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kinopoisk_id INTEGER UNIQUE,
            name TEXT NOT NULL,
            original_name TEXT,
            year INTEGER,
            rating_kp REAL,
            votes INTEGER,
            poster_url TEXT,
            poster_urls TEXT,
            description TEXT,
            short_description TEXT,
            is_mini_series INTEGER DEFAULT 0,
            seasons_total INTEGER,
            episodes_total INTEGER,
            runtime_episode_min INTEGER,
            status TEXT,
            countries TEXT,
            genres TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_series_kinopoisk ON series(kinopoisk_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_series_year ON series(year)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_series_rating ON series(rating_kp)")
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS series_favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            series_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (series_id) REFERENCES series(id),
            UNIQUE(user_id, series_id)
        )
        """
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_series_favorites_user ON series_favorites(user_id)")
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS series_watched (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            series_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (series_id) REFERENCES series(id),
            UNIQUE(user_id, series_id)
        )
        """
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_series_watched_user ON series_watched(user_id)")
    await db.commit()


async def _ensure_shown_recently_table(db: aiosqlite.Connection) -> None:
    """Таблица недавно показанных фильмов: не показывать в следующих N выдачах."""
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS shown_recently (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            delivery_number INTEGER NOT NULL,
            movie_id INTEGER,
            kinopoisk_id INTEGER,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_shown_recently_user_delivery ON shown_recently(user_id, delivery_number)"
    )
    await db.commit()


async def _ensure_movies_oscar_flags(db: aiosqlite.Connection) -> None:
    """Добавляет в movies колонки nominated_oscar и won_oscar (boolean)."""
    cursor = await db.execute("PRAGMA table_info(movies)")
    rows = await cursor.fetchall()
    columns = [r[1] for r in rows] if rows else []
    for col, typ in [("nominated_oscar", "INTEGER"), ("won_oscar", "INTEGER")]:
        if col not in columns:
            await db.execute(f"ALTER TABLE movies ADD COLUMN {col} {typ} DEFAULT 0")
    await db.commit()


async def _ensure_user_settings_table(db: aiosqlite.Connection) -> None:
    """Создаёт таблицу user_settings (настройки пользователей), если её ещё нет."""
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            min_rating_filter_enabled INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT
        )
        """
    )
    await db.commit()


async def _ensure_not_interested_table(db: aiosqlite.Connection) -> None:
    """Создаёт таблицу not_interested, если её ещё нет (миграция для старых БД)."""
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='not_interested'"
    )
    if await cursor.fetchone():
        return
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS not_interested (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            movie_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (movie_id) REFERENCES movies(id),
            UNIQUE(user_id, movie_id)
        )
        """
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_not_interested_user ON not_interested(user_id)"
    )
    await db.commit()


async def _ensure_top250_poster_column(db: aiosqlite.Connection) -> None:
    """Добавляет колонку poster_url в kinopoisk_top250, если её ещё нет."""
    cursor = await db.execute("PRAGMA table_info(kinopoisk_top250)")
    rows = await cursor.fetchall()
    columns = [r[1] for r in rows] if rows else []
    if "poster_url" not in columns:
        await db.execute("ALTER TABLE kinopoisk_top250 ADD COLUMN poster_url TEXT")
        await db.commit()


async def _ensure_top250_movie_id(db: aiosqlite.Connection) -> None:
    """Добавляет movie_id (FK → movies) в kinopoisk_top250 и заполняет из существующих строк."""
    cursor = await db.execute("PRAGMA table_info(kinopoisk_top250)")
    rows = await cursor.fetchall()
    columns = [r[1] for r in rows] if rows else []
    if "movie_id" not in columns:
        await db.execute("ALTER TABLE kinopoisk_top250 ADD COLUMN movie_id INTEGER REFERENCES movies(id)")
        await db.commit()

    # Обратное заполнение: для каждой строки без movie_id найти или создать запись в movies
    cursor = await db.execute(
        "SELECT id, kinopoisk_id, title, year, genres, rating_kp, age_rating, poster_url FROM kinopoisk_top250 WHERE movie_id IS NULL"
    )
    rows = await cursor.fetchall()
    for row in rows:
        t_id, kp_id, title, year, genres, rating_kp, age_rating, poster_url = row
        cur = await db.execute("SELECT id FROM movies WHERE kinopoisk_id = ? LIMIT 1", (kp_id,))
        movie_row = await cur.fetchone()
        if movie_row:
            movie_id = movie_row[0]
        else:
            await db.execute(
                """INSERT INTO movies (kinopoisk_id, title, year, age_rating, rating_kp, poster_url, genres, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (kp_id, title or "", year, age_rating, rating_kp, poster_url, genres or ""),
            )
            cur = await db.execute("SELECT last_insert_rowid()")
            movie_id = (await cur.fetchone())[0]
        await db.execute("UPDATE kinopoisk_top250 SET movie_id = ? WHERE id = ?", (movie_id, t_id))
    await db.commit()


async def _ensure_movies_extra_columns(db: aiosqlite.Connection) -> None:
    """Добавляет колонки Кинопоиска в movies, если их ещё нет."""
    cursor = await db.execute("PRAGMA table_info(movies)")
    rows = await cursor.fetchall()
    columns = [r[1] for r in rows] if rows else []
    # SQLite ALTER TABLE не допускает DEFAULT CURRENT_TIMESTAMP — добавляем без default
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
            await db.execute("ALTER TABLE movies ADD COLUMN " + col + " " + typ)
    await db.commit()


async def _ensure_movies_poster_urls(db: aiosqlite.Connection) -> None:
    """Добавляет колонку poster_urls (JSON-массив URL постеров) в movies, если её ещё нет."""
    cursor = await db.execute("PRAGMA table_info(movies)")
    rows = await cursor.fetchall()
    columns = [r[1] for r in rows] if rows else []
    if "poster_urls" not in columns:
        await db.execute("ALTER TABLE movies ADD COLUMN poster_urls TEXT")
    await db.commit()


async def _ensure_movies_short_description(db: aiosqlite.Connection) -> None:
    """Добавляет колонку short_description (краткое описание для карточки, генерируется ИИ ночью)."""
    cursor = await db.execute("PRAGMA table_info(movies)")
    rows = await cursor.fetchall()
    columns = [r[1] for r in rows] if rows else []
    if "short_description" not in columns:
        await db.execute("ALTER TABLE movies ADD COLUMN short_description TEXT")
    await db.commit()


async def _ensure_movies_unique_title_year(db: aiosqlite.Connection) -> None:
    """Уникальность записей по связке название+год. Если в таблице уже есть дубли — миграция может упасть."""
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_movies_title_year_unique'"
    )
    if await cursor.fetchone():
        return
    await db.execute(
        "CREATE UNIQUE INDEX idx_movies_title_year_unique ON movies(title, year)"
    )
    await db.commit()


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
            norm_where = "REPLACE(REPLACE(LOWER(TRIM(COALESCE(title,''))), 'ё', 'е'), 'э', 'е') = REPLACE(REPLACE(LOWER(TRIM(?)), 'ё', 'е'), 'э', 'е')"
            cursor = await db.execute(
                f"SELECT id FROM movies WHERE (year IS NULL AND ? IS NULL OR year = ?) AND {norm_where} LIMIT 1",
                (year, year, title.strip()),
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

