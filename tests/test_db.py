# Тесты БД: init_db создаёт нужные таблицы.
import pytest


@pytest.mark.asyncio
async def test_init_db_creates_tables(db_initialized):
    import aiosqlite
    settings = db_initialized
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in await cursor.fetchall()]
    assert "movies" in tables
    assert "favorites" in tables
    assert "watched" in tables
    assert "not_interested" in tables
    assert "user_settings" in tables
    assert "kinopoisk_top250" in tables
    assert "flow_log" in tables
