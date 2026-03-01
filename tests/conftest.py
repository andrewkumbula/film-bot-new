# Фикстуры для регрессионных тестов. Перед импортом app выставляем тестовое окружение.
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TEST_DB_DIR = ROOT / "tests" / ".tmp"
TEST_DB_DIR.mkdir(parents=True, exist_ok=True)
TEST_DB_PATH = TEST_DB_DIR / "pytest_bot.db"


def _set_test_env():
    os.environ.setdefault("BOT_TOKEN", "test-token-for-pytest")
    os.environ.setdefault("OPENROUTER_API_KEY", "test-key-for-pytest")
    os.environ["DB_PATH"] = str(TEST_DB_PATH)


_set_test_env()


@pytest.fixture
def settings():
    from app.config import load_settings
    return load_settings()


@pytest.fixture
async def db_initialized(settings):
    from app.db.database import init_db
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
    await init_db(settings)
    return settings
