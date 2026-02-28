from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

import os


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"


@dataclass
class Settings:
    bot_token: str
    openrouter_api_key: str
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    model: str = "xiaomi/mimo-v2-flash"
    debug: bool = False
    db_path: Path = BASE_DIR / "app_data" / "bot.db"
    # API Кинопоиска (poiskkino.dev) для проверки возрастного рейтинга
    kinopoisk_api_key: str = ""
    kinopoisk_base_url: str = "https://api.poiskkino.dev"
    # Ежедневный отчёт по flow_log: куда слать и в какое время (HH:MM, локальное время сервера)
    report_chat_id: str = ""
    report_time: str = "09:00"


def load_settings() -> Settings:
    # Загружаем .env, если он существует
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip()
    model = os.getenv("OPENROUTER_MODEL", "xiaomi/mimo-v2-flash").strip()
    debug = os.getenv("DEBUG", "false").lower() in {"1", "true", "yes"}

    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set in environment or .env file")
    if not openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set in environment or .env file")

    db_path = Path(os.getenv("DB_PATH", str(BASE_DIR / "app_data" / "bot.db")))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    kinopoisk_api_key = os.getenv("KINOPOISK_API_KEY", "").strip()
    kinopoisk_base_url = os.getenv("KINOPOISK_BASE_URL", "https://api.poiskkino.dev").strip()
    report_chat_id = os.getenv("REPORT_CHAT_ID", "").strip()
    report_time = os.getenv("REPORT_TIME", "09:00").strip() or "09:00"

    return Settings(
        bot_token=bot_token,
        openrouter_api_key=openrouter_api_key,
        openrouter_base_url=openrouter_base_url,
        model=model,
        debug=debug,
        db_path=db_path,
        kinopoisk_api_key=kinopoisk_api_key,
        kinopoisk_base_url=kinopoisk_base_url,
        report_chat_id=report_chat_id,
        report_time=report_time,
    )

