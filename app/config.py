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
    # Модель для генерации кратких описаний фильмов (ночной бэкфилл); по умолчанию Haiku
    model_short_desc: str = "anthropic/claude-3-5-haiku"
    debug: bool = False
    db_path: Path = BASE_DIR / "app_data" / "bot.db"
    # API Кинопоиска (poiskkino.dev) для поиска фильмов и постеров
    kinopoisk_api_key: str = ""
    kinopoisk_base_url: str = "https://api.poiskkino.dev"
    # Топ 250: если задан URL неофициального API (kinopoiskapiunofficial.tech), используется
    # GET /api/v2.2/films/top?type=TOP_250_BEST_FILMS — порядок в ответе = позиция 1–250.
    # Ключ для неофициального API — отдельный (KINOPOISK_UNOFFICIAL_API_KEY), с сайта kinopoiskapiunofficial.tech
    kinopoisk_top250_base_url: str = ""
    kinopoisk_unofficial_api_key: str = ""
    # Ежедневный отчёт по flow_log: куда слать и в какое время (HH:MM, локальное время сервера)
    report_chat_id: str = ""
    report_time: str = "09:00"
    # Сколько постеров показывать в карточке фильма: 1 или 3 (если API отдал несколько)
    show_posters_count: int = 1
    # Tavily Search API для ночного дозаполнения kinopoisk_id (поиск «название год кинопоиск» → ИИ извлекает точное название)
    tavily_api_key: str = ""


def load_settings() -> Settings:
    # Загружаем .env, если он существует
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip()
    model = os.getenv("OPENROUTER_MODEL", "xiaomi/mimo-v2-flash").strip()
    model_short_desc = os.getenv("OPENROUTER_MODEL_SHORT_DESC", "anthropic/claude-3-5-haiku").strip() or "anthropic/claude-3-5-haiku"
    debug = os.getenv("DEBUG", "false").lower() in {"1", "true", "yes"}

    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set in environment or .env file")
    if not openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set in environment or .env file")

    db_path = Path(os.getenv("DB_PATH", str(BASE_DIR / "app_data" / "bot.db")))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    kinopoisk_api_key = os.getenv("KINOPOISK_API_KEY", "").strip()
    kinopoisk_base_url = os.getenv("KINOPOISK_BASE_URL", "https://api.poiskkino.dev").strip()
    kinopoisk_top250_base_url = os.getenv("KINOPOISK_TOP250_BASE_URL", "").strip()
    kinopoisk_unofficial_api_key = os.getenv("KINOPOISK_UNOFFICIAL_API_KEY", "").strip()
    report_chat_id = os.getenv("REPORT_CHAT_ID", "").strip()
    report_time = os.getenv("REPORT_TIME", "09:00").strip() or "09:00"
    show_posters_raw = os.getenv("SHOW_POSTERS_COUNT", "1").strip()
    show_posters_count = 3 if show_posters_raw == "3" else 1
    tavily_api_key = os.getenv("TAVILY_API_KEY", "").strip()

    return Settings(
        bot_token=bot_token,
        openrouter_api_key=openrouter_api_key,
        openrouter_base_url=openrouter_base_url,
        model=model,
        model_short_desc=model_short_desc,
        debug=debug,
        db_path=db_path,
        kinopoisk_api_key=kinopoisk_api_key,
        kinopoisk_base_url=kinopoisk_base_url,
        kinopoisk_top250_base_url=kinopoisk_top250_base_url,
        kinopoisk_unofficial_api_key=kinopoisk_unofficial_api_key,
        report_chat_id=report_chat_id,
        report_time=report_time,
        show_posters_count=show_posters_count,
        tavily_api_key=tavily_api_key,
    )

