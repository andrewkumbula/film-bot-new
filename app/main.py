import logging
import os
import sys

from .config import load_settings
from .bot import start_polling


def _setup_logging() -> None:
    level = logging.DEBUG if os.getenv("DEBUG", "").lower() in ("1", "true", "yes") else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)
    log_path = os.getenv("LOG_PATH", "").strip() or os.getenv("LOG_FILE", "").strip()
    if log_path:
        try:
            handler = logging.FileHandler(log_path, encoding="utf-8")
            handler.setFormatter(logging.Formatter(fmt))
            logging.getLogger().addHandler(handler)
        except Exception as e:
            logging.getLogger(__name__).warning("Cannot add log file %s: %s", log_path, e)


_setup_logging()


async def run() -> None:
    """
    Основная точка входа приложения.
    """
    settings = load_settings()
    await start_polling(settings)

