import logging

from .config import load_settings
from .bot import start_polling


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def run() -> None:
    """
    Основная точка входа приложения.
    """
    settings = load_settings()
    await start_polling(settings)

