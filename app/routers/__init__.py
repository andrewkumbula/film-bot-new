from aiogram import Dispatcher

from ..config import Settings
from . import start, flow_movie, favorites, report


def register_routers(dp: Dispatcher, settings: Settings) -> None:
    """
    Регистрирует все роутеры приложения.
    """
    dp.include_router(start.get_router(settings))
    dp.include_router(flow_movie.get_router(settings))
    dp.include_router(favorites.get_router(settings))
    dp.include_router(report.get_router(settings))

