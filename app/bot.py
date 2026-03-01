from __future__ import annotations

import logging

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from .config import Settings
from .routers import register_routers
from .db.database import init_db


logger = logging.getLogger(__name__)


async def create_bot_and_dispatcher(settings: Settings) -> Dispatcher:
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    register_routers(dp, settings)

    # Инициализируем базу данных перед стартом поллинга
    await init_db(settings)

    # Если таблица Топ 250 пуста и есть ключ Кинопоиска — загружаем один раз при старте (не ждём 1-го числа)
    if settings.kinopoisk_api_key:
        from .services.top250 import get_top250_count, refresh_top250
        if await get_top250_count(settings) == 0:
            logger.info("Top250 table empty, running one-time refresh on startup...")
            try:
                await refresh_top250(settings)
            except Exception as e:
                logger.warning("Top250 startup refresh failed: %s", e)

    # Сохраним объект бота в массиве зависимостей, чтобы было проще получать его при необходимости
    dp["settings"] = settings
    dp["bot"] = bot

    return dp


async def start_polling(settings: Settings) -> None:
    from aiogram.client.session.aiohttp import AiohttpSession  # lazy import

    dp = await create_bot_and_dispatcher(settings)
    bot: Bot = dp["bot"]

    # Запуск ежедневной отправки отчёта по расписанию (если задан REPORT_CHAT_ID)
    import asyncio
    from .scheduler import daily_report_scheduler, top250_refresh_scheduler
    asyncio.create_task(daily_report_scheduler(bot, settings))
    asyncio.create_task(top250_refresh_scheduler(settings))

    logger.info("Starting bot polling...")
    # Long polling
    await dp.start_polling(bot)

