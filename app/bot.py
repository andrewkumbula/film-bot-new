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

    # Сохраним объект бота в массиве зависимостей, чтобы было проще получать его при необходимости
    dp["settings"] = settings
    dp["bot"] = bot

    return dp


async def start_polling(settings: Settings) -> None:
    from aiogram.client.session.aiohttp import AiohttpSession  # lazy import

    dp = await create_bot_and_dispatcher(settings)
    bot: Bot = dp["bot"]

    logger.info("Starting bot polling...")
    # Long polling
    await dp.start_polling(bot)

