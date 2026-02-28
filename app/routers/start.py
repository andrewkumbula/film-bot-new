from __future__ import annotations

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardRemove

from ..config import Settings
from ..keyboards.main_menu import main_menu_keyboard


def get_router(settings: Settings) -> Router:
    router = Router(name="start")

    @router.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        text = (
            "👋 Привет! Я бот <b>«Фильм на вечер»</b>.\n\n"
            "Помогу за пару минут подобрать крутой фильм под твоё настроение. "
            "Используй кнопки ниже, чтобы начать 🚀"
        )
        await message.answer(text, reply_markup=main_menu_keyboard())

    return router

