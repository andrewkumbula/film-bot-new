"""
Отчёт по логам флоу за сутки: команда /daily_report и отправка по расписанию.
"""
from __future__ import annotations

import logging

from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message

from ..config import Settings
from ..services.report import build_flow_log_csv

logger = logging.getLogger(__name__)


def get_router(settings: Settings) -> Router:
    router = Router(name="report")

    @router.message(Command("daily_report"))
    async def cmd_daily_report(message: Message) -> None:
        # Если задан REPORT_CHAT_ID — отчёт только ему; иначе отчёт получает тот, кто вызвал команду
        if settings.report_chat_id:
            try:
                allowed_id = int(settings.report_chat_id)
            except ValueError:
                allowed_id = None
            if allowed_id is not None and message.from_user and message.from_user.id != allowed_id:
                await message.answer("У вас нет доступа к этой команде.")
                return

        await message.answer("Формирую отчёт за последние 24 часа…")
        try:
            csv_bytes, filename = await build_flow_log_csv(hours=24)
        except Exception as e:
            logger.exception("Build report failed")
            await message.answer(f"Ошибка при формировании отчёта: {e}")
            return

        if len(csv_bytes) <= 1:
            await message.answer("За последние 24 часа записей в логе нет.")
            return

        doc = BufferedInputFile(csv_bytes, filename=filename)
        await message.answer_document(doc, caption="📊 Лог движений пользователей за сутки (flow_log)")

    return router


async def send_daily_report_to_chat(bot: Bot, chat_id: str) -> None:
    """Собирает отчёт за 24 часа и отправляет в указанный чат. Вызывается по расписанию."""
    try:
        csv_bytes, filename = await build_flow_log_csv(hours=24)
    except Exception as e:
        logger.exception("Build report failed")
        await bot.send_message(chat_id, f"Ошибка при формировании отчёта: {e}")
        return
    if len(csv_bytes) <= 1:
        await bot.send_message(chat_id, "За последние 24 часа записей в flow_log нет.")
        return
    doc = BufferedInputFile(csv_bytes, filename=filename)
    await bot.send_document(chat_id, doc, caption="📊 Ежедневный отчёт: лог движений пользователей (flow_log)")
