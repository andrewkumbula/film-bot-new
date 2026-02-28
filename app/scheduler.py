"""
Фоновая задача: ежедневная отправка отчёта по flow_log в REPORT_CHAT_ID в REPORT_TIME.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta

from aiogram import Bot

from .config import Settings
from .routers.report import send_daily_report_to_chat

logger = logging.getLogger(__name__)


def _parse_report_time(report_time: str) -> time:
    """Парсит 'HH:MM' в time. При ошибке возвращает 09:00."""
    try:
        parts = report_time.strip().split(":")
        if len(parts) >= 2:
            h, m = int(parts[0]), int(parts[1])
            if 0 <= h <= 23 and 0 <= m <= 59:
                return time(hour=h, minute=m)
    except (ValueError, IndexError):
        pass
    return time(9, 0)


def _seconds_until(target: time) -> float:
    """Секунды до следующего наступления target (локальное время)."""
    now = datetime.now().time()
    if now < target:
        # сегодня
        today = datetime.now().replace(hour=target.hour, minute=target.minute, second=0, microsecond=0)
        return (today - datetime.now()).total_seconds()
    # завтра
    tomorrow = datetime.now().replace(hour=target.hour, minute=target.minute, second=0, microsecond=0) + timedelta(days=1)
    return (tomorrow - datetime.now()).total_seconds()


async def daily_report_scheduler(bot: Bot, settings: Settings) -> None:
    """
    В бесконечном цикле: ждёт до REPORT_TIME, отправляет отчёт в REPORT_CHAT_ID, повторяет раз в сутки.
    Если REPORT_CHAT_ID не задан — задача ничего не делает.
    """
    if not settings.report_chat_id:
        logger.info("REPORT_CHAT_ID not set, daily report scheduler disabled")
        return

    target_time = _parse_report_time(settings.report_time)
    logger.info("Daily report scheduler started, will send at %s to chat %s", target_time, settings.report_chat_id)

    while True:
        delay = _seconds_until(target_time)
        if delay < 0:
            delay = 0
        if delay > 0:
            logger.debug("Next report in %.0f s", delay)
            await asyncio.sleep(delay)

        try:
            await send_daily_report_to_chat(bot, settings.report_chat_id)
        except Exception as e:
            logger.exception("Daily report send failed: %s", e)

        # не отправлять дважды в одну минуту — подождать до следующего дня
        await asyncio.sleep(60)
