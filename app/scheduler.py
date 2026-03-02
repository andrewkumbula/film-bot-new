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
from .services.movie_mapping_cleanup import run_cleanup_level1
from .services.short_descriptions import backfill_short_descriptions
from .services.top250 import refresh_top250

logger = logging.getLogger(__name__)

# Топ 250: обновление раз в месяц (1-е число в 04:00, локальное время)
TOP250_REFRESH_TIME = time(4, 0)
# Краткие описания для карточек: ночной бэкфилл в 03:00
SHORT_DESC_BACKFILL_TIME = time(3, 0)
# Маппинг пустых записей movies на полные (100% название + год ±1): в 02:30
MOVIE_CLEANUP_TIME = time(2, 30)


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


def _seconds_until_first_of_month(at_time: time) -> float:
    """Секунды до следующего 1-го числа месяца в at_time (локальное время)."""
    now = datetime.now()
    next_first = now.replace(day=1, hour=at_time.hour, minute=at_time.minute, second=0, microsecond=0)
    if now >= next_first:
        if next_first.month == 12:
            next_first = next_first.replace(year=next_first.year + 1, month=1)
        else:
            next_first = next_first.replace(month=next_first.month + 1)
    return (next_first - now).total_seconds()


async def short_descriptions_backfill_scheduler(settings: Settings) -> None:
    """
    Раз в сутки в SHORT_DESC_BACKFILL_TIME запускает бэкфилл кратких описаний:
    для фильмов с description, но без short_description, ИИ генерирует краткое описание.
    """
    if not settings.openrouter_api_key:
        logger.info("OPENROUTER_API_KEY not set, short descriptions backfill disabled")
        return
    logger.info("Short descriptions backfill scheduler started, will run daily at %s", SHORT_DESC_BACKFILL_TIME)
    while True:
        delay = _seconds_until(SHORT_DESC_BACKFILL_TIME)
        if delay < 0:
            delay = 0
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            n = await backfill_short_descriptions(settings)
            if n:
                logger.info("Short descriptions backfill: %s movies updated", n)
        except Exception as e:
            logger.exception("Short descriptions backfill failed: %s", e)
        await asyncio.sleep(60)


async def movie_mapping_cleanup_scheduler(settings: Settings) -> None:
    """
    Раз в сутки в MOVIE_CLEANUP_TIME: пустые записи movies (без kinopoisk_id)
    с 100% совпадением названия и годом ±1 маппятся на полные, ссылки переносятся, пустая удаляется.
    """
    logger.info("Movie mapping cleanup scheduler started, will run daily at %s", MOVIE_CLEANUP_TIME)
    while True:
        delay = _seconds_until(MOVIE_CLEANUP_TIME)
        if delay < 0:
            delay = 0
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            result = await run_cleanup_level1(settings)
            if result["merged"]:
                logger.info("Movie mapping cleanup: merged %s records", result["merged"])
            if result["errors"]:
                logger.warning("Movie mapping cleanup: %s errors", len(result["errors"]))
        except Exception as e:
            logger.exception("Movie mapping cleanup failed: %s", e)
        await asyncio.sleep(60)


async def top250_refresh_scheduler(settings: Settings) -> None:
    """
    Раз в месяц (1-е число в 04:00) обновляет таблицу kinopoisk_top250 с API Кинопоиска.
    Один полный цикл = 5 запросов к API (5 страниц по 50 фильмов). При лимите 200/день — с запасом.
    """
    if not settings.kinopoisk_api_key:
        logger.info("KINOPOISK_API_KEY not set, Top250 refresh scheduler disabled")
        return
    logger.info("Top250 refresh scheduler started, will run on 1st of each month at %s", TOP250_REFRESH_TIME)
    while True:
        delay = _seconds_until_first_of_month(TOP250_REFRESH_TIME)
        if delay < 0:
            delay = 0
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            await refresh_top250(settings)
        except Exception as e:
            logger.exception("Top250 refresh failed: %s", e)
        await asyncio.sleep(60)
