"""
Отчёт по логам флоу за сутки: команда /daily_report и отправка по расписанию.
"""
from __future__ import annotations

import logging

from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message

from ..config import Settings
from ..services.report import (
    build_flow_log_csv,
    build_movies_csv,
    build_top250_csv,
    delete_movie_from_cache,
    run_movies_backfill,
)
from ..services.top250 import get_top250_count, refresh_top250

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

    @router.message(Command("export_films"))
    async def cmd_export_films(message: Message) -> None:
        """Выгрузка картотеки movies (фильмы из рекомендаций и избранного) в CSV."""
        if settings.report_chat_id:
            try:
                allowed_id = int(settings.report_chat_id)
            except ValueError:
                allowed_id = None
            if allowed_id is not None and message.from_user and message.from_user.id != allowed_id:
                await message.answer("У вас нет доступа к этой команде.")
                return

        await message.answer("Формирую выгрузку картотеки…")
        try:
            csv_bytes, filename = await build_movies_csv()
        except Exception as e:
            logger.exception("Export films failed")
            await message.answer(f"Ошибка при выгрузке: {e}")
            return

        if len(csv_bytes) <= 1:
            await message.answer("В базе пока нет фильмов.")
            return

        # Считаем строки (минус BOM и заголовок)
        row_count = max(0, csv_bytes.decode("utf-8").count("\n") - 1)
        caption = (
            f"🎬 Картотека (movies): фильмы из рекомендаций и избранного — {row_count} шт. "
            "Для полного списка Топ 250 используй /export_top250"
        )
        doc = BufferedInputFile(csv_bytes, filename=filename)
        await message.answer_document(doc, caption=caption)

    @router.message(Command("backfill_movies"))
    async def cmd_backfill_movies(message: Message) -> None:
        """Дозаполняет данные фильмов (постер, описание, жанры и т.д.) из API Кинопоиска. До 15 записей за раз."""
        if settings.report_chat_id:
            try:
                allowed_id = int(settings.report_chat_id)
            except ValueError:
                allowed_id = None
            if allowed_id is not None and message.from_user and message.from_user.id != allowed_id:
                await message.answer("У вас нет доступа к этой команде.")
                return

        await message.answer("Дозаполняю данные фильмов из Кинопоиска (до 15 за раз)…")
        try:
            updated, err = await run_movies_backfill(limit=15)
        except Exception as e:
            logger.exception("Backfill failed")
            await message.answer(f"Ошибка: {e}")
            return
        if err:
            await message.answer(err)
            return
        await message.answer(f"Готово. Обновлено записей: {updated}. Можно повторить команду для следующих.")

    @router.message(Command("export_top250"))
    async def cmd_export_top250(message: Message) -> None:
        """Выгрузка Кинопоиск Топ 250 в CSV (до 250 фильмов)."""
        if settings.report_chat_id:
            try:
                allowed_id = int(settings.report_chat_id)
            except ValueError:
                allowed_id = None
            if allowed_id is not None and message.from_user and message.from_user.id != allowed_id:
                await message.answer("У вас нет доступа к этой команде.")
                return

        await message.answer("Формирую выгрузку Топ 250…")
        try:
            csv_bytes, filename, count = await build_top250_csv()
        except Exception as e:
            logger.exception("Export top250 failed")
            await message.answer(f"Ошибка при выгрузке: {e}")
            return

        if count == 0:
            await message.answer(
                "Таблица Топ 250 пуста. Заполняется при старте бота и 1-го числа каждого месяца (нужен KINOPOISK_API_KEY)."
            )
            return

        caption = f"⭐ Кинопоиск Топ 250 — {count} фильмов"
        doc = BufferedInputFile(csv_bytes, filename=filename)
        await message.answer_document(doc, caption=caption)

    @router.message(Command("delete_film"))
    async def cmd_delete_film(message: Message) -> None:
        """
        Удаляет фильм из кэша (movies), чтобы при следующем запросе данные подтянулись заново из Кинопоиска.
        Только для админа (REPORT_CHAT_ID). Примеры: /delete_film Достать ножи  или  /delete_film Достать ножи 2019
        """
        if settings.report_chat_id:
            try:
                allowed_id = int(settings.report_chat_id)
            except ValueError:
                allowed_id = None
            if allowed_id is not None and message.from_user and message.from_user.id != allowed_id:
                await message.answer("У вас нет доступа к этой команде.")
                return

        text = (message.text or "").strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer(
                "Использование: /delete_film <название> [год]\n"
                "Пример: /delete_film Достать ножи  или  /delete_film Достать ножи 2019\n"
                "Удаляет запись из кэша — при следующем запросе фильм подтянется из Кинопоиска заново."
            )
            return

        args = parts[1].strip()
        year = None
        title = args
        if args:
            tokens = args.split()
            if len(tokens) >= 2 and tokens[-1].isdigit() and len(tokens[-1]) == 4:
                try:
                    year = int(tokens[-1])
                    title = " ".join(tokens[:-1]).strip()
                except ValueError:
                    pass

        if not title:
            await message.answer("Укажи название фильма после команды.")
            return

        try:
            deleted, msg = await delete_movie_from_cache(title, year)
        except Exception as e:
            logger.exception("delete_film failed")
            await message.answer(f"Ошибка: {e}")
            return
        await message.answer(msg)

    @router.message(Command("refresh_top250"))
    async def cmd_refresh_top250(message: Message) -> None:
        """
        Принудительно обновляет список Кинопоиск Топ 250 с API (загрузка в БД).
        Только для админа (REPORT_CHAT_ID). Обычно список обновляется 1-го числа каждого месяца.
        """
        if settings.report_chat_id:
            try:
                allowed_id = int(settings.report_chat_id)
            except ValueError:
                allowed_id = None
            if allowed_id is not None and message.from_user and message.from_user.id != allowed_id:
                await message.answer("У вас нет доступа к этой команде.")
                return

        if not settings.kinopoisk_api_key:
            await message.answer("KINOPOISK_API_KEY не задан. Добавь ключ в .env и перезапусти бота.")
            return

        await message.answer("Обновляю список Топ 250 с Кинопоиска… (до ~30 сек)")
        try:
            await refresh_top250(settings)
            count = await get_top250_count(settings)
            if count > 0:
                await message.answer(f"✅ Готово. В Топ 250 загружено {count} фильмов.")
            else:
                await message.answer(
                    "Не удалось загрузить данные. Проверь KINOPOISK_API_KEY и лимиты API (например 200 запросов/день)."
                )
        except Exception as e:
            logger.exception("refresh_top250 failed")
            await message.answer(f"Ошибка при обновлении: {e}")

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
