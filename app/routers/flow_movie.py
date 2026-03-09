from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Метрики подбора фильмов (обычный флоу): логируем длительность этапов
def _log_stage(user_id: int, stage: str, duration_sec: float, **extra: Any) -> None:
    payload = " ".join(f"{k}={v}" for k, v in extra.items())
    logger.info(
        "movie_flow_timing user_id=%s stage=%s duration_sec=%.2f %s",
        user_id, stage, duration_sec, payload or "",
    )

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto

from ..config import Settings, load_settings
from ..keyboards.flow import (
    source_keyboard,
    mood_keyboard,
    genres_keyboard,
    age_keyboard,
    company_keyboard,
    negative_keyboard,
    year_era_keyboard,
    recommendations_control_keyboard,
    oscar_type_keyboard,
    oscar_year_keyboard,
)
from ..keyboards.main_menu import main_menu_keyboard
from ..llm.service import get_recommendations_from_llm, get_top250_picks_from_llm, LlmError
from ..services.favorites import (
    add_favorite_for_user,
    add_favorite_by_movie_id,
    add_watched_for_user,
    add_watched_by_movie_id,
    get_or_create_movie,
    get_watched_kinopoisk_ids,
    get_watched_movie_ids,
    is_favorite,
    is_watched,
    rec_in_watched,
)
from ..services.not_interested import (
    add_not_interested,
    add_not_interested_by_movie_id,
    is_not_interested,
    rec_in_not_interested,
    get_not_interested_kinopoisk_ids,
    get_not_interested_movie_ids,
)
from ..services.flow_log import log_flow_step
from ..services.kinopoisk import ensure_movie_details_by_id, get_movie_from_db, get_movie_info, refresh_movie_from_api, KinopoiskMovieInfo
from ..services.top250 import get_filtered_top250, get_top250_count, get_top250_positions_map, match_picks_to_candidates
from ..services.oscar import get_filtered_oscar, get_oscar_count, get_oscar_flags, get_movie_id_by_kinopoisk, link_oscar_to_movie
from ..services.user_settings import get_min_rating_filter_enabled
from ..services.users import ensure_user
from ..services.recently_shown import (
    RECENT_DELIVERIES_COUNT,
    get_next_delivery_number,
    get_recently_shown_ids,
    filter_out_recently_shown,
    record_shown,
)


def _oscar_card_kb(
    idx: int, movie_id: Optional[int], in_fav: bool, in_watched: bool, in_ni: bool
) -> InlineKeyboardMarkup:
    """callback_data с movie_id (m:123) чтобы кнопки работали и со старых карточек; иначе i:idx."""
    fav_data = f"oscar_fav:m:{movie_id}" if movie_id is not None else f"oscar_fav:i:{idx}"
    watched_data = f"oscar_watched:m:{movie_id}" if movie_id is not None else f"oscar_watched:i:{idx}"
    ni_data = f"oscar_not_interested:m:{movie_id}" if movie_id is not None else f"oscar_not_interested:i:{idx}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ В избранном" if in_fav else "⭐️ В избранное", callback_data=fav_data),
                InlineKeyboardButton(text="✅ Смотрел" if in_watched else "🎬 Смотрел", callback_data=watched_data),
            ],
            [InlineKeyboardButton(text="✅ Не интересно" if in_ni else "👎 Не интересно", callback_data=ni_data)],
        ]
    )


async def _update_oscar_card_buttons(
    callback: CallbackQuery,
    idx: int,
    movie_id: Optional[int],
    *,
    in_fav: bool,
    in_watched: bool,
    in_ni: bool,
) -> None:
    try:
        await callback.message.edit_reply_markup(reply_markup=_oscar_card_kb(idx, movie_id, in_fav, in_watched, in_ni))
    except Exception:
        pass


async def _send_movie_card(
    responder: Message,
    poster_urls: List[str],
    text: str,
    kb: InlineKeyboardMarkup,
    settings: Settings,
) -> None:
    """Отправляет карточку фильма: 1 постер или до 3 постеров (media group) + кнопки."""
    valid = [u for u in (poster_urls or []) if u and str(u).strip().startswith("http")]
    count = min(settings.show_posters_count, 3)
    count = max(1, count)
    to_show = valid[:count] if valid else []
    if len(to_show) == 1:
        await responder.answer_photo(photo=to_show[0], caption=text, reply_markup=kb)
    elif len(to_show) > 1:
        media = [InputMediaPhoto(media=url) for url in to_show]
        media[0].caption = text
        await responder.bot.send_media_group(chat_id=responder.chat.id, media=media)
        await responder.answer("👇", reply_markup=kb)
    else:
        await responder.answer(text, reply_markup=kb)


class MovieFlow(StatesGroup):
    source = State()
    mood = State()
    genres = State()
    age = State()
    company = State()
    negative = State()
    recommendations = State()


class Top250Flow(StatesGroup):
    mood = State()
    genres = State()
    year_era = State()
    recommendations = State()


class OscarFlow(StatesGroup):
    type_filter = State()
    year_era = State()
    recommendations = State()


def get_router(settings: Settings) -> Router:
    router = Router(name="movie_flow")

    # Старт флоу: только кнопка «Подобрать фильм» → развилка по источнику
    @router.message(F.text.endswith("Подобрать фильм"))
    async def start_flow(message: Message, state: FSMContext) -> None:
        if message.from_user:
            await ensure_user(
                message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
            )
        await state.clear()
        session_id = uuid.uuid4().hex
        await state.set_state(MovieFlow.source)
        await state.update_data(preferences={}, session_id=session_id)
        await log_flow_step(message.from_user.id, session_id, "start", "source")
        await message.answer("Начнём подбор фильма ✨")
        await message.answer(
            "Откуда подбирать фильмы? 👇",
            reply_markup=source_keyboard(),
        )

    # Развилка: выбор источника
    @router.callback_query(MovieFlow.source, F.data.startswith("source:"))
    async def choose_source(callback: CallbackQuery, state: FSMContext) -> None:
        source = callback.data.split(":", 1)[1]
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["source"] = source
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "source", source)
        await callback.answer()

        if source == "default":
            await state.set_state(MovieFlow.mood)
            await callback.message.edit_text(
                "Для начала давай выберем <b>настроение</b> для фильма 👇",
                reply_markup=mood_keyboard(),
            )
            return
        if source == "top250":
            await state.set_state(Top250Flow.mood)
            await state.update_data(preferences=prefs, session_id=session_id)
            await log_flow_step(callback.from_user.id, session_id, "top250_start", "mood")
            await callback.message.edit_text(
                "Выбери <b>настроение / тип просмотра</b> 👇",
                reply_markup=mood_keyboard(prefix="t250_"),
            )
            return
        if source == "oscar":
            await state.set_state(OscarFlow.type_filter)
            await state.update_data(preferences=prefs, session_id=session_id)
            await log_flow_step(callback.from_user.id, session_id, "oscar_start", "type")
            await callback.message.edit_text(
                "Выбери <b>тип</b> подборки по Оскару 👇",
                reply_markup=oscar_type_keyboard(),
            )
            return

    # --- Ветка Топ 250 ---
    @router.callback_query(Top250Flow.mood, F.data.startswith("t250_mood:"))
    async def top250_mood(callback: CallbackQuery, state: FSMContext) -> None:
        mood_code = callback.data.replace("t250_mood:", "", 1)
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["mood"] = mood_code
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "top250_mood", mood_code)
        await state.set_state(Top250Flow.genres)
        await callback.message.edit_text(
            "Теперь выбери <b>жанры</b>. Можно несколько, потом «✅ Готово».",
            reply_markup=genres_keyboard(set(), cb_prefix="t250_"),
        )
        await callback.answer()

    @router.callback_query(Top250Flow.genres, F.data.startswith("t250_"))
    async def top250_genres(callback: CallbackQuery, state: FSMContext) -> None:
        data = await state.get_data()
        prefs = data.get("preferences", {})
        selected = set(prefs.get("genres", []))
        session_id = data.get("session_id") or uuid.uuid4().hex

        if callback.data == "t250_genres_done":
            if not selected:
                await callback.answer("Выбери хотя бы один жанр 🙏", show_alert=True)
                return
            await log_flow_step(callback.from_user.id, session_id, "top250_genres", ",".join(sorted(selected)))
            await state.set_state(Top250Flow.year_era)
            await callback.message.edit_text(
                "Выбери <b>год (эпоху)</b> 👇",
                reply_markup=year_era_keyboard("t250_"),
            )
            await callback.answer()
            return
        if callback.data == "t250_genres_skip":
            prefs["genres"] = []
            await state.update_data(preferences=prefs)
            await log_flow_step(callback.from_user.id, session_id, "top250_genres", "skip")
            await state.set_state(Top250Flow.year_era)
            await callback.message.edit_text(
                "Выбери <b>год (эпоху)</b> 👇",
                reply_markup=year_era_keyboard("t250_"),
            )
            await callback.answer()
            return
        if callback.data.startswith("t250_genre:"):
            code = callback.data.replace("t250_genre:", "", 1)
            if code in selected:
                selected.remove(code)
            else:
                selected.add(code)
            prefs["genres"] = list(selected)
            await state.update_data(preferences=prefs)
            try:
                await callback.message.edit_reply_markup(reply_markup=genres_keyboard(selected, cb_prefix="t250_"))
            except TelegramBadRequest as e:
                if "message is not modified" not in str(e).lower():
                    raise
            await callback.answer()

    @router.callback_query(F.data.startswith("t250_year:"))
    async def top250_year_era(callback: CallbackQuery, state: FSMContext) -> None:
        year_era = callback.data.replace("t250_year:", "", 1).strip()
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["year_era"] = year_era
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "top250_year_era", year_era)
        await callback.answer()
        try:
            await callback.message.edit_text("Подбираю фильмы… 🎬")
        except Exception:
            pass

        mood = prefs.get("mood") or "any"
        genre_codes = prefs.get("genres") or []
        settings = load_settings()
        candidates: List[Dict[str, Any]] = []
        films: List[Dict[str, Any]] = []
        header_msg = "Вот подборка из Кинопоиск Топ 250 🎬"

        try:
            candidates = await get_filtered_top250(settings, mood, genre_codes, year_era, limit=80)
        except Exception as e:
            logger.exception("Top250 get_filtered_top250 failed: %s", e)
            await callback.message.edit_text(
                "Не удалось загрузить список Топ 250 из базы. Попробуй позже или выбери <b>Обычный подбор</b>.",
                reply_markup=recommendations_control_keyboard("t250_"),
            )
            return

        try:
            ni_kp = await get_not_interested_kinopoisk_ids(callback.from_user.id)
            watched_kp = await get_watched_kinopoisk_ids(callback.from_user.id)
        except Exception as e:
            logger.warning("Top250 get ni/watched ids failed, using empty: %s", e)
            ni_kp = set()
            watched_kp = set()

        candidates = [
            c for c in candidates
            if c.get("kinopoisk_id") not in ni_kp and c.get("kinopoisk_id") not in watched_kp
        ]
        min_rating_on = await get_min_rating_filter_enabled(callback.from_user.id, settings)
        if min_rating_on:
            candidates = [
                c for c in candidates
                if c.get("rating_kp") is None or (isinstance(c.get("rating_kp"), (int, float)) and float(c["rating_kp"]) >= 6.0)
            ]
        exclude_movie_ids, exclude_kinopoisk_ids = await get_recently_shown_ids(
            settings, callback.from_user.id, RECENT_DELIVERIES_COUNT
        )
        candidates = filter_out_recently_shown(candidates, exclude_movie_ids, exclude_kinopoisk_ids)
        delivery_number = await get_next_delivery_number(settings, callback.from_user.id)
        await state.set_state(Top250Flow.recommendations)

        if not candidates:
            try:
                total = await get_top250_count(settings)
            except Exception:
                total = 0
            if total == 0:
                text = (
                    "Топ 250 ещё не загружен — возможно, лимит запросов Кинопоиска (200/день) исчерпан или это первый запуск.\n\n"
                    "Данные подгружаются при старте бота и 1-го числа каждого месяца. Попробуй перезапустить бота позже или выбери <b>Обычный подбор</b>."
                )
            else:
                extra = " или по настройке «рейтинг не ниже 6.0»" if min_rating_on else ""
                text = (
                    f"По твоим фильтрам ничего не нашлось в Топ 250 — всё уже в «Не интересно», ты это смотрел 🎬{extra}.\n"
                    "Попробуй ослабить жанр/эпоху, отключить фильтр в Настройках или нажми «Подобрать ещё»."
                )
            await callback.message.edit_text(text, reply_markup=recommendations_control_keyboard("t250_"))
            return

        try:
            llm_picks = await get_top250_picks_from_llm(settings, mood, genre_codes, year_era, candidates)
            picks_as_dicts = [{"title": p.title, "year": p.year} for p in llm_picks.recommendations]
            films = match_picks_to_candidates(picks_as_dicts, candidates)
        except LlmError:
            logger.info("Top250 LLM failed, using random 5")
            films = candidates[:5] if len(candidates) <= 5 else random.sample(candidates, 5)
        except Exception as e:
            logger.warning("Top250 LLM/network error, using random 5: %s", e)
            films = candidates[:5] if len(candidates) <= 5 else random.sample(candidates, 5)

        if not films:
            films = candidates[:5]

        await state.update_data(recommendations=films)
        await callback.message.edit_text(header_msg)
        for idx, rec in enumerate(films):
            if rec.get("kinopoisk_id") and not rec.get("movie_id"):
                rec["movie_id"] = await get_movie_id_by_kinopoisk(settings, rec["kinopoisk_id"])
            if not (rec.get("age_rating") or "").strip() and rec.get("kinopoisk_id") and settings.kinopoisk_api_key:
                try:
                    extra = await ensure_movie_details_by_id(settings, int(rec["kinopoisk_id"]))
                    if extra:
                        rec.update(extra)
                except (TypeError, ValueError) as e:
                    logger.debug("Top250 ensure_movie_details_by_id %s: %s", rec.get("kinopoisk_id"), e)
            parts = [
                f"{idx + 1}. <b>{rec.get('title') or '—'}</b>",
            ]
            age_raw = (rec.get("age_rating") or "").strip()
            age_str = f"{age_raw}+" if age_raw and not str(age_raw).endswith("+") else (age_raw or "—")
            rating_str = rec.get("rating_kp")
            if rating_str is not None and isinstance(rating_str, (int, float)):
                rating_str = f"{float(rating_str):.1f}"
            else:
                rating_str = "—"
            parts.append(f"🔞 {age_str}   ⭐ Кинопоиск: {rating_str}")
            pos = rec.get("position")
            if pos is not None:
                parts.append(f"🏆 № {pos} в Топ 250 Кинопоиска")
            nominated_oscar, won_oscar = await get_oscar_flags(settings, kinopoisk_id=rec.get("kinopoisk_id"))
            if won_oscar:
                parts.append("🏆 Победитель Оскара")
            elif nominated_oscar:
                parts.append("📋 Номинант Оскара")
            if rec.get("year"):
                parts[0] += f" ({rec['year']})"
            if rec.get("genres"):
                parts.append("🎭 " + (rec["genres"][:80] + "…" if len(rec.get("genres", "")) > 80 else rec["genres"]))
            if (rec.get("countries") or "").strip():
                parts.append("Страна: " + (rec["countries"] or "").strip())
            short_desc = (rec.get("short_description") or "").strip()
            if short_desc:
                parts.append("📝 " + short_desc)
            text = "\n".join(parts)
            in_fav = await is_favorite(callback.from_user.id, rec)
            in_watched = await is_watched(callback.from_user.id, rec)
            in_ni = await is_not_interested(callback.from_user.id, rec)
            fav_l = "✅ В избранном" if in_fav else "⭐️ В избранное"
            watched_l = "✅ Смотрел" if in_watched else "🎬 Смотрел"
            not_int_l = "✅ Не интересно" if in_ni else "👎 Не интересно"
            mid = rec.get("movie_id")
            t_fav = f"t250_fav:m:{mid}" if mid is not None else f"t250_fav:i:{idx}"
            t_watched = f"t250_watched:m:{mid}" if mid is not None else f"t250_watched:i:{idx}"
            t_ni = f"t250_not_interested:m:{mid}" if mid is not None else f"t250_not_interested:i:{idx}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=fav_l, callback_data=t_fav),
                        InlineKeyboardButton(text=watched_l, callback_data=t_watched),
                    ],
                    [InlineKeyboardButton(text=not_int_l, callback_data=t_ni)],
                ]
            )
            try:
                urls = rec.get("poster_urls") or ([rec.get("poster_url")] if rec.get("poster_url") else [])
                settings = load_settings()
                await _send_movie_card(callback.message, urls, text, kb, settings)
            except Exception as e:
                logger.warning("Top250: не удалось отправить карточку фильма %s: %s", rec.get("title"), e)
                await callback.message.answer(text, reply_markup=kb)
        try:
            await record_shown(settings, callback.from_user.id, delivery_number, films)
        except Exception as e:
            logger.warning("record_shown (Top250) failed: %s", e)
        await callback.message.answer(
            "Можем подобрать ещё или вернуться в меню 👇",
            reply_markup=recommendations_control_keyboard("t250_"),
        )

    @router.callback_query(F.data.startswith("t250_fav:"))
    async def top250_fav(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, idx = _parse_fav_cb(callback.data, "t250_fav")
        if movie_id is not None:
            added = await add_favorite_by_movie_id(user_id, movie_id)
            await callback.answer("Добавлено в избранное ⭐️" if added else "Уже в избранном 👍")
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx is None or idx < 0 or idx >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[idx]
        added = await add_favorite_for_user(user_id, rec)
        await callback.answer("Добавлено в избранное ⭐️" if added else "Уже в избранном 👍")
        in_watched = await is_watched(user_id, rec)
        in_ni = await is_not_interested(user_id, rec)
        mid = rec.get("movie_id")
        t_fav = f"t250_fav:m:{mid}" if mid is not None else f"t250_fav:i:{idx}"
        t_watched = f"t250_watched:m:{mid}" if mid is not None else f"t250_watched:i:{idx}"
        t_ni = f"t250_not_interested:m:{mid}" if mid is not None else f"t250_not_interested:i:{idx}"
        not_int_l = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном", callback_data=t_fav),
                    InlineKeyboardButton(text="✅ Смотрел" if in_watched else "🎬 Смотрел", callback_data=t_watched),
                ],
                [InlineKeyboardButton(text=not_int_l, callback_data=t_ni)],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(F.data.startswith("t250_watched:"))
    async def top250_watched(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, idx = _parse_fav_cb(callback.data, "t250_watched")
        if movie_id is not None:
            added = await add_watched_by_movie_id(user_id, movie_id)
            await callback.answer("Добавлено в «Смотрел» 🎬" if added else "Уже в списке 👍")
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx is None or idx < 0 or idx >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[idx]
        added = await add_watched_for_user(user_id, rec)
        await callback.answer("Добавлено в «Смотрел» 🎬" if added else "Уже в списке 👍")
        in_fav = await is_favorite(user_id, rec)
        in_ni = await is_not_interested(user_id, rec)
        mid = rec.get("movie_id")
        t_fav = f"t250_fav:m:{mid}" if mid is not None else f"t250_fav:i:{idx}"
        t_watched = f"t250_watched:m:{mid}" if mid is not None else f"t250_watched:i:{idx}"
        t_ni = f"t250_not_interested:m:{mid}" if mid is not None else f"t250_not_interested:i:{idx}"
        not_int_l = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном" if in_fav else "⭐️ В избранное", callback_data=t_fav),
                    InlineKeyboardButton(text="✅ Смотрел", callback_data=t_watched),
                ],
                [InlineKeyboardButton(text=not_int_l, callback_data=t_ni)],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(F.data.startswith("t250_not_interested:"))
    async def top250_not_interested(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, idx = _parse_fav_cb(callback.data, "t250_not_interested")
        if movie_id is not None:
            added = await add_not_interested_by_movie_id(user_id, movie_id)
            await callback.answer("Отметил: не интересно 👎" if added else "Уже в списке «Не интересно»")
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx is None or idx < 0 or idx >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[idx]
        added = await add_not_interested(user_id, rec)
        await callback.answer("Отметил: не интересно 👎" if added else "Уже в списке «Не интересно»")
        in_fav = await is_favorite(user_id, rec)
        in_watched = await is_watched(user_id, rec)
        mid = rec.get("movie_id")
        t_fav = f"t250_fav:m:{mid}" if mid is not None else f"t250_fav:i:{idx}"
        t_watched = f"t250_watched:m:{mid}" if mid is not None else f"t250_watched:i:{idx}"
        t_ni = f"t250_not_interested:m:{mid}" if mid is not None else f"t250_not_interested:i:{idx}"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном" if in_fav else "⭐️ В избранное", callback_data=t_fav),
                    InlineKeyboardButton(text="✅ Смотрел" if in_watched else "🎬 Смотрел", callback_data=t_watched),
                ],
                [InlineKeyboardButton(text="✅ Не интересно", callback_data=t_ni)],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(Top250Flow.recommendations, F.data == "t250_reco:again")
    async def top250_reco_again(callback: CallbackQuery, state: FSMContext) -> None:
        """Подобрать ещё — возврат к выбору года, затем новая подборка из 5."""
        await callback.answer()
        await state.set_state(Top250Flow.year_era)
        await callback.message.answer(
            "Выбери год (эпоху) ещё раз — покажу новую подборку 👇",
            reply_markup=year_era_keyboard("t250_"),
        )

    @router.callback_query(Top250Flow.recommendations, F.data == "t250_reco:menu")
    async def top250_reco_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await callback.message.answer("Главное меню 👇", reply_markup=main_menu_keyboard())

    # Fallback: кнопки Топ 250 без состояния (после перезапуска бота и т.п.)
    @router.callback_query(F.data == "t250_reco:menu")
    async def fallback_t250_reco_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await callback.message.answer("Главное меню 👇", reply_markup=main_menu_keyboard())

    @router.callback_query(F.data == "t250_reco:again")
    async def fallback_t250_reco_again(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.set_state(Top250Flow.year_era)
        await callback.message.answer(
            "Выбери год (эпоху) ещё раз — покажу новую подборку 👇",
            reply_markup=year_era_keyboard("t250_"),
        )

    # --- Ветка Оскар ---
    @router.callback_query(OscarFlow.type_filter, F.data.startswith("oscar_type:"))
    async def oscar_type(callback: CallbackQuery, state: FSMContext) -> None:
        type_filter = callback.data.split(":", 1)[1]
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["oscar_type"] = type_filter
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "oscar_type", type_filter)
        await state.set_state(OscarFlow.year_era)
        await callback.message.edit_text(
            "Выбери <b>период</b> (год церемонии) 👇",
            reply_markup=oscar_year_keyboard(),
        )
        await callback.answer()

    @router.callback_query(OscarFlow.year_era, F.data.startswith("oscar_year:"))
    async def oscar_year(callback: CallbackQuery, state: FSMContext) -> None:
        year_era = callback.data.split(":", 1)[1]
        data = await state.get_data()
        prefs = data.get("preferences", {})
        type_filter = prefs.get("oscar_type") or "all"
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences={**prefs, "oscar_year_era": year_era}, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "oscar_year", year_era)
        await callback.answer()
        try:
            await callback.message.edit_text("Подбираю фильмы по Оскару… 🎬")
        except Exception:
            pass

        settings = load_settings()
        candidates: List[Dict[str, Any]] = []
        try:
            candidates = await get_filtered_oscar(settings, type_filter, year_era, limit=100)
        except Exception as e:
            logger.exception("Oscar get_filtered_oscar failed: %s", e)
            err_msg = str(e).strip()[:200].replace("<", " ").replace(">", " ")
            text = (
                "Не удалось загрузить список Оскара. Попробуй позже или выбери другой подбор.\n\n"
                "Если список ещё не загружался: на сервере выполни из корня проекта:\n"
                "<code>python scripts/parse_oscar_wikipedia.py</code>"
            )
            if err_msg:
                text += f"\n\nДля администратора: <code>{err_msg}</code>"
            await callback.message.edit_text(
                text,
                reply_markup=recommendations_control_keyboard("oscar_"),
            )
            return

        ni_kp = set()
        watched_kp = set()
        try:
            ni_kp = await get_not_interested_kinopoisk_ids(callback.from_user.id)
            watched_kp = await get_watched_kinopoisk_ids(callback.from_user.id)
        except Exception:
            pass
        candidates = [
            c for c in candidates
            if (c.get("kinopoisk_id") or 0) not in ni_kp and (c.get("kinopoisk_id") or 0) not in watched_kp
        ]
        min_rating_on = await get_min_rating_filter_enabled(callback.from_user.id, settings)
        if min_rating_on:
            # Фильтр по рейтингу только для уже привязанных; без привязки показываем
            candidates = [
                c for c in candidates
                if c.get("movie_id") is None
                or c.get("rating_kp") is None
                or (isinstance(c.get("rating_kp"), (int, float)) and float(c["rating_kp"]) >= 6.0)
            ]
        exclude_movie_ids, exclude_kinopoisk_ids = await get_recently_shown_ids(
            settings, callback.from_user.id, RECENT_DELIVERIES_COUNT
        )
        candidates = filter_out_recently_shown(candidates, exclude_movie_ids, exclude_kinopoisk_ids)
        delivery_number = await get_next_delivery_number(settings, callback.from_user.id)

        await state.set_state(OscarFlow.recommendations)
        total = await get_oscar_count(settings, with_movie_only=False)
        if not candidates:
            if total == 0:
                text = (
                    "В базе пока нет номинаций Оскара.\n\n"
                    "Запусти: python scripts/parse_oscar_wikipedia.py"
                )
            else:
                text = "По выбранным фильтрам ничего не нашлось или все отфильтрованы (смотрел / не интересно / рейтинг). Попробуй другой период или тип."
            await callback.message.edit_text(text, reply_markup=recommendations_control_keyboard("oscar_"))
            return

        films = candidates[:5] if len(candidates) <= 5 else random.sample(candidates, 5)
        await state.update_data(recommendations=films)
        await callback.message.edit_text("Вот подборка по Оскару 🏆")
        for idx, rec in enumerate(films):
            # Если нет kinopoisk_id и данных из Кинопоиска — пробуем подтянуть при выдаче (и при movie_id, и без)
            if rec.get("kinopoisk_id") is None and rec.get("oscar_id") and settings.kinopoisk_api_key:
                title = (rec.get("title") or rec.get("title_from_source") or "").strip()
                year = rec.get("year") or rec.get("year_from_source")
                if year is None and rec.get("ceremony_year"):
                    year = int(rec["ceremony_year"]) - 1
                if title:
                    try:
                        ok = await refresh_movie_from_api(settings, title=title, year=year)
                        if not ok and year is not None:
                            ok = await refresh_movie_from_api(settings, title=title, year=year - 1)
                        if not ok and year is not None:
                            ok = await refresh_movie_from_api(settings, title=title, year=year + 1)
                        info = await get_movie_info(settings, title, year) if ok else None
                        if info is None and year is not None:
                            info = await get_movie_info(settings, title, year - 1)
                        if info is None and year is not None:
                            info = await get_movie_info(settings, title, year + 1)
                        if info:
                            rec["kinopoisk_id"] = info.kinopoisk_id
                            rec["age_rating"] = info.age_rating
                            rec["rating_kp"] = info.rating_kp
                            rec["poster_url"] = info.poster_url
                            rec["poster_urls"] = info.poster_urls
                            rec["short_description"] = info.short_description
                            rec["countries"] = info.countries
                            if rec.get("year") is None and year is not None:
                                rec["year"] = year
                            if rec.get("movie_id") is None and info.kinopoisk_id is not None:
                                movie_id = await get_movie_id_by_kinopoisk(settings, info.kinopoisk_id)
                                if movie_id:
                                    await link_oscar_to_movie(settings, rec["oscar_id"], movie_id)
                                    rec["movie_id"] = movie_id
                    except Exception as e:
                        logger.debug("Oscar on-the-fly Kinopoisk enrich for %s: %s", title, e)

            parts = [f"{idx + 1}. <b>{rec.get('title') or rec.get('title_from_source') or '—'}</b>"]
            if rec.get("year"):
                parts[0] += f" ({rec['year']})"
            if rec.get("is_winner"):
                parts.append("🏆 Победитель Оскара")
            else:
                parts.append("📋 Номинант Оскара")
            age_raw = (rec.get("age_rating") or "").strip()
            age_str = f"{age_raw}+" if age_raw and not str(age_raw).endswith("+") else (age_raw or "—")
            rating_str = rec.get("rating_kp")
            if rating_str is not None and isinstance(rating_str, (int, float)):
                rating_str = f"{float(rating_str):.1f}"
            else:
                rating_str = "—"
            parts.append(f"🔞 {age_str}   ⭐ Кинопоиск: {rating_str}")
            if rec.get("genres"):
                parts.append("🎭 " + (rec["genres"][:80] + "…" if len(rec.get("genres", "")) > 80 else rec["genres"]))
            if (rec.get("countries") or "").strip():
                parts.append("Страна: " + (rec["countries"] or "").strip())
            if rec.get("short_description"):
                parts.append("📝 " + (rec["short_description"] or "").strip())
            text = "\n".join(parts)
            in_fav = await is_favorite(callback.from_user.id, rec)
            in_watched = await is_watched(callback.from_user.id, rec)
            in_ni = await is_not_interested(callback.from_user.id, rec)
            kb = _oscar_card_kb(idx, rec.get("movie_id"), in_fav, in_watched, in_ni)
            urls = rec.get("poster_urls") or ([rec.get("poster_url")] if rec.get("poster_url") else [])
            try:
                await _send_movie_card(callback.message, urls or [], text, kb, settings)
            except Exception as e:
                logger.warning("Oscar: не удалось отправить карточку %s: %s", rec.get("title"), e)
                await callback.message.answer(text, reply_markup=kb)
        try:
            await record_shown(settings, callback.from_user.id, delivery_number, films)
        except Exception as e:
            logger.warning("record_shown (Oscar) failed: %s", e)
        await callback.message.answer(
            "Можем подобрать ещё или вернуться в меню 👇",
            reply_markup=recommendations_control_keyboard("oscar_"),
        )

    def _parse_oscar_cb(data: str, prefix: str) -> tuple[Optional[int], Optional[int]]:
        """Возвращает (movie_id, idx): m:123 -> (123, None), i:0 -> (None, 0)."""
        if f"{prefix}:" not in data:
            return None, None
        suffix = data.split(prefix, 1)[1].lstrip(":")
        if suffix.startswith("m:"):
            try:
                return int(suffix[2:]), None
            except ValueError:
                return None, None
        if suffix.startswith("i:"):
            try:
                return None, int(suffix[2:])
            except ValueError:
                return None, None
        return None, None

    @router.callback_query(F.data.startswith("oscar_fav:"))
    async def oscar_fav(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, idx = _parse_oscar_cb(callback.data, "oscar_fav")
        if movie_id is not None:
            added = await add_favorite_by_movie_id(user_id, movie_id)
            await callback.answer("Добавлено в избранное ⭐️" if added else "Уже в избранном 👍")
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx is None or idx < 0 or idx >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[idx]
        added = await add_favorite_for_user(user_id, rec)
        await callback.answer("Добавлено в избранное ⭐️" if added else "Уже в избранном 👍")
        in_watched = await is_watched(user_id, rec)
        in_ni = await is_not_interested(user_id, rec)
        await _update_oscar_card_buttons(callback, idx, rec.get("movie_id"), in_fav=True, in_watched=in_watched, in_ni=in_ni)

    @router.callback_query(F.data.startswith("oscar_watched:"))
    async def oscar_watched(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, idx = _parse_oscar_cb(callback.data, "oscar_watched")
        if movie_id is not None:
            added = await add_watched_by_movie_id(user_id, movie_id)
            await callback.answer("Добавлено в «Смотрел» 🎬" if added else "Уже в списке 👍")
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx is None or idx < 0 or idx >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[idx]
        added = await add_watched_for_user(user_id, rec)
        await callback.answer("Добавлено в «Смотрел» 🎬" if added else "Уже в списке 👍")
        in_fav = await is_favorite(user_id, rec)
        in_ni = await is_not_interested(user_id, rec)
        await _update_oscar_card_buttons(callback, idx, rec.get("movie_id"), in_fav=in_fav, in_watched=True, in_ni=in_ni)

    @router.callback_query(F.data.startswith("oscar_not_interested:"))
    async def oscar_not_interested(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, idx = _parse_oscar_cb(callback.data, "oscar_not_interested")
        if movie_id is not None:
            added = await add_not_interested_by_movie_id(user_id, movie_id)
            await callback.answer("Отметил: не интересно 👎" if added else "Уже в списке «Не интересно»")
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx is None or idx < 0 or idx >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[idx]
        added = await add_not_interested(user_id, rec)
        await callback.answer("Отметил: не интересно 👎" if added else "Уже в списке «Не интересно»")
        in_fav = await is_favorite(user_id, rec)
        in_watched = await is_watched(user_id, rec)
        await _update_oscar_card_buttons(callback, idx, rec.get("movie_id"), in_fav=in_fav, in_watched=in_watched, in_ni=True)

    @router.callback_query(F.data == "oscar_reco:again")
    async def oscar_reco_again(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.set_state(OscarFlow.year_era)
        await callback.message.answer(
            "Выбери период ещё раз 👇",
            reply_markup=oscar_year_keyboard(),
        )

    @router.callback_query(F.data == "oscar_reco:menu")
    async def oscar_reco_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await callback.message.answer("Главное меню 👇", reply_markup=main_menu_keyboard())

    # 1. Настроение (обычный подбор)
    @router.callback_query(MovieFlow.mood, F.data.startswith("mood:"))
    async def choose_mood(callback: CallbackQuery, state: FSMContext) -> None:
        mood_code = callback.data.split(":", 1)[1]
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["mood"] = mood_code
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "mood", mood_code)

        await state.set_state(MovieFlow.genres)
        await callback.message.edit_text(
            "Отлично! Теперь выбери <b>жанры</b>.\n"
            "Можно несколько вариантов, потом нажми «✅ Готово».",
            reply_markup=genres_keyboard(set()),
        )
        await callback.answer()

    # 2. Жанры (мультивыбор)
    @router.callback_query(MovieFlow.genres)
    async def choose_genres(callback: CallbackQuery, state: FSMContext) -> None:
        data = await state.get_data()
        prefs = data.get("preferences", {})
        selected = set(prefs.get("genres", []))

        if callback.data == "genres_done":
            if not selected:
                await callback.answer("Выбери хотя бы один жанр 🙏", show_alert=True)
                return

            session_id = data.get("session_id") or uuid.uuid4().hex
            await log_flow_step(callback.from_user.id, session_id, "genres", ",".join(sorted(selected)))
            prefs["duration"] = "any"
            await state.update_data(preferences=prefs)
            await state.set_state(MovieFlow.age)
            await callback.message.edit_text(
                "Какой <b>возрастной рейтинг</b> подходит? 🔞\n\n"
                "Проверить рейтинг: <a href=\"https://www.kinopoisk.ru\">Кинопоиск</a>",
                reply_markup=age_keyboard(),
            )
            await callback.answer()
            return

        if callback.data == "genres_skip":
            prefs["genres"] = []
            prefs["duration"] = "any"
            session_id = data.get("session_id") or uuid.uuid4().hex
            await state.update_data(preferences=prefs, session_id=session_id)
            await log_flow_step(callback.from_user.id, session_id, "genres", "skip")
            await state.set_state(MovieFlow.age)
            await callback.message.edit_text(
                "Какой <b>возрастной рейтинг</b> подходит? 🔞\n\n"
                "Проверить рейтинг: <a href=\"https://www.kinopoisk.ru\">Кинопоиск</a>",
                reply_markup=age_keyboard(),
            )
            await callback.answer()
            return

        if callback.data.startswith("genre:"):
            code = callback.data.split(":", 1)[1]
            if code in selected:
                selected.remove(code)
            else:
                selected.add(code)
            prefs["genres"] = list(selected)
            await state.update_data(preferences=prefs)
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=genres_keyboard(selected)
                )
            except TelegramBadRequest as e:
                if "message is not modified" not in str(e).lower():
                    raise
            await callback.answer()

    # 3. Возраст
    @router.callback_query(MovieFlow.age, F.data.startswith("age:"))
    async def choose_age(callback: CallbackQuery, state: FSMContext) -> None:
        age_code = callback.data.split(":", 1)[1]
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["age"] = age_code
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "age", age_code)

        await state.set_state(MovieFlow.company)
        await callback.message.edit_text(
            "С кем будем смотреть? 👀",
            reply_markup=company_keyboard(),
        )
        await callback.answer()

    # 5. С кем смотрим
    @router.callback_query(MovieFlow.company, F.data.startswith("comp:"))
    async def choose_company(callback: CallbackQuery, state: FSMContext) -> None:
        company_code = callback.data.split(":", 1)[1]
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["company"] = company_code
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "company", company_code)

        await state.set_state(MovieFlow.negative)
        await state.update_data(negative_codes=[])
        await callback.message.edit_text(
            "Почти готово! ✨\n\n"
            "<b>Есть ли что-то, чего точно НЕ хочешь видеть в фильме?</b>\n"
            "Можно выбрать несколько пунктов, затем нажать «Готово».",
            reply_markup=negative_keyboard(set()),
        )
        await callback.answer()

    NEGATIVE_TO_PROMPT = {
        "neg:violence": "жестокость, насилие",
        "neg:heavydrama": "тяжёлые драмы, депрессивные",
        "neg:old": "старое кино, до 1990-х",
        "neg:sad": "грустный финал, несчастливый конец",
        "neg:none": "",
    }

    async def _do_recommendations(
        responder: Message,
        state: FSMContext,
        user_id: int,
        prefs: Dict[str, Any],
        negative_text: str,
    ) -> None:
        settings = load_settings()
        t_total = time.perf_counter()
        await responder.answer("Супер, думаю над вариантами фильма 🎬\nДай мне пару секунд…")
        try:
            t0 = time.perf_counter()
            llm_response = await get_recommendations_from_llm(
                settings=settings,
                user_id=user_id,
                preferences=prefs,
                negative=negative_text,
            )
            _log_stage(user_id, "llm", time.perf_counter() - t0, count=len(llm_response.recommendations))
        except LlmError as e:
            await responder.answer(
                "😔 Не получилось получить рекомендации от ИИ.\n"
                "Попробуем ещё раз чуть позже.\n\n"
                f"Техническая деталь: {e.user_message}",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return

        await state.set_state(MovieFlow.recommendations)

        await responder.answer(f"📝 <b>Кратко о твоих предпочтениях</b>:\n{llm_response.session_summary}\n")

        # Сначала только из БД (без запросов в API): рейтинг/возраст для фильтра и для выдачи, если уже есть
        t0 = time.perf_counter()
        db_infos: List[Optional[KinopoiskMovieInfo]] = await asyncio.gather(
            *[get_movie_from_db(settings, title=rec.title, year=rec.year) for rec in llm_response.recommendations]
        )
        kinopoisk_infos = db_infos
        _log_stage(user_id, "kinopoisk_db_only", time.perf_counter() - t0, count=len(kinopoisk_infos))

        # Фильтр по рейтингу: только если у пользователя включена настройка (нет рейтинга = показываем)
        t0 = time.perf_counter()
        min_rating_on = await get_min_rating_filter_enabled(user_id, settings)
        filtered_pairs: List[tuple] = []
        for rec, info in zip(llm_response.recommendations, kinopoisk_infos):
            if min_rating_on and info is not None and info.rating_kp is not None and info.rating_kp < 6.0:
                continue
            filtered_pairs.append((rec, info))
        _log_stage(user_id, "filter_rating", time.perf_counter() - t0, after=len(filtered_pairs))

        # Исключить фильмы «Не интересно» и «Смотрел»
        t0 = time.perf_counter()
        ni_kp = await get_not_interested_kinopoisk_ids(user_id)
        ni_movies = await get_not_interested_movie_ids(user_id)
        watched_kp = await get_watched_kinopoisk_ids(user_id)
        watched_movies = await get_watched_movie_ids(user_id)
        n_before_filter = len(filtered_pairs)
        pairs_filtered: List[tuple] = []
        for rec, info in filtered_pairs:
            rec_d = {**rec.model_dump(), "kinopoisk_id": (info.kinopoisk_id if info else None) or rec.model_dump().get("kinopoisk_id")}
            if await rec_in_not_interested(user_id, rec_d, ni_kinopoisk_ids=ni_kp, ni_movie_ids=ni_movies):
                continue
            if await rec_in_watched(rec_d, watched_kinopoisk_ids=watched_kp, watched_movie_ids=watched_movies):
                continue
            pairs_filtered.append((rec, info))
        filtered_pairs = pairs_filtered
        _log_stage(user_id, "filter_ni_watched", time.perf_counter() - t0, after=len(filtered_pairs))

        # Исключить фильмы, показанные в последних N выдачах
        t0 = time.perf_counter()
        delivery_number = await get_next_delivery_number(settings, user_id)
        exclude_movie_ids, exclude_kinopoisk_ids = await get_recently_shown_ids(
            settings, user_id, RECENT_DELIVERIES_COUNT
        )
        if exclude_kinopoisk_ids or exclude_movie_ids:
            pairs_filtered = []
            for rec, info in filtered_pairs:
                kp = (info.kinopoisk_id if info else None) or getattr(rec, "kinopoisk_id", None)
                if kp is not None and kp in exclude_kinopoisk_ids:
                    continue
                pairs_filtered.append((rec, info))
            filtered_pairs = pairs_filtered
        _log_stage(user_id, "filter_recently", time.perf_counter() - t0, after=len(filtered_pairs))

        if not filtered_pairs:
            data = await state.get_data()
            session_id = data.get("session_id") or uuid.uuid4().hex
            await log_flow_step(user_id, session_id, "recommendations", "0")
            if n_before_filter > 0:
                await responder.answer(
                    "Все эти варианты ты уже отметил как неинтересные 👎 или смотрел 🎬\nНажми «Подобрать ещё» — подберу другие фильмы.",
                    reply_markup=recommendations_control_keyboard(),
                )
            else:
                msg = (
                    "По твоим критериям не нашлось подходящих фильмов."
                    if not min_rating_on
                    else "По твоим критериям не нашлось фильмов с рейтингом Кинопоиска ≥ 6.0 (включено в Настройках). Попробуй отключить или подобрать ещё раз 🔁"
                )
                await responder.answer(msg, reply_markup=recommendations_control_keyboard())
            await state.set_state(MovieFlow.recommendations)
            await state.update_data(recommendations=[])
            return

        # Показываем не более 5 фильмов; если после фильтров осталось больше — берём первые 5
        RECOMMENDATIONS_TO_SHOW = 5
        pairs_to_show = filtered_pairs[:RECOMMENDATIONS_TO_SHOW]

        # В API Кинопоиска идём только по выбранным для выдачи и только если в БД ещё нет kinopoisk_id
        t0 = time.perf_counter()
        enriched_pairs: List[tuple] = []
        for rec, info in pairs_to_show:
            if info is None or info.kinopoisk_id is None:
                info = await get_movie_info(settings, rec.title, rec.year)
            enriched_pairs.append((rec, info))
        pairs_to_show = enriched_pairs
        _log_stage(user_id, "kinopoisk_gather", time.perf_counter() - t0, count=len(pairs_to_show))

        # Сохраняем в состоянии только то, что показываем (до 5)
        t0 = time.perf_counter()
        recs_for_state: List[Dict[str, Any]] = []
        for rec, info in pairs_to_show:
            rec_dict = rec.model_dump()
            if info:
                if info.kinopoisk_id is not None:
                    rec_dict["kinopoisk_id"] = info.kinopoisk_id
                    mid = await get_movie_id_by_kinopoisk(settings, info.kinopoisk_id)
                    if mid is not None:
                        rec_dict["movie_id"] = mid
                if info.age_rating:
                    rec_dict["age_rating"] = info.age_rating
                if info.rating_kp is not None:
                    rec_dict["rating_kp"] = info.rating_kp
                if info.countries:
                    rec_dict["countries"] = info.countries
            else:
                # Фильм не найден в Кинопоиске — всё равно добавляем в movies для ночного дозаполнения (ИИ уточнит название, повторный поиск)
                mid = await get_or_create_movie(
                    kinopoisk_id=None,
                    title=rec.title,
                    year=rec.year,
                    age_rating=None,
                    rating_kp=None,
                )
                if mid is not None:
                    rec_dict["movie_id"] = mid
            recs_for_state.append(rec_dict)
        await state.update_data(recommendations=recs_for_state)
        _log_stage(user_id, "movie_id_resolve_and_state", time.perf_counter() - t0, count=len(pairs_to_show))

        data = await state.get_data()
        session_id = data.get("session_id") or uuid.uuid4().hex
        await log_flow_step(user_id, session_id, "recommendations", str(len(pairs_to_show)))

        t0 = time.perf_counter()
        top250_positions = await get_top250_positions_map(settings)
        _log_stage(user_id, "top250_positions", time.perf_counter() - t0)

        t0_cards = time.perf_counter()
        for idx, (rec, info) in enumerate(pairs_to_show):
            parts: List[str] = []
            title_line = f"{idx + 1}. <b>{rec.title}</b>"
            if rec.year:
                title_line += f" ({rec.year})"
            parts.append(title_line)
            # Возрастное ограничение и рейтинг Кинопоиска в карточке (всегда показываем строку)
            age_str = f"🔞 {info.age_rating}+" if info and info.age_rating else "🔞 Возраст: —"
            rating_str = f"⭐ Кинопоиск: {info.rating_kp:.1f}" if info and info.rating_kp is not None else "⭐ Кинопоиск: —"
            parts.append(f"{age_str}   {rating_str}")
            if info and info.kinopoisk_id is not None and info.kinopoisk_id in top250_positions:
                parts.append(f"🏆 № {top250_positions[info.kinopoisk_id]} в Топ 250 Кинопоиска")
            nominated_oscar, won_oscar = await get_oscar_flags(settings, kinopoisk_id=info.kinopoisk_id if info else None)
            if won_oscar:
                parts.append("🏆 Победитель Оскара")
            elif nominated_oscar:
                parts.append("📋 Номинант Оскара")
            if info and (info.short_description or "").strip():
                parts.append("📝 " + (info.short_description or "").strip())
            if info and (info.countries or "").strip():
                parts.append("Страна: " + (info.countries or "").strip())
            if rec.genres:
                parts.append("🎭 Жанры: " + ", ".join(rec.genres))
            if rec.mood_tags:
                parts.append("🔖 Настроение: " + " ".join(rec.mood_tags))
            if rec.why:
                parts.append("💡 Почему подходит: " + rec.why)
            if rec.warnings:
                parts.append("⚠️ Предупреждения: " + "; ".join(rec.warnings))
            if rec.similar_if_liked:
                parts.append("🎞 Понравится, если любишь: " + ", ".join(rec.similar_if_liked))
            text = "\n".join(parts)
            rec_dict = recs_for_state[idx]
            in_fav = await is_favorite(user_id, rec_dict)
            in_watched = await is_watched(user_id, rec_dict)
            in_ni = await is_not_interested(user_id, rec_dict)
            fav_label = "✅ В избранном" if in_fav else "⭐️ В избранное"
            watched_label = "✅ Смотрел" if in_watched else "🎬 Смотрел"
            not_int_label = "✅ Не интересно" if in_ni else "👎 Не интересно"
            mid = rec_dict.get("movie_id")
            fav_data = f"fav:m:{mid}" if mid is not None else f"fav:i:{idx}"
            w_data = f"watched:m:{mid}" if mid is not None else f"watched:i:{idx}"
            ni_data = f"not_interested:m:{mid}" if mid is not None else f"not_interested:i:{idx}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=fav_label, callback_data=fav_data),
                        InlineKeyboardButton(text=watched_label, callback_data=w_data),
                    ],
                    [InlineKeyboardButton(text=not_int_label, callback_data=ni_data)],
                ]
            )
            urls = (info.poster_urls or ([info.poster_url] if info and info.poster_url else [])) if info else []
            await _send_movie_card(responder, urls, text, kb, settings)

        _log_stage(user_id, "build_and_send_cards", time.perf_counter() - t0_cards, count=len(pairs_to_show))
        try:
            await record_shown(settings, user_id, delivery_number, recs_for_state)
        except Exception as e:
            logger.warning("record_shown (ordinary) failed: %s", e)

        total_sec = time.perf_counter() - t_total
        _log_stage(user_id, "total", total_sec, films_shown=len(pairs_to_show))
        logger.info(
            "movie_flow_timing user_id=%s summary: total_sec=%.2f (llm + kinopoisk + filters + cards)",
            user_id, total_sec,
        )
        await responder.answer(
            "Если хочешь — можем подобрать ещё варианты или вернуться в меню 👇",
            reply_markup=recommendations_control_keyboard(),
        )

    @router.callback_query(MovieFlow.negative, F.data.startswith("neg:"))
    async def negative_choice(callback: CallbackQuery, state: FSMContext) -> None:
        data = await state.get_data()
        prefs = data.get("preferences", {})
        session_id = data.get("session_id") or uuid.uuid4().hex
        selected = set(data.get("negative_codes") or [])

        if callback.data == "neg:none":
            await state.update_data(negative="", negative_codes=[], session_id=session_id)
            await log_flow_step(callback.from_user.id, session_id, "negative", "neg:none")
            await callback.answer()
            await _do_recommendations(callback.message, state, callback.from_user.id, prefs, "")
            return

        if callback.data == "neg:done":
            negative_text = ", ".join(
                NEGATIVE_TO_PROMPT[c].strip()
                for c in selected
                if NEGATIVE_TO_PROMPT.get(c, "").strip()
            )
            await state.update_data(negative=negative_text, session_id=session_id)
            await log_flow_step(
                callback.from_user.id, session_id, "negative", ",".join(sorted(selected)) or "none"
            )
            await callback.answer()
            await _do_recommendations(
                callback.message, state, callback.from_user.id, prefs, negative_text
            )
            return

        # Переключение пункта
        if callback.data in selected:
            selected.discard(callback.data)
        else:
            selected.add(callback.data)
        await state.update_data(negative_codes=list(selected), session_id=session_id)
        try:
            await callback.message.edit_reply_markup(reply_markup=negative_keyboard(selected))
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
        await callback.answer()

    def _parse_fav_cb(data: str, prefix: str) -> tuple[Optional[int], Optional[int]]:
        """Возвращает (movie_id, idx): m:123 -> (123, None), i:0 -> (None, 0)."""
        if f"{prefix}:" not in data:
            return None, None
        suffix = data.split(prefix, 1)[1].lstrip(":")
        if suffix.startswith("m:"):
            try:
                return int(suffix[2:]), None
            except ValueError:
                return None, None
        if suffix.startswith("i:"):
            try:
                return None, int(suffix[2:])
            except ValueError:
                return None, None
        return None, None

    @router.callback_query(F.data.startswith("fav:"))
    async def add_to_favorites(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, index = _parse_fav_cb(callback.data, "fav")
        if movie_id is not None:
            added = await add_favorite_by_movie_id(user_id, movie_id)
            await callback.answer("Добавлено в избранное ⭐️" if added else "Уже в избранном 👍")
            return
        data = await state.get_data()
        recs: List[Dict[str, Any]] = data.get("recommendations") or []
        if index is None or index < 0 or index >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[index]
        added = await add_favorite_for_user(user_id, rec)
        await callback.answer("Добавлено в избранное ⭐️" if added else "Уже в избранном 👍")
        in_watched = await is_watched(user_id, rec)
        in_ni = await is_not_interested(user_id, rec)
        mid = rec.get("movie_id")
        fav_data = f"fav:m:{mid}" if mid is not None else f"fav:i:{index}"
        w_data = f"watched:m:{mid}" if mid is not None else f"watched:i:{index}"
        ni_data = f"not_interested:m:{mid}" if mid is not None else f"not_interested:i:{index}"
        not_int_label = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном", callback_data=fav_data),
                    InlineKeyboardButton(text="✅ Смотрел" if in_watched else "🎬 Смотрел", callback_data=w_data),
                ],
                [InlineKeyboardButton(text=not_int_label, callback_data=ni_data)],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(F.data.startswith("watched:"))
    async def add_to_watched(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, index = _parse_fav_cb(callback.data, "watched")
        if movie_id is not None:
            added = await add_watched_by_movie_id(user_id, movie_id)
            await callback.answer("Добавлено в «Смотрел» 🎬" if added else "Уже в списке 👍")
            return
        data = await state.get_data()
        recs: List[Dict[str, Any]] = data.get("recommendations") or []
        if index is None or index < 0 or index >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[index]
        added = await add_watched_for_user(user_id, rec)
        await callback.answer("Добавлено в «Смотрел» 🎬" if added else "Уже в списке 👍")
        in_fav = await is_favorite(user_id, rec)
        in_ni = await is_not_interested(user_id, rec)
        mid = rec.get("movie_id")
        fav_data = f"fav:m:{mid}" if mid is not None else f"fav:i:{index}"
        w_data = f"watched:m:{mid}" if mid is not None else f"watched:i:{index}"
        ni_data = f"not_interested:m:{mid}" if mid is not None else f"not_interested:i:{index}"
        not_int_label = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном" if in_fav else "⭐️ В избранное", callback_data=fav_data),
                    InlineKeyboardButton(text="✅ Смотрел", callback_data=w_data),
                ],
                [InlineKeyboardButton(text=not_int_label, callback_data=ni_data)],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(F.data.startswith("not_interested:"))
    async def add_to_not_interested(callback: CallbackQuery, state: FSMContext) -> None:
        user_id = callback.from_user.id if callback.from_user else 0
        movie_id, index = _parse_fav_cb(callback.data, "not_interested")
        if movie_id is not None:
            added = await add_not_interested_by_movie_id(user_id, movie_id)
            await callback.answer("Отметил: не интересно 👎" if added else "Уже в списке «Не интересно»")
            return
        data = await state.get_data()
        recs: List[Dict[str, Any]] = data.get("recommendations") or []
        if index is None or index < 0 or index >= len(recs):
            await callback.answer("Подборка устарела. Нажмите «Подобрать ещё» и выберите фильм снова.", show_alert=True)
            return
        rec = recs[index]
        added = await add_not_interested(user_id, rec)
        await callback.answer("Отметил: не интересно 👎" if added else "Уже в списке «Не интересно»")
        in_fav = await is_favorite(user_id, rec)
        in_watched = await is_watched(user_id, rec)
        mid = rec.get("movie_id")
        fav_data = f"fav:m:{mid}" if mid is not None else f"fav:i:{index}"
        w_data = f"watched:m:{mid}" if mid is not None else f"watched:i:{index}"
        ni_data = f"not_interested:m:{mid}" if mid is not None else f"not_interested:i:{index}"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном" if in_fav else "⭐️ В избранное", callback_data=fav_data),
                    InlineKeyboardButton(text="✅ Смотрел" if in_watched else "🎬 Смотрел", callback_data=w_data),
                ],
                [InlineKeyboardButton(text="✅ Не интересно", callback_data=ni_data)],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    # Кнопки управления после рекомендаций (подобрать ещё — по тому же запросу)
    @router.callback_query(MovieFlow.recommendations, F.data == "reco:again")
    async def recommendations_again(callback: CallbackQuery, state: FSMContext) -> None:
        data = await state.get_data()
        prefs = data.get("preferences", {})
        negative_text = data.get("negative", "") or ""
        await callback.answer()
        await _do_recommendations(
            callback.message, state, callback.from_user.id, prefs, negative_text
        )

    @router.callback_query(MovieFlow.recommendations, F.data == "reco:menu")
    async def recommendations_to_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await callback.message.answer(
            "Главное меню 👇",
            reply_markup=main_menu_keyboard(),
        )

    # После перезапуска бота состояние теряется — обрабатываем кнопки без состояния
    @router.callback_query(F.data == "reco:menu")
    async def fallback_reco_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await callback.message.answer(
            "Главное меню 👇",
            reply_markup=main_menu_keyboard(),
        )

    @router.callback_query(F.data == "reco:again")
    async def fallback_reco_again(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await state.set_state(MovieFlow.mood)
        await state.update_data(preferences={})
        await callback.message.answer(
            "Давай подберём фильм! Выбери <b>настроение</b> 👇",
            reply_markup=mood_keyboard(),
        )

    return router

