from __future__ import annotations

import asyncio
import logging
import random
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from ..config import Settings
from ..keyboards.flow import (
    source_keyboard,
    mood_keyboard,
    genres_keyboard,
    duration_keyboard,
    age_keyboard,
    company_keyboard,
    negative_keyboard,
    year_era_keyboard,
    recommendations_control_keyboard,
)
from ..keyboards.main_menu import main_menu_keyboard
from ..llm.service import get_recommendations_from_llm, get_top250_picks_from_llm, LlmError
from ..services.favorites import add_favorite_for_user, add_watched_for_user, is_favorite, is_watched
from ..services.not_interested import (
    add_not_interested,
    is_not_interested,
    rec_in_not_interested,
    get_not_interested_kinopoisk_ids,
    get_not_interested_movie_ids,
)
from ..services.flow_log import log_flow_step
from ..services.kinopoisk import get_movie_info, KinopoiskMovieInfo
from ..services.top250 import get_filtered_top250, get_top250_count, get_top250_positions_map, match_picks_to_candidates
from ..services.users import ensure_user


class MovieFlow(StatesGroup):
    source = State()
    mood = State()
    genres = State()
    duration = State()
    age = State()
    company = State()
    negative = State()
    recommendations = State()


class Top250Flow(StatesGroup):
    mood = State()
    genres = State()
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
        # Оскар, фестивали — заглушка
        await state.clear()
        await callback.message.edit_text(
            "Этот режим ещё в разработке 🚧\nПока используй <b>Обычный подбор</b> или <b>Кинопоиск Топ 250</b>."
        )
        await callback.message.answer("Выбери действие:", reply_markup=main_menu_keyboard())

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
        candidates = []
        films = []
        header_msg = "Вот подборка из Кинопоиск Топ 250 🎬"

        try:
            # Кандидаты по жанру и эпохе (до 50), затем ИИ выбирает 5 по настроению и предпочтениям
            candidates = await get_filtered_top250(settings, mood, genre_codes, year_era, limit=50)
            # Исключить фильмы, отмеченные пользователем как «Не интересно»
            ni_kp = await get_not_interested_kinopoisk_ids(callback.from_user.id)
            candidates = [c for c in candidates if c.get("kinopoisk_id") not in ni_kp]
            await state.set_state(Top250Flow.recommendations)

            if not candidates:
                total = await get_top250_count(settings)
                if total == 0:
                    text = (
                        "Топ 250 ещё не загружен — возможно, лимит запросов Кинопоиска (200/день) исчерпан или это первый запуск.\n\n"
                        "Данные подгружаются при старте бота и 1-го числа каждого месяца. Попробуй перезапустить бота позже или выбери <b>Обычный подбор</b>."
                    )
                else:
                    text = "По твоим фильтрам ничего не нашлось в Топ 250 😅\nПопробуй ослабить жанр или эпоху."
                await callback.message.edit_text(text, reply_markup=recommendations_control_keyboard("t250_"))
                return

            try:
                llm_picks = await get_top250_picks_from_llm(settings, mood, genre_codes, year_era, candidates)
                picks_as_dicts = [{"title": p.title, "year": p.year} for p in llm_picks.recommendations]
                films = match_picks_to_candidates(picks_as_dicts, candidates)
            except LlmError:
                films = candidates[:5] if len(candidates) <= 5 else random.sample(candidates, 5)

            if not films:
                films = candidates[:5]

        except Exception:
            logger.exception("Top250 year_era: error after 'Подбираю фильмы'")
            if candidates:
                films = candidates[:5] if len(candidates) <= 5 else random.sample(candidates, 5)
                await state.set_state(Top250Flow.recommendations)
                header_msg = "Сервис подбора временно недоступен. Вот подборка по твоим фильтрам 🎬"
            else:
                await callback.message.edit_text(
                    "Произошла ошибка при подборе (таймаут или сеть). Попробуй ещё раз или выбери <b>Обычный подбор</b>.",
                    reply_markup=recommendations_control_keyboard("t250_"),
                )
                return

        await state.update_data(recommendations=films)
        await callback.message.edit_text(header_msg)
        for idx, rec in enumerate(films):
            parts = [
                f"{idx + 1}. <b>{rec.get('title') or '—'}</b>",
                f"🔞 {rec.get('age_rating') or '—'}+   ⭐ Кинопоиск: {rec.get('rating_kp') or '—'}",
            ]
            pos = rec.get("position")
            if pos is not None:
                parts.append(f"🏆 № {pos} в Топ 250 Кинопоиска")
            if rec.get("year"):
                parts[0] += f" ({rec['year']})"
            if rec.get("genres"):
                parts.append("🎭 " + (rec["genres"][:80] + "…" if len(rec.get("genres", "")) > 80 else rec["genres"]))
            text = "\n".join(parts)
            in_fav = await is_favorite(callback.from_user.id, rec)
            in_watched = await is_watched(callback.from_user.id, rec)
            in_ni = await is_not_interested(callback.from_user.id, rec)
            fav_l = "✅ В избранном" if in_fav else "⭐️ В избранное"
            watched_l = "✅ Посмотрел" if in_watched else "🎬 Посмотрел"
            not_int_l = "✅ Не интересно" if in_ni else "👎 Не интересно"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=fav_l, callback_data=f"t250_fav:{idx}"),
                        InlineKeyboardButton(text=watched_l, callback_data=f"t250_watched:{idx}"),
                    ],
                    [InlineKeyboardButton(text=not_int_l, callback_data=f"t250_not_interested:{idx}")],
                ]
            )
            try:
                poster_url = rec.get("poster_url")
                if poster_url and str(poster_url).strip().startswith("http"):
                    await callback.message.answer_photo(photo=poster_url, caption=text, reply_markup=kb)
                else:
                    await callback.message.answer(text, reply_markup=kb)
            except Exception as e:
                logger.warning("Top250: не удалось отправить карточку фильма %s: %s", rec.get("title"), e)
                await callback.message.answer(text, reply_markup=kb)
        await callback.message.answer(
            "Можем подобрать ещё или вернуться в меню 👇",
            reply_markup=recommendations_control_keyboard("t250_"),
        )

    @router.callback_query(Top250Flow.recommendations, F.data.startswith("t250_fav:"))
    async def top250_fav(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            idx = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer()
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx < 0 or idx >= len(recs):
            await callback.answer()
            return
        rec = recs[idx]
        added = await add_favorite_for_user(callback.from_user.id, rec)
        await callback.answer("Добавлено в избранное ⭐️" if added else "Уже в избранном 👍")
        in_watched = await is_watched(callback.from_user.id, rec)
        in_ni = await is_not_interested(callback.from_user.id, rec)
        not_int_l = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном", callback_data=f"t250_fav:{idx}"),
                    InlineKeyboardButton(
                        text="✅ Посмотрел" if in_watched else "🎬 Посмотрел",
                        callback_data=f"t250_watched:{idx}",
                    ),
                ],
                [InlineKeyboardButton(text=not_int_l, callback_data=f"t250_not_interested:{idx}")],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(Top250Flow.recommendations, F.data.startswith("t250_watched:"))
    async def top250_watched(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            idx = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer()
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx < 0 or idx >= len(recs):
            await callback.answer()
            return
        rec = recs[idx]
        added = await add_watched_for_user(callback.from_user.id, rec)
        await callback.answer("Добавлено в «Посмотрел» 🎬" if added else "Уже в списке 👍")
        in_fav = await is_favorite(callback.from_user.id, rec)
        in_ni = await is_not_interested(callback.from_user.id, rec)
        not_int_l = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ В избранном" if in_fav else "⭐️ В избранное",
                        callback_data=f"t250_fav:{idx}",
                    ),
                    InlineKeyboardButton(text="✅ Посмотрел", callback_data=f"t250_watched:{idx}"),
                ],
                [InlineKeyboardButton(text=not_int_l, callback_data=f"t250_not_interested:{idx}")],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(Top250Flow.recommendations, F.data.startswith("t250_not_interested:"))
    async def top250_not_interested(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            idx = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer()
            return
        data = await state.get_data()
        recs = data.get("recommendations") or []
        if idx < 0 or idx >= len(recs):
            await callback.answer()
            return
        rec = recs[idx]
        added = await add_not_interested(callback.from_user.id, rec)
        await callback.answer("Отметил: не интересно 👎" if added else "Уже в списке «Не интересно»")
        in_fav = await is_favorite(callback.from_user.id, rec)
        in_watched = await is_watched(callback.from_user.id, rec)
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном" if in_fav else "⭐️ В избранное", callback_data=f"t250_fav:{idx}"),
                    InlineKeyboardButton(text="✅ Посмотрел" if in_watched else "🎬 Посмотрел", callback_data=f"t250_watched:{idx}"),
                ],
                [InlineKeyboardButton(text="✅ Не интересно", callback_data=f"t250_not_interested:{idx}")],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(Top250Flow.recommendations, F.data == "t250_reco:again")
    async def top250_reco_again(callback: CallbackQuery, state: FSMContext) -> None:
        """Подобрать ещё — возврат к выбору года, затем новая подборка из 5."""
        await state.set_state(Top250Flow.year_era)
        await callback.message.answer(
            "Выбери год (эпоху) ещё раз — покажу новую подборку 👇",
            reply_markup=year_era_keyboard("t250_"),
        )
        await callback.answer()

    @router.callback_query(Top250Flow.recommendations, F.data == "t250_reco:menu")
    async def top250_reco_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await callback.message.answer("Главное меню 👇", reply_markup=main_menu_keyboard())
        await callback.answer()

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
            await state.set_state(MovieFlow.duration)
            await callback.message.edit_text(
                "Супер! Теперь выбери желаемую <b>длительность</b> фильма 🕒",
                reply_markup=duration_keyboard(),
            )
            await callback.answer()
            return

        if callback.data == "genres_skip":
            prefs["genres"] = []
            session_id = data.get("session_id") or uuid.uuid4().hex
            await state.update_data(preferences=prefs, session_id=session_id)
            await log_flow_step(callback.from_user.id, session_id, "genres", "skip")
            await state.set_state(MovieFlow.duration)
            await callback.message.edit_text(
                "Супер! Теперь выбери желаемую <b>длительность</b> фильма 🕒",
                reply_markup=duration_keyboard(),
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

    # 3. Длительность
    @router.callback_query(MovieFlow.duration, F.data.startswith("dur:"))
    async def choose_duration(callback: CallbackQuery, state: FSMContext) -> None:
        dur_code = callback.data.split(":", 1)[1]
        data = await state.get_data()
        prefs = data.get("preferences", {})
        prefs["duration"] = dur_code
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(preferences=prefs, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "duration", dur_code)

        await state.set_state(MovieFlow.age)
        await callback.message.edit_text(
            "Какой <b>возрастной рейтинг</b> подходит? 🔞\n\n"
            "Проверить рейтинг: <a href=\"https://www.kinopoisk.ru\">Кинопоиск</a>",
            reply_markup=age_keyboard(),
        )
        await callback.answer()

    # 4. Возраст
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
        await responder.answer("Супер, думаю над вариантами фильма 🎬\nДай мне пару секунд…")
        try:
            llm_response = await get_recommendations_from_llm(
                settings=settings,
                user_id=user_id,
                preferences=prefs,
                negative=negative_text,
            )
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

        # Параллельно запрашиваем данные из API Кинопоиска (рейтинг и возраст)
        kinopoisk_tasks = [
            get_movie_info(settings, rec.title, rec.year)
            for rec in llm_response.recommendations
        ]
        kinopoisk_infos: List[Optional[KinopoiskMovieInfo]] = await asyncio.gather(*kinopoisk_tasks)

        # Не показывать фильмы с рейтингом Кинопоиска ниже 6.0 (если рейтинг известен)
        MIN_KP_RATING = 6.0
        filtered_pairs: List[tuple] = []
        for rec, info in zip(llm_response.recommendations, kinopoisk_infos):
            if info is not None and info.rating_kp is not None and info.rating_kp < MIN_KP_RATING:
                continue
            filtered_pairs.append((rec, info))

        # Исключить фильмы, отмеченные пользователем как «Не интересно»
        ni_kp = await get_not_interested_kinopoisk_ids(user_id)
        ni_movies = await get_not_interested_movie_ids(user_id)
        n_before_ni = len(filtered_pairs)
        pairs_no_ni: List[tuple] = []
        for rec, info in filtered_pairs:
            rec_d = {**rec.model_dump(), "kinopoisk_id": (info.kinopoisk_id if info else None) or rec.model_dump().get("kinopoisk_id")}
            if await rec_in_not_interested(user_id, rec_d, ni_kinopoisk_ids=ni_kp, ni_movie_ids=ni_movies):
                continue
            pairs_no_ni.append((rec, info))
        filtered_pairs = pairs_no_ni

        if not filtered_pairs:
            data = await state.get_data()
            session_id = data.get("session_id") or uuid.uuid4().hex
            await log_flow_step(user_id, session_id, "recommendations", "0")
            if n_before_ni > 0:
                await responder.answer(
                    "Все эти варианты ты уже отметил как неинтересные 👎\nНажми «Подобрать ещё» — подберу другие фильмы.",
                    reply_markup=recommendations_control_keyboard(),
                )
            else:
                await responder.answer(
                    "По твоим критериям не нашлось фильмов с рейтингом Кинопоиска ≥ 6.0. "
                    "Попробуй подобрать ещё раз 🔁",
                    reply_markup=recommendations_control_keyboard(),
                )
            await state.set_state(MovieFlow.recommendations)
            await state.update_data(recommendations=[])
            return

        # Сохраняем в состоянии только отфильтрованные рекомендации
        recs_for_state: List[Dict[str, Any]] = []
        for rec, info in filtered_pairs:
            rec_dict = rec.model_dump()
            if info:
                if info.kinopoisk_id is not None:
                    rec_dict["kinopoisk_id"] = info.kinopoisk_id
                if info.age_rating:
                    rec_dict["age_rating"] = info.age_rating
                if info.rating_kp is not None:
                    rec_dict["rating_kp"] = info.rating_kp
            recs_for_state.append(rec_dict)
        await state.update_data(recommendations=recs_for_state)

        data = await state.get_data()
        session_id = data.get("session_id") or uuid.uuid4().hex
        await log_flow_step(user_id, session_id, "recommendations", str(len(filtered_pairs)))

        top250_positions = await get_top250_positions_map(settings)
        for idx, (rec, info) in enumerate(filtered_pairs):
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
            watched_label = "✅ Посмотрел" if in_watched else "🎬 Посмотрел"
            not_int_label = "✅ Не интересно" if in_ni else "👎 Не интересно"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=fav_label, callback_data=f"fav:{idx}"),
                        InlineKeyboardButton(text=watched_label, callback_data=f"watched:{idx}"),
                    ],
                    [InlineKeyboardButton(text=not_int_label, callback_data=f"not_interested:{idx}")],
                ]
            )
            poster_url = info.poster_url if info else None
            if poster_url and str(poster_url).strip().startswith("http"):
                await responder.answer_photo(photo=poster_url, caption=text, reply_markup=kb)
            else:
                await responder.answer(text, reply_markup=kb)

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

    # Добавление в избранное по кнопке
    @router.callback_query(MovieFlow.recommendations, F.data.startswith("fav:"))
    async def add_to_favorites(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            index = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer("Не удалось сохранить фильм 😔", show_alert=True)
            return

        data = await state.get_data()
        recs: List[Dict[str, Any]] = data.get("recommendations") or []
        if index < 0 or index >= len(recs):
            await callback.answer("Не удалось найти фильм 😔", show_alert=True)
            return

        rec = recs[index]
        added = await add_favorite_for_user(callback.from_user.id, rec)
        if added:
            await callback.answer("Добавлено в избранное ⭐️", show_alert=False)
        else:
            await callback.answer("Этот фильм уже в избранном 👍", show_alert=False)
        # Обновляем кнопки: показываем ✅ В избранном и актуальный статус «Посмотрел» и «Не интересно»
        in_watched = await is_watched(callback.from_user.id, rec)
        in_ni = await is_not_interested(callback.from_user.id, rec)
        not_int_label = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном", callback_data=f"fav:{index}"),
                    InlineKeyboardButton(
                        text="✅ Посмотрел" if in_watched else "🎬 Посмотрел",
                        callback_data=f"watched:{index}",
                    ),
                ],
                [InlineKeyboardButton(text=not_int_label, callback_data=f"not_interested:{index}")],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(MovieFlow.recommendations, F.data.startswith("watched:"))
    async def add_to_watched(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            index = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer("Не удалось отметить фильм 😔", show_alert=True)
            return

        data = await state.get_data()
        recs: List[Dict[str, Any]] = data.get("recommendations") or []
        if index < 0 or index >= len(recs):
            await callback.answer("Не удалось найти фильм 😔", show_alert=True)
            return

        rec = recs[index]
        added = await add_watched_for_user(callback.from_user.id, rec)
        if added:
            await callback.answer("Добавлено в «Посмотрел» 🎬", show_alert=False)
        else:
            await callback.answer("Уже в списке «Посмотрел» 👍", show_alert=False)
        # Обновляем кнопки: показываем ✅ Посмотрел и актуальный статус избранного и «Не интересно»
        in_fav = await is_favorite(callback.from_user.id, rec)
        in_ni = await is_not_interested(callback.from_user.id, rec)
        not_int_label = "✅ Не интересно" if in_ni else "👎 Не интересно"
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ В избранном" if in_fav else "⭐️ В избранное",
                        callback_data=f"fav:{index}",
                    ),
                    InlineKeyboardButton(text="✅ Посмотрел", callback_data=f"watched:{index}"),
                ],
                [InlineKeyboardButton(text=not_int_label, callback_data=f"not_interested:{index}")],
            ]
        )
        try:
            await callback.message.edit_reply_markup(reply_markup=new_kb)
        except Exception:
            pass

    @router.callback_query(MovieFlow.recommendations, F.data.startswith("not_interested:"))
    async def add_to_not_interested(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            index = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer()
            return

        data = await state.get_data()
        recs: List[Dict[str, Any]] = data.get("recommendations") or []
        if index < 0 or index >= len(recs):
            await callback.answer()
            return

        rec = recs[index]
        added = await add_not_interested(callback.from_user.id, rec)
        if added:
            await callback.answer("Отметил: не интересно 👎", show_alert=False)
        else:
            await callback.answer("Уже в списке «Не интересно»", show_alert=False)

        in_fav = await is_favorite(callback.from_user.id, rec)
        in_watched = await is_watched(callback.from_user.id, rec)
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном" if in_fav else "⭐️ В избранное", callback_data=f"fav:{index}"),
                    InlineKeyboardButton(text="✅ Посмотрел" if in_watched else "🎬 Посмотрел", callback_data=f"watched:{index}"),
                ],
                [InlineKeyboardButton(text="✅ Не интересно", callback_data=f"not_interested:{index}")],
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
        await state.clear()
        await callback.message.answer(
            "Главное меню 👇",
            reply_markup=main_menu_keyboard(),
        )
        await callback.answer()

    # После перезапуска бота состояние теряется — обрабатываем кнопки без состояния
    @router.callback_query(F.data == "reco:menu")
    async def fallback_reco_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await callback.message.answer(
            "Главное меню 👇",
            reply_markup=main_menu_keyboard(),
        )
        await callback.answer()

    @router.callback_query(F.data == "reco:again")
    async def fallback_reco_again(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await state.set_state(MovieFlow.mood)
        await state.update_data(preferences={})
        await callback.message.answer(
            "Давай подберём фильм! Выбери <b>настроение</b> 👇",
            reply_markup=mood_keyboard(),
        )
        await callback.answer()

    return router

