from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List, Optional

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from ..config import Settings
from ..keyboards.flow import (
    mood_keyboard,
    genres_keyboard,
    duration_keyboard,
    age_keyboard,
    company_keyboard,
    negative_keyboard,
    recommendations_control_keyboard,
)
from ..keyboards.main_menu import main_menu_keyboard
from ..llm.service import get_recommendations_from_llm, LlmError
from ..services.favorites import add_favorite_for_user, add_watched_for_user, is_favorite, is_watched
from ..services.flow_log import log_flow_step
from ..services.kinopoisk import get_movie_info, KinopoiskMovieInfo


class MovieFlow(StatesGroup):
    mood = State()
    genres = State()
    duration = State()
    age = State()
    company = State()
    negative = State()
    recommendations = State()


def get_router(settings: Settings) -> Router:
    router = Router(name="movie_flow")

    # Старт флоу: только кнопка «Подобрать фильм» (остальные пункты убраны из меню)
    @router.message(F.text.endswith("Подобрать фильм"))
    async def start_flow(message: Message, state: FSMContext) -> None:
        await state.clear()
        session_id = uuid.uuid4().hex
        await state.set_state(MovieFlow.mood)
        await state.update_data(preferences={}, session_id=session_id)
        await log_flow_step(message.from_user.id, session_id, "start", "mood")
        await message.answer("Начнём подбор фильма ✨")
        await message.answer(
            "Для начала давай выберем <b>настроение</b> для фильма 👇",
            reply_markup=mood_keyboard(),
        )

    # 1. Настроение
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
            await callback.message.edit_reply_markup(
                reply_markup=genres_keyboard(selected)
            )
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
        await callback.message.edit_text(
            "Почти готово! ✨\n\n"
            "<b>Есть ли что-то, чего точно НЕ хочешь видеть в фильме?</b>",
            reply_markup=negative_keyboard(),
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

        if not filtered_pairs:
            data = await state.get_data()
            session_id = data.get("session_id") or uuid.uuid4().hex
            await log_flow_step(user_id, session_id, "recommendations", "0")
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
            fav_label = "✅ В избранном" if in_fav else "⭐️ В избранное"
            watched_label = "✅ Посмотрел" if in_watched else "🎬 Посмотрел"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=fav_label, callback_data=f"fav:{idx}"),
                        InlineKeyboardButton(text=watched_label, callback_data=f"watched:{idx}"),
                    ]
                ]
            )
            await responder.answer(text, reply_markup=kb)

        await responder.answer(
            "Если хочешь — можем подобрать ещё варианты или вернуться в меню 👇",
            reply_markup=recommendations_control_keyboard(),
        )

    @router.callback_query(MovieFlow.negative, F.data.startswith("neg:"))
    async def negative_choice(callback: CallbackQuery, state: FSMContext) -> None:
        negative_text = NEGATIVE_TO_PROMPT.get(callback.data, "").strip()
        data = await state.get_data()
        prefs = data.get("preferences", {})
        session_id = data.get("session_id") or uuid.uuid4().hex
        await state.update_data(negative=negative_text, session_id=session_id)
        await log_flow_step(callback.from_user.id, session_id, "negative", callback.data)
        await callback.answer()
        await _do_recommendations(
            callback.message, state, callback.from_user.id, prefs, negative_text
        )

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
        # Обновляем кнопки: показываем ✅ В избранном и актуальный статус «Посмотрел»
        in_watched = await is_watched(callback.from_user.id, rec)
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ В избранном", callback_data=f"fav:{index}"),
                    InlineKeyboardButton(
                        text="✅ Посмотрел" if in_watched else "🎬 Посмотрел",
                        callback_data=f"watched:{index}",
                    ),
                ]
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
        # Обновляем кнопки: показываем ✅ Посмотрел и актуальный статус избранного
        in_fav = await is_favorite(callback.from_user.id, rec)
        new_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ В избранном" if in_fav else "⭐️ В избранное",
                        callback_data=f"fav:{index}",
                    ),
                    InlineKeyboardButton(text="✅ Посмотрел", callback_data=f"watched:{index}"),
                ]
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

