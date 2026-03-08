"""
Флоу подбора сериалов по ТЗ: время → формат → настроение → ограничения → 3 карточки.
Подбор через ИИ + обогащение данными с Кинопоиска (как для фильмов).
"""
from __future__ import annotations

import logging
import random
from typing import Any, Dict, List

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup

from ..config import Settings, load_settings
from ..keyboards.main_menu import main_menu_keyboard
from ..keyboards.series import (
    PREFIX,
    series_time_keyboard,
    series_format_keyboard,
    series_mood_keyboard,
    series_restrictions_keyboard,
    series_card_keyboard,
    series_reco_control_keyboard,
)
from ..llm.service import get_series_recommendations_from_llm, LlmError
from ..services.kinopoisk import get_or_create_series_from_llm, enrich_series_from_kinopoisk
from ..services.series import (
    get_filtered_series,
    get_series_count,
    get_series_by_id,
    get_series_watched_ids,
    is_series_in_favorites,
    add_series_favorite,
    add_series_watched,
)

logger = logging.getLogger(__name__)


class SeriesFlow(StatesGroup):
    time = State()
    format = State()
    mood = State()
    restrictions = State()
    recommendations = State()


def get_router(settings: Settings) -> Router:
    router = Router(name="flow_series")

    async def _send_series_card(
        msg: Message,
        series: Dict[str, Any],
        kb: InlineKeyboardMarkup,
    ) -> None:
        """Отправляет карточку сериала: постер (если есть) + текст + кнопки."""
        text = _format_series_card(series)
        poster_urls = series.get("poster_urls") or []
        if not poster_urls and series.get("poster_url"):
            poster_urls = [series["poster_url"]]
        valid = [u for u in poster_urls if u and str(u).strip().startswith("http")]
        if valid:
            try:
                await msg.answer_photo(photo=valid[0], caption=text, reply_markup=kb)
            except Exception:
                await msg.answer(text, reply_markup=kb)
        else:
            await msg.answer(text, reply_markup=kb)

    def _format_series_card(s: Dict[str, Any]) -> str:
        """Формат карточки по ТЗ: название, год • жанры • рейтинг, описание, серий/длительность/сезонов."""
        parts = [f"<b>{s.get('name') or '—'}</b>"]
        line2 = []
        if s.get("year"):
            line2.append(str(s["year"]))
        if s.get("genres"):
            line2.append(s["genres"][:60] + ("…" if len(s.get("genres", "")) > 60 else ""))
        if s.get("rating_kp") is not None:
            line2.append(f"⭐ {float(s['rating_kp']):.1f}")
        if line2:
            parts.append(" • ".join(line2))
        if s.get("short_description"):
            parts.append(s["short_description"].strip())
        elif s.get("description"):
            desc = (s["description"] or "").strip()[:300]
            if len((s.get("description") or "")) > 300:
                desc += "…"
            parts.append(desc)
        meta = []
        if s.get("episodes_total"):
            meta.append(f"Серий: {s['episodes_total']}")
        if s.get("runtime_episode_min"):
            meta.append(f"Длительность: {s['runtime_episode_min']} мин")
        if s.get("seasons_total"):
            meta.append(f"Сезонов: {s['seasons_total']}")
        if meta:
            parts.append("\n".join(meta))
        if s.get("why"):
            parts.append(f"💡 {s['why']}")
        return "\n\n".join(parts)

    # ---- Вход: кнопка "Подобрать сериал" (в меню с эмодзи: "📺 Подобрать сериал") ----
    @router.message(F.text.endswith("Подобрать сериал"))
    async def start_series_flow(message: Message, state: FSMContext) -> None:
        await state.clear()
        await state.set_state(SeriesFlow.time)
        await message.answer(
            "Сколько времени планируете смотреть?",
            reply_markup=series_time_keyboard(),
        )

    # ---- Шаг 1: время ----
    @router.callback_query(SeriesFlow.time, F.data.startswith(f"{PREFIX}time:"))
    async def series_time(callback: CallbackQuery, state: FSMContext) -> None:
        time_val = callback.data.split(":", 1)[1]
        await state.update_data(series_time=time_val)
        await state.set_state(SeriesFlow.format)
        await callback.message.edit_text(
            "Какой формат сериала хотите?",
            reply_markup=series_format_keyboard(),
        )
        await callback.answer()

    # ---- Шаг 2: формат ----
    @router.callback_query(SeriesFlow.format, F.data.startswith(f"{PREFIX}fmt:"))
    async def series_format(callback: CallbackQuery, state: FSMContext) -> None:
        fmt_val = callback.data.split(":", 1)[1]
        await state.update_data(series_format=fmt_val)
        await state.set_state(SeriesFlow.mood)
        await callback.message.edit_text(
            "Какое настроение сейчас?",
            reply_markup=series_mood_keyboard(),
        )
        await callback.answer()

    # ---- Шаг 3: настроение ----
    @router.callback_query(SeriesFlow.mood, F.data.startswith(f"{PREFIX}mood:"))
    async def series_mood(callback: CallbackQuery, state: FSMContext) -> None:
        mood_val = callback.data.split(":", 1)[1]
        await state.update_data(series_mood=mood_val)
        await state.set_state(SeriesFlow.restrictions)
        await state.update_data(series_restrictions=[])
        await callback.message.edit_text(
            "Есть ли ограничения? (можно отметить несколько)",
            reply_markup=series_restrictions_keyboard(set()),
        )
        await callback.answer()

    # ---- Шаг 4: ограничения (мультивыбор) ----
    @router.callback_query(SeriesFlow.restrictions, F.data.startswith(f"{PREFIX}res:"))
    async def series_restrictions(callback: CallbackQuery, state: FSMContext) -> None:
        val = callback.data.split(":", 1)[1]
        if val == "done":
            data = await state.get_data()
            time_slot = data.get("series_time") or "any"
            format_type = data.get("series_format") or "any"
            mood = data.get("series_mood") or "any"
            restrictions = data.get("series_restrictions") or []
            user_id = callback.from_user.id if callback.from_user else 0
            settings = load_settings()
            await state.set_state(SeriesFlow.recommendations)

            # 1) Запрос к ИИ: список сериалов (title, year, why)
            try:
                await callback.message.edit_text("Подбираю сериалы… 🤖")
            except Exception:
                pass
            await callback.answer()

            try:
                llm_response = await get_series_recommendations_from_llm(
                    settings, time_slot, format_type, mood, restrictions,
                )
            except LlmError as e:
                logger.warning("LLM series error: %s", e)
                # Fallback: только из БД
                candidates = await get_filtered_series(
                    settings, user_id,
                    time_slot=time_slot,
                    format_type=format_type,
                    mood=mood,
                    restrictions=restrictions,
                    limit=30,
                )
                total = await get_series_count(settings)
                if not candidates:
                    if total == 0:
                        await callback.message.edit_text(
                            "Не удалось подобрать через ИИ, а в базе пока нет сериалов. Попробуйте позже.",
                            reply_markup=series_reco_control_keyboard(),
                        )
                    else:
                        await callback.message.edit_text(
                            "Не удалось подобрать через ИИ. По параметрам в базе ничего не нашлось — попробуйте ослабить ограничения.",
                            reply_markup=series_reco_control_keyboard(),
                        )
                    return
                show_count = min(3, len(candidates))
                films = random.sample(candidates, show_count) if len(candidates) > show_count else candidates
                await state.update_data(series_recommendations=films)
                try:
                    await callback.message.edit_text("Вот подборка сериалов 📺")
                except Exception:
                    pass
                for idx, series in enumerate(films):
                    in_fav = await is_series_in_favorites(settings, user_id, series["id"])
                    in_watched = await _is_series_watched(settings, user_id, series["id"])
                    kb = series_card_keyboard(series["id"], idx, in_fav, in_watched)
                    await _send_series_card(callback.message, series, kb)
                await callback.message.answer(
                    "Можем подобрать ещё или вернуться в меню 👇",
                    reply_markup=series_reco_control_keyboard(),
                )
                return

            # 2) ИИ — источник списка: создаём/берём сериал в таблице (заглушка: название, год, «почему»)
            # 3) Опционально обогащаем из Кинопоиска (постер, рейтинг и т.д.) и сохраняем в ту же таблицу
            # 4) Показываем до 3 карточек — с постером, если Кинопоиск ответил, иначе без
            watched_ids = await get_series_watched_ids(settings, user_id)
            candidates: List[Dict[str, Any]] = []
            for pick in llm_response.recommendations:
                if len(candidates) >= 3:
                    break
                rec = await get_or_create_series_from_llm(
                    settings, pick.title, pick.year, getattr(pick, "why", "") or "",
                )
                if rec is None or rec.get("id") in watched_ids:
                    continue
                await enrich_series_from_kinopoisk(settings, rec["id"])
                refreshed = await get_series_by_id(settings, rec["id"])
                if refreshed is None:
                    continue
                refreshed["why"] = getattr(pick, "why", "") or ""
                candidates.append(refreshed)

            if not candidates:
                db_candidates = await get_filtered_series(
                    settings, user_id,
                    time_slot=time_slot,
                    format_type=format_type,
                    mood=mood,
                    restrictions=restrictions,
                    limit=30,
                )
                if db_candidates:
                    show_count = min(3, len(db_candidates))
                    films = random.sample(db_candidates, show_count) if len(db_candidates) > show_count else db_candidates
                    await state.update_data(series_recommendations=films)
                    try:
                        await callback.message.edit_text("Вот подборка из базы 📺")
                    except Exception:
                        pass
                    for idx, series in enumerate(films):
                        in_fav = await is_series_in_favorites(settings, user_id, series["id"])
                        in_watched = await _is_series_watched(settings, user_id, series["id"])
                        kb = series_card_keyboard(series["id"], idx, in_fav, in_watched)
                        await _send_series_card(callback.message, series, kb)
                    await callback.message.answer(
                        "Можем подобрать ещё или вернуться в меню 👇",
                        reply_markup=series_reco_control_keyboard(),
                    )
                    return
                await callback.message.edit_text(
                    "Все подходящие сериалы уже в «Уже смотрел». Ослабьте ограничения или нажмите «Подобрать ещё».",
                    reply_markup=series_reco_control_keyboard(),
                )
                return

            await state.update_data(series_recommendations=candidates)
            try:
                await callback.message.edit_text("Вот подборка сериалов 📺")
            except Exception:
                pass
            for idx, series in enumerate(candidates):
                in_fav = await is_series_in_favorites(settings, user_id, series["id"])
                in_watched = await _is_series_watched(settings, user_id, series["id"])
                kb = series_card_keyboard(series["id"], idx, in_fav, in_watched)
                await _send_series_card(callback.message, series, kb)
            await callback.message.answer(
                "Можем подобрать ещё или вернуться в меню 👇",
                reply_markup=series_reco_control_keyboard(),
            )
            return
        # Toggle restriction
        data = await state.get_data()
        rest: List[str] = list(data.get("series_restrictions") or [])
        if val in rest:
            rest.remove(val)
        else:
            rest.append(val)
        await state.update_data(series_restrictions=rest)
        selected = set(rest)
        await callback.message.edit_reply_markup(reply_markup=series_restrictions_keyboard(selected))
        await callback.answer()

    async def _is_series_watched(settings: Settings, user_id: int, series_id: int) -> bool:
        from ..services.series import is_series_watched
        return await is_series_watched(settings, user_id, series_id)

    # ---- Кнопки карточки: избранное, уже смотрел, другой вариант ----
    @router.callback_query(SeriesFlow.recommendations, F.data.startswith(f"{PREFIX}fav:"))
    async def series_fav(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            idx = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer()
            return
        data = await state.get_data()
        recs = data.get("series_recommendations") or []
        if idx < 0 or idx >= len(recs):
            await callback.answer()
            return
        series = recs[idx]
        added = await add_series_favorite(load_settings(), callback.from_user.id, series["id"])
        await callback.answer("Добавлено в избранное ❤️" if added else "Уже в избранном")
        in_watched = await _is_series_watched(load_settings(), callback.from_user.id, series["id"])
        kb = series_card_keyboard(series["id"], idx, True, in_watched)
        try:
            await callback.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

    @router.callback_query(SeriesFlow.recommendations, F.data.startswith(f"{PREFIX}watched:"))
    async def series_watched_btn(callback: CallbackQuery, state: FSMContext) -> None:
        try:
            idx = int(callback.data.split(":", 1)[1])
        except (ValueError, IndexError):
            await callback.answer()
            return
        data = await state.get_data()
        recs = data.get("series_recommendations") or []
        if idx < 0 or idx >= len(recs):
            await callback.answer()
            return
        series = recs[idx]
        await add_series_watched(load_settings(), callback.from_user.id, series["id"])
        await callback.answer("Отмечено: уже смотрел 🚫")
        in_fav = await is_series_in_favorites(load_settings(), callback.from_user.id, series["id"])
        kb = series_card_keyboard(series["id"], idx, in_fav, True)
        try:
            await callback.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

    @router.callback_query(SeriesFlow.recommendations, F.data.startswith(f"{PREFIX}other:"))
    async def series_other(callback: CallbackQuery, state: FSMContext) -> None:
        """Другой вариант — подставить новый сериал из кандидатов (упрощённо: просто ответ «подобрать ещё»)."""
        await callback.answer("Нажми «Подобрать ещё» для новой подборки 👇")

    @router.callback_query(SeriesFlow.recommendations, F.data.startswith(f"{PREFIX}watch:"))
    async def series_watch(callback: CallbackQuery, state: FSMContext) -> None:
        idx = callback.data.split(":", 1)[1] if ":" in callback.data else ""
        data = await state.get_data()
        recs = data.get("series_recommendations") or []
        try:
            i = int(idx)
            if 0 <= i < len(recs) and recs[i].get("kinopoisk_id"):
                kp_id = recs[i]["kinopoisk_id"]
                await callback.answer(f"Кинопоиск: kp.ru/film/{kp_id}", show_alert=False)
            else:
                await callback.answer()
        except (ValueError, IndexError):
            await callback.answer()

    @router.callback_query(SeriesFlow.recommendations, F.data.startswith(f"{PREFIX}similar:"))
    async def series_similar(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer("Похожие сериалы — в следующих версиях 🔜")

    @router.callback_query(SeriesFlow.recommendations, F.data == f"{PREFIX}reco:again")
    async def series_reco_again(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.set_state(SeriesFlow.time)
        await callback.message.answer(
            "Сколько времени планируете смотреть?",
            reply_markup=series_time_keyboard(),
        )

    @router.callback_query(SeriesFlow.recommendations, F.data == f"{PREFIX}reco:menu")
    async def series_reco_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await callback.message.answer("Главное меню 👇", reply_markup=main_menu_keyboard())

    # Fallback без состояния (если пользователь перезапустил бота)
    @router.callback_query(F.data == f"{PREFIX}reco:menu")
    async def series_fallback_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await callback.answer()
        await state.clear()
        await callback.message.answer("Главное меню 👇", reply_markup=main_menu_keyboard())

    return router