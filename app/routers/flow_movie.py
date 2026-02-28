from __future__ import annotations

from typing import Any, Dict, List

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove

from ..config import Settings
from ..keyboards.flow import (
    mood_keyboard,
    genres_keyboard,
    duration_keyboard,
    age_keyboard,
    company_keyboard,
    recommendations_control_keyboard,
)
from ..keyboards.main_menu import main_menu_keyboard
from ..llm.service import get_recommendations_from_llm, LlmError
from ..services.favorites import add_favorite_for_user


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

    # Старт флоу: главная кнопка и несколько алиасов
    @router.message(
        F.text.endswith("Подобрать фильм")
        | F.text.endswith("Выбрать жанр")
        | F.text.endswith("Длительность")
        | F.text.endswith("Настроение")
        | F.text.endswith("С кем смотрим")
    )
    async def start_flow(message: Message, state: FSMContext) -> None:
        await state.clear()
        await state.set_state(MovieFlow.mood)
        await state.update_data(preferences={})
        # Сначала убираем нижнюю reply-клавиатуру (главное меню),
        # чтобы в режиме анкеты остались только инлайн-кнопки.
        await message.answer("Начнём подбор фильма ✨", reply_markup=ReplyKeyboardRemove())
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
        await state.update_data(preferences=prefs)

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
        await state.update_data(preferences=prefs)

        await state.set_state(MovieFlow.age)
        await callback.message.edit_text(
            "Какой <b>возрастной рейтинг</b> подходит? 🔞",
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
        await state.update_data(preferences=prefs)

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
        await state.update_data(preferences=prefs)

        await state.set_state(MovieFlow.negative)
        await callback.message.edit_text(
            "Почти готово! ✨\n\n"
            "<b>Есть ли что-то, чего точно НЕ хочешь видеть в фильме?</b>\n"
            "Например: «жестокость», «очень тяжёлые драмы», «старое кино».\n\n"
            "Если ограничений нет — просто напиши «нет».",
        )
        await callback.answer()

    # 6. Текстовый уточняющий вопрос
    @router.message(MovieFlow.negative)
    async def negative_preferences(message: Message, state: FSMContext) -> None:
        negative_text = (message.text or "").strip()
        if not negative_text or negative_text.lower() in {"нет", "не", "-"}:
            negative_text = ""

        data = await state.get_data()
        prefs: Dict[str, Any] = data.get("preferences", {})
        await state.update_data(negative=negative_text)

        await message.answer("Супер, думаю над вариантами фильма 🎬\nДай мне пару секунд…")

        try:
            llm_response = await get_recommendations_from_llm(
                settings=settings,
                user_id=message.from_user.id,
                preferences=prefs,
                negative=negative_text,
            )
        except LlmError as e:
            await message.answer(
                "😔 Не получилось получить рекомендации от ИИ.\n"
                "Попробуем ещё раз чуть позже.\n\n"
                f"Техническая деталь: {e.user_message}",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return

        # Сохраним рекомендации в состоянии, чтобы можно было добавить в избранное
        await state.update_data(recommendations=[r.model_dump() for r in llm_response.recommendations])
        await state.set_state(MovieFlow.recommendations)

        summary_lines = [f"📝 <b>Кратко о твоих предпочтениях</b>:\n{llm_response.session_summary}\n"]
        await message.answer("\n".join(summary_lines))

        # Выводим сами рекомендации
        for idx, rec in enumerate(llm_response.recommendations):
            parts: List[str] = []
            title_line = f"{idx + 1}. <b>{rec.title}</b>"
            if rec.year:
                title_line += f" ({rec.year})"
            parts.append(title_line)

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

            from aiogram.utils.formatting import Text

            text = "\n".join(parts)
            # Для каждой рекомендации делаем свою кнопку избранного
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="⭐️ В избранное",
                            callback_data=f"fav:{idx}",
                        )
                    ]
                ]
            )
            await message.answer(text, reply_markup=kb)

        # Общие кнопки управления
        await message.answer(
            "Если хочешь — можем подобрать ещё варианты или вернуться в меню 👇",
            reply_markup=recommendations_control_keyboard(),
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
        await add_favorite_for_user(callback.from_user.id, rec)
        await callback.answer("Добавлено в избранное ⭐️", show_alert=False)

    # Кнопки управления после рекомендаций
    @router.callback_query(MovieFlow.recommendations, F.data == "reco:again")
    async def recommendations_again(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await state.set_state(MovieFlow.mood)
        await state.update_data(preferences={})
        await callback.message.answer(
            "Давай подберём ещё один фильм! Снова выбери <b>настроение</b> 👇",
            reply_markup=mood_keyboard(),
        )
        await callback.answer()

    @router.callback_query(MovieFlow.recommendations, F.data == "reco:menu")
    async def recommendations_to_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await callback.message.answer(
            "Главное меню 👇",
            reply_markup=main_menu_keyboard(),
        )
        await callback.answer()

    return router

