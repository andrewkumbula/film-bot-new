"""Клавиатуры для флоу подбора сериалов (ТЗ: время, формат, настроение, ограничения)."""
from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

PREFIX = "s_"


def series_mode_keyboard() -> InlineKeyboardMarkup:
    """Первый шаг: способ подбора — обычный, похожее на, по описанию."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Обычный подбор", callback_data=f"{PREFIX}mode:ordinary")],
            [InlineKeyboardButton(text="🔄 Похожее на…", callback_data=f"{PREFIX}mode:similar")],
            [InlineKeyboardButton(text="✏️ По текстовому описанию", callback_data=f"{PREFIX}mode:by_description")],
        ]
    )


def series_time_keyboard() -> InlineKeyboardMarkup:
    """Сколько времени есть на просмотр."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="2–4 часа", callback_data=f"{PREFIX}time:2-4h")],
            [InlineKeyboardButton(text="Несколько вечеров", callback_data=f"{PREFIX}time:several")],
            [InlineKeyboardButton(text="Долго и беспощадно", callback_data=f"{PREFIX}time:long")],
            [InlineKeyboardButton(text="Не важно", callback_data=f"{PREFIX}time:any")],
        ]
    )


def series_format_keyboard() -> InlineKeyboardMarkup:
    """Формат сериала."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Мини-сериал", callback_data=f"{PREFIX}fmt:mini")],
            [InlineKeyboardButton(text="1 сезон", callback_data=f"{PREFIX}fmt:one_season")],
            [InlineKeyboardButton(text="Несколько сезонов", callback_data=f"{PREFIX}fmt:several_seasons")],
            [InlineKeyboardButton(text="Не важно", callback_data=f"{PREFIX}fmt:any")],
        ]
    )


def series_mood_keyboard() -> InlineKeyboardMarkup:
    """Настроение."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="😌 Лёгкое", callback_data=f"{PREFIX}mood:light"),
                InlineKeyboardButton(text="😬 Напряжённое", callback_data=f"{PREFIX}mood:tense"),
            ],
            [
                InlineKeyboardButton(text="😂 Смешное", callback_data=f"{PREFIX}mood:funny"),
                InlineKeyboardButton(text="🌌 Атмосферное", callback_data=f"{PREFIX}mood:atmospheric"),
            ],
            [
                InlineKeyboardButton(text="🌑 Мрачное", callback_data=f"{PREFIX}mood:dark"),
                InlineKeyboardButton(text="💕 Романтика", callback_data=f"{PREFIX}mood:romance"),
            ],
            [InlineKeyboardButton(text="🤯 Удиви", callback_data=f"{PREFIX}mood:surprise")],
            [InlineKeyboardButton(text="➖ Не важно", callback_data=f"{PREFIX}mood:any")],
        ]
    )


# Ограничения — мультивыбор (toggle). Показываем кнопки с префиксом ✅ если выбрано.
RESTRICTION_OPTIONS = [
    ("Только завершённые", "completed_only"),
    ("Без ужасов", "no_horror"),
    ("Без тяжёлой драмы", "no_heavy_drama"),
    ("Рейтинг 7+", "rating_7_plus"),
    ("Без российских сериалов", "no_russian"),
]


def series_restrictions_keyboard(selected: set[str] | None = None) -> InlineKeyboardMarkup:
    """Ограничения (мультивыбор). selected — множество callback_data значений."""
    selected = selected or set()
    rows = []
    for label, code in RESTRICTION_OPTIONS:
        prefix = "✅ " if code in selected else ""
        rows.append([
            InlineKeyboardButton(text=f"{prefix}{label}", callback_data=f"{PREFIX}res:{code}"),
        ])
    rows.append([InlineKeyboardButton(text="✅ Готово", callback_data=f"{PREFIX}res:done")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def series_card_keyboard(series_id: int, idx: int, in_fav: bool, in_watched: bool) -> InlineKeyboardMarkup:
    """Кнопки под карточкой сериала: Смотреть, Похожее, Другой, В избранное, Уже смотрел."""
    fav_text = "✅ В избранном" if in_fav else "❤️ В избранное"
    watched_text = "✅ Уже смотрел" if in_watched else "🚫 Уже смотрел"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="▶️ Смотреть", callback_data=f"{PREFIX}watch:{idx}"),
                InlineKeyboardButton(text="➕ Похожее", callback_data=f"{PREFIX}similar:{idx}"),
            ],
            [
                InlineKeyboardButton(text="⏭ Другой вариант", callback_data=f"{PREFIX}other:{idx}"),
            ],
            [
                InlineKeyboardButton(text=fav_text, callback_data=f"{PREFIX}fav:{idx}"),
                InlineKeyboardButton(text=watched_text, callback_data=f"{PREFIX}watched:{idx}"),
            ],
        ]
    )


def series_reco_control_keyboard() -> InlineKeyboardMarkup:
    """Подобрать ещё / В меню после выдачи."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔁 Подобрать ещё", callback_data=f"{PREFIX}reco:again"),
                InlineKeyboardButton(text="🏠 В меню", callback_data=f"{PREFIX}reco:menu"),
            ]
        ]
    )
