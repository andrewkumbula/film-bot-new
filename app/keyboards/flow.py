from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def source_keyboard() -> InlineKeyboardMarkup:
    """Развилка: откуда подбирать фильмы. Обычный подбор — сверху, Топ 250 — второй."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✨ Обычный подбор (настроение, жанр…)", callback_data="source:default")],
            [InlineKeyboardButton(text="⭐ Кинопоиск Топ 250", callback_data="source:top250")],
            [InlineKeyboardButton(text="🏆 Оскар (номинанты и победители)", callback_data="source:oscar")],
        ]
    )


def mood_keyboard(prefix: str = "") -> InlineKeyboardMarkup:
    p = prefix
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="😂 Весело", callback_data=f"{p}mood:fun"),
                InlineKeyboardButton(text="😱 Страшно", callback_data=f"{p}mood:scary"),
            ],
            [
                InlineKeyboardButton(text="🥹 Трогательно", callback_data=f"{p}mood:touching"),
                InlineKeyboardButton(text="🧠 Умно", callback_data=f"{p}mood:smart"),
            ],
            [
                InlineKeyboardButton(text="🤯 Взрыв мозга", callback_data=f"{p}mood:mindblown"),
                InlineKeyboardButton(text="😌 Лёгкое", callback_data=f"{p}mood:light"),
            ],
            [InlineKeyboardButton(text="➖ Не важно", callback_data=f"{p}mood:any")],
        ]
    )


GENRE_OPTIONS = [
    ("🎭 Комедия", "comedy"),
    ("🕵️ Детектив", "detective"),
    ("🚀 Фантастика", "scifi"),
    ("🧙 Фэнтези", "fantasy"),
    ("💘 Романтика", "romance"),
    ("😱 Ужасы", "horror"),
    ("🎬 Драма", "drama"),
    ("⚔️ Боевик", "action"),
    ("👨‍👩‍👧 Семейный", "family"),
    ("📽️ Артхаус", "arthouse"),
    ("🧒 Мультфильм", "animation"),
    ("🎌 Аниме", "anime"),
]


def genres_keyboard(selected: set[str] | None = None, cb_prefix: str = "") -> InlineKeyboardMarkup:
    selected = selected or set()
    p = cb_prefix

    rows = []
    for i in range(0, len(GENRE_OPTIONS), 2):
        row = []
        for label, code in GENRE_OPTIONS[i : i + 2]:
            prefix = "✅ " if code in selected else ""
            row.append(
                InlineKeyboardButton(
                    text=f"{prefix}{label}",
                    callback_data=f"{p}genre:{code}",
                )
            )
        rows.append(row)

    rows.append(
        [
            InlineKeyboardButton(text="✅ Готово", callback_data=f"{p}genres_done"),
            InlineKeyboardButton(text="➖ Не важно", callback_data=f"{p}genres_skip"),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


def year_era_keyboard(cb_prefix: str = "") -> InlineKeyboardMarkup:
    """Год (эпоха) для ветки Топ 250."""
    p = cb_prefix
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🆕 Новое (2010+)", callback_data=f"{p}year:new")],
            [InlineKeyboardButton(text="📼 90-е–00-е", callback_data=f"{p}year:90s00s")],
            [InlineKeyboardButton(text="🎞 Классика (до 1990)", callback_data=f"{p}year:classic")],
            [InlineKeyboardButton(text="➖ Не важно", callback_data=f"{p}year:any")],
        ]
    )


def duration_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🕐 до 90", callback_data="dur:short"),
                InlineKeyboardButton(text="🕑 90–120", callback_data="dur:medium"),
                InlineKeyboardButton(text="🕒 120+", callback_data="dur:long"),
            ],
            [InlineKeyboardButton(text="➖ Не важно", callback_data="dur:any")],
        ]
    )


def age_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👶 0+", callback_data="age:0"),
                InlineKeyboardButton(text="🧒 6+", callback_data="age:6"),
                InlineKeyboardButton(text="🧒 12+", callback_data="age:12"),
            ],
            [
                InlineKeyboardButton(text="🧑 16+", callback_data="age:16"),
                InlineKeyboardButton(text="🔞 18+", callback_data="age:18"),
            ],
            [InlineKeyboardButton(text="➖ Не важно", callback_data="age:any")],
        ]
    )


# Коды для этапа «чего избегать» → текст для промпта ИИ (множественный выбор)
NEGATIVE_OPTIONS = [
    ("🚫 Жестокость", "neg:violence"),
    ("😢 Тяжёлые драмы", "neg:heavydrama"),
    ("📼 Старое кино", "neg:old"),
    ("😞 Грустный финал", "neg:sad"),
]
# neg:none = «Нет ограничений», neg:done = «Готово» (подтвердить выбранное)


def negative_keyboard(selected: set[str] | None = None) -> InlineKeyboardMarkup:
    """Клавиатура «чего избегать» с множественным выбором. selected — множество callback_data (neg:violence и т.д.)."""
    selected = selected or set()
    rows = []
    for i in range(0, len(NEGATIVE_OPTIONS), 2):
        row = []
        for label, code in NEGATIVE_OPTIONS[i : i + 2]:
            prefix = "✅ " if code in selected else ""
            row.append(
                InlineKeyboardButton(text=f"{prefix}{label}", callback_data=code),
            )
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton(text="✅ Готово", callback_data="neg:done"),
            InlineKeyboardButton(text="➖ Нет ограничений", callback_data="neg:none"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def company_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👫 Вдвоём", callback_data="comp:couple"),
                InlineKeyboardButton(text="👨‍👩‍👧 Семья", callback_data="comp:family"),
            ],
            [
                InlineKeyboardButton(text="🧑‍🤝‍🧑 Компания", callback_data="comp:friends"),
                InlineKeyboardButton(text="🧍 Один", callback_data="comp:solo"),
            ],
            [InlineKeyboardButton(text="➖ Не важно", callback_data="comp:any")],
        ]
    )


def recommendations_control_keyboard(cb_prefix: str = "") -> InlineKeyboardMarkup:
    p = cb_prefix
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔁 Подобрать ещё", callback_data=f"{p}reco:again"),
                InlineKeyboardButton(text="🏠 В меню", callback_data=f"{p}reco:menu"),
            ]
        ]
    )

