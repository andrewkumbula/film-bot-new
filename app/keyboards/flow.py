from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def mood_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="😂 Весело", callback_data="mood:fun"),
                InlineKeyboardButton(text="😱 Страшно", callback_data="mood:scary"),
            ],
            [
                InlineKeyboardButton(text="🥹 Трогательно", callback_data="mood:touching"),
                InlineKeyboardButton(text="🧠 Умно", callback_data="mood:smart"),
            ],
            [
                InlineKeyboardButton(text="🤯 Взрыв мозга", callback_data="mood:mindblown"),
                InlineKeyboardButton(text="😌 Лёгкое", callback_data="mood:light"),
            ],
            [InlineKeyboardButton(text="➖ Не важно", callback_data="mood:any")],
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
]


def genres_keyboard(selected: set[str] | None = None) -> InlineKeyboardMarkup:
    selected = selected or set()

    rows = []
    for i in range(0, len(GENRE_OPTIONS), 2):
        row = []
        for label, code in GENRE_OPTIONS[i : i + 2]:
            prefix = "✅ " if code in selected else ""
            row.append(
                InlineKeyboardButton(
                    text=f"{prefix}{label}",
                    callback_data=f"genre:{code}",
                )
            )
        rows.append(row)

    # Кнопки "Готово" и "Не важно"
    rows.append(
        [
            InlineKeyboardButton(text="✅ Готово", callback_data="genres_done"),
            InlineKeyboardButton(text="➖ Не важно", callback_data="genres_skip"),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


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


# Коды для этапа «чего избегать» → текст для промпта ИИ
NEGATIVE_OPTIONS = [
    ("🚫 Жестокость", "neg:violence"),
    ("😢 Тяжёлые драмы", "neg:heavydrama"),
    ("📼 Старое кино", "neg:old"),
    ("😞 Грустный финал", "neg:sad"),
    ("➖ Нет ограничений", "neg:none"),
]


def negative_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🚫 Жестокость", callback_data="neg:violence"),
                InlineKeyboardButton(text="😢 Тяжёлые драмы", callback_data="neg:heavydrama"),
            ],
            [
                InlineKeyboardButton(text="📼 Старое кино", callback_data="neg:old"),
                InlineKeyboardButton(text="😞 Грустный финал", callback_data="neg:sad"),
            ],
            [InlineKeyboardButton(text="➖ Нет ограничений", callback_data="neg:none")],
        ]
    )


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


def recommendations_control_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔁 Подобрать ещё", callback_data="reco:again"),
                InlineKeyboardButton(text="🏠 В меню", callback_data="reco:menu"),
            ]
        ]
    )

