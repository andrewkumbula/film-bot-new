## Telegram-бот «Фильм на вечер»

Бот помогает за 1–2 минуты подобрать фильм на вечер через диалог с ИИ (OpenRouter, модель `xiaomi/mimo-v2-flash`).

### Стек

- **Python** 3.11+
- **aiogram** v3 (long polling)
- **httpx**
- **python-dotenv**
- **SQLite** через **aiosqlite**
- **Pydantic** (валидация JSON от LLM)
- **OpenRouter API**

### Подготовка

1. Установите зависимости:

```bash
pip install -r requirements.txt
```

2. Создайте файл `.env` на основе `.env.example` и заполните:

- `BOT_TOKEN` — токен Telegram-бота
- `OPENROUTER_API_KEY` — API-ключ OpenRouter

### Запуск

```bash
python -m app
```

Бот запустится в режиме long polling. Команда `/start` покажет главное меню с кнопками.

