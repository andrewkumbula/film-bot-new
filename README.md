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
- `KINOPOISK_API_KEY` — (опционально) токен API Кинопоиска для отображения возрастного рейтинга в рекомендациях. Получить: Telegram-бот [@poiskkinodev_bot](https://t.me/poiskkinodev_bot)
- `REPORT_CHAT_ID` — (опционально) ваш Telegram chat_id для ежедневной рассылки отчёта по логам флоу за сутки (CSV). Узнать chat_id: напишите боту [@userinfobot](https://t.me/userinfobot).
- `REPORT_TIME` — (опционально) время отправки отчёта в формате `HH:MM` (по умолчанию `09:00`, локальное время сервера).

Команда `/daily_report` формирует и отправляет тот же отчёт (за последние 24 часа) в чат того, кто вызвал команду. Если задан `REPORT_CHAT_ID`, выполнять команду может только пользователь с этим id.

### Запуск

```bash
python -m app
```

Бот запустится в режиме long polling. Команда `/start` покажет главное меню с кнопками.

### Деплой и миграции БД

При старте бот вызывает `init_db()`: все таблицы создаются через `CREATE TABLE IF NOT EXISTS`. **Новые таблицы** (например `users`) появляются автоматически после `git pull` и перезапуска — отдельная миграция не нужна.

Если в будущем понадобится изменить схему существующей таблицы (добавить колонку, переименовать и т.п.), в `app/db/database.py` добавляют функцию миграции (по аналогии с `_migrate_old_favorites_if_needed`) и вызывают её из `init_db()`.

