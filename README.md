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

### Деплой на сервер

1. **Локально** — закоммитить и отправить код:
   ```bash
   git add .
   git commit -m "описание изменений"
   git push origin main
   ```

2. **На сервере** — обновить код и зависимости:
   ```bash
   cd /path/to/project   # каталог проекта на сервере
   git pull origin main
   source .venv/bin/activate   # или активировать своё виртуальное окружение
   pip install -r requirements.txt
   ```

3. **Переменные окружения** — в корне проекта должен быть `.env` с минимумом:
   - `BOT_TOKEN`, `OPENROUTER_API_KEY`
   - по желанию: `KINOPOISK_API_KEY`, `DB_PATH`, `REPORT_CHAT_ID`, `REPORT_TIME`

4. **Миграции БД** — если менялась схема (новые таблицы/колонки), один раз выполнить:
   ```bash
   python scripts/migrate_add_users.py    # если таблиц ещё нет
   python scripts/run_migrations.py      # обновление существующей схемы
   ```

5. **Перезапуск бота** — как запускаешь обычно (systemd, screen, pm2 и т.п.), например:
   ```bash
   python -m app
   ```
   Или перезапустить сервис: `sudo systemctl restart film-bot` (если настроен).

Итого: **push → на сервере pull, pip install, при необходимости миграции → перезапуск.**

---

### Деплой и миграции БД

**1. Начальная схема (новый сервер или «no such table»)**  
Из корня проекта:

```bash
python scripts/migrate_add_users.py
```

Скрипт создаёт все таблицы (movies, favorites, watched, users, flow_log, kinopoisk_top250), если их ещё нет. Путь к БД — `DB_PATH` из `.env` или `app_data/bot.db`.

**2. Обновление схемы после изменений в коде**  
После `git pull` с изменениями в БД выполните один раз:

```bash
python scripts/run_migrations.py
```

Скрипт по очереди применяет миграции из `scripts/migrations/`:
- **001** — колонки в `movies` (poster_url, description, genres, countries, votes, raw_json, updated_at)
- **002** — уникальный индекс `movies(title, year)` (при дубликатах — пропуск с предупреждением)
- **003** — колонка `poster_url` в `kinopoisk_top250`
- **004** — колонка `movie_id` в `kinopoisk_top250` и обратное заполнение из `movies`

Миграции идемпотентны: повторный запуск безопасен. При старте бот тоже выполняет те же шаги через `init_db()`, но на сервере удобнее один раз запустить `run_migrations.py` после деплоя.

