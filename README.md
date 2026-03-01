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
- `KINOPOISK_API_KEY` — (опционально) токен для poiskkino.dev (поиск фильмов, постеры). Получить: Telegram-бот [@poiskkinodev_bot](https://t.me/poiskkinodev_bot).
- `KINOPOISK_TOP250_BASE_URL` — (опционально) для корректных позиций в Топ 250 без дублей: `https://kinopoiskapiunofficial.tech`. Тогда используется `GET /api/v2.2/films/top?type=TOP_250_BEST_FILMS`.
- `KINOPOISK_UNOFFICIAL_API_KEY` — (нужен, если задан TOP250_BASE_URL) ключ с сайта [kinopoiskapiunofficial.tech](https://kinopoiskapiunofficial.tech); у неофициального API свой ключ, не от poiskkino.dev.
- `SHOW_POSTERS_COUNT` — (опционально) сколько постеров показывать в карточке: `1` (по умолчанию) или `3`. Если API отдал несколько URL (url, previewUrl, preview), все сохраняются в БД; отображается 1 или 3.
- `REPORT_CHAT_ID` — (опционально) ваш Telegram chat_id для ежедневной рассылки отчёта по логам флоу за сутки (CSV). Узнать chat_id: напишите боту [@userinfobot](https://t.me/userinfobot).
- `REPORT_TIME` — (опционально) время отправки отчёта в формате `HH:MM` (по умолчанию `09:00`, локальное время сервера).

**Краткие описания в карточках:** для фильмов с полным описанием в БД раз в сутки (в 03:00) запускается задача: ИИ генерирует краткое описание до 120 символов и сохраняет в `short_description`. В карточке показывается только это краткое описание; если его ещё нет — строка с описанием не выводится.

Команда `/daily_report` формирует и отправляет тот же отчёт (за последние 24 часа) в чат того, кто вызвал команду. Если задан `REPORT_CHAT_ID`, выполнять команду может только пользователь с этим id. Тот же админ может использовать: `/delete_film <название> [год]` — удаление фильма из кэша (movies); `/refresh_top250` — принудительное обновление списка «Кинопоиск Топ 250» с API (иначе список обновляется 1-го числа каждого месяца).

### Запуск

```bash
python -m app
```

Бот запустится в режиме long polling. Команда `/start` покажет главное меню с кнопками.

### Логи

- **В терминале:** при запуске `python -m app` логи пишутся в stderr — всё видно в консоли.
- **На сервере:** если бот запущен через systemd, смотри логи: `journalctl -u film-bot -f` (имя сервиса подставь своё). Если через screen/tmux — в том же терминале или в nohup.out.
- **В файл:** задай в `.env` переменную `LOG_PATH=/путь/к/bot.log` — тогда те же логи дублируются в файл. Просмотр: `tail -f /путь/к/bot.log`.
- **Подробнее (для отладки):** в `.env` добавь `DEBUG=true` — уровень логирования станет DEBUG (больше сообщений, в т.ч. от библиотек).

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

**3. Перенос Топ 250 (таблицы с фильмами) на прод**  
Фильмы в Топ 250 одинаковы для дева и прода — можно один раз выгрузить с дева и подгрузить на прод, не тратя лимит API Кинопоиска на проде.

- **На деве** (где уже есть заполненный Топ 250):
  ```bash
  python scripts/export_top250_for_prod.py
  ```
  Создаётся файл `top250_export.json` в корне проекта (путь можно задать через `TOP250_EXPORT_PATH` в `.env`).

- Перенеси файл на прод (scp, rsync, или положи в репозиторий и не коммить в git — файл в `.gitignore`).

- **На проде** (после миграций и при наличии `top250_export.json`):
  ```bash
  python scripts/import_top250_to_prod.py
  # или с путём к файлу:
  python scripts/import_top250_to_prod.py /path/to/top250_export.json
  ```
  В БД подтягиваются записи в `movies` и `kinopoisk_top250`; существующие фильмы по `kinopoisk_id` обновляются.

