#!/usr/bin/env python3
"""
Парсинг страницы Википедии «Премия «Оскар» за лучший фильм» и загрузка в oscar_nominations.
Запуск: из корня проекта: python scripts/parse_oscar_wikipedia.py
Требует: pip install httpx beautifulsoup4 python-dotenv aiosqlite
"""
from __future__ import annotations

import asyncio
import os
import re
import sys

# корень проекта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import aiosqlite
import httpx
from bs4 import BeautifulSoup

from app.config import load_settings
from app.services.oscar import CATEGORY_BEST_PICTURE

URL = "https://ru.wikipedia.org/wiki/Премия_«Оскар»_за_лучший_фильм"


def _normalize_year_label(raw: str) -> str:
    """Извлекает метку года: 1929, 1930-I, 1930-II, 2024."""
    raw = (raw or "").strip()
    # Удаляем вики-разметку ссылок [1929](url) -> 1929
    m = re.search(r"\[([^\]]+)\]", raw)
    if m:
        raw = m.group(1)
    raw = raw.strip()
    # Оставляем только год и суффикс типа -I, -II
    m = re.match(r"(\d{4})(-[IVXLCDM]+)?", raw, re.I)
    if m:
        return m.group(1) + (m.group(2) or "")
    if raw.isdigit():
        return raw
    return raw[:20] if raw else ""


def _extract_film_title(cell) -> tuple[str, bool]:
    """Из ячейки «Фильм» извлекает (название, is_winner). Победитель помечен ★."""
    text = cell.get_text(separator=" ", strip=True) if hasattr(cell, "get_text") else str(cell)
    winner = "★" in text or "&#9733;" in text
    # Убираем ★ и кавычки « »
    title = re.sub(r"[★*]\s*", "", text)
    title = re.sub(r"^[«\"]\s*", "", title)
    title = re.sub(r"\s*[»\"]$", "", title)
    title = re.sub(r"\s*\([^)]*\)\s*$", "", title)  # убираем (фильм, 2024) в конце если есть
    title = title.strip()
    if not title:
        return "", winner
    return title[:500], winner


async def fetch_and_parse() -> list[tuple[str, str, str, bool]]:
    """Возвращает список (ceremony_label, title_from_source, is_winner)."""
    headers = {"User-Agent": "FilmBot/1.0 (https://github.com/; educational project)"}
    # trust_env=False — не использовать системный прокси (часто даёт 403 на Википедию)
    async with httpx.AsyncClient(
        follow_redirects=True, timeout=30.0, headers=headers, trust_env=False
    ) as client:
        r = await client.get(URL)
        r.raise_for_status()
        html = r.text
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="wikitable")
    result = []
    for table in tables:
        rows = table.find_all("tr")
        current_year = ""
        for tr in rows:
            cells = tr.find_all("td")
            if len(cells) >= 2:
                year_cell, film_cell = cells[0], cells[1]
                year_text = year_cell.get_text(separator=" ", strip=True) if hasattr(year_cell, "get_text") else ""
                if year_text:
                    current_year = _normalize_year_label(year_text)
            elif len(cells) == 1:
                film_cell = cells[0]
            else:
                continue
            if not current_year or not re.match(r"\d{4}", current_year):
                continue
            title, is_winner = _extract_film_title(film_cell)
            if not title:
                continue
            result.append((current_year, title.strip(), is_winner))
    return result


def _ceremony_year_number(label: str) -> int:
    """Для сортировки: 1930-I -> 1930, 1930-II -> 1930."""
    m = re.match(r"(\d{4})", label)
    return int(m.group(1)) if m else 0


async def main() -> None:
    settings = load_settings()
    rows = await fetch_and_parse()
    print(f"Распознано записей: {len(rows)}")
    if not rows:
        print("Ничего не распознано. Проверьте URL и разметку Вики.")
        return

    # Уникальность по (ceremony_label, title)
    seen = set()
    unique = []
    for label, title, is_winner in rows:
        key = (label, title.strip().lower())
        if key in seen:
            continue
        seen.add(key)
        unique.append((label, title, is_winner))

    async with aiosqlite.connect(settings.db_path) as db:
        inserted = 0
        for ceremony_label, title_from_source, is_winner in unique:
            ceremony_year = _ceremony_year_number(ceremony_label)
            try:
                await db.execute(
                    """
                    INSERT INTO oscar_nominations (category, ceremony_year, ceremony_label, title_from_source, is_winner)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (CATEGORY_BEST_PICTURE, ceremony_year, ceremony_label, title_from_source, 1 if is_winner else 0),
                )
                inserted += 1
            except Exception as e:
                if "UNIQUE" in str(e) or "unique" in str(e).lower():
                    pass  # уже есть
                else:
                    print(f"Ошибка вставки {ceremony_label} {title_from_source}: {e}")
        await db.commit()
    print(f"Добавлено в oscar_nominations: {inserted}. Дальше: запустите маппинг на Кинопоиск (scripts/map_oscar_to_kinopoisk.py).")


if __name__ == "__main__":
    asyncio.run(main())
