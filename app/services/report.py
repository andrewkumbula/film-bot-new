"""
Формирование отчёта по логам флоу (flow_log) за сутки — CSV для отправки в Telegram.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta
import aiosqlite

from ..config import load_settings


async def build_flow_log_csv(hours: int = 24) -> tuple[bytes, str]:
    """
    Собирает из flow_log записи за последние hours часов, возвращает (csv_bytes, filename).
    """
    settings = load_settings()
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT user_id, session_id, step, value, created_at
            FROM flow_log
            WHERE datetime(created_at) >= datetime('now', '-' || ? || ' hours')
            ORDER BY created_at
            """,
            (hours,),
        )
        rows = await cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "session_id", "step", "value", "created_at"])
    for row in rows:
        writer.writerow([
            row["user_id"],
            row["session_id"],
            row["step"],
            row["value"] or "",
            row["created_at"] or "",
        ])

    csv_str = output.getvalue()
    # CSV в UTF-8 с BOM для корректного открытия в Excel
    bom = "\ufeff"
    csv_bytes = (bom + csv_str).encode("utf-8")
    date_label = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"flow_log_{date_label}.csv"
    return csv_bytes, filename
