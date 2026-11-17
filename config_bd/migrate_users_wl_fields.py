"""
Миграция таблицы users — лимит трафика Антиглушилка:
- trafic_wl FLOAT DEFAULT 0  (GB)
- limit_wl FLOAT DEFAULT 0   (GB)
- traffic_gift BOOLEAN DEFAULT 0  (получен подарок 3 ГБ из рассылки)

Запуск из корня проекта:
  python -m config_bd.migrate_users_wl_fields
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from sqlalchemy import text

from config_bd.models import engine

_COLUMNS = (
    ("trafic_wl", "FLOAT DEFAULT 0"),
    ("limit_wl", "FLOAT DEFAULT 0"),
    ("traffic_gift", "BOOLEAN DEFAULT 0"),
)


async def _existing_columns(conn) -> set[str]:
    result = await conn.execute(text("PRAGMA table_info(users)"))
    return {row[1] for row in result.fetchall()}


async def migrate() -> None:
    async with engine.begin() as conn:
        table_check = await conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        )
        if not table_check.fetchone():
            return

        existing = await _existing_columns(conn)
        for name, col_type in _COLUMNS:
            if name in existing:
                continue
            await conn.execute(text(f"ALTER TABLE users ADD COLUMN {name} {col_type}"))

        await conn.execute(text("UPDATE users SET trafic_wl = 0 WHERE trafic_wl IS NULL"))
        await conn.execute(text("UPDATE users SET limit_wl = 0 WHERE limit_wl IS NULL"))
        await conn.execute(text("UPDATE users SET traffic_gift = 0 WHERE traffic_gift IS NULL"))

    print("OK: users trafic_wl, limit_wl, traffic_gift.")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
