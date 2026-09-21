"""
Миграция таблицы users — Telegram username и fullname:
- username VARCHAR NULL
- fullname VARCHAR NULL

Запуск из корня проекта:
  python -m config_bd.migrate_users_username_fullname
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
    ("username", "VARCHAR"),
    ("fullname", "VARCHAR"),
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
        added: list[str] = []
        for name, col_type in _COLUMNS:
            if name in existing:
                continue
            await conn.execute(text(f"ALTER TABLE users ADD COLUMN {name} {col_type}"))
            added.append(name)

    if added:
        print(f"OK: users {', '.join(added)}.")
    else:
        print("OK: users username, fullname already present.")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
