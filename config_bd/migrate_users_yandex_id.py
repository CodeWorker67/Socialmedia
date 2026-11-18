"""
Миграция users — поле yandex_id (String, nullable).

  python -m config_bd.migrate_users_yandex_id
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
        if "yandex_id" not in existing:
            await conn.execute(text("ALTER TABLE users ADD COLUMN yandex_id VARCHAR(100)"))

    print("OK: users.yandex_id")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()