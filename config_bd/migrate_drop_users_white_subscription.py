"""
Удаление устаревших колонок users:
- white_subscription_end_date
- white_subscription

Требуется SQLite 3.35+ (ALTER TABLE ... DROP COLUMN).

Запуск из корня проекта:
  python -m config_bd.migrate_drop_users_white_subscription
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

_COLUMNS_TO_DROP = (
    "white_subscription_end_date",
    "white_subscription",
)


async def _existing_columns(conn) -> set[str]:
    result = await conn.execute(text("PRAGMA table_info(users)"))
    return {row[1] for row in result.fetchall()}


async def migrate() -> None:
    async with engine.begin() as conn:
        existing = await _existing_columns(conn)
        dropped: list[str] = []
        for name in _COLUMNS_TO_DROP:
            if name not in existing:
                continue
            await conn.execute(text(f"ALTER TABLE users DROP COLUMN {name}"))
            dropped.append(name)

    if dropped:
        print(f"OK: dropped users columns: {', '.join(dropped)}")
    else:
        print("OK: white_subscription columns already absent.")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
