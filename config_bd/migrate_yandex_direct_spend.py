"""
Таблица yandex_direct_spend — траты по кабинетам Yandex Direct.

  python -m config_bd.migrate_yandex_direct_spend
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


async def migrate() -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS yandex_direct_spend (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stamp VARCHAR(100) NOT NULL,
                    spend_usd NUMERIC(12, 2) NOT NULL,
                    spend_date DATE NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_yandex_direct_spend_stamp_date UNIQUE (stamp, spend_date)
                )
                """
            )
        )

    print("OK: yandex_direct_spend")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()