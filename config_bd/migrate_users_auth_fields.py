"""
Миграция SQLite для веб-авторизации (email, linking, reset codes).

Запуск из корня проекта:
  python -m config_bd.migrate_users_auth_fields
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from sqlalchemy import text

from config_bd.models import LinkingCodes, PasswordResetCodes, engine


async def _column_exists(conn, table: str, column: str) -> bool:
    result = await conn.execute(text(f"PRAGMA table_info({table})"))
    return any(row[1] == column for row in result.fetchall())


async def migrate() -> None:
    async with engine.begin() as conn:
        for col, ddl in (
            ("password_hash", "ALTER TABLE users ADD COLUMN password_hash VARCHAR(255)"),
        ):
            if not await _column_exists(conn, "users", col):
                await conn.execute(text(ddl))

        def _create_auth_tables(sync_conn):
            LinkingCodes.__table__.create(sync_conn, checkfirst=True)
            PasswordResetCodes.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_auth_tables)

        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email "
                "ON users (email) WHERE email IS NOT NULL"
            )
        )
        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_linking_codes_code "
                "ON linking_codes (code)"
            )
        )

    print(
        "OK: users password_hash; "
        "linking_codes, password_reset_codes (SQLite)."
    )


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
