"""Создаёт таблицу second_site для landing API (если ещё нет)."""
from sqlalchemy import inspect

from config_bd.models import Base, SecondSite, engine


async def migrate() -> None:
    async with engine.begin() as conn:
        def _ensure(sync_conn):
            insp = inspect(sync_conn)
            if not insp.has_table(SecondSite.__tablename__):
                Base.metadata.create_all(sync_conn, tables=[SecondSite.__table__])

        await conn.run_sync(_ensure)