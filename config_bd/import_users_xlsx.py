"""
Импорт пользователей из users.xlsx (lead tracker) в config_bd/socialvpn.db.
Существующие user_id пропускаются.

Запуск из корня проекта:
  python config_bd/import_users_xlsx.py
  python config_bd/import_users_xlsx.py path/to/users.xlsx
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Optional, Set, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_bd.import_from_excel import (  # noqa: E402
    DB_PATH,
    _dt_sql,
    _to_bigint,
    _to_datetime,
    _to_optional_str,
)
from config_bd.models import create_tables  # noqa: E402

DEFAULT_XLSX = ROOT / "users.xlsx"

INSERT_COLS = [
    "user_id", "ref", "is_delete", "in_panel", "is_connect", "create_user",
    "in_chanel", "reserve_field", "subscription_end_date",
    "last_notification_date", "last_broadcast_date", "stamp",
    "ttclid", "subscribtion", "email", "password_hash",
    "activation_pass", "field_str_1", "field_str_2", "field_str_3",
    "field_bool_1", "field_bool_2", "field_bool_3", "partner", "partner_balance",
    "partner_pay", "partner_flag",
]


def _existing_user_ids(conn: sqlite3.Connection) -> Set[int]:
    rows = conn.execute("SELECT user_id FROM users").fetchall()
    return {int(r[0]) for r in rows if r[0] is not None}


def _row_from_record(rec: dict) -> Optional[Tuple[Any, ...]]:
    uid = _to_bigint(rec.get("user_id"))
    if uid is None:
        return None

    created_at = _to_datetime(rec.get("created_at")) or datetime.now()
    trial_at = _to_datetime(rec.get("trial_at"))
    connected_at = _to_datetime(rec.get("connected_at"))
    source = _to_optional_str(rec.get("source"))

    in_panel = trial_at is not None
    subscription_end = (trial_at + timedelta(days=3)) if trial_at else None
    is_connect = connected_at is not None
    stamp = source if source else ""

    return (
        uid,
        None,  # ref
        0,  # is_delete
        1 if in_panel else 0,
        1 if is_connect else 0,
        _dt_sql(created_at),
        0,  # in_chanel
        0,  # reserve_field
        _dt_sql(subscription_end),
        None,  # last_notification_date
        None,  # last_broadcast_date
        stamp,
        None,  # ttclid
        None,  # subscribtion
        None,  # email
        None,  # password_hash
        None,  # activation_pass
        None,  # field_str_1
        None,  # field_str_2
        None,  # field_str_3
        0,  # field_bool_1
        0,  # field_bool_2
        0,  # field_bool_3
        None,  # partner
        0,  # partner_balance
        0,  # partner_pay
        0,  # partner_flag
    )


def import_users_xlsx(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(path)

    df = pd.read_excel(path)
    required = {"user_id", "created_at", "trial_at", "connected_at", "source"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В Excel нет колонок: {sorted(missing)}")

    asyncio.run(create_tables())

    stats = {"total": len(df), "skipped": 0, "inserted": 0, "invalid": 0}

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        existing = _existing_user_ids(conn)
        rows: List[Tuple[Any, ...]] = []

        for rec in df.to_dict("records"):
            row = _row_from_record(rec)
            if row is None:
                stats["invalid"] += 1
                continue
            uid = int(row[0])
            if uid in existing:
                stats["skipped"] += 1
                continue
            rows.append(row)
            existing.add(uid)

        if rows:
            placeholders = ", ".join("?" * len(INSERT_COLS))
            col_list = ", ".join(INSERT_COLS)
            sql = f"INSERT INTO users ({col_list}) VALUES ({placeholders})"
            conn.executemany(sql, rows)
            conn.commit()
            stats["inserted"] = len(rows)
    finally:
        conn.close()

    return stats


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="Import users.xlsx into SQLite (skip existing user_id)")
    parser.add_argument(
        "xlsx",
        nargs="?",
        type=Path,
        default=DEFAULT_XLSX,
        help=f"Path to users.xlsx (default: {DEFAULT_XLSX})",
    )
    args = parser.parse_args()
    stats = import_users_xlsx(args.xlsx.resolve())
    print("=== Импорт users.xlsx ===")
    print(f"  Строк в файле: {stats['total']}")
    print(f"  Пропущено (уже в БД): {stats['skipped']}")
    print(f"  Без user_id: {stats['invalid']}")
    print(f"  Добавлено: {stats['inserted']}")
    print(f"  База: {DB_PATH}")


if __name__ == "__main__":
    main()
