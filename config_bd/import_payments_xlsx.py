"""
Импорт платежей из payments.xlsx в payments_fk_sbp и продление подписки по dct_price.

Запуск из корня проекта:
  python config_bd/import_payments_xlsx.py
  python config_bd/import_payments_xlsx.py path/to/payments.xlsx
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_bd.import_from_excel import (  # noqa: E402
    DB_PATH,
    _dt_sql,
    _to_bigint,
    _to_datetime,
)
from config_bd.models import create_tables  # noqa: E402
from config_bd.utils import (  # noqa: E402
    _billing_duration_from_amount_fallback,
    _sum_subscription_end_dates,
    _white_days_from_amount_fallback,
)

DEFAULT_XLSX = ROOT / "payments.xlsx"

FK_INSERT_COLS = [
    "user_id", "amount", "time_created", "is_gift", "status", "transaction_id",
    "fk_order_id", "payload", "nonce", "signature", "method",
]

TRACKER_TXN = "from_tracker"
TRACKER_SIG = "from_tracker"
FK_ORDER_ID = 67
NONCE = 67
METHOD = "fk_qr_sbp"


def _existing_tracker_payments(conn: sqlite3.Connection) -> Set[Tuple[int, int, str]]:
    rows = conn.execute(
        """
        SELECT user_id, amount, time_created
        FROM payments_fk_sbp
        WHERE transaction_id = ? AND signature = ?
        """,
        (TRACKER_TXN, TRACKER_SIG),
    ).fetchall()
    return {(int(r[0]), int(r[1]), str(r[2])) for r in rows}


def _parse_db_dt(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(val[:26], fmt)
        except ValueError:
            continue
    return None


def _days_for_amount(amount: int) -> Tuple[Optional[int], bool]:
    """(дни, is_white)."""
    white = _white_days_from_amount_fallback(amount)
    if white is not None:
        return white, True
    pro = _billing_duration_from_amount_fallback(amount)
    if pro is not None:
        return pro, False
    return None, False


def _extend_end_date(
    current: Optional[datetime], days: int, now: datetime
) -> datetime:
    from payments.tariff_gate import add_tariff_period

    anchor = add_tariff_period(now, days)
    result = _sum_subscription_end_dates(current, anchor, now)
    return result if result is not None else anchor


def import_payments_xlsx(path: Path) -> Dict[str, int]:
    if not path.is_file():
        raise FileNotFoundError(path)

    df = pd.read_excel(path)
    required = {"user_id", "amount", "created_at"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В Excel нет колонок: {sorted(missing)}")

    asyncio.run(create_tables())

    stats = {
        "total": len(df),
        "inserted": 0,
        "skipped_dup": 0,
        "invalid": 0,
        "unknown_amount": 0,
        "user_missing": 0,
        "sub_pro": 0,
        "sub_mobile": 0,
    }

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        existing = _existing_tracker_payments(conn)
        fk_rows: List[Tuple[Any, ...]] = []
        sub_cache: Dict[int, Optional[datetime]] = {}

        records = sorted(
            df.to_dict("records"),
            key=lambda r: _to_datetime(r.get("created_at")) or datetime.min,
        )

        for rec in records:
            uid = _to_bigint(rec.get("user_id"))
            if uid is None:
                stats["invalid"] += 1
                continue

            try:
                amount = int(float(rec["amount"]))
            except (TypeError, ValueError):
                stats["invalid"] += 1
                continue

            created_at = _to_datetime(rec.get("created_at")) or datetime.now()
            time_sql = _dt_sql(created_at)
            dup_key = (uid, amount, time_sql)
            if dup_key in existing:
                stats["skipped_dup"] += 1
                continue

            fk_rows.append(
                (
                    uid,
                    amount,
                    time_sql,
                    0,
                    "confirmed",
                    TRACKER_TXN,
                    FK_ORDER_ID,
                    None,
                    NONCE,
                    TRACKER_SIG,
                    METHOD,
                )
            )
            existing.add(dup_key)

            days, is_white = _days_for_amount(amount)
            if days is None:
                stats["unknown_amount"] += 1
                continue

            if uid not in sub_cache:
                user_row = conn.execute(
                    "SELECT subscription_end_date FROM users WHERE user_id = ?",
                    (uid,),
                ).fetchone()
                if user_row is None:
                    stats["user_missing"] += 1
                    continue
                sub_cache[uid] = _parse_db_dt(user_row[0])

            pro_end = sub_cache[uid]
            now = created_at
            pro_end = _extend_end_date(pro_end, days, now)
            sub_cache[uid] = pro_end
            if is_white:
                stats["sub_mobile"] += 1
            else:
                stats["sub_pro"] += 1

        if fk_rows:
            placeholders = ", ".join("?" * len(FK_INSERT_COLS))
            col_list = ", ".join(FK_INSERT_COLS)
            sql = f"INSERT INTO payments_fk_sbp ({col_list}) VALUES ({placeholders})"
            conn.executemany(sql, fk_rows)
            stats["inserted"] = len(fk_rows)

        for uid, pro_end in sub_cache.items():
            conn.execute(
                "UPDATE users SET subscription_end_date = ? WHERE user_id = ?",
                (_dt_sql(pro_end), uid),
            )

        conn.commit()
    finally:
        conn.close()

    return stats


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(
        description="Import payments.xlsx into payments_fk_sbp and extend subscriptions"
    )
    parser.add_argument(
        "xlsx",
        nargs="?",
        type=Path,
        default=DEFAULT_XLSX,
        help=f"Path to payments.xlsx (default: {DEFAULT_XLSX})",
    )
    args = parser.parse_args()
    stats = import_payments_xlsx(args.xlsx.resolve())
    print("=== Импорт payments.xlsx ===")
    print(f"  Строк в файле: {stats['total']}")
    print(f"  Платежей добавлено: {stats['inserted']}")
    print(f"  Пропущено (дубликат tracker): {stats['skipped_dup']}")
    print(f"  Некорректные строки: {stats['invalid']}")
    print(f"  Неизвестная сумма (без дней): {stats['unknown_amount']}")
    print(f"  Пользователь не в users: {stats['user_missing']}")
    print(f"  Продлено PRO (subscription_end_date): {stats['sub_pro']}")
    print(f"  Платежи mobile-тарифа (тоже subscription_end_date): {stats['sub_mobile']}")
    print(f"  База: {DB_PATH}")


if __name__ == "__main__":
    main()
