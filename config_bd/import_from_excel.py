"""
Импорт Excel-экспорта (/export, /export_full) в config_bd/socialvpn.db.
Запуск: python config_bd/import_from_excel.py "путь/к/файлу.xlsx"
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_bd.models import Users, create_tables  # noqa: E402

DB_PATH = ROOT / "config_bd" / "socialvpn.db"

DEFAULTS_REPORT: List[str] = []


def _report(msg: str) -> None:
    DEFAULTS_REPORT.append(msg)
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="backslashreplace").decode("ascii"))


def _is_na(val: Any) -> bool:
    if val is None:
        return True
    try:
        return bool(pd.isna(val))
    except (TypeError, ValueError):
        return False


def _to_optional_str(val: Any) -> Optional[str]:
    if _is_na(val):
        return None
    s = str(val).strip()
    return s if s else None


def _to_str(val: Any, default: str = "") -> str:
    if _is_na(val):
        return default
    return str(val).strip()


def _to_bool(val: Any, default: bool = False) -> bool:
    if _is_na(val):
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(int(val))
    return str(val).strip().lower() in ("1", "true", "yes", "y")


def _to_int(val: Any, default: int = 0) -> int:
    if _is_na(val):
        return default
    return int(float(val))


def _to_bigint(val: Any) -> Optional[int]:
    if _is_na(val):
        return None
    return int(float(val))


def _to_float(val: Any, default: float = 0.0) -> float:
    if _is_na(val):
        return default
    return float(val)


def _to_datetime(val: Any) -> Optional[datetime]:
    if _is_na(val):
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime.combine(val, datetime.min.time())
    ts = pd.to_datetime(val, errors="coerce")
    if _is_na(ts):
        return None
    if hasattr(ts, "to_pydatetime"):
        return ts.to_pydatetime().replace(tzinfo=None)
    return None


def _to_date(val: Any) -> Optional[date]:
    if _is_na(val):
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    ts = pd.to_datetime(val, errors="coerce")
    if _is_na(ts):
        return None
    return ts.date()


def _dt_sql(val: Any) -> Optional[str]:
    dt = _to_datetime(val)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


def _date_sql(val: Any) -> Optional[str]:
    d = _to_date(val)
    return d.isoformat() if d else None


def _clear_tables(conn: sqlite3.Connection) -> None:
    tables = [
        "password_reset_codes",
        "linking_codes",
        "white_counter",
        "online",
        "gifts",
        "payments_cryptobot",
        "payments_stars",
        "payments_fk_sbp",
        "payments_wata_card",
        "payments_wata_sbp",
        "payments_platega_crypto",
        "payments_cards",
        "payments",
        "users",
    ]
    conn.execute("PRAGMA foreign_keys=OFF")
    for name in tables:
        conn.execute(f"DELETE FROM {name}")
    conn.commit()


def _update_sqlite_sequence(conn: sqlite3.Connection, table: str, pk_col: str = "id") -> None:
    """Синхронизирует sqlite_sequence после импорта с явными id (если таблица уже есть)."""
    row = conn.execute(f"SELECT MAX({pk_col}) FROM {table}").fetchone()
    if not row or row[0] is None:
        return
    max_id = int(row[0])
    try:
        updated = conn.execute(
            "UPDATE sqlite_sequence SET seq=? WHERE name=?", (max_id, table)
        ).rowcount
        if not updated:
            conn.execute(
                "INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)", (table, max_id)
            )
    except sqlite3.OperationalError:
        pass


def _bulk_insert(
    conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    rows: List[Tuple[Any, ...]],
    *,
    pk_col: str = "id",
    update_sequence: bool = True,
) -> int:
    if not rows:
        return 0
    placeholders = ", ".join("?" * len(columns))
    col_list = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    conn.executemany(sql, rows)
    if update_sequence:
        _update_sqlite_sequence(conn, table, pk_col)
    return len(rows)


def _import_users(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    db_cols = {c.key for c in Users.__table__.columns}
    excel_cols = set(df.columns)
    missing = sorted(db_cols - excel_cols)
    if missing:
        _report(f"[users] В Excel нет колонок БД — подставлены значения по умолчанию: {missing}")

    stamp_defaulted = 0
    rows: List[Tuple[Any, ...]] = []
    for rec in df.to_dict("records"):
        stamp_val = rec.get("stamp")
        if _is_na(stamp_val):
            stamp_defaulted += 1
            stamp_val = ""
        rows.append(
            (
                _to_int(rec["id"]),
                _to_bigint(rec["user_id"]),
                _to_optional_str(rec.get("ref")),
                _to_bool(rec.get("is_delete")),
                _to_bool(rec.get("in_panel")),
                _to_bool(rec.get("is_connect")),
                _dt_sql(rec.get("create_user")) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                _to_bool(rec.get("in_chanel")),
                _to_bool(rec.get("reserve_field")),
                _dt_sql(rec.get("subscription_end_date")),
                _date_sql(rec.get("last_notification_date")),
                _to_optional_str(rec.get("last_broadcast_status")),
                _dt_sql(rec.get("last_broadcast_date")),
                _to_str(stamp_val, ""),
                _to_optional_str(rec.get("ttclid")),
                _to_optional_str(rec.get("subscribtion")),
                _to_optional_str(rec.get("email")),
                _to_optional_str(rec.get("password")),
                None,  # password_hash
                None,  # linked_telegram_id
                _to_optional_str(rec.get("activation_pass")),
                _to_optional_str(rec.get("field_str_1")),
                _to_optional_str(rec.get("field_str_2")),
                _to_optional_str(rec.get("field_str_3")),
                _to_bool(rec.get("field_bool_1")),
                _to_bool(rec.get("field_bool_2")),
                _to_bool(rec.get("field_bool_3")),
                _to_optional_str(rec.get("partner")),
                _to_int(rec.get("partner_balance")),
                _to_int(rec.get("partner_pay")),
                _to_bool(rec.get("partner_flag")),
            )
        )
    if stamp_defaulted:
        _report(f"[users] stamp пустой у {stamp_defaulted} строк — заменён на ''")

    cols = [
        "id", "user_id", "ref", "is_delete", "in_panel", "is_connect", "create_user",
        "in_chanel", "reserve_field", "subscription_end_date",
        "last_notification_date", "last_broadcast_status", "last_broadcast_date", "stamp",
        "ttclid", "subscribtion", "email", "password", "password_hash",
        "linked_telegram_id", "activation_pass", "field_str_1", "field_str_2", "field_str_3",
        "field_bool_1", "field_bool_2", "field_bool_3", "partner", "partner_balance",
        "partner_pay", "partner_flag",
    ]
    return _bulk_insert(conn, "users", cols, rows)


def _import_payments_sbp(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    _report("[payments] Колонка payload отсутствует в Excel — в БД остаётся NULL")
    return _import_payments_generic(conn, df, "payments")


def _import_payments_generic(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table: str,
    *,
    with_payload: bool = False,
    payload_default: Optional[str] = None,
) -> int:
    has_payload_col = any(str(c).lower() == "payload" for c in df.columns)
    if with_payload and not has_payload_col:
        _report(f"[{table}] Колонка payload отсутствует — используется NULL")
    rows = []
    for rec in df.to_dict("records"):
        row: List[Any] = [
            _to_int(rec["ID"]),
            _to_bigint(rec["User ID"]),
            _to_int(rec["Amount"]),
            _dt_sql(rec["Time Created"]) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            _to_bool(rec.get("Is Gift")),
            _to_optional_str(rec.get("Status")),
            _to_optional_str(rec.get("Transaction_Id")),
        ]
        if with_payload:
            payload = rec.get("Payload")
            row.append(_to_optional_str(payload) if not _is_na(payload) else payload_default)
        rows.append(tuple(row))

    cols = ["id", "user_id", "amount", "time_created", "is_gift", "status", "transaction_id"]
    if with_payload:
        cols.append("payload")
    return _bulk_insert(conn, table, cols, rows)


def _import_payments_stars(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    _report("[payments_stars] Колонка payload отсутствует в Excel — используется NULL")
    rows = []
    for rec in df.to_dict("records"):
        amount = rec.get("Amount (Stars)", rec.get("Amount"))
        rows.append(
            (
                _to_int(rec["ID"]),
                _to_bigint(rec["User ID"]),
                _to_int(amount),
                _dt_sql(rec["Time Created"]) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                _to_bool(rec.get("Is Gift")),
                _to_optional_str(rec.get("Status")) or "confirmed",
                None,
            )
        )
    cols = ["id", "user_id", "amount", "time_created", "is_gift", "status", "payload"]
    return _bulk_insert(conn, "payments_stars", cols, rows)


def _import_payments_fk(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    nonce_defaulted = 0
    method_defaulted = 0
    rows = []
    for rec in df.to_dict("records"):
        nonce = rec.get("Nonce")
        if _is_na(nonce):
            nonce_defaulted += 1
            nonce = 0
        method = rec.get("Method")
        if _is_na(method):
            method_defaulted += 1
            method = "fk_qr_card"
        rows.append(
            (
                _to_int(rec["ID"]),
                _to_bigint(rec["User ID"]),
                _to_int(rec["Amount"]),
                _dt_sql(rec["Time Created"]) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                _to_bool(rec.get("Is Gift")),
                _to_optional_str(rec.get("Status")),
                _to_optional_str(rec.get("Transaction_Id")),
                _to_int(rec.get("FK_Order_Id")) if not _is_na(rec.get("FK_Order_Id")) else None,
                _to_optional_str(rec.get("Payload")),
                int(float(nonce)),
                _to_optional_str(rec.get("Signature")),
                _to_str(method, "fk_qr_card"),
            )
        )
    if nonce_defaulted:
        _report(f"[payments_fk_sbp] nonce пустой у {nonce_defaulted} строк — подставлен 0")
    if method_defaulted:
        _report(f"[payments_fk_sbp] method пустой у {method_defaulted} строк — подставлен fk_qr_card")

    cols = [
        "id", "user_id", "amount", "time_created", "is_gift", "status", "transaction_id",
        "fk_order_id", "payload", "nonce", "signature", "method",
    ]
    return _bulk_insert(conn, "payments_fk_sbp", cols, rows)


def _import_payments_cryptobot(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = []
    for rec in df.to_dict("records"):
        rows.append(
            (
                _to_int(rec["ID"]),
                _to_bigint(rec["User ID"]),
                _to_float(rec["Amount"]),
                _to_str(rec.get("Currency"), "RUB"),
                _dt_sql(rec["Time Created"]) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                _to_bool(rec.get("Is Gift")),
                _to_optional_str(rec.get("Status")) or "pending",
                _to_optional_str(rec.get("Invoice ID")),
                _to_optional_str(rec.get("Payload")),
            )
        )
    cols = [
        "id", "user_id", "amount", "currency", "time_created", "is_gift",
        "status", "invoice_id", "payload",
    ]
    return _bulk_insert(conn, "payments_cryptobot", cols, rows)


def _import_gifts(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = []
    for rec in df.to_dict("records"):
        rows.append(
            (
                _to_str(rec["gift_id"]),
                _to_bigint(rec["giver_id"]),
                _to_int(rec["duration"]),
                _to_bigint(rec.get("recepient_id")),
                _to_bool(rec.get("white_flag")),
                _to_bool(rec.get("flag")),
            )
        )
    cols = ["gift_id", "giver_id", "duration", "recepient_id", "white_flag", "flag"]
    return _bulk_insert(conn, "gifts", cols, rows, update_sequence=False)


def _import_online(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    col_map = {
        "ID": "online_id",
        "Дата сбора": "online_date",
        "Всего в панели": "users_panel",
        "Активны сегодня": "users_active",
        "Платных": "users_pay",
        "Триальных": "users_trial",
        "С активной подпиской": "users_subscribed",
    }
    rows = []
    for rec in df.to_dict("records"):
        rows.append(
            (
                _to_int(rec["ID"]),
                _dt_sql(rec["Дата сбора"]) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                _to_int(rec["Всего в панели"]),
                _to_int(rec["Активны сегодня"]),
                _to_int(rec["Платных"]),
                _to_int(rec["Триальных"]),
                _to_bigint(rec.get("С активной подпиской")),
            )
        )
    cols = [
        "online_id", "online_date", "users_panel", "users_active",
        "users_pay", "users_trial", "users_subscribed",
    ]
    return _bulk_insert(conn, "online", cols, rows, pk_col="online_id")


def _import_white_counter(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = []
    for rec in df.to_dict("records"):
        rows.append(
            (
                _to_int(rec["ID"]),
                _to_bigint(rec["User ID"]),
                _dt_sql(rec["Time Created"]) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
    cols = ["id", "user_id", "time_created"]
    return _bulk_insert(conn, "white_counter", cols, rows)


SHEET_IMPORTERS: Dict[str, Callable[[sqlite3.Connection, pd.DataFrame], int]] = {
    "users": _import_users,
    "payments_sbp": lambda c, d: _import_payments_sbp(c, d),
    "payments_cards": lambda c, d: _import_payments_generic(c, d, "payments_cards", with_payload=True),
    "payments_stars": _import_payments_stars,
    "payments_platega_crypto": lambda c, d: _import_payments_generic(
        c, d, "payments_platega_crypto", with_payload=True
    ),
    "payments_fk_sbp": _import_payments_fk,
    "payments_wata_sbp": lambda c, d: _import_payments_generic(c, d, "payments_wata_sbp", with_payload=True),
    "payments_wata_card": lambda c, d: _import_payments_generic(c, d, "payments_wata_card", with_payload=True),
    "payments_cryptobot": _import_payments_cryptobot,
    "gifts": _import_gifts,
    "online": _import_online,
    "white_counter": _import_white_counter,
}


def import_excel(path: Path, *, replace: bool = True) -> Dict[str, int]:
    if not path.is_file():
        raise FileNotFoundError(path)

    asyncio.run(create_tables())

    xls = pd.ExcelFile(path)
    stats: Dict[str, int] = {}

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        if replace:
            _clear_tables(conn)

        for sheet in xls.sheet_names:
            if sheet not in SHEET_IMPORTERS:
                _report(f"[{sheet}] Лист пропущен — нет обработчика")
                continue
            df = pd.read_excel(xls, sheet_name=sheet)
            count = SHEET_IMPORTERS[sheet](conn, df)
            conn.commit()
            stats[sheet] = count
            print(f"OK {sheet}: {count} rows")
    finally:
        conn.close()

    return stats


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    parser = argparse.ArgumentParser(description="Import SocialmediaVPN Excel export into SQLite DB")
    parser.add_argument("xlsx", type=Path, help="Path to .xlsx export file")
    parser.add_argument(
        "--no-replace",
        action="store_true",
        help="Append without clearing existing tables",
    )
    args = parser.parse_args()
    stats = import_excel(args.xlsx.resolve(), replace=not args.no_replace)
    print("\n=== Итого ===")
    for sheet, n in stats.items():
        print(f"  {sheet}: {n}")
    if DEFAULTS_REPORT:
        print("\n=== Подставленные значения по умолчанию ===")
        for line in DEFAULTS_REPORT:
            print(f"  {line}")
    print(f"\nБаза: {DB_PATH}")


if __name__ == "__main__":
    main()
