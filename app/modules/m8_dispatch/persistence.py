"""M8 持久层：预警、派单、日值依据、回执序号、事件台账、幂等键。

独立 SQLite 文件（dispatch_db_path），与主库 hydrocalendar.sqlite 分开，
派单子系统的表结构演进不影响既有七个模块。

并发约定：
- SQLite 开启 WAL + busy_timeout，连接级事务串行化；
- 服务层再用一把进程内互斥锁包住所有状态迁移，保证「读-判-写」原子；
- 撤警并发用 warnings.version 做乐观锁（CAS），过期决定返回 409。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS m8_daily (
    station TEXT NOT NULL,
    rain_date TEXT NOT NULL,
    version INTEGER NOT NULL,
    rainfall_mm TEXT NOT NULL,
    batch_no INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (station, rain_date, version)
);
CREATE TABLE IF NOT EXISTS m8_warnings (
    warning_id TEXT PRIMARY KEY,
    station TEXT NOT NULL,
    month TEXT NOT NULL,
    rainfall_mm TEXT NOT NULL,
    threshold_mm TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    basis_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (station, month)
);
CREATE TABLE IF NOT EXISTS m8_orders (
    order_id TEXT PRIMARY KEY,
    warning_id TEXT NOT NULL,
    station TEXT NOT NULL,
    month TEXT NOT NULL,
    state TEXT NOT NULL,
    close_reason TEXT,
    dispatch_seq INTEGER,
    withdraw_seq INTEGER,
    accept_seq INTEGER,
    withdraw_confirm_seq INTEGER,
    last_error TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_m8_orders_warning ON m8_orders(warning_id);
CREATE INDEX IF NOT EXISTS idx_m8_orders_state ON m8_orders(state);
CREATE TABLE IF NOT EXISTS m8_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    ref_type TEXT NOT NULL,
    ref_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_m8_ledger_ref ON m8_ledger(ref_type, ref_id, id);
CREATE TABLE IF NOT EXISTS m8_idempotency (
    idem_key TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m8_station_seen (
    order_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    seq INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (order_id, kind, seq)
);
"""


class DispatchStore:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        # PRAGMA journal_mode 不能在事务内执行，用独立自动提交连接设置
        boot = sqlite3.connect(self.path, timeout=20.0, isolation_level=None)
        try:
            boot.execute("PRAGMA journal_mode=WAL")
            boot.execute("PRAGMA foreign_keys=ON")
            boot.executescript(SCHEMA)
        finally:
            boot.close()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=20.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=20000")
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------------- 日值 ----------------
    def replace_daily(self, conn, records: list[dict], batch_no: int, stamp: str) -> None:
        """整批覆盖日值（同 station+date+version 替换），保留历史版本行用于回读。"""
        for row in records:
            conn.execute(
                """INSERT INTO m8_daily (station,rain_date,version,rainfall_mm,batch_no,created_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(station,rain_date,version) DO UPDATE SET
                     rainfall_mm=excluded.rainfall_mm, batch_no=excluded.batch_no,
                     created_at=excluded.created_at""",
                (row["station"], row["date"], row["version"], row["rainfall_mm"],
                 batch_no, stamp))

    def list_daily(self, station: str | None = None,
                   month: str | None = None) -> list[dict]:
        sql = "SELECT * FROM m8_daily WHERE 1=1"
        args: list = []
        if station:
            sql += " AND station=?"
            args.append(station)
        if month:
            sql += " AND substr(rain_date,1,7)=?"
            args.append(month)
        sql += " ORDER BY station, rain_date, version"
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]

    def next_batch_no(self, conn) -> int:
        row = conn.execute("SELECT COALESCE(MAX(batch_no),0)+1 AS n FROM m8_daily").fetchone()
        return int(row["n"])

    # ---------------- 预警 ----------------
    def get_warning(self, conn, warning_id: str) -> dict | None:
        row = conn.execute("SELECT * FROM m8_warnings WHERE warning_id=?",
                           (warning_id,)).fetchone()
        return dict(row) if row else None

    def find_warning(self, conn, station: str, month: str) -> dict | None:
        row = conn.execute(
            "SELECT * FROM m8_warnings WHERE station=? AND month=?",
            (station, month)).fetchone()
        return dict(row) if row else None

    def list_warnings(self, state: str | None = None) -> list[dict]:
        sql = "SELECT * FROM m8_warnings"
        args: list = []
        if state:
            sql += " WHERE state=?"
            args.append(state)
        sql += " ORDER BY month, station"
        with self.connect() as conn:
            rows = [dict(r) for r in conn.execute(sql, args).fetchall()]
        for row in rows:
            row["basis"] = json.loads(row.pop("basis_json") or "{}")
        return rows

    def upsert_warning(self, conn, warning: dict) -> None:
        conn.execute(
            """INSERT INTO m8_warnings (warning_id,station,month,rainfall_mm,threshold_mm,
                 state,version,basis_json,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(warning_id) DO UPDATE SET
                 rainfall_mm=excluded.rainfall_mm, threshold_mm=excluded.threshold_mm,
                 state=excluded.state, version=excluded.version,
                 basis_json=excluded.basis_json, updated_at=excluded.updated_at""",
            (warning["warning_id"], warning["station"], warning["month"],
             warning["rainfall_mm"], warning["threshold_mm"], warning["state"],
             warning["version"], json.dumps(warning.get("basis", {}), ensure_ascii=False),
             warning["created_at"], warning["updated_at"]))

    def cas_warning_state(self, conn, warning_id: str, expected_version: int,
                          new_state: str, stamp: str) -> dict | None:
        """乐观锁：仅当 version 未变才落撤警决定，返回更新后的行，过期返回 None。"""
        cur = self.get_warning(conn, warning_id)
        if cur is None or cur["version"] != expected_version:
            return None
        conn.execute(
            "UPDATE m8_warnings SET state=?, version=version+1, updated_at=? "
            "WHERE warning_id=? AND version=?",
            (new_state, stamp, warning_id, expected_version))
        return self.get_warning(conn, warning_id)

    # ---------------- 派单 ----------------
    def insert_order(self, conn, order: dict) -> None:
        conn.execute(
            """INSERT INTO m8_orders (order_id,warning_id,station,month,state,close_reason,
                 dispatch_seq,withdraw_seq,accept_seq,last_error,attempts,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (order["order_id"], order["warning_id"], order["station"], order["month"],
             order["state"], order.get("close_reason"), order.get("dispatch_seq"),
             order.get("withdraw_seq"), order.get("accept_seq"),
             order.get("last_error", ""), order.get("attempts", 0),
             order["created_at"], order["updated_at"]))

    def get_order(self, conn, order_id: str) -> dict | None:
        row = conn.execute("SELECT * FROM m8_orders WHERE order_id=?",
                           (order_id,)).fetchone()
        return dict(row) if row else None

    def list_orders(self, warning_id: str | None = None,
                    state: str | None = None) -> list[dict]:
        sql = "SELECT * FROM m8_orders WHERE 1=1"
        args: list = []
        if warning_id:
            sql += " AND warning_id=?"
            args.append(warning_id)
        if state:
            sql += " AND state=?"
            args.append(state)
        sql += " ORDER BY created_at, order_id"
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]

    def list_orders_conn(self, conn, warning_id: str) -> list[dict]:
        rows = conn.execute(
            "SELECT * FROM m8_orders WHERE warning_id=? ORDER BY created_at, order_id",
            (warning_id,)).fetchall()
        return [dict(r) for r in rows]

    def update_order(self, conn, order_id: str, **fields) -> None:
        if not fields:
            return
        columns = ", ".join(f"{key}=?" for key in fields)
        values = list(fields.values()) + [order_id]
        conn.execute(f"UPDATE m8_orders SET {columns} WHERE order_id=?", values)

    # ---------------- ledger / 幂等 / 回执去重 ----------------
    def append_ledger(self, conn, event_type: str, ref_type: str, ref_id: str,
                      detail: dict, stamp: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(event_seq),0)+1 AS n FROM m8_ledger "
            "WHERE ref_type=? AND ref_id=?", (ref_type, ref_id)).fetchone()
        seq = int(row["n"])
        conn.execute(
            """INSERT INTO m8_ledger (event_type,ref_type,ref_id,event_seq,detail_json,created_at)
               VALUES (?,?,?,?,?,?)""",
            (event_type, ref_type, ref_id, seq,
             json.dumps(detail, ensure_ascii=False), stamp))
        return seq

    def list_ledger(self, ref_type: str | None = None,
                    ref_id: str | None = None) -> list[dict]:
        sql = "SELECT * FROM m8_ledger WHERE 1=1"
        args: list = []
        if ref_type:
            sql += " AND ref_type=?"
            args.append(ref_type)
        if ref_id:
            sql += " AND ref_id=?"
            args.append(ref_id)
        sql += " ORDER BY id"
        with self.connect() as conn:
            rows = [dict(r) for r in conn.execute(sql, args).fetchall()]
        for row in rows:
            row["detail"] = json.loads(row.pop("detail_json") or "{}")
        return rows

    def save_idem(self, conn, key: str, response: dict, stamp: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO m8_idempotency (idem_key,response_json,created_at) "
            "VALUES (?,?,?)",
            (key, json.dumps(response, ensure_ascii=False), stamp))

    def get_idem(self, key: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT response_json FROM m8_idempotency WHERE idem_key=?",
                               (key,)).fetchone()
        return json.loads(row["response_json"]) if row else None

    def seen_receipt(self, conn, order_id: str, kind: str, seq: int) -> bool:
        row = conn.execute(
            "SELECT 1 FROM m8_station_seen WHERE order_id=? AND kind=? AND seq=?",
            (order_id, kind, seq)).fetchone()
        return row is not None

    def remember_receipt(self, conn, order_id: str, kind: str, seq: int,
                         stamp: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO m8_station_seen (order_id,kind,seq,created_at) "
            "VALUES (?,?,?,?)", (order_id, kind, seq, stamp))
