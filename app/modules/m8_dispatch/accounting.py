"""月雨量核算：从实际日值出发，复用既有采信规则得出月量依据并比对警戒线。

重要边界：
- 日值版本裁决、零雨/缺测/冲突处理一律调用 m5_months.hydro.select_daily，
  月合计调用 hydro.monthly；本函数只消费其结果，不复制、不改写采信规则；
- 本模块新增的只是「月量 vs 警戒线」的预警判定，阈值可按站配置。
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation

from ...errors import BadRequest
from ..m5_months.hydro import monthly, select_daily
from . import constants as C

# 预警判定方向：月量 >= 警戒线（含等于）即预警；更正到严格小于阈值才撤警
# （边界含义写进返回字段 below_threshold，避免调用方各自再判一次）。


def _parse_record(row: dict, index: int) -> dict:
    """单条日值校验；采信本身不在这做，这里只保证字段可被 hydro 消费。"""
    prefix = f"日值第 {index + 1} 行"
    station = row.get("station")
    date_text = row.get("date")
    if station in (None, ""):
        raise BadRequest(f"{prefix}缺少 station", code=C.E_BAD_RECORD)
    if not date_text:
        raise BadRequest(f"{prefix}缺少 date", code=C.E_BAD_RECORD)
    try:
        dt.date.fromisoformat(str(date_text))
    except ValueError as exc:
        raise BadRequest(f"{prefix}日期格式应为 YYYY-MM-DD：{date_text}",
                         code=C.E_BAD_RECORD) from exc
    try:
        rainfall = Decimal(str(row.get("rainfall_mm")))
    except (InvalidOperation, TypeError) as exc:
        raise BadRequest(f"{prefix}雨量不是数值：{row.get('rainfall_mm')}",
                         code=C.E_BAD_RECORD) from exc
    if rainfall < 0:
        raise BadRequest(f"{prefix}雨量不能为负：{rainfall}", code=C.E_BAD_RECORD)
    try:
        version = int(row.get("version", 0))
    except (TypeError, ValueError) as exc:
        raise BadRequest(f"{prefix}版本号不是整数：{row.get('version')}",
                         code=C.E_BAD_RECORD) from exc
    return {"station": str(station), "date": str(date_text),
            "rainfall_mm": str(rainfall), "version": version}


def normalize_records(records: list[dict]) -> list[dict]:
    return [_parse_record(row, i) for i, row in enumerate(records or [])]


def expected_days_for(months: list[str]) -> dict[str, int]:
    """按自然月日历给出应有天数，月量依据里保留缺测日信息。"""
    out: dict[str, int] = {}
    for month in months:
        year, mon = (int(part) for part in month.split("-"))
        if mon == 12:
            nxt = dt.date(year + 1, 1, 1)
        else:
            nxt = dt.date(year, mon + 1, 1)
        last = nxt - dt.timedelta(days=1)
        out[month] = last.day
    return out


def evaluate(records: list[dict],
             thresholds: dict[str, Decimal] | None = None,
             default_threshold: Decimal = C.DEFAULT_THRESHOLD_MM) -> dict:
    """核算入口：日值 → 采信裁决 → 月合计 → 警戒线判定。

    返回结构：
    {
      "selection": {...},          # select_daily 原始裁决（含 duplicates/conflicts）
      "monthly": [ {...}, ... ],   # hydro.monthly 月量（含 days/missing_days/state）
      "basis": [                   # 逐月逐站的月量依据（回读用）
        {station, month, rainfall_mm, days, missing_days, state,
         threshold_mm, above_threshold, conflicts}
      ],
      "warning_keys": [(station, month), ...]  # 达到/超过警戒线的组合
    }
    """
    clean = normalize_records(records)
    selection = select_daily(clean)
    months = sorted({str(row["date"])[:7] for row in selection["selected"]})
    expected = expected_days_for(months)
    totals = monthly(selection["selected"], expected)

    conflict_keys = {(item["station"], item["date"][:7])
                     for item in selection["conflicts"]}
    thresholds = {str(k): Decimal(str(v)) for k, v in (thresholds or {}).items()}

    def _basis_row(station: str, month: str, amount: Decimal | None,
                   days: int, missing: int, state: str,
                   conflict_count: int) -> dict:
        threshold = thresholds.get(station, Decimal(str(default_threshold)))
        text_amount = str(amount.quantize(C.RAIN_QUANT)) if amount is not None else None
        above = amount is not None and amount >= threshold
        return {
            "station": station,
            "month": month,
            "rainfall_mm": text_amount,
            "days": days,
            "missing_days": missing,
            "state": state,
            "threshold_mm": str(threshold),
            "above_threshold": above,
            "below_threshold": amount is None or amount < threshold,
            "conflicts": conflict_count,
        }

    basis: list[dict] = []
    warning_keys: list[tuple[str, str]] = []
    seen_keys: set[tuple[str, str]] = set()
    conflict_months: dict[tuple[str, str], int] = {}
    for key in conflict_keys:
        conflict_months[key] = conflict_months.get(key, 0) + 1

    for item in totals:
        station, month = item["station"], item["month"]
        amount = Decimal(item["rainfall_mm"])
        seen_keys.add((station, month))
        row = _basis_row(station, month, amount, item["days"],
                         item["missing_days"], item["state"],
                         conflict_months.get((station, month), 0))
        basis.append(row)
        if row["above_threshold"]:
            warning_keys.append((station, month))

    # 整月日值都在版本冲突中被隔离时，月合计没有行；依据里仍要显式挂出，
    # 标明 state=conflict 且不产生预警（无可信月量，禁止据此派单）。
    for (station, month), conflict_count in sorted(conflict_months.items()):
        if (station, month) in seen_keys:
            continue
        expected = expected.get(month, 0)
        basis.append(_basis_row(station, month, None, 0, expected,
                                "conflict", conflict_count))
    return {"selection": selection, "monthly": totals, "basis": basis,
            "warning_keys": warning_keys}
