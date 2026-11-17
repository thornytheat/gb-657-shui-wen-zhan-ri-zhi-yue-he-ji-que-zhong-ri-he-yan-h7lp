"""M8 月量核算与状态机：采信规则复用、警戒线边界、乱序不倒退。"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.modules.m8_dispatch import accounting, constants as C, statemachine


def _records(station="S1", month="2026-06", values=None, version=1):
    values = values or []
    return [{"station": station, "date": f"{month}-{day:02d}",
             "rainfall_mm": str(value), "version": version}
            for day, value in enumerate(values, start=1)]


def test_evaluate_reuses_hydro_selection_rules():
    # 同日两个版本：高版本胜出（复用 select_daily，本模块不重写采信）
    records = [
        {"station": "S1", "date": "2026-06-01", "rainfall_mm": "5.0", "version": 1},
        {"station": "S1", "date": "2026-06-01", "rainfall_mm": "9.0", "version": 2},
        {"station": "S1", "date": "2026-06-02", "rainfall_mm": "0.0", "version": 1},
    ]
    result = accounting.evaluate(records, default_threshold=Decimal("10"))
    basis = {b["month"]: b for b in result["basis"]}
    item = basis["2026-06"]
    assert item["rainfall_mm"] == "9.0"          # 9.0（v2）+ 0.0
    assert result["selection"]["duplicates"]


def test_threshold_boundary_equal_is_warning_strict_below_clears():
    over = accounting.evaluate(_records(values=[300.0]),
                               default_threshold=Decimal("300"))
    assert over["basis"][0]["above_threshold"] is True
    assert over["basis"][0]["below_threshold"] is False
    assert over["warning_keys"] == [("S1", "2026-06")]

    below = accounting.evaluate(_records(values=[299.9]),
                                default_threshold=Decimal("300"))
    assert below["basis"][0]["above_threshold"] is False
    assert below["basis"][0]["below_threshold"] is True
    assert below["warning_keys"] == []


def test_station_specific_threshold_overrides_default():
    records = _records(station="S9", values=[120.0])
    result = accounting.evaluate(records, thresholds={"S9": Decimal("100")},
                                 default_threshold=Decimal("300"))
    assert result["basis"][0]["threshold_mm"] == "100"
    assert result["basis"][0]["above_threshold"] is True


def test_bad_records_raise_bad_request():
    with pytest.raises(Exception):
        accounting.evaluate([{"station": "S1", "date": "bad", "rainfall_mm": "1"}])
    with pytest.raises(Exception):
        accounting.evaluate([{"station": "S1", "date": "2026-06-01",
                              "rainfall_mm": "-2"}])


def test_version_conflict_is_counted_not_silently_dropped():
    records = [
        {"station": "S1", "date": "2026-06-01", "rainfall_mm": "5", "version": 1},
        {"station": "S1", "date": "2026-06-01", "rainfall_mm": "8", "version": 1},
    ]
    result = accounting.evaluate(records, default_threshold=Decimal("1"))
    assert result["selection"]["conflicts"]
    row = result["basis"][0]
    assert row["state"] == "conflict"
    assert row["rainfall_mm"] is None
    assert row["conflicts"] == 1
    assert result["warning_keys"] == []  # 无可信月量，禁止据此派单


# ---------------- 状态机 ----------------
def _order(state, **kw):
    base = {"state": state, "close_reason": None, "accept_seq": None,
            "withdraw_seq": None, "withdraw_confirm_seq": None}
    base.update(kw)
    return base


def test_state_machine_is_forward_only():
    assert statemachine.can_transition(C.PENDING, C.DISPATCHED)
    assert statemachine.can_transition(C.DISPATCHED, C.WITHDRAWING)
    assert statemachine.can_transition(C.WITHDRAWING, C.CLOSED)
    assert not statemachine.can_transition(C.CLOSED, C.DISPATCHED)
    assert not statemachine.can_transition(C.WITHDRAWING, C.DISPATCHED)
    assert statemachine.is_terminal(C.CLOSED)


def test_out_of_order_accept_never_regresses():
    order = _order(C.DISPATCHED, accept_seq=5)
    action, _ = statemachine.decide_accept(order, 4)
    assert action == "stale"
    action, _ = statemachine.decide_accept(order, 5)  # 同序号重复
    assert action == "stale"


def test_late_accept_during_withdraw_goes_to_withdrawing():
    order = _order(C.WITHDRAWING)
    action, reason = statemachine.decide_accept(order, 3)
    assert action == "late_to_withdraw"
    assert reason == "WITHDRAW_IN_PROGRESS"


def test_cancelled_order_late_accept_reopens_but_withdrawn_does_not():
    cancelled = _order(C.CLOSED, close_reason=C.CLOSE_CANCELLED)
    action, _ = statemachine.decide_accept(cancelled, 2)
    assert action == "late_reopen"

    withdrawn = _order(C.CLOSED, close_reason=C.CLOSE_WITHDRAWN)
    action, reason = statemachine.decide_accept(withdrawn, 2)
    assert action == "stale"
    assert reason == "ALREADY_WITHDRAWN"


def test_withdraw_confirm_only_closes_in_withdrawing():
    order = _order(C.DISPATCHED)
    action, reason = statemachine.decide_withdraw_confirm(order, 1)
    assert action == "stale" and reason == "WITHDRAW_NOT_REQUESTED"
    order2 = _order(C.WITHDRAWING)
    action, _ = statemachine.decide_withdraw_confirm(order2, 1)
    assert action == "close"
