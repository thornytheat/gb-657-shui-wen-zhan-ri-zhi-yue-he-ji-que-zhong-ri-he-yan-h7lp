"""M8 HTTP 端到端：核算预警 → 派单 → 撤警 → 回执 → 中断恢复 → 对账回读。

全程只打本地模拟值班站（/api/station-sim/*），没有任何真实通知出口。
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.errors import Conflict
from app.modules.m8_dispatch.service import _now, _order_id


def _rain(station="S1", days=3, per_day="120.0", month="2026-06", version=1):
    return [{"station": station, "date": f"{month}-{d:02d}",
             "rainfall_mm": per_day, "version": version} for d in range(1, days + 1)]


def test_evaluate_dispatch_withdraw_and_readback_over_http(client):
    # 1) 日值核算并自动派单
    resp = client.post("/api/dispatch/warnings/evaluate",
                       json={"records": _rain(), "auto_dispatch": True})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    warning_id = body["warnings_opened"][0]
    assert body["basis"][0]["rainfall_mm"] == "360.0"

    # 2) 月量依据可回读
    basis = client.get("/api/dispatch/basis").json()["basis"]
    assert any(b["station"] == "S1" and b["above_threshold"] for b in basis)

    # 3) 待派/已接状态：派单成功后为 DISPATCHED
    orders = client.get("/api/dispatch/orders").json()["orders"]
    assert len(orders) == 1
    oid = orders[0]["order_id"]
    assert orders[0]["state"] == "DISPATCHED"

    # 4) 模拟值班站接单，回执入账
    accept = client.post(f"/api/station-sim/orders/{oid}/accept").json()
    r = client.post(f"/api/dispatch/orders/{oid}/receipts/accept",
                    json={"accept_seq": accept["accept_seq"]})
    assert r.status_code == 200
    assert r.json()["state"] == "DISPATCHED"

    # 5) 手动确认模式后撤警：进入待撤，确认后结案
    client.post("/api/station-sim/faults",
                json={"order_id": oid, "manual_confirm": True})
    wd = client.post(f"/api/dispatch/warnings/{warning_id}/withdraw").json()
    assert wd["withdrawing_orders"] == [oid]
    assert client.get(f"/api/dispatch/orders/{oid}").json()["state"] == "WITHDRAWING"
    confirm = client.post(f"/api/station-sim/orders/{oid}/withdraw-confirm").json()
    client.post(f"/api/dispatch/orders/{oid}/receipts/withdraw-confirm",
                json={"confirm_seq": confirm["withdraw_confirm_seq"]})
    final = client.get(f"/api/dispatch/orders/{oid}").json()
    assert final["state"] == "CLOSED" and final["close_reason"] == "WITHDRAWN"
    assert client.get(f"/api/dispatch/warnings/{warning_id}").json()["state"] == "WITHDRAWN"

    # 6) 台账可回读，处置没有被删除
    ledger = client.get("/api/dispatch/ledger",
                        params={"ref_type": "order", "ref_id": oid}).json()["ledger"]
    events = {row["event_type"] for row in ledger}
    assert "WITHDRAW_CONFIRMED" in events and "ORDER_CREATED" in events


def test_transport_interrupt_recover_no_duplicate_dispatch(client):
    # 生成预警并派单
    opened = client.post("/api/dispatch/warnings/evaluate",
                         json={"records": _rain(station="S7")}).json()
    warning_id = opened["warnings_opened"][0]
    created = client.post(f"/api/dispatch/warnings/{warning_id}/dispatch").json()
    oid = created["order_id"]

    # 模拟「发送中断后」的真实状态：本地回退 PENDING，对端无此单（响应未知、未达）
    ctx = client.app.state.dispatch_ctx
    ctx.backend.reset()
    with ctx.store.connect() as conn:
        ctx.store.update_order(conn, oid, state="PENDING", dispatch_seq=None)
    assert ctx.client.get_order(oid) is None

    recover = client.post("/api/dispatch/recover").json()
    actions = {item["order_id"]: item["action"] for item in recover["recovered"]}
    assert actions[oid] == "REDISPATCHED"
    # 对端只有一张同 ID 订单 —— 没有重复派单
    remote = client.get("/api/station-sim/orders").json()["orders"]
    assert [r["order_id"] for r in remote].count(oid) == 1
    assert client.get(f"/api/dispatch/orders/{oid}").json()["state"] == "DISPATCHED"


def test_dispatch_interrupt_returns_conflict_and_stays_pending(client):
    # 先建一张 PENDING 单，再注入下一次发送中断：接口报 409，状态不倒退
    opened = client.post("/api/dispatch/warnings/evaluate",
                         json={"records": _rain(station="SB")}).json()
    warning_id = opened["warnings_opened"][0]
    ctx = client.app.state.dispatch_ctx
    with ctx.store.connect() as conn:
        warning = ctx.store.get_warning(conn, warning_id)
    oid = _order_id(warning["station"], warning["month"])
    with ctx.store.connect() as conn:
        ctx.store.insert_order(conn, {
            "order_id": oid, "warning_id": warning_id, "station": warning["station"],
            "month": warning["month"], "state": "PENDING",
            "created_at": _now(), "updated_at": _now()})
    client.post("/api/station-sim/transport/fail-next",
                json={"order_id": oid, "kind": "dispatch"})
    # 直接驱动发送（与派单接口同一内部方法）
    with pytest.raises(Conflict):
        ctx.service._send_dispatch(oid)
    assert client.get(f"/api/dispatch/orders/{oid}").json()["state"] == "PENDING"
    # 恢复后正常送达
    client.post("/api/dispatch/recover")
    assert client.get(f"/api/dispatch/orders/{oid}").json()["state"] == "DISPATCHED"


def test_concurrent_withdraw_stale_decision_rejected(client):
    opened = client.post("/api/dispatch/warnings/evaluate",
                         json={"records": _rain(station="S8")}).json()
    warning_id = opened["warnings_opened"][0]
    order = client.post(f"/api/dispatch/warnings/{warning_id}/dispatch").json()
    client.post("/api/station-sim/faults",
                json={"order_id": order["order_id"], "manual_confirm": True})
    accept = client.post(f"/api/station-sim/orders/{order['order_id']}/accept").json()
    client.post(f"/api/dispatch/orders/{order['order_id']}/receipts/accept",
                json={"accept_seq": accept["accept_seq"]})

    first = client.post(f"/api/dispatch/warnings/{warning_id}/withdraw",
                        json={"expected_version": 1})
    assert first.status_code == 200
    stale = client.post(f"/api/dispatch/warnings/{warning_id}/withdraw",
                        json={"expected_version": 1})
    assert stale.status_code == 409
    assert stale.json()["error"] == "E_STALE_DECISION"


def test_reconcile_reports_balanced_then_detects_drift(client):
    opened = client.post("/api/dispatch/warnings/evaluate",
                         json={"records": _rain(station="S9")}).json()
    warning_id = opened["warnings_opened"][0]
    order = client.post(f"/api/dispatch/warnings/{warning_id}/dispatch").json()
    # 正常情况下两账平衡
    assert client.post("/api/dispatch/reconcile").json()["balanced"] is True

    # 制造漂移：本地回退 PENDING，对端已接单
    ctx = client.app.state.dispatch_ctx
    ctx.backend.accept(order["order_id"])
    with ctx.store.connect() as conn:
        ctx.store.update_order(conn, order["order_id"], state="PENDING",
                               dispatch_seq=None, accept_seq=None)
    report = client.post("/api/dispatch/reconcile").json()
    assert report["balanced"] is False
    client.post("/api/dispatch/reconcile", json={"repair": True})
    assert client.post("/api/dispatch/reconcile").json()["balanced"] is True


def test_unknown_entities_return_404(client):
    assert client.get("/api/dispatch/warnings/WARN-x-1999-01").status_code == 404
    assert client.get("/api/dispatch/orders/ORD-nope").status_code == 404


def test_threshold_config_takes_effect(client, settings):
    # 通过自定义设置验证按站阈值（settings fixture 的阈值默认 300）
    assert isinstance(settings.rain_warn_threshold_mm, Decimal)
    under = client.post("/api/dispatch/warnings/evaluate",
                        json={"records": _rain(station="SA", per_day="50.0")}).json()
    assert under["warnings_opened"] == []
    assert under["below_threshold"]
