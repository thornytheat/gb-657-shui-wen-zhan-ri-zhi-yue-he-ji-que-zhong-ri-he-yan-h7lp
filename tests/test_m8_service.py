"""M8 派单服务：派单、撤警、迟到回执、发送中断恢复、并发撤警、对账。

不经过 HTTP，直接组装 DispatchService + 真实 SQLite + 内存模拟值班站，
时间断言只看状态与台账，不依赖当前时钟。
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.errors import Conflict, NotFound
from app.modules.m8_dispatch import constants as C
from app.modules.m8_dispatch.context import DispatchContext
from app.modules.m8_dispatch.service import DispatchService
from app.modules.m8_dispatch.station_client import TransportError
from app import config


def _rain(station="S1", month="2026-06", days=3, per_day="120.0", version=1):
    return [{"station": station, "date": f"{month}-{d:02d}",
             "rainfall_mm": per_day, "version": version} for d in range(1, days + 1)]


@pytest.fixture()
def service(tmp_path) -> DispatchService:
    settings = config.Settings(
        db_path=str(tmp_path / "main.sqlite"),
        dispatch_db_path=str(tmp_path / "dispatch.sqlite"),
        rain_warn_threshold_mm=Decimal("300"))
    context = DispatchContext(settings)
    return context.service


def _open_warning(service, **kw) -> str:
    result = service.ingest_and_evaluate(_rain(**kw))
    assert result["warnings_opened"]
    return result["warnings_opened"][0]


# ---------------- 预警生成与派单 ----------------
def test_warning_generated_from_actual_daily_and_dispatch(service):
    warning_id = _open_warning(service)
    view = service.get_warning_view(warning_id)
    assert view["state"] == C.ACTIVE
    assert view["rainfall_mm"] == "360.0"  # 3 * 120
    order = service.dispatch_warning(warning_id)
    assert order["state"] == C.DISPATCHED
    # 重复派单返回同一张，不产生第二张单
    again = service.dispatch_warning(warning_id)
    assert again["order_id"] == order["order_id"]
    assert again.get("duplicate") is True
    assert len(service.list_order_views()) == 1


def test_no_dispatch_after_correction_below_threshold(service):
    warning_id = _open_warning(service)
    # 更正：同一日期高版本把雨量拉到警戒线以下
    corrected = [{"station": "S1", "date": "2026-06-01",
                  "rainfall_mm": "10.0", "version": 2}]
    result = service.ingest_and_evaluate(corrected)
    assert result["below_threshold"]
    view = service.get_warning_view(warning_id)
    assert view["state"] in (C.WITHDRAWING, C.WITHDRAWN)  # 自动撤警，未删警
    with pytest.raises(Conflict):
        service.dispatch_warning(warning_id)
    # 预警行与台账仍在，没有物理删除
    assert service.ledger("warning", warning_id)


def test_idempotency_key_replays_without_new_orders(service):
    key = "batch-001"
    first = service.ingest_and_evaluate(_rain(), idem_key=key)
    second = service.ingest_and_evaluate(_rain(), idem_key=key)
    assert second.get("idempotent_replay") is True
    assert second["warnings_opened"] == first["warnings_opened"]
    assert len(service.list_warning_views()) == 1


# ---------------- 撤警：未接取消 / 已接撤回 ----------------
def test_withdraw_cancels_pending_and_withdrawals_accepted(service):
    warning_id = _open_warning(service)
    pending_warning = _open_warning(service, station="S2")
    order1 = service.dispatch_warning(warning_id)

    # order2：派单发送中断，对端未收，本地保持 PENDING（真正的「未接单项」）
    from app.modules.m8_dispatch.service import _order_id, _now
    with service.store.connect() as conn:
        pw = service.store.get_warning(conn, pending_warning)
    oid2 = _order_id(pw["station"], pw["month"])
    with service.store.connect() as conn:
        service.store.insert_order(conn, {
            "order_id": oid2, "warning_id": pending_warning,
            "station": pw["station"], "month": pw["month"], "state": C.PENDING,
            "created_at": _now(), "updated_at": _now()})
        service.store.append_ledger(conn, C.EV_ORDER_CREATED, "order", oid2,
                                    {"warning_id": pending_warning}, _now())
    assert service.get_order_view(oid2)["state"] == C.PENDING

    # order1 模拟值班站接单
    snap = service.client.backend.accept(order1["order_id"])
    service.record_accept(order1["order_id"], snap["accept_seq"])

    result = service.withdraw_warning(warning_id)
    result2 = service.withdraw_warning(pending_warning)

    closed_pending = service.get_order_view(oid2)
    assert closed_pending["state"] == C.CLOSED
    assert closed_pending["close_reason"] == C.CLOSE_CANCELLED
    assert result2["cancelled_orders"] == [oid2]

    withdrawing = service.get_order_view(order1["order_id"])
    assert withdrawing["state"] == C.CLOSED  # 模拟站默认即时确认撤回
    assert withdrawing["close_reason"] == C.CLOSE_WITHDRAWN
    assert result["withdrawing_orders"] == [order1["order_id"]]


def test_withdraw_confirm_required_before_close(service):
    warning_id = _open_warning(service)
    order = service.dispatch_warning(warning_id)
    service.client.backend.manual_confirm.add(order["order_id"])
    snap = service.client.backend.accept(order["order_id"])
    service.record_accept(order["order_id"], snap["accept_seq"])
    service.withdraw_warning(warning_id)
    view = service.get_order_view(order["order_id"])
    assert view["state"] == C.WITHDRAWING  # 已发撤回，等确认，不结案
    confirmed = service.client.backend.confirm_withdraw(order["order_id"])
    service.record_withdraw_confirm(order["order_id"],
                                    confirmed["withdraw_confirm_seq"])
    remote = service.client.get_order(order["order_id"])
    view = service.get_order_view(order["order_id"])
    assert view["state"] == C.CLOSED and view["close_reason"] == C.CLOSE_WITHDRAWN
    assert service.get_warning_view(warning_id)["state"] == C.WITHDRAWN
    assert remote["withdraw_confirm_seq"] is not None


# ---------------- 撤警后迟到的接单 ----------------
def test_late_accept_after_withdraw_is_booked_and_withdraw_resent(service):
    warning_id = _open_warning(service)
    # 造一张本地 PENDING（对端未达）的单，撤警时按「未接」取消
    from app.modules.m8_dispatch.service import _order_id, _now
    with service.store.connect() as conn:
        warning = service.store.get_warning(conn, warning_id)
    oid = _order_id(warning["station"], warning["month"])
    with service.store.connect() as conn:
        service.store.insert_order(conn, {
            "order_id": oid, "warning_id": warning_id, "station": warning["station"],
            "month": warning["month"], "state": C.PENDING,
            "created_at": _now(), "updated_at": _now()})
        service.store.append_ledger(conn, C.EV_ORDER_CREATED, "order", oid,
                                    {"warning_id": warning_id}, _now())
    # 先撤警：本地认定未接单 → 直接 CANCELLED
    service.withdraw_warning(warning_id)
    assert service.get_order_view(oid)["state"] == C.CLOSED
    assert service.get_order_view(oid)["close_reason"] == C.CLOSE_CANCELLED
    # 值班站其实已接单（对端在撤警前就受理，回执迟到）；
    # 用手动确认模式，便于观察「入账→待撤→确认结案」的中间态
    service.client.backend.manual_confirm.add(oid)
    service.client.backend.receive_dispatch(oid, {"order_id": oid})
    snap = service.client.backend.accept(oid)
    assert snap["accept_seq"] is not None
    view = service.record_accept(oid, snap["accept_seq"])
    assert view["state"] == C.WITHDRAWING          # 入账并复活，补撤回（等待确认）
    assert view["accept_seq"] == snap["accept_seq"]
    late_events = [e for e in service.ledger("order", oid)
                   if e["event_type"] == C.EV_LATE_ACCEPT]
    assert late_events
    # 撤回确认后才结案，预警随之结案
    confirmed = service.client.backend.confirm_withdraw(oid)
    service.record_withdraw_confirm(oid, confirmed["withdraw_confirm_seq"])
    final = service.get_order_view(oid)
    assert final["state"] == C.CLOSED
    assert final["close_reason"] == C.CLOSE_WITHDRAWN
    assert service.get_warning_view(warning_id)["state"] == C.WITHDRAWN


def test_duplicate_and_out_of_order_receipts_are_stale(service):
    warning_id = _open_warning(service)
    order = service.dispatch_warning(warning_id)
    snap = service.client.backend.accept(order["order_id"])
    service.record_accept(order["order_id"], snap["accept_seq"])
    # 同序号重复
    dup = service.record_accept(order["order_id"], snap["accept_seq"])
    assert dup.get("stale") is True
    # 更小序号乱序
    stale = service.record_accept(order["order_id"], snap["accept_seq"] - 1)
    assert stale.get("stale") is True
    assert service.get_order_view(order["order_id"])["state"] == C.DISPATCHED


# ---------------- 发送中断恢复：先查后试，不重复派单 ----------------
def test_dispatch_interrupt_recover_queries_then_retries(service):
    warning_id = _open_warning(service)
    # 构造一张 PENDING 单（模拟派单已落库但尚未送达）
    from app.modules.m8_dispatch.service import _order_id, _now
    with service.store.connect() as conn:
        warning = service.store.get_warning(conn, warning_id)
    oid = _order_id(warning["station"], warning["month"])
    with service.store.connect() as conn:
        service.store.insert_order(conn, {
            "order_id": oid, "warning_id": warning_id, "station": warning["station"],
            "month": warning["month"], "state": C.PENDING,
            "created_at": _now(), "updated_at": _now()})
        service.store.append_ledger(conn, C.EV_ORDER_CREATED, "order", oid,
                                    {"warning_id": warning_id}, _now())

    service.client.fail_next(oid, "dispatch", times=1)
    with pytest.raises(Conflict) as exc:
        service._send_dispatch(oid)
    assert exc.value.code == C.E_STATION_UNREACHABLE
    assert service.get_order_view(oid)["state"] == C.PENDING
    assert service.client.get_order(oid) is None  # 对端确实没收到

    result = service.recover()
    actions = {item["order_id"]: item["action"] for item in result["recovered"]}
    assert actions[oid] == "REDISPATCHED"
    # 对端只有一张同 ID 订单，没有重复派单
    assert len(service.client.backend.list_orders()) == 1
    assert service.get_order_view(oid)["state"] == C.DISPATCHED


def test_recover_when_remote_got_it_does_not_redispatch(service):
    warning_id = _open_warning(service)
    order = service.dispatch_warning(warning_id)
    oid = order["order_id"]
    # 本地强行回退到 PENDING，模拟「响应丢失但对端已收」
    with service.store.connect() as conn:
        service.store.update_order(conn, oid, state=C.PENDING, dispatch_seq=None)
    # 对端已存在（甚至已接单）
    snap = service.client.backend.accept(oid)
    result = service.recover()
    actions = {item["order_id"]: item["action"] for item in result["recovered"]}
    assert actions[oid].startswith("ALIGNED")
    assert service.get_order_view(oid)["accept_seq"] == snap["accept_seq"]
    assert len(service.client.backend.list_orders()) == 1  # 没有重派


def test_withdraw_interrupt_recover_queries_and_resends(service):
    warning_id = _open_warning(service)
    order = service.dispatch_warning(warning_id)
    oid = order["order_id"]
    service.client.backend.manual_confirm.add(oid)
    snap = service.client.backend.accept(oid)
    service.record_accept(oid, snap["accept_seq"])
    # 撤回请求发送中断
    service.client.fail_next(oid, "withdraw", times=1)
    service.withdraw_warning(warning_id)
    view = service.get_order_view(oid)
    assert view["state"] == C.DISPATCHED  # 中断未改状态
    assert view["last_error"]
    # 恢复：对端没有撤回请求 → 重发；随后确认
    result = service.recover()
    assert any(i["order_id"] == oid and i["action"] == "WITHDRAW_RESENT"
               for i in result["recovered"])
    confirmed = service.client.backend.confirm_withdraw(oid)
    service.record_withdraw_confirm(oid, confirmed["withdraw_confirm_seq"])
    assert service.get_order_view(oid)["state"] == C.CLOSED


# ---------------- 并发撤警：乐观锁拒绝过期决定 ----------------
def test_concurrent_withdraw_rejects_stale_decision(service):
    warning_id = _open_warning(service)
    order = service.dispatch_warning(warning_id)
    # 手动确认，让预警停在 WITHDRAWING（版本已推进但未结案）
    service.client.backend.manual_confirm.add(order["order_id"])
    snap = service.client.backend.accept(order["order_id"])
    service.record_accept(order["order_id"], snap["accept_seq"])
    before = service.get_warning_view(warning_id)
    assert before["version"] == 1
    # 第一次撤警成功，版本推进
    service.withdraw_warning(warning_id, expected_version=1)
    assert service.get_warning_view(warning_id)["version"] == 2
    # 携带过期版本的并发决定必须被拒绝
    with pytest.raises(Conflict) as exc:
        service.withdraw_warning(warning_id, expected_version=1)
    assert exc.value.code == C.E_STALE_DECISION
    stale = [e for e in service.ledger("warning", warning_id)
             if e["event_type"] == C.EV_STALE_RECEIPT]
    assert stale
    # 当前版本重放是幂等的，不报错也不重复处置
    replay = service.withdraw_warning(warning_id, expected_version=2)
    assert replay.get("idempotent_replay") is True


# ---------------- 对账 ----------------
def test_reconcile_detects_and_repairs_drift(service):
    warning_id = _open_warning(service)
    order = service.dispatch_warning(warning_id)
    oid = order["order_id"]
    # 制造漂移：本地 PENDING，对端已接单
    with service.store.connect() as conn:
        service.store.update_order(conn, oid, state=C.PENDING, dispatch_seq=None,
                                   accept_seq=None)
    service.client.backend.accept(oid)
    report = service.reconcile()
    assert report["balanced"] is False
    assert any("REMOTE_ACCEPTED_LOCAL_PENDING" in d["type"] for d in report["diffs"])
    service.reconcile(repair=True)
    assert service.reconcile()["balanced"] is True
    assert service.get_order_view(oid)["state"] == C.DISPATCHED


def test_reconcile_reopens_cancelled_order_when_remote_open(service):
    from app.modules.m8_dispatch.service import _order_id, _now
    warning_id = _open_warning(service)
    with service.store.connect() as conn:
        warning = service.store.get_warning(conn, warning_id)
    oid = _order_id(warning["station"], warning["month"])
    with service.store.connect() as conn:
        service.store.insert_order(conn, {
            "order_id": oid, "warning_id": warning_id, "station": warning["station"],
            "month": warning["month"], "state": C.PENDING,
            "created_at": _now(), "updated_at": _now()})
        service.store.append_ledger(conn, C.EV_ORDER_CREATED, "order", oid,
                                    {"warning_id": warning_id}, _now())
    service.withdraw_warning(warning_id)  # 本地按未接 CANCELLED
    # 对端其实受理并接了单（证据迟到，先在对账里暴露）
    service.client.backend.receive_dispatch(oid, {"order_id": oid})
    service.client.backend.accept(oid)
    report = service.reconcile(repair=True)
    # 修复当次报告基于修复前快照；修复后再次对账必须无差异
    assert service.reconcile()["balanced"] is True
    assert report["diffs"]  # 当次确实识别到了漂移
    view = service.get_order_view(oid)
    assert view["state"] in (C.WITHDRAWING, C.CLOSED)  # 已复活并补撤回
    assert any(e["event_type"] == C.EV_RECONCILE_REPAIR
               for e in service.ledger("order", oid))


# ---------------- 回读五态 ----------------
def test_readback_includes_basis_pending_dispatched_withdrawing_closed(service):
    from app.modules.m8_dispatch.service import _order_id, _now
    w1 = _open_warning(service, station="S1")
    w2 = _open_warning(service, station="S2")
    o1 = service.dispatch_warning(w1)

    # o2 造一张真正未送达的 PENDING 单
    with service.store.connect() as conn:
        pw2 = service.store.get_warning(conn, w2)
    oid2 = _order_id(pw2["station"], pw2["month"])
    with service.store.connect() as conn:
        service.store.insert_order(conn, {
            "order_id": oid2, "warning_id": w2, "station": pw2["station"],
            "month": pw2["month"], "state": C.PENDING,
            "created_at": _now(), "updated_at": _now()})
        service.store.append_ledger(conn, C.EV_ORDER_CREATED, "order", oid2,
                                    {"warning_id": w2}, _now())

    service.client.backend.manual_confirm.add(o1["order_id"])
    snap = service.client.backend.accept(o1["order_id"])
    service.record_accept(o1["order_id"], snap["accept_seq"])
    service.withdraw_warning(w1)  # o1 → WITHDRAWING
    service.withdraw_warning(w2)  # o2 PENDING → CANCELLED/CLOSED

    basis = service.monthly_basis()
    assert {b["station"] for b in basis} == {"S1", "S2"}
    assert service.list_order_views(C.PENDING) == []
    states = {o["order_id"]: o["state"] for o in service.list_order_views()}
    assert states[o1["order_id"]] == C.WITHDRAWING
    assert states[oid2] == C.CLOSED
    view = service.get_warning_view(w1)
    assert view["counts"]["withdrawing"] == 1
    assert service.ledger()  # 全量台账可回读


def test_unknown_entities_raise_not_found(service):
    with pytest.raises(NotFound):
        service.get_warning_view("WARN-nope-nope")
    with pytest.raises(NotFound):
        service.get_order_view("ORD-nope")
