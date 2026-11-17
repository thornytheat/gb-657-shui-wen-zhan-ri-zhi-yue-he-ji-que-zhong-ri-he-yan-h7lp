"""M8 HTTP 入口：预警核算、派单、撤警、值班站回执、恢复、对账与回读。

所有接口只对接本地模拟值班站（app.state.dispatch_ctx.backend），
没有任何真实通知通道的出口。
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ...errors import BadRequest
from . import constants as C
from .context import DispatchContext, get_context

router = APIRouter(prefix="/api/dispatch", tags=["M8 预警派单"])
station_router = APIRouter(prefix="/api/station-sim", tags=["M8 模拟值班站"])


def ctx(request: Request) -> DispatchContext:
    return get_context(request)


# ---------------- 预警生成与月量依据 ----------------
@router.post("/warnings/evaluate")
def evaluate_warnings(request: Request, payload: dict | None = None):
    """导入实际日值并核算预警；auto_dispatch=true 时对新预警立即派单。"""
    payload = payload or {}
    records = payload.get("records") or []
    idem_key = payload.get("idempotency_key")
    return ctx(request).service.ingest_and_evaluate(
        records, auto_dispatch=bool(payload.get("auto_dispatch")),
        idem_key=idem_key)


@router.get("/basis")
def read_basis(request: Request, station: str | None = None,
               month: str | None = None):
    """回读月量依据：实际日值 → 既有采信规则 → 月量与警戒线判定。"""
    return {"basis": ctx(request).service.monthly_basis(station, month)}


@router.get("/warnings")
def list_warnings(request: Request, state: str | None = None):
    return {"warnings": ctx(request).service.list_warning_views(state)}


@router.get("/warnings/{warning_id}")
def get_warning(request: Request, warning_id: str):
    return ctx(request).service.get_warning_view(warning_id)


# ---------------- 派单 / 撤警 ----------------
@router.post("/warnings/{warning_id}/dispatch")
def dispatch_warning(request: Request, warning_id: str, payload: dict | None = None):
    idem_key = (payload or {}).get("idempotency_key")
    return ctx(request).service.dispatch_warning(warning_id, idem_key=idem_key)


@router.post("/warnings/{warning_id}/withdraw")
def withdraw_warning(request: Request, warning_id: str, payload: dict | None = None):
    payload = payload or {}
    expected_version = payload.get("expected_version")
    return ctx(request).service.withdraw_warning(
        warning_id,
        expected_version=None if expected_version is None else int(expected_version),
        reason=str(payload.get("reason") or "MANUAL_WITHDRAW"),
        idem_key=payload.get("idempotency_key"))


# ---------------- 派单回读 ----------------
@router.get("/orders")
def list_orders(request: Request, state: str | None = None,
                warning_id: str | None = None):
    service = ctx(request).service
    if warning_id:
        return {"orders": [o for o in service.list_order_views(state)
                           if o["warning_id"] == warning_id]}
    return {"orders": service.list_order_views(state)}


@router.get("/orders/{order_id}")
def get_order(request: Request, order_id: str):
    return ctx(request).service.get_order_view(order_id)


# ---------------- 值班站回执（模拟站回调主系统） ----------------
@router.post("/orders/{order_id}/receipts/accept")
def accept_receipt(request: Request, order_id: str, payload: dict | None = None):
    seq = (payload or {}).get("accept_seq")
    if seq is None:
        raise BadRequest("缺少 accept_seq", code=C.E_BAD_RECORD)
    return ctx(request).service.record_accept(order_id, int(seq))


@router.post("/orders/{order_id}/receipts/withdraw-confirm")
def withdraw_confirm_receipt(request: Request, order_id: str,
                             payload: dict | None = None):
    seq = (payload or {}).get("confirm_seq")
    if seq is None:
        raise BadRequest("缺少 confirm_seq", code=C.E_BAD_RECORD)
    return ctx(request).service.record_withdraw_confirm(order_id, int(seq))


# ---------------- 中断恢复 / 对账 ----------------
@router.post("/recover")
def recover(request: Request):
    """发送中断后恢复：超时先查值班站再重试，不重复派单。"""
    return ctx(request).service.recover()


@router.post("/reconcile")
def reconcile(request: Request, payload: dict | None = None):
    repair = bool((payload or {}).get("repair"))
    return ctx(request).service.reconcile(repair=repair)


@router.get("/ledger")
def read_ledger(request: Request, ref_type: str | None = None,
                ref_id: str | None = None):
    return {"ledger": ctx(request).service.ledger(ref_type, ref_id)}


# ---------------- 模拟值班站操作与故障注入（仅本地） ----------------
@station_router.post("/orders/{order_id}/accept")
def sim_accept(request: Request, order_id: str):
    return ctx(request).backend.accept(order_id)


@station_router.post("/orders/{order_id}/withdraw-confirm")
def sim_confirm(request: Request, order_id: str):
    return ctx(request).backend.confirm_withdraw(order_id)


@station_router.get("/orders")
def sim_orders(request: Request):
    return {"orders": ctx(request).backend.list_orders()}


@station_router.post("/faults")
def sim_fault(request: Request, payload: dict | None = None):
    """故障注入：dispatch_fail/withdraw_fail 次数、auto_accept、manual_confirm。"""
    payload = payload or {}
    return ctx(request).backend.configure(
        order_id=payload.get("order_id"),
        dispatch_fail=int(payload.get("dispatch_fail", 0)),
        withdraw_fail=int(payload.get("withdraw_fail", 0)),
        auto_accept=bool(payload.get("auto_accept", False)),
        manual_confirm=bool(payload.get("manual_confirm", False)))


@station_router.post("/transport/fail-next")
def sim_transport_fail(request: Request, payload: dict | None = None):
    """模拟发送中断（响应未知）：kind=dispatch|withdraw，下 N 次调用前抛中断。"""
    payload = payload or {}
    order_id = payload.get("order_id")
    kind = payload.get("kind", "dispatch")
    if not order_id or kind not in ("dispatch", "withdraw"):
        raise BadRequest("需要 order_id 与 kind(dispatch|withdraw)",
                         code=C.E_BAD_RECORD)
    ctx(request).client.fail_next(order_id, kind, int(payload.get("times", 1)))
    return {"order_id": order_id, "kind": kind,
            "times": int(payload.get("times", 1))}


@station_router.post("/reset")
def sim_reset(request: Request):
    context = ctx(request)
    context.backend.reset()
    context.client.reset_failures()
    return {"reset": True}
