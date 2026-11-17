"""本地模拟值班站：派单/接单/撤回的对端，纯内存实现。

不接任何真实通知渠道（无短信/无外网 webhook），只在本进程内提供 HTTP 接口。
职责：
- 接收派单并生成站内序号，记录派单内容（撤警不能让对端丢失处置上下文）；
- 接单（accept）与撤回确认（withdraw confirm）各自产生单调递增回执序号；
- 故障注入：dispatch/withdraw 可配置前 N 次失败，用于验证超时重试与恢复；
- 自动接单开关：模拟「撤警后迟到的接单回执」场景。

回执序号语义：accept_seq / withdraw_seq 对每个订单各自单调递增，
主系统用 (order_id, kind, seq) 去重，乱序到达也不会重复入账或状态倒退。
"""
from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass, field

from ...errors import BadRequest, Conflict, NotFound

# 站内订单状态
ST_RECEIVED = "RECEIVED"          # 已派达，未接单
ST_ACCEPTED = "ACCEPTED"          # 值班站已接单
ST_WITHDRAW_SENT = "WITHDRAWING"  # 收到撤回请求，待确认
ST_WITHDRAWN = "WITHDRAWN"        # 撤回确认结案


@dataclass
class StationOrder:
    order_id: str
    payload: dict
    state: str = ST_RECEIVED
    accept_seq: int | None = None
    withdraw_seq: int | None = None
    withdraw_confirm_seq: int | None = None
    history: list[dict] = field(default_factory=list)


class StationBackend:
    """线程安全的内存值班站；测试可直接操作，HTTP 层只做参数适配。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._orders: dict[str, StationOrder] = {}
        self._global_seq = itertools.count(1)
        # 故障注入：order_id -> {"dispatch_fail": n, "withdraw_fail": n}
        self.faults: dict[str, dict[str, int]] = {}
        # 自动接单：派单成功后立即接单，模拟回执先于撤警决定到达/迟到
        self.auto_accept: set[str] = set()
        # 撤回是否需要显式确认（默认收到撤回请求即确认）
        self.manual_confirm: set[str] = set()

    def _next(self) -> int:
        return next(self._global_seq)

    def reset(self) -> None:
        with self._lock:
            self._orders.clear()
            self.faults.clear()
            self.auto_accept.clear()
            self.manual_confirm.clear()

    # ---------- 故障注入（仅本地模拟用） ----------
    def configure(self, *, order_id: str | None = None,
                  dispatch_fail: int = 0, withdraw_fail: int = 0,
                  auto_accept: bool = False, manual_confirm: bool = False) -> dict:
        with self._lock:
            if order_id:
                self.faults[order_id] = {"dispatch_fail": int(dispatch_fail),
                                         "withdraw_fail": int(withdraw_fail)}
                if auto_accept:
                    self.auto_accept.add(order_id)
                else:
                    self.auto_accept.discard(order_id)
                if manual_confirm:
                    self.manual_confirm.add(order_id)
                else:
                    self.manual_confirm.discard(order_id)
                return {"order_id": order_id, **self.faults[order_id],
                        "auto_accept": auto_accept, "manual_confirm": manual_confirm}
            return {"orders": len(self._orders)}

    def _consume_fault(self, order_id: str, kind: str) -> bool:
        slot = self.faults.get(order_id)
        if not slot or slot.get(kind, 0) <= 0:
            return False
        slot[kind] -= 1
        return True

    # ---------- 派单 ----------
    def receive_dispatch(self, order_id: str, payload: dict) -> dict:
        with self._lock:
            # 幂等：重复派单不新建、不产生新序号，返回当前快照
            existing = self._orders.get(order_id)
            if existing is not None:
                return self._snapshot(existing, duplicate=True)
            if self._consume_fault(order_id, "dispatch_fail"):
                raise BadRequest(f"模拟故障：值班站暂不受理派单 {order_id}",
                                 code="E_STATION_SIMULATED_FAULT")
            order = StationOrder(order_id=order_id, payload=dict(payload))
            order.history.append({"event": "DISPATCH_RECEIVED", "seq": self._next()})
            self._orders[order_id] = order
            if order_id in self.auto_accept:
                self._accept_locked(order)
            return self._snapshot(order)

    # ---------- 接单 ----------
    def _accept_locked(self, order: StationOrder) -> dict:
        if order.state == ST_ACCEPTED:
            return self._snapshot(order, duplicate=True)
        if order.state in (ST_WITHDRAW_SENT, ST_WITHDRAWN):
            # 已进入撤回流程后到达的接单：站内仍如实登记（不丢处置），
            # 分配接单序号，但不覆盖撤回状态。
            order.accept_seq = self._next()
            order.history.append({"event": "ACCEPT_LATE", "seq": order.accept_seq})
            return self._snapshot(order, late=True)
        order.state = ST_ACCEPTED
        order.accept_seq = self._next()
        order.history.append({"event": "ACCEPTED", "seq": order.accept_seq})
        return self._snapshot(order)

    def accept(self, order_id: str) -> dict:
        with self._lock:
            order = self._require(order_id)
            return self._accept_locked(order)

    # ---------- 撤回 ----------
    def request_withdraw(self, order_id: str, reason: str = "") -> dict:
        with self._lock:
            order = self._require(order_id)
            if order.state == ST_WITHDRAWN:
                return self._snapshot(order, duplicate=True)
            if self._consume_fault(order_id, "withdraw_fail"):
                raise BadRequest(f"模拟故障：值班站撤回通道暂不可用 {order_id}",
                                 code="E_STATION_SIMULATED_FAULT")
            order.withdraw_seq = self._next()
            order.state = ST_WITHDRAW_SENT
            order.history.append({"event": "WITHDRAW_REQUESTED", "seq": order.withdraw_seq,
                                  "reason": reason})
            if order_id not in self.manual_confirm:
                self._confirm_locked(order)
            return self._snapshot(order)

    def _confirm_locked(self, order: StationOrder) -> None:
        order.state = ST_WITHDRAWN
        order.withdraw_confirm_seq = self._next()
        order.history.append({"event": "WITHDRAW_CONFIRMED",
                              "seq": order.withdraw_confirm_seq})

    def confirm_withdraw(self, order_id: str) -> dict:
        with self._lock:
            order = self._require(order_id)
            if order.state == ST_WITHDRAWN:
                return self._snapshot(order, duplicate=True)
            if order.state != ST_WITHDRAW_SENT:
                raise Conflict(f"订单 {order_id} 不在待确认撤回状态（当前 {order.state}）",
                               code="E_STATION_BAD_STATE")
            self._confirm_locked(order)
            return self._snapshot(order)

    # ---------- 查询/对账 ----------
    def get(self, order_id: str) -> dict:
        with self._lock:
            return self._snapshot(self._require(order_id))

    def list_orders(self) -> list[dict]:
        with self._lock:
            return [self._snapshot(order) for order in self._orders.values()]

    def _require(self, order_id: str) -> StationOrder:
        order = self._orders.get(order_id)
        if order is None:
            raise NotFound(f"值班站无此订单 {order_id}", code="E_STATION_ORDER_NOT_FOUND")
        return order

    def _snapshot(self, order: StationOrder, *, duplicate: bool = False,
                  late: bool = False) -> dict:
        return {
            "order_id": order.order_id,
            "state": order.state,
            "accept_seq": order.accept_seq,
            "withdraw_seq": order.withdraw_seq,
            "withdraw_confirm_seq": order.withdraw_confirm_seq,
            "duplicate": duplicate,
            "late": late,
            "history": list(order.history),
        }
