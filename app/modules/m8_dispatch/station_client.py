"""值班站客户端：把「发送中断/超时」收敛成可恢复的传输异常。

对接的是本地模拟值班站（station.StationBackend），没有任何真实网络通知。
设计要点：
- 所有写操作带幂等键（order_id 天然幂等），对端重复收到也只返回同一快照；
- 发送超时/中断抛 TransportError，调用方不得假定成功或失败，
  必须先 get_order 查询对端真实状态，再决定重试还是补账，杜绝重复派单；
- fail_next 是测试钩子，模拟进程在收到对端响应前崩溃/断连。
"""
from __future__ import annotations

import threading

from .station import StationBackend


class TransportError(RuntimeError):
    """发送中断：请求可能已到达对端，也可能没有，恢复时必须先查后试。"""

    def __init__(self, message: str, *, kind: str, order_id: str):
        super().__init__(message)
        self.kind = kind
        self.order_id = order_id


class StationClient:
    def __init__(self, backend: StationBackend):
        self.backend = backend
        self._lock = threading.RLock()
        # 测试钩子：order_id -> {"dispatch": n, "withdraw": n}，在调用对端前抛中断
        self._fail_next: dict[str, dict[str, int]] = {}

    def reset_failures(self) -> None:
        with self._lock:
            self._fail_next.clear()

    def fail_next(self, order_id: str, kind: str, times: int = 1) -> None:
        with self._lock:
            self._fail_next.setdefault(order_id, {})[kind] = (
                self._fail_next.get(order_id, {}).get(kind, 0) + times)

    def _consume_interrupt(self, order_id: str, kind: str) -> None:
        slot = self._fail_next.get(order_id)
        if slot and slot.get(kind, 0) > 0:
            slot[kind] -= 1
            raise TransportError(
                f"发送中断（{kind}）：响应未知，恢复须先查询 {order_id}",
                kind=kind, order_id=order_id)

    # ---------- 写 ----------
    def dispatch(self, order_id: str, payload: dict) -> dict:
        self._consume_interrupt(order_id, "dispatch")
        return self.backend.receive_dispatch(order_id, payload)

    def withdraw(self, order_id: str, reason: str = "") -> dict:
        self._consume_interrupt(order_id, "withdraw")
        return self.backend.request_withdraw(order_id, reason)

    def confirm_withdraw(self, order_id: str) -> dict:
        return self.backend.confirm_withdraw(order_id)

    # ---------- 读（恢复/对账用，绝不产生副作用） ----------
    def get_order(self, order_id: str) -> dict | None:
        try:
            return self.backend.get(order_id)
        except Exception:  # noqa: BLE001 - 对端不存在视为未达
            return None

    def list_orders(self) -> list[dict]:
        return self.backend.list_orders()
