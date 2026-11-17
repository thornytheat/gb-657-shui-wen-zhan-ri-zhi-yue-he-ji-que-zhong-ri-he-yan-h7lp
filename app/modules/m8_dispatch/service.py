"""M8 派单领域服务：预警生成 → 派单 → 撤警 → 回执 → 恢复 → 对账。

所有状态迁移满足：
1. 一把进程锁 + 一个写事务，读-判-写原子；
2. 每次迁移写 m8_ledger，删警不可能发生（撤警只走状态机，数据行保留）；
3. 对端写操作幂等（order_id 即幂等键），中断后先查后试，不重复派单；
4. 撤警用 warning.version 乐观锁，并发的过期决定得到 409；
5. 回执按站内序号去重且只进不退，乱序/迟到都留痕可回读。
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from ...errors import BadRequest, Conflict, NotFound
from . import accounting, constants as C, statemachine
from .persistence import DispatchStore
from .station import ST_ACCEPTED, ST_WITHDRAW_SENT, ST_WITHDRAWN
from .station_client import StationClient, TransportError


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _warning_id(station: str, month: str) -> str:
    return f"WARN-{station}-{month}"


def _order_id(station: str, month: str) -> str:
    return f"ORD-{station}-{month}-{uuid.uuid4().hex[:10]}"


class DispatchService:
    def __init__(self, store: DispatchStore, client: StationClient,
                 default_threshold: Decimal = C.DEFAULT_THRESHOLD_MM,
                 thresholds: dict[str, Decimal] | None = None):
        self.store = store
        self.client = client
        self.default_threshold = Decimal(str(default_threshold))
        self.thresholds = {k: Decimal(str(v)) for k, v in (thresholds or {}).items()}
        self._lock = threading.RLock()

    # ================= 日值导入与预警生成 =================
    def ingest_and_evaluate(self, records: list[dict], *, auto_dispatch: bool = False,
                            idem_key: str | None = None) -> dict:
        """导入实际日值 → 核算月量 → 生成/刷新预警；不改动月量采信规则。"""
        if idem_key:
            cached = self.store.get_idem(idem_key)
            if cached is not None:
                return {**cached, "idempotent_replay": True}
        result = accounting.evaluate(records, self.thresholds, self.default_threshold)
        stamp = _now()
        opened, refreshed, below_line = [], [], []
        with self._lock, self.store.connect() as conn:
            batch_no = self.store.next_batch_no(conn)
            self.store.replace_daily(conn, result["selection"]["selected"], batch_no, stamp)
            for basis in result["basis"]:
                station, month = basis["station"], basis["month"]
                warning_id = _warning_id(station, month)
                existing = self.store.find_warning(conn, station, month)
                payload = {
                    "warning_id": warning_id, "station": station, "month": month,
                    "rainfall_mm": basis["rainfall_mm"],
                    "threshold_mm": basis["threshold_mm"],
                    "basis": basis, "created_at": stamp, "updated_at": stamp,
                }
                if basis["above_threshold"]:
                    if existing is None:
                        payload["state"] = C.ACTIVE
                        payload["version"] = 1
                        self.store.upsert_warning(conn, payload)
                        self.store.append_ledger(
                            conn, C.EV_WARNING_OPENED, "warning", warning_id,
                            {"rainfall_mm": basis["rainfall_mm"],
                             "threshold_mm": basis["threshold_mm"]}, stamp)
                        opened.append(warning_id)
                    else:
                        # 已存在（含撤警中/已撤警）：月量更正后仍超标，只刷新依据，
                        # 不复活预警、不改状态（状态只进不退）。
                        payload["state"] = existing["state"]
                        payload["version"] = existing["version"]
                        payload["created_at"] = existing["created_at"]
                        self.store.upsert_warning(conn, payload)
                        refreshed.append(warning_id)
                else:
                    below_line.append({"station": station, "month": month,
                                       "warning_id": warning_id,
                                       "rainfall_mm": basis["rainfall_mm"]})
                    if existing is not None and existing["state"] == C.ACTIVE:
                        # 更正降到警戒线以下：先 CAS 进入撤警流程（不删警、不删单）
                        updated = self.store.cas_warning_state(
                            conn, warning_id, existing["version"],
                            C.WITHDRAWING, stamp)
                        if updated is not None:
                            self._withdraw_locked(
                                conn, updated,
                                expected_version=existing["version"],
                                reason="AUTO_BELOW_THRESHOLD", stamp=stamp)
        response = {
            "batch_no": batch_no,
            "selection": result["selection"],
            "basis": result["basis"],
            "warnings_opened": opened,
            "warnings_refreshed": refreshed,
            "below_threshold": below_line,
        }
        if auto_dispatch:
            response["dispatched"] = [
                self.dispatch_warning(warning_id) for warning_id in opened]
        if idem_key:
            with self.store.connect() as conn:
                self.store.save_idem(conn, idem_key, response, _now())
        return response

    # ================= 派单 =================
    def dispatch_warning(self, warning_id: str, *, idem_key: str | None = None) -> dict:
        """对生效预警派单；已派未结案则幂等返回当前单，绝不重复派单。"""
        if idem_key:
            cached = self.store.get_idem(idem_key)
            if cached is not None:
                return {**cached, "idempotent_replay": True}
        stamp = _now()
        with self._lock, self.store.connect() as conn:
            warning = self._require_warning(conn, warning_id)
            if warning["state"] != C.ACTIVE:
                raise Conflict(f"预警 {warning_id} 状态 {warning['state']}，不可新派单",
                               code=C.E_NOT_WITHDRAWABLE)
            existing = [o for o in self.store.list_orders_conn(conn, warning_id)
                        if o["state"] != C.CLOSED]
            if existing:
                order = existing[0]
                self.store.append_ledger(
                    conn, C.EV_DISPATCH_SENT, "order", order["order_id"],
                    {"duplicate": True, "state": order["state"]}, stamp)
                return self._order_view(order, duplicate=True)
            order = {
                "order_id": _order_id(warning["station"], warning["month"]),
                "warning_id": warning_id, "station": warning["station"],
                "month": warning["month"], "state": C.PENDING,
                "created_at": stamp, "updated_at": stamp,
            }
            self.store.insert_order(conn, order)
            self.store.append_ledger(conn, C.EV_ORDER_CREATED, "order",
                                     order["order_id"], {"warning_id": warning_id}, stamp)
        # 事务外发对端（耗时/可能中断）；中断后订单留 PENDING，由 recover 收口
        return self._send_dispatch(order["order_id"], idem_key=idem_key)

    def _send_dispatch(self, order_id: str, *, idem_key: str | None = None) -> dict:
        with self.store.connect() as conn:
            order = self._require_order(conn, order_id)
            payload = self._order_payload(order)
        try:
            snapshot = self.client.dispatch(order_id, payload)
        except TransportError as exc:
            self._record_failure(order_id, C.EV_DISPATCH_FAILED, str(exc))
            raise Conflict(str(exc), code=C.E_STATION_UNREACHABLE) from exc
        stamp = _now()
        with self._lock, self.store.connect() as conn:
            order = self._require_order(conn, order_id)
            fields = {"attempts": order["attempts"] + 1, "updated_at": stamp,
                      "last_error": ""}
            if snapshot.get("accept_seq") is not None:
                # 对端自动接单：派达即已接
                fields["state"] = C.DISPATCHED
                fields["accept_seq"] = snapshot["accept_seq"]
                fields["dispatch_seq"] = self._last_history_seq(snapshot, "DISPATCH_RECEIVED")
            elif order["state"] == C.PENDING:
                fields["state"] = C.DISPATCHED
                fields["dispatch_seq"] = self._last_history_seq(snapshot, "DISPATCH_RECEIVED")
            self.store.update_order(conn, order_id, **fields)
            self.store.append_ledger(conn, C.EV_DISPATCH_SENT, "order", order_id,
                                     {"snapshot": snapshot}, stamp)
            view = self._order_view(self._require_order(conn, order_id))
        if idem_key:
            with self.store.connect() as conn:
                self.store.save_idem(conn, idem_key, view, _now())
        return view

    # ================= 撤警 =================
    def withdraw_warning(self, warning_id: str, *, expected_version: int | None = None,
                         reason: str = "MANUAL_WITHDRAW",
                         idem_key: str | None = None) -> dict:
        """撤警：CAS 拒绝并发过期决定；未接单项取消，已接单项发撤回请求。"""
        if idem_key:
            cached = self.store.get_idem(idem_key)
            if cached is not None:
                return {**cached, "idempotent_replay": True}
        stamp = _now()
        stale_error: Conflict | None = None
        with self._lock, self.store.connect() as conn:
            warning = self._require_warning(conn, warning_id)
            version = warning["version"] if expected_version is None else expected_version
            if warning["state"] in (C.WITHDRAWING, C.WITHDRAWN):
                # 已撤警链路：同版本重放幂等返回当前处置摘要；过期版本仍拒绝
                if version != warning["version"]:
                    self.store.append_ledger(
                        conn, C.EV_STALE_RECEIPT, "warning", warning_id,
                        {"rejected_version": version,
                         "current_version": warning["version"],
                         "current_state": warning["state"]}, stamp)
                    stale_error = Conflict(
                        f"撤警决定已过期：期望版本 {version}，当前版本 {warning['version']}",
                        code=C.E_STALE_DECISION)
                else:
                    orders = self.store.list_orders_conn(conn, warning_id)
                    result = {"warning_id": warning_id, "state": warning["state"],
                              "cancelled_orders": [
                                  o["order_id"] for o in orders
                                  if o.get("close_reason") == C.CLOSE_CANCELLED],
                              "withdrawing_orders": [
                                  o["order_id"] for o in orders
                                  if o["state"] == C.WITHDRAWING],
                              "version": warning["version"],
                              "idempotent_replay": True}
            else:
                updated = self.store.cas_warning_state(
                    conn, warning_id, version, C.WITHDRAWING, stamp)
                if updated is None:
                    # 过期并发决定：留痕并拒绝，不允许覆盖较新的撤警结论
                    self.store.append_ledger(
                        conn, C.EV_STALE_RECEIPT, "warning", warning_id,
                        {"rejected_version": version,
                         "current_version": warning["version"],
                         "current_state": warning["state"]}, stamp)
                    stale_error = Conflict(
                        f"撤警决定已过期：期望版本 {version}，"
                        f"当前版本 {warning['version']}",
                        code=C.E_STALE_DECISION)
                else:
                    result = self._withdraw_locked(
                        conn, updated, expected_version=version,
                        reason=reason, stamp=stamp)
        # 事务已提交，过期留痕不会随异常回滚
        if stale_error is not None:
            raise stale_error
        if idem_key:
            with self.store.connect() as conn:
                self.store.save_idem(conn, idem_key, result, _now())
        return result

    def _withdraw_locked(self, conn, warning: dict, *, expected_version: int,
                         reason: str, stamp: str) -> dict:
        """调用方已持锁且在事务内；返回撤警处置摘要。"""
        warning_id = warning["warning_id"]
        orders = self.store.list_orders_conn(conn, warning_id)
        cancelled, withdrawing, no_orders = [], [], []
        if not orders:
            no_orders.append(warning_id)
        for order in orders:
            oid = order["order_id"]
            if order["state"] == C.CLOSED:
                continue
            if order["state"] == C.PENDING:
                # 未接单项：直接取消，不向值班站发请求
                self.store.update_order(
                    conn, oid, state=C.CLOSED, close_reason=C.CLOSE_CANCELLED,
                    updated_at=stamp)
                self.store.append_ledger(conn, C.EV_ORDER_CANCELLED, "order", oid,
                                         {"reason": reason}, stamp)
                cancelled.append(oid)
                continue
            # DISPATCHED / WITHDRAWING：已接单项，发撤回请求（收到确认才结案）
            self._request_withdraw_in_tx(conn, order, reason, stamp)
            withdrawing.append(oid)
        self.store.append_ledger(
            conn, C.EV_WITHDRAW_DECISION, "warning", warning_id,
            {"version": expected_version, "reason": reason,
             "cancelled": cancelled, "withdrawing": withdrawing}, stamp)
        self._maybe_close_warning(conn, warning_id, stamp)
        return {"warning_id": warning_id, "state": C.WITHDRAWING,
                "cancelled_orders": cancelled, "withdrawing_orders": withdrawing,
                "version": warning["version"] + 1}

    def _request_withdraw_in_tx(self, conn, order: dict, reason: str, stamp: str) -> None:
        oid = order["order_id"]
        try:
            snapshot = self.client.withdraw(oid, reason)
        except TransportError as exc:
            # 撤回请求中断：订单保留原状态并记错，recover 先查后试
            self.store.update_order(conn, oid, last_error=str(exc),
                                    attempts=order["attempts"] + 1, updated_at=stamp)
            self.store.append_ledger(conn, C.EV_WITHDRAW_FAILED, "order", oid,
                                     {"error": str(exc)}, stamp)
            return
        fields = {"updated_at": stamp, "last_error": "",
                  "attempts": order["attempts"] + 1,
                  "withdraw_seq": snapshot.get("withdraw_seq")}
        if order["state"] != C.WITHDRAWING:
            fields["state"] = C.WITHDRAWING
        self.store.update_order(conn, oid, **fields)
        self.store.append_ledger(conn, C.EV_WITHDRAW_SENT, "order", oid,
                                 {"snapshot": snapshot, "reason": reason}, stamp)
        if snapshot.get("withdraw_confirm_seq") is not None:
            self._apply_withdraw_confirm(conn, oid, snapshot, stamp)

    # ================= 值班站回执 =================
    def record_accept(self, order_id: str, accept_seq: int) -> dict:
        """接单回执入账：乱序不倒退；撤警后迟到的接单仍入账并补发撤回。"""
        stamp = _now()
        with self._lock, self.store.connect() as conn:
            order = self._require_order(conn, order_id)
            if self.store.seen_receipt(conn, order_id, "accept", accept_seq):
                self.store.append_ledger(conn, C.EV_STALE_RECEIPT, "order", order_id,
                                         {"kind": "accept", "seq": accept_seq,
                                          "reason": "DUP_SEQ"}, stamp)
                return self._order_view(order, stale=True, reason="DUP_SEQ")
            action, reason = statemachine.decide_accept(order, accept_seq)
            self.store.remember_receipt(conn, order_id, "accept", accept_seq, stamp)
            if action == "stale":
                self.store.append_ledger(conn, C.EV_STALE_RECEIPT, "order", order_id,
                                         {"kind": "accept", "seq": accept_seq,
                                          "reason": reason}, stamp)
                return self._order_view(order, stale=True, reason=reason)
            fields = {"accept_seq": accept_seq, "updated_at": stamp,
                      "close_reason": None}
            if action == "apply_dispatch":
                fields["state"] = C.DISPATCHED
            elif action in ("late_to_withdraw", "late_reopen"):
                # 撤警流程中 / 已按未接取消后迟到的接单：入账并进入待撤，补发撤回
                fields["state"] = C.WITHDRAWING
            self.store.update_order(conn, order_id, **fields)
            self.store.append_ledger(
                conn, C.EV_LATE_ACCEPT, "order", order_id,
                {"accept_seq": accept_seq, "action": action, "reason": reason}, stamp)
            if action in ("late_to_withdraw", "late_reopen"):
                # 撤警后迟到的接单：入账后补发撤回请求，收到确认才结案
                refreshed = self._require_order(conn, order_id)
                if action == "late_reopen":
                    self._reopen_warning_if_needed(conn, order["warning_id"], stamp)
                self._request_withdraw_in_tx(
                    conn, refreshed, "LATE_ACCEPT_WITHDRAW", stamp)
            view = self._order_view(self._require_order(conn, order_id))
            self._maybe_close_warning(conn, order["warning_id"], stamp)
            return view

    def record_withdraw_confirm(self, order_id: str, confirm_seq: int) -> dict:
        """撤回确认入账；只有 WITHDRAWING 上的有效确认才能结案。"""
        stamp = _now()
        with self._lock, self.store.connect() as conn:
            order = self._require_order(conn, order_id)
            if self.store.seen_receipt(conn, order_id, "withdraw_confirm", confirm_seq):
                self.store.append_ledger(conn, C.EV_STALE_RECEIPT, "order", order_id,
                                         {"kind": "withdraw_confirm", "seq": confirm_seq,
                                          "reason": "DUP_SEQ"}, stamp)
                return self._order_view(order, stale=True, reason="DUP_SEQ")
            action, reason = statemachine.decide_withdraw_confirm(order, confirm_seq)
            self.store.remember_receipt(conn, order_id, "withdraw_confirm", confirm_seq,
                                        stamp)
            if action != "close":
                self.store.append_ledger(conn, C.EV_STALE_RECEIPT, "order", order_id,
                                         {"kind": "withdraw_confirm", "seq": confirm_seq,
                                          "reason": reason}, stamp)
                return self._order_view(order, stale=True, reason=reason)
            snapshot = {"withdraw_confirm_seq": confirm_seq}
            self._apply_withdraw_confirm(conn, order_id, snapshot, stamp)
            view = self._order_view(self._require_order(conn, order_id))
            self._maybe_close_warning(conn, order["warning_id"], stamp)
            return view

    def _apply_withdraw_confirm(self, conn, order_id: str, snapshot: dict,
                                stamp: str) -> None:
        order = self._require_order(conn, order_id)
        if order["state"] == C.CLOSED:
            self.store.append_ledger(conn, C.EV_STALE_RECEIPT, "order", order_id,
                                     {"reason": "ALREADY_CLOSED"}, stamp)
            return
        self.store.update_order(
            conn, order_id, state=C.CLOSED, close_reason=C.CLOSE_WITHDRAWN,
            withdraw_confirm_seq=snapshot.get("withdraw_confirm_seq"),
            updated_at=stamp)
        self.store.append_ledger(conn, C.EV_WITHDRAW_CONFIRMED, "order", order_id,
                                 {"snapshot": snapshot}, stamp)

    # ================= 发送中断恢复 =================
    def recover(self) -> dict:
        """中断恢复：超时先查对端真实状态，再决定补账/重试，不重复派单。"""
        stamp = _now()
        pending_orders = self.store.list_orders(state=C.PENDING)
        withdrawing = self.store.list_orders(state=C.WITHDRAWING)
        # 撤警已决定但撤回请求发送中断：单子仍在 DISPATCHED 且挂着错误
        failed_withdraw = [o for o in self.store.list_orders(state=C.DISPATCHED)
                           if o.get("last_error")]
        repaired: list[dict] = []
        with self._lock:
            for order in pending_orders:
                repaired.append(self._recover_dispatch(order, stamp))
            for order in withdrawing:
                repaired.append(self._recover_withdraw(order, stamp))
            for order in failed_withdraw:
                repaired.append(self._recover_withdraw(order, stamp))
        return {"recovered": [item for item in repaired if item is not None]}

    def _recover_dispatch(self, order: dict, stamp: str) -> dict | None:
        oid = order["order_id"]
        remote = self.client.get_order(oid)
        with self.store.connect() as conn:
            local = self._require_order(conn, oid)
            if remote is None:
                # 对端没收到 → 安全重试派单（幂等键 oid，不会产生第二张单）
                self.store.append_ledger(conn, C.EV_RECOVER_SKIP, "order", oid,
                                         {"remote": None, "next": "REDISPATCH"}, stamp)
                payload = self._order_payload(local)
                try:
                    snapshot = self.client.dispatch(oid, payload)
                except TransportError as exc:
                    self.store.update_order(conn, oid, last_error=str(exc),
                                            updated_at=stamp)
                    return {"order_id": oid, "action": "RETRY_FAILED", "error": str(exc)}
                fields = {"attempts": local["attempts"] + 1, "updated_at": stamp,
                          "last_error": ""}
                if snapshot.get("accept_seq") is not None:
                    fields["state"] = C.DISPATCHED
                    fields["accept_seq"] = snapshot["accept_seq"]
                elif local["state"] == C.PENDING:
                    fields["state"] = C.DISPATCHED
                self.store.update_order(conn, oid, **fields)
                self.store.append_ledger(conn, C.EV_DISPATCH_SENT, "order", oid,
                                         {"recover": True, "snapshot": snapshot}, stamp)
                return {"order_id": oid, "action": "REDISPATCHED",
                        "state": fields["state"]}
            # 对端已存在 → 绝不重派，按对端快照补账
            action = self._align_order_to_remote(conn, local, remote, stamp)
            return {"order_id": oid, "action": action, "remote_state": remote["state"]}

    def _recover_withdraw(self, order: dict, stamp: str) -> dict | None:
        oid = order["order_id"]
        remote = self.client.get_order(oid)
        with self.store.connect() as conn:
            local = self._require_order(conn, oid)
            if remote is None:
                return {"order_id": oid, "action": "REMOTE_MISSING",
                        "note": "对端无订单，保留待撤状态等待人工对账"}
            if remote["state"] == ST_WITHDRAWN:
                self._apply_withdraw_confirm(
                    conn, oid, {"withdraw_confirm_seq": remote["withdraw_confirm_seq"]},
                    stamp)
                self._maybe_close_warning(conn, local["warning_id"], stamp)
                return {"order_id": oid, "action": "WITHDRAW_CONFIRMED_REPLAY"}
            if remote["state"] == ST_WITHDRAW_SENT:
                # 请求已达但确认未到：幂等重试确认通道（不重复发撤回请求）
                try:
                    snapshot = self.client.confirm_withdraw(oid)
                except Exception as exc:  # noqa: BLE001
                    return {"order_id": oid, "action": "CONFIRM_RETRY_FAILED",
                            "error": str(exc)}
                self._apply_withdraw_confirm(
                    conn, oid, {"withdraw_confirm_seq": snapshot["withdraw_confirm_seq"]},
                    stamp)
                self._maybe_close_warning(conn, local["warning_id"], stamp)
                return {"order_id": oid, "action": "WITHDRAW_CONFIRMED"}
            # 对端仍在 RECEIVED/ACCEPTED：撤回请求未达，幂等重发
            self._request_withdraw_in_tx(conn, local, "RECOVER_WITHDRAW", stamp)
            return {"order_id": oid, "action": "WITHDRAW_RESENT",
                    "remote_state": remote["state"]}

    def _align_order_to_remote(self, conn, local: dict, remote: dict,
                               stamp: str) -> str:
        """按对端快照补齐本地账，序号只增不减，状态不倒退。"""
        oid = local["order_id"]
        updates: dict = {"updated_at": stamp, "last_error": ""}
        if remote.get("accept_seq") is not None:
            if local.get("accept_seq") is None or remote["accept_seq"] > local["accept_seq"]:
                updates["accept_seq"] = remote["accept_seq"]
                self.store.remember_receipt(conn, oid, "accept",
                                            remote["accept_seq"], stamp)
        if remote["state"] in (ST_ACCEPTED, ST_WITHDRAW_SENT, ST_WITHDRAWN):
            if local["state"] == C.PENDING:
                updates["state"] = C.DISPATCHED
        # 本地按未接取消但对端仍开着：复活到撤警流程并补发撤回
        if (local["state"] == C.CLOSED
                and local.get("close_reason") == C.CLOSE_CANCELLED
                and remote["state"] != ST_WITHDRAWN):
            updates["state"] = C.WITHDRAWING
            updates["close_reason"] = None
            self.store.update_order(conn, oid, **updates)
            self._reopen_warning_if_needed(conn, local["warning_id"], stamp)
            self.store.append_ledger(conn, C.EV_LATE_ACCEPT, "order", oid,
                                     {"reopen": True, "remote": remote}, stamp)
            self._request_withdraw_in_tx(
                conn, self._require_order(conn, oid), "RECONCILE_WITHDRAW", stamp)
            return "REOPENED_WITHDRAWING"
        if remote["state"] == ST_WITHDRAWN and local["state"] != C.CLOSED:
            self.store.update_order(conn, oid, **updates)
            self._apply_withdraw_confirm(
                conn, oid, {"withdraw_confirm_seq": remote["withdraw_confirm_seq"]}, stamp)
            self._maybe_close_warning(conn, local["warning_id"], stamp)
            return "ALIGNED_WITHDRAWN"
        if updates:
            self.store.update_order(conn, oid, **updates)
        self.store.append_ledger(conn, C.EV_RECOVER_SKIP, "order", oid,
                                 {"remote": remote, "align": updates}, stamp)
        return "ALIGNED"

    # ================= 对账 =================
    def reconcile(self, *, repair: bool = False) -> dict:
        """本地账 vs 值班站账：逐单比对状态与序号，输出差异，可选安全修复。"""
        stamp = _now()
        local_orders = self.store.list_orders()
        remote_index = {item["order_id"]: item for item in self.client.list_orders()}
        diffs: list[dict] = []
        with self._lock:
            for local in local_orders:
                oid = local["order_id"]
                remote = remote_index.get(oid)
                diff = self._diff_one(local, remote)
                if diff:
                    diffs.append(diff)
                    if repair:
                        with self.store.connect() as conn:
                            self._repair_one(conn, local, remote, diff, stamp)
        balanced = not diffs
        return {"balanced": balanced, "diff_count": len(diffs), "diffs": diffs,
                "repaired": repair,
                "orders_checked": len(local_orders)}

    def _diff_one(self, local: dict, remote: dict | None) -> dict | None:
        oid = local["order_id"]
        if remote is None:
            if local["state"] == C.PENDING:
                return None  # 尚未送达是正常在途态
            return {"order_id": oid, "type": "REMOTE_MISSING",
                    "local_state": local["state"], "remote_state": None}
        problems: list[str] = []
        if local.get("accept_seq") != remote.get("accept_seq"):
            problems.append("ACCEPT_SEQ_MISMATCH")
        if local["state"] == C.CLOSED and remote["state"] != ST_WITHDRAWN:
            if local.get("close_reason") == C.CLOSE_WITHDRAWN:
                problems.append("LOCAL_CLOSED_REMOTE_OPEN")
            else:
                # 本地按「未接」取消，对端却仍开着（可能已接单）：需复活补撤回
                problems.append("LOCAL_CANCELLED_REMOTE_OPEN")
        if remote["state"] == ST_WITHDRAWN and local["state"] != C.CLOSED:
            problems.append("REMOTE_WITHDRAWN_LOCAL_OPEN")
        if remote["state"] == ST_ACCEPTED and local["state"] == C.PENDING:
            problems.append("REMOTE_ACCEPTED_LOCAL_PENDING")
        if not problems:
            return None
        return {"order_id": oid, "type": problems,
                "local_state": local["state"], "remote_state": remote["state"],
                "local_accept_seq": local.get("accept_seq"),
                "remote_accept_seq": remote.get("accept_seq")}

    def _repair_one(self, conn, local: dict, remote: dict | None,
                    diff: dict, stamp: str) -> None:
        oid = local["order_id"]
        if remote is None:
            self.store.append_ledger(conn, C.EV_RECONCILE_REPAIR, "order", oid,
                                     {"diff": diff, "action": "MANUAL_REQUIRED"}, stamp)
            return
        action = self._align_order_to_remote(conn, local, remote, stamp)
        self.store.append_ledger(conn, C.EV_RECONCILE_REPAIR, "order", oid,
                                 {"diff": diff, "action": action}, stamp)

    # ================= 回读 =================
    def get_warning_view(self, warning_id: str) -> dict:
        with self.store.connect() as conn:
            warning = self._require_warning(conn, warning_id)
            orders = self.store.list_orders_conn(conn, warning_id)
        return self._warning_view(warning, orders)

    def list_warning_views(self, state: str | None = None) -> list[dict]:
        views = []
        for warning in self.store.list_warnings(state):
            orders = self.store.list_orders(warning["warning_id"])
            views.append(self._warning_view(warning, orders))
        return views

    def get_order_view(self, order_id: str) -> dict:
        with self.store.connect() as conn:
            order = self._require_order(conn, order_id)
        return self._order_view(order)

    def list_order_views(self, state: str | None = None) -> list[dict]:
        return [self._order_view(o) for o in self.store.list_orders(state=state)]

    def monthly_basis(self, station: str | None = None,
                      month: str | None = None) -> list[dict]:
        """回读月量依据：来自实际日值核算后落库的日值，重新走同一核算函数。"""
        rows = self.store.list_daily(station, month)
        records = [{"station": r["station"], "date": r["rain_date"],
                    "rainfall_mm": r["rainfall_mm"], "version": r["version"]}
                   for r in rows]
        return accounting.evaluate(records, self.thresholds,
                                   self.default_threshold)["basis"]

    def ledger(self, ref_type: str | None = None,
               ref_id: str | None = None) -> list[dict]:
        return self.store.list_ledger(ref_type, ref_id)

    # ================= 内部工具 =================
    def _maybe_close_warning(self, conn, warning_id: str, stamp: str) -> None:
        warning = self.store.get_warning(conn, warning_id)
        if warning is None or warning["state"] == C.WITHDRAWN:
            return
        orders = self.store.list_orders_conn(conn, warning_id)
        open_orders = [o for o in orders if o["state"] != C.CLOSED]
        if not open_orders and warning["state"] == C.WITHDRAWING:
            self.store.cas_warning_state(conn, warning_id, warning["version"],
                                         C.WITHDRAWN, stamp)
            self.store.append_ledger(conn, C.EV_WARNING_WITHDRAWN, "warning",
                                     warning_id, {"version": warning["version"] + 1},
                                     stamp)

    def _reopen_warning_if_needed(self, conn, warning_id: str, stamp: str) -> None:
        """迟到证据证明仍有在途处置时，已结案预警回到撤警中（只在撤警链路内回退）。"""
        warning = self.store.get_warning(conn, warning_id)
        if warning is not None and warning["state"] == C.WITHDRAWN:
            self.store.cas_warning_state(conn, warning_id, warning["version"],
                                         C.WITHDRAWING, stamp)
            self.store.append_ledger(
                conn, C.EV_LATE_ACCEPT, "warning", warning_id,
                {"reopen": C.WITHDRAWING, "reason": "OPEN_ORDER_EVIDENCE"}, stamp)

    def _record_failure(self, order_id: str, event: str, message: str) -> None:
        with self.store.connect() as conn:
            order = self.store.get_order(conn, order_id)
            if order:
                self.store.update_order(conn, order_id, last_error=message,
                                        attempts=order["attempts"] + 1,
                                        updated_at=_now())
                self.store.append_ledger(conn, event, "order", order_id,
                                         {"error": message}, _now())

    def _require_warning(self, conn, warning_id: str) -> dict:
        warning = self.store.get_warning(conn, warning_id)
        if warning is None:
            raise NotFound(f"预警 {warning_id} 不存在", code=C.E_WARNING_NOT_FOUND)
        return warning

    def _require_order(self, conn, order_id: str) -> dict:
        order = self.store.get_order(conn, order_id)
        if order is None:
            raise NotFound(f"派单 {order_id} 不存在", code=C.E_ORDER_NOT_FOUND)
        return order

    def _order_payload(self, order: dict) -> dict:
        return {"order_id": order["order_id"], "warning_id": order["warning_id"],
                "station": order["station"], "month": order["month"]}

    @staticmethod
    def _last_history_seq(snapshot: dict, event: str) -> int | None:
        for item in reversed(snapshot.get("history", [])):
            if item.get("event") == event:
                return item.get("seq")
        return None

    def _warning_view(self, warning: dict, orders: list[dict]) -> dict:
        counts = {C.PENDING: 0, C.DISPATCHED: 0, C.WITHDRAWING: 0, C.CLOSED: 0}
        for order in orders:
            counts[order["state"]] = counts.get(order["state"], 0) + 1
        return {
            "warning_id": warning["warning_id"], "station": warning["station"],
            "month": warning["month"], "state": warning["state"],
            "version": warning["version"], "rainfall_mm": warning["rainfall_mm"],
            "threshold_mm": warning["threshold_mm"],
            "basis": warning.get("basis") or {},
            "counts": {"pending": counts[C.PENDING], "dispatched": counts[C.DISPATCHED],
                       "withdrawing": counts[C.WITHDRAWING], "closed": counts[C.CLOSED]},
            "orders": [self._order_view(o) for o in orders],
        }

    def _order_view(self, order: dict, *, duplicate: bool = False,
                    stale: bool = False, reason: str | None = None) -> dict:
        view = {k: order.get(k) for k in (
            "order_id", "warning_id", "station", "month", "state", "close_reason",
            "dispatch_seq", "withdraw_seq", "accept_seq", "withdraw_confirm_seq",
            "attempts", "last_error", "created_at", "updated_at")}
        if duplicate:
            view["duplicate"] = True
        if stale:
            view["stale"] = True
            view["stale_reason"] = reason
        return view
