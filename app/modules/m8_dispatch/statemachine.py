"""派单状态机：集中定义「只进不退」的迁移规则，服务层不得自行改 state。

迁移图：
    PENDING      --dispatch ok-->           DISPATCHED
    PENDING      --撤警(未接)-->             CLOSED(CANCELLED)
    DISPATCHED   --接单回执-->               DISPATCHED（登记 accept_seq，状态不变）
    DISPATCHED   --撤警-->                   WITHDRAWING
    WITHDRAWING  --撤回确认-->               CLOSED(WITHDRAWN)
    任意非结案   --撤警后迟到接单-->          登记后进入 WITHDRAWING（补撤回，不倒退）
    CLOSED       --任何回执-->               拒绝/仅留痕，状态不变

规则要点：
- 乱序回执（seq 小于已登记序号）不倒退，只进 ledger；
- CLOSED 是吸收态，任何后续回执都不能改状态；
- 撤回确认只在 WITHDRAWING 生效，DISPATCHED 上先到确认按乱序留痕。
"""
from __future__ import annotations

from . import constants as C

# state -> 允许迁移到的 state
FORWARD = {
    C.PENDING: {C.DISPATCHED, C.WITHDRAWING, C.CLOSED},
    C.DISPATCHED: {C.WITHDRAWING, C.CLOSED},
    C.WITHDRAWING: {C.CLOSED},
    C.CLOSED: set(),
}


def can_transition(current: str, target: str) -> bool:
    return target in FORWARD.get(current, set())


def is_terminal(state: str) -> bool:
    return state == C.CLOSED


def receipt_is_forward(last_seq: int | None, incoming_seq: int) -> bool:
    """回执序号必须严格增大；None 表示此前未登记过任何该类回执。"""
    if incoming_seq is None:
        return False
    if last_seq is None:
        return True
    return incoming_seq > last_seq


def decide_accept(order: dict, accept_seq: int) -> tuple[str, str]:
    """接单回执到达时的决策。

    返回 (action, reason)：
      apply_dispatch     正常接单（PENDING 上先到回执，补登并推进到 DISPATCHED）
      record_only        已在 DISPATCHED，仅登记新序号
      late_to_withdraw   撤警流程中迟到的接单：登记并保持/进入 WITHDRAWING（补撤回）
      late_reopen        已按「未接」取消结案，却收到新接单回执：入账并复活进撤警流程
      stale              乱序/已撤回结案，不倒退
    """
    state = order["state"]
    if is_terminal(state):
        # 已撤回确认结案：对端处置已闭环，迟到接单不再有意义，仅留痕
        if order.get("close_reason") == C.CLOSE_WITHDRAWN:
            return "stale", "ALREADY_WITHDRAWN"
        # 按未接取消的单子收到「此前未见过的」新接单回执：必须入账并补撤回
        if receipt_is_forward(order.get("accept_seq"), accept_seq):
            return "late_reopen", "CANCELLED_BUT_ACCEPTED"
        return "stale", "ORDER_CLOSED"
    if not receipt_is_forward(order.get("accept_seq"), accept_seq):
        return "stale", "OUT_OF_ORDER"
    if state == C.WITHDRAWING:
        return "late_to_withdraw", "WITHDRAW_IN_PROGRESS"
    if state == C.DISPATCHED:
        return "record_only", "ALREADY_DISPATCHED"
    return "apply_dispatch", "OK"


def decide_withdraw_confirm(order: dict, confirm_seq: int) -> tuple[str, str]:
    """撤回确认到达时的决策；确认序号只与已登记的确认序号比较（不同于撤回请求序号）。"""
    state = order["state"]
    if is_terminal(state):
        if order.get("close_reason") == C.CLOSE_WITHDRAWN:
            return "stale", "ALREADY_WITHDRAWN"
        return "stale", "ORDER_CLOSED"
    if not receipt_is_forward(order.get("withdraw_confirm_seq"), confirm_seq):
        return "stale", "OUT_OF_ORDER"
    if state != C.WITHDRAWING:
        # 撤回请求尚未登记确认就先到：留痕但不推进，等撤回请求对齐
        return "stale", "WITHDRAW_NOT_REQUESTED"
    return "close", "OK"
