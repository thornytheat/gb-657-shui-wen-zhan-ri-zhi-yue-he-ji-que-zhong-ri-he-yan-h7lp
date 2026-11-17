"""M8 常量：状态机、原因码、默认警戒线。

状态约定（dispatch_orders.state）：
    PENDING      待派：已生成派单，尚未成功送达值班站
    DISPATCHED   已接（待处置）：值班站已接单
    WITHDRAWING  待撤：已向值班站发撤回请求，等待撤回确认
    CLOSED       结案：派单生命周期结束（撤单成功 / 撤回确认 / 过期回执不再处理）

预警状态（warnings.state）：
    ACTIVE       生效中：月量仍在警戒线（含）以上，可派单
    WITHDRAWING  撤警中：已决定撤警，尚有派单未结案
    WITHDRAWN    已撤警：全部派单结案

不变量：
- 月量低于警戒线后不会再产生新派单（ACTIVE → 非 ACTIVE 单向）；
- 已接单的处置永远登记在册，撤警不能删单，只能走撤回流程；
- 乱序回执不允许状态倒退（只进不退，过期决定进 ledger 但不改状态）。
"""
from __future__ import annotations

from decimal import Decimal

# ---- 派单状态 ----
PENDING = "PENDING"
DISPATCHED = "DISPATCHED"
WITHDRAWING = "WITHDRAWING"
CLOSED = "CLOSED"

ORDER_STATES = (PENDING, DISPATCHED, WITHDRAWING, CLOSED)

# ---- 预警状态 ----
ACTIVE = "ACTIVE"
WITHDRAWING = "WITHDRAWING"
WITHDRAWN = "WITHDRAWN"
WARNING_STATES = (ACTIVE, WITHDRAWING, WITHDRAWN)

# ---- 结案原因 ----
CLOSE_CANCELLED = "CANCELLED"          # 撤警时取消的「未接」单项
CLOSE_WITHDRAWN = "WITHDRAWN"          # 值班站确认撤回
CLOSE_STALE = "STALE"                  # 已结案后迟到且无法再处理的回执

# ---- ledger 事件类型 ----
EV_ORDER_CREATED = "ORDER_CREATED"
EV_DISPATCH_SENT = "DISPATCH_SENT"
EV_DISPATCH_FAILED = "DISPATCH_FAILED"
EV_ACCEPT_RECORDED = "ACCEPT_RECORDED"
EV_WITHDRAW_DECISION = "WITHDRAW_DECISION"
EV_ORDER_CANCELLED = "ORDER_CANCELLED"
EV_WITHDRAW_SENT = "WITHDRAW_SENT"
EV_WITHDRAW_FAILED = "WITHDRAW_FAILED"
EV_WITHDRAW_CONFIRMED = "WITHDRAW_CONFIRMED"
EV_STALE_RECEIPT = "STALE_RECEIPT"
EV_LATE_ACCEPT = "LATE_ACCEPT"
EV_RECOVER_SKIP = "RECOVER_SKIP"
EV_RECONCILE_REPAIR = "RECONCILE_REPAIR"
EV_WARNING_OPENED = "WARNING_OPENED"
EV_WARNING_WITHDRAWN = "WARNING_WITHDRAWN"

# ---- 业务原因码 ----
E_WARNING_NOT_FOUND = "E_WARNING_NOT_FOUND"
E_ORDER_NOT_FOUND = "E_ORDER_NOT_FOUND"
E_STALE_DECISION = "E_STALE_DECISION"
E_NOT_WITHDRAWABLE = "E_NOT_WITHDRAWABLE"
E_NO_DISPATCH = "E_NO_DISPATCH"
E_BAD_RECORD = "E_BAD_RECORD"
E_STATION_UNREACHABLE = "E_STATION_UNREACHABLE"
E_VERSION_CONFLICT = "E_VERSION_CONFLICT"

# 月雨量以 0.1 mm 为刻度（与 m5 hydro.monthly 的 quantize 保持一致）
RAIN_QUANT = Decimal("0.1")

# 默认警戒线（mm）：月量 >= 阈值即预警；可按站覆盖，再回落到全局默认
DEFAULT_THRESHOLD_MM = Decimal("300.0")
