# M8 月雨量预警派单与撤警对账

面向「月雨量更正降到警戒线以下要停止派单，但值班站已接单的处置仍须如实登记」的场景。
直接删警会丢处置，继续派发又会误报；本模块用**不删警、只走状态机**的方式同时满足两者。

## 1 边界

- **不改月量采信规则**：日值版本裁决、零雨/缺测/版本冲突一律复用
  `app/modules/m5_months/hydro.py` 的 `select_daily` / `monthly`，M8 只消费结果并与警戒线比对。
- **不接真实通知**：值班站是本进程内的内存模拟实现（`station.StationBackend`），
  没有短信、外网 webhook 等任何真实出口；故障与中断都通过模拟接口注入。
- **独立库**：派单数据落在 `DISPATCH_DB`（默认 `var/dispatch.sqlite`），
  与主库 `hydrocalendar.sqlite` 分开，表结构演进互不影响。

## 2 警戒线判定

- 月量 `>= 警戒线`（含等于）即预警；月量**严格小于**阈值才允许撤警。
- 默认阈值 `RAIN_WARN_THRESHOLD_MM=300.0`（mm），可用 `RAIN_WARN_STATION_THRESHOLDS`
  传 JSON 按站覆盖，如 `{"0012": 250}`。
- 月量以 0.1 mm 为刻度（Decimal，不用 float）。
- 整月日值都因版本冲突被隔离时，月量依据标 `state=conflict`、`rainfall_mm=null`，
  **不产生预警**（无可信月量，禁止据此派单）。

## 3 状态机

派单状态（`m8_orders.state`，只进不退）：

```
PENDING ──派达──> DISPATCHED ──撤警──> WITHDRAWING ──撤回确认──> CLOSED(WITHDRAWN)
   │                 ▲  ▲                  ▲
   │                 │  └──── 接单回执（登记序号，状态不变）
   └──撤警(未接)──> CLOSED(CANCELLED)      │
                    ▲                      │
                    └── 迟到接单复活（late_reopen）┘
```

- `PENDING`：已生成派单但未成功送达；撤警时**直接取消**（`CANCELLED`），不向值班站发请求。
- `DISPATCHED`：值班站已派达；接单回执只登记 `accept_seq`，不改变状态。
- `WITHDRAWING`：已接单项的撤回请求已发，**收到撤回确认才结案**。
- `CLOSED`：吸收态。按未接取消的单若收到「此前未见过的」新接单回执，
  会复活进 `WITHDRAWING` 并补发撤回；已 `WITHDRAWN` 结案的单不再受理迟到接单（仅留痕）。

预警状态（`m8_warnings.state`）：`ACTIVE → WITHDRAWING → WITHDRAWN`。
预警带单调 `version`，撤警用乐观锁 CAS；并发的过期决定返回 `409 E_STALE_DECISION` 并留痕。

## 4 关键可靠性语义

| 需求 | 实现 |
|---|---|
| 撤警取消未接单项 | `PENDING` 单直接 `CLOSED/CANCELLED`，不发对端请求 |
| 已接单项另发撤回，确认才结案 | `DISPATCHED/WITHDRAWING` 单发撤回，`WITHDRAWING` 等 `withdraw_confirm` |
| 撤警后迟到接单仍入账并补发撤回 | `record_accept` 的 `late_to_withdraw/late_reopen`，入账后 `_request_withdraw_in_tx` |
| 发送中断恢复，超时先查后重试，不重复派单 | `POST /api/dispatch/recover`：先 `get_order` 查对端，未达才用同一 `order_id` 幂等重派 |
| 乱序回执不倒退 | 站内序号 `(order_id, kind, seq)` 去重 + `receipt_is_forward` 严格递增校验，过期进 ledger |
| 并发撤警拒绝过期决定 | `warnings.version` CAS，冲突留痕后在事务提交外抛 409（留痕不回滚） |
| 不丢处置 | 无任何物理删除；全部状态迁移追加 `m8_ledger` |

## 5 接口

主系统前缀 `/api/dispatch`：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/warnings/evaluate` | 导入实际日值核算预警；`auto_dispatch=true` 对新预警立即派单；支持 `idempotency_key` |
| GET | `/basis` | 回读月量依据（日值→采信→月量→警戒线），可按 `station/month` 过滤 |
| GET | `/warnings[?state]` | 回读预警及各状态计数（待派/已接/待撤/结案） |
| GET | `/warnings/{id}` | 单个预警详情（含订单列表） |
| POST | `/warnings/{id}/dispatch` | 派单；存在未结案单时幂等返回，不重复派 |
| POST | `/warnings/{id}/withdraw` | 撤警；body 可带 `expected_version`、`reason` |
| GET | `/orders[?state&warning_id]` | 回读派单 |
| GET | `/orders/{id}` | 单个派单 |
| POST | `/orders/{id}/receipts/accept` | 接单回执 `{accept_seq}` |
| POST | `/orders/{id}/receipts/withdraw-confirm` | 撤回确认 `{confirm_seq}` |
| POST | `/recover` | 中断恢复（先查后试） |
| POST | `/reconcile` | 对账；`{repair:true}` 安全修复漂移 |
| GET | `/ledger[?ref_type&ref_id]` | 回读事件台账 |

模拟值班站前缀 `/api/station-sim`（仅本地）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/orders/{id}/accept` | 模拟值班站接单 |
| POST | `/orders/{id}/withdraw-confirm` | 模拟撤回确认（手动确认模式用） |
| GET | `/orders` | 值班站侧订单账 |
| POST | `/faults` | 故障注入：`dispatch_fail/withdraw_fail/auto_accept/manual_confirm` |
| POST | `/transport/fail-next` | 注入「响应未知」的发送中断 `{order_id, kind, times}` |
| POST | `/reset` | 清空模拟站 |

## 6 一次完整演练

```bash
BASE=http://localhost:18216
# 1) 核算并自动派单（340mm >= 300）
curl -sS -X POST $BASE/api/dispatch/warnings/evaluate -H 'Content-Type: application/json' -d '{
  "records":[
    {"station":"0012","date":"2026-06-01","rainfall_mm":"180.0","version":1},
    {"station":"0012","date":"2026-06-02","rainfall_mm":"160.0","version":1}],
  "auto_dispatch":true}'
# 2) 月量更正到警戒线以下（高版本覆盖），预警自动转撤警
curl -sS -X POST $BASE/api/dispatch/warnings/evaluate -H 'Content-Type: application/json' -d '{
  "records":[{"station":"0012","date":"2026-06-01","rainfall_mm":"10.0","version":2}]}'
# 3) 回读五态与台账
curl -sS "$BASE/api/dispatch/warnings"
curl -sS "$BASE/api/dispatch/ledger"
# 4) 中断恢复与对账
curl -sS -X POST $BASE/api/dispatch/recover
curl -sS -X POST $BASE/api/dispatch/reconcile -H 'Content-Type: application/json' -d '{"repair":true}'
```

## 7 数据表

- `m8_daily`：采信后的日值（含版本、批次号），月量依据的来源。
- `m8_warnings`：预警（站+月唯一，状态与乐观锁版本、月量依据快照）。
- `m8_orders`：派单（状态、各回执序号、尝试次数、最近错误）。
- `m8_ledger`：全量事件台账（每引用对象内 `event_seq` 递增）。
- `m8_idempotency`：幂等键 → 首次响应。
- `m8_station_seen`：回执序号去重表。
