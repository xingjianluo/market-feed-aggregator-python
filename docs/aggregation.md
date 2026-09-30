# 聚合、去重与窗口规则

## 时间窗口

- 窗口长度固定（默认 `window_ms=1000`），对齐 epoch 网格：
  窗口 k 覆盖 `[k*window_ms, (k+1)*window_ms)`。
- 事件按 **事件时间** `timestamp_ms` 归入窗口，与到达顺序无关。
- 水位（watermark）**按 symbol 独立维护** = 该 symbol 已接收事件的最大时间戳。
  一个标的的事件进度不会影响另一个标的的乱序判定与窗口封口。
  某 symbol 窗口结束时间 `<= 该 symbol 水位` 时封口：`open -> closed`。
  （`/health` 暴露的是所有 symbol 水位的最大值。）
- 可配置 `allowed_lateness_ms`（默认 0）：
  `timestamp_ms + allowed_lateness_ms >= 该 symbol 水位` 的迟到事件允许接收，
  否则以 `timestamp_out_of_order` 拒绝。默认严格按序。

## 每个窗口的聚合结果

成交（trades）：

- `count` 笔数、`volume` 总量、`notional` 成交额（price*size 之和）
- `high` / `low` / `last_price`
- `buy_count` / `sell_count`

盘口（quotes）：

- `count` 快照笔数
- `best_bid`（窗口内最高首档买价）、`best_ask`（窗口内最低首档卖价）
- `mid_price_avg`（每笔报价中间价的算术平均；双边齐全的报价才计入）

结果中还包含 `event_ids`（按接收顺序）与 `event_count`，便于核对去重。

## 去重规则

- 去重主键是 `event_id`。去重判定在所有其它校验之前，
  重复推送返回 `409 duplicate_event`，**不更新任何统计、不推进水位**。
- 重复推送 2 次与 200 次效果相同。
- 去重表随窗口保留策略一起淘汰：事件 id 只保证在保留窗口内可识别。

## 缺口（gap）显式标记

查询按窗口网格逐个返回，缺数据**绝不补零、绝不跳过**。
对每个 symbol 独立判定：

| 状态 | 含义 |
|---|---|
| `open` | 当前仍在累积的窗口（已收到过事件） |
| `closed` | 已封口且收到过事件的窗口 |
| `gap` | 已封口、位于该 symbol 首末事件窗口之间，但从未收到事件 —— 明确的数据缺口 |
| `no_data` | 首个事件窗口之前、最后事件窗口之后，或尚未封口的空网格 —— 不臆造缺口 |

`gap` 窗口额外返回 `gap_from_ms` / `gap_to_ms`，且统计字段保持零值，
与“收到过零值事件”可区分（`event_count=0` 且 `status=gap`）。

示例（窗口 1s，事件在 1000 与 5000ms）：

```
[1000,2000) closed   [2000,3000) gap   [3000,4000) gap
[4000,5000) gap      [5000,6000) open
```

## 保留与过期

- 只保留最近 `retention_windows` 个网格窗口（默认 60，HTTP 服务 120）。
- 水位推进后，超出保留范围的物化窗口与事件 id 被淘汰。
- 查询起点落在已淘汰区间时返回 `410 window_expired`，
  错误信息给出最早仍保留的窗口起点，与超载（503）明确区分。

## 并发一致性

- 写入在锁内完成“去重检查 -> 入窗 -> 推进水位 -> 封口 -> 淘汰”，
  并发接入不会重复累计或读到半成品。
- 查询在同一把锁内对窗口结果做**深拷贝快照**，读取期间写入继续进行，
  读到的永远是某个已完整提交的时刻（读已提交），总量单调不减。
- 接入使用有界许可做背压：许可耗尽时立即返回 `503 over_capacity`
  并带 `Retry-After: 1`，不排队拖垮服务。
- 未预期内部异常只记录服务端日志，对调用方统一返回 `500 internal_error`，
  不透出堆栈或内部信息。

## HTTP 错误码一览

| 状态码 | error | 场景 |
|---|---|---|
| 400 | `bad_request` | 其它领域参数错误 |
| 404 | `replay_file_not_found` | 回放文件不存在 |
| 409 | `duplicate_event` | event_id 重复 |
| 410 | `window_expired` | 查询窗口已过期淘汰 |
| 422 | `validation_failed` | 事件/参数校验失败（`reason` 细分） |
| 500 | `internal_error` | 未预期内部异常（不透细节） |
| 503 | `over_capacity` | 超承载能力，可重试 |
