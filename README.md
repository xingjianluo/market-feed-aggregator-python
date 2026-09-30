# market-feed-aggregator

本地行情接入与时间窗口聚合服务。用**可回放的本地数据源**模拟行情推送，
保证在**乱序、重复、断线重连/整体重放**下结果前后一致：重复不二次累计、
乱序事件被显式拒绝、数据缺口显式标出，写入与查询可并发且看到一致快照。

## 1. 支持的事件类型与格式

接入 `POST /events`，请求体必须是一个 JSON 对象。公共字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | 非空字符串 | 事件唯一标识，**全局去重键** |
| `symbol` | 非空字符串 | 品种代码，不同品种互不影响 |
| `type` | 字符串 | 事件类型，目前支持 `trade` / `quote` |
| `timestamp_ms` | 非负整数 | 事件时间（毫秒）。布尔值不接受 |

### `trade`（成交）

| 字段 | 约束 |
|---|---|
| `price` | 有限正数（NaN/Inf/负数/0/布尔均拒绝） |
| `size` | 有限正数 |

```json
{"event_id":"BTC-0001","symbol":"BTCUSD","type":"trade",
 "timestamp_ms":1000,"price":100.5,"size":1.5}
```

### `quote`（盘口最优买卖快照）

| 字段 | 约束 |
|---|---|
| `bid_price` / `ask_price` | 有限正数 |
| `bid_size` / `ask_size` | 有限正数 |

```json
{"event_id":"BTC-0002","symbol":"BTCUSD","type":"quote","timestamp_ms":1100,
 "bid_price":100.4,"bid_size":3,"ask_price":100.7,"ask_size":2}
```

非法事件**不会被静默接收**，HTTP 返回 `422`，响应体内的 `reason` 区分原因：

| reason | 触发条件 |
|---|---|
| `not_a_dict` | 请求体不是 JSON 对象 |
| `missing_field` | 缺少必填字段 |
| `bad_type` | 字段类型不对（含布尔冒充数字、空字符串） |
| `unknown_event_type` | `type` 不是 trade/quote |
| `invalid_price` | 价格非有限正数（含 NaN/Inf） |
| `invalid_size` | 量非有限正数 |
| `invalid_timestamp` | 时间戳为负数 |
| `out_of_order` | 时间戳早于该品种已接受事件的水位 |

直接使用 `MarketDataStore.ingest()` 时返回 `IngestResult`，`status` 为
`accepted` / `duplicate` / `rejected`，`reason` 与上表一致。

## 2. 聚合与去重规则

### 去重

- 以 `event_id` 为幂等键：同一 ID 第二次及以后到达一律返回
  `duplicate`（HTTP 200，幂等成功），**只计入一次**。
- 即使重复事件的字段内容与首次不同，也以首次为准。
- 乱序被拒的事件同样会占用其 `event_id`（视为"已见过"），
  因此重复推送不会在第二次被错误地接受，保证判定对重放确定。

### 乱序

- 每个 `symbol` 维护一个已接受事件的最大时间戳水位。
- `timestamp_ms < 水位` 的事件以 `out_of_order` 拒绝，不进入聚合。
- 等于水位允许（同毫秒多笔是合法的）。水位按 symbol 独立，跨品种互不影响。

### 时间窗口聚合

- 固定窗口、按 epoch 对齐：
  `window_start = timestamp_ms // window_size_ms * window_size_ms`，默认 1000ms。
- 每个 `(symbol, type)` 独立聚合。
- `trade` 窗口产出：`count`、`open/high/low/close`、`total_size`。
- `quote` 窗口产出：`count`、`avg_bid/avg_ask`（算术平均）、
  `last_bid/last_ask`、`total_size`（买卖挂量之和）。
- 查询：`GET /summary?symbol=BTCUSD&start_ms=1000&end_ms=4000`
  （半开区间 `[start_ms, end_ms)`，与区间相交的对齐窗口都会返回）。

### 数据缺口（不补零、不跳过）

在某品种某类型**已观测到的时间范围内部**，没有任何事件落入的整个窗口会作为
`gaps` 显式返回：

```json
"gaps": {"trade": [{"start_ms": 2000, "end_ms": 3000}]}
```

查询范围之外（更早或更晚、从未观测过）的无数据**不算缺口**，不会凭空造洞。
缺口窗口也不会以全零的形式混进 `windows`。

## 3. 断线续传 / 重放语义

`marketfeed.replay.ReplayFeed` 提供可回放的本地数据源，两种语义结果都与首次一致：

1. **同一连接上的重复投递（at-least-once 重发）**：不重置直接整段重放，
   已处理事件返回 `duplicate`，聚合结果不变 —— 不重复累计。
2. **全新连接从起点续传**：调用 `MarketDataStore.reset_session()` 清空接入状态后，
   从头投递同一段数据。由于投递顺序固定（`ReplayFeed` 可按
   `(timestamp_ms, event_id)` 稳定排序），校验/去重/乱序判定都是确定性的，
   逐条判定与窗口结果与首次完全一致 —— 不漏算、不重算。

`run(ingest, ordered=False)` 可按原始顺序投递，用于主动制造乱序场景。
断线后续传某一段用 `replay_segment(...)`（不做任何重置）。

## 4. 并发、承载能力与过期

- 所有写入（去重 + 水位判定 + 聚合）在同一把锁内完成；查询返回窗口的**快照副本**，
  并发读不会看到半成品，写入期间观测到的计数单调不减。
- HTTP 写入走有界异步队列（默认容量 1024，入队等待 50ms）：
  - 队列打满（超承载能力）→ **HTTP 503**，`reason=backpressure`，立即失败、不阻塞。
- 保留策略：每个 symbol 只保留最近 `retention_windows` 个窗口（默认 60）。
  - 查询区间完全位于淘汰水位之前 → **HTTP 410**，`reason=expired`
    （库内抛 `RequestExpiredError`，`code=expired`）；
  - 查询区间部分跨入已淘汰范围 → 正常返回但 `evicted=true`，调用方需自行决定是否信任。
- 参数非法（如 `end_ms <= start_ms`）→ **HTTP 400** `bad_request`。
- 任何内部异常都被统一映射为 **HTTP 500** `internal_error`，
  不把堆栈/内部异常文本透传给调用方（库内统一包成 `InternalFeedError`）。

`backpressure`（503）、`expired`（410）、`bad_request`（400）、
`internal_error`（500）四类失败可明确区分。

## 5. 本地验证方法

需要 Python 3.14 与 [uv](https://docs.astral.sh/uv/)。

```bash
# 安装依赖
uv sync

# 运行全部单测（日志中打印每条用例的输入、判定依据与最终结果）
uv run pytest -s -o log_cli=true

# 只跑乱序/重复/重放相关
uv run pytest tests/test_ingest_dedup_order.py -v

# 离线回放演示（含乱序、重复、缺口、非法事件；对比首次/重放/全新重放）
uv run python examples/demo_replay.py

# 启动 HTTP 服务
uv run python main.py
# 然后：
curl -s -X POST localhost:8000/events -H 'content-type: application/json' -d '{
  "event_id":"t1","symbol":"BTCUSD","type":"trade",
  "timestamp_ms":1000,"price":100.5,"size":1.5}'
curl -s 'localhost:8000/summary?symbol=BTCUSD&start_ms=0&end_ms=4000'
```

测试结构：

| 文件 | 覆盖内容 |
|---|---|
| `tests/test_validator.py` | 各事件类型的正常/边界字段、按原因区分的拒绝 |
| `tests/test_ingest_dedup_order.py` | 重复只计一次、乱序拒绝、整体重放一致、断线续传分段一致 |
| `tests/test_aggregator.py` | OHLC、总量、盘口均价、缺口显式标记、窗口边界、类型/品种隔离 |
| `tests/test_concurrency_lifecycle.py` | 并发读写一致快照、并发重复、保留淘汰与过期请求 |
| `tests/test_api.py` | HTTP 错误码可区分、503 背压、410 过期、HTTP 并发读写 |

## 6. 模块速览

```
marketfeed/
  errors.py      # RejectReason 与可区分异常
  models.py      # MarketEvent / IngestResult / WindowSummary / Gap / QueryResult
  validator.py   # 原始 dict -> MarketEvent，按原因拒绝
  dedup.py       # 线程安全的 event_id 幂等集合
  aggregator.py  # 固定窗口聚合 + 内部缺口检测
  store.py       # 线程安全门面：校验->去重->乱序->聚合，背压槽位，保留淘汰
  replay.py      # 可回放本地数据源（排序/分段/重置会话）
  api.py         # FastAPI：有界队列写入、查询、统一错误映射
```