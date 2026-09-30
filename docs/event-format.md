# 行情事件格式

接入端点：`POST /events`，请求体必须是一个 **JSON 对象**。
每行/每个对象代表一笔行情事件，成交（trade）与盘口（quote）共用公共字段，
类型专属字段按 `event_type` 区分。

## 公共字段（两类事件都必填）

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | 非空字符串 | 事件全局唯一标识。**去重主键**，同一 id 永远只计入一次 |
| `event_type` | 字符串 | `trade`（成交）或 `quote`（盘口） |
| `symbol` | 非空字符串 | 标的代码，如 `BTCUSDT` |
| `timestamp_ms` | 非负整数 | 事件时间，epoch 毫秒；用于乱序判定与窗口归属 |

数值类字段（价格、数量、盘口档位）接受 JSON number **或数字字符串**
（如 `"65000.50"`，交易所常见），但必须能解析为有限正数；
布尔值、`NaN`、`Infinity`、空串一律拒绝。`timestamp_ms` 不接受字符串或小数。

## trade（成交）

额外必填：

| 字段 | 类型 | 说明 |
|---|---|---|
| `price` | 正数 | 成交价 |
| `size` | 正数 | 成交量 |
| `side` | `buy` / `sell` | 主动方向 |

```json
{"event_id": "t1", "event_type": "trade", "symbol": "BTCUSDT",
 "timestamp_ms": 1000, "price": "65000.5", "size": "0.10", "side": "buy"}
```

## quote（盘口）

额外必填：

| 字段 | 类型 | 说明 |
|---|---|---|
| `bids` | 二维数组 | 买盘档位 `[[price, size], ...]`，每档两个正数 |
| `asks` | 二维数组 | 卖盘档位，结构同上 |

`bids` 与 `asks` 不能同时为空（`empty_book`）。允许只有单边。
档位必须是长度为 2 的数组，价格/数量都必须为正。

```json
{"event_id": "q1", "event_type": "quote", "symbol": "BTCUSDT",
 "timestamp_ms": 1200,
 "bids": [["65000.0", "0.5"]],
 "asks": [["65001.0", "0.4"]]}
```

## 拒绝原因（不会静默接收）

校验按固定顺序进行，同一输入总得到同一个原因。HTTP 状态码统一为 `422`，
响应体 `reason` 区分具体原因；重复推送单独使用 `409`。

| reason | 触发条件 |
|---|---|
| `not_an_object` | 请求体不是 JSON 对象（如数组、数字、非法 JSON） |
| `missing_field` | 公共字段或类型专属必填字段缺失 |
| `unknown_event_type` | `event_type` 不是 `trade`/`quote` |
| `bad_field_type` | `event_id` 类型错，或数值字段是非布尔/非数字等类型问题 |
| `bad_symbol` | `symbol` 为空或非字符串 |
| `bad_timestamp` | 时间戳缺失、非整数、为负、`NaN` |
| `timestamp_out_of_order` | 时间戳早于该 symbol 的会话水位（超过配置的迟到容忍）；水位按 symbol 独立 |
| `bad_price` | 成交价缺失/非正数/非有限 |
| `bad_size` | 成交量缺失/非正数/非有限 |
| `bad_side` | `side` 不是 `buy`/`sell` |
| `empty_book` | quote 的买卖盘同时为空 |
| `inconsistent_levels` | bids/asks 不是二维档位数组，或档位价格/数量非法 |

判定优先级要点：

1. **身份去重优先**：`event_id` 已接收时一律返回重复（`409`），
   即使该事件时间戳早于当前水位，也不会被报成乱序。
2. 乱序判定先于类型专属字段：一笔既乱序又带负价格的事件报 `timestamp_out_of_order`。
3. 未知 `event_type` 时不会去检查类型专属字段。
