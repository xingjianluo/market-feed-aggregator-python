# 本地数据源、回放与断线续传

## 本地数据源（JSONL）

`data/sample_feed.jsonl` 是可回放的本地行情源，每行一个 JSON 事件，
格式见 [event-format.md](event-format.md)。文件顺序即“推送顺序”，
因此可以用来模拟：

- 正常顺序推送：直接顺序读；
- **乱序推送**：`shuffled=true`，固定随机种子（默认 `20260930`）做确定性打乱，
  打乱后晚到的旧事件按 `timestamp_out_of_order` 拒绝；
- **重复推送**：文件内插入重复行，或对同一文件多次回放，
  重复事件按 `duplicate_event` 识别，不重复累计；
- **断线重连**：从头重放文件到断点位置（见下）。

数据源文件本身有问题（文件不存在、某行不是合法 JSON、不是对象）会
**显式报错并带行号**，不会被当成普通事件静默跳过。

编程接口：

```python
from market_feed.replay import ReplayFeed, replay_into

records = ReplayFeed("data/sample_feed.jsonl").read_raw()  # 保持文件顺序
ReplayFeed.write("out.jsonl", records)                     # 生成新数据文件
summary = replay_into("data/sample_feed.jsonl", svc.submit, shuffled=True)
# summary: total / accepted / duplicates / rejected / items(逐条 outcome+reason)
```

## 整体重放语义（幂等）

对同一服务实例把同一段数据回放任意次：

- 首次：全部 `accepted`；
- 再次：全部 `duplicate`，水位、逐窗口统计、事件集合与首次**完全一致**，
  不会出现重复累计或漏算。

对两个全新实例分别回放同一段数据，得到的查询结果逐窗口可直接判等。

## 断线续传语义

`POST /resume`（或 `MarketFeedService.resume_from(path, last_event_id)`）：

1. 从文件**开头**重新发送数据，停在 `last_event_id`（含）所在位置；
2. 服务端是“按 event_id 去重的追加日志”，所以重放任意前缀都幂等：
   - 断线前已计入的事件 -> `duplicate`，不产生第二次累计；
   - 断线窗口内漏掉的事件 -> `accepted`，被补齐；
3. 续传完成后，水位与每个窗口的聚合结果与“从未断线、连续推送”完全一致。

注意：`last_event_id` 只是“本次重放到哪里停”的游标，不参与过滤；
即使不传或传一个不存在的 id（=整文件重放），结果仍然一致，靠的是幂等而不是游标。
乱序是另一类问题：若重连时服务器补发的某条事件时间戳早于当前水位
且超过迟到容忍，会以 `timestamp_out_of_order` 拒绝并计入 `rejected`。

## HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `GET /health` | 存活与当前水位 |
| `POST /events` | 接入单个事件（body 为 JSON 对象） |
| `GET /symbols/{symbol}/windows?from_ms=&to_ms=` | 查询 `[from,to)` 窗口聚合 |
| `POST /replay` | body `{"path": "...", "shuffled": false}` 整体回放 |
| `POST /resume` | body `{"path": "...", "last_event_id": "t2"}` 断线续传 |

## 本地验证方法

```bash
# 1. 安装依赖（需要 Python 3.14）
uv sync

# 2. 跑全部单测（日志会打印每条用例的输入、判定依据、最终结果）
uv run pytest -s

# 只跑某一类
uv run pytest tests/test_replay.py -s
uv run pytest tests/test_concurrency.py -s
uv run pytest tests/test_validation.py -s

# 3. 启动 HTTP 服务
uv run uvicorn main:app --reload --port 8000
```

另一终端验证主要场景：

```bash
# 接入合法事件
curl -s -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"event_id":"t1","event_type":"trade","symbol":"BTCUSDT",
       "timestamp_ms":1000,"price":65000.5,"size":0.1,"side":"buy"}'

# 重复推送 -> 409
curl -i -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"event_id":"t1","event_type":"trade","symbol":"BTCUSDT",
       "timestamp_ms":1000,"price":65000.5,"size":0.1,"side":"buy"}'

# 字段缺失 -> 422，reason=missing_field
curl -s -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"event_id":"x","event_type":"trade"}'

# 整体回放本地文件
curl -s -X POST localhost:8000/replay -H 'content-type: application/json' \
  -d '{"path":"data/sample_feed.jsonl"}'

# 查询窗口（可见 gap / no_data 显式标记）
curl -s 'localhost:8000/symbols/BTCUSDT/windows?from_ms=0&to_ms=7000'

# 断线续传：重放到 t2（含）为止
curl -s -X POST localhost:8000/resume -H 'content-type: application/json' \
  -d '{"path":"data/sample_feed.jsonl","last_event_id":"t2"}'
```

## 测试组织

| 文件 | 覆盖重点 |
|---|---|
| `tests/test_validation.py` | 12 类拒绝原因、数字字符串、乱序/迟到边界 |
| `tests/test_dedup_and_order.py` | 重复只计一次、身份优先于乱序、并发重复 |
| `tests/test_windowing.py` | 聚合数值、封口时机、gap/no_data、多 symbol |
| `tests/test_replay.py` | 整体重放幂等、乱序、断线续传结果一致、坏文件 |
| `tests/test_concurrency.py` | 并发读写一致性、深拷贝快照、503/410 区分 |
| `tests/test_api.py` | HTTP 状态码区分、错误响应不泄漏内部细节 |
