# 本地行情接入与窗口聚合（market-feed-aggregator）

用可回放的本地 JSONL 数据源模拟行情推送，提供：

- **统一接入**成交（trade）与盘口（quote）事件；字段缺失、时间戳乱序、
  类型错误按原因分类拒绝，绝不静默接收；同一 `event_id` 只计入一次。
- **时间窗口聚合**与可查询结果；断线重连/整段重放与首次结果一致；
  数据缺口显式标记为 `gap`，不补零、不跳过。
- **读写并行**：读已提交快照，并发接入与读取结果一致；
  超容量（503）与已过期（410）是明确可区分的失败，内部异常不透给调用方。

## 目录

```
market_feed/
  errors.py      领域错误（校验/重复/超载/过期）
  models.py      事件类型、拒绝原因、事件与窗口结果模型
  windowing.py   窗口网格、聚合、gap/no_data 合成
  store.py       去重、水位、保留淘汰、并发与背压
  replay.py      JSONL 本地数据源、确定性乱序回放
  service.py     统一门面（校验+接入+查询+回放+续传）
main.py          FastAPI 接入层与错误码映射
data/sample_feed.jsonl
tests/           6 组测试（乱序/重复/整体重放为重点）
docs/            event-format.md / aggregation.md / replay.md
```

## 快速开始

```bash
uv sync
uv run pytest -s                      # 全部测试（含判定依据日志）
uv run uvicorn main:app --port 8000   # HTTP 服务
```

```python
from market_feed.service import MarketFeedService

svc = MarketFeedService(window_ms=1000)
svc.submit({"event_id": "t1", "event_type": "trade", "symbol": "BTCUSDT",
            "timestamp_ms": 1000, "price": "65000.5", "size": 0.1, "side": "buy"})
svc.submit({...同一个事件...})          # 抛 DuplicateEventError，只计一次

svc.replay_file("data/sample_feed.jsonl")          # 整体回放
svc.replay_file("data/sample_feed.jsonl")          # 再放一次：全部重复，结果不变
svc.resume_from("data/sample_feed.jsonl", "t2")    # 断线续传，结果与连续推送一致

for w in svc.query("BTCUSDT", 0, 7000):
    print(w.status.value, w.to_dict())             # closed / open / gap / no_data
```

## 文档

- [事件类型与格式](docs/event-format.md)
- [聚合、去重与窗口规则](docs/aggregation.md)
- [回放、断线续传与本地验证](docs/replay.md)
