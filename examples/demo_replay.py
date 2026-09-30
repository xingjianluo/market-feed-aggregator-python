"""本地回放演示：加载样例数据 -> 首次接入 -> 整体重放 -> 对比一致性。

运行：uv run python examples/demo_replay.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from marketfeed.replay import ReplayFeed
from marketfeed.store import MarketDataStore

WINDOW_MS = 1000
SYMBOL = "BTCUSD"


def signature(results):
    return [(r.event_id, r.status.value, r.reason.value if r.reason else "-") for r in results]


def dump(title, store):
    q = store.query(SYMBOL, 0, 4000)
    print(f"\n=== {title} ===")
    for etype, buckets in sorted(q.windows.items()):
        for ws, w in sorted(buckets.items()):
            extra = f"o={w.open} h={w.high} l={w.low} c={w.close} v={w.total_size}"
            if etype == "quote":
                extra = f"avg_bid={w.avg_bid} avg_ask={w.avg_ask}"
            print(f"  [{etype}] window={ws} count={w.count} {extra}")
    for etype, gaps in sorted(q.gaps.items()):
        for g in gaps:
            print(f"  [{etype}] GAP {g.start_ms}..{g.end_ms} （无数据，显式缺口）")


def main() -> int:
    data = json.loads((Path(__file__).parent / "sample_events.json").read_text())
    store = MarketDataStore(window_size_ms=WINDOW_MS, retention_windows=100)

    print("--- 输入（按原始顺序，含乱序/重复/非法）---")
    for i, raw in enumerate(data):
        print(f"  {i}: {raw}")

    # 首次：不排序，模拟真实乱序到达
    first = ReplayFeed(data).run(store.ingest, ordered=False)
    print("\n--- 首次判定（event_id, status, reason）---")
    for row in signature(first):
        print(" ", row)
    dump("首次接入结果", store)

    # 断线续传语义 A：同会话整段重发
    again = ReplayFeed(data).run(store.ingest, ordered=False)
    dump("同会话整体重放后（应与首次一致）", store)

    # 断线续传语义 B：全新连接从头回放
    fresh = ReplayFeed(data).run(store.ingest, ordered=False, reset=store.reset_session)
    dump("全新连接从头回放后（应与首次一致）", store)

    ok = signature(first) == signature(fresh)
    print("\n逐条判定首次 == 全新重放:", ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
