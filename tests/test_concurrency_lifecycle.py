"""并发接入与读取一致性、过期淘汰、失败反馈区分。"""

from __future__ import annotations

import threading

import pytest

from marketfeed.errors import RequestExpiredError
from marketfeed.models import IngestStatus
from marketfeed.store import MarketDataStore


def trade(eid, ts, p=10.0, s=1.0, sym="BTC"):
    return {"event_id": eid, "symbol": sym, "type": "trade",
            "timestamp_ms": ts, "price": p, "size": s}


def test_concurrent_writes_and_reads_consistent(case_logger):
    store = MarketDataStore(window_size_ms=100, retention_windows=100_000)
    n_per_thread = 200
    n_threads = 8
    symbols = [f"S{k}" for k in range(n_threads)]
    errors: list[Exception] = []

    def writer(k):
        # 每个线程独立 symbol、时间戳在该 symbol 内严格递升，避免跨流乱序
        try:
            for i in range(n_per_thread):
                store.ingest(trade(f"t{k}-{i}", i * 100, sym=symbols[k]))
        except Exception as exc:  # 内部异常不应出现
            errors.append(exc)

    snapshots: list[int] = []

    def reader():
        # 每次快照只做单次查询（锁内原子），用于验证读到的计数单调不减；
        # 不把多次独立查询拼成一个"快照"，因为查询之间允许有其它写入。
        try:
            for _ in range(100):
                q = store.query(symbols[0], 0, n_per_thread * 100 + 1)
                snapshots.append(
                    sum(w.count for b in q.windows.values() for w in b.values())
                )
        except Exception as exc:
            errors.append(exc)

    tw = [threading.Thread(target=writer, args=(k,)) for k in range(n_threads)]
    tr = [threading.Thread(target=reader) for _ in range(4)]
    for t in tw + tr:
        t.start()
    for t in tw + tr:
        t.join()

    total = 0
    for sym in symbols:
        q = store.query(sym, 0, n_per_thread * 100 + 1)
        total += sum(w.count for b in q.windows.values() for w in b.values())
    case_logger("并发", errors=errors, final_count=total,
                expected=n_per_thread * n_threads, snapshots_sample=snapshots[:5])
    assert errors == []
    assert total == n_per_thread * n_threads  # 无重复累计、无漏算
    # 读取过程中计数应单调不减（一致快照，不允许回退）
    assert all(b >= a for a, b in zip(snapshots, snapshots[1:]))


def test_concurrent_duplicate_events_count_once(case_logger):
    store = MarketDataStore(window_size_ms=1000)
    accepted: list[str] = []
    lock = threading.Lock()

    def worker():
        r = store.ingest(trade("same-id", 100))
        with lock:
            accepted.append(r.status.value)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    w = store.query("BTC", 0, 1000).windows["trade"][0]
    from collections import Counter
    dist = Counter(accepted)
    case_logger("并发重复", distribution=dict(dist), window_count=w.count)
    assert dist["accepted"] == 1 and w.count == 1


def test_retention_eviction_and_expired_query(case_logger):
    store = MarketDataStore(window_size_ms=1000, retention_windows=3)
    for ts in (1, 1001, 2001, 5001):
        store.ingest(trade(f"e{ts}", ts))
    q = store.query("BTC", 3000, 6000)
    case_logger("淘汰后查询", windows=sorted(q.windows.get("trade", {})),
                gaps=[(g.start_ms, g.end_ms) for g in q.gaps.get("trade", [])],
                evicted=q.evicted)
    assert sorted(q.windows.get("trade", {})) == [5000]
    # 窗口 3000/4000 已淘汰：查询触及 3000 但已无数据 -> 标记 evicted
    # 整段请求都落在淘汰水位之前 -> 抛 RequestExpiredError，与背压明确区分
    with pytest.raises(RequestExpiredError) as exc:
        store.query("BTC", 0, 1000)
    case_logger("过期请求", code=exc.value.code.value)
    assert exc.value.code.value == "expired"
