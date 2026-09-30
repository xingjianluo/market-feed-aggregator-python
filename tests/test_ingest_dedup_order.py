"""去重、乱序与整体重放：重复只计一次；乱序按原因拒绝；重放结果与首次一致。"""

from __future__ import annotations

import logging

from marketfeed.errors import RejectReason
from marketfeed.models import IngestStatus
from marketfeed.replay import ReplayFeed
from marketfeed.store import MarketDataStore

log = logging.getLogger("marketfeed.tests")


def trade(eid, ts, p=10.0, s=1.0, sym="BTC"):
    return {"event_id": eid, "symbol": sym, "type": "trade",
            "timestamp_ms": ts, "price": p, "size": s}


def signature(results):
    return [(r.status, r.reason, r.event_id) for r in results]


def window_tuples(store, symbol="BTC", end=6000):
    q = store.query(symbol, 0, end)
    out = []
    for ws, w in sorted(q.windows.get("trade", {}).items()):
        out.append((ws, w.open, w.high, w.low, w.close, w.total_size, w.count))
    gaps = [(g.start_ms, g.end_ms) for g in q.gaps.get("trade", [])]
    return out, gaps


def test_duplicate_counts_once(case_logger):
    store = MarketDataStore(window_size_ms=1000)
    raw = trade("a", 100)
    r1 = store.ingest(raw)
    r2 = store.ingest(raw)
    r3 = store.ingest(trade("a", 100, p=999))  # 同 ID 改内容仍判重
    case_logger("重复推送", first=r1.status, second=r2.status, third=r3.status,
                reason2=r2.reason, reason3=r3.reason)
    assert r1.status is IngestStatus.ACCEPTED
    assert r2.status is IngestStatus.DUPLICATE and r2.reason is RejectReason.DUPLICATE
    assert r3.status is IngestStatus.DUPLICATE
    w = store.query("BTC", 0, 1000).windows["trade"][0]
    assert w.count == 1 and w.close == 10.0


def test_out_of_order_rejected(case_logger):
    store = MarketDataStore(window_size_ms=1000)
    r_a = store.ingest(trade("a", 1000))
    r_b = store.ingest(trade("b", 999))       # 早于水位
    r_c = store.ingest(trade("c", 1000))      # 等于水位，允许
    case_logger("乱序判定", a=r_a.status, b=(r_b.status, r_b.reason), c=r_c.status,
                detail=r_b.detail)
    assert r_b.status is IngestStatus.REJECTED
    assert r_b.reason is RejectReason.OUT_OF_ORDER
    assert r_c.status is IngestStatus.ACCEPTED
    w = store.query("BTC", 0, 2000).windows["trade"][1000]
    assert w.count == 2  # b 未计入


def test_unordered_replay_is_deterministic(case_logger):
    data = [trade("a", 100, p=10), trade("x", 50, p=999),
            trade("a", 100, p=10), trade("b", 900, p=12),
            trade("c", 2500, p=9), 42]  # 含乱序、重复、非法、缺口

    store = MarketDataStore(window_size_ms=1000)
    first = ReplayFeed(data).run(store.ingest, ordered=False)
    win1, gaps1 = window_tuples(store)
    case_logger("首次乱序投递", sig=signature(first), windows=win1, gaps=gaps1)

    # 语义 A：同会话整体重放 -> 已见事件全部 duplicate，聚合不变
    again = ReplayFeed(data).run(store.ingest, ordered=False)
    win2, gaps2 = window_tuples(store)
    dup_ok = all(r.status is IngestStatus.DUPLICATE
                 for r in again if r.event_id in {"a", "b", "c"})
    case_logger("同会话重放", dup_ok=dup_ok, windows=win2)
    assert dup_ok and win2 == win1 and gaps2 == gaps1

    # 语义 B：全新连接从头续传（reset）-> 逐条判定与首次完全一致
    fresh = ReplayFeed(data).run(store.ingest, ordered=False, reset=store.reset_session)
    win3, gaps3 = window_tuples(store)
    case_logger("全新连接重放", sig_equal=(signature(fresh) == signature(first)), windows=win3)
    assert signature(fresh) == signature(first)
    assert win3 == win1 and gaps3 == gaps1


def test_segment_resume_after_reconnect(case_logger):
    """模拟断线：先投前半段，重连后投递含重复的后半段，结果等于一次性投递。"""
    segment1 = [trade("a", 100, p=10), trade("b", 900, p=12)]
    # 重连后服务端可能再收到已处理过的 b（at-least-once 重发）以及新事件
    segment2 = [trade("b", 900, p=12), trade("c", 1500, p=11), trade("d", 2500, p=9)]

    store = MarketDataStore(window_size_ms=1000)
    ReplayFeed(segment1).run(store.ingest, ordered=False)
    results2 = ReplayFeed(segment2).run(store.ingest, ordered=False)
    win, gaps = window_tuples(store)
    case_logger("断线续传", second_pass=signature(results2), windows=win, gaps=gaps)

    one_shot = MarketDataStore(window_size_ms=1000)
    ReplayFeed(segment1 + [trade("c", 1500, p=11), trade("d", 2500, p=9)]).run(
        one_shot.ingest, ordered=False
    )
    win_ref, gaps_ref = window_tuples(one_shot)
    assert win == win_ref and gaps == gaps_ref
    assert results2[0].status is IngestStatus.DUPLICATE  # 重发的 b 不重复累计
