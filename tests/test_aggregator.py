"""窗口聚合正确性：OHLC、总量、盘口均价、缺口显式标记、查询边界。"""

from __future__ import annotations

from marketfeed.aggregator import WindowAggregator
from marketfeed.validator import validate_event


def ev(raw):
    e, r = validate_event(raw)
    assert e and not r
    return e


def trade(eid, ts, p, s=1.0, sym="BTC"):
    return ev({"event_id": eid, "symbol": sym, "type": "trade",
               "timestamp_ms": ts, "price": p, "size": s})


def quote(eid, ts, bp, ap, sym="BTC"):
    return ev({"event_id": eid, "symbol": sym, "type": "quote", "timestamp_ms": ts,
               "bid_price": bp, "bid_size": 1, "ask_price": ap, "ask_size": 1})


def test_trade_ohlc_and_size(case_logger):
    agg = WindowAggregator(1000)
    for e in [trade("a", 0, 10, 2), trade("b", 500, 13, 1), trade("c", 999, 9, 3)]:
        agg.add(e)
    w = agg.query("BTC", 0, 1000).windows["trade"][0]
    case_logger("OHLC", open=w.open, high=w.high, low=w.low, close=w.close,
                total=w.total_size, count=w.count)
    assert (w.open, w.high, w.low, w.close) == (10, 13, 9, 9)
    assert w.total_size == 6 and w.count == 3


def test_gap_is_explicit_not_zero_filled(case_logger):
    agg = WindowAggregator(1000)
    agg.add(trade("a", 100, 10))
    agg.add(trade("b", 2500, 11))
    q = agg.query("BTC", 0, 3000)
    gaps = [(g.start_ms, g.end_ms) for g in q.gaps["trade"]]
    case_logger("缺口", windows=sorted(q.windows["trade"]), gaps=gaps)
    assert gaps == [(1000, 2000)]
    assert 1000 not in q.windows["trade"]  # 没有补零窗口


def test_no_gap_outside_observed_range(case_logger):
    agg = WindowAggregator(1000)
    agg.add(trade("a", 5000, 10))
    q_before = agg.query("BTC", 0, 4000)
    q_after = agg.query("BTC", 6000, 9000)
    case_logger("范围外无缺口", before_gaps=q_before.gaps, after_gaps=q_after.gaps,
                before_windows=q_before.windows)
    assert q_before.gaps == {} and q_after.gaps == {}
    assert q_before.windows == {} and q_after.windows == {}


def test_partial_overlap_window_boundaries(case_logger):
    agg = WindowAggregator(1000)
    agg.add(trade("a", 100, 10))
    agg.add(trade("b", 1100, 11))
    # 查询 [500, 1500)：覆盖窗口0尾部和窗口1头部，两个窗口都应返回
    q = agg.query("BTC", 500, 1500)
    case_logger("边界重叠", windows=sorted(q.windows["trade"]), gaps=q.gaps)
    assert sorted(q.windows["trade"]) == [0, 1000]
    assert q.gaps == {}  # 两窗口相邻，无内部缺口


def test_quote_aggregation_and_type_isolation(case_logger):
    agg = WindowAggregator(1000)
    agg.add(quote("q1", 10, 10.0, 11.0))
    agg.add(quote("q2", 20, 20.0, 22.0))
    agg.add(trade("t1", 10, 100, 5))
    q = agg.query("BTC", 0, 1000)
    qw = q.windows["quote"][0]
    tw = q.windows["trade"][0]
    case_logger("盘口均价", avg_bid=qw.avg_bid, avg_ask=qw.avg_ask,
                last_bid=qw.last_bid, last_ask=qw.last_ask,
                trade_close=tw.close, quote_keys=list(q.windows))
    assert (qw.avg_bid, qw.avg_ask, qw.last_bid, qw.last_ask) == (15.0, 16.5, 20.0, 22.0)
    assert tw.close == 100  # 类型互不串数据


def test_symbol_isolation(case_logger):
    agg = WindowAggregator(1000)
    agg.add(trade("a", 10, 10, sym="BTC"))
    agg.add(trade("b", 10, 50, sym="ETH"))
    btc = agg.query("BTC", 0, 1000).windows["trade"][0]
    eth = agg.query("ETH", 0, 1000).windows["trade"][0]
    case_logger("品种隔离", btc=btc.close, eth=eth.close)
    assert btc.close == 10 and eth.close == 50
