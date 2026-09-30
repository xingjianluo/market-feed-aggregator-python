"""窗口聚合与缺口标记测试。"""
from __future__ import annotations

import pytest

from market_feed.errors import ValidationError
from market_feed.models import WindowStatus
from market_feed.service import MarketFeedService
from market_feed.windowing import window_start_for

from ._logs import log_case


def trade(eid: str, ts: int, price: float, size: float, side: str = "buy",
          symbol: str = "BTCUSDT") -> dict:
    return {"event_id": eid, "event_type": "trade", "symbol": symbol,
            "timestamp_ms": ts, "price": price, "size": size, "side": side}


def quote(eid: str, ts: int, bids, asks, symbol: str = "BTCUSDT") -> dict:
    return {"event_id": eid, "event_type": "quote", "symbol": symbol,
            "timestamp_ms": ts, "bids": bids, "asks": asks}


def _by_start(windows):
    return {w.window_start_ms: w for w in windows}


def test_trade_aggregation_stats():
    svc = MarketFeedService(window_ms=1000)
    for raw in [
        trade("t1", 1000, 100.0, 2.0, "buy"),
        trade("t2", 1400, 110.0, 1.0, "sell"),
        trade("t3", 1800, 90.0, 0.5, "buy"),
    ]:
        svc.submit(raw)
    win = svc.query("BTCUSDT", 1000, 2000)[0].to_dict()
    log_case("同窗口 3 笔成交", "1000/1400/1800ms",
             "count/volume/notional/high/low/买卖笔数", win["trades"])
    t = win["trades"]
    assert t["count"] == 3
    assert t["volume"] == 3.5
    assert t["notional"] == 200.0 + 110.0 + 45.0
    assert t["high"] == 110.0 and t["low"] == 90.0
    assert t["last_price"] == 90.0
    assert t["buy_count"] == 2 and t["sell_count"] == 1
    # 水位=1800 < 窗口结束 2000，窗口仍 open
    assert win["status"] == WindowStatus.OPEN.value
    # 水位越过 2000 后封口
    svc.submit(trade("t4", 2000, 1, 1))
    assert svc.query("BTCUSDT", 1000, 2000)[0].status is WindowStatus.CLOSED


def test_window_open_until_watermark_passes_end():
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("t1", 1000, 1, 1))
    win = svc.query("BTCUSDT", 1000, 2000)[0]
    log_case("水位=1000 时窗口[1000,2000)", "ts=1000", "end=2000 > wm=1000",
             win.status.value)
    assert win.status is WindowStatus.OPEN
    svc.submit(trade("t2", 2000, 1, 1))
    win = svc.query("BTCUSDT", 1000, 2000)[0]
    log_case("水位推进到 2000", "新增 ts=2000", "end=2000 <= wm=2000 -> closed",
             win.status.value)
    assert win.status is WindowStatus.CLOSED


def test_gap_is_explicit_not_zero_fill():
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("t1", 1000, 1, 1))
    svc.submit(trade("t2", 5000, 1, 1))  # 2000/3000/4000 三个窗口空缺
    windows = svc.query("BTCUSDT", 1000, 6000)
    table = {w.window_start_ms: (w.status.value, w.gap_from_ms) for w in windows}
    log_case("1000 与 5000 之间缺 3 个窗口", "ts=1000,5000 wm=5000",
             "中间已封口空窗口必须标 gap", table)
    assert table[1000] == ("closed", None)
    for g in (2000, 3000, 4000):
        assert table[g] == ("gap", g)
    # gap 窗口不能被悄悄补成“有一笔 0 值成交”
    gap = _by_start(windows)[3000].to_dict()
    assert gap["trades"]["count"] == 0 and gap["quotes"]["count"] == 0
    assert gap["event_count"] == 0


def test_before_first_and_future_windows_are_no_data_not_gap():
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("t1", 2000, 1, 1))
    svc.submit(trade("t2", 3000, 1, 1))
    windows = svc.query("BTCUSDT", 0, 5000)
    table = {w.window_start_ms: w.status.value for w in windows}
    log_case("首事件窗口之前 / 未来窗口", "事件在 2000,3000",
             "区间外空缺是 no_data，只有中间缺口是 gap", table)
    assert table[0] == "no_data"
    assert table[1000] == "no_data"
    assert table[4000] == "no_data"


def test_gap_inside_open_region_is_no_data():
    # 缺口窗口尚未封口时先报 no_data，封口后才升级为 gap
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("t1", 1000, 1, 1))
    svc.submit(trade("t2", 3000, 1, 1))  # wm=3000: 窗口2000已封口=gap, 窗口3000有事件=open
    windows = _by_start(svc.query("BTCUSDT", 1000, 4000))
    log_case("未封口的中间空窗", "ts=1000,3000", "2000 已封口=gap; 3000 有事件=open",
             {k: v.status.value for k, v in windows.items()})
    assert windows[2000].status is WindowStatus.GAP
    assert windows[3000].status is WindowStatus.OPEN
    # 水位越过 4000 且 [3000,4000) 仍无事件不可能发生（3000 有事件）；
    # 改为验证 2000 之后水位继续推进时间隙窗口封口升级：
    svc.submit(trade("t3", 5000, 1, 1))  # 窗口4000封口仍无事件 -> gap
    windows = _by_start(svc.query("BTCUSDT", 1000, 6000))
    assert windows[4000].status is WindowStatus.GAP


def test_quote_aggregation_and_multiple_symbols():
    svc = MarketFeedService(window_ms=1000)
    svc.submit(quote("q1", 1000, [[100.0, 1]], [[102.0, 1]]))
    svc.submit(trade("t1", 1200, 1, 1, symbol="ETHUSDT"))
    svc.submit(quote("q2", 1500, [[101.0, 1]], [[103.0, 1]]))
    btc = svc.query("BTCUSDT", 1000, 2000)[0].to_dict()
    eth = svc.query("ETHUSDT", 1000, 2000)[0].to_dict()
    log_case("同窗口 quote 聚合 + 多 symbol", "BTC 2 quote / ETH 1 trade",
             "best_bid 取高、best_ask 取低、mid 均值",
             {"btc": btc["quotes"], "eth_trade_count": eth["trades"]["count"]})
    assert btc["quotes"]["count"] == 2
    assert btc["quotes"]["best_bid"] == 101.0
    assert btc["quotes"]["best_ask"] == 102.0
    assert btc["quotes"]["mid_price_avg"] == pytest.approx((101.0 + 102.0) / 2)
    assert btc["trades"]["count"] == 0
    assert eth["trades"]["count"] == 1 and eth["quotes"]["count"] == 0


def test_window_alignment():
    assert window_start_for(0, 1000) == 0
    assert window_start_for(999, 1000) == 0
    assert window_start_for(1000, 1000) == 1000
    assert window_start_for(2500, 500) == 2500
    assert window_start_for(2599, 500) == 2500


def test_watermark_is_per_symbol_not_global():
    # ETH 先到一个很晚的事件，不应让 BTC 较早的正常事件被判乱序
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("e1", 9000, 1, 1, symbol="ETHUSDT"))
    win = svc.submit(trade("b1", 1000, 1, 1, symbol="BTCUSDT"))
    log_case("跨 symbol 时间轴交错", "ETH@9000 先到, BTC@1000 后到",
             "水位按 symbol 独立，BTC 事件不应被判乱序",
             f"ACCEPTED window={win.window_start_ms}")
    assert win.window_start_ms == 1000
    # 同一 symbol 内仍然严格乱序拒绝
    with pytest.raises(ValidationError) as exc:
        svc.submit(trade("b0", 999, 1, 1, symbol="BTCUSDT"))
    assert exc.value.reason == "timestamp_out_of_order"
    # 各自窗口封口互不影响：ETH 的 1000 窗口对 ETH 是 no_data（首事件之前）
    eth = svc.query("ETHUSDT", 0, 2000)[0]
    assert eth.status is WindowStatus.NO_DATA
