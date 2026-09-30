"""校验拒绝原因分类测试。

每条用例都打印：输入 -> 判定依据（检查顺序/规则）-> 最终结果（接受或拒绝原因），
失败断言会带上实际原因，不同原因严格区分。
"""
from __future__ import annotations

import pytest

from market_feed.errors import ValidationError
from market_feed.models import EventType
from market_feed.service import validate_event

from ._logs import log_case


def trade(**over) -> dict:
    base = {
        "event_id": "t1",
        "event_type": "trade",
        "symbol": "BTCUSDT",
        "timestamp_ms": 1000,
        "price": 65000.5,
        "size": 0.1,
        "side": "buy",
    }
    base.update(over)
    return base


def quote(**over) -> dict:
    base = {
        "event_id": "q1",
        "event_type": "quote",
        "symbol": "BTCUSDT",
        "timestamp_ms": 1000,
        "bids": [[65000.0, 0.5]],
        "asks": [[65001.0, 0.4]],
    }
    base.update(over)
    return base


REJECT_CASES = [
    ("载荷不是对象", ["not", "an", "object"], 0, "not_an_object"),
    ("载荷是数字", 123, 0, "not_an_object"),
    ("缺少公共字段 event_id", {"event_type": "trade"}, 0, "missing_field"),
    ("trade 缺少 price", trade(price=None) if False else {
        k: v for k, v in trade().items() if k != "price"}, 0, "missing_field"),
    ("未知事件类型", trade(event_type="depth"), 0, "unknown_event_type"),
    ("event_id 为空串", trade(event_id="   "), 0, "bad_field_type"),
    ("event_id 是数字", trade(event_id=1), 0, "bad_field_type"),
    ("symbol 为空", trade(symbol="  "), 0, "bad_symbol"),
    ("symbol 类型错", trade(symbol=60000), 0, "bad_symbol"),
    ("timestamp 是字符串", trade(timestamp_ms="1000"), 0, "bad_timestamp"),
    ("timestamp 是小数", trade(timestamp_ms=1000.5), 0, "bad_timestamp"),
    ("timestamp 为负", trade(timestamp_ms=-1), 0, "bad_timestamp"),
    ("timestamp 为 NaN", trade(timestamp_ms=float("nan")), 0, "bad_timestamp"),
    ("时间戳早于水位=乱序", trade(timestamp_ms=999), 1000, "timestamp_out_of_order"),
    ("布尔不能当价格", trade(price=True), 0, "bad_price"),
    ("价格为零", trade(price=0), 0, "bad_price"),
    ("价格为负", trade(price=-1), 0, "bad_price"),
    ("价格是 Inf", trade(price=float("inf")), 0, "bad_price"),
    ("数量为负", trade(size=-0.1), 0, "bad_size"),
    ("side 非法", trade(side="long"), 0, "bad_side"),
    ("quote 买卖盘全空", quote(bids=[], asks=[]), 0, "empty_book"),
    ("quote 盘口条目结构错", quote(bids=[65000], asks=[]), 0, "inconsistent_levels"),
    ("quote 档位不是二维", quote(bids=[[65000]], asks=[]), 0, "inconsistent_levels"),
    ("quote 档位价格非正", quote(bids=[[0, 0.5]], asks=[]), 0, "inconsistent_levels"),
    ("quote bids 不是列表", quote(bids="65000", asks=[]), 0, "inconsistent_levels"),
    ("quote 缺少 asks", {k: v for k, v in quote().items() if k != "asks"}, 0, "missing_field"),
    ("乱序判定优先于类型专属字段", trade(timestamp_ms=1, price=-1), 1000,
     "timestamp_out_of_order"),
]


@pytest.mark.parametrize("title,raw,watermark,expect_reason", REJECT_CASES)
def test_reject_reasons(title, raw, watermark, expect_reason):
    with pytest.raises(ValidationError) as exc_info:
        validate_event(raw, watermark)
    reason = exc_info.value.reason
    log_case(title, raw, f"watermark={watermark}; 期望原因={expect_reason}",
             f"REJECTED reason={reason}")
    assert reason == expect_reason, f"{title}: 期望 {expect_reason}, 实际 {reason}"


def test_valid_trade_accepted():
    event = validate_event(trade(), 0)
    log_case("合法成交", trade(), "字段齐全且取值合法",
             f"ACCEPTED type={event.event_type.value} price={event.price}")
    assert event.event_type is EventType.TRADE
    assert event.symbol == "BTCUSDT"
    assert event.side == "buy"


def test_numeric_string_fields_accepted():
    # 交易所常见的字符串数字应被接受，但布尔/非数字字符串仍然拒绝
    raw = trade(price="65000.50", size="0.10")
    event = validate_event(raw, 0)
    log_case("字符串数字价格/数量", raw, "可解析为有限正浮点数",
             f"ACCEPTED price={event.price} size={event.size}")
    assert event.price == 65000.5 and event.size == 0.1

    for bad in ("abc", "", "NaN"):
        with pytest.raises(ValidationError) as exc:
            validate_event(trade(price=bad), 0)
        log_case("非数字字符串价格", bad, "无法解析为有限正数 -> 按 price 字段拒绝",
                 f"REJECTED reason={exc.value.reason}")
        assert exc.value.reason == "bad_price"


def test_out_of_order_boundary():
    # 恰好等于水位不算乱序；早 1ms 算乱序
    assert validate_event(trade(timestamp_ms=1000), 1000).timestamp_ms == 1000
    with pytest.raises(ValidationError) as exc:
        validate_event(trade(timestamp_ms=999), 1000)
    log_case("乱序边界", {"ts": 999, "watermark": 1000},
             "ts < watermark 即乱序", f"REJECTED reason={exc.value.reason}")
    assert exc.value.reason == "timestamp_out_of_order"


def test_allowed_lateness():
    event = validate_event(trade(timestamp_ms=900), 1000, allowed_lateness_ms=100)
    log_case("容忍迟到 100ms", {"ts": 900, "watermark": 1000, "lateness": 100},
             "ts + lateness >= watermark", f"ACCEPTED ts={event.timestamp_ms}")
    assert event.timestamp_ms == 900
