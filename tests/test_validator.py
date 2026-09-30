"""校验器测试：字段缺失、乱序字段类型、非法取值、未知类型必须按原因区分拒绝。"""

from __future__ import annotations

import pytest

from marketfeed.errors import RejectReason
from marketfeed.models import EventType
from marketfeed.validator import validate_event


def trade(**over):
    base = {"event_id": "e1", "symbol": "BTC", "type": "trade",
            "timestamp_ms": 1000, "price": 10.5, "size": 2}
    base.update(over)
    return base


def quote(**over):
    base = {"event_id": "q1", "symbol": "BTC", "type": "quote", "timestamp_ms": 1000,
            "bid_price": 10, "bid_size": 1, "ask_price": 10.5, "ask_size": 2}
    base.update(over)
    return base


ACCEPT_CASES = [
    ("trade 正常", trade()),
    ("quote 正常", quote()),
    ("零时刻成交", trade(timestamp_ms=0)),
    ("整数价格", trade(price=1)),
]


@pytest.mark.parametrize("title,raw", ACCEPT_CASES)
def test_accept(title, raw, case_logger):
    event, rejected = validate_event(raw)
    case_logger(title, input=raw, rejected=rejected)
    assert rejected is None and event is not None
    assert event.type in (EventType.TRADE, EventType.QUOTE)


REJECT_CASES = [
    ("非对象", 42, RejectReason.NOT_A_DICT),
    ("缺 event_id", {k: v for k, v in trade().items() if k != "event_id"},
     RejectReason.MISSING_FIELD),
    ("缺 price", {k: v for k, v in trade().items() if k != "price"},
     RejectReason.MISSING_FIELD),
    ("quote 缺 ask_size", {k: v for k, v in quote().items() if k != "ask_size"},
     RejectReason.MISSING_FIELD),
    ("event_id 为数字", trade(event_id=123), RejectReason.BAD_TYPE),
    ("symbol 为空串", trade(symbol=""), RejectReason.BAD_TYPE),
    ("timestamp 为字符串", trade(timestamp_ms="1000"), RejectReason.BAD_TYPE),
    ("timestamp 为布尔", trade(timestamp_ms=True), RejectReason.BAD_TYPE),
    ("price 为布尔", trade(price=True), RejectReason.BAD_TYPE),
    ("未知类型", trade(type="depth"), RejectReason.UNKNOWN_EVENT_TYPE),
    ("负价格", trade(price=-1), RejectReason.INVALID_PRICE),
    ("零价格", trade(price=0), RejectReason.INVALID_PRICE),
    ("NaN 价格", trade(price=float("nan")), RejectReason.INVALID_PRICE),
    ("负量", trade(size=-0.1), RejectReason.INVALID_SIZE),
    ("零成交量", trade(size=0), RejectReason.INVALID_SIZE),
    ("负时间戳", trade(timestamp_ms=-1), RejectReason.INVALID_TIMESTAMP),
]


@pytest.mark.parametrize("title,raw,expected", REJECT_CASES)
def test_reject_reasons(title, raw, expected, case_logger):
    event, rejected = validate_event(raw)
    case_logger(title, input=raw, expect=expected.value,
                got=(rejected.reason.value if rejected else None), detail=rejected.detail if rejected else None)
    assert event is None
    assert rejected is not None
    assert rejected.reason is expected, f"{title}: 期望 {expected} 实际 {rejected.reason}"
