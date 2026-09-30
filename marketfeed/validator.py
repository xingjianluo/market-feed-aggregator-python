"""把原始 dict 事件校验并归一化为 MarketEvent。

拒绝原因区分（见 RejectReason）：
- not_a_dict        输入不是对象
- missing_field     必填字段缺失
- bad_type          字段类型不对 / 空字符串
- unknown_event_type type 取值不在支持范围内
- invalid_price     价格非有限正数
- invalid_size      量非有限非负数（成交量须为正）
- invalid_timestamp 时间戳非法
"""

from __future__ import annotations

import math
from typing import Any

from .errors import RejectReason
from .models import EventType, IngestResult, IngestStatus, MarketEvent

_COMMON_FIELDS = ("event_id", "symbol", "type", "timestamp_ms")
_TRADE_FIELDS = ("price", "size")
_QUOTE_FIELDS = ("bid_price", "bid_size", "ask_price", "ask_size")


def _reject(reason: RejectReason, detail: str, event_id: str | None = None) -> IngestResult:
    return IngestResult(
        status=IngestStatus.REJECTED, reason=reason, detail=detail, event_id=event_id
    )


def _is_number(v: Any) -> bool:
    """真正的数值类型：int/float，排除 bool（含 NaN/Inf，有限性另行判定）。"""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_real_number(v: Any) -> bool:
    """有限的数值。"""
    return _is_number(v) and math.isfinite(v)


def _check_price(value: Any, name: str) -> IngestResult | None:
    if not _is_number(value):
        return _reject(RejectReason.BAD_TYPE, f"{name} must be a number")
    if not math.isfinite(value) or value <= 0:
        return _reject(RejectReason.INVALID_PRICE, f"{name} must be a finite positive number")
    return None


def _check_size(value: Any, name: str, *, positive: bool) -> IngestResult | None:
    if not _is_number(value):
        return _reject(RejectReason.BAD_TYPE, f"{name} must be a number")
    requirement = "positive" if positive else "non-negative"
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        return _reject(RejectReason.INVALID_SIZE, f"{name} must be a finite {requirement} number")
    return None


def validate_event(raw: object) -> tuple[MarketEvent | None, IngestResult | None]:
    """返回 (事件, None) 表示通过；返回 (None, 拒绝结果) 表示拒绝。"""
    if not isinstance(raw, dict):
        return None, _reject(RejectReason.NOT_A_DICT, "event must be a JSON object")

    event_id = raw.get("event_id")
    if isinstance(event_id, str):
        eid: str | None = event_id
    else:
        eid = None

    # 1) 公共必填字段存在性
    for f in _COMMON_FIELDS:
        if f not in raw:
            return None, _reject(RejectReason.MISSING_FIELD, f"missing field: {f}", eid)

    # 2) 字符串字段类型
    if not isinstance(event_id, str) or not event_id:
        return None, _reject(RejectReason.BAD_TYPE, "event_id must be a non-empty string")
    symbol = raw["symbol"]
    if not isinstance(symbol, str) or not symbol:
        return None, _reject(RejectReason.BAD_TYPE, "symbol must be a non-empty string", eid)

    # 3) 事件类型
    raw_type = raw["type"]
    if not isinstance(raw_type, str):
        return None, _reject(RejectReason.BAD_TYPE, "type must be a string", eid)
    try:
        event_type = EventType(raw_type)
    except ValueError:
        return None, _reject(
            RejectReason.UNKNOWN_EVENT_TYPE, f"unsupported type: {raw_type!r}", eid
        )

    # 4) 时间戳：整数、非负（bool 被视为类型错误）
    ts = raw["timestamp_ms"]
    if isinstance(ts, bool) or not isinstance(ts, int):
        return None, _reject(RejectReason.BAD_TYPE, "timestamp_ms must be an integer", eid)
    if ts < 0:
        return None, _reject(RejectReason.INVALID_TIMESTAMP, "timestamp_ms must be >= 0", eid)

    # 5) 类型专有字段
    if event_type is EventType.TRADE:
        for f in _TRADE_FIELDS:
            if f not in raw:
                return None, _reject(RejectReason.MISSING_FIELD, f"missing field: {f}", eid)
        if err := _check_price(raw["price"], "price"):
            return None, IngestResult(
                status=IngestStatus.REJECTED, reason=err.reason, detail=err.detail, event_id=eid
            )
        if err := _check_size(raw["size"], "size", positive=True):
            return None, IngestResult(
                status=IngestStatus.REJECTED, reason=err.reason, detail=err.detail, event_id=eid
            )
        return (
            MarketEvent(
                event_id=event_id,
                symbol=symbol,
                type=event_type,
                timestamp_ms=ts,
                price=float(raw["price"]),
                size=float(raw["size"]),
            ),
            None,
        )

    # QUOTE
    for f in _QUOTE_FIELDS:
        if f not in raw:
            return None, _reject(RejectReason.MISSING_FIELD, f"missing field: {f}", eid)
    for name in ("bid_price", "ask_price"):
        if err := _check_price(raw[name], name):
            return None, IngestResult(
                status=IngestStatus.REJECTED, reason=err.reason, detail=err.detail, event_id=eid
            )
    for name in ("bid_size", "ask_size"):
        if err := _check_size(raw[name], name, positive=True):
            return None, IngestResult(
                status=IngestStatus.REJECTED, reason=err.reason, detail=err.detail, event_id=eid
            )
    return (
        MarketEvent(
            event_id=event_id,
            symbol=symbol,
            type=event_type,
            timestamp_ms=ts,
            bid_price=float(raw["bid_price"]),
            bid_size=float(raw["bid_size"]),
            ask_price=float(raw["ask_price"]),
            ask_size=float(raw["ask_size"]),
        ),
        None,
    )
