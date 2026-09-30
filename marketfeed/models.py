"""统一行情事件与聚合结果的类型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .errors import RejectReason


class EventType(str, Enum):
    """支持的行情事件类型。"""

    TRADE = "trade"   # 成交
    QUOTE = "quote"   # 盘口快照（最优买卖价量）


class IngestStatus(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class MarketEvent:
    """归一化后的行情事件。

    对成交：price/size 为成交价与成交量，bid/ask 为空。
    对盘口：price/size 为空，bid_price/bid_size/ask_price/ask_size 必填。
    """

    event_id: str
    symbol: str
    type: EventType
    timestamp_ms: int
    price: float | None = None
    size: float | None = None
    bid_price: float | None = None
    bid_size: float | None = None
    ask_price: float | None = None
    ask_size: float | None = None


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    reason: RejectReason | None = None
    detail: str | None = None
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class Gap:
    """聚合区间内的显式缺口：该时间范围内没有任何事件到达。"""

    start_ms: int
    end_ms: int


@dataclass(slots=True)
class WindowSummary:
    symbol: str
    event_type: EventType
    window_start_ms: int
    window_end_ms: int
    count: int = 0
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    total_size: float = 0.0
    # 盘口专用
    avg_bid: float | None = None
    avg_ask: float | None = None
    last_bid: float | None = None
    last_ask: float | None = None


@dataclass(slots=True)
class QueryResult:
    symbol: str
    window_size_ms: int
    range_start_ms: int
    range_end_ms: int
    # key 为事件类型字符串，value 为 window_start_ms -> 窗口汇总
    windows: dict[str, dict[int, WindowSummary]] = field(default_factory=dict)
    # key 为事件类型字符串，value 为观测区间内部的显式缺口
    gaps: dict[str, list[Gap]] = field(default_factory=dict)
    # True 表示查询区间触及了因保留策略已被淘汰的旧数据
    evicted: bool = False
