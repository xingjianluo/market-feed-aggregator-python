"""事件与聚合结果的数据模型、类型与拒绝原因枚举。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EventType(str, Enum):
    """支持的行情事件类型。"""

    TRADE = "trade"   # 成交通知
    QUOTE = "quote"   # 盘口快照/变化


class RejectReason(str, Enum):
    """事件被拒绝的原因分类（必须可区分，不能静默丢弃）。"""

    NOT_A_OBJECT = "not_an_object"          # 载荷不是对象
    MISSING_FIELD = "missing_field"         # 必填字段缺失
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    BAD_TIMESTAMP = "bad_timestamp"         # 时间戳缺失/类型错/非有限
    TIMESTAMP_OUT_OF_ORDER = "timestamp_out_of_order"  # 早于当前会话水位
    BAD_SYMBOL = "bad_symbol"               # 标的代码非法
    BAD_PRICE = "bad_price"                 # 价格类型错/非正
    BAD_SIZE = "bad_size"                   # 数量类型错/非正
    BAD_FIELD_TYPE = "bad_field_type"       # 其他字段类型不对
    BAD_SIDE = "bad_side"                   # trade.side 非法
    EMPTY_BOOK = "empty_book"               # quote 盘口买卖盘全空
    INCONSISTENT_LEVELS = "inconsistent_levels"  # bid/ask 条目结构非法


class WindowStatus(str, Enum):
    """聚合窗口状态。"""

    OPEN = "open"        # 当前仍在累积的窗口
    CLOSED = "closed"    # 已封口（水位越过窗口结束）
    GAP = "gap"          # 已封口区间但没有任何事件，且落在数据缺口上
    NO_DATA = "no_data"  # 未来窗口/查询范围超出已知数据


@dataclass(frozen=True, slots=True)
class MarketEvent:
    """归一化后的行情事件。"""

    event_id: str
    event_type: EventType
    symbol: str
    timestamp_ms: int
    price: float
    size: float
    side: str | None = None
    bids: tuple[tuple[float, float], ...] = ()
    asks: tuple[tuple[float, float], ...] = ()


@dataclass(slots=True)
class TradeStats:
    count: int = 0
    volume: float = 0.0
    notional: float = 0.0
    high: float | None = None
    low: float | None = None
    last_price: float | None = None
    buy_count: int = 0
    sell_count: int = 0


@dataclass(slots=True)
class QuoteStats:
    count: int = 0
    best_bid: float | None = None
    best_ask: float | None = None
    mid_price_sum: float = 0.0


@dataclass(slots=True)
class WindowResult:
    """单个 (symbol, 时间窗口) 的可查询聚合结果。"""

    symbol: str
    window_start_ms: int
    window_end_ms: int
    status: WindowStatus
    watermark_ms: int
    trades: TradeStats = field(default_factory=TradeStats)
    quotes: QuoteStats = field(default_factory=QuoteStats)
    event_ids: list[str] = field(default_factory=list)
    gap_from_ms: int | None = None
    gap_to_ms: int | None = None

    def to_dict(self) -> dict:
        def _level(levels: tuple[tuple[float, float], ...]) -> list[list[float]]:
            return [[float(p), float(q)] for p, q in levels]

        return {
            "symbol": self.symbol,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "status": self.status.value,
            "watermark_ms": self.watermark_ms,
            "trades": {
                "count": self.trades.count,
                "buy_count": self.trades.buy_count,
                "sell_count": self.trades.sell_count,
                "volume": self.trades.volume,
                "notional": self.trades.notional,
                "high": self.trades.high,
                "low": self.trades.low,
                "last_price": self.trades.last_price,
            },
            "quotes": {
                "count": self.quotes.count,
                "best_bid": self.quotes.best_bid,
                "best_ask": self.quotes.best_ask,
                "mid_price_avg": (
                    self.quotes.mid_price_sum / self.quotes.count
                    if self.quotes.count
                    else None
                ),
            },
            "event_count": len(self.event_ids),
            "event_ids": list(self.event_ids),
            "gap_from_ms": self.gap_from_ms,
            "gap_to_ms": self.gap_to_ms,
        }
