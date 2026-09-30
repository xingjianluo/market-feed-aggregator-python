"""按固定时间窗口聚合单品种事件，并显式标记数据缺口。

规则：
- 窗口按 epoch 对齐：window_start = timestamp_ms // window_size_ms * window_size_ms。
- 每个 (symbol, 事件类型) 独立聚合。
- 缺口：在该 symbol/类型已观测到的时间范围内部（首尾窗口之间），
  任何没有事件落入的窗口都作为 Gap 显式返回，绝不补零或跳过。
- 缺口不会基于查询区间被"凭空"扩展：查询区间端点之外无数据不属于缺口。
"""

from __future__ import annotations

import threading
from dataclasses import replace

from .models import EventType, Gap, MarketEvent, QueryResult, WindowSummary


class _Series:
    """单个 symbol 的所有事件类型聚合状态。"""

    __slots__ = ("windows", "min_start", "max_start")

    def __init__(self) -> None:
        # (EventType) -> {window_start: WindowSummary}
        self.windows: dict[EventType, dict[int, WindowSummary]] = {}
        self.min_start: dict[EventType, int] = {}
        self.max_start: dict[EventType, int] = {}


def _window_start(timestamp_ms: int, window_size_ms: int) -> int:
    return (timestamp_ms // window_size_ms) * window_size_ms


class WindowAggregator:
    def __init__(self, window_size_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        self.window_size_ms = window_size_ms
        self._series: dict[str, _Series] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ ingest
    def add(self, event: MarketEvent) -> None:
        ws = _window_start(event.timestamp_ms, self.window_size_ms)
        we = ws + self.window_size_ms
        with self._lock:
            series = self._series.setdefault(event.symbol, _Series())
            buckets = series.windows.setdefault(event.type, {})
            if ws not in buckets:
                buckets[ws] = WindowSummary(
                    symbol=event.symbol,
                    event_type=event.type,
                    window_start_ms=ws,
                    window_end_ms=we,
                )
                series.min_start[event.type] = min(series.min_start.get(event.type, ws), ws)
                series.max_start[event.type] = max(series.max_start.get(event.type, ws), ws)
            self._fold(buckets[ws], event)

    @staticmethod
    def _fold(win: WindowSummary, e: MarketEvent) -> None:
        win.count += 1
        if e.type is EventType.TRADE:
            assert e.price is not None and e.size is not None
            if win.count == 1:
                win.open = win.high = win.low = win.close = e.price
            else:
                win.high = max(win.high, e.price)
                win.low = min(win.low, e.price)
                win.close = e.price
            win.total_size += e.size
        else:  # QUOTE：时间加权无法实现（快照间时长未知），采用算术平均
            assert e.bid_price is not None and e.ask_price is not None
            n = win.count
            assert e.bid_size is not None and e.ask_size is not None
            if n == 1:
                win.avg_bid = e.bid_price
                win.avg_ask = e.ask_price
            else:
                assert win.avg_bid is not None and win.avg_ask is not None
                win.avg_bid += (e.bid_price - win.avg_bid) / n
                win.avg_ask += (e.ask_price - win.avg_ask) / n
            win.last_bid = e.bid_price
            win.last_ask = e.ask_price
            win.total_size += e.bid_size + e.ask_size

    # ------------------------------------------------------------------- query
    def query(self, symbol: str, start_ms: int, end_ms: int) -> QueryResult:
        """查询 [start_ms, end_ms) 范围内的窗口。返回的是快照副本。"""
        if start_ms < 0 or end_ms < 0:
            raise ValueError("time range must be non-negative")
        if end_ms <= start_ms:
            raise ValueError("end_ms must be greater than start_ms")

        q_start = _window_start(start_ms, self.window_size_ms)
        q_end_excl = (
            _window_start(end_ms - 1, self.window_size_ms) + self.window_size_ms
        )

        result = QueryResult(
            symbol=symbol,
            window_size_ms=self.window_size_ms,
            range_start_ms=start_ms,
            range_end_ms=end_ms,
        )
        with self._lock:
            series = self._series.get(symbol)
            if series is None:
                return result
            for etype, buckets in series.windows.items():
                lo = series.min_start[etype]
                hi = series.max_start[etype]
                # 只遍历"观测范围 ∩ 查询范围"；
                # 观测范围内部缺失的窗口即缺口，查询范围之外的无数据不算缺口。
                first_ws = max(q_start, lo)
                last_ws = min(q_end_excl - self.window_size_ms, hi)
                chosen: dict[int, WindowSummary] = {}
                gap_list: list[Gap] = []
                ws = first_ws
                while ws <= last_ws:
                    existing = buckets.get(ws)
                    if existing is None:
                        gap_list.append(Gap(start_ms=ws, end_ms=ws + self.window_size_ms))
                    else:
                        chosen[ws] = replace(existing)
                    ws += self.window_size_ms
                key = etype.value
                if chosen:
                    result.windows[key] = dict(sorted(chosen.items()))
                if gap_list:
                    result.gaps[key] = gap_list
        return result

    def earliest_window_start(self, symbol: str) -> int | None:
        with self._lock:
            series = self._series.get(symbol)
            if series is None:
                return None
            return min(series.min_start.values())
