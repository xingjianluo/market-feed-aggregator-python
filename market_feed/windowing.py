"""时间窗口的分配、聚合与缺口标记。

窗口按固定长度 window_ms 对齐到 epoch 网格：[k*w, (k+1)*w)。
缺口语义（对每个 symbol 独立判定）：
  * 该 symbol 首个有事件窗口之前的空窗口 -> no_data（还没有数据）
  * 首/末事件窗口之间已封口但无事件的窗口 -> gap（显式数据缺口，不补零）
  * 未来窗口（起点 >= 水位）              -> no_data
窗口起点早于保留下界 -> 已被淘汰，查询由 store 抛 ExpiredRequestError。
"""
from __future__ import annotations

import threading

from .models import (
    MarketEvent,
    QuoteStats,
    TradeStats,
    WindowResult,
    WindowStatus,
)


def align_floor(timestamp_ms: int, window_ms: int) -> int:
    """时间戳向下对齐到窗口网格。"""
    return (timestamp_ms // window_ms) * window_ms


def window_start_for(timestamp_ms: int, window_ms: int) -> int:
    """返回事件时间所属窗口的起点（对齐到 window_ms 网格）。"""
    return align_floor(timestamp_ms, window_ms)


def is_closed(window_end_ms: int, watermark_ms: int) -> bool:
    """窗口结束时间 <= 水位即视为封口。"""
    return window_end_ms <= watermark_ms


def _fold_trade(stats: TradeStats, event: MarketEvent) -> None:
    stats.count += 1
    stats.volume += event.size
    stats.notional += event.price * event.size
    stats.high = event.price if stats.high is None else max(stats.high, event.price)
    stats.low = event.price if stats.low is None else min(stats.low, event.price)
    stats.last_price = event.price
    if event.side == "buy":
        stats.buy_count += 1
    elif event.side == "sell":
        stats.sell_count += 1


def _fold_quote(stats: QuoteStats, event: MarketEvent) -> None:
    stats.count += 1
    if event.bids:
        bid_px = event.bids[0][0]
        stats.best_bid = bid_px if stats.best_bid is None else max(stats.best_bid, bid_px)
    if event.asks:
        ask_px = event.asks[0][0]
        stats.best_ask = ask_px if stats.best_ask is None else min(stats.best_ask, ask_px)
    if event.bids and event.asks:
        stats.mid_price_sum += (event.bids[0][0] + event.asks[0][0]) / 2.0


class WindowAggregator:
    """全部 symbol 的窗口聚合器：(symbol, window_start) -> WindowResult。

    仅为收到过事件的窗口物化结果；gap/no_data 窗口在查询时按规则合成。
    """

    def __init__(self, window_ms: int) -> None:
        if window_ms <= 0:
            raise ValueError("window_ms must be positive")
        self.window_ms = window_ms
        self._windows: dict[tuple[str, int], WindowResult] = {}
        self._first_index: dict[str, int] = {}
        self._last_index: dict[str, int] = {}
        self._lock = threading.RLock()

    def add(self, event: MarketEvent) -> WindowResult:
        """把已通过校验与去重的事件计入对应窗口，返回该窗口结果。"""
        with self._lock:
            start = window_start_for(event.timestamp_ms, self.window_ms)
            idx = start // self.window_ms
            key = (event.symbol, start)
            win = self._windows.get(key)
            if win is None:
                win = WindowResult(
                    symbol=event.symbol,
                    window_start_ms=start,
                    window_end_ms=start + self.window_ms,
                    status=WindowStatus.OPEN,
                    watermark_ms=0,
                )
                self._windows[key] = win
                prev = self._first_index.get(event.symbol)
                self._first_index[event.symbol] = idx if prev is None else min(prev, idx)
                cur = self._last_index.get(event.symbol)
                self._last_index[event.symbol] = idx if cur is None else max(cur, idx)
            win.event_ids.append(event.event_id)
            if event.event_type.value == "trade":
                _fold_trade(win.trades, event)
            else:
                _fold_quote(win.quotes, event)
            return win

    def close_up_to(self, watermarks: dict[str, int]) -> None:
        """推进水位：各 symbol 结束时间 <= 该 symbol 水位的窗口标记为 closed。"""
        with self._lock:
            for win in self._windows.values():
                wm = watermarks.get(win.symbol, 0)
                win.watermark_ms = wm
                if win.status is WindowStatus.OPEN and is_closed(win.window_end_ms, wm):
                    win.status = WindowStatus.CLOSED

    def _empty_window(self, symbol: str, start: int, status: WindowStatus, wm: int) -> WindowResult:
        win = WindowResult(
            symbol=symbol,
            window_start_ms=start,
            window_end_ms=start + self.window_ms,
            status=status,
            watermark_ms=wm,
        )
        if status is WindowStatus.GAP:
            win.gap_from_ms = start
            win.gap_to_ms = start + self.window_ms
        return win

    def get(self, symbol: str, window_start_ms: int, watermark_ms: int) -> WindowResult | None:
        """取单个窗口；不存在的网格窗口按 gap/no_data 规则合成。"""
        with self._lock:
            key = (symbol, window_start_ms)
            win = self._windows.get(key)
            if win is not None:
                return win
            return self._synthesize(symbol, window_start_ms // self.window_ms, watermark_ms)

    def _synthesize(self, symbol: str, idx: int, wm: int) -> WindowResult | None:
        start = idx * self.window_ms
        first = self._first_index.get(symbol)
        last = self._last_index.get(symbol)
        if first is None:
            # 该 symbol 从未出现：不臆造结果
            return None
        if idx < first or idx > last:
            status = WindowStatus.NO_DATA
        elif not is_closed(start + self.window_ms, wm):
            # 落在已知事件区间内但尚无事件、且尚未封口：
            # 不臆造“在途数据”，按无数据处理，封口后升级为 gap
            status = WindowStatus.NO_DATA
        else:
            status = WindowStatus.GAP
        return self._empty_window(symbol, start, status, wm)

    def query_range(
        self, symbol: str, from_ms: int, to_ms: int, watermark_ms: int
    ) -> list[WindowResult]:
        """返回与 [from_ms, to_ms) 相交的所有网格窗口，缺口显式标注。"""
        with self._lock:
            start_idx = align_floor(from_ms, self.window_ms) // self.window_ms
            end_bound = align_floor(to_ms - 1, self.window_ms) if to_ms > from_ms else from_ms
            end_idx = end_bound // self.window_ms
            out: list[WindowResult] = []
            for idx in range(start_idx, end_idx + 1):
                start = idx * self.window_ms
                win = self._windows.get((symbol, start))
                out.append(win if win is not None else self._synthesize(symbol, idx, watermark_ms))
            return [w for w in out if w is not None]

    def known_symbols(self) -> list[str]:
        with self._lock:
            return sorted(self._first_index)

    def prune_before(self, cutoffs: dict[str, int]) -> None:
        """淘汰起点早于该 symbol 保留下界的物化窗口与区间标记。"""
        with self._lock:
            dead = [
                k for k in self._windows
                if k[1] < cutoffs.get(k[0], 0)
            ]
            for k in dead:
                del self._windows[k]
            for symbol in list(self._first_index):
                cutoff_idx = cutoffs.get(symbol, 0) // self.window_ms
                if self._last_index[symbol] < cutoff_idx:
                    del self._first_index[symbol]
                    del self._last_index[symbol]
                elif self._first_index[symbol] < cutoff_idx:
                    self._first_index[symbol] = cutoff_idx
