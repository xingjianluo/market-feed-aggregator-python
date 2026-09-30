"""事件存储：去重、水位推进、窗口保留与并发控制。"""
from __future__ import annotations

import copy
import threading

from .errors import (
    DuplicateEventError,
    ExpiredRequestError,
    OverCapacityError,
    ValidationError,
)
from .models import MarketEvent, WindowResult
from .windowing import WindowAggregator, window_start_for


class EventStore:
    """线程安全的接入与聚合核心。

    一致性策略：
      * 内部一把可重入锁保护去重表、水位与聚合器；
      * 水位按 symbol 独立维护（一个标的的乱序/进度不影响另一个标的）；
      * 写入在锁内完成“去重 -> 入窗 -> 推进该 symbol 水位 -> 封口 -> 淘汰”，
        保证并发写入不会重复累计，读到的都是完整提交后的快照；
      * 查询在锁内深拷贝窗口结果，读取期间写入继续进行也不会读到半成品；
      * 接入用有界许可做背压，拿不到许可抛 OverCapacityError；
      * 请求命中该 symbol 已淘汰区间抛 ExpiredRequestError，与超载明确区分。
    """

    def __init__(
        self,
        window_ms: int = 1000,
        retention_windows: int = 60,
        max_inflight_writes: int = 1024,
        allowed_lateness_ms: int = 0,
    ) -> None:
        if window_ms <= 0:
            raise ValueError("window_ms must be positive")
        if retention_windows <= 0:
            raise ValueError("retention_windows must be positive")
        self.window_ms = window_ms
        self.retention_windows = retention_windows
        self.allowed_lateness_ms = max(0, allowed_lateness_ms)
        self._aggregator = WindowAggregator(window_ms)
        self._seen: dict[str, tuple[str, int]] = {}  # event_id -> (symbol, 窗口起点)
        self._watermarks: dict[str, int] = {}        # 每个 symbol 独立水位
        self._lock = threading.RLock()
        self._permits = threading.BoundedSemaphore(max_inflight_writes)

    def is_duplicate(self, event_id: object) -> bool:
        """event_id 是否已被接收（身份判定优先于时间戳乱序判定）。"""
        with self._lock:
            return isinstance(event_id, str) and event_id in self._seen

    def ingest(self, event: MarketEvent) -> WindowResult:
        """计入一个已归一化事件；重复抛 DuplicateEventError。调用方持有许可。"""
        with self._lock:
            if event.event_id in self._seen:
                raise DuplicateEventError(
                    f"event_id {event.event_id!r} already accepted; each event counts once"
                )
            win = self._aggregator.add(event)
            self._seen[event.event_id] = (event.symbol, win.window_start_ms)
            cur = self._watermarks.get(event.symbol, 0)
            if event.timestamp_ms > cur:
                self._watermarks[event.symbol] = event.timestamp_ms
            self._aggregator.close_up_to(self._watermarks)
            self._prune()
            return copy.deepcopy(win)

    def acquire_permit(self) -> None:
        """非阻塞获取接入许可，失败说明已超承载能力。"""
        if not self._permits.acquire(blocking=False):
            raise OverCapacityError(
                "ingestion at capacity: too many in-flight writes, back off and retry"
            )

    def release_permit(self) -> None:
        self._permits.release()

    def _cutoff_for(self, symbol: str, wm: int) -> int:
        """该 symbol 当前仍保留的最早窗口起点。"""
        current_origin = window_start_for(wm, self.window_ms)
        return current_origin - (self.retention_windows - 1) * self.window_ms

    def _cutoffs(self) -> dict[str, int]:
        return {s: self._cutoff_for(s, wm) for s, wm in self._watermarks.items()}

    def _prune(self) -> None:
        cutoffs = self._cutoffs()
        if not any(c > 0 for c in cutoffs.values()):
            return
        self._aggregator.prune_before(cutoffs)
        dead = [
            eid for eid, (symbol, start) in self._seen.items()
            if start < cutoffs.get(symbol, 0)
        ]
        for eid in dead:
            del self._seen[eid]

    def query(self, symbol: str, from_ms: int, to_ms: int) -> list[WindowResult]:
        """查询 [from_ms, to_ms) 的聚合窗口快照。"""
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("bad_symbol", "symbol must be a non-empty string")
        if not isinstance(from_ms, int) or isinstance(from_ms, bool) or from_ms < 0:
            raise ValidationError("bad_timestamp", "from_ms must be a non-negative integer")
        if not isinstance(to_ms, int) or isinstance(to_ms, bool) or to_ms < 0:
            raise ValidationError("bad_timestamp", "to_ms must be a non-negative integer")
        if from_ms > to_ms:
            raise ValidationError("bad_timestamp", "from_ms must be <= to_ms")

        with self._lock:
            wm = self._watermarks.get(symbol)
            requested_origin = window_start_for(from_ms, self.window_ms)
            if wm is not None and requested_origin < self._cutoff_for(symbol, wm):
                cutoff = self._cutoff_for(symbol, wm)
                raise ExpiredRequestError(
                    f"window starting {requested_origin} has expired; "
                    f"earliest retained window starts at {cutoff}"
                )
            windows = self._aggregator.query_range(
                symbol, from_ms, to_ms, wm if wm is not None else 0
            )
            return copy.deepcopy(windows)

    def watermark(self, symbol: str | None = None) -> int:
        """指定 symbol 的水位；不给定则返回所有 symbol 中的最大水位。"""
        with self._lock:
            if symbol is not None:
                return self._watermarks.get(symbol, 0)
            return max(self._watermarks.values(), default=0)

    def accepted_event_ids(self) -> set[str]:
        with self._lock:
            return set(self._seen)

    def retained_cutoff_ms(self, symbol: str) -> int:
        with self._lock:
            return self._cutoff_for(symbol, self._watermarks.get(symbol, 0))
