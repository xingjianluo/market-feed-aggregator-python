"""线程安全的行情接入门面。

职责：
1. 校验（validator）-> 去重（dedup）-> 乱序判定 -> 聚合（aggregator），
   全程在同一把锁内完成，保证并发写入与查询看到一致快照。
2. 有界背压：待处理槽位（信号量）达到上限时，ingest 立即返回 backpressure，
   不落盘、不阻塞、不静默丢弃。
3. 过期淘汰：每个 symbol 只保留最近 retention_windows 个窗口；查询触及
   已淘汰区间时显式给出 evicted 反馈；请求参数本身过期（结束时间不大于
   淘汰水位）抛 RequestExpiredError，与背压错误可明确区分。
4. 任何内部异常统一包装为 InternalFeedError，不把栈/内部异常透给调用方。
"""

from __future__ import annotations

import threading

from .aggregator import WindowAggregator, _window_start
from .dedup import Deduplicator
from .errors import (
    InternalFeedError,
    RejectReason,
    RequestExpiredError,
    StoreOverloadedError,
)
from .models import IngestResult, IngestStatus, MarketEvent, QueryResult


class MarketDataStore:
    def __init__(
        self,
        window_size_ms: int = 1000,
        max_pending: int = 1024,
        retention_windows: int = 60,
    ) -> None:
        if max_pending <= 0:
            raise ValueError("max_pending must be positive")
        if retention_windows <= 0:
            raise ValueError("retention_windows must be positive")
        self._window_size = window_size_ms
        self._retention = retention_windows
        # 有界"在途"槽位：同步实现中处理极快，占槽仅为模拟接入承载上限
        self._slots = threading.BoundedSemaphore(max_pending)
        self._dedup = Deduplicator()
        self._aggregator = WindowAggregator(window_size_ms)
        self._lock = threading.RLock()
        # symbol -> 已接受事件的最大时间戳（乱序判定水位）
        self._watermark: dict[str, int] = {}
        # symbol -> 当前保留窗口的最小 window_start（淘汰水位）
        self._retain_floor: dict[str, int] = {}

    # ------------------------------------------------------------------ ingest
    def ingest(self, raw: object) -> IngestResult:
        if not self._slots.acquire(blocking=False):
            return IngestResult(
                status=IngestStatus.REJECTED,
                reason=RejectReason.BACKPRESSURE,
                detail="store overloaded: capacity exhausted",
            )
        try:
            try:
                event, rejected = _safe_validate(raw)
            except Exception as exc:  # 校验器内部任何意外都不外泄
                raise InternalFeedError() from exc
            if rejected is not None:
                return rejected
            assert event is not None

            with self._lock:
                if not self._dedup.mark(event.event_id):
                    return IngestResult(
                        status=IngestStatus.DUPLICATE,
                        reason=RejectReason.DUPLICATE,
                        detail=f"event_id already ingested: {event.event_id}",
                        event_id=event.event_id,
                    )

                wm = self._watermark.get(event.symbol)
                if wm is not None and event.timestamp_ms < wm:
                    # 已计入去重集合，但不进入聚合，保证重放确定性
                    return IngestResult(
                        status=IngestStatus.REJECTED,
                        reason=RejectReason.OUT_OF_ORDER,
                        detail=(
                            f"timestamp_ms {event.timestamp_ms} < watermark {wm} "
                            f"for symbol {event.symbol}"
                        ),
                        event_id=event.event_id,
                    )

                self._watermark[event.symbol] = event.timestamp_ms
                self._aggregator.add(event)
                self._evict_if_needed(event.symbol)

            return IngestResult(
                status=IngestStatus.ACCEPTED, event_id=event.event_id
            )
        except InternalFeedError:
            raise
        except Exception as exc:
            raise InternalFeedError("ingestion failed") from exc
        finally:
            self._slots.release()

    def _evict_if_needed(self, symbol: str) -> None:
        """按该 symbol 的最高水位窗口裁剪，保留最近 retention_windows 个。"""
        head = _window_start(self._watermark[symbol], self._window_size)
        floor = head - (self._retention - 1) * self._window_size
        old_floor = self._retain_floor.get(symbol)
        if old_floor is not None and floor <= old_floor:
            return
        self._retain_floor[symbol] = floor
        series = self._aggregator._series.get(symbol)
        if series is None:
            return
        for etype, buckets in series.windows.items():
            for ws in [w for w in buckets if w < floor]:
                del buckets[ws]
        # 重算每类型的 min/max，保证缺口判定只基于现存窗口
        for etype, buckets in series.windows.items():
            if buckets:
                series.min_start[etype] = min(buckets)
                series.max_start[etype] = max(buckets)

    # ------------------------------------------------------------------- query
    def query(self, symbol: str, start_ms: int, end_ms: int) -> QueryResult:
        try:
            with self._lock:
                floor = self._retain_floor.get(symbol)
                if start_ms < 0 or end_ms <= start_ms:
                    raise ValueError("invalid query range")
                if floor is not None and end_ms <= floor:
                    raise RequestExpiredError(
                        f"requested data before retention floor {floor} was evicted"
                    )
                result = self._aggregator.query(symbol, start_ms, end_ms)
                if floor is not None and start_ms < floor:
                    result.evicted = True
                return result
        except RequestExpiredError:
            raise
        except ValueError:
            raise
        except Exception as exc:
            raise InternalFeedError("query failed") from exc

    # ----------------------------------------------------------------- helpers
    def reset_session(self) -> None:
        """清空全部接入状态，等价于一个全新连接（供从头续传验证）。"""
        with self._lock:
            self._dedup.reset()
            self._aggregator = WindowAggregator(self._window_size)
            self._watermark.clear()
            self._retain_floor.clear()

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "symbols": len(self._watermark),
                "dedup_size": len(self._dedup._seen),
            }


def _safe_validate(raw: object) -> tuple[MarketEvent | None, IngestResult | None]:
    from .validator import validate_event

    return validate_event(raw)
