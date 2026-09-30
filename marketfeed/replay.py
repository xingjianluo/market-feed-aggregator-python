"""可回放的本地行情数据源。

- 数据源是一组本地原始事件（dict），投递前按 (timestamp_ms, event_id) 稳定排序，
  模拟"同一批数据"的确定性推送。
- 调用方可以故意以乱序/重复的原始数据喂入，以验证接入端的判定。
- 断线续传语义通过两种重放方式表达：
  1) 不重置去重器，整段重新投递：重放部分产生 duplicate，聚合结果不变；
  2) 重置会话（reset_dedup=True）后从头投递：等价于一个全新连接从起点
     续传，因为事件按相同顺序进入、乱序与重复判定是确定性的，
     聚合结果与首次完全一致。
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from .models import IngestResult


class ReplayFeed:
    def __init__(self, events: Iterable[object]) -> None:
        self._events = list(events)

    def ordered(self) -> list[object]:
        """按 (timestamp_ms, event_id) 稳定排序后的副本。

        无法解析为 dict 或缺少时间戳的事件排在最前，交由接入端拒绝，
        排序本身不丢弃任何事件。
        """

        def key(raw: object) -> tuple[int, int | str]:
            if isinstance(raw, dict):
                ts = raw.get("timestamp_ms")
                eid = raw.get("event_id", "")
                if isinstance(ts, bool) or not isinstance(ts, int):
                    return (0, str(eid))
                return (ts, str(eid))
            return (0, "")

        return sorted(self._events, key=key)

    def run(
        self,
        ingest: Callable[[object], IngestResult],
        *,
        ordered: bool = True,
        reset: Callable[[], None] | None = None,
    ) -> list[IngestResult]:
        """把全部事件投递给 ingest，返回逐条结果（含拒绝/重复）。

        reset 不为空时，在投递前调用（由接入端提供，表达"全新连接从起点
        续传"）；不提供 reset 直接整段重放则体现"同一连接上的重复投递"。
        """
        batch = self.ordered() if ordered else list(self._events)
        if reset is not None:
            reset()
        return [ingest(raw) for raw in batch]


def replay_segment(
    events: Iterable[object],
    ingest: Callable[[object], IngestResult],
    *,
    ordered: bool = True,
) -> list[IngestResult]:
    """投递一段数据（断线后续传的那一段），不做任何重置。"""
    feed = ReplayFeed(events)
    return feed.run(ingest, ordered=ordered)
