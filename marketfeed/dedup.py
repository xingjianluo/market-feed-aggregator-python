"""事件去重：同一 event_id 全局只计入一次（线程安全）。"""

from __future__ import annotations

import threading


class Deduplicator:
    """基于 event_id 的幂等集合。

    mark() 返回 True 表示首次出现并已记录；返回 False 表示重复。
    reset() 用于明确开启一个新的接入会话；不 reset 直接整段重放时，
    重复事件会被判为 duplicate，因此同样不会重复累计。
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def seen(self, event_id: str) -> bool:
        with self._lock:
            return event_id in self._seen

    def mark(self, event_id: str) -> bool:
        with self._lock:
            if event_id in self._seen:
                return False
            self._seen.add(event_id)
            return True

    def reset(self) -> None:
        with self._lock:
            self._seen.clear()
