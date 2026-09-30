"""本地可回放行情数据源。

数据文件为 JSON Lines，每行一个行情事件（格式见 docs/event-format.md）。
播放器按文件顺序把事件交给接入函数，用于模拟推送、整体重放与断线续传。
文件本身格式错误（非 JSON / 非对象）属于数据源问题，显式抛出，不静默跳过。
"""
from __future__ import annotations

import json
import random
from collections.abc import Callable, Iterator
from pathlib import Path

from .errors import DuplicateEventError, FeedError, OverCapacityError
from .models import MarketEvent  # noqa: F401  (对外再导出)


class ReplayFeed:
    """从 JSONL 文件顺序读取原始事件字典。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read_raw(self) -> list[dict]:
        """读取全部原始记录（保持文件顺序，用于乱序重排测试）。"""
        return list(self.iter_raw())

    def iter_raw(self) -> Iterator[dict]:
        """逐条产出原始记录；空行允许，其他解析错误显式报错并带行号。"""
        if not self.path.exists():
            raise FileNotFoundError(f"replay data file not found: {self.path}")
        with self.path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{self.path}:{lineno}: invalid JSON: {exc}") from exc
                if not isinstance(record, dict):
                    raise ValueError(  # noqa: TRY004 - 数据源损坏，统一用带行号的 ValueError
                        f"{self.path}:{lineno}: record must be a JSON object"
                    )
                yield record

    @staticmethod
    def write(path: str | Path, records: list[dict]) -> None:
        """把事件记录写成 JSONL 文件。"""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(record, ensure_ascii=False))
                fh.write("\n")


def replay_into(
    path: str | Path,
    ingest_callable: Callable[[dict], object],
    *,
    shuffled: bool = False,
    seed: int = 20260930,
    stop_after_event_id: str | None = None,
) -> dict:
    """把本地数据文件回放进接入函数。

    - shuffled=True 时用固定种子做确定性打乱，用来模拟乱序推送；
    - stop_after_event_id 用于断线续传：文件顺序回放到该 id（含）即停；
    - 每条记录分类为 accepted / duplicate / rejected / failed，
      重复与其它拒绝原因严格区分，内部异常不做吞并。

    返回 {"accepted", "duplicates", "rejected", "failed", "items", "total"}。
    """
    feed = ReplayFeed(path)
    records = feed.read_raw()
    if shuffled:
        random.Random(seed).shuffle(records)

    summary = {"accepted": 0, "duplicates": [], "rejected": [], "failed": [], "items": []}
    for seq, raw in enumerate(records):
        event_id = raw.get("event_id") if isinstance(raw, dict) else None
        item = {"seq": seq, "event_id": event_id, "outcome": None, "reason": None, "detail": ""}
        try:
            ingest_callable(raw)
        except DuplicateEventError as exc:
            item["outcome"] = "duplicate"
            item["reason"] = "duplicate_event_id"
            item["detail"] = str(exc)
            summary["duplicates"].append(event_id)
        except OverCapacityError as exc:
            item["outcome"] = "failed"
            item["reason"] = "over_capacity"
            item["detail"] = str(exc)
            summary["failed"].append(item)
        except FeedError as exc:
            reason = getattr(exc, "reason", exc.__class__.__name__)
            item["outcome"] = "rejected"
            item["reason"] = reason
            item["detail"] = getattr(exc, "detail", str(exc))
            summary["rejected"].append(item)
        else:
            item["outcome"] = "accepted"
            summary["accepted"] += 1
        summary["items"].append(item)
        if stop_after_event_id is not None and event_id == stop_after_event_id:
            break

    summary["total"] = len(summary["items"])
    summary["duplicate_count"] = len(summary["duplicates"])
    summary["rejected_count"] = len(summary["rejected"])
    return summary
