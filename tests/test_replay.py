"""整体重放 / 乱序 / 断线续传一致性测试。"""
from __future__ import annotations

from pathlib import Path

import pytest

from market_feed.replay import ReplayFeed
from market_feed.service import MarketFeedService

from ._logs import log_case

DATA = Path(__file__).resolve().parent.parent / "data" / "sample_feed.jsonl"


def _snapshot(svc: MarketFeedService, symbols=("BTCUSDT", "ETHUSDT"), to_ms=10000):
    out = {}
    for symbol in symbols:
        out[symbol] = [
            (w.window_start_ms, w.status.value, tuple(w.event_ids),
             w.trades.count, w.quotes.count)
            for w in svc.query(symbol, 0, to_ms)
        ]
    return out


def test_full_replay_is_idempotent():
    svc = MarketFeedService(window_ms=1000)
    first = svc.replay_file(DATA)
    snap1 = _snapshot(svc)
    log_case("首次整体回放", str(DATA), "顺序文件全部接收",
             {"accepted": first["accepted"], "dup": first["duplicate_count"]})
    assert first["accepted"] == 7 and first["duplicate_count"] == 0

    second = svc.replay_file(DATA)
    snap2 = _snapshot(svc)
    dup_ids = sorted(second["duplicates"])
    log_case("第二次整体回放", str(DATA), "event_id 去重，全部判 duplicate，不重复累计",
             {"accepted": second["accepted"], "duplicates": dup_ids})
    assert second["accepted"] == 0
    assert second["duplicate_count"] == 7
    assert snap1 == snap2  # 聚合结果逐窗口一致


def test_replay_from_scratch_matches_resume():
    # 方式 A：从头连续推送；方式 B：先续传到断点再续传完整文件，结果必须一致
    svc_a = MarketFeedService(window_ms=1000)
    svc_a.replay_file(DATA)

    svc_b = MarketFeedService(window_ms=1000)
    r1 = svc_b.resume_from(DATA, last_event_id="t2")  # 断在第 3 条之后
    log_case("断线续传(断点 t2)", "last_event_id=t2", "重放前缀",
             {"accepted": r1["accepted"], "total": r1["total"]})
    r2 = svc_b.resume_from(DATA, last_event_id="t4")  # 再连，全量前缀
    log_case("断线续传(到 t4)", "last_event_id=t4",
             "已计入=duplicate，未计入=accepted",
             {"accepted": r2["accepted"], "dup": r2["duplicate_count"],
              "total": r2["total"]})
    assert r1["accepted"] == 3
    assert r2["accepted"] == 4 and r2["duplicate_count"] == 3

    snap_a, snap_b = _snapshot(svc_a), _snapshot(svc_b)
    log_case("续传结果 vs 连续推送", "两条路径", "逐窗口 (状态, 事件集合, 计数) 相等",
             snap_a == snap_b)
    assert snap_a == snap_b
    assert svc_a.watermark() == svc_b.watermark() == 5900


def test_shuffled_replay_rejects_late_events():
    svc = MarketFeedService(window_ms=1000)
    summary = svc.replay_file(DATA, shuffled=True)
    outcomes = [(i["event_id"], i["outcome"], i["reason"]) for i in summary["items"]]
    late = [i for i in summary["items"] if i["reason"] == "timestamp_out_of_order"]
    log_case("确定性乱序回放", "固定种子 shuffle",
             "先到的事件推进水位，后到的旧事件按乱序拒绝且不计入",
             outcomes)
    assert summary["accepted"] + summary["rejected_count"] + summary[
        "duplicate_count"] == summary["total"]
    assert len(late) >= 1
    # 被乱序拒绝的事件不出现在任何窗口
    for item in late:
        for symbol in ("BTCUSDT", "ETHUSDT"):
            for w in svc.query(symbol, 0, 10000):
                assert item["event_id"] not in w.event_ids


def test_shuffled_replay_is_deterministic():
    outcomes = []
    for _ in range(2):
        svc = MarketFeedService(window_ms=1000)
        s = svc.replay_file(DATA, shuffled=True)
        outcomes.append([(i["event_id"], i["outcome"]) for i in s["items"]])
    log_case("乱序回放确定性", "同一种子两次", "事件顺序与判定一致", outcomes[0] == outcomes[1])
    assert outcomes[0] == outcomes[1]


def test_replay_with_inline_duplicates(tmp_path: Path):
    # 文件内部就含重复行（模拟重连后服务器重发）
    records = ReplayFeed(DATA).read_raw()
    records.insert(4, records[0])  # t1 第二次出现
    path = tmp_path / "dup.jsonl"
    ReplayFeed.write(path, records)
    svc = MarketFeedService(window_ms=1000)
    summary = svc.replay_file(path)
    log_case("文件内含重复行", "t1 出现两次", "第二次判 duplicate",
             {"accepted": summary["accepted"], "dup": summary["duplicates"]})
    assert summary["accepted"] == 7
    assert summary["duplicates"] == ["t1"]


def test_corrupt_file_raises_explicitly(tmp_path: Path):
    bad = tmp_path / "bad.jsonl"
    bad.write_text('{"event_id":"t1"}\nthis-is-not-json\n', encoding="utf-8")
    svc = MarketFeedService(window_ms=1000)
    with pytest.raises(ValueError) as exc:
        svc.replay_file(bad)
    log_case("损坏的回放文件", "第 2 行非 JSON", "数据源问题显式报错并带行号，不静默跳过",
             str(exc.value))
    assert "line 2" in str(exc.value) or ":2:" in str(exc.value)


def test_missing_file_raises(tmp_path: Path):
    svc = MarketFeedService(window_ms=1000)
    with pytest.raises(FileNotFoundError):
        svc.replay_file(tmp_path / "nope.jsonl")
