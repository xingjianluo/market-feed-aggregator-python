"""重复与乱序测试：同一笔事件重复推送只能计入一次。"""
from __future__ import annotations

import threading

import pytest

from market_feed.errors import DuplicateEventError, ValidationError
from market_feed.service import MarketFeedService

from ._logs import log_case


def trade(eid: str, ts: int, side: str = "buy", price: float = 100.0) -> dict:
    return {
        "event_id": eid,
        "event_type": "trade",
        "symbol": "BTCUSDT",
        "timestamp_ms": ts,
        "price": price,
        "size": 1.0,
        "side": side,
    }


def test_duplicate_counts_once():
    svc = MarketFeedService(window_ms=1000)
    event = trade("t1", 1000)
    win1 = svc.submit(event)
    log_case("重复推送同一事件(第2次)", event, "event_id=t1 已在去重表", "应抛 duplicate")
    with pytest.raises(DuplicateEventError):
        svc.submit(event)
    log_case("重复推送同一事件(第3次)", event, "event_id=t1 仍在去重表", "应抛 duplicate")
    with pytest.raises(DuplicateEventError):
        svc.submit(event)

    windows = svc.query("BTCUSDT", 1000, 2000)
    d = windows[0].to_dict()
    log_case("重复后查询窗口", {"event_id": "t1", "推送次数": 3},
             "去重表只允许计入一次", d)
    assert d["trades"]["count"] == 1
    assert d["trades"]["volume"] == 1.0
    assert d["event_ids"] == ["t1"]
    assert win1.to_dict()["event_ids"] == ["t1"]


def test_duplicate_identity_beats_out_of_order():
    # 水位推进后，重发旧事件应判 duplicate 而不是 timestamp_out_of_order
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("t1", 1000))
    svc.submit(trade("t2", 5000, side="sell"))
    try:
        svc.submit(trade("t1", 1000))
        raise AssertionError("应当抛 DuplicateEventError")
    except DuplicateEventError:
        log_case("水位后重发旧事件", trade("t1", 1000),
                 "身份去重优先于乱序判定", "REJECTED duplicate_event_id")
    except ValidationError as exc:
        pytest.fail(f"期望 duplicate，实际 {exc.reason}")


def test_new_old_event_is_out_of_order():
    # 新 event_id 但时间戳回退 -> 乱序拒绝，且不影响聚合
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("t1", 5000))
    with pytest.raises(ValidationError) as exc:
        svc.submit(trade("t_old", 4999))
    log_case("新ID旧时间戳", trade("t_old", 4999),
             "event_id 未见但 ts < watermark=5000", f"REJECTED {exc.value.reason}")
    assert exc.value.reason == "timestamp_out_of_order"
    assert "t_old" not in svc.store.accepted_event_ids()


def test_late_event_within_allowed_lateness_accepted():
    svc = MarketFeedService(window_ms=1000, allowed_lateness_ms=500)
    svc.submit(trade("t1", 5000))
    win = svc.submit(trade("t_late", 4600))  # 迟到 400ms，允许
    log_case("允许 500ms 迟到", trade("t_late", 4600),
             "5000-4600=400 <= 500", f"ACCEPTED window={win.window_start_ms}")
    assert win.window_start_ms == 4000


def test_concurrent_duplicate_submission_counts_once():
    svc = MarketFeedService(window_ms=1000)
    event = trade("only", 1000)
    outcomes: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            svc.submit(event)
            result = "accepted"
        except DuplicateEventError:
            result = "duplicate"
        with lock:
            outcomes.append(result)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log_case("16 线程并发提交同一事件", event, "store 锁内去重",
             {"accepted": outcomes.count("accepted"),
              "duplicate": outcomes.count("duplicate")})
    assert outcomes.count("accepted") == 1
    assert outcomes.count("duplicate") == 15
    d = svc.query("BTCUSDT", 1000, 2000)[0].to_dict()
    assert d["trades"]["count"] == 1
