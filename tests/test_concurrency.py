"""并发读写、容量与过期失败测试。"""
from __future__ import annotations

import threading

import pytest

from market_feed.errors import ExpiredRequestError, FeedError, OverCapacityError
from market_feed.models import WindowStatus
from market_feed.service import MarketFeedService
from market_feed.store import EventStore

from ._logs import log_case


def trade(eid: str, ts: int, price: float = 100.0) -> dict:
    return {"event_id": eid, "event_type": "trade", "symbol": "BTCUSDT",
            "timestamp_ms": ts, "price": price, "size": 1.0, "side": "buy"}


def test_over_capacity_is_distinct_failure():
    # 许可数=1，手动占用唯一许可后再接入 -> 明确的 OverCapacityError
    store = EventStore(window_ms=1000, retention_windows=10, max_inflight_writes=1)
    store.acquire_permit()
    from market_feed.service import validate_event
    event = validate_event(trade("t1", 1000), 0)
    with pytest.raises(OverCapacityError):
        store.acquire_permit()
    log_case("接入超容量", "max_inflight=1 且唯一许可被占用",
             "非阻塞获取许可失败 -> over_capacity，可重试", "RAISED OverCapacityError")
    store.release_permit()  # 归还后恢复可用
    win = store.ingest(event)
    assert win.event_ids == ["t1"]


def test_expired_window_is_distinct_failure():
    # 只保留 3 个窗口：水位推到 5000 后，窗口 1000 已淘汰
    svc = MarketFeedService(window_ms=1000, retention_windows=3)
    svc.submit(trade("t1", 1000))
    svc.submit(trade("t2", 2000))
    svc.submit(trade("t3", 5000))
    with pytest.raises(ExpiredRequestError) as exc:
        svc.query("BTCUSDT", 1000, 2000)
    log_case("查询已淘汰窗口", "retention=3, wm=5000, 查询[1000,2000)",
             "起点 < retained cutoff -> expired", str(exc.value))
    # 未过期区间仍可查询，过期与超载是两种不同失败
    windows = svc.query("BTCUSDT", 3000, 6000)
    statuses = [(w.window_start_ms, w.status.value) for w in windows]
    log_case("同条件下查询保留区间", "[3000,6000)", "应正常返回含 gap 窗口", statuses)
    assert statuses[0] == (3000, "gap")
    assert statuses[-1][0] == 5000


def test_concurrent_reads_see_consistent_snapshots():
    svc = MarketFeedService(window_ms=100, retention_windows=100000)
    stop = threading.Event()
    errors: list[Exception] = []
    accepted = {"n": 0}
    counter_lock = threading.Lock()

    def writer() -> None:
        i = 0
        while not stop.is_set():
            # 乱序/超载在压力下属于合法响应，只有“真正异常”才是缺陷
            try:
                svc.submit(trade(f"e{i}", i * 10, price=100.0 + i))
            except Exception as exc:  # noqa: BLE001
                if not isinstance(exc, FeedError):
                    errors.append(exc)
                    return
            else:
                with counter_lock:
                    accepted["n"] += 1
            i += 1

    snapshots: list[int] = []

    def reader() -> None:
        while not stop.is_set():
            try:
                ws = svc.query("BTCUSDT", 0, 10_000_000)
                ids = [eid for w in ws for eid in w.event_ids]
                total = sum(w.trades.count for w in ws)
                # 一致性：无重复计入，count 总和与 id 数一致
                if len(ids) != total or len(set(ids)) != len(ids):
                    errors.append(AssertionError("inconsistent snapshot"))
                    return
                snapshots.append(total)
            except ExpiredRequestError:
                pass
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                return

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(4)
    ]
    for t in threads:
        t.start()
    stop.wait(1.0)
    stop.set()
    for t in threads:
        t.join(timeout=5)

    log_case("1 写 4 读并发 1s", f"快照数={len(snapshots)}, 写入={accepted['n']}",
             "每个快照 count==唯一id数，随写入单调不减",
             {"errors": errors[:3], "final_total": snapshots[-1] if snapshots else None})
    assert not errors
    assert accepted["n"] > 0 and snapshots
    assert snapshots == sorted(snapshots)  # 读已提交：总量单调不减


def test_concurrent_writes_aggregate_without_loss():
    # 每个 worker 写独立 symbol、独立时间轴；水位按 symbol 独立，
    # 因此任意线程调度顺序都不会触发跨标的乱序，纯压并发“不丢失、不重复”。
    n_workers = 8
    per_worker = 50
    svc = MarketFeedService(window_ms=10_000, retention_windows=100)

    def worker(worker_id: int) -> None:
        symbol = f"SYM{worker_id:02d}"
        for i in range(per_worker):
            raw = {
                "event_id": f"w{worker_id:02d}-{i:02d}",
                "event_type": "trade",
                "symbol": symbol,
                "timestamp_ms": 1000 + i * 10_000,
                "price": 100.0,
                "size": 1.0,
                "side": "buy",
            }
            svc.submit(raw)

    threads = [threading.Thread(target=worker, args=(wid,)) for wid in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = 0
    for wid in range(n_workers):
        windows = svc.query(f"SYM{wid:02d}", 0, 600_000)
        total += sum(w.trades.count for w in windows)
    log_case(f"{n_workers} 线程并发写入 {n_workers * per_worker} 事件",
             "每线程独立 symbol 与时间轴，水位按 symbol 独立",
             "无丢失、无重复", {"total": total})
    assert total == n_workers * per_worker
    assert len(svc.store.accepted_event_ids()) == n_workers * per_worker


def test_snapshot_is_deep_copied():
    # 读返回的是快照副本，调用方修改不影响内部状态
    svc = MarketFeedService(window_ms=1000)
    svc.submit(trade("t1", 1000))
    win = svc.query("BTCUSDT", 1000, 2000)[0]
    win.event_ids.append("tampered")
    win.trades.count = 999
    win.status = WindowStatus.GAP
    again = svc.query("BTCUSDT", 1000, 2000)[0].to_dict()
    log_case("篡改查询结果对象", "append id / 改 count / 改 status",
             "深拷贝快照，内部状态不受影响", again)
    assert again["event_ids"] == ["t1"]
    assert again["trades"]["count"] == 1
    assert again["status"] == "open"
