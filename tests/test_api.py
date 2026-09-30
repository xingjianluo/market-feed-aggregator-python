"""HTTP 接口：错误映射可区分；承载能力与过期反馈不同；并发读写结果一致。"""

from __future__ import annotations

import asyncio
import threading

import pytest
from httpx import ASGITransport, AsyncClient

from marketfeed.api import create_app
from marketfeed.store import MarketDataStore


def trade(eid, ts, p=10.0, s=1.0, sym="BTC"):
    return {"event_id": eid, "symbol": sym, "type": "trade",
            "timestamp_ms": ts, "price": p, "size": s}


@pytest.fixture
def client():
    async def _make(store=None, **kw):
        store = store or MarketDataStore(window_size_ms=1000, retention_windows=100)
        app = create_app(store, **kw)
        transport = ASGITransport(app=app)
        return store, AsyncClient(transport=transport, base_url="http://t")
    return _make


async def test_event_lifecycle(client, case_logger):
    store, c = await client()
    async with c:
        r1 = await c.post("/events", json=trade("a", 100))
        r2 = await c.post("/events", json=trade("a", 100))
        r3 = await c.post("/events", json={"x": 1})
        r4 = await c.post("/events", data="not-json",
                          headers={"content-type": "application/json"})
        case_logger("HTTP 生命周期", accept=r1.status_code, dup=r2.json()["status"],
                    reject=(r3.status_code, r3.json()["reason"]),
                    badjson=(r4.status_code, r4.json()["reason"]))
        assert r1.status_code == 200
        assert r2.json()["status"] == "duplicate"
        assert r3.status_code == 422 and r3.json()["reason"] == "missing_field"
        assert r4.status_code == 422 and r4.json()["reason"] == "not_a_dict"


async def test_summary_gap_and_expired(client, case_logger):
    store = MarketDataStore(window_size_ms=1000, retention_windows=3)
    _, c = await client(store)
    async with c:
        for ts in (1, 3001, 5001):
            await c.post("/events", json=trade(f"e{ts}", ts))
        body = (await c.get("/summary", params={"symbol": "BTC", "start_ms": 3000, "end_ms": 6000})).json()
        expired = await c.get("/summary", params={"symbol": "BTC", "start_ms": 0, "end_ms": 1000})
        bad = await c.get("/summary", params={"symbol": "BTC", "start_ms": 10, "end_ms": 10})
        case_logger("HTTP 查询", gaps=body["gaps"], expired=(expired.status_code, expired.json()["reason"]),
                    bad=(bad.status_code, bad.json()["reason"]))
        assert body["gaps"]["trade"] == [{"start_ms": 4000, "end_ms": 5000}]
        assert expired.status_code == 410 and expired.json()["reason"] == "expired"
        assert bad.status_code == 400


async def test_backpressure_distinguishable(client, case_logger):
    gate = threading.Event()
    store = MarketDataStore()

    def blocked_ingest(raw):
        gate.wait(timeout=5)
        return store.ingest(raw)

    _, c = await client(store, ingest_fn=blocked_ingest, queue_max=2, enqueue_timeout_s=0.05)
    async with c:
        tasks = [asyncio.create_task(c.post("/events", json=trade(f"e{i}", i)))
                 for i in range(30)]
        await asyncio.sleep(0.3)
        gate.set()
        codes = sorted({(await t).status_code for t in tasks})
        bodies = [(await t).json().get("reason") for t in tasks]
        case_logger("HTTP 背压", codes=codes, reasons=sorted({b for b in bodies if b}))
        assert 503 in codes and 200 in codes
        assert "backpressure" in bodies  # 与 expired 明确区分


async def test_concurrent_http_read_write(client, case_logger):
    store = MarketDataStore(window_size_ms=100, retention_windows=1000)
    _, c = await client(store)

    async with c:
        async def writes():
            for i in range(300):
                await c.post("/events", json=trade(f"w{i}", i * 100))

        async def reads():
            counts = []
            for _ in range(100):
                r = await c.get("/summary", params={"symbol": "BTC", "start_ms": 0, "end_ms": 40000})
                counts.append(sum(len(v) for v in r.json()["windows"].values()))
            return counts

        wt = asyncio.create_task(writes())
        reads_t = [asyncio.create_task(reads()) for _ in range(3)]
        await wt
        sequences = await asyncio.gather(*reads_t)
        non_decreasing = all(
            all(b >= a for a, b in zip(seq, seq[1:])) for seq in sequences
        )
        final = await c.get("/summary", params={"symbol": "BTC", "start_ms": 0, "end_ms": 40000})
        n_windows = sum(len(v) for v in final.json()["windows"].values())
        case_logger("HTTP 并发", final_windows=n_windows,
                    reader_count=len(sequences), non_decreasing=non_decreasing)
        assert n_windows == 300
        assert non_decreasing
