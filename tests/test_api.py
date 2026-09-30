"""HTTP 层错误码区分与端到端接口测试。"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main as main_module
from main import app

from ._logs import log_case

DATA = Path(__file__).resolve().parent.parent / "data" / "sample_feed.jsonl"


@pytest.fixture()
def client():
    # 每个测试用干净的服务实例
    main_module.service = main_module.MarketFeedService(window_ms=1000, retention_windows=120)
    return TestClient(app)


def trade(eid="t1", ts=1000, **over):
    base = {"event_id": eid, "event_type": "trade", "symbol": "BTCUSDT",
            "timestamp_ms": ts, "price": 100.0, "size": 1.0, "side": "buy"}
    base.update(over)
    return base


def test_health_and_ingest_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    r = client.post("/events", json=trade())
    body = r.json()
    log_case("POST /events 合法事件", trade(), "201? no, 200", body["window"]["status"])
    assert r.status_code == 200 and body["ok"] is True
    assert body["window"]["event_ids"] == ["t1"]


def test_reject_status_codes_are_distinct(client):
    cases = [
        ("载荷非对象", "[]", 422, "not_an_object"),
        ("缺字段", json_missing(), 422, "missing_field"),
        ("未知类型", trade(event_type="depth"), 422, "unknown_event_type"),
        ("时间戳乱序", trade(ts=500), 422, "timestamp_out_of_order"),
        ("价格非法", trade(price=-1), 422, "bad_price"),
    ]
    client.post("/events", json=trade("t0", 1000))  # 建立水位
    for title, payload, expect_code, expect_reason in cases[1:]:
        r = client.post("/events", json=payload)
        log_case(title, payload, f"期望 {expect_code}/{expect_reason}",
                 f"{r.status_code}/{r.json().get('reason')}")
        assert r.status_code == expect_code
        assert r.json()["reason"] == expect_reason

    r = client.post("/events", content=b"[]", headers={"content-type": "application/json"})
    log_case("载荷非对象", "[]", "422 not_an_object", f"{r.status_code}/{r.json()}")
    assert r.status_code == 422 and r.json()["reason"] == "not_an_object"


def json_missing():
    return {"event_id": "x", "event_type": "trade"}


def test_duplicate_status_409(client):
    client.post("/events", json=trade("dup", 1000))
    r = client.post("/events", json=trade("dup", 1000))
    log_case("重复事件", trade("dup"), "身份去重 -> 409", r.json())
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_event"
    # 乱序重发旧 id 仍是 409 而不是 422
    client.post("/events", json=trade("later", 5000))
    r = client.post("/events", json=trade("dup", 1000))
    assert r.status_code == 409


def test_expired_status_410(client, tmp_path):
    main_module.service = main_module.MarketFeedService(window_ms=1000, retention_windows=3)
    client = TestClient(app)
    for eid, ts in [("a", 1000), ("b", 2000), ("c", 5000)]:
        assert client.post("/events", json=trade(eid, ts)).status_code == 200
    r = client.get("/symbols/BTCUSDT/windows", params={"from_ms": 1000, "to_ms": 2000})
    log_case("查询过期窗口", {"from": 1000, "to": 2000}, "已淘汰 -> 410", r.json())
    assert r.status_code == 410 and r.json()["error"] == "window_expired"
    r = client.get("/symbols/BTCUSDT/windows", params={"from_ms": 3000, "to_ms": 6000})
    assert r.status_code == 200 and len(r.json()["windows"]) == 3


def test_over_capacity_status_503(client, monkeypatch):
    # 模拟接入层满载：许可获取始终失败
    def deny() -> None:
        from market_feed.errors import OverCapacityError

        raise OverCapacityError("simulated saturation")

    monkeypatch.setattr(main_module.service.store, "acquire_permit", deny)
    r = client.post("/events", json=trade())
    log_case("超容量接入", "acquire_permit 恒失败", "503 over_capacity + Retry-After",
             {"status": r.status_code, "body": r.json(), "retry_after": r.headers.get("retry-after")})
    assert r.status_code == 503
    assert r.json()["error"] == "over_capacity"
    assert r.headers["retry-after"] == "1"


def test_replay_and_resume_endpoints(client):
    r = client.post("/replay", json={"path": str(DATA)})
    assert r.status_code == 200
    s = r.json()["summary"]
    log_case("POST /replay", str(DATA), "7 accepted", s)
    assert s["accepted"] == 7

    r = client.post("/replay", json={"path": str(DATA)})
    s = r.json()["summary"]
    assert s["accepted"] == 0 and s["duplicates"] == 7

    r = client.post("/resume", json={"path": str(DATA), "last_event_id": "t2"})
    s = r.json()["summary"]
    log_case("POST /resume 断点 t2", "重放前缀", "3 dup", s)
    assert s["accepted"] == 0 and s["duplicates"] == 3

    r = client.post("/replay", json={"path": "/no/such/file.jsonl"})
    assert r.status_code == 404
    r = client.post("/replay", json={"nopath": 1})
    assert r.status_code == 422 and r.json()["reason"] == "missing_field"


def test_query_endpoint_shape_and_gap(client):
    client.post("/replay", json={"path": str(DATA)})
    r = client.get("/symbols/BTCUSDT/windows", params={"from_ms": 1000, "to_ms": 7000})
    windows = r.json()["windows"]
    by_start = {w["window_start_ms"]: w for w in windows}
    log_case("GET windows", "[1000,7000)", "gap 窗口显式标注",
             {k: v["status"] for k, v in by_start.items()})
    assert by_start[2000]["status"] == "gap"
    assert by_start[2000]["gap_from_ms"] == 2000
    assert by_start[2000]["trades"]["count"] == 0
    assert by_start[1000]["trades"]["count"] == 2
