"""HTTP 接入层：把统一门面暴露为接口，并把领域错误映射为可区分的响应。

状态码约定：
  400  请求体/参数格式错误（非 JSON 对象、回放文件损坏等）
  404  回放文件不存在
  422  事件校验失败（body.reason 为细粒度拒绝原因）
  409  事件重复（event_id 已计入）
  410  请求的窗口已过期淘汰
  503  接入超承载能力（背压，可重试）
  500  未预期内部错误（不回传内部细节）
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from market_feed.errors import (
    DuplicateEventError,
    ExpiredRequestError,
    FeedError,
    OverCapacityError,
    ValidationError,
)
from market_feed.service import MarketFeedService

logger = logging.getLogger("market_feed.api")

app = FastAPI(title="local-market-feed-aggregator", version="0.1.0")
service = MarketFeedService(window_ms=1000, retention_windows=120)


def _payload(error: str, exc: Exception | None = None, **extra: Any) -> dict:
    body = {"ok": False, "error": error}
    body.update(extra)
    if exc is not None:
        body["detail"] = str(exc)
    return body


@app.exception_handler(ValidationError)
async def _validation_handler(_request: Request, exc: ValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422, content=_payload("validation_failed", reason=exc.reason, detail=exc.detail)
    )


@app.exception_handler(DuplicateEventError)
async def _duplicate_handler(_request: Request, exc: DuplicateEventError) -> JSONResponse:
    return JSONResponse(status_code=409, content=_payload("duplicate_event", exc))


@app.exception_handler(ExpiredRequestError)
async def _expired_handler(_request: Request, exc: ExpiredRequestError) -> JSONResponse:
    return JSONResponse(status_code=410, content=_payload("window_expired", exc))


@app.exception_handler(OverCapacityError)
async def _capacity_handler(_request: Request, exc: OverCapacityError) -> JSONResponse:
    # 503 + Retry-After，语义上是“超容量，稍后重试”
    resp = JSONResponse(
        status_code=503,
        content=_payload("over_capacity", exc, retry_after_ms=100),
    )
    resp.headers["Retry-After"] = "1"
    return resp


@app.exception_handler(FeedError)
async def _feed_handler(_request: Request, exc: FeedError) -> JSONResponse:
    return JSONResponse(status_code=400, content=_payload("bad_request", exc))


@app.exception_handler(FileNotFoundError)
async def _not_found_handler(_request: Request, exc: FileNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content=_payload("replay_file_not_found", exc))


@app.exception_handler(Exception)
async def _unexpected_handler(_request: Request, exc: Exception) -> JSONResponse:
    # 内部异常只记录日志，不透给调用方
    logger.exception("unexpected error: %s", exc)
    return JSONResponse(status_code=500, content={"ok": False, "error": "internal_error"})


@app.get("/health")
def health() -> dict:
    return {"ok": True, "watermark_ms": service.watermark()}


@app.post("/events")
async def submit_event(request: Request) -> dict:
    """接入单个行情事件，body 必须是 JSON 对象。"""
    try:
        raw = await request.json()
    except Exception as exc:
        raise ValidationError("not_an_object", "request body must be a JSON object") from exc
    win = service.submit(raw)
    return {"ok": True, "window": win.to_dict()}


@app.get("/symbols/{symbol}/windows")
def query_windows(symbol: str, from_ms: int, to_ms: int) -> dict:
    """查询 [from_ms, to_ms) 内的时间窗口聚合结果。"""
    windows = [w.to_dict() for w in service.query(symbol, from_ms, to_ms)]
    return {"ok": True, "symbol": symbol, "windows": windows}


@app.post("/replay")
async def replay(request: Request) -> dict:
    """从本地 JSONL 文件回放；{"path": "...", "shuffled": false}。"""
    body = await _require_object(request)
    path = body.get("path")
    if not isinstance(path, str) or not path:
        raise ValidationError("missing_field", "path must be a non-empty string")
    summary = service.replay_file(path, shuffled=bool(body.get("shuffled", False)))
    return {"ok": True, "summary": _public_summary(summary)}


@app.post("/resume")
async def resume(request: Request) -> dict:
    """断线续传；{"path": "...", "last_event_id": "可选"}。"""
    body = await _require_object(request)
    path = body.get("path")
    if not isinstance(path, str) or not path:
        raise ValidationError("missing_field", "path must be a non-empty string")
    last_id = body.get("last_event_id")
    if last_id is not None and not isinstance(last_id, str):
        raise ValidationError("bad_field_type", "last_event_id must be a string")
    summary = service.resume_from(path, last_event_id=last_id)
    return {"ok": True, "summary": _public_summary(summary)}


async def _require_object(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception as exc:
        raise ValidationError("not_an_object", "request body must be a JSON object") from exc
    if not isinstance(body, dict):
        raise ValidationError("not_an_object", "request body must be a JSON object")
    return body


def _public_summary(summary: dict) -> dict:
    return {
        "total": summary["total"],
        "accepted": summary["accepted"],
        "duplicates": summary["duplicate_count"],
        "rejected": summary["rejected_count"],
        "items": summary["items"],
    }
