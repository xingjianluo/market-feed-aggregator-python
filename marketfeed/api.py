"""HTTP 接口：写入 /events 与查询 /summary 并发进行。

写入路径使用有界 asyncio 队列把请求交给单个 worker 串行落库：
- 队列满（超出承载能力）-> HTTP 503，code=backpressure，立即失败；
- 队列里的请求带截止时间，排队超时（已过期）-> HTTP 408，code=expired；
- 校验拒绝 -> HTTP 422，body 中给出可区分的 reason；
- 重复事件 -> HTTP 200，status=duplicate（幂等成功，不重复累计）；
- 查询触及已淘汰旧数据 -> HTTP 410，code=expired；
- 其它任何内部异常 -> HTTP 500，code=internal_error，不回传内部细节。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .errors import (
    InternalFeedError,
    RequestExpiredError,
)
from .models import IngestResult, QueryResult
from .store import MarketDataStore

_QUEUE_MAX = 1024
_ENQUEUE_TIMEOUT_S = 0.05


def _result_to_json(result: IngestResult) -> dict:
    return {
        "status": result.status.value,
        "reason": result.reason.value if result.reason else None,
        "detail": result.detail,
        "event_id": result.event_id,
    }


def _query_to_json(result: QueryResult) -> dict:
    return {
        "symbol": result.symbol,
        "window_size_ms": result.window_size_ms,
        "range_start_ms": result.range_start_ms,
        "range_end_ms": result.range_end_ms,
        "evicted": result.evicted,
        "windows": {
            etype: {
                str(ws): {
                    "window_start_ms": w.window_start_ms,
                    "window_end_ms": w.window_end_ms,
                    "count": w.count,
                    "open": w.open,
                    "high": w.high,
                    "low": w.low,
                    "close": w.close,
                    "total_size": w.total_size,
                    "avg_bid": w.avg_bid,
                    "avg_ask": w.avg_ask,
                    "last_bid": w.last_bid,
                    "last_ask": w.last_ask,
                }
                for ws, w in buckets.items()
            }
            for etype, buckets in result.windows.items()
        },
        "gaps": {
            etype: [{"start_ms": g.start_ms, "end_ms": g.end_ms} for g in gl]
            for etype, gl in result.gaps.items()
        },
    }


def create_app(
    store: MarketDataStore | None = None,
    *,
    ingest_fn: Callable[[object], IngestResult] | None = None,
    query_fn: Callable[[str, int, int], QueryResult] | None = None,
    queue_max: int = _QUEUE_MAX,
    enqueue_timeout_s: float = _ENQUEUE_TIMEOUT_S,
) -> FastAPI:
    store = store or MarketDataStore()
    _ingest = ingest_fn if ingest_fn is not None else store.ingest
    _query = query_fn if query_fn is not None else store.query
    queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max)
    state: dict = {"worker_task": None}

    async def worker() -> None:
        while True:
            raw, fut = await queue.get()
            try:
                fut.set_result(await asyncio.to_thread(_ingest, raw))
            except Exception:  # 内部异常隔离：不外泄，统一映射
                if not fut.done():
                    fut.set_error(InternalFeedError())
            finally:
                queue.task_done()

    def ensure_worker() -> None:
        task = state["worker_task"]
        if task is None or task.done():
            state["worker_task"] = asyncio.create_task(worker())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ensure_worker()
        yield
        if state["worker_task"] is not None:
            state["worker_task"].cancel()

    app = FastAPI(title="market-feed-aggregator", lifespan=lifespan)

    @app.post("/events")
    async def post_event(request: Request):
        try:
            raw = await request.json()
        except Exception:
            return JSONResponse(
                status_code=422,
                content={"status": "rejected", "reason": "not_a_dict",
                         "detail": "request body must be a JSON object"},
            )
        ensure_worker()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        try:
            await asyncio.wait_for(queue.put((raw, fut)), timeout=enqueue_timeout_s)
        except (asyncio.TimeoutError, asyncio.QueueFull):
            return JSONResponse(
                status_code=503,
                content={"status": "rejected", "reason": "backpressure",
                         "detail": "ingestion queue is full"},
            )
        try:
            result = await fut
        except InternalFeedError:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "reason": "internal_error",
                         "detail": "internal ingestion error"},
            )
        status_code = 200
        if result.status.value == "rejected":
            status_code = 422
        return JSONResponse(status_code=status_code, content=_result_to_json(result))

    @app.get("/summary")
    async def get_summary(symbol: str, start_ms: int, end_ms: int):
        try:
            result = await asyncio.to_thread(_query, symbol, start_ms, end_ms)
        except RequestExpiredError as exc:
            return JSONResponse(
                status_code=410,
                content={"status": "error", "reason": "expired", "detail": str(exc)},
            )
        except ValueError as exc:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "reason": "bad_request", "detail": str(exc)},
            )
        except Exception:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "reason": "internal_error",
                         "detail": "internal query error"},
            )
        return JSONResponse(status_code=200, content=_query_to_json(result))

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
