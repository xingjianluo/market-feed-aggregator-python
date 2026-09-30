"""统一门面：事件归一化校验、接入、查询、回放与断线续传。"""
from __future__ import annotations

import math
from pathlib import Path

from .errors import DuplicateEventError, ValidationError
from .models import EventType, MarketEvent, RejectReason, WindowResult
from .replay import replay_into
from .store import EventStore

_VALID_SIDES = frozenset({"buy", "sell"})
_REQUIRED_COMMON = ("event_id", "event_type", "symbol", "timestamp_ms")


def _is_real_number(value: object) -> bool:
    """int/float 且非布尔、非 NaN/Inf。"""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return False


def _coerce_number(value: object, field: str) -> float:
    """数字字段接受 number 或数字字符串（交易所 JSON 常见），其余按类型错误拒绝。"""
    if isinstance(value, bool):
        raise ValidationError(
            RejectReason.BAD_FIELD_TYPE.value, f"{field} must be a number, got boolean"
        )
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError(RejectReason.BAD_FIELD_TYPE.value, f"{field} must be finite")
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            parsed = float(text)
        except ValueError:
            raise ValidationError(
                RejectReason.BAD_FIELD_TYPE.value, f"{field} must be numeric, got {value!r}"
            )
        if not math.isfinite(parsed):
            raise ValidationError(RejectReason.BAD_FIELD_TYPE.value, f"{field} must be finite")
        return parsed
    raise ValidationError(
        RejectReason.BAD_FIELD_TYPE.value, f"{field} must be a number or numeric string"
    )


def _positive_number(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value > 0
    if isinstance(value, float):
        return math.isfinite(value) and value > 0
    if isinstance(value, str):
        try:
            parsed = float(value.strip())
        except ValueError:
            return False
        return math.isfinite(parsed) and parsed > 0
    return False


def _check_levels(value: object, field_name: str) -> tuple[tuple[tuple[float, float], ...], ...]:
    if not isinstance(value, list):
        raise ValidationError(
            RejectReason.INCONSISTENT_LEVELS.value,
            f"{field_name} must be a list of [price, size] pairs",
        )
    levels: list[tuple[float, float]] = []
    for i, entry in enumerate(value):
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            raise ValidationError(
                RejectReason.INCONSISTENT_LEVELS.value,
                f"{field_name}[{i}] must be a [price, size] pair",
            )
        px, qty = entry
        if not _positive_number(px):
            raise ValidationError(
                RejectReason.INCONSISTENT_LEVELS.value,
                f"{field_name}[{i}].price must be a positive finite number",
            )
        if not _positive_number(qty):
            raise ValidationError(
                RejectReason.INCONSISTENT_LEVELS.value,
                f"{field_name}[{i}].size must be a positive finite number",
            )
        levels.append((float(px), float(qty)))
    return tuple(levels)


def validate_event(
    raw: object, watermark_ms: int, allowed_lateness_ms: int = 0
) -> MarketEvent:
    """把原始对象校验并归一化为 MarketEvent。

    检查顺序固定，保证同一输入总是得到同一个拒绝原因：
    载荷形态 -> 必填字段 -> 类型枚举 -> 事件ID -> 标的 -> 时间戳(含乱序)
    -> 类型专属字段（trade: 价格/数量/方向；quote: 买卖盘）。
    """
    if not isinstance(raw, dict):
        raise ValidationError(
            RejectReason.NOT_A_OBJECT.value, "event payload must be a JSON object"
        )

    for name in _REQUIRED_COMMON:
        if name not in raw:
            raise ValidationError(RejectReason.MISSING_FIELD.value, f"missing field: {name}")

    type_value = raw["event_type"]
    if not isinstance(type_value, str) or type_value not in (e.value for e in EventType):
        raise ValidationError(
            RejectReason.UNKNOWN_EVENT_TYPE.value,
            f"event_type must be one of {[e.value for e in EventType]}, got {type_value!r}",
        )
    event_type = EventType(type_value)

    event_id = raw["event_id"]
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValidationError(
            RejectReason.BAD_FIELD_TYPE.value, "event_id must be a non-empty string"
        )

    symbol = raw["symbol"]
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValidationError(RejectReason.BAD_SYMBOL.value, "symbol must be a non-empty string")

    ts = raw["timestamp_ms"]
    if not _is_real_number(ts) or isinstance(ts, float) and not float(ts).is_integer():
        raise ValidationError(
            RejectReason.BAD_TIMESTAMP.value,
            "timestamp_ms must be an integer epoch millisecond",
        )
    ts = int(ts)
    if ts < 0:
        raise ValidationError(RejectReason.BAD_TIMESTAMP.value, "timestamp_ms must be >= 0")
    if ts + allowed_lateness_ms < watermark_ms:
        raise ValidationError(
            RejectReason.TIMESTAMP_OUT_OF_ORDER.value,
            f"timestamp {ts} is earlier than session watermark {watermark_ms} "
            f"(allowed lateness {allowed_lateness_ms}ms)",
        )

    price = 0.0
    size = 0.0
    side: str | None = None
    bids: tuple[tuple[float, float], ...] = ()
    asks: tuple[tuple[float, float], ...] = ()

    if event_type is EventType.TRADE:
        for name in ("price", "size", "side"):
            if name not in raw:
                raise ValidationError(RejectReason.MISSING_FIELD.value, f"missing field: {name}")
        if not _positive_number(raw["price"]):
            raise ValidationError(
                RejectReason.BAD_PRICE.value, "trade.price must be a positive finite number"
            )
        if not _positive_number(raw["size"]):
            raise ValidationError(
                RejectReason.BAD_SIZE.value, "trade.size must be a positive finite number"
            )
        side = raw["side"]
        if not isinstance(side, str) or side not in _VALID_SIDES:
            raise ValidationError(
                RejectReason.BAD_SIDE.value, f"trade.side must be one of {sorted(_VALID_SIDES)}"
            )
        price, size = float(raw["price"]), float(raw["size"])
    else:
        for name in ("bids", "asks"):
            if name not in raw:
                raise ValidationError(RejectReason.MISSING_FIELD.value, f"missing field: {name}")
        bids = _check_levels(raw["bids"], "bids")
        asks = _check_levels(raw["asks"], "asks")
        if not bids and not asks:
            raise ValidationError(
                RejectReason.EMPTY_BOOK.value, "quote must have at least one bid or ask level"
            )

    return MarketEvent(
        event_id=event_id,
        event_type=event_type,
        symbol=symbol.strip(),
        timestamp_ms=ts,
        price=price,
        size=size,
        side=side,
        bids=bids,
        asks=asks,
    )


class MarketFeedService:
    """对接入层暴露的唯一门面。"""

    def __init__(
        self,
        window_ms: int = 1000,
        retention_windows: int = 60,
        max_inflight_writes: int = 1024,
        allowed_lateness_ms: int = 0,
    ) -> None:
        self.store = EventStore(
            window_ms, retention_windows, max_inflight_writes, allowed_lateness_ms
        )

    def submit(self, raw: dict) -> WindowResult:
        """校验 + 接入单个原始事件。

        顺序：先做承载能力控制；身份去重优先（同一 event_id 一律只计入一次，
        不因时间戳早于水位而被报成乱序）；随后完整校验，乱序在此被拒；
        最后入店。任何失败都以领域错误形式返回。
        """
        self.store.acquire_permit()
        try:
            if self.store.is_duplicate(raw.get("event_id") if isinstance(raw, dict) else None):
                raise DuplicateEventError(
                    f"event_id {raw.get('event_id')!r} already accepted; each event counts once"
                )
            # 乱序按“该 symbol 自己的水位”判定：
            # 非法 symbol 时水位取 0，后续 validate_event 仍会以 bad_symbol 拒绝
            peek_symbol = raw.get("symbol") if isinstance(raw, dict) else None
            symbol_wm = (
                self.store.watermark(peek_symbol) if isinstance(peek_symbol, str) else 0
            )
            event = validate_event(raw, symbol_wm, self.store.allowed_lateness_ms)
            return self.store.ingest(event)
        finally:
            self.store.release_permit()

    def query(self, symbol: str, from_ms: int, to_ms: int) -> list[WindowResult]:
        return self.store.query(symbol, from_ms, to_ms)

    def replay_file(self, path: str | Path, shuffled: bool = False) -> dict:
        """从本地文件整体回放（可打乱顺序模拟乱序）。"""
        return replay_into(path, self.submit, shuffled=shuffled)

    def resume_from(self, path: str | Path, last_event_id: str | None = None) -> dict:
        """断线续传：从头重放文件，停在 last_event_id（含）。

        语义：服务端是“按 event_id 去重的追加日志”，重放任意前缀都幂等。
        因此续传等价于重新发送从会话起点到断线位置的整段数据：
        已计入的事件返回 duplicate（不重复累计），断线期间未计入的被补上，
        最终水位与聚合结果与连续推送完全一致。last_event_id 不在文件中时，
        等同于整文件重放。
        """
        return replay_into(path, self.submit, stop_after_event_id=last_event_id)

    def watermark(self) -> int:
        return self.store.watermark()
