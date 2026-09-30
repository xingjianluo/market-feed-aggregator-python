"""可区分的错误类型。

所有对调用方可见的失败都使用这里定义的异常/错误码，
内部异常不会直接透传到接入与查询接口之外。
"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """事件被拒绝（或重复）的原因，测试可据此区分失败类型。"""

    NOT_A_DICT = "not_a_dict"
    MISSING_FIELD = "missing_field"
    BAD_TYPE = "bad_type"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    INVALID_PRICE = "invalid_price"
    INVALID_SIZE = "invalid_size"
    INVALID_TIMESTAMP = "invalid_timestamp"
    OUT_OF_ORDER = "out_of_order"
    DUPLICATE = "duplicate"
    BACKPRESSURE = "backpressure"
    EXPIRED = "expired"


class StoreOverloadedError(Exception):
    """接入端超过承载能力（有界队列已满）。"""

    code = RejectReason.BACKPRESSURE


class RequestExpiredError(Exception):
    """请求读取的窗口数据已过期淘汰。"""

    code = RejectReason.EXPIRED


class InternalFeedError(Exception):
    """内部异常的统一包装，避免内部细节直接透给调用方。"""

    def __init__(self, message: str = "internal error") -> None:
        super().__init__(message)
