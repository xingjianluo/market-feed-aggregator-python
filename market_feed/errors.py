"""领域错误类型。

所有对调用方暴露的失败都通过这里定义的错误表达，
HTTP 层负责把它们映射成不同的状态码，不透出内部异常。
"""
from __future__ import annotations


class FeedError(Exception):
    """所有行情领域错误的基类。"""


class ValidationError(FeedError):
    """事件格式/内容校验失败（按原因细分，见 reject_reason）。"""

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}")


class DuplicateEventError(FeedError):
    """事件重复推送（同一 event_id 已计入）。"""


class OverCapacityError(FeedError):
    """系统超出承载能力（接入队列/背压）。"""


class ExpiredRequestError(FeedError):
    """请求的数据窗口已过期被淘汰，或请求时间戳过期。"""
