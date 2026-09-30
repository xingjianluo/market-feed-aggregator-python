"""测试日志辅助：统一打印输入、判定依据与最终结果。"""
from __future__ import annotations

import logging

logger = logging.getLogger("market_feed.tests")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def log_case(title: str, raw_input: object, basis: str, result: object) -> None:
    logger.info("CASE %s | input=%s | basis=%s | result=%s", title, raw_input, basis, result)
