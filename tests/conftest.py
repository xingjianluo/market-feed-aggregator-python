"""公共夹具：让每条用例都打印输入、判定依据与最终结果。"""

from __future__ import annotations

import logging

import pytest

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("marketfeed.tests")


@pytest.fixture
def case_logger():
    def _log(title: str, **fields):
        log.info("CASE=%s | %s", title, " | ".join(f"{k}={v!r}" for k, v in fields.items()))

    return _log
