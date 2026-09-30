"""骨架冒烟：确认包与应用工厂可导入。"""

from marketfeed import MarketDataStore
from marketfeed.api import create_app


def test_smoke():
    store = MarketDataStore()
    app = create_app(store)
    assert app is not None
