"""开发启动入口：uv run uvicorn main:app --reload 或 uv run python main.py"""

import uvicorn

from marketfeed.api import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
