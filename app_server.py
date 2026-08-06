"""Единый процесс: Telegram-бот и API MiniApp используют одну базу данных."""

import asyncio
import os
import threading

from webapp_server import app
from main import main as bot_main


def run_api() -> None:
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False, use_reloader=False)


if __name__ == "__main__":
    threading.Thread(target=run_api, daemon=True).start()
    asyncio.run(bot_main())
