"""Единый процесс: Telegram-бот и API MiniApp используют одну базу данных."""

import asyncio
import os
import threading
import time

from cloud_state import restore_sqlite, snapshot_sqlite


def run_api(app) -> None:
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False, use_reloader=False)


if __name__ == "__main__":
    try:
        restored = restore_sqlite()
        print("Cloud database restored" if restored else "Cloud database is empty")
    except Exception as error:
        print(f"Cloud restore skipped: {error}")

    from webapp_server import app
    from main import main as bot_main

    def backup_loop() -> None:
        while True:
            time.sleep(20)
            try:
                snapshot_sqlite()
            except Exception as error:
                print(f"Cloud backup failed: {error}")

    threading.Thread(target=backup_loop, daemon=True).start()
    threading.Thread(target=run_api, args=(app,), daemon=True).start()
    asyncio.run(bot_main())
