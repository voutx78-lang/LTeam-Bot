"""Small in-process diagnostics buffer for Telegram update failures.

The Render service runs the bot and Flask API in one Python process.  Keeping
the last failures in memory lets an authenticated owner inspect the real
production traceback without exposing sensitive data in a public endpoint.
"""

from __future__ import annotations

import secrets
import threading
import traceback
from collections import deque
from datetime import datetime, timezone
from typing import Any


_ERRORS: deque[dict[str, Any]] = deque(maxlen=30)
_LOCK = threading.Lock()


def record_update_error(exception: BaseException, update: Any) -> str:
    reference = secrets.token_hex(3).upper()
    message = getattr(update, "message", None)
    callback = getattr(update, "callback_query", None)
    actor = getattr(message, "from_user", None) or getattr(callback, "from_user", None)
    payload = {
        "reference": reference,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "error_type": type(exception).__name__,
        "message": str(exception)[:2000],
        "traceback": "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))[-12000:],
        "update_id": getattr(update, "update_id", None),
        "user_id": getattr(actor, "id", None),
        "kind": "callback" if callback else "message" if message else "other",
        "command": (getattr(message, "text", "") or "")[:120],
        "callback_data": (getattr(callback, "data", "") or "")[:120],
    }
    with _LOCK:
        _ERRORS.appendleft(payload)
    return reference


def recent_update_errors() -> list[dict[str, Any]]:
    with _LOCK:
        return [dict(item) for item in _ERRORS]
