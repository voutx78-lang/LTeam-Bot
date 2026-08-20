"""Persistent marketplace events shared by the API and Telegram notifications."""

from datetime import datetime
import sqlite3


def create_event(
    connection: sqlite3.Connection,
    user_id: int,
    event_type: str,
    title: str,
    body: str = "",
    route: str = "home",
    entity_id: int = 0,
) -> int:
    if not user_id:
        return 0
    cursor = connection.execute(
        """INSERT INTO notification_events
           (user_id, event_type, title, body, route, entity_id, is_read, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 0, ?)""",
        (
            int(user_id),
            str(event_type)[:60],
            str(title)[:180],
            str(body)[:1200],
            str(route)[:80] or "home",
            int(entity_id or 0),
            datetime.now().isoformat(),
        ),
    )
    return int(cursor.lastrowid or 0)
