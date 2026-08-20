"""Persistent aiogram FSM storage backed by the shared marketplace database."""

from __future__ import annotations

import json
from datetime import datetime

from aiogram.fsm.state import State
from aiogram.fsm.storage.base import BaseStorage, StateType, StorageKey

from app.database import db


def _key(value: StorageKey) -> str:
    return ":".join(str(part or "") for part in (
        value.bot_id, value.chat_id, value.user_id, value.thread_id,
        value.business_connection_id, value.destiny,
    ))


class SQLiteStorage(BaseStorage):
    """Keep interrupted forms available across Render process restarts."""

    async def set_state(self, key: StorageKey, state: StateType = None) -> None:
        value = state.state if isinstance(state, State) else state
        with db() as connection:
            connection.execute("""INSERT INTO fsm_storage(storage_key, state, data, updated_at)
                VALUES (?, ?, '{}', ?)
                ON CONFLICT(storage_key) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at""",
                (_key(key), value, datetime.now().isoformat()))
            connection.commit()

    async def get_state(self, key: StorageKey) -> str | None:
        with db() as connection:
            row = connection.execute("SELECT state FROM fsm_storage WHERE storage_key=?", (_key(key),)).fetchone()
        return row[0] if row else None

    async def set_data(self, key: StorageKey, data: dict) -> None:
        payload = json.dumps(data or {}, ensure_ascii=False, default=str)
        with db() as connection:
            connection.execute("""INSERT INTO fsm_storage(storage_key, state, data, updated_at)
                VALUES (?, NULL, ?, ?)
                ON CONFLICT(storage_key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at""",
                (_key(key), payload, datetime.now().isoformat()))
            connection.commit()

    async def get_data(self, key: StorageKey) -> dict:
        with db() as connection:
            row = connection.execute("SELECT data FROM fsm_storage WHERE storage_key=?", (_key(key),)).fetchone()
        if not row:
            return {}
        try:
            value = json.loads(row[0] or "{}")
            return value if isinstance(value, dict) else {}
        except (TypeError, json.JSONDecodeError):
            return {}

    async def close(self) -> None:
        return None
