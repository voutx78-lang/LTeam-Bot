"""Persistent SQLite snapshots stored in the configured PostgreSQL database.

The production bot keeps its mature SQLite queries unchanged.  This module
restores that file before startup and periodically saves an atomic snapshot,
so Render's ephemeral filesystem never loses marketplace data.
"""

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
SQLITE_PATH = BASE_DIR / "market.db"
load_dotenv(BASE_DIR / ".env")


def database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def restore_sqlite() -> bool:
    """Restore the latest snapshot when cloud persistence is configured."""
    url = database_url()
    if not url:
        return False
    import psycopg

    with psycopg.connect(url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lteam_sqlite_snapshots (
                    id SMALLINT PRIMARY KEY,
                    data BYTEA NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cursor.execute("SELECT data FROM lteam_sqlite_snapshots WHERE id=1")
            row = cursor.fetchone()
        connection.commit()
    if not row:
        return False
    temporary = SQLITE_PATH.with_suffix(".restore")
    temporary.write_bytes(bytes(row[0]))
    temporary.replace(SQLITE_PATH)
    return True


def snapshot_sqlite() -> bool:
    """Create a consistent SQLite backup and upload it to PostgreSQL."""
    url = database_url()
    if not url or not SQLITE_PATH.exists():
        return False
    snapshot = SQLITE_PATH.with_suffix(".snapshot")
    try:
        source = sqlite3.connect(SQLITE_PATH)
        target = sqlite3.connect(snapshot)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        data = snapshot.read_bytes()
        import psycopg
        with psycopg.connect(url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO lteam_sqlite_snapshots(id, data, updated_at)
                    VALUES (1, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
                """, (data,))
            connection.commit()
        return True
    finally:
        snapshot.unlink(missing_ok=True)
