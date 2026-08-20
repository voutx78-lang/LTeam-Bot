"""Additive product schema for the LT Market Mini App.

The legacy bot still uses the original tables.  These migrations are deliberately
additive so both interfaces can run during the transition without losing data.
"""

import sqlite3


def _add_columns(connection: sqlite3.Connection, statements: list[str]) -> None:
    for statement in statements:
        try:
            connection.execute(statement)
        except sqlite3.OperationalError:
            pass


def init_product_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER PRIMARY KEY,
            market_role TEXT NOT NULL DEFAULT 'both',
            theme TEXT NOT NULL DEFAULT 'system',
            notification_settings TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL DEFAULT '',
            route TEXT NOT NULL DEFAULT 'home',
            entity_id INTEGER NOT NULL DEFAULT 0,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_notification_user ON notification_events(user_id, is_read, id DESC)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS marketplace_drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            draft_type TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, draft_type)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS listing_packages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER NOT NULL,
            package_key TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            price INTEGER NOT NULL,
            delivery_time TEXT NOT NULL,
            revisions INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            UNIQUE(listing_id, package_key)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS deal_deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            version INTEGER NOT NULL,
            comment TEXT NOT NULL,
            file_data TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            UNIQUE(deal_id, version)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS revision_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            requester_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,
            resolved_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS deal_milestones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            amount INTEGER NOT NULL DEFAULT 0,
            deadline TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'planned',
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS recently_viewed (
            user_id INTEGER NOT NULL,
            listing_id INTEGER NOT NULL,
            viewed_at TEXT NOT NULL,
            PRIMARY KEY(user_id, listing_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fsm_storage (
            storage_key TEXT PRIMARY KEY,
            state TEXT,
            data TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        )
        """
    )

    _add_columns(
        connection,
        [
            "ALTER TABLE users ADD COLUMN bio TEXT DEFAULT ''",
            "ALTER TABLE users ADD COLUMN skills_json TEXT DEFAULT '[]'",
            "ALTER TABLE users ADD COLUMN market_role TEXT DEFAULT 'both'",
            "ALTER TABLE listings ADD COLUMN revisions INTEGER DEFAULT 1",
            "ALTER TABLE listings ADD COLUMN requirements TEXT DEFAULT ''",
            "ALTER TABLE listings ADD COLUMN result_description TEXT DEFAULT ''",
            "ALTER TABLE deals ADD COLUMN terms_json TEXT DEFAULT '{}'",
            "ALTER TABLE deals ADD COLUMN revision_limit INTEGER DEFAULT 1",
            "ALTER TABLE deals ADD COLUMN updated_at TEXT",
            "ALTER TABLE reviews ADD COLUMN quality_rating INTEGER DEFAULT 0",
            "ALTER TABLE reviews ADD COLUMN communication_rating INTEGER DEFAULT 0",
            "ALTER TABLE reviews ADD COLUMN deadline_rating INTEGER DEFAULT 0",
            "ALTER TABLE reviews ADD COLUMN reviewee_id INTEGER DEFAULT 0",
        ],
    )
