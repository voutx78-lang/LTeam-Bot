"""?????? ? SQLite-????? LTeam Market."""

import os
import sqlite3
import threading
from pathlib import Path

DB_PATH = str(Path(__file__).resolve().parent.parent / "market.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
_CLOUD_LOCK = threading.Lock()
_CLOUD_READY = False


def _cloud_connect():
    """Optional durable backup for free Render instances without rewriting the SQLite bot."""
    if not DATABASE_URL:
        return None
    try:
        import psycopg
        return psycopg.connect(DATABASE_URL, connect_timeout=5)
    except Exception:
        return None


def _prepare_cloud(connection) -> None:
    global _CLOUD_READY
    if _CLOUD_READY:
        return
    with connection.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS lteam_sqlite_backup (
            backup_key TEXT PRIMARY KEY, payload BYTEA NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())""")
    connection.commit()
    _CLOUD_READY = True


def _restore_cloud_snapshot() -> None:
    """Restore only for a new container; never overwrite an existing local working database."""
    path = Path(DB_PATH)
    if not DATABASE_URL or (path.exists() and path.stat().st_size > 0):
        return
    with _CLOUD_LOCK:
        cloud = _cloud_connect()
        if not cloud:
            return
        try:
            _prepare_cloud(cloud)
            with cloud.cursor() as cursor:
                cursor.execute("SELECT payload FROM lteam_sqlite_backup WHERE backup_key='market'")
                row = cursor.fetchone()
            if row and row[0]:
                temporary = path.with_suffix(".restore")
                temporary.write_bytes(bytes(row[0]))
                temporary.replace(path)
        except Exception:
            pass
        finally:
            cloud.close()


def _backup_cloud_snapshot() -> None:
    path = Path(DB_PATH)
    if not DATABASE_URL or not path.exists() or path.stat().st_size == 0:
        return
    with _CLOUD_LOCK:
        cloud = _cloud_connect()
        if not cloud:
            return
        try:
            _prepare_cloud(cloud)
            payload = path.read_bytes()
            with cloud.cursor() as cursor:
                cursor.execute("""INSERT INTO lteam_sqlite_backup (backup_key, payload, updated_at)
                    VALUES ('market', %s, NOW())
                    ON CONFLICT (backup_key) DO UPDATE SET payload=EXCLUDED.payload, updated_at=NOW()""", (payload,))
            cloud.commit()
        except Exception:
            pass
        finally:
            cloud.close()


class SyncedConnection:
    """SQLite-compatible proxy that snapshots only after successful commits."""
    def __init__(self, connection: sqlite3.Connection):
        object.__setattr__(self, "_connection", connection)

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __setattr__(self, name, value):
        if name == "_connection":
            object.__setattr__(self, name, value)
        else:
            setattr(self._connection, name, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type:
            self._connection.rollback()
        else:
            self.commit()
        return False

    def commit(self):
        result = self._connection.commit()
        _backup_cloud_snapshot()
        return result

    def close(self):
        return self._connection.close()

def db():
    _restore_cloud_snapshot()
    return SyncedConnection(sqlite3.connect(DB_PATH))


def init_db():
    with db() as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            created_at TEXT
        )
        """)

        try:
            cur.execute("ALTER TABLE users ADD COLUMN verified INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id INTEGER,
            title TEXT,
            category TEXT,
            item_type TEXT,
            condition TEXT,
            price INTEGER,
            description TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT
        )
        """)

        for column_sql in [
            "ALTER TABLE listings ADD COLUMN is_top INTEGER DEFAULT 0",
            "ALTER TABLE listings ADD COLUMN is_highlight INTEGER DEFAULT 0",
            "ALTER TABLE listings ADD COLUMN bumped_at TEXT",
            "ALTER TABLE listings ADD COLUMN top_until TEXT",
            "ALTER TABLE listings ADD COLUMN highlight_until TEXT",
            "ALTER TABLE listings ADD COLUMN seller_requisites TEXT",
            "ALTER TABLE listings ADD COLUMN delivery_time TEXT",
            "ALTER TABLE listings ADD COLUMN image_data TEXT",
            "ALTER TABLE listings ADD COLUMN portfolio_data TEXT DEFAULT '[]'",
        ]:
            try:
                cur.execute(column_sql)
            except sqlite3.OperationalError:
                pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            user_id INTEGER,
            promo_type TEXT,
            amount INTEGER,
            payment_method TEXT,
            status TEXT DEFAULT 'waiting_payment',
            receipt TEXT,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            user_id INTEGER,
            listing_id INTEGER,
            UNIQUE(user_id, listing_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            buyer_id INTEGER,
            seller_id INTEGER,
            amount INTEGER,
            commission INTEGER,
            payout INTEGER,
            payment_method TEXT,
            status TEXT,
            receipt TEXT,
            created_at TEXT
        )
        """)

        for column_sql in [
            "ALTER TABLE deals ADD COLUMN order_id INTEGER DEFAULT 0",
            "ALTER TABLE deals ADD COLUMN source_type TEXT DEFAULT 'listing'",
            "ALTER TABLE deals ADD COLUMN final_price_set_by INTEGER DEFAULT 0",
            "ALTER TABLE deals ADD COLUMN final_price_confirmed_by INTEGER DEFAULT 0",
            "ALTER TABLE deals ADD COLUMN payment_requested_by INTEGER DEFAULT 0",
            "ALTER TABLE deals ADD COLUMN payment_approved_by INTEGER DEFAULT 0",
            "ALTER TABLE deals ADD COLUMN payment_rejected_by INTEGER DEFAULT 0",
            "ALTER TABLE deals ADD COLUMN payment_admin_comment TEXT",
            "ALTER TABLE deals ADD COLUMN payment_approved_at TEXT",
            "ALTER TABLE deals ADD COLUMN payment_rejected_at TEXT",
        ]:
            try:
                cur.execute(column_sql)
            except sqlite3.OperationalError:
                pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            text TEXT,
            status TEXT DEFAULT 'open',
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER,
            reviewer_id INTEGER,
            seller_id INTEGER,
            rating INTEGER,
            text TEXT,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            listing_id INTEGER,
            reason TEXT,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            title TEXT,
            category TEXT,
            budget INTEGER,
            description TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT
        )
        """)

        try:
            cur.execute("ALTER TABLE orders ADD COLUMN deadline TEXT")
        except sqlite3.OperationalError:
            pass

        try:
            cur.execute("ALTER TABLE orders ADD COLUMN reference_image_data TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_screens (
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER,
            message_id INTEGER
        )
        """)

        for column_sql in [
            "ALTER TABLE reports ADD COLUMN target_type TEXT DEFAULT 'listing'",
            "ALTER TABLE reports ADD COLUMN target_id INTEGER",
            "ALTER TABLE reports ADD COLUMN status TEXT DEFAULT 'new'",
            "ALTER TABLE orders ADD COLUMN executor_id INTEGER",
        ]:
            try:
                cur.execute(column_sql)
            except sqlite3.OperationalError:
                pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS banned_users
 (
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            banned_by INTEGER,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS deal_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER,
            sender_id INTEGER,
            receiver_id INTEGER,
            text TEXT,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS listing_discussion_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            buyer_id INTEGER,
            seller_id INTEGER,
            message TEXT,
            status TEXT DEFAULT 'new',
            deal_id INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS order_applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            executor_id INTEGER,
            customer_id INTEGER,
            status TEXT DEFAULT 'new',
            created_at TEXT,
            UNIQUE(order_id, executor_id)
        )
        """)

        for column_sql in [
            "ALTER TABLE order_applications ADD COLUMN price INTEGER DEFAULT 0",
            "ALTER TABLE order_applications ADD COLUMN deadline TEXT",
            "ALTER TABLE order_applications ADD COLUMN comment TEXT",
            "ALTER TABLE order_applications ADD COLUMN updated_at TEXT",
            "ALTER TABLE order_applications ADD COLUMN executor_card_mask TEXT",
            "ALTER TABLE order_applications ADD COLUMN executor_ton_mask TEXT",
        ]:
            try:
                cur.execute(column_sql)
            except sqlite3.OperationalError:
                pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS order_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            sender_id INTEGER,
            receiver_id INTEGER,
            text TEXT,
            created_at TEXT
        )
        """)


        # ===== СЛУЖЕБНЫЕ ТАБЛИЦЫ ДЛЯ РОЛЕЙ, МУТОВ И АДМИН-ЛОГОВ =====
        cur.execute("""
        CREATE TABLE IF NOT EXISTS staff_roles (
            user_id INTEGER PRIMARY KEY,
            role TEXT DEFAULT 'user',
            assigned_by INTEGER,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_action_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id INTEGER,
            target_id INTEGER,
            action TEXT,
            details TEXT,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_action_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            requester_id INTEGER,
            target_id INTEGER,
            action TEXT,
            details TEXT,
            status TEXT DEFAULT 'pending',
            reviewer_id INTEGER,
            created_at TEXT,
            reviewed_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS muted_users (
            user_id INTEGER PRIMARY KEY,
            muted_until TEXT,
            reason TEXT,
            muted_by INTEGER DEFAULT 0,
            created_at TEXT
        )
        """)

        try:
            cur.execute("ALTER TABLE muted_users ADD COLUMN muted_by INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_message_limits (
            user_id INTEGER PRIMARY KEY,
            window_start TEXT,
            count INTEGER DEFAULT 0,
            strikes INTEGER DEFAULT 0
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS security_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            event_type TEXT,
            context TEXT,
            text TEXT,
            status TEXT DEFAULT 'new',
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            target TEXT,
            text TEXT,
            sent_count INTEGER DEFAULT 0,
            total_count INTEGER DEFAULT 0,
            created_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            admin_id INTEGER,
            reason TEXT,
            created_at TEXT
        )
        """)

        conn.commit()
