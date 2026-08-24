"""?????? ? SQLite-????? LTeam Market."""

import sqlite3
from pathlib import Path

from app.product_schema import init_product_schema
from app.star_payments import init_star_schema

DB_PATH = str(Path(__file__).resolve().parent.parent / "market.db")

def db():
    return sqlite3.connect(DB_PATH)


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

        # MiniApp reads public profile data before the bot starts polling.
        # Keep these additions here instead of relying on a later bot handler.
        for column_sql in [
            "ALTER TABLE users ADD COLUMN display_name TEXT",
            "ALTER TABLE users ADD COLUMN first_name TEXT",
            "ALTER TABLE users ADD COLUMN last_name TEXT",
        ]:
            try:
                cur.execute(column_sql)
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

        # The API and the bot share these tables.  They must exist before the
        # web server starts, not only after a finance or dispute action inside
        # the bot has happened for the first time.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_balances (
            user_id INTEGER PRIMARY KEY,
            available INTEGER DEFAULT 0,
            frozen INTEGER DEFAULT 0,
            total_earned INTEGER DEFAULT 0,
            total_withdrawn INTEGER DEFAULT 0,
            updated_at TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS balance_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            deal_id INTEGER,
            withdrawal_id INTEGER,
            tx_type TEXT,
            amount INTEGER,
            balance_after INTEGER DEFAULT 0,
            comment TEXT,
            created_at TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            requisites TEXT,
            status TEXT DEFAULT 'pending',
            admin_id INTEGER,
            admin_comment TEXT,
            created_at TEXT,
            resolved_at TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS deal_disputes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            opened_by INTEGER NOT NULL,
            buyer_id INTEGER,
            seller_id INTEGER,
            reason TEXT,
            status TEXT DEFAULT 'open',
            resolved_by INTEGER,
            resolution TEXT,
            created_at TEXT,
            resolved_at TEXT
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

        # Product-facing tables are isolated from the legacy bot schema so the
        # marketplace can evolve without destructive migrations.
        init_product_schema(conn)
        init_star_schema(conn)

        conn.commit()
