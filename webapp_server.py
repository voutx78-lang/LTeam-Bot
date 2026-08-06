"""Защищённый API и раздача собранной Telegram Mini App."""

import hashlib
import hmac
import json
import os
import sqlite3
from pathlib import Path
from urllib.parse import parse_qsl

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(value.strip()) for value in os.getenv("ADMIN_IDS", "").split(",") if value.strip().isdigit()}
app = Flask(__name__, static_folder=str(BASE_DIR / "Web" / "dist"), static_url_path="")


@app.after_request
def add_cors_headers(response):
    """Разрешает опубликованной MiniApp обращаться к отдельному API-сервису."""
    response.headers["Access-Control-Allow-Origin"] = os.getenv("WEBAPP_ORIGIN", "*")
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Telegram-Init-Data"
    return response


def get_user() -> dict:
    """Проверяет подпись Telegram initData, не доверяя данным, пришедшим от браузера."""
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    if not init_data or not BOT_TOKEN:
        return {}
    values = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = values.pop("hash", "")
    check_string = "\n".join(f"{key}={values[key]}" for key in sorted(values))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    expected_hash = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    if not received_hash or not hmac.compare_digest(expected_hash, received_hash):
        return {}
    try:
        return json.loads(values.get("user", "{}"))
    except json.JSONDecodeError:
        return {}


def db() -> sqlite3.Connection:
    connection = sqlite3.connect(BASE_DIR / "market.db")
    connection.row_factory = sqlite3.Row
    return connection


def current_user_id() -> int | None:
    user = get_user()
    return int(user["id"]) if user.get("id") else None


def require_user() -> int:
    user_id = current_user_id()
    if not user_id:
        raise PermissionError
    return user_id


@app.get("/api/me")
def me():
    user = get_user()
    if not user:
        return jsonify({"authenticated": False, "is_admin": False})
    return jsonify({"authenticated": True, "id": user.get("id"), "name": " ".join(filter(None, [user.get("first_name"), user.get("last_name")])), "username": user.get("username", ""), "is_admin": int(user.get("id", 0)) in ADMIN_IDS})


@app.get("/api/listings")
def listings():
    if not current_user_id():
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("SELECT id, title, category, price, COALESCE(description, '') AS description, seller_id, COALESCE(delivery_time, '') AS delivery_time FROM listings WHERE status='active' ORDER BY COALESCE(is_top,0) DESC, id DESC LIMIT 50").fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/listings")
def create_listing():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    title = str(payload.get("title", "")).strip()[:120]
    category = str(payload.get("category", "Другое")).strip()[:80]
    description = str(payload.get("description", "")).strip()[:2000]
    try:
        price = max(1, int(payload.get("price", 0)))
    except (TypeError, ValueError):
        price = 0
    if not title or not description or not price:
        return jsonify({"error": "validation", "message": "Заполните название, описание и цену."}), 400
    with db() as connection:
        cursor = connection.execute(
            """INSERT INTO listings (seller_id, title, category, item_type, condition, price, description, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, title, category, "Услуга", "new", price, description, "pending", datetime.now().isoformat()),
        )
        connection.commit()
    return jsonify({"ok": True, "listing_id": cursor.lastrowid, "status": "pending"}), 201


@app.get("/api/orders")
def orders():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("SELECT id, title, category, budget, COALESCE(description, '') AS description, status, deadline, created_at FROM orders WHERE status IN ('active','open','approved') OR customer_id=? ORDER BY id DESC LIMIT 50", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/orders")
def create_order():
    """Create an order from the MiniApp using the same marketplace table as the bot."""
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    title = str(payload.get("title", "")).strip()[:120]
    category = str(payload.get("category", "Другое")).strip()[:80]
    description = str(payload.get("description", "")).strip()[:2000]
    deadline = str(payload.get("deadline", "По договорённости")).strip()[:80]
    try:
        budget = max(1, int(payload.get("budget", 0)))
    except (TypeError, ValueError):
        budget = 0
    if not title or not description or not budget:
        return jsonify({"error": "validation", "message": "Заполните название, описание и бюджет."}), 400
    with db() as connection:
        cursor = connection.execute(
            """INSERT INTO orders (customer_id, title, category, budget, description, deadline, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, title, category, budget, description, deadline, "moderation", datetime.now().isoformat()),
        )
        connection.commit()
    return jsonify({"ok": True, "order_id": cursor.lastrowid, "status": "moderation"}), 201


@app.post("/api/orders/<int:order_id>/applications")
def create_order_application(order_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    comment = str(payload.get("comment", "")).strip()[:1200]
    deadline = str(payload.get("deadline", "По договорённости")).strip()[:80]
    try:
        price = max(1, int(payload.get("price", 0)))
    except (TypeError, ValueError):
        price = 0
    if len(comment) < 5 or not price:
        return jsonify({"error": "validation", "message": "Укажите цену и коротко расскажите о своём предложении."}), 400
    with db() as connection:
        order = connection.execute("SELECT customer_id FROM orders WHERE id=? AND status='active'", (order_id,)).fetchone()
        if not order:
            return jsonify({"error": "not_found", "message": "Заказ уже недоступен."}), 404
        if int(order["customer_id"]) == user_id:
            return jsonify({"error": "validation", "message": "Нельзя откликнуться на свой заказ."}), 400
        now = datetime.now().isoformat()
        connection.execute("""INSERT INTO order_applications (order_id, executor_id, customer_id, price, deadline, comment, status, created_at, updated_at, executor_card_mask, executor_ton_mask)
            VALUES (?, ?, ?, ?, ?, ?, 'new', ?, ?, ?, '')
            ON CONFLICT(order_id, executor_id) DO UPDATE SET price=excluded.price, deadline=excluded.deadline, comment=excluded.comment, status='new', updated_at=excluded.updated_at""",
            (order_id, user_id, order["customer_id"], price, deadline, comment, now, now, "будет запрошен при выводе"))
        connection.commit()
    return jsonify({"ok": True, "status": "new"}), 201


@app.get("/api/listings/mine")
def my_listings():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("SELECT id, title, category, price, COALESCE(description, '') AS description, status, created_at FROM listings WHERE seller_id=? ORDER BY id DESC LIMIT 100", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


def can_access_order(connection: sqlite3.Connection, order_id: int, user_id: int) -> bool:
    order = connection.execute("SELECT customer_id, executor_id FROM orders WHERE id=?", (order_id,)).fetchone()
    if not order:
        return False
    if user_id in {order["customer_id"], order["executor_id"]}:
        return True
    return connection.execute("SELECT 1 FROM order_messages WHERE order_id=? AND (sender_id=? OR receiver_id=?) LIMIT 1", (order_id, user_id, user_id)).fetchone() is not None


def can_access_deal(connection: sqlite3.Connection, deal_id: int, user_id: int) -> bool:
    deal = connection.execute("SELECT buyer_id, seller_id FROM deals WHERE id=?", (deal_id,)).fetchone()
    return bool(deal and user_id in {deal["buyer_id"], deal["seller_id"]})


@app.get("/api/orders/<int:order_id>/messages")
def order_messages(order_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        if not can_access_order(connection, order_id, user_id):
            return jsonify({"error": "forbidden"}), 403
        rows = connection.execute("SELECT id, sender_id, receiver_id, text, created_at FROM order_messages WHERE order_id=? ORDER BY id ASC LIMIT 300", (order_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/orders/<int:order_id>/messages")
def send_order_message(order_id: int):
    user_id = current_user_id()
    text = str((request.get_json(silent=True) or {}).get("text", "")).strip()[:1200]
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if not text:
        return jsonify({"error": "validation", "message": "Введите сообщение."}), 400
    with db() as connection:
        if not can_access_order(connection, order_id, user_id):
            return jsonify({"error": "forbidden"}), 403
        order = connection.execute("SELECT customer_id, executor_id FROM orders WHERE id=?", (order_id,)).fetchone()
        receiver_id = order["executor_id"] if order["customer_id"] == user_id else order["customer_id"]
        cursor = connection.execute("INSERT INTO order_messages(order_id, sender_id, receiver_id, text, created_at) VALUES (?, ?, ?, ?, ?)", (order_id, user_id, receiver_id, text, datetime.now().isoformat()))
        connection.commit()
    return jsonify({"ok": True, "id": cursor.lastrowid}), 201


@app.get("/api/deals/<int:deal_id>/messages")
def deal_messages(deal_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        if not can_access_deal(connection, deal_id, user_id):
            return jsonify({"error": "forbidden"}), 403
        rows = connection.execute("SELECT id, sender_id, receiver_id, text, created_at FROM deal_messages WHERE deal_id=? ORDER BY id ASC LIMIT 300", (deal_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/deals/<int:deal_id>/messages")
def send_deal_message(deal_id: int):
    user_id = current_user_id()
    text = str((request.get_json(silent=True) or {}).get("text", "")).strip()[:1200]
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if not text:
        return jsonify({"error": "validation", "message": "Введите сообщение."}), 400
    with db() as connection:
        if not can_access_deal(connection, deal_id, user_id):
            return jsonify({"error": "forbidden"}), 403
        deal = connection.execute("SELECT buyer_id, seller_id FROM deals WHERE id=?", (deal_id,)).fetchone()
        receiver_id = deal["seller_id"] if deal["buyer_id"] == user_id else deal["buyer_id"]
        cursor = connection.execute("INSERT INTO deal_messages(deal_id, sender_id, receiver_id, text, created_at) VALUES (?, ?, ?, ?, ?)", (deal_id, user_id, receiver_id, text, datetime.now().isoformat()))
        connection.commit()
    return jsonify({"ok": True, "id": cursor.lastrowid}), 201


@app.get("/api/deals")
def deals():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("""SELECT d.id, d.buyer_id, d.seller_id, d.amount, d.commission, d.payout, d.status, d.created_at,
            COALESCE(l.title, o.title, 'LTeam deal') AS title
            FROM deals d LEFT JOIN listings l ON l.id=d.listing_id LEFT JOIN orders o ON o.id=d.order_id
            WHERE d.buyer_id=? OR d.seller_id=? ORDER BY d.id DESC LIMIT 50""", (user_id, user_id)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/balance")
def balance():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        row = connection.execute("SELECT COALESCE(available, balance, 0) AS available, COALESCE(frozen, 0) AS frozen, COALESCE(total_earned, 0) AS total_earned, COALESCE(total_withdrawn, 0) AS total_withdrawn FROM user_balances WHERE user_id=?", (user_id,)).fetchone()
    return jsonify(dict(row) if row else {"available": 0, "frozen": 0, "total_earned": 0, "total_withdrawn": 0})


@app.get("/api/admin/summary")
def admin_summary():
    if int(get_user().get("id", 0)) not in ADMIN_IDS:
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        payments = connection.execute("SELECT COUNT(*) FROM deals WHERE status='waiting_admin_confirm'").fetchone()[0]
        disputes = connection.execute("SELECT COUNT(*) FROM deals WHERE status='dispute_open'").fetchone()[0]
        payouts = connection.execute("SELECT COUNT(*) FROM withdrawal_requests WHERE status='pending'").fetchone()[0]
    return jsonify({"payments": payments, "disputes": disputes, "payouts": payouts})


@app.get("/")
def index():
    index_file = Path(app.static_folder) / "index.html"
    if index_file.exists():
        return send_from_directory(app.static_folder, "index.html")
    return jsonify({"status": "ok", "service": "LTeam Market API"})


@app.get("/<path:path>")
def static_files(path: str):
    return send_from_directory(app.static_folder, path)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)
