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


@app.get("/api/orders")
def orders():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("SELECT id, title, category, budget, COALESCE(description, '') AS description, status, deadline, created_at FROM orders WHERE status IN ('active','open','approved') OR customer_id=? ORDER BY id DESC LIMIT 50", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


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
    return send_from_directory(app.static_folder, "index.html")


@app.get("/<path:path>")
def static_files(path: str):
    return send_from_directory(app.static_folder, path)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)
