"""Защищённый API и раздача собранной Telegram Mini App."""

import hashlib
import hmac
import json
import os
import sqlite3
import time
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qsl, urlencode
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_file, send_from_directory
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(value.strip()) for value in os.getenv("ADMIN_IDS", "").split(",") if value.strip().isdigit()}
app = Flask(__name__, static_folder=str(BASE_DIR / "Web" / "dist"), static_url_path="")
AVATAR_CACHE: dict[int, tuple[float, bytes, str]] = {}


def avatar_endpoint(user_id: int) -> str:
    return f"{request.host_url.rstrip('/')}/api/users/{user_id}/avatar"


def listing_cover_endpoint(listing_id: int) -> str:
    return f"{request.host_url.rstrip('/')}/api/listings/{listing_id}/cover"


def notify_admins(text: str) -> None:
    """Best-effort notification for MiniApp actions; the marketplace action itself stays available if Telegram is slow."""
    if not BOT_TOKEN:
        return
    for admin_id in ADMIN_IDS:
        try:
            body = urlencode({"chat_id": admin_id, "text": text}).encode()
            urlopen(Request(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=body), timeout=4).read()
        except Exception:
            continue


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
        rows = connection.execute("""SELECT l.id, l.title, l.category, l.price, COALESCE(l.description, '') AS description,
            l.seller_id, COALESCE(l.delivery_time, '') AS delivery_time, COALESCE(u.username, '') AS seller_username,
            COALESCE(u.display_name, '') AS seller_name, COALESCE(l.image_data, '') AS image_data,
            COALESCE(l.portfolio_data, '[]') AS portfolio_data
            , COALESCE(r.avg_rating, 0) AS avg_rating, COALESCE(r.reviews_count, 0) AS reviews_count
            FROM listings l LEFT JOIN users u ON u.user_id=l.seller_id
            LEFT JOIN (SELECT seller_id, AVG(rating) AS avg_rating, COUNT(*) AS reviews_count FROM reviews GROUP BY seller_id) r ON r.seller_id=l.seller_id
            WHERE l.status='active' ORDER BY COALESCE(l.is_top,0) DESC, l.id DESC LIMIT 50""").fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["avatar_url"] = avatar_endpoint(item["seller_id"])
        if item["image_data"].startswith("tg:"):
            item["image_data"] = listing_cover_endpoint(item["id"])
        result.append(item)
    return jsonify(result)


@app.get("/api/listings/<int:listing_id>/cover")
def listing_cover(listing_id: int):
    with db() as connection:
        row = connection.execute("SELECT COALESCE(image_data, '') AS image_data FROM listings WHERE id=?", (listing_id,)).fetchone()
    file_id = str(row["image_data"] if row else "")
    if not file_id.startswith("tg:") or not BOT_TOKEN:
        return "", 404
    try:
        file_info = json.loads(urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id[3:]}", timeout=6).read())
        path = file_info.get("result", {}).get("file_path")
        if not path:
            return "", 404
        data = urlopen(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}", timeout=8).read()
    except Exception:
        return "", 404
    return send_file(BytesIO(data), mimetype="image/jpeg", max_age=3600)


@app.get("/api/users/<int:user_id>/avatar")
def user_avatar(user_id: int):
    """Return a Telegram profile image without exposing the bot token to MiniApp clients."""
    if not BOT_TOKEN:
        return "", 404
    cached = AVATAR_CACHE.get(user_id)
    if cached and cached[0] > time.time():
        return send_file(BytesIO(cached[1]), mimetype=cached[2], max_age=3600)
    try:
        photos = json.loads(urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/getUserProfilePhotos?user_id={user_id}&limit=1", timeout=6).read())
        entries = photos.get("result", {}).get("photos", [])
        if not entries:
            return "", 404
        file_id = entries[0][-1]["file_id"]
        file_info = json.loads(urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}", timeout=6).read())
        path = file_info.get("result", {}).get("file_path")
        if not path:
            return "", 404
        data = urlopen(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}", timeout=8).read()
    except Exception:
        return "", 404
    AVATAR_CACHE[user_id] = (time.time() + 3600, data, "image/jpeg")
    return send_file(BytesIO(data), mimetype="image/jpeg", max_age=3600)


@app.post("/api/listings")
def create_listing():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    title = str(payload.get("title", "")).strip()[:120]
    category = str(payload.get("category", "Другое")).strip()[:80]
    description = str(payload.get("description", "")).strip()[:2000]
    delivery_time = str(payload.get("delivery_time", "По договорённости")).strip()[:80]
    image_data = str(payload.get("image_data", "")).strip()
    portfolio = payload.get("portfolio_data", [])
    if not isinstance(portfolio, list):
        portfolio = []
    portfolio = [str(image).strip() for image in portfolio[:4] if str(image).strip().startswith("data:image/")]
    if any(len(image) > 500_000 for image in portfolio):
        return jsonify({"error": "validation", "message": "Каждый пример портфолио должен быть до 350 КБ."}), 400
    if image_data and (not image_data.startswith("data:image/") or len(image_data) > 900_000):
        return jsonify({"error": "validation", "message": "Изображение должно быть картинкой до 650 КБ."}), 400
    try:
        price = max(1, int(payload.get("price", 0)))
    except (TypeError, ValueError):
        price = 0
    if not title or not description or not price:
        return jsonify({"error": "validation", "message": "Заполните название, описание и цену."}), 400
    with db() as connection:
        cursor = connection.execute(
            """INSERT INTO listings (seller_id, title, category, item_type, condition, price, description, delivery_time, image_data, portfolio_data, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, title, category, "Услуга", "new", price, description, delivery_time, image_data, json.dumps(portfolio, ensure_ascii=False), "pending", datetime.now().isoformat()),
        )
        connection.commit()
    notify_admins(f"Новая услуга на модерации\n\n{title}\nКатегория: {category}\nЦена: {price} ₽\nАвтор: {user_id}")
    return jsonify({"ok": True, "listing_id": cursor.lastrowid, "status": "pending"}), 201


@app.get("/api/orders")
def orders():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("""SELECT o.id, o.customer_id, o.title, o.category, o.budget, COALESCE(o.description, '') AS description,
            o.status, o.deadline, o.created_at, COALESCE(o.reference_image_data, '') AS reference_image_data, COALESCE(u.username, '') AS customer_username, COALESCE(u.display_name, '') AS customer_name
            FROM orders o LEFT JOIN users u ON u.user_id=o.customer_id
            WHERE o.status IN ('active','open','approved') OR o.customer_id=? ORDER BY o.id DESC LIMIT 50""", (user_id,)).fetchall()
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
    reference_image_data = str(payload.get("reference_image_data", "")).strip()
    if reference_image_data and (not reference_image_data.startswith("data:image/") or len(reference_image_data) > 900_000):
        return jsonify({"error": "validation", "message": "Референс должен быть картинкой до 650 КБ."}), 400
    try:
        budget = max(1, int(payload.get("budget", 0)))
    except (TypeError, ValueError):
        budget = 0
    if not title or not description or not budget:
        return jsonify({"error": "validation", "message": "Заполните название, описание и бюджет."}), 400
    with db() as connection:
        cursor = connection.execute(
            """INSERT INTO orders (customer_id, title, category, budget, description, deadline, reference_image_data, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, title, category, budget, description, deadline, reference_image_data, "moderation", datetime.now().isoformat()),
        )
        connection.commit()
    notify_admins(f"Новый заказ на модерации\n\n{title}\nКатегория: {category}\nБюджет: до {budget} ₽\nАвтор: {user_id}")
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
        rows = connection.execute("SELECT id, title, category, price, COALESCE(description, '') AS description, COALESCE(image_data, '') AS image_data, COALESCE(portfolio_data, '[]') AS portfolio_data, status, created_at FROM listings WHERE seller_id=? ORDER BY id DESC LIMIT 100", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/applications/mine")
def my_applications():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("""SELECT a.id, a.order_id, a.price, a.deadline, a.comment, a.status, a.created_at,
            o.title, o.category, o.budget, COALESCE(u.username, '') AS customer_username, COALESCE(u.display_name, '') AS customer_name
            FROM order_applications a JOIN orders o ON o.id=a.order_id
            LEFT JOIN users u ON u.user_id=a.customer_id
            WHERE a.executor_id=? ORDER BY a.id DESC LIMIT 100""", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/orders/<int:order_id>/applications")
def order_applications(order_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        order = connection.execute("SELECT customer_id FROM orders WHERE id=?", (order_id,)).fetchone()
        if not order or int(order["customer_id"]) != user_id:
            return jsonify({"error": "forbidden"}), 403
        rows = connection.execute("""SELECT a.id, a.executor_id, a.price, a.deadline, a.comment, a.status, a.created_at,
            COALESCE(u.username, '') AS executor_username, COALESCE(u.display_name, '') AS executor_name
            FROM order_applications a LEFT JOIN users u ON u.user_id=a.executor_id
            WHERE a.order_id=? ORDER BY a.id DESC""", (order_id,)).fetchall()
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


@app.get("/api/users/<int:user_id>/public")
def public_profile(user_id: int):
    """Public marketplace profile: no private contacts or payment data."""
    if not current_user_id():
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        user = connection.execute("SELECT COALESCE(username, '') AS username, COALESCE(display_name, '') AS display_name, COALESCE(verified, 0) AS verified FROM users WHERE user_id=?", (user_id,)).fetchone()
        stats = connection.execute("SELECT COALESCE(AVG(rating), 0) AS rating, COUNT(*) AS reviews_count FROM reviews WHERE seller_id=?", (user_id,)).fetchone()
        listings = connection.execute("SELECT id, title, category, price, COALESCE(delivery_time, '') AS delivery_time, COALESCE(image_data, '') AS image_data, COALESCE(portfolio_data, '[]') AS portfolio_data FROM listings WHERE seller_id=? AND status='active' ORDER BY id DESC LIMIT 30", (user_id,)).fetchall()
    if not user:
        return jsonify({"error": "not_found"}), 404
    listing_data = []
    for row in listings:
        item = dict(row)
        if item["image_data"].startswith("tg:"):
            item["image_data"] = listing_cover_endpoint(item["id"])
        listing_data.append(item)
    return jsonify({"id": user_id, **dict(user), **dict(stats), "avatar_url": avatar_endpoint(user_id), "listings": listing_data})


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
