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
from app.database import db as shared_db
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(value.strip()) for value in os.getenv("ADMIN_IDS", "").split(",") if value.strip().isdigit()}
OWNER_IDS = {int(value.strip()) for value in os.getenv("OWNER_IDS", "").split(",") if value.strip().isdigit()}
STAFF_ADMIN_IDS = ADMIN_IDS | OWNER_IDS
app = Flask(__name__, static_folder=str(BASE_DIR / "Web" / "dist"), static_url_path="")
AVATAR_CACHE: dict[int, tuple[float, bytes, str]] = {}


def avatar_endpoint(user_id: int) -> str:
    return f"{request.host_url.rstrip('/')}/api/users/{user_id}/avatar"


def listing_cover_endpoint(listing_id: int) -> str:
    return f"{request.host_url.rstrip('/')}/api/listings/{listing_id}/cover"


def order_reference_endpoint(order_id: int) -> str:
    return f"{request.host_url.rstrip('/')}/api/orders/{order_id}/reference"


def notify_admins(text: str) -> None:
    """Best-effort notification for MiniApp actions; the marketplace action itself stays available if Telegram is slow."""
    if not BOT_TOKEN:
        return
    for admin_id in STAFF_ADMIN_IDS:
        try:
            body = urlencode({"chat_id": admin_id, "text": text}).encode()
            urlopen(Request(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=body), timeout=4).read()
        except Exception:
            continue


def notify_user(user_id: int, text: str) -> None:
    """Best-effort user notification; marketplace actions remain successful if Telegram is unavailable."""
    if not BOT_TOKEN or not user_id:
        return
    try:
        body = urlencode({"chat_id": int(user_id), "text": text}).encode()
        urlopen(Request(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=body), timeout=4).read()
    except Exception:
        return


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
    connection = shared_db()
    connection.row_factory = sqlite3.Row
    return connection


def current_user_id() -> int | None:
    user = get_user()
    return int(user["id"]) if user.get("id") else None


@app.get("/api/health")
def health_check():
    """Small non-sensitive readiness check for Render and future monitoring."""
    try:
        with db() as connection:
            connection.execute("SELECT 1").fetchone()
        return jsonify({"ok": True, "storage": "cloud_snapshot" if os.getenv("DATABASE_URL") else "local"})
    except Exception:
        return jsonify({"ok": False}), 503


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
    return jsonify({"authenticated": True, "id": user.get("id"), "name": " ".join(filter(None, [user.get("first_name"), user.get("last_name")])), "username": user.get("username", ""), "is_admin": int(user.get("id", 0)) in STAFF_ADMIN_IDS})


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


@app.get("/api/orders/<int:order_id>/reference")
def order_reference(order_id: int):
    """Serve an optional reference uploaded through the Telegram bot without exposing its file id."""
    with db() as connection:
        row = connection.execute(
            "SELECT COALESCE(reference_image_data, '') AS reference_image_data FROM orders WHERE id=?",
            (order_id,),
        ).fetchone()
    file_id = str(row["reference_image_data"] if row else "")
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
    notify_user(user_id, f"Ваша услуга «{title}» отправлена на модерацию LTeam. После проверки она появится в каталоге.")
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
    result = []
    for row in rows:
        item = dict(row)
        item["customer_avatar_url"] = avatar_endpoint(item["customer_id"])
        if item["reference_image_data"].startswith("tg:"):
            item["reference_image_data"] = order_reference_endpoint(item["id"])
        result.append(item)
    return jsonify(result)


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
    notify_user(user_id, f"Заказ «{title}» отправлен на модерацию LTeam. Мы напишем, когда он станет доступен исполнителям.")
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
    notify_user(int(order["customer_id"]), "На ваш заказ появился новый отклик. Откройте «Мои заказы» в LTeam, чтобы посмотреть предложение.")
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
        rows = connection.execute("""SELECT a.id, a.order_id, a.customer_id, a.price, a.deadline, a.comment, a.status, a.created_at,
            o.title, o.category, o.budget, COALESCE(u.username, '') AS customer_username, COALESCE(u.display_name, '') AS customer_name
            FROM order_applications a JOIN orders o ON o.id=a.order_id
            LEFT JOIN users u ON u.user_id=a.customer_id
            WHERE a.executor_id=? ORDER BY a.id DESC LIMIT 100""", (user_id,)).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["customer_avatar_url"] = avatar_endpoint(item["customer_id"])
        result.append(item)
    return jsonify(result)


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
    result = []
    for row in rows:
        item = dict(row)
        item["executor_avatar_url"] = avatar_endpoint(item["executor_id"])
        result.append(item)
    return jsonify(result)


@app.post("/api/orders/<int:order_id>/applications/<int:application_id>/accept")
def accept_order_application(order_id: int, application_id: int):
    """Turn the chosen response into a protected order deal."""
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        order = connection.execute("SELECT id, customer_id, title, status, COALESCE(executor_id, 0) AS executor_id FROM orders WHERE id=?", (order_id,)).fetchone()
        if not order or int(order["customer_id"]) != user_id:
            return jsonify({"error": "forbidden"}), 403
        if order["status"] not in {"active", "open", "approved"} or int(order["executor_id"] or 0):
            return jsonify({"error": "validation", "message": "Этот заказ уже закрыт или находится на модерации."}), 400
        application = connection.execute("SELECT id, executor_id, price, deadline, comment, status FROM order_applications WHERE id=? AND order_id=?", (application_id, order_id)).fetchone()
        if not application or application["status"] != "new":
            return jsonify({"error": "not_found", "message": "Отклик недоступен."}), 404
        amount = max(1, int(application["price"] or 0))
        commission_percent = max(0, min(100, int(os.getenv("COMMISSION_PERCENT", "10"))))
        commission = int(amount * commission_percent / 100)
        payout = max(0, amount - commission)
        now = datetime.now().isoformat()
        cursor = connection.execute("""INSERT INTO deals (listing_id, order_id, source_type, buyer_id, seller_id, amount, commission, payout, payment_method, status, created_at)
            VALUES (0, ?, 'order', ?, ?, ?, ?, ?, 'admin_card_only', 'discussion', ?)""", (order_id, user_id, application["executor_id"], amount, commission, payout, now))
        connection.execute("UPDATE orders SET executor_id=?, status='discussion' WHERE id=?", (application["executor_id"], order_id))
        connection.execute("UPDATE order_applications SET status=CASE WHEN id=? THEN 'accepted' ELSE 'declined' END, updated_at=? WHERE order_id=?", (application_id, now, order_id))
        connection.commit()
    notify_user(int(application["executor_id"]), f"Вас выбрали исполнителем для заказа «{order['title']}». Откройте сделку и согласуйте детали в чате LTeam.")
    notify_user(user_id, f"Исполнитель выбран для заказа «{order['title']}». Согласуйте детали в чате сделки перед подтверждением цены и оплатой.")
    return jsonify({"ok": True, "deal_id": cursor.lastrowid, "status": "discussion"}), 201


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


def credit_deal_payout(connection: sqlite3.Connection, seller_id: int, deal_id: int, amount: int) -> bool:
    """Idempotently credit a completed deal without allowing a double payout."""
    already_credited = connection.execute(
        "SELECT 1 FROM balance_transactions WHERE deal_id=? AND tx_type='deal_credit' LIMIT 1",
        (deal_id,),
    ).fetchone()
    if already_credited:
        return False
    now = datetime.now().isoformat()
    connection.execute(
        """INSERT OR IGNORE INTO user_balances
           (user_id, available, frozen, total_earned, total_withdrawn, updated_at)
           VALUES (?, 0, 0, 0, 0, ?)""",
        (seller_id, now),
    )
    connection.execute(
        """UPDATE user_balances SET available=COALESCE(available,0)+?,
           total_earned=COALESCE(total_earned,0)+?, updated_at=? WHERE user_id=?""",
        (amount, amount, now, seller_id),
    )
    balance_row = connection.execute("SELECT COALESCE(available,0) FROM user_balances WHERE user_id=?", (seller_id,)).fetchone()
    connection.execute(
        """INSERT INTO balance_transactions
           (user_id, deal_id, withdrawal_id, tx_type, amount, balance_after, comment, created_at)
           VALUES (?, ?, NULL, 'deal_credit', ?, ?, ?, ?)""",
        (seller_id, deal_id, amount, int(balance_row[0] or 0), f"Сделка #{deal_id} завершена покупателем", now),
    )
    return True


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
    notify_user(int(receiver_id or 0), f"Новое сообщение по заказу: {text[:160]}")
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
    notify_user(int(receiver_id or 0), f"Новое сообщение по сделке #{deal_id}: {text[:160]}")
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


@app.post("/api/deals/<int:deal_id>/action")
def deal_action(deal_id: int):
    """Protected MiniApp lifecycle actions; payment approval itself remains with admins."""
    user_id = current_user_id()
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).strip()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        deal = connection.execute(
            "SELECT id, buyer_id, seller_id, amount, commission, payout, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()
        if not deal or user_id not in {int(deal["buyer_id"]), int(deal["seller_id"])}:
            return jsonify({"error": "forbidden"}), 403
        now = datetime.now().isoformat()
        buyer_id, seller_id, status = int(deal["buyer_id"]), int(deal["seller_id"]), str(deal["status"])

        if action == "set_final_price":
            try:
                amount = int(payload.get("amount", 0))
            except (TypeError, ValueError):
                amount = 0
            if user_id != seller_id or status != "discussion" or amount < 100 or amount > 150000:
                return jsonify({"error": "validation", "message": "Итоговую цену может выставить исполнитель после обсуждения."}), 400
            percent = max(0, min(100, int(os.getenv("COMMISSION_PERCENT", "10"))))
            commission = int(amount * percent / 100)
            payout = amount - commission
            connection.execute("""UPDATE deals SET amount=?, commission=?, payout=?, status='waiting_buyer_price_confirm',
                final_price_set_by=? WHERE id=?""", (amount, commission, payout, user_id, deal_id))
            target_user, notice = buyer_id, f"Исполнитель выставил итоговую цену по сделке #{deal_id}. Подтвердите её в MiniApp."
        elif action == "confirm_price":
            if user_id != buyer_id or status != "waiting_buyer_price_confirm":
                return jsonify({"error": "validation", "message": "Подтверждение цены сейчас недоступно."}), 400
            connection.execute("""UPDATE deals SET status='waiting_admin_payment_approval',
                final_price_confirmed_by=?, payment_requested_by=? WHERE id=?""", (user_id, user_id, deal_id))
            target_user, notice = seller_id, f"Покупатель подтвердил цену по сделке #{deal_id}. Ожидаем разрешение оплаты администратором."
            notify_admins(f"Нужна проверка оплаты по сделке #{deal_id}. Сумма: {int(deal['amount'] or 0)} ₽")
        elif action == "mark_done":
            if user_id != seller_id or status != "in_work":
                return jsonify({"error": "validation", "message": "Отметить работу выполненной можно после подтверждения оплаты."}), 400
            connection.execute("UPDATE deals SET status='waiting_buyer_confirm' WHERE id=?", (deal_id,))
            target_user, notice = buyer_id, f"Исполнитель отметил работу по сделке #{deal_id} выполненной. Проверьте результат."
        elif action == "confirm_done":
            if user_id != buyer_id or status != "waiting_buyer_confirm":
                return jsonify({"error": "validation", "message": "Подтверждение выполнения сейчас недоступно."}), 400
            connection.execute("UPDATE deals SET status='completed' WHERE id=?", (deal_id,))
            credited = credit_deal_payout(connection, seller_id, deal_id, int(deal["payout"] or 0))
            target_user, notice = seller_id, f"Сделка #{deal_id} завершена. На баланс зачислено: {int(deal['payout'] or 0)} ₽."
        elif action == "open_dispute":
            reason = str(payload.get("reason", "")).strip()[:1200]
            if status in {"completed", "cancelled", "dispute_open"} or len(reason) < 5:
                return jsonify({"error": "validation", "message": "Опишите проблему подробнее; спор нельзя открыть по закрытой сделке."}), 400
            existing = connection.execute("SELECT id FROM deal_disputes WHERE deal_id=? AND status='open'", (deal_id,)).fetchone()
            if existing:
                return jsonify({"error": "validation", "message": "По этой сделке уже открыт спор."}), 400
            connection.execute("INSERT INTO deal_disputes (deal_id, opened_by, buyer_id, seller_id, reason, status, created_at) VALUES (?, ?, ?, ?, ?, 'open', ?)", (deal_id, user_id, buyer_id, seller_id, reason, now))
            connection.execute("UPDATE deals SET status='dispute_open' WHERE id=?", (deal_id,))
            target_user = seller_id if user_id == buyer_id else buyer_id
            notice = f"По сделке #{deal_id} открыт спор. Администратор LTeam проверит ситуацию."
            notify_admins(f"Открыт спор по сделке #{deal_id}. Причина: {reason}")
        else:
            return jsonify({"error": "validation", "message": "Неизвестное действие по сделке."}), 400
        connection.commit()
    notify_user(target_user, notice)
    return jsonify({"ok": True, "status": "completed" if action == "confirm_done" else ({"set_final_price": "waiting_buyer_price_confirm", "confirm_price": "waiting_admin_payment_approval", "mark_done": "waiting_buyer_confirm", "open_dispute": "dispute_open"}[action])})


@app.get("/api/balance")
def balance():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        row = connection.execute("SELECT COALESCE(available, balance, 0) AS available, COALESCE(frozen, 0) AS frozen, COALESCE(total_earned, 0) AS total_earned, COALESCE(total_withdrawn, 0) AS total_withdrawn FROM user_balances WHERE user_id=?", (user_id,)).fetchone()
    return jsonify(dict(row) if row else {"available": 0, "frozen": 0, "total_earned": 0, "total_withdrawn": 0})


@app.get("/api/balance/history")
def balance_history_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    try:
        with db() as connection:
            rows = connection.execute("""SELECT id, tx_type, amount, balance_after, comment, created_at
                FROM balance_transactions WHERE user_id=? ORDER BY id DESC LIMIT 60""", (user_id,)).fetchall()
        return jsonify([dict(row) for row in rows])
    except sqlite3.OperationalError:
        return jsonify([])


@app.route("/api/tickets", methods=["GET", "POST"])
def tickets_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if request.method == "GET":
        with db() as connection:
            rows = connection.execute("SELECT id, text, status, created_at FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 30", (user_id,)).fetchall()
        return jsonify([dict(row) for row in rows])
    text = str((request.get_json(silent=True) or {}).get("text", "")).strip()[:2000]
    if len(text) < 8:
        return jsonify({"error": "validation", "message": "Опишите вопрос подробнее — минимум 8 символов."}), 400
    with db() as connection:
        cursor = connection.execute("INSERT INTO tickets (user_id, text, status, created_at) VALUES (?, ?, 'open', ?)", (user_id, text, datetime.now().isoformat()))
        connection.commit()
    notify_admins(f"Новое обращение в поддержку\n\nПользователь: {user_id}\n{text}")
    return jsonify({"ok": True, "ticket_id": cursor.lastrowid, "status": "open"}), 201


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


@app.get("/api/users/<int:user_id>/reviews")
def public_user_reviews(user_id: int):
    with db() as connection:
        rows = connection.execute("""SELECT r.id, r.rating, COALESCE(r.text, '') AS text, r.created_at,
            COALESCE(u.display_name, u.username, 'Покупатель LTeam') AS reviewer_name
            FROM reviews r LEFT JOIN users u ON u.user_id=r.reviewer_id
            WHERE r.seller_id=? ORDER BY r.id DESC LIMIT 30""", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/reviews/pending")
def pending_reviews():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("""SELECT d.id AS deal_id, d.seller_id, d.amount, COALESCE(o.title, l.title, 'Сделка LTeam') AS title,
            COALESCE(u.display_name, u.username, 'Исполнитель LTeam') AS seller_name
            FROM deals d LEFT JOIN listings l ON l.id=d.listing_id LEFT JOIN orders o ON o.id=d.order_id
            LEFT JOIN users u ON u.user_id=d.seller_id
            WHERE d.buyer_id=? AND d.status='completed' AND NOT EXISTS
              (SELECT 1 FROM reviews r WHERE r.deal_id=d.id AND r.reviewer_id=?)
            ORDER BY d.id DESC LIMIT 20""", (user_id, user_id)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/deals/<int:deal_id>/review")
def create_review(deal_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    try:
        rating = int(payload.get("rating", 0))
    except (TypeError, ValueError):
        rating = 0
    text = str(payload.get("text", "")).strip()[:1200]
    if rating < 1 or rating > 5 or len(text) < 3:
        return jsonify({"error": "validation", "message": "Поставьте оценку и оставьте короткий отзыв."}), 400
    with db() as connection:
        deal = connection.execute("SELECT buyer_id, seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not deal or int(deal["buyer_id"]) != user_id or deal["status"] != "completed":
            return jsonify({"error": "forbidden", "message": "Отзыв доступен только покупателю после завершённой сделки."}), 403
        exists = connection.execute("SELECT 1 FROM reviews WHERE deal_id=? AND reviewer_id=?", (deal_id, user_id)).fetchone()
        if exists:
            return jsonify({"error": "validation", "message": "Отзыв по этой сделке уже оставлен."}), 400
        connection.execute("INSERT INTO reviews (deal_id, reviewer_id, seller_id, rating, text, created_at) VALUES (?, ?, ?, ?, ?, ?)", (deal_id, user_id, deal["seller_id"], rating, text, datetime.now().isoformat()))
        connection.commit()
    return jsonify({"ok": True}), 201


@app.get("/api/admin/summary")
def admin_summary():
    if int(get_user().get("id", 0)) not in STAFF_ADMIN_IDS:
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        payments = connection.execute("SELECT COUNT(*) FROM deals WHERE status IN ('waiting_payment','waiting_admin_confirm')").fetchone()[0]
        disputes = connection.execute("SELECT COUNT(*) FROM deals WHERE status='dispute_open'").fetchone()[0]
        payouts = connection.execute("SELECT COUNT(*) FROM withdrawal_requests WHERE status='pending'").fetchone()[0]
        moderation = connection.execute("SELECT (SELECT COUNT(*) FROM listings WHERE status IN ('pending','moderation')) + (SELECT COUNT(*) FROM orders WHERE status='moderation')").fetchone()[0]
    return jsonify({"payments": payments, "disputes": disputes, "payouts": payouts, "moderation": moderation})


@app.get("/api/admin/moderation")
def admin_moderation_queue():
    if int(get_user().get("id", 0)) not in STAFF_ADMIN_IDS:
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        listings_rows = connection.execute("""SELECT id, seller_id AS author_id, title, category, price AS amount,
            COALESCE(description, '') AS description, 'listing' AS item_type, status, created_at
            FROM listings WHERE status IN ('pending','moderation') ORDER BY id ASC LIMIT 50""").fetchall()
        order_rows = connection.execute("""SELECT id, customer_id AS author_id, title, category, budget AS amount,
            COALESCE(description, '') AS description, 'order' AS item_type, status, created_at
            FROM orders WHERE status='moderation' ORDER BY id ASC LIMIT 50""").fetchall()
    return jsonify([dict(row) for row in [*listings_rows, *order_rows]])


@app.get("/api/admin/queues/<queue_name>")
def admin_operation_queue(queue_name: str):
    """Compact operational queues for the private MiniApp admin workspace."""
    if int(get_user().get("id", 0)) not in STAFF_ADMIN_IDS:
        return jsonify({"error": "forbidden"}), 403
    if queue_name not in {"payments", "payouts", "disputes"}:
        return jsonify({"error": "not_found"}), 404
    with db() as connection:
        if queue_name == "payments":
            rows = connection.execute("""SELECT d.id, COALESCE(o.title, l.title, 'Сделка LTeam') AS title,
                d.amount, d.status, d.buyer_id, d.seller_id, COALESCE(d.receipt, '') AS note, d.created_at
                FROM deals d LEFT JOIN orders o ON o.id=d.order_id LEFT JOIN listings l ON l.id=d.listing_id
                WHERE d.status IN ('waiting_admin_payment_approval', 'waiting_admin_confirm')
                ORDER BY d.id ASC LIMIT 50""").fetchall()
        elif queue_name == "payouts":
            rows = connection.execute("""SELECT id, 'Вывод средств' AS title, amount, status, user_id,
                COALESCE(requisites, '') AS note, created_at FROM withdrawal_requests
                WHERE status='pending' ORDER BY id ASC LIMIT 50""").fetchall()
        else:
            rows = connection.execute("""SELECT dd.id, COALESCE(o.title, l.title, 'Спор по сделке') AS title,
                d.amount, d.status, dd.opened_by AS user_id, dd.reason AS note, dd.created_at, dd.deal_id
                FROM deal_disputes dd JOIN deals d ON d.id=dd.deal_id
                LEFT JOIN orders o ON o.id=d.order_id LEFT JOIN listings l ON l.id=d.listing_id
                WHERE dd.status='open' ORDER BY dd.id ASC LIMIT 50""").fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/admin/moderation/<item_type>/<int:item_id>")
def admin_moderation_decision(item_type: str, item_id: int):
    admin_id = current_user_id()
    if not admin_id or admin_id not in STAFF_ADMIN_IDS:
        return jsonify({"error": "forbidden"}), 403
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).lower()
    if item_type not in {"listing", "order"} or action not in {"approve", "reject"}:
        return jsonify({"error": "validation", "message": "Неверное действие модерации."}), 400
    table, owner_column, success_status = ("listings", "seller_id", "active") if item_type == "listing" else ("orders", "customer_id", "active")
    with db() as connection:
        row = connection.execute(f"SELECT {owner_column} AS owner_id, title FROM {table} WHERE id=?", (item_id,)).fetchone()
        if not row:
            return jsonify({"error": "not_found"}), 404
        status = success_status if action == "approve" else "rejected"
        connection.execute(f"UPDATE {table} SET status=? WHERE id=?", (status, item_id))
        connection.commit()
    if BOT_TOKEN:
        try:
            outcome = "одобрена и опубликована" if action == "approve" else "отклонена модератором"
            body = urlencode({"chat_id": row["owner_id"], "text": f"Ваша публикация «{row['title']}» {outcome}."}).encode()
            urlopen(Request(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=body), timeout=4).read()
        except Exception:
            pass
    return jsonify({"ok": True, "status": status})


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
