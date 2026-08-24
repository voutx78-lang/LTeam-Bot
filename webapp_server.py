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
from app.events import create_event
from app.runtime_diagnostics import recent_update_errors
from app.market_policy import ALLOWED_CATEGORIES, MARKETPLACE_BETA, PAYMENTS_ENABLED, normalize_category, validate_category, validate_market_text
from app.star_payments import (
    StarPaymentError,
    create_invoice_link,
    create_pending_payment,
    expire_listing_promotions,
    list_user_payments,
    mark_invoice_failed,
    payment_status,
    public_products,
    refund_payment,
)
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


def notify_user(
    user_id: int,
    text: str,
    *,
    event_type: str = "system",
    title: str = "Событие в LT Market",
    route: str = "notifications",
    entity_id: int = 0,
) -> None:
    """Persist the event and mirror it to Telegram when possible."""
    if user_id:
        try:
            with db() as connection:
                create_event(connection, user_id, event_type, title, text, route, entity_id)
                connection.commit()
        except Exception:
            pass
    if not BOT_TOKEN or not user_id:
        return
    try:
        body = urlencode({"chat_id": int(user_id), "text": text}).encode()
        urlopen(Request(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=body), timeout=4).read()
    except Exception:
        return


@app.get("/api/market/config")
def market_config():
    """Public, non-sensitive product capabilities for the MiniApp."""
    return jsonify({
        "beta": MARKETPLACE_BETA,
        "payments_enabled": PAYMENTS_ENABLED,
        "stars_enabled": bool(BOT_TOKEN),
        "star_products": public_products(),
        "categories": list(ALLOWED_CATEGORIES),
        "payment_notice": (
            "Расчёты между участниками пока не проводятся через LTeam. "
            "Не переводите деньги по реквизитам, полученным в чате."
            if not PAYMENTS_ENABLED else ""
        ),
    })


@app.after_request
def add_cors_headers(response):
    """Разрешает опубликованной MiniApp обращаться к отдельному API-сервису."""
    response.headers["Access-Control-Allow-Origin"] = os.getenv("WEBAPP_ORIGIN", "*")
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Telegram-Init-Data"
    # Telegram WebView can keep the previous SPA bundle alive for a long time.
    # The HTML shell must always be revalidated; hashed Vite assets remain cacheable.
    if request.path in {"/", "/index.html"}:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
    return response


def get_user() -> dict:
    """Проверяет подпись Telegram initData, не доверяя данным, пришедшим от браузера."""
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    if not init_data or not BOT_TOKEN:
        return {}
    values = dict(parse_qsl(init_data, keep_blank_values=True))
    try:
        auth_date = int(values.get("auth_date", "0"))
    except (TypeError, ValueError):
        auth_date = 0
    max_age = max(300, int(os.getenv("TELEGRAM_INIT_DATA_MAX_AGE", "86400")))
    if not auth_date or abs(int(time.time()) - auth_date) > max_age:
        return {}
    received_hash = values.pop("hash", "")
    check_string = "\n".join(f"{key}={values[key]}" for key in sorted(values))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    expected_hash = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    if not received_hash or not hmac.compare_digest(expected_hash, received_hash):
        return {}
    try:
        user = json.loads(values.get("user", "{}"))
    except json.JSONDecodeError:
        return {}
    try:
        user_id = int(user.get("id", 0))
        if user_id and user_id not in STAFF_ADMIN_IDS:
            with db() as connection:
                if connection.execute("SELECT 1 FROM banned_users WHERE user_id=?", (user_id,)).fetchone():
                    return {}
    except Exception:
        pass
    return user


def db() -> sqlite3.Connection:
    connection = shared_db()
    connection.row_factory = sqlite3.Row
    return connection


def current_user_id() -> int | None:
    user = get_user()
    return int(user["id"]) if user.get("id") else None


def require_admin_id() -> int | None:
    """Return the signed-in administrator id without trusting client flags."""
    user_id = current_user_id()
    return user_id if user_id in STAFF_ADMIN_IDS else None


def log_admin_action(connection: sqlite3.Connection, actor_id: int, action: str, target_id: int | None = None, details: str = "") -> None:
    connection.execute(
        "INSERT INTO admin_action_logs(actor_id, target_id, action, details, created_at) VALUES (?, ?, ?, ?, ?)",
        (actor_id, target_id, action, details[:1000], datetime.now().isoformat()),
    )


@app.get("/api/health")
def health_check():
    """Small non-sensitive readiness check for Render and future monitoring."""
    try:
        with db() as connection:
            connection.execute("SELECT 1").fetchone()
        return jsonify({
            "ok": True,
            "product": "LT Market",
            "version": "2026.08-create-cinematic-v1",
            "payments_enabled": PAYMENTS_ENABLED,
            "stars_enabled": bool(BOT_TOKEN),
            "storage": "cloud_snapshot" if os.getenv("DATABASE_URL") else "local",
        })
    except Exception:
        return jsonify({"ok": False}), 503


@app.get("/api/admin/runtime-errors")
def admin_runtime_errors():
    """Return recent bot failures only to a signed administrator."""
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    return jsonify({"errors": recent_update_errors()})


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
    user_id = int(user.get("id", 0))
    name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")])).strip()
    now = datetime.now().isoformat()
    with db() as connection:
        connection.execute("""INSERT INTO users(user_id, username, created_at, display_name, first_name, last_name)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,
            display_name=CASE WHEN COALESCE(users.display_name, '')='' THEN excluded.display_name ELSE users.display_name END,
            first_name=excluded.first_name, last_name=excluded.last_name""",
            (user_id, user.get("username", ""), now, name, user.get("first_name", ""), user.get("last_name", "")))
        preference = connection.execute("SELECT market_role, theme, notification_settings FROM user_preferences WHERE user_id=?", (user_id,)).fetchone()
        profile = connection.execute("SELECT COALESCE(display_name, '') AS display_name, COALESCE(bio, '') AS bio, COALESCE(skills_json, '[]') AS skills_json FROM users WHERE user_id=?", (user_id,)).fetchone()
        unread = connection.execute("SELECT COUNT(*) FROM notification_events WHERE user_id=? AND is_read=0", (user_id,)).fetchone()[0]
        connection.commit()
    try:
        profile_skills = json.loads(profile["skills_json"] or "[]") if profile else []
    except (TypeError, json.JSONDecodeError):
        profile_skills = []
    return jsonify({
        "authenticated": True,
        "id": user_id,
        "name": (profile["display_name"] if profile and profile["display_name"] else name),
        "username": user.get("username", ""),
        "photo_url": user.get("photo_url", "") or avatar_endpoint(user_id),
        "is_admin": user_id in STAFF_ADMIN_IDS,
        "role": preference["market_role"] if preference else "both",
        "theme": preference["theme"] if preference else "system",
        "unread_notifications": int(unread or 0),
        "bio": profile["bio"] if profile else "",
        "skills": profile_skills,
    })


@app.post("/api/payments/stars/invoice")
def create_star_invoice():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if not BOT_TOKEN:
        return jsonify({"error": "unavailable", "message": "Оплата Stars временно недоступна."}), 503
    payload = request.get_json(silent=True) or {}
    try:
        listing_id = int(payload.get("listing_id", 0))
    except (TypeError, ValueError):
        listing_id = 0
    try:
        with db() as connection:
            payment = create_pending_payment(
                connection,
                user_id=user_id,
                product_code=str(payload.get("product_code", "")),
                listing_id=listing_id,
            )
            connection.commit()
        invoice_link = create_invoice_link(BOT_TOKEN, payment)
    except StarPaymentError as error:
        if "payment" in locals():
            with db() as connection:
                mark_invoice_failed(connection, payment["id"])
                connection.commit()
        return jsonify({"error": "payment", "message": str(error)}), 400
    return jsonify({
        "payment_id": payment["id"],
        "invoice_link": invoice_link,
        "stars": payment["stars"],
        "title": payment["title"],
        "status": "pending",
    }), 201


@app.get("/api/payments/stars")
def star_payment_history():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = list_user_payments(connection, user_id)
    return jsonify(rows)


@app.get("/api/payments/stars/<int:payment_id>")
def star_payment_state(payment_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        item = payment_status(connection, payment_id, user_id)
    if not item:
        return jsonify({"error": "not_found"}), 404
    item.pop("telegram_payment_charge_id", None)
    return jsonify(item)


@app.post("/api/admin/payments/stars/<int:payment_id>/refund")
def admin_refund_star_payment(payment_id: int):
    admin_id = require_admin_id()
    if not admin_id or admin_id not in OWNER_IDS:
        return jsonify({"error": "forbidden", "message": "Возвраты доступны только владельцу."}), 403
    try:
        with db() as connection:
            payment = refund_payment(connection, BOT_TOKEN, payment_id)
            log_admin_action(connection, admin_id, "refund_stars", int(payment["user_id"]), f"payment_id={payment_id}; stars={payment['stars']}")
            connection.commit()
    except StarPaymentError as error:
        return jsonify({"error": "payment", "message": str(error)}), 400
    notify_user(int(payment["user_id"]), f"Telegram Stars по операции #{payment_id} возвращены.", event_type="payment", title="Возврат Stars", route="promotions", entity_id=payment_id)
    return jsonify({"ok": True, "status": "refunded"})


@app.route("/api/preferences", methods=["GET", "PUT"])
def preferences_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        if request.method == "PUT":
            payload = request.get_json(silent=True) or {}
            role = str(payload.get("role", "both"))
            theme = str(payload.get("theme", "system"))
            if role not in {"customer", "executor", "both"} or theme not in {"system", "light", "dark"}:
                return jsonify({"error": "validation", "message": "Некорректные настройки."}), 400
            settings = payload.get("notifications", {})
            if not isinstance(settings, dict):
                settings = {}
            settings = {
                "messages": settings.get("messages", True) is not False,
                "orders": settings.get("orders", True) is not False,
                "recommendations": settings.get("recommendations", True) is not False,
            }
            display = payload.get("display", {})
            if not isinstance(display, dict):
                display = {}
            display = {
                "animations": display.get("animations", True) is not False,
                "haptics": display.get("haptics", True) is not False,
                "compact_cards": bool(display.get("compact_cards", False)),
                "language": "ru",
                "accent": str(display.get("accent", "violet")) if str(display.get("accent", "violet")) in {"violet", "ocean", "mint", "sunset"} else "violet",
            }
            connection.execute("""INSERT INTO user_preferences(user_id, market_role, theme, notification_settings, display_settings, updated_at)
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET market_role=excluded.market_role,
                theme=excluded.theme, notification_settings=excluded.notification_settings,
                display_settings=excluded.display_settings, updated_at=excluded.updated_at""",
                (user_id, role, theme, json.dumps(settings, ensure_ascii=False), json.dumps(display, ensure_ascii=False), datetime.now().isoformat()))
            connection.execute("UPDATE users SET market_role=? WHERE user_id=?", (role, user_id))
            connection.commit()
        row = connection.execute("SELECT market_role, theme, notification_settings, display_settings FROM user_preferences WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        return jsonify({"role": "both", "theme": "system", "notifications": {"messages": True, "orders": True, "recommendations": True}, "display": {"animations": True, "haptics": True, "compact_cards": False, "language": "ru", "accent": "violet"}})
    try:
        settings = json.loads(row["notification_settings"] or "{}")
    except json.JSONDecodeError:
        settings = {}
    if not isinstance(settings, dict):
        settings = {}
    settings = {
        "messages": settings.get("messages", True) is not False,
        "orders": settings.get("orders", True) is not False,
        "recommendations": settings.get("recommendations", True) is not False,
    }
    try:
        display = json.loads(row["display_settings"] or "{}")
    except json.JSONDecodeError:
        display = {}
    display = {"animations": display.get("animations", True) is not False, "haptics": display.get("haptics", True) is not False, "compact_cards": bool(display.get("compact_cards", False)), "language": "ru", "accent": str(display.get("accent", "violet")) if str(display.get("accent", "violet")) in {"violet", "ocean", "mint", "sunset"} else "violet"}
    return jsonify({"role": row["market_role"], "theme": row["theme"], "notifications": settings, "display": display})


@app.get("/api/notifications")
def notifications_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("""SELECT id, event_type, title, body, route, entity_id, is_read, created_at
            FROM notification_events WHERE user_id=? ORDER BY id DESC LIMIT 100""", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/notifications/read")
def read_notifications_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids", [])
    with db() as connection:
        if isinstance(ids, list) and ids:
            clean_ids = [int(value) for value in ids[:100] if str(value).isdigit()]
            if clean_ids:
                placeholders = ",".join("?" for _ in clean_ids)
                connection.execute(f"UPDATE notification_events SET is_read=1 WHERE user_id=? AND id IN ({placeholders})", (user_id, *clean_ids))
        else:
            connection.execute("UPDATE notification_events SET is_read=1 WHERE user_id=?", (user_id,))
        connection.commit()
    return jsonify({"ok": True})


@app.route("/api/favorites", methods=["GET", "POST", "DELETE"])
def favorites_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    with db() as connection:
        if request.method in {"POST", "DELETE"}:
            try:
                listing_id = int(payload.get("listing_id", 0))
            except (TypeError, ValueError):
                listing_id = 0
            if not listing_id:
                return jsonify({"error": "validation"}), 400
            if request.method == "POST":
                connection.execute("INSERT OR IGNORE INTO favorites(user_id, listing_id) VALUES (?, ?)", (user_id, listing_id))
            else:
                connection.execute("DELETE FROM favorites WHERE user_id=? AND listing_id=?", (user_id, listing_id))
            connection.commit()
        rows = connection.execute("SELECT listing_id FROM favorites WHERE user_id=? ORDER BY rowid DESC", (user_id,)).fetchall()
    return jsonify({"ids": [int(row[0]) for row in rows]})


@app.route("/api/drafts/<draft_type>", methods=["GET", "PUT", "DELETE"])
def drafts_api(draft_type: str):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if draft_type not in {"listing", "order"}:
        return jsonify({"error": "validation"}), 400
    with db() as connection:
        if request.method == "PUT":
            payload = request.get_json(silent=True) or {}
            serialized = json.dumps(payload, ensure_ascii=False)
            if len(serialized) > 250_000:
                return jsonify({"error": "validation", "message": "Черновик слишком большой."}), 400
            connection.execute("""INSERT INTO marketplace_drafts(user_id, draft_type, payload, updated_at)
                VALUES (?, ?, ?, ?) ON CONFLICT(user_id, draft_type) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at""",
                (user_id, draft_type, serialized, datetime.now().isoformat()))
            connection.commit()
            return jsonify({"ok": True})
        if request.method == "DELETE":
            connection.execute("DELETE FROM marketplace_drafts WHERE user_id=? AND draft_type=?", (user_id, draft_type))
            connection.commit()
            return jsonify({"ok": True})
        row = connection.execute("SELECT payload, updated_at FROM marketplace_drafts WHERE user_id=? AND draft_type=?", (user_id, draft_type)).fetchone()
    if not row:
        return jsonify({"payload": None})
    try:
        payload = json.loads(row["payload"] or "{}")
    except json.JSONDecodeError:
        payload = {}
    return jsonify({"payload": payload, "updated_at": row["updated_at"]})


@app.get("/api/listings")
def listings():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        expire_listing_promotions(connection)
        connection.commit()
        rows = connection.execute("""SELECT l.id, l.title, l.category, l.price, COALESCE(l.description, '') AS description,
            l.seller_id, COALESCE(l.delivery_time, '') AS delivery_time, COALESCE(u.username, '') AS seller_username,
            COALESCE(u.display_name, '') AS seller_name, COALESCE(l.image_data, '') AS image_data,
            COALESCE(l.portfolio_data, '[]') AS portfolio_data, COALESCE(l.revisions, 1) AS revisions,
            COALESCE(l.requirements, '') AS requirements, COALESCE(l.result_description, '') AS result_description,
            COALESCE(u.verified, 0) AS seller_verified, l.created_at,
            COALESCE(r.avg_rating, 0) AS avg_rating, COALESCE(r.reviews_count, 0) AS reviews_count,
            COALESCE(s.completed_orders, 0) AS completed_orders
            FROM listings l LEFT JOIN users u ON u.user_id=l.seller_id
            LEFT JOIN (SELECT seller_id, AVG(rating) AS avg_rating, COUNT(*) AS reviews_count FROM reviews GROUP BY seller_id) r ON r.seller_id=l.seller_id
            LEFT JOIN (SELECT seller_id, COUNT(*) AS completed_orders FROM deals WHERE status='completed' GROUP BY seller_id) s ON s.seller_id=l.seller_id
            WHERE l.status='active' ORDER BY COALESCE(l.is_top,0) DESC, l.id DESC LIMIT 50""").fetchall()
        favorite_ids = {int(row[0]) for row in connection.execute("SELECT listing_id FROM favorites WHERE user_id=?", (user_id,)).fetchall()}
        package_rows = connection.execute("SELECT listing_id, package_key, title, description, price, delivery_time, revisions FROM listing_packages ORDER BY listing_id, sort_order, id").fetchall()
    packages_by_listing: dict[int, list[dict]] = {}
    for package in package_rows:
        packages_by_listing.setdefault(int(package["listing_id"]), []).append(dict(package))
    result = []
    for row in rows:
        item = dict(row)
        item["avatar_url"] = avatar_endpoint(item["seller_id"])
        if item["image_data"].startswith("tg:"):
            item["image_data"] = listing_cover_endpoint(item["id"])
        try:
            item["portfolio_data"] = json.loads(item["portfolio_data"] or "[]")
        except (TypeError, json.JSONDecodeError):
            item["portfolio_data"] = []
        item["packages"] = packages_by_listing.get(int(item["id"]), [])
        item["is_favorite"] = int(item["id"]) in favorite_ids
        result.append(item)
    return jsonify(result)


@app.get("/api/listings/<int:listing_id>")
def listing_detail(listing_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        row = connection.execute("""SELECT l.id, l.title, l.category, l.price, COALESCE(l.description, '') AS description,
            l.seller_id, COALESCE(l.delivery_time, '') AS delivery_time, COALESCE(l.image_data, '') AS image_data,
            COALESCE(l.portfolio_data, '[]') AS portfolio_data, COALESCE(l.revisions, 1) AS revisions,
            COALESCE(l.requirements, '') AS requirements, COALESCE(l.result_description, '') AS result_description,
            COALESCE(u.username, '') AS seller_username, COALESCE(u.display_name, '') AS seller_name,
            COALESCE(u.verified, 0) AS seller_verified, COALESCE(u.bio, '') AS seller_bio,
            COALESCE(r.avg_rating, 0) AS avg_rating, COALESCE(r.reviews_count, 0) AS reviews_count,
            COALESCE(s.completed_orders, 0) AS completed_orders, l.created_at
            FROM listings l LEFT JOIN users u ON u.user_id=l.seller_id
            LEFT JOIN (SELECT seller_id, AVG(rating) AS avg_rating, COUNT(*) AS reviews_count FROM reviews GROUP BY seller_id) r ON r.seller_id=l.seller_id
            LEFT JOIN (SELECT seller_id, COUNT(*) AS completed_orders FROM deals WHERE status='completed' GROUP BY seller_id) s ON s.seller_id=l.seller_id
            WHERE l.id=? AND l.status='active'""", (listing_id,)).fetchone()
        if not row:
            return jsonify({"error": "not_found"}), 404
        packages = connection.execute("SELECT package_key, title, description, price, delivery_time, revisions FROM listing_packages WHERE listing_id=? ORDER BY sort_order, id", (listing_id,)).fetchall()
        reviews = connection.execute("""SELECT r.id, r.rating, r.text, r.created_at,
            COALESCE(u.display_name, u.username, 'Заказчик LT Market') AS reviewer_name
            FROM reviews r LEFT JOIN users u ON u.user_id=r.reviewer_id
            WHERE r.seller_id=? ORDER BY r.id DESC LIMIT 8""", (row["seller_id"],)).fetchall()
        connection.execute("""INSERT INTO recently_viewed(user_id, listing_id, viewed_at) VALUES (?, ?, ?)
            ON CONFLICT(user_id, listing_id) DO UPDATE SET viewed_at=excluded.viewed_at""", (user_id, listing_id, datetime.now().isoformat()))
        connection.commit()
    item = dict(row)
    item["avatar_url"] = avatar_endpoint(item["seller_id"])
    if item["image_data"].startswith("tg:"):
        item["image_data"] = listing_cover_endpoint(item["id"])
    try:
        item["portfolio_data"] = json.loads(item["portfolio_data"] or "[]")
    except (TypeError, json.JSONDecodeError):
        item["portfolio_data"] = []
    item["packages"] = [dict(package) for package in packages]
    item["reviews"] = [dict(review) for review in reviews]
    return jsonify(item)


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
        return "", 204
    cached = AVATAR_CACHE.get(user_id)
    if cached and cached[0] > time.time():
        return send_file(BytesIO(cached[1]), mimetype=cached[2], max_age=3600)
    try:
        photos = json.loads(urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/getUserProfilePhotos?user_id={user_id}&limit=1", timeout=6).read())
        entries = photos.get("result", {}).get("photos", [])
        if not entries:
            return "", 204
        file_id = entries[0][-1]["file_id"]
        file_info = json.loads(urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}", timeout=6).read())
        path = file_info.get("result", {}).get("file_path")
        if not path:
            return "", 204
        data = urlopen(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}", timeout=8).read()
    except Exception:
        return "", 204
    AVATAR_CACHE[user_id] = (time.time() + 3600, data, "image/jpeg")
    return send_file(BytesIO(data), mimetype="image/jpeg", max_age=3600)


@app.post("/api/listings")
def create_listing():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    title = str(payload.get("title", "")).strip()[:120]
    category = normalize_category(str(payload.get("category", "")).strip()[:80])
    description = str(payload.get("description", "")).strip()[:2000]
    delivery_time = str(payload.get("delivery_time", "По договорённости")).strip()[:80]
    requirements = str(payload.get("requirements", "")).strip()[:1200]
    result_description = str(payload.get("result_description", "")).strip()[:1200]
    try:
        revisions = max(0, min(20, int(payload.get("revisions", 1))))
    except (TypeError, ValueError):
        revisions = 1
    image_data = str(payload.get("image_data", "")).strip()
    portfolio = payload.get("portfolio_data", [])
    packages = payload.get("packages", [])
    category_error = validate_category(category)
    content_error = validate_market_text(f"{title}\n{description}")
    if category_error or content_error:
        return jsonify({"error": "validation", "message": category_error or content_error}), 400
    if not isinstance(portfolio, list):
        portfolio = []
    if not isinstance(packages, list):
        packages = []
    portfolio = [str(image).strip() for image in portfolio[:4] if str(image).strip().startswith("data:image/")]
    if any(len(image) > 500_000 for image in portfolio):
        return jsonify({"error": "validation", "message": "Каждый пример портфолио должен быть до 350 КБ."}), 400
    if not image_data:
        return jsonify({"error": "validation", "message": "Добавьте обложку услуги — она обязательна для публикации."}), 400
    if not image_data.startswith("data:image/") or len(image_data) > 900_000:
        return jsonify({"error": "validation", "message": "Изображение должно быть картинкой до 650 КБ."}), 400
    try:
        price = max(1, int(payload.get("price", 0)))
    except (TypeError, ValueError):
        price = 0
    if not title or not description or not price:
        return jsonify({"error": "validation", "message": "Заполните название, описание и цену."}), 400
    with db() as connection:
        cursor = connection.execute(
            """INSERT INTO listings (seller_id, title, category, item_type, condition, price, description,
               delivery_time, image_data, portfolio_data, revisions, requirements, result_description, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, title, category, "Услуга", "new", price, description, delivery_time, image_data,
             json.dumps(portfolio, ensure_ascii=False), revisions, requirements, result_description, "pending", datetime.now().isoformat()),
        )
        listing_id = int(cursor.lastrowid)
        clean_packages = []
        for index, package in enumerate(packages[:3]):
            if not isinstance(package, dict):
                continue
            try:
                package_price = max(1, int(package.get("price", 0)))
                package_revisions = max(0, min(20, int(package.get("revisions", revisions))))
            except (TypeError, ValueError):
                continue
            package_title = str(package.get("title", "")).strip()[:60]
            if not package_title or not package_price:
                continue
            clean_packages.append((
                listing_id,
                str(package.get("package_key", ["basic", "standard", "premium"][index]))[:20],
                package_title,
                str(package.get("description", "")).strip()[:600],
                package_price,
                str(package.get("delivery_time", delivery_time)).strip()[:80],
                package_revisions,
                index,
            ))
        if not clean_packages:
            clean_packages.append((listing_id, "basic", "Базовый", result_description or description[:240], price, delivery_time, revisions, 0))
        connection.executemany("""INSERT INTO listing_packages
            (listing_id, package_key, title, description, price, delivery_time, revisions, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", clean_packages)
        connection.execute("DELETE FROM marketplace_drafts WHERE user_id=? AND draft_type='listing'", (user_id,))
        connection.commit()
    notify_admins(f"Новая услуга на модерации\n\n{title}\nКатегория: {category}\nЦена: {price} ₽\nАвтор: {user_id}")
    notify_user(user_id, f"Ваша услуга «{title}» отправлена на модерацию LTeam. После проверки она появится в каталоге.")
    return jsonify({"ok": True, "listing_id": listing_id, "status": "pending"}), 201


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
    category = normalize_category(str(payload.get("category", "")).strip()[:80])
    description = str(payload.get("description", "")).strip()[:2000]
    deadline = str(payload.get("deadline", "По договорённости")).strip()[:80]
    reference_image_data = str(payload.get("reference_image_data", "")).strip()
    category_error = validate_category(category)
    content_error = validate_market_text(f"{title}\n{description}")
    if category_error or content_error:
        return jsonify({"error": "validation", "message": category_error or content_error}), 400
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
        connection.execute("DELETE FROM marketplace_drafts WHERE user_id=? AND draft_type='order'", (user_id,))
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


@app.get("/api/listing-requests/mine")
def my_listing_requests():
    """Seller inbox for service discussion requests created in either UI."""
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("""SELECT r.id, r.listing_id, r.buyer_id, r.message, r.status, r.deal_id, r.created_at,
            l.title, l.category, l.price, COALESCE(u.username, '') AS buyer_username, COALESCE(u.display_name, '') AS buyer_name
            FROM listing_discussion_requests r
            JOIN listings l ON l.id=r.listing_id
            LEFT JOIN users u ON u.user_id=r.buyer_id
            WHERE r.seller_id=? ORDER BY CASE WHEN r.status='new' THEN 0 ELSE 1 END, r.id DESC LIMIT 100""", (user_id,)).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["buyer_avatar_url"] = avatar_endpoint(item["buyer_id"])
        result.append(item)
    return jsonify(result)


@app.post("/api/listings/<int:listing_id>/requests")
def create_listing_request(listing_id: int):
    """Start a protected service discussion without moving the user into a bot-only form."""
    buyer_id = current_user_id()
    if not buyer_id:
        return jsonify({"error": "unauthorized"}), 401
    message = str((request.get_json(silent=True) or {}).get("message", "")).strip()[:1200]
    if len(message) < 10:
        return jsonify({"error": "validation", "message": "Опишите задачу подробнее — минимум 10 символов."}), 400
    with db() as connection:
        listing = connection.execute("SELECT seller_id, title FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone()
        if not listing:
            return jsonify({"error": "not_found", "message": "Эта услуга больше недоступна."}), 404
        seller_id = int(listing["seller_id"])
        if seller_id == buyer_id:
            return jsonify({"error": "validation", "message": "Нельзя отправить запрос по своей услуге."}), 400
        duplicate = connection.execute("SELECT id FROM listing_discussion_requests WHERE listing_id=? AND buyer_id=? AND status='new'", (listing_id, buyer_id)).fetchone()
        if duplicate:
            return jsonify({"error": "validation", "message": "Ваш запрос уже ждёт ответа исполнителя."}), 409
        now = datetime.now().isoformat()
        cursor = connection.execute("""INSERT INTO listing_discussion_requests
            (listing_id, buyer_id, seller_id, message, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'new', ?, ?)""", (listing_id, buyer_id, seller_id, message, now, now))
        connection.commit()
    notify_user(seller_id, f"Новый запрос по услуге «{listing['title']}». Откройте MiniApp → Профиль → Мои объявления, чтобы принять его.")
    return jsonify({"ok": True, "request_id": cursor.lastrowid, "status": "new"}), 201


@app.post("/api/listing-requests/<int:request_id>/decision")
def decide_listing_request(request_id: int):
    """Seller accepts a request into a deal or rejects it; both branches notify the buyer."""
    seller_id = current_user_id()
    if not seller_id:
        return jsonify({"error": "unauthorized"}), 401
    action = str((request.get_json(silent=True) or {}).get("action", "")).strip().lower()
    if action not in {"accept", "reject"}:
        return jsonify({"error": "validation", "message": "Неизвестное действие."}), 400
    with db() as connection:
        row = connection.execute("""SELECT r.listing_id, r.buyer_id, r.seller_id, r.message, r.status,
            l.title, l.price FROM listing_discussion_requests r JOIN listings l ON l.id=r.listing_id WHERE r.id=?""", (request_id,)).fetchone()
        if not row or int(row["seller_id"]) != seller_id:
            return jsonify({"error": "forbidden"}), 403
        if row["status"] != "new":
            return jsonify({"error": "validation", "message": "Этот запрос уже обработан."}), 409
        now = datetime.now().isoformat()
        buyer_id = int(row["buyer_id"])
        if action == "reject":
            connection.execute("UPDATE listing_discussion_requests SET status='rejected', updated_at=? WHERE id=?", (now, request_id))
            connection.commit()
            deal_id = 0
        else:
            amount = max(1, int(row["price"] or 0))
            percent = max(0, min(100, int(os.getenv("COMMISSION_PERCENT", "10"))))
            commission = int(amount * percent / 100)
            payout = amount - commission
            deal = connection.execute("""INSERT INTO deals
                (listing_id, order_id, source_type, buyer_id, seller_id, amount, commission, payout, payment_method, status, created_at)
                VALUES (?, 0, 'listing', ?, ?, ?, ?, ?, 'admin_card_only', 'discussion', ?)""",
                (row["listing_id"], buyer_id, seller_id, amount, commission, payout, now))
            deal_id = int(deal.lastrowid)
            connection.execute("UPDATE listing_discussion_requests SET status='accepted', deal_id=?, updated_at=? WHERE id=?", (deal_id, now, request_id))
            connection.execute("INSERT INTO deal_messages(deal_id, sender_id, receiver_id, text, created_at) VALUES (?, ?, ?, ?, ?)",
                (deal_id, buyer_id, seller_id, row["message"], now))
            connection.commit()
    if action == "reject":
        notify_user(buyer_id, f"Исполнитель отклонил запрос по услуге «{row['title']}». Можно выбрать другую услугу в каталоге.")
        return jsonify({"ok": True, "status": "rejected"})
    notify_user(buyer_id, f"Исполнитель принял запрос по услуге «{row['title']}». Сделка #{deal_id} открыта — продолжите обсуждение в MiniApp.")
    return jsonify({"ok": True, "status": "accepted", "deal_id": deal_id}), 201


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
        rows = connection.execute("""SELECT d.id, d.listing_id, d.order_id, d.source_type, d.buyer_id, d.seller_id,
            d.amount, d.status, d.created_at, COALESCE(d.updated_at, d.created_at) AS updated_at,
            COALESCE(d.revision_limit, 1) AS revision_limit, COALESCE(d.terms_json, '{}') AS terms_json,
            COALESCE(l.title, o.title, 'Заказ LT Market') AS title,
            COALESCE(b.display_name, b.username, 'Заказчик') AS buyer_name,
            COALESCE(s.display_name, s.username, 'Исполнитель') AS seller_name
            FROM deals d LEFT JOIN listings l ON l.id=d.listing_id LEFT JOIN orders o ON o.id=d.order_id
            LEFT JOIN users b ON b.user_id=d.buyer_id LEFT JOIN users s ON s.user_id=d.seller_id
            WHERE d.buyer_id=? OR d.seller_id=? ORDER BY d.id DESC LIMIT 50""", (user_id, user_id)).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["role"] = "customer" if int(item["buyer_id"]) == user_id else "executor"
        item["buyer_avatar_url"] = avatar_endpoint(item["buyer_id"])
        item["seller_avatar_url"] = avatar_endpoint(item["seller_id"])
        try:
            item["terms"] = json.loads(item.pop("terms_json") or "{}")
        except json.JSONDecodeError:
            item["terms"] = {}
        result.append(item)
    return jsonify(result)


@app.post("/api/deals/<int:deal_id>/action")
def deal_action(deal_id: int):
    """Run a transparent work lifecycle; payment processing is opt-in only."""
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

        next_status = status
        event_type = "deal"
        if action == "set_final_price":
            try:
                amount = int(payload.get("amount", 0))
            except (TypeError, ValueError):
                amount = 0
            if user_id != seller_id or status not in {"discussion", "waiting_final_price", "payment_rejected"} or amount < 100 or amount > 150000:
                return jsonify({"error": "validation", "message": "Итоговую цену может выставить исполнитель после обсуждения."}), 400
            percent = max(0, min(100, int(os.getenv("COMMISSION_PERCENT", "10"))))
            commission = int(amount * percent / 100)
            payout = amount - commission
            terms = payload.get("terms", {}) if isinstance(payload.get("terms", {}), dict) else {}
            terms["amount"] = amount
            connection.execute("""UPDATE deals SET amount=?, commission=?, payout=?, status='waiting_buyer_price_confirm',
                final_price_set_by=?, terms_json=?, updated_at=? WHERE id=?""",
                (amount, commission, payout, user_id, json.dumps(terms, ensure_ascii=False), now, deal_id))
            target_user, notice = buyer_id, f"Исполнитель выставил итоговую цену по сделке #{deal_id}. Подтвердите её в MiniApp."
            next_status = "waiting_buyer_price_confirm"
        elif action == "confirm_price":
            if user_id != buyer_id or status != "waiting_buyer_price_confirm":
                return jsonify({"error": "validation", "message": "Подтверждение цены сейчас недоступно."}), 400
            next_status = "waiting_admin_payment_approval" if PAYMENTS_ENABLED else "terms_confirmed"
            connection.execute("""UPDATE deals SET status=?,
                final_price_confirmed_by=?, payment_requested_by=?, updated_at=? WHERE id=?""",
                (next_status, user_id, user_id if PAYMENTS_ENABLED else 0, now, deal_id))
            if PAYMENTS_ENABLED:
                target_user, notice = seller_id, f"Покупатель подтвердил условия сделки #{deal_id}. Ожидается подтверждение оплаты."
                notify_admins(f"Нужна проверка оплаты по сделке #{deal_id}. Сумма: {int(deal['amount'] or 0)} ₽")
            else:
                target_user, notice = seller_id, f"Покупатель подтвердил условия сделки #{deal_id}. Можно начинать работу."
        elif action == "start_work":
            if user_id != seller_id or status != "terms_confirmed":
                return jsonify({"error": "validation", "message": "Начать работу можно после подтверждения условий заказчиком."}), 400
            next_status = "in_work"
            connection.execute("UPDATE deals SET status=?, updated_at=? WHERE id=?", (next_status, now, deal_id))
            target_user, notice = buyer_id, f"Исполнитель начал работу по заказу #{deal_id}."
        elif action == "confirm_done":
            if user_id != buyer_id or status != "waiting_buyer_confirm":
                return jsonify({"error": "validation", "message": "Подтверждение выполнения сейчас недоступно."}), 400
            next_status = "completed"
            connection.execute("UPDATE deals SET status='completed', updated_at=? WHERE id=?", (now, deal_id))
            if PAYMENTS_ENABLED:
                credit_deal_payout(connection, seller_id, deal_id, int(deal["payout"] or 0))
            target_user, notice = seller_id, f"Заказ #{deal_id} принят заказчиком. Теперь участники могут оставить отзывы."
        elif action == "request_revision":
            reason = str(payload.get("reason", "")).strip()[:1200]
            if user_id != buyer_id or status != "waiting_buyer_confirm" or len(reason) < 5:
                return jsonify({"error": "validation", "message": "Опишите, что нужно исправить."}), 400
            next_status = "in_revision"
            connection.execute("INSERT INTO revision_requests(deal_id, requester_id, text, status, created_at) VALUES (?, ?, ?, 'open', ?)", (deal_id, user_id, reason, now))
            connection.execute("UPDATE deals SET status=?, updated_at=? WHERE id=?", (next_status, now, deal_id))
            target_user, notice = seller_id, f"По заказу #{deal_id} запрошена правка: {reason[:180]}"
            event_type = "revision"
        elif action == "cancel":
            if status not in {"discussion", "waiting_buyer_price_confirm", "terms_confirmed"}:
                return jsonify({"error": "validation", "message": "На текущем этапе отмена доступна через поддержку."}), 400
            next_status = "cancelled"
            connection.execute("UPDATE deals SET status='cancelled', updated_at=? WHERE id=?", (now, deal_id))
            target_user = seller_id if user_id == buyer_id else buyer_id
            notice = f"Участник отменил заказ #{deal_id} до начала работы."
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
            next_status = "dispute_open"
            event_type = "dispute"
        else:
            return jsonify({"error": "validation", "message": "Неизвестное действие по сделке."}), 400
        connection.commit()
    notify_user(target_user, notice, event_type=event_type, title=f"Заказ #{deal_id}", route=f"deal:{deal_id}", entity_id=deal_id)
    return jsonify({"ok": True, "status": next_status})


@app.route("/api/deals/<int:deal_id>/deliveries", methods=["GET", "POST"])
def deal_deliveries_api(deal_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        deal = connection.execute("SELECT buyer_id, seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not deal or user_id not in {int(deal["buyer_id"]), int(deal["seller_id"])}:
            return jsonify({"error": "forbidden"}), 403
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            comment = str(payload.get("comment", "")).strip()[:1600]
            file_data = str(payload.get("file_data", "")).strip()
            if user_id != int(deal["seller_id"]) or deal["status"] not in {"in_work", "in_revision"}:
                return jsonify({"error": "validation", "message": "Сейчас нельзя отправить результат."}), 400
            if len(comment) < 5:
                return jsonify({"error": "validation", "message": "Опишите переданный результат."}), 400
            if file_data and len(file_data) > 900_000:
                return jsonify({"error": "validation", "message": "Файл слишком большой для текущей бета-версии."}), 400
            version = int(connection.execute("SELECT COALESCE(MAX(version), 0) + 1 FROM deal_deliveries WHERE deal_id=?", (deal_id,)).fetchone()[0])
            now = datetime.now().isoformat()
            cursor = connection.execute("INSERT INTO deal_deliveries(deal_id, sender_id, version, comment, file_data, created_at) VALUES (?, ?, ?, ?, ?, ?)", (deal_id, user_id, version, comment, file_data, now))
            connection.execute("UPDATE revision_requests SET status='resolved', resolved_at=? WHERE deal_id=? AND status='open'", (now, deal_id))
            connection.execute("UPDATE deals SET status='waiting_buyer_confirm', updated_at=? WHERE id=?", (now, deal_id))
            connection.commit()
            notify_user(int(deal["buyer_id"]), f"Исполнитель отправил версию {version} по заказу #{deal_id}. Проверьте результат.", event_type="delivery", title="Получен результат", route=f"deal:{deal_id}", entity_id=deal_id)
            return jsonify({"ok": True, "delivery_id": cursor.lastrowid, "version": version, "status": "waiting_buyer_confirm"}), 201
        rows = connection.execute("SELECT id, sender_id, version, comment, file_data, created_at FROM deal_deliveries WHERE deal_id=? ORDER BY version DESC", (deal_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/deals/<int:deal_id>/revisions")
def deal_revisions_api(deal_id: int):
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        if not can_access_deal(connection, deal_id, user_id):
            return jsonify({"error": "forbidden"}), 403
        rows = connection.execute("SELECT id, requester_id, text, status, created_at, resolved_at FROM revision_requests WHERE deal_id=? ORDER BY id DESC", (deal_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/balance")
def balance():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if not PAYMENTS_ENABLED:
        return jsonify({"enabled": False, "available": 0, "frozen": 0, "total_earned": 0, "total_withdrawn": 0})
    with db() as connection:
        row = connection.execute("SELECT COALESCE(available, 0) AS available, COALESCE(frozen, 0) AS frozen, COALESCE(total_earned, 0) AS total_earned, COALESCE(total_withdrawn, 0) AS total_withdrawn FROM user_balances WHERE user_id=?", (user_id,)).fetchone()
    return jsonify(dict(row) if row else {"available": 0, "frozen": 0, "total_earned": 0, "total_withdrawn": 0})


@app.get("/api/balance/history")
def balance_history_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if not PAYMENTS_ENABLED:
        return jsonify([])
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
        user = connection.execute("""SELECT COALESCE(username, '') AS username, COALESCE(display_name, '') AS display_name,
            COALESCE(verified, 0) AS verified, COALESCE(bio, '') AS bio, COALESCE(skills_json, '[]') AS skills_json,
            COALESCE(market_role, 'both') AS market_role, created_at FROM users WHERE user_id=?""", (user_id,)).fetchone()
        stats = connection.execute("SELECT COALESCE(AVG(rating), 0) AS rating, COUNT(*) AS reviews_count FROM reviews WHERE seller_id=?", (user_id,)).fetchone()
        completed = connection.execute("SELECT COUNT(*) FROM deals WHERE seller_id=? AND status='completed'", (user_id,)).fetchone()[0]
        listings = connection.execute("SELECT id, title, category, price, COALESCE(delivery_time, '') AS delivery_time, COALESCE(image_data, '') AS image_data, COALESCE(portfolio_data, '[]') AS portfolio_data FROM listings WHERE seller_id=? AND status='active' ORDER BY id DESC LIMIT 30", (user_id,)).fetchall()
    if not user:
        return jsonify({"error": "not_found"}), 404
    listing_data = []
    for row in listings:
        item = dict(row)
        if item["image_data"].startswith("tg:"):
            item["image_data"] = listing_cover_endpoint(item["id"])
        try:
            item["portfolio_data"] = json.loads(item["portfolio_data"] or "[]")
        except (TypeError, json.JSONDecodeError):
            item["portfolio_data"] = []
        listing_data.append(item)
    data = dict(user)
    try:
        data["skills"] = json.loads(data.pop("skills_json") or "[]")
    except json.JSONDecodeError:
        data["skills"] = []
    level = "Лучший исполнитель" if completed >= 25 and float(stats["rating"] or 0) >= 4.8 else "Надёжный исполнитель" if completed >= 10 else "Активный исполнитель" if completed >= 3 else "Новый исполнитель"
    return jsonify({"id": user_id, **data, **dict(stats), "completed_orders": int(completed), "level": level, "avatar_url": avatar_endpoint(user_id), "is_admin": user_id in STAFF_ADMIN_IDS, "listings": listing_data})


@app.put("/api/profile")
def update_profile_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    display_name = str(payload.get("display_name", "")).strip()[:80]
    bio = str(payload.get("bio", "")).strip()[:1000]
    skills = payload.get("skills", [])
    if not isinstance(skills, list):
        skills = []
    skills = [str(item).strip()[:40] for item in skills[:8] if str(item).strip()]
    if len(display_name) < 2:
        return jsonify({"error": "validation", "message": "Укажите имя или название команды."}), 400
    with db() as connection:
        connection.execute("UPDATE users SET display_name=?, bio=?, skills_json=? WHERE user_id=?", (display_name, bio, json.dumps(skills, ensure_ascii=False), user_id))
        connection.commit()
    return jsonify({"ok": True, "display_name": display_name, "bio": bio, "skills": skills})


@app.delete("/api/recently-viewed")
def clear_recently_viewed_api():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        connection.execute("DELETE FROM recently_viewed WHERE user_id=?", (user_id,))
        connection.commit()
    return jsonify({"ok": True})


@app.get("/api/users/<int:user_id>/reviews")
def public_user_reviews(user_id: int):
    with db() as connection:
        rows = connection.execute("""SELECT r.id, r.rating, COALESCE(r.text, '') AS text, r.created_at,
            COALESCE(u.display_name, u.username, 'Покупатель LTeam') AS reviewer_name
            FROM reviews r LEFT JOIN users u ON u.user_id=r.reviewer_id
            WHERE r.seller_id=? ORDER BY r.id DESC LIMIT 30""", (user_id,)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/tickets/history")
def tickets_history_api():
    """Return the current user's support history from the same table as the bot."""
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute(
            """SELECT id, text, status, created_at
               FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 100""",
            (user_id,),
        ).fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/tickets/new")
def create_ticket_api():
    """Create a support request that admins can handle from the Telegram bot."""
    user_id = current_user_id()
    payload = request.get_json(silent=True) or {}
    text = str(payload.get("text", "")).strip()[:2000]
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    if len(text) < 8:
        return jsonify({"error": "validation", "message": "Опишите вопрос минимум в 8 символах."}), 400
    with db() as connection:
        cursor = connection.execute(
            "INSERT INTO tickets(user_id, text, status, created_at) VALUES (?, ?, 'open', ?)",
            (user_id, text, datetime.now().isoformat()),
        )
        ticket_id = cursor.lastrowid
        connection.commit()
    notify_admins(f"🆘 Новое обращение #{ticket_id} от пользователя {user_id}\n\n{text[:900]}")
    return jsonify({"ok": True, "ticket_id": ticket_id}), 201


@app.get("/api/reviews/pending")
def pending_reviews():
    user_id = current_user_id()
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401
    with db() as connection:
        rows = connection.execute("""SELECT d.id AS deal_id, d.buyer_id, d.seller_id, d.amount, COALESCE(o.title, l.title, 'Заказ LT Market') AS title,
            CASE WHEN d.buyer_id=? THEN COALESCE(s.display_name, s.username, 'Исполнитель') ELSE COALESCE(b.display_name, b.username, 'Заказчик') END AS reviewee_name,
            CASE WHEN d.buyer_id=? THEN d.seller_id ELSE d.buyer_id END AS reviewee_id
            FROM deals d LEFT JOIN listings l ON l.id=d.listing_id LEFT JOIN orders o ON o.id=d.order_id
            LEFT JOIN users s ON s.user_id=d.seller_id LEFT JOIN users b ON b.user_id=d.buyer_id
            WHERE (d.buyer_id=? OR d.seller_id=?) AND d.status='completed' AND NOT EXISTS
              (SELECT 1 FROM reviews r WHERE r.deal_id=d.id AND r.reviewer_id=?)
            ORDER BY d.id DESC LIMIT 20""", (user_id, user_id, user_id, user_id, user_id)).fetchall()
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
        if not deal or user_id not in {int(deal["buyer_id"]), int(deal["seller_id"])} or deal["status"] != "completed":
            return jsonify({"error": "forbidden", "message": "Отзыв доступен участникам после завершённого заказа."}), 403
        exists = connection.execute("SELECT 1 FROM reviews WHERE deal_id=? AND reviewer_id=?", (deal_id, user_id)).fetchone()
        if exists:
            return jsonify({"error": "validation", "message": "Отзыв по этой сделке уже оставлен."}), 400
        reviewee_id = int(deal["seller_id"]) if user_id == int(deal["buyer_id"]) else int(deal["buyer_id"])
        quality = max(1, min(5, int(payload.get("quality_rating", rating) or rating)))
        communication = max(1, min(5, int(payload.get("communication_rating", rating) or rating)))
        deadline = max(1, min(5, int(payload.get("deadline_rating", rating) or rating)))
        connection.execute("""INSERT INTO reviews
            (deal_id, reviewer_id, seller_id, reviewee_id, rating, quality_rating, communication_rating, deadline_rating, text, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (deal_id, user_id, reviewee_id, reviewee_id, rating, quality, communication, deadline, text, datetime.now().isoformat()))
        connection.commit()
    notify_user(reviewee_id, f"По завершённому заказу #{deal_id} появился новый отзыв.", event_type="review", title="Новый отзыв", route=f"profile:{reviewee_id}", entity_id=reviewee_id)
    return jsonify({"ok": True}), 201


@app.get("/api/admin/summary")
def admin_summary():
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        payments = connection.execute("SELECT COUNT(*) FROM deals WHERE status IN ('waiting_admin_payment_approval','waiting_admin_confirm')").fetchone()[0] if PAYMENTS_ENABLED else 0
        disputes = connection.execute("SELECT COUNT(*) FROM deals WHERE status='dispute_open'").fetchone()[0]
        payouts = connection.execute("SELECT COUNT(*) FROM withdrawal_requests WHERE status='pending'").fetchone()[0] if PAYMENTS_ENABLED else 0
        moderation = connection.execute("SELECT (SELECT COUNT(*) FROM listings WHERE status IN ('pending','moderation')) + (SELECT COUNT(*) FROM orders WHERE status='moderation')").fetchone()[0]
        tickets = connection.execute("SELECT COUNT(*) FROM tickets WHERE status IN ('open', 'answered')").fetchone()[0]
        users = connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        active_listings = connection.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
        active_orders = connection.execute("SELECT COUNT(*) FROM orders WHERE status='active'").fetchone()[0]
        completed = connection.execute("SELECT COUNT(*) FROM deals WHERE status='completed'").fetchone()[0]
        reports = connection.execute("SELECT COUNT(*) FROM reports WHERE COALESCE(status, 'new')='new'").fetchone()[0]
    return jsonify({"payments": payments, "disputes": disputes, "payouts": payouts, "moderation": moderation, "tickets": tickets, "users": users, "active_listings": active_listings, "active_orders": active_orders, "completed": completed, "reports": reports})


@app.get("/api/admin/analytics")
def admin_analytics():
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        totals = {
            "users": connection.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "listings": connection.execute("SELECT COUNT(*) FROM listings").fetchone()[0],
            "orders": connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
            "deals": connection.execute("SELECT COUNT(*) FROM deals").fetchone()[0],
            "completed": connection.execute("SELECT COUNT(*) FROM deals WHERE status='completed'").fetchone()[0],
            "reviews": connection.execute("SELECT COUNT(*) FROM reviews").fetchone()[0],
        }
        days = connection.execute("""WITH RECURSIVE dates(day) AS (
            SELECT date('now', '-6 day') UNION ALL SELECT date(day, '+1 day') FROM dates WHERE day < date('now')
        ) SELECT day,
            (SELECT COUNT(*) FROM users WHERE substr(created_at,1,10)=day) AS users,
            (SELECT COUNT(*) FROM listings WHERE substr(created_at,1,10)=day) AS listings,
            (SELECT COUNT(*) FROM orders WHERE substr(created_at,1,10)=day) AS orders,
            (SELECT COUNT(*) FROM deals WHERE substr(created_at,1,10)=day) AS deals
            FROM dates ORDER BY day""").fetchall()
    totals["completion_rate"] = round((totals["completed"] / totals["deals"] * 100), 1) if totals["deals"] else 0
    return jsonify({"totals": totals, "days": [dict(row) for row in days]})


@app.get("/api/admin/users")
def admin_users():
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    query = str(request.args.get("query", "")).strip()[:80]
    status = str(request.args.get("status", "all"))
    where, params = [], []
    if query:
        where.append("(CAST(u.user_id AS TEXT) LIKE ? OR COALESCE(u.username,'') LIKE ? OR COALESCE(u.display_name,'') LIKE ?)")
        token = f"%{query}%"
        params.extend([token, token, token])
    if status == "banned":
        where.append("b.user_id IS NOT NULL")
    elif status == "active":
        where.append("b.user_id IS NULL")
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with db() as connection:
        rows = connection.execute(f"""SELECT u.user_id,
            COALESCE(NULLIF(u.display_name,''), NULLIF(u.username,''), 'Пользователь LT') AS display_name,
            COALESCE(u.username,'') AS username, COALESCE(u.verified,0) AS verified, u.created_at,
            CASE WHEN b.user_id IS NULL THEN 0 ELSE 1 END AS banned,
            COALESCE(b.reason,'') AS ban_reason,
            (SELECT COUNT(*) FROM listings l WHERE l.seller_id=u.user_id) AS listings_count,
            (SELECT COUNT(*) FROM orders o WHERE o.customer_id=u.user_id) AS orders_count,
            (SELECT COUNT(*) FROM deals d WHERE (d.buyer_id=u.user_id OR d.seller_id=u.user_id) AND d.status='completed') AS completed_count,
            (SELECT COUNT(*) FROM admin_warnings w WHERE w.user_id=u.user_id) AS warnings_count
            FROM users u LEFT JOIN banned_users b ON b.user_id=u.user_id {clause}
            ORDER BY u.created_at DESC LIMIT 80""", params).fetchall()
    return jsonify([{**dict(row), "avatar_url": avatar_endpoint(int(row["user_id"])), "is_admin": int(row["user_id"]) in STAFF_ADMIN_IDS} for row in rows])


@app.get("/api/admin/users/<int:target_id>")
def admin_user_detail(target_id: int):
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        row = connection.execute("""SELECT u.user_id,
            COALESCE(NULLIF(u.display_name,''), NULLIF(u.username,''), 'Пользователь LT') AS display_name,
            COALESCE(u.username,'') AS username, COALESCE(u.bio,'') AS bio, COALESCE(u.verified,0) AS verified,
            COALESCE(u.market_role,'both') AS market_role, u.created_at,
            CASE WHEN b.user_id IS NULL THEN 0 ELSE 1 END AS banned, COALESCE(b.reason,'') AS ban_reason,
            (SELECT COUNT(*) FROM listings l WHERE l.seller_id=u.user_id) AS listings_count,
            (SELECT COUNT(*) FROM orders o WHERE o.customer_id=u.user_id) AS orders_count,
            (SELECT COUNT(*) FROM deals d WHERE d.buyer_id=u.user_id OR d.seller_id=u.user_id) AS deals_count,
            (SELECT COUNT(*) FROM deals d WHERE (d.buyer_id=u.user_id OR d.seller_id=u.user_id) AND d.status='completed') AS completed_count,
            (SELECT COUNT(*) FROM reports r WHERE r.user_id=u.user_id OR r.target_id=u.user_id) AS reports_count,
            (SELECT COUNT(*) FROM admin_warnings w WHERE w.user_id=u.user_id) AS warnings_count
            FROM users u LEFT JOIN banned_users b ON b.user_id=u.user_id WHERE u.user_id=?""", (target_id,)).fetchone()
        warnings = connection.execute("SELECT id, reason, created_at FROM admin_warnings WHERE user_id=? ORDER BY id DESC LIMIT 10", (target_id,)).fetchall()
    if not row:
        return jsonify({"error": "not_found"}), 404
    return jsonify({**dict(row), "avatar_url": avatar_endpoint(target_id), "is_admin": target_id in STAFF_ADMIN_IDS, "warnings": [dict(item) for item in warnings]})


@app.post("/api/admin/users/<int:target_id>/action")
def admin_user_action(target_id: int):
    admin_id = require_admin_id()
    if not admin_id:
        return jsonify({"error": "forbidden"}), 403
    if target_id in STAFF_ADMIN_IDS:
        return jsonify({"error": "protected", "message": "Нельзя применять санкции к владельцу или администратору."}), 403
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).lower()
    reason = str(payload.get("reason", "")).strip()[:500]
    if action not in {"warn", "ban", "unban"}:
        return jsonify({"error": "validation", "message": "Неизвестное действие."}), 400
    if action in {"warn", "ban"} and len(reason) < 5:
        return jsonify({"error": "validation", "message": "Укажите понятную причину минимум из 5 символов."}), 400
    with db() as connection:
        if not connection.execute("SELECT 1 FROM users WHERE user_id=?", (target_id,)).fetchone():
            return jsonify({"error": "not_found"}), 404
        if action == "warn":
            connection.execute("INSERT INTO admin_warnings(user_id, admin_id, reason, created_at) VALUES (?, ?, ?, ?)", (target_id, admin_id, reason, datetime.now().isoformat()))
        elif action == "ban":
            connection.execute("INSERT OR REPLACE INTO banned_users(user_id, reason, banned_by, created_at) VALUES (?, ?, ?, ?)", (target_id, reason, admin_id, datetime.now().isoformat()))
        else:
            connection.execute("DELETE FROM banned_users WHERE user_id=?", (target_id,))
        log_admin_action(connection, admin_id, f"miniapp_{action}", target_id, reason or "Блокировка снята")
        connection.commit()
    message = {"warn": f"Администратор LT Market вынес предупреждение: {reason}", "ban": f"Доступ к LT Market ограничен. Причина: {reason}", "unban": "Доступ к LT Market восстановлен."}[action]
    notify_user(target_id, message, event_type="system", title="Решение администрации", route="support")
    return jsonify({"ok": True, "action": action})


@app.get("/api/admin/audit")
def admin_audit():
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        rows = connection.execute("""SELECT a.id, a.actor_id, a.target_id, a.action, COALESCE(a.details,'') AS details, a.created_at,
            COALESCE(NULLIF(actor.display_name,''), NULLIF(actor.username,''), CAST(a.actor_id AS TEXT)) AS actor_name,
            COALESCE(NULLIF(target.display_name,''), NULLIF(target.username,''), CAST(a.target_id AS TEXT), '—') AS target_name
            FROM admin_action_logs a LEFT JOIN users actor ON actor.user_id=a.actor_id
            LEFT JOIN users target ON target.user_id=a.target_id ORDER BY a.id DESC LIMIT 100""").fetchall()
    return jsonify([dict(row) for row in rows])


@app.get("/api/admin/moderation")
def admin_moderation_queue():
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    with db() as connection:
        listings_rows = connection.execute("""SELECT l.id, l.seller_id AS author_id, l.title, l.category, l.price AS amount,
            COALESCE(l.description, '') AS description, 'listing' AS item_type, l.status, l.created_at,
            COALESCE(NULLIF(u.display_name,''), NULLIF(u.username,''), 'Пользователь LT') AS author_name
            FROM listings l LEFT JOIN users u ON u.user_id=l.seller_id WHERE l.status IN ('pending','moderation') ORDER BY l.id ASC LIMIT 50""").fetchall()
        order_rows = connection.execute("""SELECT o.id, o.customer_id AS author_id, o.title, o.category, o.budget AS amount,
            COALESCE(o.description, '') AS description, 'order' AS item_type, o.status, o.created_at,
            COALESCE(NULLIF(u.display_name,''), NULLIF(u.username,''), 'Пользователь LT') AS author_name
            FROM orders o LEFT JOIN users u ON u.user_id=o.customer_id WHERE o.status='moderation' ORDER BY o.id ASC LIMIT 50""").fetchall()
    return jsonify([dict(row) for row in [*listings_rows, *order_rows]])


@app.get("/api/admin/queues/<queue_name>")
def admin_operation_queue(queue_name: str):
    """Compact operational queues for the private MiniApp admin workspace."""
    if not require_admin_id():
        return jsonify({"error": "forbidden"}), 403
    if queue_name not in {"payments", "payouts", "disputes", "tickets", "reports"}:
        return jsonify({"error": "not_found"}), 404
    if queue_name in {"payments", "payouts"} and not PAYMENTS_ENABLED:
        return jsonify([])
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
        elif queue_name == "tickets":
            rows = connection.execute("""SELECT id, 'Support request' AS title, 0 AS amount,
                status, user_id, text AS note, created_at FROM tickets
                WHERE status IN ('open', 'answered') ORDER BY id ASC LIMIT 50""").fetchall()
        elif queue_name == "reports":
            rows = connection.execute("""SELECT r.id, 'Жалоба пользователя' AS title, 0 AS amount,
                COALESCE(r.status,'new') AS status, r.user_id, r.reason AS note, r.created_at,
                COALESCE(r.target_type,'listing') AS target_type, COALESCE(r.target_id,r.listing_id,0) AS target_id
                FROM reports r WHERE COALESCE(r.status,'new')='new' ORDER BY r.id ASC LIMIT 50""").fetchall()
        else:
            rows = connection.execute("""SELECT dd.id, COALESCE(o.title, l.title, 'Спор по сделке') AS title,
                d.amount, d.status, dd.opened_by AS user_id, dd.reason AS note, dd.created_at, dd.deal_id
                FROM deal_disputes dd JOIN deals d ON d.id=dd.deal_id
                LEFT JOIN orders o ON o.id=d.order_id LEFT JOIN listings l ON l.id=d.listing_id
                WHERE dd.status='open' ORDER BY dd.id ASC LIMIT 50""").fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/admin/moderation/<item_type>/<int:item_id>")
def admin_moderation_decision(item_type: str, item_id: int):
    admin_id = require_admin_id()
    if not admin_id:
        return jsonify({"error": "forbidden"}), 403
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).lower()
    note = str(payload.get("note", "")).strip()[:500]
    if item_type not in {"listing", "order"} or action not in {"approve", "reject"}:
        return jsonify({"error": "validation", "message": "Неверное действие модерации."}), 400
    table, owner_column, success_status = ("listings", "seller_id", "active") if item_type == "listing" else ("orders", "customer_id", "active")
    with db() as connection:
        row = connection.execute(f"SELECT {owner_column} AS owner_id, title FROM {table} WHERE id=?", (item_id,)).fetchone()
        if not row:
            return jsonify({"error": "not_found"}), 404
        status = success_status if action == "approve" else "rejected"
        connection.execute(f"UPDATE {table} SET status=? WHERE id=?", (status, item_id))
        log_admin_action(connection, admin_id, f"moderation_{action}_{item_type}", int(row["owner_id"]), f"#{item_id} {row['title']}; {note}".strip())
        connection.commit()
    if BOT_TOKEN:
        try:
            outcome = "одобрена и опубликована" if action == "approve" else "отклонена модератором"
            comment = f"\n\nКомментарий администратора: {note}" if action == "reject" and note else ""
            body = urlencode({"chat_id": row["owner_id"], "text": f"Ваша публикация «{row['title']}» {outcome}.{comment}"}).encode()
            urlopen(Request(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=body), timeout=4).read()
        except Exception:
            pass
    return jsonify({"ok": True, "status": status})


@app.post("/api/admin/queues/<queue_name>/<int:item_id>/action")
def admin_queue_action(queue_name: str, item_id: int):
    admin_id = require_admin_id()
    if not admin_id:
        return jsonify({"error": "forbidden"}), 403
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).lower()
    note = str(payload.get("note", "")).strip()[:500]
    with db() as connection:
        if queue_name == "tickets" and action in {"close", "reopen"}:
            row = connection.execute("SELECT user_id FROM tickets WHERE id=?", (item_id,)).fetchone()
            if not row:
                return jsonify({"error": "not_found"}), 404
            status = "closed" if action == "close" else "open"
            connection.execute("UPDATE tickets SET status=? WHERE id=?", (status, item_id))
            target_id = int(row["user_id"])
        elif queue_name == "reports" and action == "close":
            row = connection.execute("SELECT user_id FROM reports WHERE id=?", (item_id,)).fetchone()
            if not row:
                return jsonify({"error": "not_found"}), 404
            connection.execute("UPDATE reports SET status='closed' WHERE id=?", (item_id,))
            target_id = int(row["user_id"])
            status = "closed"
        else:
            return jsonify({"error": "validation", "message": "Это действие недоступно для очереди."}), 400
        log_admin_action(connection, admin_id, f"{queue_name}_{action}", target_id, f"#{item_id} {note}".strip())
        connection.commit()
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
