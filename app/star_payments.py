"""Telegram Stars billing for LT Market-owned digital promotion products.

Order settlement is intentionally not implemented here: Telegram requires Stars
for digital services, while the Bot API cannot split an incoming Star payment
between independent marketplace sellers.  This module only sells promotion
that LT Market itself can deliver immediately and refund through the Bot API.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import datetime, timedelta
from urllib.request import Request, urlopen


PROMO_PRODUCTS = {
    "bump": {
        "code": "promo_bump",
        "title": "Поднять объявление",
        "description": "Поднять объявление выше в каталоге и свежей выдаче.",
        "stars": 25,
        "days": 0,
    },
    "highlight": {
        "code": "promo_highlight",
        "title": "Выделить на 7 дней",
        "description": "Добавить заметное оформление карточки на семь дней.",
        "stars": 45,
        "days": 7,
    },
    "top": {
        "code": "promo_top",
        "title": "ТОП на 7 дней",
        "description": "Закрепить объявление в приоритетной выдаче на семь дней.",
        "stars": 75,
        "days": 7,
    },
}

PRODUCT_BY_CODE = {item["code"]: {**item, "promo_type": promo_type} for promo_type, item in PROMO_PRODUCTS.items()}


class StarPaymentError(RuntimeError):
    """A safe payment error that may be shown to the user."""


def init_star_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS star_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_code TEXT NOT NULL,
            listing_id INTEGER NOT NULL DEFAULT 0,
            invoice_payload TEXT NOT NULL UNIQUE,
            stars INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'XTR',
            status TEXT NOT NULL DEFAULT 'pending',
            telegram_payment_charge_id TEXT,
            provider_payment_charge_id TEXT,
            created_at TEXT NOT NULL,
            paid_at TEXT,
            refunded_at TEXT
        )
        """
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_star_payment_charge ON star_payments(telegram_payment_charge_id) WHERE telegram_payment_charge_id IS NOT NULL"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_star_payments_user ON star_payments(user_id, id DESC)")


def public_products() -> list[dict]:
    return [
        {
            "code": product["code"],
            "promo_type": promo_type,
            "title": product["title"],
            "description": product["description"],
            "stars": product["stars"],
            "days": product["days"],
        }
        for promo_type, product in PROMO_PRODUCTS.items()
    ]


def _listing_for_payment(connection: sqlite3.Connection, listing_id: int, user_id: int):
    row = connection.execute(
        "SELECT id, seller_id, title, status FROM listings WHERE id=?",
        (int(listing_id),),
    ).fetchone()
    if not row or str(row[3]) != "active":
        raise StarPaymentError("Объявление не найдено или ещё не опубликовано.")
    if int(row[1]) != int(user_id):
        raise StarPaymentError("Продвигать объявление может только его владелец.")
    return row


def create_pending_payment(
    connection: sqlite3.Connection,
    *,
    user_id: int,
    product_code: str,
    listing_id: int,
) -> dict:
    product = PRODUCT_BY_CODE.get(str(product_code))
    if not product:
        raise StarPaymentError("Неизвестный вариант продвижения.")
    listing = _listing_for_payment(connection, listing_id, user_id)
    created_at = datetime.now().isoformat()
    temporary_payload = f"ltsp:pending:{secrets.token_urlsafe(8)}"
    cursor = connection.execute(
        """INSERT INTO star_payments
           (user_id, product_code, listing_id, invoice_payload, stars, currency, status, created_at)
           VALUES (?, ?, ?, ?, ?, 'XTR', 'pending', ?)""",
        (int(user_id), product["code"], int(listing_id), temporary_payload, int(product["stars"]), created_at),
    )
    payment_id = int(cursor.lastrowid)
    invoice_payload = f"ltsp:{payment_id}:{secrets.token_urlsafe(10)}"
    connection.execute("UPDATE star_payments SET invoice_payload=? WHERE id=?", (invoice_payload, payment_id))
    return {
        "id": payment_id,
        "user_id": int(user_id),
        "listing_id": int(listing_id),
        "listing_title": str(listing[2] or "Объявление LT Market"),
        "invoice_payload": invoice_payload,
        "product_code": product["code"],
        "promo_type": product["promo_type"],
        "title": product["title"],
        "description": product["description"],
        "stars": int(product["stars"]),
        "status": "pending",
    }


def telegram_api(bot_token: str, method: str, payload: dict) -> object:
    if not bot_token:
        raise StarPaymentError("BOT_TOKEN не настроен.")
    request = Request(
        f"https://api.telegram.org/bot{bot_token}/{method}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=12) as response:
            result = json.loads(response.read().decode("utf-8"))
    except Exception as error:
        raise StarPaymentError("Telegram временно не создал счёт. Попробуйте ещё раз.") from error
    if not result.get("ok"):
        raise StarPaymentError(str(result.get("description") or "Telegram отклонил платёжный запрос."))
    return result.get("result")


def create_invoice_link(bot_token: str, payment: dict) -> str:
    title = str(payment["title"])[:32]
    description = f"{payment['description']} Объявление: {payment['listing_title']}"[:255]
    result = telegram_api(
        bot_token,
        "createInvoiceLink",
        {
            "title": title,
            "description": description,
            "payload": payment["invoice_payload"],
            "currency": "XTR",
            "prices": [{"label": title, "amount": int(payment["stars"])}],
        },
    )
    return str(result)


def validate_pre_checkout(
    connection: sqlite3.Connection,
    *,
    invoice_payload: str,
    user_id: int,
    currency: str,
    total_amount: int,
) -> tuple[bool, str]:
    row = connection.execute(
        "SELECT user_id, listing_id, product_code, stars, currency, status FROM star_payments WHERE invoice_payload=?",
        (invoice_payload,),
    ).fetchone()
    if not row:
        return False, "Счёт не найден. Создайте новый счёт в LT Market."
    owner_id, listing_id, product_code, stars, expected_currency, status = row
    if str(status) != "pending":
        return False, "Этот счёт уже обработан."
    if int(owner_id) != int(user_id):
        return False, "Счёт создан для другого пользователя."
    if str(currency) != "XTR" or str(expected_currency) != "XTR" or int(total_amount) != int(stars):
        return False, "Параметры счёта изменились. Создайте новый счёт."
    if str(product_code) not in PRODUCT_BY_CODE:
        return False, "Товар больше недоступен."
    try:
        _listing_for_payment(connection, int(listing_id), int(user_id))
    except StarPaymentError as error:
        return False, str(error)
    return True, ""


def _extended_until(current_value: str | None, days: int) -> str:
    now = datetime.now()
    start = now
    if current_value:
        try:
            current = datetime.fromisoformat(str(current_value))
            if current > start:
                start = current
        except ValueError:
            pass
    return (start + timedelta(days=days)).isoformat()


def apply_listing_promo(connection: sqlite3.Connection, listing_id: int, promo_type: str) -> None:
    now = datetime.now().isoformat()
    if promo_type == "bump":
        connection.execute("UPDATE listings SET bumped_at=? WHERE id=?", (now, int(listing_id)))
        return
    if promo_type == "top":
        row = connection.execute("SELECT top_until FROM listings WHERE id=?", (int(listing_id),)).fetchone()
        until = _extended_until(row[0] if row else None, int(PROMO_PRODUCTS[promo_type]["days"]))
        connection.execute("UPDATE listings SET is_top=1, top_until=? WHERE id=?", (until, int(listing_id)))
        return
    if promo_type == "highlight":
        row = connection.execute("SELECT highlight_until FROM listings WHERE id=?", (int(listing_id),)).fetchone()
        until = _extended_until(row[0] if row else None, int(PROMO_PRODUCTS[promo_type]["days"]))
        connection.execute("UPDATE listings SET is_highlight=1, highlight_until=? WHERE id=?", (until, int(listing_id)))


def expire_listing_promotions(connection: sqlite3.Connection) -> None:
    now = datetime.now().isoformat()
    connection.execute("UPDATE listings SET is_top=0 WHERE is_top=1 AND COALESCE(top_until, '') < ?", (now,))
    connection.execute("UPDATE listings SET is_highlight=0 WHERE is_highlight=1 AND COALESCE(highlight_until, '') < ?", (now,))


def complete_payment(
    connection: sqlite3.Connection,
    *,
    invoice_payload: str,
    user_id: int,
    currency: str,
    total_amount: int,
    telegram_charge_id: str,
    provider_charge_id: str = "",
) -> tuple[dict, bool]:
    valid, reason = validate_pre_checkout(
        connection,
        invoice_payload=invoice_payload,
        user_id=user_id,
        currency=currency,
        total_amount=total_amount,
    )
    row = connection.execute(
        "SELECT id, user_id, product_code, listing_id, stars, status, telegram_payment_charge_id FROM star_payments WHERE invoice_payload=?",
        (invoice_payload,),
    ).fetchone()
    if row and str(row[5]) == "paid" and str(row[6] or "") == str(telegram_charge_id):
        product = PRODUCT_BY_CODE[str(row[2])]
        return {"id": row[0], "listing_id": row[3], "stars": row[4], **product}, False
    if not valid or not row:
        raise StarPaymentError(reason or "Платёж не прошёл серверную проверку.")
    product = PRODUCT_BY_CODE[str(row[2])]
    try:
        connection.execute(
            """UPDATE star_payments SET status='paid', telegram_payment_charge_id=?,
               provider_payment_charge_id=?, paid_at=? WHERE id=? AND status='pending'""",
            (str(telegram_charge_id), str(provider_charge_id or ""), datetime.now().isoformat(), int(row[0])),
        )
    except sqlite3.IntegrityError as error:
        raise StarPaymentError("Этот платёж уже был зачислен.") from error
    apply_listing_promo(connection, int(row[3]), str(product["promo_type"]))
    return {"id": row[0], "listing_id": row[3], "stars": row[4], **product}, True


def payment_status(connection: sqlite3.Connection, payment_id: int, user_id: int | None = None) -> dict | None:
    params: list[object] = [int(payment_id)]
    where = "id=?"
    if user_id is not None:
        where += " AND user_id=?"
        params.append(int(user_id))
    row = connection.execute(
        f"""SELECT id, user_id, product_code, listing_id, stars, currency, status,
            telegram_payment_charge_id, created_at, paid_at, refunded_at
            FROM star_payments WHERE {where}""",
        params,
    ).fetchone()
    if not row:
        return None
    keys = ("id", "user_id", "product_code", "listing_id", "stars", "currency", "status", "telegram_payment_charge_id", "created_at", "paid_at", "refunded_at")
    return dict(zip(keys, row))


def list_user_payments(connection: sqlite3.Connection, user_id: int, limit: int = 40) -> list[dict]:
    rows = connection.execute(
        """SELECT id, product_code, listing_id, stars, currency, status, created_at, paid_at, refunded_at
           FROM star_payments WHERE user_id=? ORDER BY id DESC LIMIT ?""",
        (int(user_id), max(1, min(100, int(limit)))),
    ).fetchall()
    keys = ("id", "product_code", "listing_id", "stars", "currency", "status", "created_at", "paid_at", "refunded_at")
    return [dict(zip(keys, row)) for row in rows]


def mark_invoice_failed(connection: sqlite3.Connection, payment_id: int) -> None:
    connection.execute("UPDATE star_payments SET status='invoice_failed' WHERE id=? AND status='pending'", (int(payment_id),))


def refund_payment(connection: sqlite3.Connection, bot_token: str, payment_id: int) -> dict:
    payment = payment_status(connection, payment_id)
    if not payment or payment["status"] != "paid" or not payment["telegram_payment_charge_id"]:
        raise StarPaymentError("Оплаченная операция не найдена или уже возвращена.")
    telegram_api(
        bot_token,
        "refundStarPayment",
        {
            "user_id": int(payment["user_id"]),
            "telegram_payment_charge_id": payment["telegram_payment_charge_id"],
        },
    )
    product = PRODUCT_BY_CODE.get(str(payment["product_code"]))
    if product and product["promo_type"] == "top":
        connection.execute("UPDATE listings SET is_top=0, top_until=NULL WHERE id=?", (int(payment["listing_id"]),))
    elif product and product["promo_type"] == "highlight":
        connection.execute("UPDATE listings SET is_highlight=0, highlight_until=NULL WHERE id=?", (int(payment["listing_id"]),))
    connection.execute(
        "UPDATE star_payments SET status='refunded', refunded_at=? WHERE id=?",
        (datetime.now().isoformat(), int(payment_id)),
    )
    return {**payment, "status": "refunded"}
