import os
import sqlite3
import html
import re
from datetime import datetime, timedelta

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
    WebAppInfo,
    BotCommand,
)

load_dotenv()  # Эта строка обязательна, она загрузит переменные из .env
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

from typing import Any, Awaitable, Callable, Dict


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
OWNER_IDS = [int(x.strip()) for x in os.getenv("OWNER_IDS", "").split(",") if x.strip()] or (ADMIN_IDS[:1] if ADMIN_IDS else [])
MODERATOR_IDS = [int(x.strip()) for x in os.getenv("MODERATOR_IDS", "").split(",") if x.strip()]
STAFF_ROLE_LEVELS = {"user": 0, "moderator": 1, "admin": 2, "owner": 3}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_PATH = "market.db"
BANNER_PATH = "Baner.png"
COMMISSION_PERCENT = 10
MIN_ORDER_BUDGET = 100
MAX_ORDER_BUDGET = 100_000
MAX_APPLICATION_PRICE = 150_000
MAX_LISTING_PRICE = 150_000
MAX_CHAT_MESSAGE_LEN = 1200

SBP_BANK = os.getenv("SBP_BANK", "Не указан")
SBP_NAME = os.getenv("SBP_NAME", "Не указан")
SBP_PHONE = os.getenv("SBP_PHONE", "Не указан")
CRYPTO_WALLET = os.getenv("CRYPTO_WALLET", "Не указан")
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()

CATEGORIES = [
    "🎨 Дизайн",
    "🤖 Telegram-боты",
    "🧠 AI-услуги",
    "✍️ Тексты",
    "🎬 Монтаж",
    "📦 Цифровые товары",
    "🛠 Другое",
]

ITEM_TYPES = [
    "🛠 Услуга",
    "📦 Товар",
    "🔑 Доступ/аккаунт",
    "📁 Файл/шаблон",
]

CATEGORY_ITEM_TYPES = {
    "🤖 Telegram-боты": [
        "🛠 Разработка бота",
        "🤖 Готовый бот",
        "⚙️ Настройка/доработка",
        "📁 Шаблон/исходник",
    ],
    "🎨 Дизайн": [
        "🛠 Дизайн-услуга",
        "🖼 Готовый дизайн",
        "📁 Шаблон/исходник",
        "🎨 Оформление проекта",
    ],
    "🧠 AI-услуги": [
        "🛠 AI-услуга",
        "⚙️ Настройка AI",
        "📁 Промпты/шаблоны",
        "🤖 AI-бот/ассистент",
    ],
    "✍️ Тексты": [
        "🛠 Написание текста",
        "✏️ Редактура",
        "📄 Готовый текст/пакет",
        "📁 Шаблон текста",
    ],
    "🎬 Монтаж": [
        "🛠 Монтаж-услуга",
        "🎞 Готовый ролик",
        "📁 Шаблон/проект",
        "🎧 Обработка звука",
    ],
    "📦 Цифровые товары": [
        "📦 Цифровой товар",
        "📁 Файл/шаблон",
        "📚 Гайд/инструкция",
        "🔑 Доступ/аккаунт",
    ],
    "🛠 Другое": [
        "🛠 Услуга",
        "📦 Товар",
        "📁 Файл/шаблон",
        "⚙️ Настройка/помощь",
    ],
}

CATEGORY_TYPE_HINTS = {
    "🤖 Telegram-боты": "Выберите формат: разработка с нуля, готовый бот, доработка или шаблон.",
    "🎨 Дизайн": "Здесь лучше подходят дизайн-услуги, готовые макеты и шаблоны.",
    "🧠 AI-услуги": "Можно разместить настройку ИИ, промпты, AI-бота или автоматизацию.",
    "✍️ Тексты": "Для текстов доступны написание, редактура, готовый пакет или шаблон.",
    "🎬 Монтаж": "Для монтажа выберите услугу, готовый ролик, проект/шаблон или звук.",
    "📦 Цифровые товары": "Для цифровых товаров подходят файлы, гайды, шаблоны и доступы.",
    "🛠 Другое": "Выберите наиболее близкий формат объявления.",
}

CATEGORY_EXAMPLES = {
    "🤖 Telegram-боты": ["Создам Telegram-бота для заявок", "Настрою бота-магазин", "Сделаю бота с кнопками"],
    "🎨 Дизайн": ["Сделаю логотип для канала", "Оформлю Telegram-канал", "Создам баннер для проекта"],
    "🧠 AI-услуги": ["Настрою AI-ассистента", "Напишу промпты для ChatGPT", "Автоматизирую задачу через ИИ"],
    "✍️ Тексты": ["Напишу продающий текст", "Сделаю описание товара", "Оформлю пост для Telegram"],
    "🎬 Монтаж": ["Смонтирую короткое видео", "Сделаю Reels/TikTok", "Добавлю субтитры"],
    "📦 Цифровые товары": ["Продам шаблон/гайд", "Продам готовый дизайн", "Продам файл/шаблон"],
    "🛠 Другое": ["Помогу с настройкой проекта", "Выполню цифровую задачу", "Консультация по проекту"],
}

ORDER_EXAMPLES = {
    "🤖 Telegram-боты": ["Нужен Telegram-бот для заявок", "Нужен бот-магазин", "Нужен бот с оплатой"],
    "🎨 Дизайн": ["Нужен логотип для проекта", "Нужно оформление Telegram-канала", "Нужен баннер для рекламы"],
    "🧠 AI-услуги": ["Нужен AI-ассистент", "Нужно настроить автоматизацию через ИИ", "Нужны промпты для ChatGPT"],
    "✍️ Тексты": ["Нужен продающий текст", "Нужно описание товара", "Нужен пост для Telegram"],
    "🎬 Монтаж": ["Нужно смонтировать короткое видео", "Нужны субтитры для ролика", "Нужен TikTok/Reels монтаж"],
    "📦 Цифровые товары": ["Нужен шаблон/файл", "Нужен гайд или инструкция", "Нужен цифровой материал"],
    "🛠 Другое": ["Нужна помощь с цифровой задачей", "Нужна настройка проекта", "Нужна консультация"],
}

PROMO_OPTIONS = {
    "bump": {"title": "🚀 Поднять объявление", "price": 50, "days": 0, "description": "Объявление поднимется выше в новых и результатах поиска."},
    "top": {"title": "🔥 В ТОП", "price": 150, "days": 7, "description": "Объявление попадёт в отдельный блок ТОП и будет выше в списках."},
    "highlight": {"title": "⭐ Выделить", "price": 80, "days": 7, "description": "Объявление будет визуально выделяться в списке и карточке."},
}

# ===== АВТО-МОДЕРАЦИЯ И АДМИН-УВЕДОМЛЕНИЯ =====
FORBIDDEN_WORDS = [
    "наркот", "заклад", "меф", "соль", "трава", "марих", "амфет", "кокаин",
    "оруж", "пистолет", "патрон", "взрыв", "бомб", "гранат",
    "скам", "обман", "кардинг", "дроп", "залив", "отмыв", "фишинг",
    "взлом", "хак", "ddos", "ддос", "ботнет", "спам рассылка", "спамер",
    "18+", "порно", "интим", "проститут", "эскорт",
    "продам stars", "куплю stars", "телеграм старс", "telegram stars",
]
CONTACT_PATTERNS = ["t.me/", "wa.me/", "vk.com/", "discord.gg/", "instagram.com/"]

def moderation_check(text: str, *, allow_contacts: bool = False) -> tuple[bool, str]:
    value = (text or "").lower()
    for word in FORBIDDEN_WORDS:
        if word in value:
            return False, f"запрещённая тема: {word}"
    if not allow_contacts:
        for pattern in CONTACT_PATTERNS:
            if pattern in value:
                return False, "контакты нельзя указывать в объявлении/заказе — общение через LTeam"
    return True, ""

def parse_money(value: str) -> int | None:
    clean = (value or "").replace(" ", "").replace("_", "")
    return int(clean) if clean.isdigit() else None


def validate_order_budget(value: int) -> tuple[bool, str]:
    if value < MIN_ORDER_BUDGET:
        return False, f"минимальный бюджет — {MIN_ORDER_BUDGET}₽"
    if value > MAX_ORDER_BUDGET:
        return False, f"максимальный бюджет — {MAX_ORDER_BUDGET}₽"
    return True, ""


def validate_listing_price(value: int) -> tuple[bool, str]:
    if value < MIN_ORDER_BUDGET:
        return False, f"минимальная цена — {MIN_ORDER_BUDGET}₽"
    if value > MAX_LISTING_PRICE:
        return False, f"максимальная цена объявления — {MAX_LISTING_PRICE}₽"
    return True, ""


def validate_application_price(value: int, order_budget: int | None = None) -> tuple[bool, str]:
    if value < MIN_ORDER_BUDGET:
        return False, f"минимальная цена отклика — {MIN_ORDER_BUDGET}₽"
    if value > MAX_APPLICATION_PRICE:
        return False, f"максимальная цена отклика — {MAX_APPLICATION_PRICE}₽"
    if order_budget and value > max(MAX_ORDER_BUDGET, order_budget * 3):
        return False, "цена отклика слишком сильно выше бюджета заказа"
    return True, ""


def text_has_too_big_number(text: str, limit: int) -> tuple[bool, int | None]:
    for raw in re.findall(r"\d[\d\s_]{2,}", text or ""):
        num = parse_money(raw)
        if num and num > limit:
            return True, num
    return False, None


def order_chat_moderation(text: str, order_budget: int | None = None) -> tuple[bool, str]:
    if len(text or "") > MAX_CHAT_MESSAGE_LEN:
        return False, f"сообщение слишком длинное, максимум {MAX_CHAT_MESSAGE_LEN} символов"

    ok, reason = moderation_check(text, allow_contacts=False)
    if not ok:
        return False, reason

    if looks_like_bypass_attempt(text):
        return False, "нельзя уводить сделку в личку, писать контакты или просить оплату напрямую"

    limit = max(MAX_APPLICATION_PRICE, (order_budget or 0) * 3)
    too_big, number = text_has_too_big_number(text, limit)
    if too_big:
        return False, f"слишком большая сумма в сообщении: {number}₽"

    return True, ""


async def notify_admins(text: str, reply_markup=None):
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, text, reply_markup=reply_markup, parse_mode="HTML")
        except Exception:
            pass

def user_public_status(user_id: int) -> str:
    if is_admin(user_id):
        return "👑 Official LTeam"
    return seller_stats(user_id).get("status", "🆕 Новый пользователь")


class CreateListing(StatesGroup):
    category = State()
    title = State()
    item_type = State()
    delivery_time = State()
    price = State()
    payout_details = State()
    description = State()


class SearchState(StatesGroup):
    query = State()


class MarketFilterState(StatesGroup):
    budget_manual = State()

class CreateOrder(StatesGroup):
    category = State()
    title = State()
    budget = State()
    deadline = State()
    description = State()


class ReceiptState(StatesGroup):
    receipt = State()


class SupportState(StatesGroup):
    text = State()
    admin_reply = State()


class ReviewState(StatesGroup):
    rating = State()
    text = State()


class ReportState(StatesGroup):
    reason = State()


class DisputeState(StatesGroup):
    reason = State()


class DealChatState(StatesGroup):
    text = State()


class OrderChatState(StatesGroup):
    text = State()


class OrderResponseState(StatesGroup):
    price = State()
    deadline = State()
    text = State()


class AdminBanState(StatesGroup):
    user_id = State()


class AdminUnbanState(StatesGroup):
    user_id = State()


class AdminSearchUserState(StatesGroup):
    user_id = State()


class AdminMessageState(StatesGroup):
    text = State()


class AdminWarnState(StatesGroup):
    reason = State()


class AdminReasonState(StatesGroup):
    reason = State()


class AdminRoleState(StatesGroup):
    user_id = State()


class AdminMuteState(StatesGroup):
    user_id = State()
    duration = State()
    reason = State()


class BroadcastState(StatesGroup):
    text = State()


class PromoState(StatesGroup):
    receipt = State()


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


def save_user(message: Message):
    with db() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, created_at)
            VALUES (?, ?, ?)
            """,
            (
                message.from_user.id,
                message.from_user.username or "",
                datetime.now().isoformat(),
            ),
        )
        conn.commit()



def get_role(user_id: int) -> str:
    """Возвращает роль пользователя: owner/admin/moderator/user."""
    if user_id in OWNER_IDS:
        return "owner"
    with db() as conn:
        row = conn.execute("SELECT role FROM staff_roles WHERE user_id=?", (user_id,)).fetchone()
    if row and row[0] in STAFF_ROLE_LEVELS:
        return row[0]
    if user_id in ADMIN_IDS:
        return "admin"
    if user_id in MODERATOR_IDS:
        return "moderator"
    return "user"


def role_level(user_id: int) -> int:
    return STAFF_ROLE_LEVELS.get(get_role(user_id), 0)


def role_badge(user_id: int) -> str:
    return {
        "owner": "👑 Владелец",
        "admin": "🛡 Админ",
        "moderator": "🔧 Модератор",
        "user": "👤 Пользователь",
    }.get(get_role(user_id), "👤 Пользователь")


def is_owner(user_id: int) -> bool:
    return get_role(user_id) == "owner"


def is_admin(user_id: int) -> bool:
    return role_level(user_id) >= STAFF_ROLE_LEVELS["admin"]


def is_staff(user_id: int) -> bool:
    return role_level(user_id) >= STAFF_ROLE_LEVELS["moderator"]


def can_act(actor_id: int, target_id: int | None = None, action: str = "") -> tuple[bool, str]:
    """Единая проверка опасных админ-действий.

    Главное правило:
    - пользователь без роли не может админ-действия;
    - нельзя действовать на самого себя;
    - нельзя действовать на равную или старшую роль;
    - владельца нельзя банить/мутить/наказывать через обычные кнопки, даже другому владельцу.
    """
    actor_role = get_role(actor_id)
    actor_level = role_level(actor_id)
    target_role = get_role(target_id) if target_id else "user"
    target_level = role_level(target_id) if target_id else 0

    if actor_level <= 0:
        return False, "Нет доступа."

    if target_id and int(actor_id) == int(target_id):
        return False, "Нельзя выполнять это действие над самим собой."

    protected_target_actions = {
        "ban", "unban", "mute", "warn", "verify", "unverify",
        "deal_manage", "force_unban"
    }

    if target_id and target_role == "owner" and action in protected_target_actions:
        return False, "Владельца нельзя банить, мутить или наказывать через админку."

    if target_id and actor_level <= target_level:
        return False, "Нельзя выполнять действие над равной или старшей ролью."

    admin_actions = {"ban", "unban", "verify", "unverify", "broadcast", "finance", "deal_manage"}
    owner_actions = {"set_role", "remove_role", "force_unban", "approve_admin_request"}
    moderator_actions = {"moderate", "warn", "mute", "view_user", "view_reports"}

    if action in owner_actions and actor_role != "owner":
        return False, "Это действие доступно только владельцу."
    if action in admin_actions and actor_level < STAFF_ROLE_LEVELS["admin"]:
        return False, "Это действие доступно только админу или владельцу."
    if action in moderator_actions and actor_level < STAFF_ROLE_LEVELS["moderator"]:
        return False, "Это действие доступно только модератору, админу или владельцу."
    return True, ""


def log_admin_action(actor_id: int, action: str, target_id: int | None = None, details: str = "") -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO admin_action_logs (actor_id, target_id, action, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (actor_id, target_id, action, details, datetime.now().isoformat()),
        )
        conn.commit()


def is_banned(user_id: int) -> bool:
    # Staff не должен блокироваться даже если его случайно занесли в banned_users.
    # Это защита от ситуации, когда админ/владелец случайно забанил другого staff раньше.
    if is_staff(user_id):
        return False
    with db() as conn:
        return conn.execute("SELECT 1 FROM banned_users WHERE user_id=?", (user_id,)).fetchone() is not None


def get_mute(user_id: int):
    with db() as conn:
        row = conn.execute("SELECT muted_until, reason FROM muted_users WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        return None
    try:
        until = datetime.fromisoformat(row[0])
    except Exception:
        return None
    if until <= datetime.now():
        with db() as conn:
            conn.execute("DELETE FROM muted_users WHERE user_id=?", (user_id,))
            conn.commit()
        return None
    return until, row[1]


def set_mute(user_id: int, minutes: int, reason: str, muted_by: int = 0) -> None:
    until = datetime.now() + timedelta(minutes=minutes)
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO muted_users (user_id, muted_until, reason, muted_by, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, until.isoformat(), reason, muted_by, datetime.now().isoformat()),
        )
        conn.commit()


def register_message_rate(user_id: int) -> tuple[bool, int, str]:
    """Антиспам: возвращает (muted, minutes, reason). Стафф не мутится автоматически."""
    if is_staff(user_id):
        return False, 0, ""
    now = datetime.now()
    with db() as conn:
        row = conn.execute("SELECT window_start, count, strikes FROM user_message_limits WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            conn.execute("INSERT OR REPLACE INTO user_message_limits (user_id, window_start, count, strikes) VALUES (?, ?, 1, 0)", (user_id, now.isoformat()))
            conn.commit()
            return False, 0, ""
        try:
            start = datetime.fromisoformat(row[0])
        except Exception:
            start = now
        count = int(row[1] or 0)
        strikes = int(row[2] or 0)
        if (now - start).total_seconds() > 10:
            conn.execute("UPDATE user_message_limits SET window_start=?, count=1 WHERE user_id=?", (now.isoformat(), user_id))
            conn.commit()
            return False, 0, ""
        count += 1
        if count >= 7:
            strikes += 1
            minutes = 1 if strikes == 1 else (10 if strikes == 2 else 60)
            reason = f"Авто-мут за спам: {count} сообщений за 10 секунд"
            conn.execute("UPDATE user_message_limits SET window_start=?, count=0, strikes=? WHERE user_id=?", (now.isoformat(), strikes, user_id))
            conn.commit()
            set_mute(user_id, minutes, reason, muted_by=0)
            return True, minutes, reason
        conn.execute("UPDATE user_message_limits SET count=? WHERE user_id=?", (count, user_id))
        conn.commit()
    return False, 0, ""


class BanMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user and is_banned(user.id):
            if isinstance(event, Message):
                await event.answer("🚫 Вы заблокированы в LTeam Market. Можно подать апелляцию через поддержку владельцам.")
            elif isinstance(event, CallbackQuery):
                await event.answer("🚫 Вы заблокированы в LTeam Market.", show_alert=True)
            return
        if user:
            mute = get_mute(user.id)
            if mute:
                until, reason = mute
                text = f"🔇 Вы в муте до {until.strftime('%d.%m %H:%M')}. Причина: {html.escape(reason or 'не указана')}"
                if isinstance(event, Message):
                    await event.answer(text, parse_mode="HTML")
                elif isinstance(event, CallbackQuery):
                    await event.answer(text, show_alert=True)
                return
            if isinstance(event, Message) and event.text and not event.text.startswith("/"):
                muted, minutes, reason = register_message_rate(user.id)
                if muted:
                    await event.answer(f"🔇 Авто-мут на {minutes} мин. Причина: {html.escape(reason)}", parse_mode="HTML")
                    await notify_admins(f"""🔇 <b>Авто-мут за спам</b>

Пользователь: <code>{user.id}</code>
Срок: <b>{minutes} мин.</b>
Причина: {html.escape(reason)}""")
                    return
        return await handler(event, data)


dp.message.middleware(BanMiddleware())
dp.callback_query.middleware(BanMiddleware())


def save_screen(user_id: int, chat_id: int, message_id: int) -> None:
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO user_screens (user_id, chat_id, message_id) VALUES (?, ?, ?)",
            (user_id, chat_id, message_id),
        )
        conn.commit()


def get_screen(user_id: int):
    with db() as conn:
        return conn.execute(
            "SELECT chat_id, message_id FROM user_screens WHERE user_id=?",
            (user_id,),
        ).fetchone()


async def show_screen(call: CallbackQuery, text: str, reply_markup=None, parse_mode: str = "HTML"):
    """Обновляет текущий экран. Если сообщение нельзя редактировать — удаляет его и отправляет новый."""
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        save_screen(call.from_user.id, call.message.chat.id, call.message.message_id)
        return
    except Exception:
        try:
            await call.message.delete()
        except Exception:
            pass

        sent = await call.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
        save_screen(call.from_user.id, sent.chat.id, sent.message_id)


async def screen_answer(message: Message, text: str, reply_markup=None, parse_mode: str = "HTML", **kwargs):
    """Показывает следующий экран после текстового ввода пользователя без лишнего спама."""
    user_id = message.from_user.id

    # Удаляем сообщение пользователя: оно было нужно только как ввод для шага.
    try:
        await message.delete()
    except Exception:
        pass

    # Удаляем прошлый экран бота, если он известен.
    row = get_screen(user_id)
    if row:
        chat_id, message_id = row
        try:
            await bot.delete_message(chat_id, message_id)
        except Exception:
            pass

    sent = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode, **kwargs)
    save_screen(user_id, sent.chat.id, sent.message_id)
    return sent


def user_contact(user_id: int) -> str:
    """Красивый контакт пользователя для сообщений админам/клиентам."""
    with db() as conn:
        row = conn.execute(
            "SELECT username FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()

    if row and row[0]:
        return f"@{row[0]} (<code>{user_id}</code>)"
    return f"<code>{user_id}</code>"




def seller_stats(user_id: int) -> dict:
    """Статистика продавца для красивой карточки объявления."""
    with db() as conn:
        username_row = conn.execute("SELECT username, created_at, COALESCE(verified, 0) FROM users WHERE user_id=?", (user_id,)).fetchone()
        active_listings = conn.execute("SELECT COUNT(*) FROM listings WHERE seller_id=? AND status='active'", (user_id,)).fetchone()[0]
        sales_count = conn.execute("SELECT COUNT(*) FROM deals WHERE seller_id=? AND status='completed'", (user_id,)).fetchone()[0]
        rating_row = conn.execute("SELECT AVG(rating), COUNT(*) FROM reviews WHERE seller_id=?", (user_id,)).fetchone()
        reports_count = conn.execute("SELECT COUNT(*) FROM reports r JOIN listings l ON l.id=r.listing_id WHERE l.seller_id=?", (user_id,)).fetchone()[0]

    username = username_row[0] if username_row and username_row[0] else "не указан"
    created_at = username_row[1] if username_row and username_row[1] else "неизвестно"
    verified = int(username_row[2]) if username_row and len(username_row) > 2 and username_row[2] is not None else 0
    avg_rating, reviews_count = rating_row
    avg_rating_value = 0 if avg_rating is None else float(avg_rating)
    rating_text = "нет отзывов" if avg_rating is None else f"{avg_rating_value:.1f} ⭐ ({reviews_count})"

    if is_admin(user_id):
        status = "👑 Official LTeam"
    elif verified:
        status = "🛡 Верифицирован LTeam"
    elif sales_count >= 5 and avg_rating_value >= 4.5 and reports_count == 0:
        status = "🏆 Кандидат на LTeam Verified"
    elif sales_count >= 3 and avg_rating_value >= 4.5:
        status = "✅ Проверенный продавец"
    elif sales_count > 0:
        status = "📈 Есть продажи"
    else:
        status = "🆕 Новый продавец"

    return {
        "username": username,
        "created_at": created_at,
        "active_listings": active_listings,
        "sales_count": sales_count,
        "rating_text": rating_text,
        "reviews_count": reviews_count or 0,
        "reports_count": reports_count,
        "verified": verified,
        "status": status,
    }


def seller_card_text(user_id: int) -> str:
    stats = seller_stats(user_id)
    return f"""
━━━━━━━━━━━━━━
👤 <b>Продавец</b>
━━━━━━━━━━━━━━

🆔 ID: <code>{user_id}</code>
🔗 Username: @{html.escape(stats['username'])}
⭐ Рейтинг: <b>{stats['rating_text']}</b>
💰 Завершённых продаж: <b>{stats['sales_count']}</b>
📦 Активных объявлений: <b>{stats['active_listings']}</b>
🏷 Статус: <b>{stats['status']}</b>
"""


# ===== UX / БЕЗОПАСНОСТЬ / ИСТОРИЯ ЧАТОВ =====

SCAM_TRIGGERS = [
    "переведи напрямую",
    "перевод напрямую",
    "без гаранта",
    "мимо гаранта",
    "в личку",
    "напиши в лс",
    "мой номер",
    "карта напрямую",
    "оплата напрямую",
    "скинь на карту",
    "telegram.me/",
    "t.me/",
    "@",
]

def looks_like_bypass_attempt(text: str) -> bool:
    clean = (text or "").lower()
    return any(trigger in clean for trigger in SCAM_TRIGGERS)

async def warn_if_bypass_attempt(sender_id: int, text: str, context: str):
    if not looks_like_bypass_attempt(text):
        return

    try:
        ensure_admin_tables()
        with db() as conn:
            conn.execute(
                "INSERT INTO security_events (user_id, event_type, context, text, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (sender_id, "bypass_attempt", context, (text or "")[:1000], "new", datetime.now().isoformat()),
            )
            conn.commit()
    except Exception:
        pass

    try:
        await bot.send_message(
            sender_id,
            "⚠️ <b>LTeam Protect</b>\n\n"
            "В переписке нельзя уводить сделку в личные сообщения или просить оплату напрямую. "
            "Для безопасности используйте чат и гарант LTeam.",
            parse_mode="HTML",
        )
    except Exception:
        pass

    await notify_admins(
        f"""
🛡 <b>LTeam Protect: подозрительное сообщение</b>

Контекст: <b>{html.escape(context)}</b>
Пользователь: <code>{sender_id}</code>

Текст:
{html.escape((text or '')[:800])}
"""
    )

def format_chat_history(rows, current_user_id: int | None = None, limit_note: str = "последние сообщения") -> str:
    if not rows:
        return f"Пока нет сообщений ({limit_note})."

    lines = []
    for sender_id, text, created_at in rows:
        who = "Вы" if current_user_id and int(sender_id) == int(current_user_id) else f"ID {sender_id}"
        time_text = ""
        if created_at:
            try:
                time_text = created_at.replace("T", " ")[:16]
            except Exception:
                time_text = str(created_at)[:16]
        lines.append(f"• <b>{html.escape(who)}</b> <code>{html.escape(time_text)}</code>\n{html.escape(text or '')}")
    return "\n\n".join(lines)

def get_deal_chat_history(deal_id: int, limit: int = 10):
    with db() as conn:
        return conn.execute(
            """
            SELECT sender_id, text, created_at
            FROM deal_messages
            WHERE deal_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (deal_id, limit),
        ).fetchall()[::-1]

def get_order_chat_history(order_id: int, limit: int = 10):
    with db() as conn:
        return conn.execute(
            """
            SELECT sender_id, text, created_at
            FROM order_messages
            WHERE order_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (order_id, limit),
        ).fetchall()[::-1]

async def setup_bot_commands():
    await bot.set_my_commands([
        BotCommand(command="start", description="🔄 Перезапустить бота"),
        BotCommand(command="menu", description="☰ Открыть меню"),
        BotCommand(command="help", description="❓ Помощь"),
        BotCommand(command="rules", description="📜 Правила"),
    ])


def main_menu(user_id: int | None = None):
    buttons = []

    if WEBAPP_URL.startswith("http"):
        buttons.append([
            InlineKeyboardButton(text="🚀 Открыть LTeam App", web_app=WebAppInfo(url=WEBAPP_URL))
        ])

    buttons.extend([
        [InlineKeyboardButton(text="🛒 ОТКРЫТЬ МАРКЕТ", callback_data="market")],
        [
            InlineKeyboardButton(text="➕ Разместить", callback_data="create_listing"),
            InlineKeyboardButton(text="📌 Создать заказ", callback_data="create_order"),
        ],
        [
            InlineKeyboardButton(text="📦 Мои объявления", callback_data="my_listings"),
            InlineKeyboardButton(text="💬 Мои сделки", callback_data="my_deals"),
        ],
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
            InlineKeyboardButton(text="🆘 Поддержка", callback_data="support"),
        ],
        [
            InlineKeyboardButton(text="ℹ️ О компании", callback_data="about_company"),
            InlineKeyboardButton(text="📜 Правила", callback_data="rules"),
        ],
    ])

    if user_id and is_staff(user_id):
        buttons.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def back_home():

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")]
    ])


def listings_keyboard(rows):
    buttons = []
    for listing_id, title, price in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"{title} — {price}₽",
                callback_data=f"view_listing:{listing_id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Маркет", callback_data="market")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def send_home(message: Message):
    text = """
━━━━━━━━━━━━━━
🚀 <b>LTeam Market</b>
━━━━━━━━━━━━━━

Маркет цифровых услуг, заказов и безопасных сделок через гаранта LTeam.

🛒 Маркет — найти услугу или товар
➕ Разместить — создать своё объявление
📌 Создать заказ — найти исполнителя под задачу

Выберите действие:
"""


    if os.path.exists(BANNER_PATH):
        await message.answer_photo(
            FSInputFile(BANNER_PATH),
            caption=text,
            reply_markup=main_menu(message.from_user.id),
            parse_mode="HTML",
        )
    else:
        await screen_answer(message,text, reply_markup=main_menu(message.from_user.id), parse_mode="HTML")


@dp.message(CommandStart())
async def start(message: Message):
    save_user(message)
    await send_home(message)


@dp.message(Command("menu"))
async def command_menu(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    await screen_answer(
        message,
        """
━━━━━━━━━━━━━━
☰ <b>Меню LTeam</b>
━━━━━━━━━━━━━━

Команды:
• /start — перезапустить бота
• /menu — открыть это меню
• /help — помощь и правила

Основные разделы доступны ниже:
""",
        reply_markup=main_menu(message.from_user.id),
        parse_mode="HTML",
    )

@dp.message(Command("help"))
async def command_help(message: Message):
    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
❓ <b>Помощь LTeam</b>
━━━━━━━━━━━━━━

🛡 Сделки проходят через гаранта LTeam.
💬 Общение — только через бот.
💰 Комиссия сервиса: <b>{COMMISSION_PERCENT}%</b>.

Если возникла проблема — используйте жалобу, спор или поддержку.
""",
        reply_markup=main_menu(message.from_user.id),
        parse_mode="HTML",
    )


@dp.message(Command("rules"))
async def command_rules(message: Message):
    await screen_answer(
        message,
        f"""
📜 <b>Правила LTeam Market</b>

1. Не переходите в личные сообщения для сделки, если заказ начался через LTeam.
2. Не отправляйте оплату напрямую продавцу или покупателю.
3. Оплата проходит только по реквизитам LTeam, которые показывает бот.
4. Комиссия сервиса: <b>{COMMISSION_PERCENT}%</b>.
5. Запрещены обман, спам, фейковые чеки и запрещённые товары/услуги.
6. При проблеме открывайте спор или пишите в поддержку.

Нарушение правил может привести к муту, предупреждению или блокировке.
""",
        reply_markup=main_menu(message.from_user.id),
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "home")
async def home(call: CallbackQuery):
    await show_screen(
        call,
        """
━━━━━━━━━━━━━━
🚀 <b>LTeam Market</b>
━━━━━━━━━━━━━━

Выберите действие:
""",
        reply_markup=main_menu(call.from_user.id),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "about_company")
async def about_company(call: CallbackQuery):
    await show_screen(
        call,
        """
ℹ️ <b>О компании LTeam</b>

LTeam — команда, которая создаёт Telegram-ботов, Mini App, цифровые услуги и свои проекты.

Сейчас основной продукт — <b>LTeam Market</b>: площадка цифровых услуг и товаров с безопасной сделкой через гаранта.

Наша цель — сделать сеть полезных ботов, развить соцсети, собрать портфолио и выйти на рынок заказов.
""",
        reply_markup=back_home(),
    )
    await call.answer()


@dp.callback_query(F.data == "rules")
async def rules(call: CallbackQuery):
    await show_screen(
        call,
        f"""
📜 <b>Правила LTeam Market</b>

1. Не переходите в личные сообщения для сделки, если заказ начался через LTeam.
2. Не отправляйте оплату напрямую продавцу или покупателю.
3. Оплата проходит только по реквизитам LTeam, которые показывает бот.
4. Комиссия сервиса: <b>{COMMISSION_PERCENT}%</b>.
5. Запрещены обман, спам, фейковые чеки и запрещённые товары/услуги.
6. При проблеме открывайте спор или пишите в поддержку.

Нарушение правил может привести к блокировке аккаунта в боте.
""",
        reply_markup=back_home(),
    )
    await call.answer()


@dp.callback_query(F.data == "admin_panel")
async def admin_panel(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    with db() as conn:
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        listing_moderation = conn.execute("SELECT COUNT(*) FROM listings WHERE status='moderation'").fetchone()[0]
        order_moderation = conn.execute("SELECT COUNT(*) FROM orders WHERE status='moderation'").fetchone()[0]
        active_deals = conn.execute("SELECT COUNT(*) FROM deals WHERE status NOT IN ('completed', 'cancelled', 'deleted')").fetchone()[0]
        waiting_payment = conn.execute("SELECT COUNT(*) FROM deals WHERE status='waiting_admin_confirm'").fetchone()[0]
        waiting_payout = conn.execute("SELECT COUNT(*) FROM deals WHERE status='waiting_payout'").fetchone()[0]
        reports_count = conn.execute("SELECT COUNT(*) FROM reports WHERE COALESCE(status, 'new')='new'").fetchone()[0]
        tickets_count = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
        turnover = conn.execute("SELECT COALESCE(SUM(amount), 0) FROM deals WHERE status IN ('completed', 'waiting_payout')").fetchone()[0]
        commission = conn.execute("SELECT COALESCE(SUM(commission), 0) FROM deals WHERE status='completed'").fetchone()[0]

    moderation_total = listing_moderation + order_moderation
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
⚙️ <b>Админ-центр LTeam</b>
━━━━━━━━━━━━━━

Ваш доступ: <b>{role_badge(call.from_user.id)}</b>

🧭 <b>Главное</b>
👥 Пользователей: <b>{users_count}</b>
⏳ На модерации: <b>{moderation_total}</b>
   • объявлений: <b>{listing_moderation}</b>
   • заказов: <b>{order_moderation}</b>

💰 <b>Сделки и финансы</b>
🧾 Чеков на проверке: <b>{waiting_payment}</b>
💸 Ожидают выплаты: <b>{waiting_payout}</b>
🤝 Активных сделок: <b>{active_deals}</b>
📈 Оборот: <b>{turnover}₽</b>
💵 Комиссия LTeam: <b>{commission}₽</b>

🛡 <b>Безопасность</b>
🚨 Новых жалоб: <b>{reports_count}</b>
🆘 Обращений в поддержку: <b>{tickets_count}</b>
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⏳ Модерация ({moderation_total})", callback_data="admin_moderation")],
            [InlineKeyboardButton(text=f"📦 Объявления ({listing_moderation})", callback_data="admin_mod_listings"), InlineKeyboardButton(text=f"📌 Заказы ({order_moderation})", callback_data="admin_mod_orders")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"), InlineKeyboardButton(text="💰 Финансы 2.0", callback_data="admin_finance_v2")],
            [InlineKeyboardButton(text="🤝 Сделки", callback_data="admin_deals_center"), InlineKeyboardButton(text="💸 Выплаты", callback_data="admin_deals_payouts")],
            [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users_page:0"), InlineKeyboardButton(text="🔎 Найти", callback_data="admin_find_user")],
            [InlineKeyboardButton(text=f"🛡 Безопасность ({reports_count})", callback_data="admin_security_center"), InlineKeyboardButton(text="👑 Роли", callback_data="admin_roles_panel")],
            [InlineKeyboardButton(text="💰 Продвижение на проверке", callback_data="admin_promo_pending")],
            [InlineKeyboardButton(text="🚫 Забанить", callback_data="admin_ban_start"), InlineKeyboardButton(text="✅ Разбанить", callback_data="admin_unban_start")],
            [InlineKeyboardButton(text="👀 Чаты", callback_data="admin_chat_hint"), InlineKeyboardButton(text="📢 Рассылка 2.0", callback_data="admin_broadcast_target")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_panel")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()

@dp.callback_query(F.data == "admin_chat_hint")
async def admin_chat_hint(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await show_screen(
        call,
        """
━━━━━━━━━━━━━━
👀 <b>Просмотр чатов</b>
━━━━━━━━━━━━━━

Команды для админов:
<code>/deal_chat 15</code> — чат сделки
<code>/order_chat 7</code> — чат заказа

Также кнопки просмотра чата доступны внутри карточек сделок и заказов.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "admin_ban_start")
async def admin_ban_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminBanState.user_id)
    await show_screen(call, "🚫 Отправьте Telegram ID пользователя, которого нужно забанить:")
    await call.answer()


@dp.message(AdminBanState.user_id)
async def admin_ban_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    if not message.text or not message.text.strip().isdigit():
        await screen_answer(message,"Отправьте только числовой ID.")
        return
    user_id = int(message.text.strip())
    ok, reason = can_act(message.from_user.id, user_id, "ban")
    if not ok:
        await state.clear()
        await screen_answer(message, f"❌ {html.escape(reason)}", parse_mode="HTML")
        return
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO banned_users (user_id, reason, banned_by, created_at) VALUES (?, ?, ?, ?)",
            (user_id, "Блокировка администратором", message.from_user.id, datetime.now().isoformat()),
        )
        conn.commit()
    log_admin_action(message.from_user.id, "ban_user", user_id, "Блокировка через ввод ID")
    await state.clear()
    await screen_answer(message,f"✅ Пользователь <code>{user_id}</code> забанен.", parse_mode="HTML")


@dp.callback_query(F.data == "admin_unban_start")
async def admin_unban_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminUnbanState.user_id)
    await show_screen(call, "✅ Отправьте Telegram ID пользователя, которого нужно разбанить:")
    await call.answer()


@dp.message(AdminUnbanState.user_id)
async def admin_unban_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    if not message.text or not message.text.strip().isdigit():
        await screen_answer(message,"Отправьте только числовой ID.")
        return
    user_id = int(message.text.strip())
    ok, reason = can_act(message.from_user.id, user_id, "unban")
    if not ok:
        await state.clear()
        await screen_answer(message, f"❌ {html.escape(reason)}", parse_mode="HTML")
        return
    with db() as conn:
        ban = conn.execute("SELECT banned_by FROM banned_users WHERE user_id=?", (user_id,)).fetchone()
        banned_by = int(ban[0] or 0) if ban else 0
        if ban and (is_owner(message.from_user.id) or banned_by in (0, message.from_user.id)):
            conn.execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
            conn.commit()
            log_admin_action(message.from_user.id, "unban_user", user_id, "Разбан через ввод ID")
            await state.clear()
            await screen_answer(message,f"✅ Пользователь <code>{user_id}</code> разбанен.", parse_mode="HTML")
            return
    await state.clear()
    await screen_answer(message, "🔁 Этот бан выдал другой админ. Откройте карточку пользователя и нажмите разбан — будет создан запрос.", parse_mode="HTML")


@dp.callback_query(F.data == "guarantee")
async def guarantee(call: CallbackQuery):
    await show_screen(call, 
        f"""
🛡 <b>Как работает гарант LTeam</b>

1. Покупатель выбирает объявление.
2. Оплачивает заказ на реквизиты LTeam.
3. Админ подтверждает оплату.
4. Исполнитель выполняет заказ.
5. Покупатель подтверждает выполнение.
6. LTeam переводит деньги исполнителю.

Комиссия сервиса: <b>{COMMISSION_PERCENT}%</b>.
""",
        reply_markup=back_home(),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "market")
async def market(call: CallbackQuery):
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
        new_count = conn.execute("SELECT COUNT(*) FROM listings WHERE status='active' AND id >= (SELECT COALESCE(MAX(id),0)-20 FROM listings)").fetchone()[0]

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
🛒 <b>LTeam Market</b>
━━━━━━━━━━━━━━

Найдите услугу, товар, исполнителя или заказ под вашу задачу.

📦 Активных объявлений: <b>{total}</b>
🆕 Новых объявлений: <b>{new_count}</b>

Выберите удобный способ поиска:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_start"), InlineKeyboardButton(text="🎯 Подбор", callback_data="market_filter")],
            [InlineKeyboardButton(text="📂 Категории", callback_data="market_categories"), InlineKeyboardButton(text="🆕 Новые", callback_data="market_new")],
            [InlineKeyboardButton(text="🔥 ТОП", callback_data="market_top"), InlineKeyboardButton(text="🛡 LTeam Verified", callback_data="market_verified")],
            [InlineKeyboardButton(text="📋 Заказы клиентов", callback_data="orders_list")],
            [InlineKeyboardButton(text="➕ Разместить", callback_data="create_listing"), InlineKeyboardButton(text="📌 Создать заказ", callback_data="create_order")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


def rating_label(value: float) -> str:
    if value <= 0:
        return "нет отзывов"
    return f"{value:.1f} ⭐"


def seller_badge(sales_count: int, avg_rating: float, reports_count: int, verified: int = 0) -> str:
    if verified:
        return "🛡 LTeam"
    if sales_count >= 5 and avg_rating >= 4.5 and reports_count == 0:
        return "🏆 Кандидат"
    if sales_count >= 3 and avg_rating >= 4.5:
        return "✅ Проверен"
    if sales_count > 0:
        return "📈 Есть продажи"
    return "🆕 Новый"


def get_market_results(category=None, min_price=None, max_price=None, min_rating=None, verified_mode="all", order="new", limit=10):
    where = ["l.status='active'"]
    params = []

    if category and category != "all":
        where.append("l.category=?")
        params.append(category)
    if min_price is not None:
        where.append("l.price>=?")
        params.append(min_price)
    if max_price is not None:
        where.append("l.price<=?")
        params.append(max_price)
    if min_rating and min_rating > 0:
        where.append("COALESCE(rs.avg_rating,0)>=?")
        params.append(min_rating)
    if verified_mode == "trusted":
        where.append("(COALESCE(u.verified,0)=1 OR (COALESCE(ds.sales_count,0)>=3 AND COALESCE(rs.avg_rating,0)>=4.5))")
    elif verified_mode == "verified":
        where.append("COALESCE(u.verified,0)=1")

    order_sql = "l.id DESC"
    if order == "cheap":
        order_sql = "l.price ASC"
    elif order == "expensive":
        order_sql = "l.price DESC"
    elif order == "rating":
        order_sql = "COALESCE(rs.avg_rating,0) DESC, l.id DESC"

    sql = f"""
        SELECT l.id, l.title, l.price, l.category, l.seller_id,
               COALESCE(rs.avg_rating,0) AS avg_rating,
               COALESCE(rs.reviews_count,0) AS reviews_count,
               COALESCE(ds.sales_count,0) AS sales_count,
               COALESCE(rep.reports_count,0) AS reports_count,
               COALESCE(u.verified,0) AS verified,
               COALESCE(l.is_top,0) AS is_top,
               COALESCE(l.is_highlight,0) AS is_highlight
        FROM listings l
        LEFT JOIN users u ON u.user_id = l.seller_id
        LEFT JOIN (
            SELECT seller_id, AVG(rating) AS avg_rating, COUNT(*) AS reviews_count
            FROM reviews
            GROUP BY seller_id
        ) rs ON rs.seller_id = l.seller_id
        LEFT JOIN (
            SELECT seller_id, COUNT(*) AS sales_count
            FROM deals
            WHERE status='completed'
            GROUP BY seller_id
        ) ds ON ds.seller_id = l.seller_id
        LEFT JOIN (
            SELECT l2.seller_id, COUNT(r.id) AS reports_count
            FROM listings l2
            LEFT JOIN reports r ON r.listing_id = l2.id
            GROUP BY l2.seller_id
        ) rep ON rep.seller_id = l.seller_id
        WHERE {' AND '.join(where)}
        ORDER BY COALESCE(l.is_top,0) DESC, COALESCE(l.is_highlight,0) DESC, {order_sql}
        LIMIT ?
    """
    params.append(limit)
    with db() as conn:
        return conn.execute(sql, params).fetchall()


def promo_marker(is_top: int = 0, is_highlight: int = 0) -> str:
    parts = []
    if is_top:
        parts.append("🔥 ТОП")
    if is_highlight:
        parts.append("⭐ Выделено")
    return " | ".join(parts)


def market_results_keyboard(rows, back_callback="market"):
    buttons = []
    row_buttons = []
    for i, row in enumerate(rows, start=1):
        listing_id = row[0]
        row_buttons.append(InlineKeyboardButton(text=f"{i}️⃣", callback_data=f"view_listing:{listing_id}"))
        if len(row_buttons) == 5:
            buttons.append(row_buttons)
            row_buttons = []
    if row_buttons:
        buttons.append(row_buttons)
    buttons.append([InlineKeyboardButton(text="🎯 Изменить фильтр", callback_data="market_filter")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def market_results_text(title: str, rows) -> str:
    if not rows:
        return f"""
━━━━━━━━━━━━━━
{title}
━━━━━━━━━━━━━━

❌ Ничего не найдено.

Попробуйте изменить категорию, бюджет или рейтинг.
"""
    lines = [f"━━━━━━━━━━━━━━\n{title}\n━━━━━━━━━━━━━━\n", f"Найдено: <b>{len(rows)}</b>\n"]
    for i, row in enumerate(rows, start=1):
        listing_id, item_title, price, category, seller_id, avg_rating, reviews_count, sales_count, reports_count, verified = row[:10]
        is_top = row[10] if len(row) > 10 else 0
        is_highlight = row[11] if len(row) > 11 else 0
        badge = seller_badge(sales_count, avg_rating, reports_count, verified)
        marker = promo_marker(is_top, is_highlight)
        prefix = "⭐ " if is_highlight else ""
        extra = f" | {marker}" if marker else ""
        lines.append(
            f"<b>{i}. {prefix}{html.escape(item_title or 'Без названия')}</b>\n"
            f"💰 <b>{price}₽</b> | {rating_label(avg_rating)} | {badge}{extra}\n"
            f"📂 {html.escape(category or '—')}\n"
        )
    lines.append("Нажмите номер объявления, чтобы открыть карточку.")
    return "\n".join(lines)


def filter_defaults() -> dict:
    return {
        "category": "all",
        "min_price": None,
        "max_price": None,
        "min_rating": 0.0,
        "verified_mode": "all",
    }


def budget_text(min_price, max_price) -> str:
    if min_price is None and max_price is None:
        return "любой"
    if min_price is None:
        return f"до {max_price}₽"
    if max_price is None:
        return f"от {min_price}₽"
    return f"{min_price} — {max_price}₽"


def filter_summary(data: dict) -> str:
    category = data.get("category", "all")
    category_text = "любая" if category == "all" else category
    rating = float(data.get("min_rating") or 0)
    verified_mode = data.get("verified_mode", "all")
    verified_text = {
        "all": "все продавцы",
        "trusted": "проверенные",
        "verified": "LTeam Verified",
    }.get(verified_mode, "все продавцы")
    rating_text_value = "любой" if rating <= 0 else f"от {rating:g}⭐"
    return f"""
📂 Категория: <b>{html.escape(str(category_text))}</b>
💰 Бюджет: <b>{budget_text(data.get('min_price'), data.get('max_price'))}</b>
⭐ Рейтинг: <b>{rating_text_value}</b>
🛡 Продавцы: <b>{verified_text}</b>
"""


def filter_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 Категория", callback_data="filter_category"), InlineKeyboardButton(text="💰 Бюджет", callback_data="filter_budget")],
        [InlineKeyboardButton(text="⭐ Рейтинг", callback_data="filter_rating"), InlineKeyboardButton(text="🛡 Продавцы", callback_data="filter_verified")],
        [InlineKeyboardButton(text="🔍 Показать объявления", callback_data="filter_show")],
        [InlineKeyboardButton(text="🔄 Сбросить", callback_data="market_filter_reset"), InlineKeyboardButton(text="⬅️ Маркет", callback_data="market")],
    ])


async def show_filter_screen(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data:
        data = filter_defaults()
        await state.update_data(**data)
    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
🎯 <b>Подбор услуг</b>
━━━━━━━━━━━━━━

Настройте параметры, как в фильтре маркетплейса:

{filter_summary(data)}
━━━━━━━━━━━━━━
После выбора нажмите <b>Показать объявления</b>.
""",
        reply_markup=filter_keyboard(),
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "market_filter")
async def market_filter(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not any(k in data for k in ["category", "min_price", "max_price", "min_rating", "verified_mode"]):
        await state.update_data(**filter_defaults())
    await show_filter_screen(call, state)
    await call.answer()


@dp.callback_query(F.data == "market_filter_reset")
async def market_filter_reset(call: CallbackQuery, state: FSMContext):
    await state.update_data(**filter_defaults())
    await show_filter_screen(call, state)
    await call.answer("Фильтр сброшен")


@dp.callback_query(F.data == "filter_category")
async def filter_category(call: CallbackQuery):
    buttons = [[InlineKeyboardButton(text="🌐 Любая категория", callback_data="filter_set_category:all")]]
    buttons += [[InlineKeyboardButton(text=cat, callback_data=f"filter_set_category:{cat}")] for cat in CATEGORIES]
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="market_filter")])
    await show_screen(call,
        """
━━━━━━━━━━━━━━
📂 <b>Категория</b>
━━━━━━━━━━━━━━

Выберите категорию для подбора:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("filter_set_category:"))
async def filter_set_category(call: CallbackQuery, state: FSMContext):
    category = call.data.split(":", 1)[1]
    await state.update_data(category=category)
    await show_filter_screen(call, state)
    await call.answer("Категория обновлена")


@dp.callback_query(F.data == "filter_budget")
async def filter_budget(call: CallbackQuery):
    await show_screen(call,
        """
━━━━━━━━━━━━━━
💰 <b>Бюджет</b>
━━━━━━━━━━━━━━

Выберите диапазон цены:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Любой бюджет", callback_data="filter_set_budget:none:none")],
            [InlineKeyboardButton(text="до 500₽", callback_data="filter_set_budget:none:500"), InlineKeyboardButton(text="500 — 1000₽", callback_data="filter_set_budget:500:1000")],
            [InlineKeyboardButton(text="1000 — 3000₽", callback_data="filter_set_budget:1000:3000"), InlineKeyboardButton(text="3000 — 5000₽", callback_data="filter_set_budget:3000:5000")],
            [InlineKeyboardButton(text="5000₽+", callback_data="filter_set_budget:5000:none")],
            [InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="filter_budget_manual")],
            [InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="market_filter")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("filter_set_budget:"))
async def filter_set_budget(call: CallbackQuery, state: FSMContext):
    _, min_raw, max_raw = call.data.split(":")
    min_price = None if min_raw == "none" else int(min_raw)
    max_price = None if max_raw == "none" else int(max_raw)
    await state.update_data(min_price=min_price, max_price=max_price)
    await show_filter_screen(call, state)
    await call.answer("Бюджет обновлён")


@dp.callback_query(F.data == "filter_budget_manual")
async def filter_budget_manual(call: CallbackQuery, state: FSMContext):
    await state.set_state(MarketFilterState.budget_manual)
    await show_screen(call,
        """
━━━━━━━━━━━━━━
✏️ <b>Свой бюджет</b>
━━━━━━━━━━━━━━

Введите диапазон двумя числами через пробел.

Пример:
<code>500 3000</code>

Если нужен бюджет от 5000₽ и выше:
<code>5000 0</code>
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к бюджету", callback_data="filter_budget")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(MarketFilterState.budget_manual)
async def filter_budget_manual_save(message: Message, state: FSMContext):
    parts = (message.text or "").replace("—", " ").replace("-", " ").split()
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        await screen_answer(message, "Введите два числа. Пример: <code>500 3000</code>", parse_mode="HTML")
        return
    min_price, max_price = map(int, parts)
    if min_price == 0:
        min_price = None
    if max_price == 0:
        max_price = None
    if min_price is not None and max_price is not None and min_price > max_price:
        min_price, max_price = max_price, min_price
    await state.update_data(min_price=min_price, max_price=max_price)
    await state.set_state(None)
    data = await state.get_data()
    await screen_answer(message,
        f"""
━━━━━━━━━━━━━━
🎯 <b>Подбор услуг</b>
━━━━━━━━━━━━━━

Бюджет обновлён.

{filter_summary(data)}
""",
        reply_markup=filter_keyboard(),
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "filter_rating")
async def filter_rating(call: CallbackQuery):
    await show_screen(call,
        """
━━━━━━━━━━━━━━
⭐ <b>Минимальный рейтинг</b>
━━━━━━━━━━━━━━

Выберите минимальный рейтинг продавца:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Любой рейтинг", callback_data="filter_set_rating:0")],
            [InlineKeyboardButton(text="от 3.0⭐", callback_data="filter_set_rating:3"), InlineKeyboardButton(text="от 4.0⭐", callback_data="filter_set_rating:4")],
            [InlineKeyboardButton(text="от 4.5⭐", callback_data="filter_set_rating:4.5"), InlineKeyboardButton(text="только 5⭐", callback_data="filter_set_rating:5")],
            [InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="market_filter")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("filter_set_rating:"))
async def filter_set_rating(call: CallbackQuery, state: FSMContext):
    rating = float(call.data.split(":", 1)[1])
    await state.update_data(min_rating=rating)
    await show_filter_screen(call, state)
    await call.answer("Рейтинг обновлён")


@dp.callback_query(F.data == "filter_verified")
async def filter_verified(call: CallbackQuery):
    await show_screen(call,
        """
━━━━━━━━━━━━━━
🛡 <b>Продавцы</b>
━━━━━━━━━━━━━━

Выберите уровень доверия:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Все продавцы", callback_data="filter_set_verified:all")],
            [InlineKeyboardButton(text="✅ Проверенные", callback_data="filter_set_verified:trusted")],
            [InlineKeyboardButton(text="🛡 Только LTeam Verified", callback_data="filter_set_verified:verified")],
            [InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="market_filter")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("filter_set_verified:"))
async def filter_set_verified(call: CallbackQuery, state: FSMContext):
    mode = call.data.split(":", 1)[1]
    await state.update_data(verified_mode=mode)
    await show_filter_screen(call, state)
    await call.answer("Фильтр продавцов обновлён")


@dp.callback_query(F.data == "filter_show")
async def filter_show(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data:
        data = filter_defaults()
    rows = get_market_results(
        category=data.get("category", "all"),
        min_price=data.get("min_price"),
        max_price=data.get("max_price"),
        min_rating=float(data.get("min_rating") or 0),
        verified_mode=data.get("verified_mode", "all"),
        order="rating" if float(data.get("min_rating") or 0) > 0 else "new",
        limit=10,
    )
    await show_screen(call,
        market_results_text("🔍 <b>Результаты подбора</b>", rows),
        reply_markup=market_results_keyboard(rows, back_callback="market_filter"),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "market_categories")
async def market_categories(call: CallbackQuery):
    keyboard = [[InlineKeyboardButton(text=cat, callback_data=f"market_choose_sort:{cat}")] for cat in CATEGORIES]
    keyboard.append([InlineKeyboardButton(text="⬅️ Маркет", callback_data="market")])
    await show_screen(call,
        """
━━━━━━━━━━━━━━
📂 <b>Категории</b>
━━━━━━━━━━━━━━

Выберите категорию:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "market_new")
async def market_new(call: CallbackQuery):
    rows = get_market_results(order="new", limit=10)
    await show_screen(call,
        market_results_text("🆕 <b>Новые объявления</b>", rows),
        reply_markup=market_results_keyboard(rows, back_callback="market"),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "market_top")
async def market_top(call: CallbackQuery):
    rows = get_market_results(order="rating", limit=10)
    rows = [r for r in rows if len(r) > 10 and r[10]]
    await show_screen(call,
        market_results_text("🔥 <b>ТОП объявления</b>", rows),
        reply_markup=market_results_keyboard(rows, back_callback="market"),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "market_verified")
async def market_verified(call: CallbackQuery):
    rows = get_market_results(verified_mode="verified", order="rating", limit=10)
    await show_screen(call,
        market_results_text("🛡 <b>LTeam Verified</b>", rows),
        reply_markup=market_results_keyboard(rows, back_callback="market"),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("market_choose_sort:"))
async def market_choose_sort(call: CallbackQuery):
    category = call.data.split(":", 1)[1]

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
📂 <b>{html.escape(category)}</b>
━━━━━━━━━━━━━━

Выберите сортировку:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🆕 Новые", callback_data=f"market_cat_sort:{category}:new")],
            [InlineKeyboardButton(text="💰 Дешевле", callback_data=f"market_cat_sort:{category}:cheap")],
            [InlineKeyboardButton(text="💎 Дороже", callback_data=f"market_cat_sort:{category}:expensive")],
            [InlineKeyboardButton(text="⭐ Лучший рейтинг", callback_data=f"market_cat_sort:{category}:rating")],
            [InlineKeyboardButton(text="🎯 Подбор с этой категорией", callback_data=f"filter_set_category:{category}")],
            [InlineKeyboardButton(text="⬅️ Категории", callback_data="market_categories")],
        ]),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("market_cat_sort:"))
async def market_cat_sort(call: CallbackQuery):
    _, category, sort = call.data.split(":", 2)
    rows = get_market_results(category=category, order=sort, limit=10)
    await show_screen(call,
        market_results_text(f"📂 <b>{html.escape(category)}</b>", rows),
        reply_markup=market_results_keyboard(rows, back_callback=f"market_choose_sort:{category}"),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "search_start")
async def search_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(SearchState.query)
    await show_screen(call,
        """
━━━━━━━━━━━━━━
🔍 <b>Поиск по маркету</b>
━━━━━━━━━━━━━━

Введите название услуги, категорию или задачу.

Пример:
<code>бот для заявок</code>
<code>логотип</code>
<code>монтаж shorts</code>
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Подбор по фильтрам", callback_data="market_filter")],
            [InlineKeyboardButton(text="⬅️ Маркет", callback_data="market")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(SearchState.query)
async def search_result(message: Message, state: FSMContext):
    query = (message.text or "").strip()
    await state.clear()

    with db() as conn:
        rows = conn.execute(
            """
            SELECT l.id, l.title, l.price, l.category, l.seller_id,
                   COALESCE(rs.avg_rating,0) AS avg_rating,
                   COALESCE(rs.reviews_count,0) AS reviews_count,
                   COALESCE(ds.sales_count,0) AS sales_count,
                   COALESCE(rep.reports_count,0) AS reports_count
            FROM listings l
            LEFT JOIN (
                SELECT seller_id, AVG(rating) AS avg_rating, COUNT(*) AS reviews_count
                FROM reviews
                GROUP BY seller_id
            ) rs ON rs.seller_id = l.seller_id
            LEFT JOIN (
                SELECT seller_id, COUNT(*) AS sales_count
                FROM deals
                WHERE status='completed'
                GROUP BY seller_id
            ) ds ON ds.seller_id = l.seller_id
            LEFT JOIN (
                SELECT l2.seller_id, COUNT(r.id) AS reports_count
                FROM listings l2
                LEFT JOIN reports r ON r.listing_id = l2.id
                GROUP BY l2.seller_id
            ) rep ON rep.seller_id = l.seller_id
            WHERE l.status='active'
            AND (
                LOWER(l.title) LIKE LOWER(?)
                OR LOWER(l.description) LIKE LOWER(?)
                OR LOWER(l.category) LIKE LOWER(?)
                OR LOWER(l.item_type) LIKE LOWER(?)
            )
            ORDER BY l.id DESC
            LIMIT 10
            """,
            (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%"),
        ).fetchall()

    await screen_answer(message,
        market_results_text(f"🔍 <b>Поиск:</b> {html.escape(query)}", rows),
        reply_markup=market_results_keyboard(rows, back_callback="market"),
        parse_mode="HTML",
    )
# ===== ПРОСМОТР ОБЪЯВЛЕНИЯ =====

@dp.callback_query(F.data.startswith("view_listing:"))
async def view_listing(call: CallbackQuery):
    listing_id = int(call.data.split(":")[1])

    with db() as conn:
        row = conn.execute(
            """
            SELECT id, seller_id, title, category, item_type, condition, price, description, COALESCE(delivery_time, ''),
                   COALESCE(is_top,0), COALESCE(is_highlight,0), top_until, highlight_until
            FROM listings
            WHERE id=? AND status='active'
            """,
            (listing_id,),
        ).fetchone()

        if not row:
            await call.answer("Объявление не найдено", show_alert=True)
            return

        listing_id, seller_id, title, category, item_type, condition, price, description, delivery_time, is_top, is_highlight, top_until, highlight_until = row
        fav = conn.execute(
            "SELECT 1 FROM favorites WHERE user_id=? AND listing_id=?",
            (call.from_user.id, listing_id),
        ).fetchone()

    fav_text = "⭐ Убрать из избранного" if fav else "⭐ В избранное"
    safe_title = html.escape(title or "Без названия")
    safe_category = html.escape(category or "—")
    safe_item_type = html.escape(item_type or "—")
    safe_condition = html.escape(condition or "—")
    safe_description = html.escape(description or "Без описания")
    safe_delivery_time = html.escape(delivery_time or "Не указан")

    buttons = [
        [InlineKeyboardButton(text="🛒 Купить через гаранта", callback_data=f"buy:{listing_id}")],
        [InlineKeyboardButton(text="👤 Профиль продавца", callback_data=f"seller_profile:{seller_id}")],
        [InlineKeyboardButton(text="💬 Написать через бота", callback_data=f"ask_seller:{listing_id}")],
        [InlineKeyboardButton(text=fav_text, callback_data=f"fav:{listing_id}")],
        [InlineKeyboardButton(text="🚨 Пожаловаться", callback_data=f"report:{listing_id}")],
    ]
    if call.from_user.id == seller_id:
        buttons.append([InlineKeyboardButton(text="🚀 Продвинуть объявление", callback_data=f"promo_menu:{listing_id}")])
        buttons.append([
            InlineKeyboardButton(text="✏️ Редактировать позже", callback_data=f"edit_listing_soon:{listing_id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"owner_delete_listing:{listing_id}"),
        ])
    if is_admin(call.from_user.id):
        buttons.extend([
            [InlineKeyboardButton(text="⚙️ Админ: открыть продавца", callback_data=f"admin_user:{seller_id}")],
            [
                InlineKeyboardButton(text="🗑 Удалить объявление", callback_data=f"admin_delete:{listing_id}"),
                InlineKeyboardButton(text="🚫 Забанить продавца", callback_data=f"admin_ban_user:{seller_id}"),
            ],
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в маркет", callback_data="market")])

    await show_screen(call,
        f"""
{seller_card_text(seller_id)}
━━━━━━━━━━━━━━
📦 <b>Объявление</b>
━━━━━━━━━━━━━━

<b>{safe_title}</b>

💰 Цена: <b>{price}₽</b>
📂 Категория: <b>{safe_category}</b>
📌 Формат: <b>{safe_item_type}</b>
⏳ Срок/получение: <b>{safe_delivery_time}</b>
🧾 Состояние: <b>{safe_condition}</b>

<b>Описание:</b>
{safe_description}

🚀 Продвижение: <b>{promo_marker(is_top, is_highlight) or 'обычное'}</b>

🛡 Сделка проходит через гаранта LTeam. Не переводите деньги напрямую продавцу.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("ask_seller:"))
async def ask_seller(call: CallbackQuery):
    await call.answer("Чат с продавцом откроется после создания сделки и подтверждения оплаты.", show_alert=True)


@dp.callback_query(F.data.startswith("seller_profile:"))
async def seller_profile(call: CallbackQuery):
    seller_id = int(call.data.split(":")[1])
    stats = seller_stats(seller_id)
    with db() as conn:
        reviews = conn.execute(
            "SELECT rating, text FROM reviews WHERE seller_id=? ORDER BY id DESC LIMIT 3",
            (seller_id,),
        ).fetchall()
    reviews_text = "Пока нет отзывов."
    if reviews:
        reviews_text = "\\n".join([f"• {rating}⭐ — {html.escape(text or 'Без текста')}" for rating, text in reviews])

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
👤 <b>Профиль продавца</b>
━━━━━━━━━━━━━━

🆔 ID: <code>{seller_id}</code>
🔗 Username: @{html.escape(stats['username'])}
📅 На платформе: <b>{html.escape(str(stats['created_at'])[:10])}</b>
⭐ Рейтинг: <b>{stats['rating_text']}</b>
💰 Завершённых продаж: <b>{stats['sales_count']}</b>
📦 Активных объявлений: <b>{stats['active_listings']}</b>
🏷 Статус: <b>{stats['status']}</b>

💬 <b>Последние отзывы:</b>
{reviews_text}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад в маркет", callback_data="market")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


# ===== ИЗБРАННОЕ =====

@dp.callback_query(F.data.startswith("fav:"))
async def favorite_toggle(call: CallbackQuery):
    listing_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        exists = conn.execute(
            "SELECT 1 FROM favorites WHERE user_id=? AND listing_id=?",
            (user_id, listing_id),
        ).fetchone()

        if exists:
            conn.execute(
                "DELETE FROM favorites WHERE user_id=? AND listing_id=?",
                (user_id, listing_id),
            )
            text = "Удалено из избранного"
        else:
            conn.execute(
                "INSERT OR IGNORE INTO favorites (user_id, listing_id) VALUES (?, ?)",
                (user_id, listing_id),
            )
            text = "Добавлено в избранное"

        conn.commit()

    await call.answer(text, show_alert=True)


@dp.callback_query(F.data == "favorites")
async def favorites(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        rows = conn.execute(
            """
            SELECT l.id, l.title, l.price
            FROM favorites f
            JOIN listings l ON l.id = f.listing_id
            WHERE f.user_id=? AND l.status='active'
            ORDER BY l.id DESC
            """,
            (user_id,),
        ).fetchall()

    if not rows:
        await show_screen(call, "⭐ Избранное пустое.", reply_markup=back_home())
    else:
        await show_screen(call, 
            "⭐ <b>Избранное</b>",
            reply_markup=listings_keyboard(rows),
            parse_mode="HTML",
        )

    await call.answer()


# ===== ЖАЛОБЫ =====

@dp.callback_query(F.data.startswith("report:"))
async def report_start(call: CallbackQuery, state: FSMContext):
    listing_id = int(call.data.split(":")[1])
    await state.update_data(listing_id=listing_id, target_type="listing", target_id=listing_id)
    await state.set_state(ReportState.reason)

    await show_screen(call, "🚨 Опишите причину жалобы:")
    await call.answer()


@dp.message(ReportState.reason)
async def report_save(message: Message, state: FSMContext):
    data = await state.get_data()
    target_type = data.get("target_type", "listing")
    target_id = int(data.get("target_id") or data.get("listing_id") or 0)
    listing_id = target_id if target_type == "listing" else None
    reason_text = (message.text or "").strip()

    with db() as conn:
        conn.execute(
            """
            INSERT INTO reports (user_id, listing_id, target_type, target_id, reason, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (message.from_user.id, listing_id, target_type, target_id, reason_text, "new", datetime.now().isoformat()),
        )
        conn.commit()

    await state.clear()
    await screen_answer(message, "✅ Жалоба отправлена администрации.", reply_markup=main_menu(message.from_user.id), parse_mode="HTML")
    await notify_admins(f"""
🚨 <b>Новая жалоба</b>

Тип: <b>{html.escape(target_type)}</b>
ID цели: <code>{target_id}</code>
Пользователь: <code>{message.from_user.id}</code>

Причина:
{html.escape(reason_text)}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚨 Открыть жалобы", callback_data="admin_reports")],
        [InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user:{message.from_user.id}")],
    ]))


# ===== СОЗДАНИЕ ОБЪЯВЛЕНИЯ =====

# ===== СОЗДАНИЕ ОБЪЯВЛЕНИЯ (УЛУЧШЕННОЕ) =====

# ===== СОЗДАНИЕ ОБЪЯВЛЕНИЯ — КРАСИВЫЙ UX =====

@dp.callback_query(F.data == "listing_cancel")
async def listing_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_screen(call, "❌ Размещение отменено.", reply_markup=main_menu(call.from_user.id), parse_mode="HTML")
    await call.answer()


def cancel_keyboard(extra_rows=None):
    rows = extra_rows or []
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="listing_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def category_examples_text(category: str) -> str:
    examples = CATEGORY_EXAMPLES.get(category, ["Создам полезную цифровую услугу"])
    return "\n".join([f"<code>{html.escape(x)}</code>" for x in examples])


def item_types_for_category(category: str) -> list[str]:
    return CATEGORY_ITEM_TYPES.get(category, ITEM_TYPES)


def category_type_hint(category: str) -> str:
    return CATEGORY_TYPE_HINTS.get(category, "Выберите формат, который лучше всего описывает объявление.")


@dp.callback_query(F.data == "create_listing")
async def create_listing(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(CreateListing.category)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=cat, callback_data=f"cat_create:{cat}")]
            for cat in CATEGORIES
        ] + [[InlineKeyboardButton(text="❌ Отмена", callback_data="listing_cancel")]]
    )

    await show_screen(call,
        """
━━━━━━━━━━━━━━
➕ <b>Размещение объявления</b>
━━━━━━━━━━━━━━

Создайте услугу или цифровой товар для LTeam Market.

<b>Шаг 1 из 7</b>
Выберите категорию:
""",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("cat_create:"))
async def listing_category(call: CallbackQuery, state: FSMContext):
    category = call.data.split(":", 1)[1]
    await state.update_data(category=category)
    await state.set_state(CreateListing.title)

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
📝 <b>Название объявления</b>
━━━━━━━━━━━━━━

📂 Категория: <b>{html.escape(category)}</b>

<b>Шаг 2 из 7</b>
Напишите короткое и понятное название.

💡 <b>Примеры:</b>
{category_examples_text(category)}
""",
        reply_markup=cancel_keyboard(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.message(CreateListing.title)
async def listing_title(message: Message, state: FSMContext):
    title = (message.text or "").strip()

    if len(title) < 3:
        await screen_answer(message, "Название слишком короткое. Напишите понятнее.", reply_markup=cancel_keyboard())
        return
    ok, reason = moderation_check(title)
    if not ok:
        await screen_answer(message, f"🚫 Название не прошло авто-модерацию: {html.escape(reason)}", reply_markup=cancel_keyboard(), parse_mode="HTML")
        await notify_admins(f"⚠️ <b>Авто-модерация объявления</b>\n\nПользователь: <code>{message.from_user.id}</code>\nПричина: {html.escape(reason)}\nТекст: {html.escape(title)}")
        return

    data = await state.get_data()
    category = data.get("category", "🛠 Другое")

    await state.update_data(title=title)
    await state.set_state(CreateListing.item_type)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t, callback_data=f"type_create:{t}")]
            for t in item_types_for_category(category)
        ] + [[InlineKeyboardButton(text="❌ Отмена", callback_data="listing_cancel")]]
    )

    await screen_answer(message,
        f"""
━━━━━━━━━━━━━━
📦 <b>Формат объявления</b>
━━━━━━━━━━━━━━

📂 Категория: <b>{html.escape(category)}</b>
📌 Название: <b>{html.escape(title)}</b>

<b>Шаг 3 из 7</b>
{html.escape(category_type_hint(category))}
""",
        reply_markup=keyboard,
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("type_create:"))
async def listing_type(call: CallbackQuery, state: FSMContext):
    item_type = call.data.split(":", 1)[1]
    await state.update_data(item_type=item_type)
    await state.set_state(CreateListing.delivery_time)

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
⏳ <b>Срок / доступность</b>
━━━━━━━━━━━━━━

📌 Формат: <b>{html.escape(item_type)}</b>

<b>Шаг 4 из 7</b>
Выберите срок выполнения или получения товара.
""",
        reply_markup=delivery_time_keyboard(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("listing_delivery:"))
async def listing_delivery_pick(call: CallbackQuery, state: FSMContext):
    delivery_time = call.data.split(":", 1)[1]
    await state.update_data(delivery_time=delivery_time)
    await state.set_state(CreateListing.price)

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
💰 <b>Цена</b>
━━━━━━━━━━━━━━

⏳ Срок: <b>{html.escape(delivery_time)}</b>

<b>Шаг 5 из 7</b>
Введите цену в рублях.

Лимит: от <b>{MIN_ORDER_BUDGET}₽</b> до <b>{MAX_LISTING_PRICE}₽</b>.
Пример: <code>1500</code>
""",
        reply_markup=cancel_keyboard(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.message(CreateListing.delivery_time)
async def listing_delivery_time(message: Message, state: FSMContext):
    await screen_answer(
        message,
        "⏳ Срок нужно выбрать кнопкой ниже, а не писать текстом.",
        reply_markup=delivery_time_keyboard(),
        parse_mode="HTML"
    )

@dp.message(CreateListing.price)
async def listing_price(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await screen_answer(message, "Введите цену только числом. Например: 1500", reply_markup=cancel_keyboard())
        return

    price = parse_money(message.text.strip())
    ok, reason = validate_listing_price(price)
    if not ok:
        await screen_answer(message, f"🚫 Цена не подходит: {html.escape(reason)}", reply_markup=cancel_keyboard(), parse_mode="HTML")
        return

    await state.update_data(price=price)
    await state.set_state(CreateListing.payout_details)

    await screen_answer(message,
        f"""
━━━━━━━━━━━━━━
💳 <b>Реквизиты продавца</b>
━━━━━━━━━━━━━━

💰 Цена: <b>{price}₽</b>

<b>Шаг 6 из 7</b>
Укажите, куда LTeam должен будет перевести выплату после завершения сделки.

Примеры:
<code>СБП: +79000000000, Сбер, Иван И.</code>
<code>TON/USDT: адрес кошелька</code>

⚠️ Эти реквизиты видят только админы LTeam для выплаты. Покупатель платит только LTeam.
""",
        reply_markup=cancel_keyboard(),
        parse_mode="HTML"
    )


@dp.message(CreateListing.payout_details)
async def listing_payout_details(message: Message, state: FSMContext):
    payout_details = (message.text or "").strip()
    if len(payout_details) < 5:
        await screen_answer(message, "Укажите реквизиты подробнее. Например: СБП + банк + имя или кошелёк.", reply_markup=cancel_keyboard())
        return

    await state.update_data(payout_details=payout_details)
    await state.set_state(CreateListing.description)

    await screen_answer(message,
        """
━━━━━━━━━━━━━━
🧾 <b>Описание</b>
━━━━━━━━━━━━━━

<b>Шаг 7 из 7</b>
Опишите объявление по шаблону:

1. Что вы сделаете?
2. Что получит покупатель?
3. Срок выполнения?
4. Что нужно от покупателя?

Описание можно пропустить.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить описание", callback_data="skip_desc")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="listing_cancel")],
        ]),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "skip_desc")
async def skip_description(call: CallbackQuery, state: FSMContext):
    await state.update_data(description="Без описания")
    await listing_preview(call, state)
    await call.answer()


@dp.message(CreateListing.description)
async def listing_description(message: Message, state: FSMContext):
    description = (message.text or "").strip() or "Без описания"
    if description != "Без описания":
        ok, reason = moderation_check(description)
        if not ok:
            await screen_answer(message, f"🚫 Описание не прошло авто-модерацию: {html.escape(reason)}", reply_markup=cancel_keyboard(), parse_mode="HTML")
            await notify_admins(f"⚠️ <b>Авто-модерация описания объявления</b>\n\nПользователь: <code>{message.from_user.id}</code>\nПричина: {html.escape(reason)}\nТекст: {html.escape(description)}")
            return
    await state.update_data(description=description)
    try:
        await message.delete()
    except Exception:
        pass
    # Покажем предпросмотр в старом экране.
    row = get_screen(message.from_user.id)
    fake_text, keyboard = build_listing_preview(await state.get_data())
    if row:
        chat_id, message_id = row
        try:
            await bot.edit_message_text(fake_text, chat_id=chat_id, message_id=message_id, reply_markup=keyboard, parse_mode="HTML")
            return
        except Exception:
            pass
    sent = await message.answer(fake_text, reply_markup=keyboard, parse_mode="HTML")
    save_screen(message.from_user.id, sent.chat.id, sent.message_id)


def build_listing_preview(data: dict):
    text = f"""
━━━━━━━━━━━━━━
👀 <b>Предпросмотр объявления</b>
━━━━━━━━━━━━━━

📦 <b>{html.escape(data.get('title', ''))}</b>

📂 Категория: <b>{html.escape(data.get('category', ''))}</b>
📌 Формат: <b>{html.escape(data.get('item_type', ''))}</b>
⏳ Срок/получение: <b>{html.escape(data.get('delivery_time', ''))}</b>
💰 Цена: <b>{data.get('price', 0)}₽</b>

🧾 <b>Описание:</b>
{html.escape(data.get('description', 'Без описания'))}

💳 <b>Реквизиты для выплаты:</b>
{html.escape(data.get('payout_details', ''))}

Отправляем объявление на модерацию?
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить на модерацию", callback_data="listing_publish")],
        [InlineKeyboardButton(text="✏️ Создать заново", callback_data="create_listing")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="listing_cancel")],
    ])
    return text, keyboard


async def listing_preview(call: CallbackQuery, state: FSMContext):
    text, keyboard = build_listing_preview(await state.get_data())
    await show_screen(call, text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query(F.data == "listing_publish")
async def listing_publish(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    required = ["title", "category", "item_type", "delivery_time", "price", "payout_details"]
    if any(k not in data for k in required):
        await call.answer("Данные объявления не найдены. Создайте заново.", show_alert=True)
        return

    description = data.get("description", "Без описания")
    seller_id = call.from_user.id
    seller_contact = user_contact(seller_id)

    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO listings (seller_id, title, category, item_type, condition, price, description, seller_requisites, delivery_time, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            seller_id,
            data["title"],
            data["category"],
            data["item_type"],
            "—",
            data["price"],
            description,
            data["payout_details"],
            data.get("delivery_time", ""),
            "moderation",
            datetime.now().isoformat()
        ))
        listing_id = cur.lastrowid
        conn.commit()

    await state.clear()

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
⏳ <b>Объявление отправлено на модерацию</b>
━━━━━━━━━━━━━━

📦 <b>{html.escape(data['title'])}</b>
📂 Категория: <b>{html.escape(data['category'])}</b>
💰 Цена: <b>{data['price']}₽</b>

Оно появится в маркете только после проверки администратором.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Мои объявления", callback_data="my_listings")],
            [InlineKeyboardButton(text="➕ Создать ещё", callback_data="create_listing")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML"
    )

    for admin in ADMIN_IDS:
        await bot.send_message(
            admin,
            f"""
🆕 <b>Новое объявление на модерации</b>

ID: <code>{listing_id}</code>
Продавец: {seller_contact}

📦 {html.escape(data['title'])}
📂 {html.escape(data['category'])}
📌 {html.escape(data['item_type'])}
💰 {data['price']}₽

💳 <b>Реквизиты продавца для выплаты:</b>
{html.escape(data['payout_details'])}
""",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Профиль продавца", callback_data=f"admin_user:{seller_id}")],
                [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_approve_listing:{listing_id}")],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_listing:{listing_id}")],
                [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_delete:{listing_id}")],
            ]),
            parse_mode="HTML"
        )
    await call.answer()


# ===== ПРОДВИЖЕНИЕ ОБЪЯВЛЕНИЙ =====

def promo_keyboard(listing_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🚀 Поднять — {PROMO_OPTIONS['bump']['price']}₽", callback_data=f"promo_select:{listing_id}:bump")],
        [InlineKeyboardButton(text=f"🔥 В ТОП на 7 дней — {PROMO_OPTIONS['top']['price']}₽", callback_data=f"promo_select:{listing_id}:top")],
        [InlineKeyboardButton(text=f"⭐ Выделить на 7 дней — {PROMO_OPTIONS['highlight']['price']}₽", callback_data=f"promo_select:{listing_id}:highlight")],
        [InlineKeyboardButton(text="⬅️ К объявлению", callback_data=f"view_listing:{listing_id}")],
    ])


def promo_status_text(listing_id: int) -> str:
    with db() as conn:
        row = conn.execute("SELECT COALESCE(is_top,0), COALESCE(is_highlight,0), bumped_at, top_until, highlight_until FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not row:
        return ""
    is_top, is_highlight, bumped_at, top_until, highlight_until = row
    lines = []
    if bumped_at:
        lines.append(f"🚀 Последнее поднятие: <b>{html.escape(str(bumped_at)[:16])}</b>")
    if is_top:
        lines.append(f"🔥 ТОП активно: <b>{html.escape(str(top_until or 'да')[:16])}</b>")
    if is_highlight:
        lines.append(f"⭐ Выделение активно: <b>{html.escape(str(highlight_until or 'да')[:16])}</b>")
    return "\n".join(lines) if lines else "Пока без продвижения."


@dp.callback_query(F.data.startswith("promo_menu:"))
async def promo_menu(call: CallbackQuery):
    listing_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT seller_id, title FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone()
    if not row:
        await call.answer("Объявление не найдено", show_alert=True)
        return
    seller_id, title = row
    if call.from_user.id != seller_id and not is_admin(call.from_user.id):
        await call.answer("Продвигать может только владелец объявления", show_alert=True)
        return
    await show_screen(call, f"""
━━━━━━━━━━━━━━
🚀 <b>Продвижение объявления</b>
━━━━━━━━━━━━━━

📦 <b>{html.escape(title or 'Без названия')}</b>

Текущий статус:
{promo_status_text(listing_id)}

Выберите вариант продвижения:

🚀 <b>Поднять</b> — объявление станет выше в списках.
🔥 <b>В ТОП</b> — отдельный блок ТОП + приоритет в выдаче.
⭐ <b>Выделить</b> — заметная отметка в списке и карточке.
""", reply_markup=promo_keyboard(listing_id), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("promo_select:"))
async def promo_select(call: CallbackQuery):
    _, listing_raw, promo_type = call.data.split(":")
    listing_id = int(listing_raw)
    option = PROMO_OPTIONS.get(promo_type)
    if not option:
        await call.answer("Неизвестный тип продвижения", show_alert=True)
        return
    with db() as conn:
        row = conn.execute("SELECT seller_id, title FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone()
    if not row:
        await call.answer("Объявление не найдено", show_alert=True)
        return
    seller_id, title = row
    if call.from_user.id != seller_id and not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await show_screen(call, f"""
━━━━━━━━━━━━━━
💰 <b>Оплата продвижения</b>
━━━━━━━━━━━━━━

📦 Объявление: <b>{html.escape(title or 'Без названия')}</b>
🎯 Услуга: <b>{option['title']}</b>
💵 Сумма: <b>{option['price']}₽</b>

{html.escape(option['description'])}

Выберите способ оплаты. Реквизиты LTeam покажутся только после выбора способа.
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 СБП", callback_data=f"promo_pay:{listing_id}:{promo_type}:sbp")],
        [InlineKeyboardButton(text="🪙 Крипта", callback_data=f"promo_pay:{listing_id}:{promo_type}:crypto")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"promo_menu:{listing_id}")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("promo_pay:"))
async def promo_pay(call: CallbackQuery, state: FSMContext):
    _, listing_raw, promo_type, method = call.data.split(":")
    listing_id = int(listing_raw)
    option = PROMO_OPTIONS.get(promo_type)
    if not option:
        await call.answer("Неизвестный тип продвижения", show_alert=True)
        return
    with db() as conn:
        row = conn.execute("SELECT seller_id, title FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone()
        if not row:
            await call.answer("Объявление не найдено", show_alert=True)
            return
        seller_id, title = row
        if call.from_user.id != seller_id and not is_admin(call.from_user.id):
            await call.answer("Нет доступа", show_alert=True)
            return
        cur = conn.cursor()
        cur.execute("INSERT INTO promo_payments (listing_id, user_id, promo_type, amount, payment_method, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (listing_id, call.from_user.id, promo_type, option['price'], method, "waiting_receipt", datetime.now().isoformat()))
        promo_id = cur.lastrowid
        conn.commit()
    await state.update_data(promo_id=promo_id)
    await state.set_state(PromoState.receipt)
    if method == "sbp":
        pay_block = f"🏦 Банк: <b>{html.escape(SBP_BANK)}</b>\n👤 Получатель: <b>{html.escape(SBP_NAME)}</b>\n📱 Телефон: <code>{html.escape(SBP_PHONE)}</code>"
    else:
        pay_block = f"🪙 Кошелёк LTeam:\n<code>{html.escape(CRYPTO_WALLET)}</code>"
    await show_screen(call, f"""
━━━━━━━━━━━━━━
💳 <b>Оплатите продвижение</b>
━━━━━━━━━━━━━━

Заявка: <b>#{promo_id}</b>
📦 Объявление: <b>{html.escape(title or 'Без названия')}</b>
🎯 Услуга: <b>{option['title']}</b>
💵 Сумма: <b>{option['price']}₽</b>

{pay_block}

⚠️ После оплаты отправьте чек, скрин или хэш транзакции одним сообщением.
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К объявлению", callback_data=f"view_listing:{listing_id}")]]), parse_mode="HTML")
    await call.answer()


@dp.message(PromoState.receipt)
async def promo_receipt_save(message: Message, state: FSMContext):
    data = await state.get_data()
    promo_id = data.get("promo_id")
    if not promo_id:
        await state.clear()
        await screen_answer(message, "❌ Заявка на продвижение не найдена.")
        return
    if message.photo:
        receipt_text = "📷 Пользователь отправил фото чека"
    elif message.document:
        receipt_text = "📎 Пользователь отправил документ"
    else:
        receipt_text = message.text or "Пользователь отправил подтверждение оплаты"
    with db() as conn:
        row = conn.execute("""
            SELECT p.listing_id, p.user_id, p.promo_type, p.amount, p.payment_method, l.title
            FROM promo_payments p
            JOIN listings l ON l.id = p.listing_id
            WHERE p.id=?
            """, (promo_id,)).fetchone()
        if not row:
            await state.clear()
            await screen_answer(message, "❌ Заявка на продвижение не найдена.")
            return
        listing_id, user_id, promo_type, amount, method, title = row
        conn.execute("UPDATE promo_payments SET receipt=?, status=? WHERE id=?", (receipt_text, "waiting_admin_confirm", promo_id))
        conn.commit()
    await state.clear()
    option = PROMO_OPTIONS[promo_type]
    await screen_answer(message, f"""
✅ <b>Чек отправлен</b>

Заявка на продвижение: <b>#{promo_id}</b>
Услуга: <b>{option['title']}</b>

Админ проверит оплату и активирует продвижение.
""", parse_mode="HTML")
    admin_text = f"""
━━━━━━━━━━━━━━
💰 <b>Продвижение на проверку</b>
━━━━━━━━━━━━━━

Заявка: <b>#{promo_id}</b>
📦 Объявление: <b>#{listing_id}</b> — {html.escape(title or 'Без названия')}
👤 Пользователь: <code>{user_id}</code>
🎯 Услуга: <b>{option['title']}</b>
💵 Сумма: <b>{amount}₽</b>
💳 Метод: <b>{html.escape(method or '—')}</b>

🧾 Чек / данные оплаты:
{html.escape(receipt_text)}
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_promo_ok:{promo_id}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_promo_no:{promo_id}")],
        [InlineKeyboardButton(text="📦 Открыть объявление", callback_data=f"view_listing:{listing_id}")],
    ])
    for admin in ADMIN_IDS:
        await bot.send_message(admin, admin_text, reply_markup=keyboard, parse_mode="HTML")


def apply_promo_to_listing(conn, listing_id: int, promo_type: str):
    now = datetime.now().isoformat()
    if promo_type == "bump":
        conn.execute("UPDATE listings SET bumped_at=? WHERE id=?", (now, listing_id))
    elif promo_type == "top":
        conn.execute("UPDATE listings SET is_top=1, top_until=? WHERE id=?", (now, listing_id))
    elif promo_type == "highlight":
        conn.execute("UPDATE listings SET is_highlight=1, highlight_until=? WHERE id=?", (now, listing_id))


@dp.callback_query(F.data.startswith("admin_promo_ok:"))
async def admin_promo_ok(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    promo_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT listing_id, user_id, promo_type, amount FROM promo_payments WHERE id=?", (promo_id,)).fetchone()
        if not row:
            await call.answer("Заявка не найдена", show_alert=True)
            return
        listing_id, user_id, promo_type, amount = row
        apply_promo_to_listing(conn, listing_id, promo_type)
        conn.execute("UPDATE promo_payments SET status=? WHERE id=?", ("confirmed", promo_id))
        conn.commit()
    option = PROMO_OPTIONS.get(promo_type, {"title": promo_type})
    await bot.send_message(user_id, f"✅ Продвижение <b>{option['title']}</b> по объявлению #{listing_id} активировано.", parse_mode="HTML")
    await call.message.edit_text(f"✅ Продвижение #{promo_id} подтверждено и активировано.")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_promo_no:"))
async def admin_promo_no(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    promo_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT user_id, listing_id FROM promo_payments WHERE id=?", (promo_id,)).fetchone()
        if not row:
            await call.answer("Заявка не найдена", show_alert=True)
            return
        user_id, listing_id = row
        conn.execute("UPDATE promo_payments SET status=? WHERE id=?", ("rejected", promo_id))
        conn.commit()
    await bot.send_message(user_id, f"❌ Оплата продвижения по объявлению #{listing_id} не подтверждена. Обратитесь в поддержку.")
    await call.message.edit_text(f"❌ Продвижение #{promo_id} отклонено.")
    await call.answer()

# ===== ПОКУПКА И СДЕЛКИ =====

# ===== ПОКУПКА =====

@dp.callback_query(F.data.startswith("buy:"))
async def buy(call: CallbackQuery):
    listing_id = int(call.data.split(":")[1])
    buyer_id = call.from_user.id

    with db() as conn:
        row = conn.execute(
            "SELECT seller_id, price, title FROM listings WHERE id=? AND status='active'",
            (listing_id,),
        ).fetchone()

        if not row:
            await call.answer("Объявление не найдено", show_alert=True)
            return

        seller_id, price, title = row

        if seller_id == buyer_id:
            await call.answer("Нельзя купить своё объявление", show_alert=True)
            return

        commission = int(price * COMMISSION_PERCENT / 100)
        payout = price - commission

        cur = conn.cursor()
        cur.execute(
            "INSERT INTO deals (listing_id, buyer_id, seller_id, amount, commission, payout, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                listing_id,
                buyer_id,
                seller_id,
                price,
                commission,
                payout,
                "waiting_payment",
                datetime.now().isoformat(),
            ),
        )

        deal_id = cur.lastrowid
        conn.commit()

    await show_screen(call, 
        f"""
🛒 <b>Сделка #{deal_id}</b>

📦 <b>{title}</b>

💰 Сумма: <b>{price}₽</b>
🧾 Комиссия: <b>{commission}₽</b>

━━━━━━━━━━━━━━
Выберите способ оплаты:

⏳ Проверка оплаты до 24 часов
""",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="💳 СБП", callback_data=f"pay_sbp:{deal_id}"),
                    InlineKeyboardButton(text="🪙 Крипта", callback_data=f"pay_crypto:{deal_id}")
                ],
                [
                    InlineKeyboardButton(text="⭐ Stars", callback_data=f"pay_stars:{deal_id}")
                ],
                [
                    InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")
                ]
            ]
        ),
        parse_mode="HTML"
    )

    await call.answer()


# ===== СБП =====

@dp.callback_query(F.data.startswith("pay_sbp:"))
async def pay_sbp(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        conn.execute(
            "UPDATE deals SET payment_method=?, status=? WHERE id=?",
            ("sbp", "waiting_receipt", deal_id),
        )
        conn.commit()

    await show_screen(call, 
        f"""
💳 <b>Оплата через СБП</b>

Сделка: <b>#{deal_id}</b>

🏦 Банк: <b>{SBP_BANK}</b>
👤 Получатель: <b>{SBP_NAME}</b>
📱 Телефон: <code>{SBP_PHONE}</code>

━━━━━━━━━━━━━━
⏳ Проверка оплаты до 24 часов

После оплаты нажмите кнопку ниже и отправьте чек.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"receipt:{deal_id}")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")]
        ]),
        parse_mode="HTML"
    )

    await call.answer()


@dp.callback_query(F.data.startswith("pay_crypto:"))
async def pay_crypto(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        conn.execute(
            "UPDATE deals SET payment_method=?, status=? WHERE id=?",
            ("crypto", "waiting_receipt", deal_id),
        )
        conn.commit()

    await show_screen(call, 
        f"""
🪙 <b>Оплата криптой</b>

Сделка: <b>#{deal_id}</b>

TON кошелёк LTeam:
<code>{CRYPTO_WALLET}</code>

━━━━━━━━━━━━━━
⏳ Проверка оплаты до 24 часов

После оплаты нажмите кнопку ниже и отправьте хэш транзакции или скрин.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"receipt:{deal_id}")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")]
        ]),
        parse_mode="HTML"
    )

    await call.answer()

@dp.callback_query(F.data.startswith("pay_stars:"))
async def pay_stars(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        conn.execute(
            "UPDATE deals SET payment_method=?, status=? WHERE id=?",
            ("stars", "waiting_receipt", deal_id),
        )
        conn.commit()

    await show_screen(call, 
        f"""
⭐ <b>Оплата через Telegram Stars</b>

Сделка: <b>#{deal_id}</b>

Пока автоматическая оплата Stars не подключена.
Свяжитесь с поддержкой или отправьте подтверждение оплаты после ручной оплаты.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"receipt:{deal_id}")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")]
        ]),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("receipt:"))
async def receipt_start(call: CallbackQuery, state: FSMContext):
    deal_id = int(call.data.split(":")[1])
    await state.update_data(deal_id=deal_id)
    await state.set_state(ReceiptState.receipt)

    await show_screen(call, "📎 Отправьте чек, скриншот или хэш транзакции одним сообщением.")
    await call.answer()


@dp.message(ReceiptState.receipt)
async def receipt_save(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = data["deal_id"]

    if message.photo:
        receipt_text = "📷 Пользователь отправил фото чека"
    elif message.document:
        receipt_text = "📎 Пользователь отправил документ"
    else:
        receipt_text = message.text or "Пользователь отправил чек"

    with db() as conn:
        deal = conn.execute(
            """
            SELECT buyer_id, seller_id, amount, commission, payout, payment_method
            FROM deals WHERE id=?
            """,
            (deal_id,),
        ).fetchone()

        if not deal:
            await screen_answer(message,"❌ Сделка не найдена.")
            await state.clear()
            return

        conn.execute(
            "UPDATE deals SET receipt=?, status=? WHERE id=?",
            (receipt_text, "waiting_admin_confirm", deal_id),
        )
        conn.commit()

    await state.clear()

    buyer_id, seller_id, amount, commission, payout, method = deal
    seller_contact = user_contact(seller_id)

    await screen_answer(message,
        f"""
━━━━━━━━━━━━━━
✅ <b>Чек отправлен</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>

⏳ Оплата будет проверена в течение 24 часов.
После подтверждения исполнитель сможет начать работу.
""",
        parse_mode="HTML"
    )

    admin_text = f"""
━━━━━━━━━━━━━━
🔔 <b>Оплата на проверку</b>
━━━━━━━━━━━━━━

📦 Сделка: <b>#{deal_id}</b>

👤 Покупатель: <code>{buyer_id}</code>
🏪 Продавец: {seller_contact}

💰 Сумма: <b>{amount}₽</b>
🧾 Комиссия LTeam: <b>{commission}₽</b>
💸 К выплате продавцу: <b>{payout}₽</b>

💳 Метод оплаты: <b>{method}</b>

━━━━━━━━━━━━━━
🧾 Чек / данные оплаты:
{receipt_text}
"""

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить оплату", callback_data=f"admin_pay_ok:{deal_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_pay_no:{deal_id}"),
            ]
        ]
    )

    for admin in ADMIN_IDS:
        await bot.send_message(
            admin,
            admin_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("admin_pay_ok:"))
async def admin_pay_ok(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id, seller_id FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return

        buyer_id, seller_id = deal
        conn.execute(
            "UPDATE deals SET status=? WHERE id=?",
            ("in_work", deal_id),
        )
        conn.commit()

    chat_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Написать по сделке", callback_data=f"deal_chat:{deal_id}")],
        [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
    ])

    await bot.send_message(
        buyer_id,
        f"✅ Оплата по сделке #{deal_id} подтверждена. Исполнитель начинает работу. Деньги будут переведены исполнителю только после вашего подтверждения выполнения. Общайтесь только через чат сделки в боте.",
        reply_markup=chat_keyboard,
    )
    await bot.send_message(
        seller_id,
        f"✅ Оплата по сделке #{deal_id} подтверждена. Можно начинать выполнение. Когда закончите — откройте сделку и нажмите «📦 Отметить выполненным». Общайтесь только через чат сделки в боте.",
        reply_markup=chat_keyboard,
    )
    await call.message.edit_text(f"✅ Оплата по сделке #{deal_id} подтверждена.")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_pay_no:"))
async def admin_pay_no(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return

        buyer_id = deal[0]

        conn.execute(
            "UPDATE deals SET status=? WHERE id=?",
            ("payment_rejected", deal_id),
        )
        conn.commit()

    await bot.send_message(
        buyer_id,
        f"❌ Оплата по сделке #{deal_id} не подтверждена. Свяжитесь с поддержкой."
    )

    await call.message.edit_text(f"❌ Оплата по сделке #{deal_id} отклонена.")
    await call.answer()


@dp.callback_query(F.data == "my_deals")
async def my_deals(call: CallbackQuery):
    await show_screen(call,
        """
━━━━━━━━━━━━━━
💬 <b>Мои сделки</b>
━━━━━━━━━━━━━━

Выберите раздел:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Мои покупки", callback_data="my_purchases")],
            [InlineKeyboardButton(text="💰 Мои продажи", callback_data="my_sales")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "my_purchases")
async def my_purchases(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        rows = conn.execute(
            """
            SELECT d.id, l.title, d.amount, d.status
            FROM deals d
            JOIN listings l ON l.id = d.listing_id
            WHERE d.buyer_id=?
            ORDER BY d.id DESC
            LIMIT 20
            """,
            (user_id,),
        ).fetchall()

    if not rows:
        await show_screen(call, "📦 У вас пока нет покупок.", reply_markup=back_home())
        await call.answer()
        return

    buttons = []
    for deal_id, title, amount, status in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"#{deal_id} • {title} • {amount}₽ • {status}",
                callback_data=f"deal:{deal_id}",
            )
        ])

    buttons.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")])

    await show_screen(call, 
        "📦 <b>Мои покупки</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "seller_panel")
async def seller_panel(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        active_count = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE seller_id=? AND status='active'",
            (user_id,),
        ).fetchone()[0]

        sales_count = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE seller_id=? AND status='completed'",
            (user_id,),
        ).fetchone()[0]

        rating_row = conn.execute(
            "SELECT AVG(rating), COUNT(*) FROM reviews WHERE seller_id=?",
            (user_id,),
        ).fetchone()

    avg_rating, reviews_count = rating_row
    rating_text = "Пока нет отзывов" if avg_rating is None else f"{avg_rating:.1f} ⭐ ({reviews_count})"
    seller_status = "✅ Проверенный продавец" if sales_count >= 3 and avg_rating and avg_rating >= 4.5 else "🆕 Новый продавец"

    await show_screen(call, 
        f"""
━━━━━━━━━━━━━━
🏪 <b>Кабинет продавца</b>
━━━━━━━━━━━━━━

{seller_status}

📌 Активных объявлений: <b>{active_count}</b>
💰 Завершённых продаж: <b>{sales_count}</b>
⭐ Рейтинг: <b>{rating_text}</b>

Здесь вы можете управлять своими объявлениями, продажами и репутацией.
""",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📌 Мои объявления", callback_data="my_listings")],
                [InlineKeyboardButton(text="💰 Мои продажи", callback_data="my_sales")],
                [InlineKeyboardButton(text="➕ Разместить объявление", callback_data="create_listing")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")],
            ]
        ),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data == "my_sales")
async def my_sales(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        rows = conn.execute(
            """
            SELECT d.id, l.title, d.amount, d.status
            FROM deals d
            JOIN listings l ON l.id = d.listing_id
            WHERE d.seller_id=?
            ORDER BY d.id DESC
            LIMIT 20
            """,
            (user_id,),
        ).fetchall()

    if not rows:
        await show_screen(call, "💰 Продаж пока нет.", reply_markup=back_home())
        await call.answer()
        return

    buttons = []
    for deal_id, title, amount, status in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"#{deal_id} • {title} • {amount}₽ • {status}",
                callback_data=f"deal:{deal_id}",
            )
        ])

    buttons.append([InlineKeyboardButton(text="⬅️ Кабинет продавца", callback_data="profile")])

    await show_screen(call, 
        "💰 <b>Мои продажи</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "my_listings")
async def my_listings(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, title, price, is_top, is_highlight
            FROM listings
            WHERE seller_id=? AND status='active'
            ORDER BY COALESCE(bumped_at, created_at) DESC, id DESC
            """,
            (user_id,),
        ).fetchall()

    if not rows:
        await show_screen(call,
            """
━━━━━━━━━━━━━━
📦 <b>Мои объявления</b>
━━━━━━━━━━━━━━

У вас пока нет активных объявлений.
Создайте первое объявление и оно появится в маркете.
""",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Разместить объявление", callback_data="create_listing")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")],
            ]),
            parse_mode="HTML"
        )
        await call.answer()
        return

    text = """
━━━━━━━━━━━━━━
📦 <b>Мои объявления</b>
━━━━━━━━━━━━━━

Выберите объявление для просмотра или продвижения:

"""
    buttons = []
    for idx, (listing_id, title, price, is_top, is_highlight) in enumerate(rows[:10], start=1):
        marker = promo_marker(is_top, is_highlight)
        text += f"<b>{idx}.</b> {marker} {html.escape(title)}\n💰 <b>{price}₽</b> • ID: <code>{listing_id}</code>\n\n"
        buttons.append([
            InlineKeyboardButton(text=f"{idx}️⃣ Открыть", callback_data=f"view_listing:{listing_id}"),
            InlineKeyboardButton(text="🚀 Продвинуть", callback_data=f"promo_menu:{listing_id}"),
        ])
        buttons.append([
            InlineKeyboardButton(text="✏️ Редактировать позже", callback_data=f"edit_listing_soon:{listing_id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"owner_delete_listing:{listing_id}"),
        ])

    buttons.append([InlineKeyboardButton(text="➕ Новое объявление", callback_data="create_listing")])
    buttons.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")])

    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("edit_listing_soon:"))
async def edit_listing_soon(call: CallbackQuery):
    listing_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT seller_id FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone()
    if not row:
        await call.answer("Объявление не найдено", show_alert=True)
        return
    if call.from_user.id != row[0] and not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.answer("Редактирование добавим следующим этапом. Пока можно удалить и создать заново.", show_alert=True)


@dp.callback_query(F.data.startswith("owner_delete_listing:"))
async def owner_delete_listing(call: CallbackQuery):
    listing_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT seller_id, title FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone()
    if not row:
        await call.answer("Объявление не найдено", show_alert=True)
        return
    seller_id, title = row
    if call.from_user.id != seller_id and not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
🗑 <b>Удаление объявления</b>
━━━━━━━━━━━━━━

Вы точно хотите удалить объявление?

📦 <b>{html.escape(title or 'Без названия')}</b>

После удаления оно пропадёт из маркета.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"owner_delete_listing_ok:{listing_id}")],
            [InlineKeyboardButton(text="❌ Нет, назад", callback_data="my_listings")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("owner_delete_listing_ok:"))
async def owner_delete_listing_ok(call: CallbackQuery):
    listing_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT seller_id FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone()
        if not row:
            await call.answer("Объявление не найдено", show_alert=True)
            return
        if call.from_user.id != row[0] and not is_admin(call.from_user.id):
            await call.answer("Нет доступа", show_alert=True)
            return
        conn.execute("UPDATE listings SET status='deleted' WHERE id=?", (listing_id,))
        conn.commit()

    await show_screen(
        call,
        f"✅ Объявление #{listing_id} удалено.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Мои объявления", callback_data="my_listings")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()



@dp.callback_query(F.data.startswith("deal:"))
async def view_deal(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        deal = conn.execute(
            """
            SELECT d.id, d.buyer_id, d.seller_id, d.amount, d.commission,
                   d.payout, d.payment_method, d.status, l.title
            FROM deals d
            JOIN listings l ON l.id = d.listing_id
            WHERE d.id=?
            """,
            (deal_id,),
        ).fetchone()

    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return

    (
        deal_id,
        buyer_id,
        seller_id,
        amount,
        commission,
        payout,
        payment_method,
        status,
        title,
    ) = deal

    if user_id not in [buyer_id, seller_id] and user_id not in ADMIN_IDS:
        await call.answer("Нет доступа", show_alert=True)
        return

    buttons = []

    if status in ["in_work", "waiting_buyer_confirm", "waiting_payout", "completed"]:
        buttons.append([InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")])

    if user_id == seller_id and status == "in_work":
        buttons.append([InlineKeyboardButton(text="📦 Отметить выполненным", callback_data=f"seller_done:{deal_id}")])

    if user_id == buyer_id and status == "waiting_buyer_confirm":
        buttons.append([InlineKeyboardButton(text="✅ Подтвердить выполнение", callback_data=f"buyer_done:{deal_id}")])
        buttons.append([InlineKeyboardButton(text="❌ Есть проблема", callback_data=f"deal_dispute:{deal_id}")])

    if status in ["in_work", "waiting_admin_confirm", "waiting_receipt", "waiting_buyer_confirm"]:
        buttons.append([InlineKeyboardButton(text="🚨 Открыть спор", callback_data=f"deal_dispute:{deal_id}")])

    if user_id in ADMIN_IDS:
        buttons.append([InlineKeyboardButton(text="👀 Админ: читать чат", callback_data=f"admin_deal_chat:{deal_id}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")])

    await show_screen(call, 
        f"""
📦 <b>Сделка #{deal_id}</b>

Товар/услуга: <b>{title}</b>
Сумма: <b>{amount}₽</b>
Комиссия: <b>{commission}₽</b>
К выплате продавцу: <b>{payout}₽</b>
Оплата: <b>{payment_method or "не выбрана"}</b>
Статус: <b>{status}</b>

🛡 Деньги исполнителю переводятся только после подтверждения выполнения покупателем.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("seller_done:"))
async def seller_done(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    seller_id = call.from_user.id

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id, seller_id, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return

        buyer_id, db_seller_id, status = deal

        if seller_id != db_seller_id:
            await call.answer("Это может сделать только исполнитель", show_alert=True)
            return

        if status != "in_work":
            await call.answer("Сейчас нельзя отметить выполнение", show_alert=True)
            return

        conn.execute(
            "UPDATE deals SET status=? WHERE id=?",
            ("waiting_buyer_confirm", deal_id),
        )
        conn.commit()

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
📦 <b>Выполнение отправлено на проверку</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>

Покупатель получил уведомление и должен подтвердить выполнение.
До подтверждения деньги не переводятся исполнителю.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )

    await bot.send_message(
        buyer_id,
        f"""
━━━━━━━━━━━━━━
📦 <b>Исполнитель отметил заказ выполненным</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>

Проверьте результат. Если всё хорошо — подтвердите выполнение.
Если есть проблема — откройте спор, и администратор разберёт ситуацию.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить выполнение", callback_data=f"buyer_done:{deal_id}")],
            [InlineKeyboardButton(text="❌ Есть проблема", callback_data=f"deal_dispute:{deal_id}")],
            [InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()



@dp.callback_query(F.data.startswith("buyer_done:"))
async def buyer_done(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    buyer_id = call.from_user.id

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id, seller_id, payout, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return

        db_buyer_id, seller_id, payout, status = deal

        if buyer_id != db_buyer_id:
            await call.answer("Это может сделать только покупатель", show_alert=True)
            return

        if status != "waiting_buyer_confirm":
            await call.answer("Сначала исполнитель должен отметить заказ выполненным", show_alert=True)
            return

        conn.execute(
            "UPDATE deals SET status=? WHERE id=?",
            ("waiting_payout", deal_id),
        )
        conn.commit()

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
✅ <b>Выполнение подтверждено</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>

Теперь администратор сделает выплату исполнителю.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    seller_contact = user_contact(seller_id)

    admin_text = f"""
💸 <b>Покупатель подтвердил выполнение</b>

Сделка: <b>#{deal_id}</b>
Продавец: {seller_contact}
К выплате: <b>{payout}₽</b>

Деньги можно переводить исполнителю.
"""

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выплату сделал", callback_data=f"admin_payout_done:{deal_id}")],
        [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
    ])

    for admin in ADMIN_IDS:
        await bot.send_message(admin, admin_text, reply_markup=keyboard, parse_mode="HTML")

    await bot.send_message(
        seller_id,
        f"✅ Покупатель подтвердил выполнение по сделке #{deal_id}. Ожидайте выплату от администратора."
    )

    await call.answer()




@dp.callback_query(F.data.startswith("admin_payout_done:"))
async def admin_payout_done(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id, seller_id FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return

        buyer_id, seller_id = deal

        conn.execute(
            "UPDATE deals SET status=? WHERE id=?",
            ("completed", deal_id),
        )
        conn.commit()

    await bot.send_message(buyer_id, f"✅ Сделка #{deal_id} завершена.")
    await bot.send_message(seller_id, f"✅ Выплата по сделке #{deal_id} отмечена как выполненная.")

    await bot.send_message(
        buyer_id,
        f"⭐ Оцените продавца по сделке #{deal_id}:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="1⭐", callback_data=f"review_rating:{deal_id}:1"),
                InlineKeyboardButton(text="2⭐", callback_data=f"review_rating:{deal_id}:2"),
                InlineKeyboardButton(text="3⭐", callback_data=f"review_rating:{deal_id}:3"),
                InlineKeyboardButton(text="4⭐", callback_data=f"review_rating:{deal_id}:4"),
                InlineKeyboardButton(text="5⭐", callback_data=f"review_rating:{deal_id}:5"),
            ]
        ])
    )

    await call.message.edit_text(f"✅ Сделка #{deal_id} закрыта.")
    await call.answer()


@dp.callback_query(F.data.startswith("deal_dispute:"))
async def deal_dispute(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])

    for admin in ADMIN_IDS:
        await bot.send_message(
            admin,
            f"🚨 Открыт спор по сделке #{deal_id}\nПользователь: {call.from_user.id}"
        )

    await show_screen(call, "🚨 Спор открыт. Администрация получила уведомление.")
    await call.answer()


# ===== ЧАТ СДЕЛКИ =====

@dp.callback_query(F.data.startswith("deal_chat:"))
async def deal_chat_start(call: CallbackQuery, state: FSMContext):
    deal_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id, seller_id, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return

    buyer_id, seller_id, status = deal

    if user_id not in [buyer_id, seller_id] and user_id not in ADMIN_IDS:
        await call.answer("Нет доступа", show_alert=True)
        return

    if status not in ["in_work", "waiting_payout", "completed"]:
        await call.answer("Чат откроется после подтверждения оплаты", show_alert=True)
        return

    receiver_id = seller_id if user_id == buyer_id else buyer_id
    await state.update_data(deal_id=deal_id, receiver_id=receiver_id)
    await state.set_state(DealChatState.text)

    history_rows = get_deal_chat_history(deal_id, limit=8)
    history_text = format_chat_history(history_rows, current_user_id=user_id, limit_note="последние 8")

    await show_screen(call, 
        f"""
━━━━━━━━━━━━━━
💬 <b>Чат сделки #{deal_id}</b>
━━━━━━━━━━━━━━

<b>История:</b>
{history_text}

Напишите сообщение одним текстом. Бот отправит его второй стороне без передачи ваших контактов.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Открыть сделку", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(DealChatState.text)
async def deal_chat_send(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = data.get("deal_id")
    receiver_id = data.get("receiver_id")

    if not deal_id or not receiver_id:
        await state.clear()
        await screen_answer(message,"❌ Чат сделки не найден. Откройте сделку заново.")
        return

    text = message.text or "Пользователь отправил сообщение без текста"
    await warn_if_bypass_attempt(message.from_user.id, text, f"deal #{deal_id}")

    with db() as conn:
        conn.execute(
            "INSERT INTO deal_messages (deal_id, sender_id, receiver_id, text, created_at) VALUES (?, ?, ?, ?, ?)",
            (deal_id, message.from_user.id, receiver_id, text, datetime.now().isoformat()),
        )
        conn.commit()

    await bot.send_message(
        receiver_id,
        f"💬 <b>Сообщение по сделке #{deal_id}</b>\n\n{text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩️ Ответить", callback_data=f"deal_chat:{deal_id}")],
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
        ]),
        parse_mode="HTML",
    )

    await screen_answer(message,
        "✅ Сообщение отправлено внутри сделки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать ещё", callback_data=f"deal_chat:{deal_id}")],
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
        ]),
    )
    await state.clear()


# ===== АДМИН: ПРОСМОТР ЧАТОВ =====

@dp.callback_query(F.data.startswith("admin_deal_chat:"))
async def admin_deal_chat(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    deal_id = int(call.data.split(":")[1])
    rows = get_deal_chat_history(deal_id, limit=30)
    history_text = format_chat_history(rows, current_user_id=None, limit_note="последние 30")

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
👀 <b>Чат сделки #{deal_id}</b>
━━━━━━━━━━━━━━

{history_text}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()

@dp.message(Command("deal_chat"))
async def admin_deal_chat_command(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await screen_answer(message, "Использование: <code>/deal_chat 15</code>", parse_mode="HTML")
        return

    deal_id = int(parts[1])
    rows = get_deal_chat_history(deal_id, limit=30)
    history_text = format_chat_history(rows, current_user_id=None, limit_note="последние 30")
    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
👀 <b>Чат сделки #{deal_id}</b>
━━━━━━━━━━━━━━

{history_text}
""",
        parse_mode="HTML",
    )

# ===== ПОДДЕРЖКА =====

@dp.callback_query(F.data == "support")
async def support(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        open_count = conn.execute(
            "SELECT COUNT(*) FROM tickets WHERE user_id=? AND status='open'",
            (user_id,),
        ).fetchone()[0]

    await show_screen(call, 
        f"""
🆘 <b>Поддержка LTeam</b>

Здесь можно создать обращение по оплате, сделке, объявлению или спору.

Активных обращений: <b>{open_count}</b>
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать обращение", callback_data="ticket_create")],
            [InlineKeyboardButton(text="📂 Мои обращения", callback_data="my_tickets")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "ticket_create")
async def ticket_create(call: CallbackQuery, state: FSMContext):
    await state.set_state(SupportState.text)
    await show_screen(call, "Опишите проблему одним сообщением:")
    await call.answer()


@dp.message(SupportState.text)
async def ticket_save(message: Message, state: FSMContext):
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tickets (user_id, text, status, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                message.from_user.id,
                message.text,
                "open",
                datetime.now().isoformat(),
            ),
        )
        ticket_id = cur.lastrowid
        conn.commit()

    await state.clear()
    await screen_answer(message,f"✅ Обращение #{ticket_id} создано. Ожидайте ответа поддержки.")

    admin_text = f"""
🆘 <b>Новое обращение #{ticket_id}</b>

Пользователь: <code>{message.from_user.id}</code>
Username: @{message.from_user.username or "нет"}

Текст:
{message.text}
"""

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✉️ Ответить", callback_data=f"admin_ticket_reply:{ticket_id}"),
            InlineKeyboardButton(text="✅ Закрыть", callback_data=f"admin_ticket_close:{ticket_id}"),
        ]
    ])

    for admin in ADMIN_IDS:
        await bot.send_message(admin, admin_text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query(F.data == "my_tickets")
async def my_tickets(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, status, created_at
            FROM tickets
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT 10
            """,
            (user_id,),
        ).fetchall()

    if not rows:
        await show_screen(call, "📂 У вас пока нет обращений.", reply_markup=back_home())
        await call.answer()
        return

    text = "📂 <b>Мои обращения</b>\n\n"
    for ticket_id, status, created_at in rows:
        text += f"#{ticket_id} — {status}\n"

    await show_screen(call, text, reply_markup=back_home(), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_ticket_reply:"))
async def admin_ticket_reply(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    ticket_id = int(call.data.split(":")[1])
    await state.update_data(ticket_id=ticket_id)
    await state.set_state(SupportState.admin_reply)

    await show_screen(call, f"Введите ответ пользователю по обращению #{ticket_id}:")
    await call.answer()


@dp.message(SupportState.admin_reply)
async def admin_ticket_reply_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await screen_answer(message,"Нет доступа.")
        await state.clear()
        return

    data = await state.get_data()
    ticket_id = data["ticket_id"]

    with db() as conn:
        row = conn.execute(
            "SELECT user_id FROM tickets WHERE id=?",
            (ticket_id,),
        ).fetchone()

        if not row:
            await screen_answer(message,"Обращение не найдено.")
            await state.clear()
            return

        user_id = row[0]

    await bot.send_message(
        user_id,
        f"""
💬 <b>Ответ поддержки по обращению #{ticket_id}</b>

{message.text}
""",
        parse_mode="HTML",
    )

    await screen_answer(message,f"✅ Ответ по обращению #{ticket_id} отправлен.")
    await state.clear()


@dp.callback_query(F.data.startswith("admin_ticket_close:"))
async def admin_ticket_close(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    ticket_id = int(call.data.split(":")[1])

    with db() as conn:
        row = conn.execute(
            "SELECT user_id FROM tickets WHERE id=?",
            (ticket_id,),
        ).fetchone()

        if not row:
            await call.answer("Обращение не найдено", show_alert=True)
            return

        user_id = row[0]

        conn.execute(
            "UPDATE tickets SET status='closed' WHERE id=?",
            (ticket_id,),
        )
        conn.commit()

    await bot.send_message(user_id, f"✅ Обращение #{ticket_id} закрыто поддержкой.")
    await call.message.edit_text(f"✅ Обращение #{ticket_id} закрыто.")
    await call.answer()


# ===== ОТЗЫВЫ =====

@dp.callback_query(F.data.startswith("review_rating:"))
async def review_rating(call: CallbackQuery, state: FSMContext):
    _, deal_id, rating = call.data.split(":")
    deal_id = int(deal_id)
    rating = int(rating)

    with db() as conn:
        row = conn.execute(
            "SELECT buyer_id, seller_id, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

    if not row:
        await call.answer("Сделка не найдена", show_alert=True)
        return

    buyer_id, seller_id, status = row

    if call.from_user.id != buyer_id:
        await call.answer("Отзыв может оставить только покупатель", show_alert=True)
        return

    if status != "completed":
        await call.answer("Отзыв можно оставить только после завершения сделки", show_alert=True)
        return

    await state.update_data(deal_id=deal_id, rating=rating, seller_id=seller_id)
    await state.set_state(ReviewState.text)

    await show_screen(call, "Напишите короткий отзыв о продавце:")
    await call.answer()


@dp.message(ReviewState.text)
async def review_text(message: Message, state: FSMContext):
    data = await state.get_data()

    with db() as conn:
        exists = conn.execute(
            "SELECT 1 FROM reviews WHERE deal_id=? AND reviewer_id=?",
            (data["deal_id"], message.from_user.id),
        ).fetchone()

        if exists:
            await screen_answer(message,"Вы уже оставляли отзыв по этой сделке.")
            await state.clear()
            return

        conn.execute(
            """
            INSERT INTO reviews (deal_id, reviewer_id, seller_id, rating, text, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                data["deal_id"],
                message.from_user.id,
                data["seller_id"],
                data["rating"],
                message.text,
                datetime.now().isoformat(),
            ),
        )
        conn.commit()

    await state.clear()
    await screen_answer(message,"✅ Спасибо! Отзыв сохранён.")


# ===== ПРОФИЛЬ =====

@dp.callback_query(F.data == "profile")
async def profile(call: CallbackQuery):
    user_id = call.from_user.id

    with db() as conn:
        username = conn.execute(
            "SELECT username FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()

        listings_count = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE seller_id=? AND status='active'",
            (user_id,),
        ).fetchone()[0]

        purchases_count = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE buyer_id=?",
            (user_id,),
        ).fetchone()[0]

        sales_count = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE seller_id=? AND status='completed'",
            (user_id,),
        ).fetchone()[0]

        rating_row = conn.execute(
            "SELECT AVG(rating), COUNT(*) FROM reviews WHERE seller_id=?",
            (user_id,),
        ).fetchone()

    username_text = username[0] if username and username[0] else "не указан"
    avg_rating, reviews_count = rating_row
    rating_text = "нет отзывов" if avg_rating is None else f"{avg_rating:.1f} ⭐ ({reviews_count})"

    user_status = "✅ Активный пользователь" if purchases_count + sales_count > 0 else "🆕 Новый пользователь"
    seller_status = "✅ Проверенный продавец" if sales_count >= 3 and avg_rating and avg_rating >= 4.5 else "Обычный продавец"

    await show_screen(call, 
        f"""
━━━━━━━━━━━━━━
👤 <b>Профиль</b>
━━━━━━━━━━━━━━

{user_status}

🆔 ID: <code>{user_id}</code>
🔗 Username: @{username_text}

🛒 Покупок: <b>{purchases_count}</b>
🏪 Продаж: <b>{sales_count}</b>
📌 Объявлений: <b>{listings_count}</b>

⭐ Рейтинг продавца: <b>{rating_text}</b>
🏷 Статус продавца: <b>{seller_status}</b>
""",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🏪 Кабинет продавца", callback_data="profile")],
                [InlineKeyboardButton(text="📦 Мои покупки", callback_data="my_purchases")],
                [InlineKeyboardButton(text="⭐ Избранное", callback_data="favorites")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")],
            ]
        ),
        parse_mode="HTML"
    )
    await call.answer()


# ===== АДМИНКА =====

@dp.callback_query(F.data.startswith("admin_delete:"))
async def admin_delete(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    listing_id = int(call.data.split(":")[1])

    with db() as conn:
        conn.execute(
            "UPDATE listings SET status='deleted' WHERE id=?",
            (listing_id,),
        )
        conn.commit()

    await show_screen(call, f"❌ Объявление #{listing_id} удалено.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Маркет", callback_data="market")],
        [InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")],
    ]))
    await call.answer()



@dp.callback_query(F.data.startswith("admin_user:"))
async def admin_user_profile(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    stats = seller_stats(user_id)

    with db() as conn:
        user_row = conn.execute("SELECT username, created_at, COALESCE(verified,0) FROM users WHERE user_id=?", (user_id,)).fetchone()
        purchases_count = conn.execute("SELECT COUNT(*) FROM deals WHERE buyer_id=?", (user_id,)).fetchone()[0]
        seller_deals_count = conn.execute("SELECT COUNT(*) FROM deals WHERE seller_id=?", (user_id,)).fetchone()[0]
        active_orders = conn.execute("SELECT COUNT(*) FROM orders WHERE customer_id=? AND status='active'", (user_id,)).fetchone()[0]
        moderation_orders = conn.execute("SELECT COUNT(*) FROM orders WHERE customer_id=? AND status='moderation'", (user_id,)).fetchone()[0]
        moderation_listings = conn.execute("SELECT COUNT(*) FROM listings WHERE seller_id=? AND status='moderation'", (user_id,)).fetchone()[0]
        tickets_count = conn.execute("SELECT COUNT(*) FROM tickets WHERE user_id=?", (user_id,)).fetchone()[0]
        reports_by_user = conn.execute("SELECT COUNT(*) FROM reports WHERE user_id=?", (user_id,)).fetchone()[0]
        reports_on_user = conn.execute("""
            SELECT COUNT(*)
            FROM reports r
            LEFT JOIN listings l ON l.id = r.listing_id
            LEFT JOIN orders o ON o.id = COALESCE(r.target_id, 0) AND COALESCE(r.target_type,'listing')='order'
            WHERE l.seller_id=? OR o.customer_id=?
        """, (user_id, user_id)).fetchone()[0]
        warnings_count = conn.execute("SELECT COUNT(*) FROM admin_warnings WHERE user_id=?", (user_id,)).fetchone()[0]
        banned = conn.execute("SELECT reason, created_at, banned_by FROM banned_users WHERE user_id=?", (user_id,)).fetchone()
        last_deal = conn.execute("SELECT id, status, amount, created_at FROM deals WHERE buyer_id=? OR seller_id=? ORDER BY id DESC LIMIT 1", (user_id, user_id)).fetchone()

    username = (user_row[0] if user_row and user_row[0] else stats.get("username", "не указан"))
    created_at = (user_row[1] if user_row and user_row[1] else stats.get("created_at", "неизвестно"))
    ban_status = "🚫 Забанен" if banned else "✅ Активен"
    ban_line = ""
    if banned:
        ban_line = f"\nПричина: <b>{html.escape(banned[0] or 'не указана')}</b>\nДата: <code>{html.escape(str(banned[1])[:16])}</code> • Админ: <code>{banned[2]}</code>"
    verify_status = "🛡 LTeam Verified" if stats.get("verified") else "—"
    last_deal_text = "нет"
    if last_deal:
        last_deal_text = f"#{last_deal[0]} • {html.escape(last_deal[1] or '—')} • {last_deal[2]}₽"

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
👤 <b>Админ-карточка пользователя</b>
━━━━━━━━━━━━━━

🆔 ID: <code>{user_id}</code>
🔗 Username: @{html.escape(username)}
📅 Регистрация: <b>{html.escape(str(created_at)[:10])}</b>
🚦 Доступ: <b>{ban_status}</b>{ban_line}
🎚 Роль: <b>{role_badge(user_id)}</b>
🛡 Верификация: <b>{verify_status}</b>

📦 Активных объявлений: <b>{stats['active_listings']}</b>
⏳ Объявлений на модерации: <b>{moderation_listings}</b>
📌 Активных заказов: <b>{active_orders}</b>
⏳ Заказов на модерации: <b>{moderation_orders}</b>

🛒 Покупок: <b>{purchases_count}</b>
💰 Сделок как продавец: <b>{seller_deals_count}</b>
✅ Завершённых продаж: <b>{stats['sales_count']}</b>
⭐ Рейтинг: <b>{stats['rating_text']}</b>
💼 Последняя сделка: <b>{last_deal_text}</b>

🚨 Жалоб от пользователя: <b>{reports_by_user}</b>
🚨 Жалоб на пользователя: <b>{reports_on_user}</b>
⚠️ Предупреждений: <b>{warnings_count}</b>
🆘 Обращений в поддержку: <b>{tickets_count}</b>
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Объявления", callback_data=f"admin_user_listings:{user_id}"), InlineKeyboardButton(text="📌 Заказы", callback_data=f"admin_user_orders:{user_id}")],
            [InlineKeyboardButton(text="💼 Сделки", callback_data=f"admin_user_deals:{user_id}"), InlineKeyboardButton(text="🚨 Жалобы", callback_data=f"admin_user_reports:{user_id}")],
            [InlineKeyboardButton(text="⚠️ Предупреждения", callback_data=f"admin_user_warnings:{user_id}"), InlineKeyboardButton(text="✉️ Написать", callback_data=f"admin_msg_user:{user_id}")],
            [InlineKeyboardButton(text="🛡 Выдать Verified", callback_data=f"admin_verify_user:{user_id}"), InlineKeyboardButton(text="❌ Снять Verified", callback_data=f"admin_unverify_user:{user_id}")],
            [InlineKeyboardButton(text="⚠️ Выдать предупреждение", callback_data=f"admin_warn_user:{user_id}"), InlineKeyboardButton(text="🔇 Мут", callback_data=f"admin_mute_user:{user_id}")],
            [InlineKeyboardButton(text="👑 Назначить роль", callback_data=f"admin_role_choose:{user_id}")],
            [InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin_ban_user:{user_id}"), InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin_unban_user:{user_id}")],
            [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin_verify_user:"))
async def admin_verify_user(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "verify")
    if not ok:
        await call.answer(reason, show_alert=True)
        return
    with db() as conn:
        conn.execute("UPDATE users SET verified=1 WHERE user_id=?", (user_id,))
        conn.commit()
    await notify_admins(f"🛡 <b>Верификация выдана</b>\n\nПользователь: <code>{user_id}</code>\nАдмин: <code>{call.from_user.id}</code>")
    await call.answer("🛡 Верификация LTeam выдана", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


@dp.callback_query(F.data.startswith("admin_unverify_user:"))
async def admin_unverify_user(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "unverify")
    if not ok:
        await call.answer(reason, show_alert=True)
        return
    with db() as conn:
        conn.execute("UPDATE users SET verified=0 WHERE user_id=?", (user_id,))
        conn.commit()
    await notify_admins(f"❌ <b>Верификация снята</b>\n\nПользователь: <code>{user_id}</code>\nАдмин: <code>{call.from_user.id}</code>")
    await call.answer("❌ Верификация LTeam снята", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


@dp.callback_query(F.data.startswith("admin_user_listings:"))
async def admin_user_listings(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    with db() as conn:
        rows = conn.execute("""
            SELECT id, title, price, status, created_at
            FROM listings
            WHERE seller_id=?
            ORDER BY id DESC
            LIMIT 25
        """, (user_id,)).fetchall()
    if not rows:
        await show_screen(call, "📦 У пользователя нет объявлений.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")],
        ]))
        await call.answer()
        return

    text = f"━━━━━━━━━━━━━━\n📦 <b>Объявления пользователя</b> <code>{user_id}</code>\n━━━━━━━━━━━━━━\n\n"
    buttons = []
    for lid, title, price, status, created_at in rows:
        status_badge = {"active":"✅ active", "moderation":"⏳ moderation", "rejected":"❌ rejected", "archived":"🗄 archived", "blocked":"🚫 blocked"}.get(status or "—", status or "—")
        text += f"<b>#{lid}</b> • {html.escape(title or 'Без названия')}\n💰 {price}₽ • {status_badge} • <code>{html.escape(str(created_at)[:10])}</code>\n\n"
        buttons.append([InlineKeyboardButton(text=f"#{lid} • {html.escape((title or 'Без названия')[:28])}", callback_data=f"admin_listing_actions:{lid}:{user_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_ban_user:"))
async def admin_ban_user_direct(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "ban")
    if not ok:
        await call.answer(reason, show_alert=True)
        return
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO banned_users (user_id, reason, banned_by, created_at) VALUES (?, ?, ?, ?)", (user_id, "Блокировка администратором", call.from_user.id, datetime.now().isoformat()))
        conn.commit()
    log_admin_action(call.from_user.id, "ban_user", user_id, "Блокировка администратором")
    await notify_admins(f"""🚫 <b>Пользователь забанен</b>

Пользователь: <code>{user_id}</code>
Админ: <code>{call.from_user.id}</code>""")
    await call.answer("Пользователь забанен", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


@dp.callback_query(F.data.startswith("admin_unban_user:"))
async def admin_unban_user_direct(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "unban")
    if not ok:
        await call.answer(reason, show_alert=True)
        return
    with db() as conn:
        ban = conn.execute("SELECT banned_by FROM banned_users WHERE user_id=?", (user_id,)).fetchone()
    if not ban:
        await call.answer("Пользователь не забанен", show_alert=True)
        return
    banned_by = int(ban[0] or 0)
    if is_owner(call.from_user.id) or banned_by == call.from_user.id or banned_by == 0:
        with db() as conn:
            conn.execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
            conn.commit()
        log_admin_action(call.from_user.id, "unban_user", user_id, "Прямой разбан")
        await notify_admins(f"""✅ <b>Пользователь разбанен</b>

Пользователь: <code>{user_id}</code>
Админ: <code>{call.from_user.id}</code>""")
        await call.answer("Пользователь разбанен", show_alert=True)
        call.data = f"admin_user:{user_id}"
        await admin_user_profile(call)
        return
    with db() as conn:
        cur = conn.execute("INSERT INTO admin_action_requests (request_type, target_id, requested_by, original_admin_id, reason, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)", ("unban", user_id, call.from_user.id, banned_by, "Запрос на разбан от другого админа", datetime.now().isoformat()))
        req_id = cur.lastrowid
        conn.commit()
    try:
        await bot.send_message(
            banned_by,
            f"""🔁 <b>Запрос на разбан</b>

Админ <code>{call.from_user.id}</code> хочет разбанить пользователя <code>{user_id}</code>.""",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_req_approve:{req_id}"), InlineKeyboardButton(text="❌ Отказать", callback_data=f"admin_req_reject:{req_id}")],
                [InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user:{user_id}")],
            ]),
            parse_mode="HTML",
        )
    except Exception:
        pass
    await call.answer("Создан запрос на разбан администратору, который выдал бан", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


@dp.callback_query(F.data == "admin_promo_pending")
async def admin_promo_pending(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    with db() as conn:
        rows = conn.execute("""
            SELECT p.id, p.listing_id, p.user_id, p.promo_type, p.amount, l.title
            FROM promo_payments p
            JOIN listings l ON l.id = p.listing_id
            WHERE p.status='waiting_admin_confirm'
            ORDER BY p.id DESC
            LIMIT 10
            """).fetchall()
    if not rows:
        await show_screen(call, "💰 Нет заявок на продвижение, ожидающих проверки.", reply_markup=back_home())
        await call.answer()
        return
    text = "━━━━━━━━━━━━━━\n💰 <b>Продвижение на проверке</b>\n━━━━━━━━━━━━━━\n\n"
    buttons = []
    for promo_id, listing_id, user_id, promo_type, amount, title in rows:
        option = PROMO_OPTIONS.get(promo_type, {"title": promo_type})
        text += f"#{promo_id} • {option['title']} • {amount}₽\n📦 #{listing_id}: {html.escape(title or 'Без названия')}\n👤 <code>{user_id}</code>\n\n"
        buttons.append([
            InlineKeyboardButton(text=f"✅ #{promo_id}", callback_data=f"admin_promo_ok:{promo_id}"),
            InlineKeyboardButton(text=f"❌ #{promo_id}", callback_data=f"admin_promo_no:{promo_id}"),
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()

# ===== ФИНАНСЫ ДЛЯ АДМИНОВ =====

@dp.callback_query(F.data == "admin_finance")
async def admin_finance(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    with db() as conn:
        total_commission = conn.execute(
            "SELECT COALESCE(SUM(commission), 0) FROM deals WHERE status='completed'"
        ).fetchone()[0]

        waiting_payout_sum = conn.execute(
            "SELECT COALESCE(SUM(payout), 0) FROM deals WHERE status='waiting_payout'"
        ).fetchone()[0]

        waiting_payout_count = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE status='waiting_payout'"
        ).fetchone()[0]

        completed_count = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE status='completed'"
        ).fetchone()[0]

    await show_screen(call, 
        f"""
━━━━━━━━━━━━━━
📊 <b>Финансы LTeam</b>
━━━━━━━━━━━━━━

💰 Заработано комиссии: <b>{total_commission}₽</b>
💸 Ожидает выплат: <b>{waiting_payout_sum}₽</b>
📦 Сделок ждут выплаты: <b>{waiting_payout_count}</b>
✅ Завершённых сделок: <b>{completed_count}</b>
""",
        reply_markup=back_home(),
        parse_mode="HTML"
    )
    await call.answer()

# ===== АДМИНКА: МОДЕРАЦИЯ ОБЪЯВЛЕНИЙ И ЗАКАЗОВ =====

def admin_back_moderation_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Объявления", callback_data="admin_mod_listings"), InlineKeyboardButton(text="📌 Заказы", callback_data="admin_mod_orders")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ])


def _short(text: str | None, limit: int = 90) -> str:
    value = (text or "").replace("\n", " ").strip()
    if len(value) <= limit:
        return value
    return value[:limit - 1] + "…"


@dp.callback_query(F.data == "admin_moderation")
async def admin_moderation(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    with db() as conn:
        listing_count = conn.execute("SELECT COUNT(*) FROM listings WHERE status='moderation'").fetchone()[0]
        order_count = conn.execute("SELECT COUNT(*) FROM orders WHERE status='moderation'").fetchone()[0]
        last_listings = conn.execute("""
            SELECT id, title, price, seller_id
            FROM listings
            WHERE status='moderation'
            ORDER BY id DESC
            LIMIT 3
        """).fetchall()
        last_orders = conn.execute("""
            SELECT id, title, budget, customer_id
            FROM orders
            WHERE status='moderation'
            ORDER BY id DESC
            LIMIT 3
        """).fetchall()

    text = f"""
━━━━━━━━━━━━━━
⏳ <b>Центр модерации</b>
━━━━━━━━━━━━━━

📦 Объявлений на проверке: <b>{listing_count}</b>
📌 Заказов на проверке: <b>{order_count}</b>
"""

    if last_listings:
        text += "\n<b>Последние объявления:</b>\n"
        for lid, title, price, seller_id in last_listings:
            text += f"• #{lid} — <b>{html.escape(_short(title, 45))}</b> / {price}₽ / <code>{seller_id}</code>\n"

    if last_orders:
        text += "\n<b>Последние заказы:</b>\n"
        for oid, title, budget, customer_id in last_orders:
            text += f"• #{oid} — <b>{html.escape(_short(title, 45))}</b> / {budget}₽ / <code>{customer_id}</code>\n"

    await show_screen(call, text, reply_markup=admin_back_moderation_keyboard(), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_mod_listings")
async def admin_mod_listings(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    with db() as conn:
        rows = conn.execute("""
            SELECT id, title, category, item_type, price, seller_id, created_at
            FROM listings
            WHERE status='moderation'
            ORDER BY id DESC
            LIMIT 15
        """).fetchall()

    if not rows:
        await show_screen(call, "📦 Объявлений на модерации нет.", reply_markup=admin_back_moderation_keyboard())
        await call.answer()
        return

    text = "━━━━━━━━━━━━━━\n📦 <b>Объявления на модерации</b>\n━━━━━━━━━━━━━━\n\n"
    buttons = []
    for lid, title, category, item_type, price, seller_id, created_at in rows:
        text += (
            f"<b>#{lid}</b> • {html.escape(_short(title, 55))}\n"
            f"💰 {price}₽ | {html.escape(category or '—')} | {html.escape(item_type or '—')}\n"
            f"👤 <code>{seller_id}</code> | {html.escape(str(created_at or '')[:16])}\n\n"
        )
        buttons.append([InlineKeyboardButton(text=f"🔎 Проверить объявление #{lid}", callback_data=f"admin_mod_listing:{lid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Центр модерации", callback_data="admin_moderation")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_mod_listing:"))
async def admin_mod_listing(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    listing_id = int(call.data.split(":", 1)[1])
    with db() as conn:
        row = conn.execute("""
            SELECT id, seller_id, title, category, item_type, price, description, COALESCE(delivery_time, ''), created_at
            FROM listings
            WHERE id=?
        """, (listing_id,)).fetchone()

    if not row:
        await call.answer("Объявление не найдено", show_alert=True)
        return

    lid, seller_id, title, category, item_type, price, description, delivery_time, created_at = row
    ok, reason = moderation_check(f"{title}\n{description}\n{delivery_time}", allow_contacts=False)
    auto_text = "✅ Автопроверка не нашла явных проблем" if ok else f"⚠️ Автопроверка: {html.escape(reason)}"

    await show_screen(call, f"""
━━━━━━━━━━━━━━
📦 <b>Проверка объявления #{lid}</b>
━━━━━━━━━━━━━━

👤 Продавец: {user_contact(seller_id)}
📅 Создано: <b>{html.escape(str(created_at or '')[:16])}</b>

<b>{html.escape(title or 'Без названия')}</b>

💰 Цена: <b>{price}₽</b>
📂 Категория: <b>{html.escape(category or '—')}</b>
📌 Формат: <b>{html.escape(item_type or '—')}</b>
⏳ Срок/получение: <b>{html.escape(delivery_time or '—')}</b>

<b>Описание:</b>
{html.escape(description or 'Без описания')}

🛡 {auto_text}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_approve_listing:{lid}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_listing:{lid}")],
        [InlineKeyboardButton(text="👤 Открыть продавца", callback_data=f"admin_user:{seller_id}"), InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin_ban_user:{seller_id}")],
        [InlineKeyboardButton(text="⬅️ К объявлениям", callback_data="admin_mod_listings")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_mod_orders")
async def admin_mod_orders(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    with db() as conn:
        rows = conn.execute("""
            SELECT id, title, category, budget, customer_id, COALESCE(deadline, ''), created_at
            FROM orders
            WHERE status='moderation'
            ORDER BY id DESC
            LIMIT 15
        """).fetchall()

    if not rows:
        await show_screen(call, "📌 Заказов на модерации нет.", reply_markup=admin_back_moderation_keyboard())
        await call.answer()
        return

    text = "━━━━━━━━━━━━━━\n📌 <b>Заказы на модерации</b>\n━━━━━━━━━━━━━━\n\n"
    buttons = []
    for oid, title, category, budget, customer_id, deadline, created_at in rows:
        text += (
            f"<b>#{oid}</b> • {html.escape(_short(title, 55))}\n"
            f"💰 {budget}₽ | {html.escape(category or '—')} | ⏳ {html.escape(deadline or '—')}\n"
            f"👤 <code>{customer_id}</code> | {html.escape(str(created_at or '')[:16])}\n\n"
        )
        buttons.append([InlineKeyboardButton(text=f"🔎 Проверить заказ #{oid}", callback_data=f"admin_mod_order:{oid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Центр модерации", callback_data="admin_moderation")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_mod_order:"))
async def admin_mod_order(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    order_id = int(call.data.split(":", 1)[1])
    with db() as conn:
        row = conn.execute("""
            SELECT id, customer_id, title, category, budget, COALESCE(deadline, ''), description, created_at
            FROM orders
            WHERE id=?
        """, (order_id,)).fetchone()

    if not row:
        await call.answer("Заказ не найден", show_alert=True)
        return

    oid, customer_id, title, category, budget, deadline, description, created_at = row
    ok, reason = moderation_check(f"{title}\n{description}\n{deadline}", allow_contacts=False)
    auto_text = "✅ Автопроверка не нашла явных проблем" if ok else f"⚠️ Автопроверка: {html.escape(reason)}"

    await show_screen(call, f"""
━━━━━━━━━━━━━━
📌 <b>Проверка заказа #{oid}</b>
━━━━━━━━━━━━━━

👤 Заказчик: {user_contact(customer_id)}
📅 Создано: <b>{html.escape(str(created_at or '')[:16])}</b>

<b>{html.escape(title or 'Без названия')}</b>

💰 Бюджет: <b>{budget}₽</b>
📂 Категория: <b>{html.escape(category or '—')}</b>
⏳ Срок: <b>{html.escape(deadline or '—')}</b>

<b>Описание:</b>
{html.escape(description or 'Без описания')}

🛡 {auto_text}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_approve_order:{oid}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_order:{oid}")],
        [InlineKeyboardButton(text="👤 Открыть заказчика", callback_data=f"admin_user:{customer_id}"), InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin_ban_user:{customer_id}")],
        [InlineKeyboardButton(text="⬅️ К заказам", callback_data="admin_mod_orders")],
    ]), parse_mode="HTML")
    await call.answer()

# ===== ЗАКАЗЫ =====

@dp.callback_query(F.data == "orders")
async def orders(call: CallbackQuery):
    await show_screen(call, 
        """
━━━━━━━━━━━━━━
📌 <b>Заказы</b>
━━━━━━━━━━━━━━

Здесь покупатели публикуют задания, а исполнители могут откликаться.

Например:
• нужен Telegram-бот
• нужен логотип
• нужен монтаж
• нужна аватарка
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Смотреть заказы", callback_data="orders_list")],
            [InlineKeyboardButton(text="📝 Создать заказ", callback_data="create_order")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")]
        ]),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "order_cancel")
async def order_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_screen(call, "❌ Создание заказа отменено.", reply_markup=main_menu(call.from_user.id), parse_mode="HTML")
    await call.answer()



def deadline_options_keyboard(prefix: str, cancel_callback: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚡ Срочно / сегодня", callback_data=f"{prefix}:Срочно / сегодня"),
            InlineKeyboardButton(text="1 день", callback_data=f"{prefix}:1 день"),
        ],
        [
            InlineKeyboardButton(text="2-3 дня", callback_data=f"{prefix}:2-3 дня"),
            InlineKeyboardButton(text="до 1 недели", callback_data=f"{prefix}:до 1 недели"),
        ],
        [
            InlineKeyboardButton(text="1-2 недели", callback_data=f"{prefix}:1-2 недели"),
            InlineKeyboardButton(text="до 1 месяца", callback_data=f"{prefix}:до 1 месяца"),
        ],
        [InlineKeyboardButton(text="🤝 По договорённости", callback_data=f"{prefix}:по договорённости")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_callback)],
    ])


def delivery_time_keyboard():
    return deadline_options_keyboard("listing_delivery", "listing_cancel")


def order_deadline_keyboard():
    return deadline_options_keyboard("order_deadline_pick", "order_cancel")


def order_cancel_keyboard(extra_rows=None):
    rows = extra_rows or []
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="order_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "create_order")
async def create_order(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(CreateOrder.category)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=cat, callback_data=f"order_cat:{cat}")]
            for cat in CATEGORIES
        ] + [[InlineKeyboardButton(text="❌ Отмена", callback_data="order_cancel")]]
    )

    await show_screen(call,
        """
━━━━━━━━━━━━━━
📌 <b>Создание заказа</b>
━━━━━━━━━━━━━━

Создайте задание, на которое смогут откликнуться исполнители.

<b>Шаг 1 из 5</b>
Выберите категорию:
""",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("order_cat:"))
async def order_category(call: CallbackQuery, state: FSMContext):
    category = call.data.split(":", 1)[1]
    await state.update_data(category=category)
    await state.set_state(CreateOrder.title)

    examples = ORDER_EXAMPLES.get(category, ["Нужна помощь с задачей", "Нужна цифровая услуга", "Нужен исполнитель"])
    examples_text = "\n".join(f"<code>{html.escape(example)}</code>" for example in examples)

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
📌 <b>Название заказа</b>
━━━━━━━━━━━━━━

📂 Категория: <b>{html.escape(category)}</b>

<b>Шаг 2 из 5</b>
Напишите коротко, что вам нужно.

Примеры под эту категорию:
{examples_text}
""",
        reply_markup=order_cancel_keyboard(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.message(CreateOrder.title)
async def order_title(message: Message, state: FSMContext):
    title = (message.text or "").strip()

    if len(title) < 3:
        await screen_answer(message, "Название слишком короткое.", reply_markup=order_cancel_keyboard())
        return
    ok, reason = moderation_check(title)
    if not ok:
        await screen_answer(message, f"🚫 Название заказа не прошло авто-модерацию: {html.escape(reason)}", reply_markup=order_cancel_keyboard(), parse_mode="HTML")
        await notify_admins(f"⚠️ <b>Авто-модерация заказа</b>\n\nПользователь: <code>{message.from_user.id}</code>\nПричина: {html.escape(reason)}\nТекст: {html.escape(title)}")
        return

    await state.update_data(title=title)
    await state.set_state(CreateOrder.budget)

    await screen_answer(message,
        f"""
━━━━━━━━━━━━━━
💰 <b>Бюджет</b>
━━━━━━━━━━━━━━

<b>Шаг 3 из 5</b>
Введите бюджет в рублях.

Лимит: от <b>{MIN_ORDER_BUDGET}₽</b> до <b>{MAX_ORDER_BUDGET}₽</b>.
Пример: <code>1500</code>
""",
        reply_markup=order_cancel_keyboard(),
        parse_mode="HTML"
    )


@dp.message(CreateOrder.budget)
async def order_budget(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await screen_answer(message, "Введите бюджет только числом.", reply_markup=order_cancel_keyboard())
        return

    budget = parse_money(message.text.strip())
    ok, reason = validate_order_budget(budget)
    if not ok:
        await screen_answer(message, f"🚫 Бюджет не подходит: {html.escape(reason)}", reply_markup=order_cancel_keyboard(), parse_mode="HTML")
        return

    await state.update_data(budget=budget)
    await state.set_state(CreateOrder.deadline)

    await screen_answer(message,
        """
━━━━━━━━━━━━━━
⏳ <b>Срок выполнения</b>
━━━━━━━━━━━━━━

<b>Шаг 4 из 5</b>
Выберите желаемый срок выполнения.
""",
        reply_markup=order_deadline_keyboard(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("order_deadline_pick:"))
async def order_deadline_pick(call: CallbackQuery, state: FSMContext):
    deadline = call.data.split(":", 1)[1]
    await state.update_data(deadline=deadline)
    await state.set_state(CreateOrder.description)

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
🧾 <b>Описание заказа</b>
━━━━━━━━━━━━━━

⏳ Срок: <b>{html.escape(deadline)}</b>

<b>Шаг 5 из 5</b>
Опишите задачу подробнее:
• что нужно сделать
• какие функции нужны
• примеры/пожелания
""",
        reply_markup=order_cancel_keyboard(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.message(CreateOrder.deadline)
async def order_deadline(message: Message, state: FSMContext):
    await screen_answer(
        message,
        "⏳ Срок нужно выбрать кнопкой ниже, а не писать текстом.",
        reply_markup=order_deadline_keyboard(),
        parse_mode="HTML"
    )

@dp.message(CreateOrder.description)
async def order_description(message: Message, state: FSMContext):
    description = (message.text or "").strip()
    if len(description) < 5:
        await screen_answer(message, "Описание слишком короткое. Напишите задачу подробнее.", reply_markup=order_cancel_keyboard())
        return
    ok, reason = moderation_check(description)
    if not ok:
        await screen_answer(message, f"🚫 Описание заказа не прошло авто-модерацию: {html.escape(reason)}", reply_markup=order_cancel_keyboard(), parse_mode="HTML")
        await notify_admins(f"⚠️ <b>Авто-модерация описания заказа</b>\n\nПользователь: <code>{message.from_user.id}</code>\nПричина: {html.escape(reason)}\nТекст: {html.escape(description)}")
        return

    await state.update_data(description=description)
    try:
        await message.delete()
    except Exception:
        pass

    data = await state.get_data()
    text, keyboard = build_order_preview(data)
    row = get_screen(message.from_user.id)
    if row:
        chat_id, message_id = row
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=keyboard, parse_mode="HTML")
            return
        except Exception:
            pass
    sent = await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    save_screen(message.from_user.id, sent.chat.id, sent.message_id)


def build_order_preview(data: dict):
    text = f"""
━━━━━━━━━━━━━━
👀 <b>Предпросмотр заказа</b>
━━━━━━━━━━━━━━

📌 <b>{html.escape(data.get('title', ''))}</b>

📂 Категория: <b>{html.escape(data.get('category', ''))}</b>
💰 Бюджет: <b>{data.get('budget', 0)}₽</b>
⏳ Срок: <b>{html.escape(data.get('deadline', ''))}</b>

🧾 <b>Описание:</b>
{html.escape(data.get('description', ''))}

Отправляем заказ на модерацию?
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить на модерацию", callback_data="order_publish")],
        [InlineKeyboardButton(text="✏️ Создать заново", callback_data="create_order")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="order_cancel")],
    ])
    return text, keyboard


@dp.callback_query(F.data == "order_publish")
async def order_publish(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    required = ["title", "category", "budget", "deadline", "description"]
    if any(k not in data for k in required):
        await call.answer("Данные заказа не найдены. Создайте заново.", show_alert=True)
        return

    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO orders (customer_id, title, category, budget, description, deadline, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            call.from_user.id,
            data["title"],
            data["category"],
            data["budget"],
            data["description"],
            data["deadline"],
            "moderation",
            datetime.now().isoformat()
        ))
        order_id = cur.lastrowid
        conn.commit()

    await state.clear()
    await notify_admins(f"""
📌 <b>Новый заказ на модерации</b>

ID: <code>{order_id}</code>
Заказчик: <code>{call.from_user.id}</code>

📌 {html.escape(data['title'])}
📂 {html.escape(data['category'])}
💰 {data['budget']}₽
⏳ {html.escape(data['deadline'])}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Одобрить заказ", callback_data=f"admin_approve_order:{order_id}")],
        [InlineKeyboardButton(text="❌ Отклонить заказ", callback_data=f"admin_reject_order:{order_id}")],
        [InlineKeyboardButton(text="👤 Заказчик", callback_data=f"admin_user:{call.from_user.id}")],
    ]))
    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
⏳ <b>Заказ отправлен на модерацию</b>
━━━━━━━━━━━━━━

📌 <b>{html.escape(data['title'])}</b>
📂 Категория: <b>{html.escape(data['category'])}</b>
💰 Бюджет: <b>{data['budget']}₽</b>
⏳ Срок: <b>{html.escape(data['deadline'])}</b>

После одобрения админом заказ появится в списке заказов.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Смотреть заказы", callback_data="orders_list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin_approve_listing:"))
async def admin_approve_listing(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    listing_id = int(call.data.split(":", 1)[1])
    with db() as conn:
        row = conn.execute("SELECT seller_id, title FROM listings WHERE id=?", (listing_id,)).fetchone()
        if not row:
            await call.answer("Объявление не найдено", show_alert=True)
            return
        seller_id, title = row
        conn.execute("UPDATE listings SET status='active' WHERE id=?", (listing_id,))
        conn.commit()
    try:
        await bot.send_message(seller_id, f"✅ Ваше объявление <b>{html.escape(title or '')}</b> прошло модерацию и опубликовано.", parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Объявление одобрено", show_alert=True)


@dp.callback_query(F.data.startswith("admin_reject_listing:"))
async def admin_reject_listing(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    listing_id = int(call.data.split(":", 1)[1])
    with db() as conn:
        row = conn.execute("SELECT seller_id, title FROM listings WHERE id=?", (listing_id,)).fetchone()
        if not row:
            await call.answer("Объявление не найдено", show_alert=True)
            return
        seller_id, title = row
        conn.execute("UPDATE listings SET status='rejected' WHERE id=?", (listing_id,))
        conn.commit()
    try:
        await bot.send_message(seller_id, f"❌ Ваше объявление <b>{html.escape(title or '')}</b> отклонено модерацией. Проверьте правила и создайте заново.", parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Объявление отклонено", show_alert=True)


@dp.callback_query(F.data.startswith("admin_approve_order:"))
async def admin_approve_order(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    order_id = int(call.data.split(":", 1)[1])
    with db() as conn:
        row = conn.execute("SELECT customer_id, title FROM orders WHERE id=?", (order_id,)).fetchone()
        if not row:
            await call.answer("Заказ не найден", show_alert=True)
            return
        customer_id, title = row
        conn.execute("UPDATE orders SET status='active' WHERE id=?", (order_id,))
        conn.commit()
    try:
        await bot.send_message(customer_id, f"✅ Ваш заказ <b>{html.escape(title or '')}</b> прошёл модерацию и опубликован.", parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Заказ одобрен", show_alert=True)


@dp.callback_query(F.data.startswith("admin_reject_order:"))
async def admin_reject_order(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    order_id = int(call.data.split(":", 1)[1])
    with db() as conn:
        row = conn.execute("SELECT customer_id, title FROM orders WHERE id=?", (order_id,)).fetchone()
        if not row:
            await call.answer("Заказ не найден", show_alert=True)
            return
        customer_id, title = row
        conn.execute("UPDATE orders SET status='rejected' WHERE id=?", (order_id,))
        conn.commit()
    try:
        await bot.send_message(customer_id, f"❌ Ваш заказ <b>{html.escape(title or '')}</b> отклонён модерацией. Проверьте правила и создайте заново.", parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Заказ отклонён", show_alert=True)


@dp.callback_query(F.data == "orders_list")
async def orders_list(call: CallbackQuery):
    with db() as conn:
        rows = conn.execute("""
        SELECT id, title, budget, COALESCE(deadline, 'Не указан'), category
        FROM orders
        WHERE status='active'
        ORDER BY id DESC
        LIMIT 20
        """).fetchall()

    if not rows:
        await show_screen(call, 
            "📌 Пока заказов нет.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📝 Создать заказ", callback_data="create_order")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")]
            ])
        )
        await call.answer()
        return

    buttons = []

    text = """
━━━━━━━━━━━━━━
📋 <b>Активные заказы</b>
━━━━━━━━━━━━━━

Выберите заказ:

"""
    for idx, (order_id, title, budget, deadline, category) in enumerate(rows[:10], start=1):
        text += f"<b>{idx}.</b> {html.escape(title or 'Без названия')}\n📂 {html.escape(category or '—')} • 💰 <b>{budget}₽</b> • ⏳ {html.escape(deadline or '—')}\n\n"
        buttons.append([
            InlineKeyboardButton(
                text=f"{idx}️⃣ Открыть заказ",
                callback_data=f"view_order:{order_id}"
            )
        ])

    buttons.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")])

    await show_screen(call, 
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("view_order:"))
async def view_order(call: CallbackQuery):
    order_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        row = conn.execute("""
        SELECT customer_id, title, category, budget, description, COALESCE(deadline, 'Не указан'), status
        FROM orders
        WHERE id=?
        """, (order_id,)).fetchone()

    if not row:
        await call.answer("Заказ не найден", show_alert=True)
        return

    customer_id, title, category, budget, description, deadline, order_status = row
    is_owner = user_id == customer_id

    buttons = []
    if not is_owner:
        buttons.append([InlineKeyboardButton(text="✋ Откликнуться", callback_data=f"order_apply:{order_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="📨 Смотреть отклики", callback_data=f"order_apps:{order_id}")])

    buttons.append([InlineKeyboardButton(text="🚨 Пожаловаться", callback_data=f"report_order:{order_id}")])
    if is_admin(user_id):
        buttons.append([InlineKeyboardButton(text="👤 Админ: заказчик", callback_data=f"admin_user:{customer_id}")])
        buttons.append([InlineKeyboardButton(text="👀 Админ: читать чат заказа", callback_data=f"admin_order_chat:{order_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ К заказам", callback_data="orders_list")])

    owner_note = "Это ваш заказ. Исполнители смогут откликнуться и написать вам через LTeam." if is_owner else "Контакты заказчика не раскрываются. Общайтесь только через LTeam."

    await show_screen(call, 
        f"""
━━━━━━━━━━━━━━
📌 <b>Заказ #{order_id}</b>
━━━━━━━━━━━━━━

📌 <b>{html.escape(title or 'Без названия')}</b>

📂 Категория: <b>{html.escape(category or '—')}</b>
💰 Бюджет: <b>{budget}₽</b>
⏳ Срок: <b>{html.escape(deadline or 'Не указан')}</b>
🚦 Статус: <b>{html.escape(order_status or 'active')}</b>

🧾 <b>Описание:</b>
{html.escape(description or 'Без описания')}

🛡 {owner_note}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("order_owner_hint:"))
async def order_owner_hint(call: CallbackQuery):
    await call.answer("Когда исполнитель откликнется, вам придёт уведомление с кнопкой ответа.", show_alert=True)




@dp.callback_query(F.data.startswith("order_apply:"))
async def order_apply_start(call: CallbackQuery, state: FSMContext):
    order_id = int(call.data.split(":")[1])
    executor_id = call.from_user.id

    with db() as conn:
        row = conn.execute(
            "SELECT customer_id, title, budget FROM orders WHERE id=? AND status='active'",
            (order_id,)
        ).fetchone()

        if not row:
            await call.answer("Заказ не найден", show_alert=True)
            return

        customer_id, title, budget = row

        if customer_id == executor_id:
            await call.answer("Нельзя откликнуться на свой заказ", show_alert=True)
            return

        old_app = conn.execute(
            "SELECT status FROM order_applications WHERE order_id=? AND executor_id=?",
            (order_id, executor_id),
        ).fetchone()

    if old_app and old_app[0] in ("new", "accepted"):
        await call.answer("Вы уже откликались на этот заказ", show_alert=True)
        return

    await state.clear()
    await state.update_data(order_id=order_id, customer_id=customer_id, order_title=title, order_budget=budget)
    await state.set_state(OrderResponseState.price)

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
✋ <b>Отклик на заказ #{order_id}</b>
━━━━━━━━━━━━━━

📌 <b>{html.escape(title or 'Без названия')}</b>
💰 Бюджет заказчика: <b>{budget}₽</b>

<b>Шаг 1 из 3</b>
Введите вашу цену числом.

Лимит: от <b>{MIN_ORDER_BUDGET}₽</b> до <b>{MAX_APPLICATION_PRICE}₽</b>.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"view_order:{order_id}")]
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(OrderResponseState.price)
async def order_apply_price(message: Message, state: FSMContext):
    data = await state.get_data()
    order_budget = int(data.get("order_budget") or 0)
    price = parse_money(message.text or "")

    if price is None:
        await screen_answer(message, "Введите цену только числом. Например: <code>1500</code>", parse_mode="HTML")
        return

    ok, reason = validate_application_price(price, order_budget)
    if not ok:
        await screen_answer(message, f"🚫 Цена не подходит: {html.escape(reason)}", parse_mode="HTML")
        return

    await state.update_data(price=price)
    await state.set_state(OrderResponseState.deadline)
    await screen_answer(
        message,
        """
━━━━━━━━━━━━━━
⏳ <b>Срок выполнения</b>
━━━━━━━━━━━━━━

<b>Шаг 2 из 3</b>
Напишите срок выполнения.

Пример: <code>2 дня</code>, <code>до пятницы</code>, <code>3-5 дней</code>
""",
        parse_mode="HTML",
    )


@dp.message(OrderResponseState.deadline)
async def order_apply_deadline(message: Message, state: FSMContext):
    deadline = (message.text or "").strip()
    if len(deadline) < 2 or len(deadline) > 80:
        await screen_answer(message, "Срок должен быть понятным и не слишком длинным. Например: <code>2 дня</code>", parse_mode="HTML")
        return

    ok, reason = moderation_check(deadline, allow_contacts=False)
    if not ok:
        await screen_answer(message, f"🚫 Срок не прошёл авто-модерацию: {html.escape(reason)}", parse_mode="HTML")
        return

    await state.update_data(deadline=deadline)
    await state.set_state(OrderResponseState.text)
    await screen_answer(
        message,
        """
━━━━━━━━━━━━━━
💬 <b>Комментарий</b>
━━━━━━━━━━━━━━

<b>Шаг 3 из 3</b>
Напишите коротко, почему заказчику стоит выбрать вас.

Нельзя указывать контакты, просить оплату напрямую или уводить в личку.
""",
        parse_mode="HTML",
    )


@dp.message(OrderResponseState.text)
async def order_apply_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = int(data.get("order_id") or 0)
    customer_id = int(data.get("customer_id") or 0)
    title = data.get("order_title") or "Без названия"
    price = int(data.get("price") or 0)
    deadline = data.get("deadline") or "Не указан"
    comment = (message.text or "").strip()
    executor_id = message.from_user.id
    username = message.from_user.username or "нет username"

    if len(comment) < 5:
        await screen_answer(message, "Комментарий слишком короткий. Напишите хотя бы пару слов.")
        return

    ok, reason = order_chat_moderation(comment, int(data.get("order_budget") or 0))
    if not ok:
        await screen_answer(message, f"🚫 Комментарий не прошёл авто-модерацию: {html.escape(reason)}", parse_mode="HTML")
        await notify_admins(f"⚠️ <b>Авто-модерация отклика</b>\n\nЗаказ: <code>#{order_id}</code>\nПользователь: <code>{executor_id}</code>\nПричина: {html.escape(reason)}\nТекст: {html.escape(comment[:800])}")
        return

    with db() as conn:
        row = conn.execute("SELECT customer_id FROM orders WHERE id=? AND status='active'", (order_id,)).fetchone()
        if not row or int(row[0]) != customer_id:
            await state.clear()
            await screen_answer(message, "❌ Заказ уже недоступен.", reply_markup=back_home())
            return

        conn.execute(
            """
            INSERT INTO order_applications (order_id, executor_id, customer_id, price, deadline, comment, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id, executor_id) DO UPDATE SET
                price=excluded.price,
                deadline=excluded.deadline,
                comment=excluded.comment,
                status='new',
                updated_at=excluded.updated_at
            """,
            (order_id, executor_id, customer_id, price, deadline, comment, "new", datetime.now().isoformat(), datetime.now().isoformat())
        )
        conn.commit()

    await state.clear()

    await bot.send_message(
        customer_id,
        f"""
━━━━━━━━━━━━━━
✋ <b>Новый отклик на заказ</b>
━━━━━━━━━━━━━━

📌 Заказ: <b>{html.escape(title)}</b>
ID заказа: <b>#{order_id}</b>

👤 <b>Исполнитель</b>
ID: <code>{executor_id}</code>
Username: @{html.escape(username)}
⭐ Рейтинг: <b>{seller_stats(executor_id)['rating_text']}</b>
🏷 Статус: <b>{user_public_status(executor_id)}</b>

💰 Цена: <b>{price}₽</b>
⏳ Срок: <b>{html.escape(deadline)}</b>

💬 <b>Комментарий:</b>
{html.escape(comment)}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📨 Все отклики", callback_data=f"order_apps:{order_id}")],
            [InlineKeyboardButton(text="💬 Ответить", callback_data=f"order_chat:{order_id}:{executor_id}")],
            [InlineKeyboardButton(text="📌 Открыть заказ", callback_data=f"view_order:{order_id}")],
        ]),
        parse_mode="HTML"
    )

    await notify_admins(f"""
✋ <b>Новый отклик на заказ</b>

Заказ: <code>#{order_id}</code>
Исполнитель: <code>{executor_id}</code>
Заказчик: <code>{customer_id}</code>
Цена: <b>{price}₽</b>
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Открыть заказ", callback_data=f"view_order:{order_id}")],
        [InlineKeyboardButton(text="👤 Исполнитель", callback_data=f"admin_user:{executor_id}")],
    ]))

    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
✅ <b>Отклик отправлен</b>
━━━━━━━━━━━━━━

💰 Цена: <b>{price}₽</b>
⏳ Срок: <b>{html.escape(deadline)}</b>

Заказчик получил уведомление.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать заказчику", callback_data=f"order_chat:{order_id}:{customer_id}")],
            [InlineKeyboardButton(text="⬅️ К заказу", callback_data=f"view_order:{order_id}")],
        ]),
        parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("order_apps:"))
async def order_apps(call: CallbackQuery):
    order_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        order = conn.execute("SELECT customer_id, title FROM orders WHERE id=?", (order_id,)).fetchone()
        if not order:
            await call.answer("Заказ не найден", show_alert=True)
            return
        customer_id, title = order
        if user_id != customer_id and not is_admin(user_id):
            await call.answer("Отклики видит только заказчик", show_alert=True)
            return

        apps = conn.execute(
            """
            SELECT id, executor_id, price, COALESCE(deadline,''), COALESCE(comment,''), status, created_at
            FROM order_applications
            WHERE order_id=?
            ORDER BY id DESC
            """,
            (order_id,),
        ).fetchall()

    if not apps:
        await show_screen(call, "📨 Пока нет откликов на этот заказ.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К заказу", callback_data=f"view_order:{order_id}")]
        ]))
        await call.answer()
        return

    lines = [f"━━━━━━━━━━━━━━\n📨 <b>Отклики на заказ #{order_id}</b>\n━━━━━━━━━━━━━━\n", f"📌 <b>{html.escape(title or 'Без названия')}</b>\n"]
    buttons = []
    for idx, (app_id, executor_id, price, deadline, comment, status, created_at) in enumerate(apps[:10], start=1):
        lines.append(
            f"<b>{idx}. {user_public_status(executor_id)}</b>\n"
            f"👤 ID: <code>{executor_id}</code>\n"
            f"💰 <b>{price}₽</b> • ⏳ {html.escape(deadline or '—')} • 🚦 {html.escape(status or 'new')}\n"
            f"💬 {html.escape((comment or '')[:120])}\n"
        )
        buttons.append([InlineKeyboardButton(text=f"{idx}️⃣ Открыть отклик", callback_data=f"view_app:{app_id}")])

    buttons.append([InlineKeyboardButton(text="⬅️ К заказу", callback_data=f"view_order:{order_id}")])
    await show_screen(call, "\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("view_app:"))
async def view_app(call: CallbackQuery):
    app_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        app = conn.execute(
            """
            SELECT a.order_id, a.executor_id, a.customer_id, a.price, COALESCE(a.deadline,''), COALESCE(a.comment,''), a.status, o.title
            FROM order_applications a
            JOIN orders o ON o.id=a.order_id
            WHERE a.id=?
            """,
            (app_id,),
        ).fetchone()

    if not app:
        await call.answer("Отклик не найден", show_alert=True)
        return

    order_id, executor_id, customer_id, price, deadline, comment, status, title = app
    if user_id != customer_id and user_id != executor_id and not is_admin(user_id):
        await call.answer("Нет доступа", show_alert=True)
        return

    buttons = []
    if user_id == customer_id or is_admin(user_id):
        if status != "accepted":
            buttons.append([InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_app:{app_id}")])
        if status not in ("rejected", "accepted"):
            buttons.append([InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_app:{app_id}")])
        buttons.append([InlineKeyboardButton(text="💬 Написать исполнителю", callback_data=f"order_chat:{order_id}:{executor_id}")])
    elif user_id == executor_id:
        buttons.append([InlineKeyboardButton(text="💬 Написать заказчику", callback_data=f"order_chat:{order_id}:{customer_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Все отклики", callback_data=f"order_apps:{order_id}")])

    await show_screen(call, f"""
━━━━━━━━━━━━━━
📨 <b>Отклик #{app_id}</b>
━━━━━━━━━━━━━━

📌 Заказ: <b>{html.escape(title or 'Без названия')}</b>
👤 Исполнитель: <code>{executor_id}</code>
🏷 Статус: <b>{user_public_status(executor_id)}</b>
⭐ Рейтинг: <b>{seller_stats(executor_id)['rating_text']}</b>

💰 Цена: <b>{price}₽</b>
⏳ Срок: <b>{html.escape(deadline or '—')}</b>
🚦 Статус отклика: <b>{html.escape(status or 'new')}</b>

💬 <b>Комментарий:</b>
{html.escape(comment or 'Без комментария')}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("reject_app:"))
async def reject_app(call: CallbackQuery):
    app_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        app = conn.execute("SELECT order_id, executor_id, customer_id, status FROM order_applications WHERE id=?", (app_id,)).fetchone()
        if not app:
            await call.answer("Отклик не найден", show_alert=True)
            return
        order_id, executor_id, customer_id, status = app
        if user_id != customer_id and not is_admin(user_id):
            await call.answer("Нет доступа", show_alert=True)
            return
        conn.execute("UPDATE order_applications SET status='rejected', updated_at=? WHERE id=?", (datetime.now().isoformat(), app_id))
        conn.commit()

    try:
        await bot.send_message(executor_id, f"❌ Ваш отклик на заказ #{order_id} отклонён.")
    except Exception:
        pass
    await show_screen(call, "✅ Отклик отклонён.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Все отклики", callback_data=f"order_apps:{order_id}")]
    ]))
    await call.answer()


@dp.callback_query(F.data.startswith("accept_app:"))
async def accept_app(call: CallbackQuery):
    app_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        app = conn.execute(
            """
            SELECT a.order_id, a.executor_id, a.customer_id, a.price, o.title
            FROM order_applications a
            JOIN orders o ON o.id=a.order_id
            WHERE a.id=?
            """,
            (app_id,),
        ).fetchone()
        if not app:
            await call.answer("Отклик не найден", show_alert=True)
            return
        order_id, executor_id, customer_id, price, title = app
        if user_id != customer_id and not is_admin(user_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        conn.execute("UPDATE order_applications SET status='accepted', updated_at=? WHERE id=?", (datetime.now().isoformat(), app_id))
        conn.execute("UPDATE order_applications SET status='rejected', updated_at=? WHERE order_id=? AND id<>? AND status='new'", (datetime.now().isoformat(), order_id, app_id))
        conn.execute("UPDATE orders SET executor_id=?, status='in_work' WHERE id=?", (executor_id, order_id))

        commission = int(price * COMMISSION_PERCENT / 100)
        payout = price - commission
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO deals (listing_id, buyer_id, seller_id, amount, commission, payout, payment_method, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (0, customer_id, executor_id, price, commission, payout, "order", "waiting_payment", datetime.now().isoformat())
        )
        deal_id = cur.lastrowid
        conn.commit()

    for target, text in [
        (executor_id, f"🎉 Ваш отклик на заказ #{order_id} принят! Создана сделка #{deal_id}."),
        (customer_id, f"✅ Вы приняли отклик. Создана сделка #{deal_id}. Оплатите через гаранта LTeam."),
    ]:
        try:
            await bot.send_message(target, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💬 Чат заказа", callback_data=f"order_chat:{order_id}:{executor_id if target == customer_id else customer_id}")],
                [InlineKeyboardButton(text="💬 Мои сделки", callback_data="my_deals")],
            ]))
        except Exception:
            pass

    await notify_admins(f"""
✅ <b>Отклик принят</b>

Заказ: <code>#{order_id}</code>
Сделка: <code>#{deal_id}</code>
Заказчик: <code>{customer_id}</code>
Исполнитель: <code>{executor_id}</code>
Сумма: <b>{price}₽</b>
""")

    await show_screen(call, f"✅ Исполнитель выбран. Создана сделка <b>#{deal_id}</b>.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Чат заказа", callback_data=f"order_chat:{order_id}:{executor_id}")],
        [InlineKeyboardButton(text="💬 Мои сделки", callback_data="my_deals")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("order_chat:"))
async def order_chat_start(call: CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    if len(parts) != 3:
        await call.answer("Ошибка чата заказа", show_alert=True)
        return

    order_id = int(parts[1])
    receiver_id = int(parts[2])
    sender_id = call.from_user.id

    with db() as conn:
        row = conn.execute(
            "SELECT customer_id, title, budget, status FROM orders WHERE id=?",
            (order_id,)
        ).fetchone()

        if not row:
            await call.answer("Заказ не найден", show_alert=True)
            return

        customer_id, title, budget, order_status = row

        app_row = conn.execute(
            """
            SELECT 1 FROM order_applications
            WHERE order_id=? AND customer_id=? AND executor_id=? AND status IN ('new','accepted')
            """,
            (order_id, customer_id, receiver_id if sender_id == customer_id else sender_id),
        ).fetchone()

        allowed = (sender_id == customer_id and app_row is not None) or (receiver_id == customer_id and app_row is not None) or is_admin(sender_id)

    if not allowed:
        await call.answer("Чат открывается только после отклика исполнителя.", show_alert=True)
        return

    if sender_id == receiver_id:
        await call.answer("Нельзя писать самому себе", show_alert=True)
        return

    await state.update_data(order_id=order_id, receiver_id=receiver_id, order_budget=budget)
    await state.set_state(OrderChatState.text)

    history_rows = get_order_chat_history(order_id, limit=8)
    history_text = format_chat_history(history_rows, current_user_id=sender_id, limit_note="последние 8")

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
💬 <b>Безопасный чат заказа #{order_id}</b>
━━━━━━━━━━━━━━

📌 <b>{html.escape(title or 'Без названия')}</b>
💰 Бюджет: <b>{budget}₽</b>

<b>История:</b>
{history_text}

🛡 <b>LTeam Protect включён</b>
• нельзя отправлять контакты
• нельзя уводить в личку
• нельзя просить оплату напрямую
• подозрительные суммы блокируются

Напишите сообщение одним текстом.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Открыть заказ", callback_data=f"view_order:{order_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(OrderChatState.text)
async def order_chat_send(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    receiver_id = data.get("receiver_id")

    if not order_id or not receiver_id:
        await state.clear()
        await screen_answer(message, "❌ Чат заказа не найден. Откройте заказ заново.", reply_markup=back_home())
        return

    text = (message.text or "").strip()
    if not text:
        await screen_answer(message, "Отправьте текстовое сообщение.", reply_markup=back_home())
        return

    with db() as conn:
        row = conn.execute("SELECT title, budget FROM orders WHERE id=?", (order_id,)).fetchone()
        title = row[0] if row else "Без названия"
        budget = int(row[1]) if row else 0

    ok, reason = order_chat_moderation(text, budget)
    if not ok:
        await notify_admins(f"""
🛡 <b>LTeam Protect заблокировал сообщение</b>

Заказ: <code>#{order_id}</code>
Пользователь: <code>{message.from_user.id}</code>
Причина: {html.escape(reason)}

Текст:
{html.escape(text[:800])}
""")
        await screen_answer(
            message,
            f"🚫 Сообщение не отправлено: {html.escape(reason)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💬 Написать заново", callback_data=f"order_chat:{order_id}:{receiver_id}")],
                [InlineKeyboardButton(text="📌 Открыть заказ", callback_data=f"view_order:{order_id}")],
            ]),
            parse_mode="HTML",
        )
        return

    with db() as conn:
        conn.execute(
            "INSERT INTO order_messages (order_id, sender_id, receiver_id, text, created_at) VALUES (?, ?, ?, ?, ?)",
            (order_id, message.from_user.id, receiver_id, text, datetime.now().isoformat()),
        )
        conn.commit()

    await bot.send_message(
        receiver_id,
        f"""
━━━━━━━━━━━━━━
💬 <b>Сообщение по заказу #{order_id}</b>
━━━━━━━━━━━━━━

📌 <b>{html.escape(title or 'Без названия')}</b>

{html.escape(text)}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩️ Ответить через LTeam", callback_data=f"order_chat:{order_id}:{message.from_user.id}")],
            [InlineKeyboardButton(text="📌 Открыть заказ", callback_data=f"view_order:{order_id}")],
        ]),
        parse_mode="HTML",
    )

    await state.clear()
    await screen_answer(
        message,
        "✅ Сообщение отправлено через безопасный чат LTeam.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать ещё", callback_data=f"order_chat:{order_id}:{receiver_id}")],
            [InlineKeyboardButton(text="📌 Открыть заказ", callback_data=f"view_order:{order_id}")],
        ]),
    )


# ===== ЖАЛОБЫ НА ЗАКАЗЫ / ПОЛЬЗОВАТЕЛЕЙ =====

@dp.callback_query(F.data.startswith("report_order:"))
async def report_order_start(call: CallbackQuery, state: FSMContext):
    order_id = int(call.data.split(":")[1])
    await state.update_data(target_type="order", target_id=order_id)
    await state.set_state(ReportState.reason)
    await show_screen(call, "🚨 Опишите причину жалобы на заказ:")
    await call.answer()

@dp.callback_query(F.data.startswith("report_user:"))
async def report_user_start(call: CallbackQuery, state: FSMContext):
    user_id = int(call.data.split(":")[1])
    await state.update_data(target_type="user", target_id=user_id)
    await state.set_state(ReportState.reason)
    await show_screen(call, "🚨 Опишите причину жалобы на пользователя:")
    await call.answer()

# ===== АДМИНКА 2.0 =====

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    with db() as conn:
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        listings_count = conn.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
        orders_count = conn.execute("SELECT COUNT(*) FROM orders WHERE status='active'").fetchone()[0]
        deals_count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        pending_receipts = conn.execute("SELECT COUNT(*) FROM deals WHERE status='waiting_admin_confirm'").fetchone()[0]
        reports_count = conn.execute("SELECT COUNT(*) FROM reports WHERE COALESCE(status, 'new')='new'").fetchone()[0]
        tickets_count = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
        commission = conn.execute("SELECT COALESCE(SUM(commission), 0) FROM deals WHERE status='completed'").fetchone()[0]
    await show_screen(call, f"""
━━━━━━━━━━━━━━
📊 <b>Статистика LTeam</b>
━━━━━━━━━━━━━━

👥 Пользователей: <b>{users_count}</b>
📦 Активных объявлений: <b>{listings_count}</b>
📋 Активных заказов: <b>{orders_count}</b>
💰 Сделок всего: <b>{deals_count}</b>
🧾 Чеков на проверке: <b>{pending_receipts}</b>
🚨 Новых жалоб: <b>{reports_count}</b>
🆘 Открытых обращений: <b>{tickets_count}</b>
💵 Комиссия LTeam: <b>{commission}₽</b>
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "admin_find_user")
async def admin_find_user(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminSearchUserState.user_id)
    await show_screen(call, "👥 Введите Telegram ID пользователя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]
    ]))
    await call.answer()

@dp.message(AdminSearchUserState.user_id)
async def admin_find_user_result(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    value = (message.text or "").strip()
    if not value.isdigit():
        await screen_answer(message, "Введите только числовой ID."); return
    await state.clear()
    await screen_answer(message, "Откройте профиль пользователя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👤 Пользователь {value}", callback_data=f"admin_user:{value}")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]))

@dp.callback_query(F.data == "admin_reports")
async def admin_reports(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    with db() as conn:
        rows = conn.execute("""
        SELECT id, user_id, COALESCE(target_type, 'listing'), COALESCE(target_id, listing_id), reason
        FROM reports
        WHERE COALESCE(status, 'new')='new'
        ORDER BY id DESC LIMIT 10
        """).fetchall()
    if not rows:
        await show_screen(call, "🚨 Новых жалоб нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]]))
        await call.answer(); return
    text = "━━━━━━━━━━━━━━\n🚨 <b>Новые жалобы</b>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for rid, uid, target_type, target_id, reason in rows:
        text += f"<b>#{rid}</b> • {html.escape(target_type or '—')} <code>{target_id}</code>\n👤 <code>{uid}</code>\n{html.escape((reason or '')[:120])}\n\n"
        buttons.append([InlineKeyboardButton(text=f"✅ Закрыть жалобу #{rid}", callback_data=f"admin_close_report:{rid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_close_report:"))
async def admin_close_report(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    report_id = int(call.data.split(":")[1])
    with db() as conn:
        conn.execute("UPDATE reports SET status='closed' WHERE id=?", (report_id,)); conn.commit()
    await call.answer("Жалоба закрыта", show_alert=True)
    call.data = "admin_reports"
    await admin_reports(call)

@dp.callback_query(F.data == "admin_broadcast_start")
async def admin_broadcast_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    await state.set_state(BroadcastState.text)
    await show_screen(call, "📢 Введите текст рассылки всем пользователям:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]]))
    await call.answer()

@dp.message(BroadcastState.text)
async def admin_broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    target = data.get("broadcast_target", "all")
    text = (message.text or "").strip()
    if len(text) < 3:
        await screen_answer(message, "Текст слишком короткий.")
        return
    await state.update_data(broadcast_target=target, broadcast_text=text)
    try:
        sql, manual = broadcast_target_sql(target)
        if sql is None:
            total = len(manual)
        else:
            with db() as conn:
                total = len([r[0] for r in conn.execute(sql).fetchall()])
    except NameError:
        # Если админ открыл старую рассылку, но новые функции ниже ещё не загружены — fallback всем.
        with db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    target_name = broadcast_target_name(target) if "broadcast_target_name" in globals() else "всем пользователям"
    await screen_answer(message, f"""
━━━━━━━━━━━━━━
📢 <b>Предпросмотр рассылки</b>
━━━━━━━━━━━━━━

🎯 Аудитория: <b>{target_name}</b>
👥 Получателей: <b>{total}</b>

<b>Текст:</b>
{html.escape(text)}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="admin_broadcast_send_v2"), InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_target")],
    ]), parse_mode="HTML")



# ===== АДМИНКА: ПОЛЬЗОВАТЕЛИ — ДЕТАЛИ, ПРЕДУПРЕЖДЕНИЯ, СООБЩЕНИЯ =====

@dp.callback_query(F.data.startswith("admin_user_orders:"))
async def admin_user_orders(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    with db() as conn:
        rows = conn.execute("SELECT id, title, budget, status, deadline, created_at FROM orders WHERE customer_id=? ORDER BY id DESC LIMIT 25", (user_id,)).fetchall()
    if not rows:
        await show_screen(call, "📌 У пользователя нет заказов.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]))
        await call.answer(); return
    text = f"━━━━━━━━━━━━━━\n📌 <b>Заказы пользователя</b> <code>{user_id}</code>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for oid, title, budget, status, deadline, created_at in rows:
        text += f"<b>#{oid}</b> • {html.escape(title or 'Без названия')}\n💰 {budget}₽ • ⏳ {html.escape(deadline or '—')} • <b>{html.escape(status or '—')}</b>\n<code>{html.escape(str(created_at)[:16])}</code>\n\n"
        buttons.append([InlineKeyboardButton(text=f"#{oid} • {html.escape((title or 'Без названия')[:28])}", callback_data=f"view_order:{oid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_user_deals:"))
async def admin_user_deals(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    with db() as conn:
        rows = conn.execute("""
            SELECT id, listing_id, buyer_id, seller_id, amount, commission, payout, status, created_at
            FROM deals
            WHERE buyer_id=? OR seller_id=?
            ORDER BY id DESC LIMIT 25
        """, (user_id, user_id)).fetchall()
    if not rows:
        await show_screen(call, "💼 У пользователя нет сделок.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]))
        await call.answer(); return
    text = f"━━━━━━━━━━━━━━\n💼 <b>Сделки пользователя</b> <code>{user_id}</code>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for did, listing_id, buyer_id, seller_id, amount, commission, payout, status, created_at in rows:
        role = "покупатель" if int(buyer_id) == user_id else "исполнитель"
        text += f"<b>#{did}</b> • роль: <b>{role}</b> • <b>{html.escape(status or '—')}</b>\n💰 {amount}₽ • комиссия {commission}₽ • выплата {payout}₽\n👤 buyer <code>{buyer_id}</code> / seller <code>{seller_id}</code>\n<code>{html.escape(str(created_at)[:16])}</code>\n\n"
        buttons.append([InlineKeyboardButton(text=f"💼 Сделка #{did}", callback_data=f"deal:{did}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_user_reports:"))
async def admin_user_reports(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    with db() as conn:
        rows = conn.execute("""
            SELECT id, user_id, COALESCE(target_type,'listing'), COALESCE(target_id, listing_id), reason, COALESCE(status,'new'), created_at
            FROM reports
            WHERE user_id=? OR listing_id IN (SELECT id FROM listings WHERE seller_id=?)
               OR (COALESCE(target_type,'')='order' AND target_id IN (SELECT id FROM orders WHERE customer_id=?))
            ORDER BY id DESC LIMIT 25
        """, (user_id, user_id, user_id)).fetchall()
    if not rows:
        await show_screen(call, "🚨 Жалоб по пользователю нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]))
        await call.answer(); return
    text = f"━━━━━━━━━━━━━━\n🚨 <b>Жалобы по пользователю</b> <code>{user_id}</code>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for rid, reporter_id, target_type, target_id, reason, status, created_at in rows:
        text += f"<b>#{rid}</b> • {html.escape(target_type or '—')} <code>{target_id}</code> • <b>{html.escape(status or '—')}</b>\nОт: <code>{reporter_id}</code> • <code>{html.escape(str(created_at)[:16])}</code>\n{html.escape((reason or '')[:300])}\n\n"
        if status != "closed":
            buttons.append([InlineKeyboardButton(text=f"✅ Закрыть жалобу #{rid}", callback_data=f"admin_close_report:{rid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_user_warnings:"))
async def admin_user_warnings(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    with db() as conn:
        rows = conn.execute("SELECT id, admin_id, reason, created_at FROM admin_warnings WHERE user_id=? ORDER BY id DESC LIMIT 20", (user_id,)).fetchall()
    text = f"━━━━━━━━━━━━━━\n⚠️ <b>Предупреждения</b> <code>{user_id}</code>\n━━━━━━━━━━━━━━\n\n"
    if not rows:
        text += "Предупреждений пока нет."
    else:
        for wid, admin_id, reason, created_at in rows:
            text += f"<b>#{wid}</b> • админ <code>{admin_id}</code> • <code>{html.escape(str(created_at)[:16])}</code>\n{html.escape(reason or '')}\n\n"
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⚠️ Выдать предупреждение", callback_data=f"admin_warn_user:{user_id}")], [InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_warn_user:"))
async def admin_warn_user_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    await state.update_data(admin_target_user_id=user_id)
    await state.set_state(AdminWarnState.reason)
    await show_screen(call, f"⚠️ Введите причину предупреждения для пользователя <code>{user_id}</code>.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]), parse_mode="HTML")
    await call.answer()

@dp.message(AdminWarnState.reason)
async def admin_warn_user_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    data = await state.get_data()
    user_id = int(data.get("admin_target_user_id") or 0)
    reason = (message.text or "").strip()
    if not user_id or len(reason) < 3:
        await screen_answer(message, "Причина слишком короткая."); return
    with db() as conn:
        conn.execute("INSERT INTO admin_warnings (user_id, admin_id, reason, created_at) VALUES (?, ?, ?, ?)", (user_id, message.from_user.id, reason, datetime.now().isoformat()))
        conn.commit()
    try:
        await bot.send_message(user_id, f"⚠️ <b>Предупреждение LTeam</b>\n\n{html.escape(reason)}", parse_mode="HTML")
    except Exception:
        pass
    await state.clear()
    await screen_answer(message, f"✅ Предупреждение выдано пользователю <code>{user_id}</code>.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]), parse_mode="HTML")
    await notify_admins(f"⚠️ <b>Выдано предупреждение</b>\n\nПользователь: <code>{user_id}</code>\nАдмин: <code>{message.from_user.id}</code>\nПричина:\n{html.escape(reason)}")

@dp.callback_query(F.data.startswith("admin_msg_user:"))
async def admin_msg_user_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    await state.update_data(admin_target_user_id=user_id)
    await state.set_state(AdminMessageState.text)
    await show_screen(call, f"✉️ Введите сообщение пользователю <code>{user_id}</code> от имени LTeam.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]), parse_mode="HTML")
    await call.answer()

@dp.message(AdminMessageState.text)
async def admin_msg_user_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    data = await state.get_data()
    user_id = int(data.get("admin_target_user_id") or 0)
    text = (message.text or "").strip()
    if not user_id or len(text) < 2:
        await screen_answer(message, "Сообщение слишком короткое."); return
    try:
        await bot.send_message(user_id, f"✉️ <b>Сообщение от LTeam</b>\n\n{html.escape(text)}", parse_mode="HTML")
        result = "✅ Сообщение отправлено."
    except Exception as e:
        result = f"❌ Не удалось отправить сообщение: <code>{html.escape(str(e))}</code>"
    await state.clear()
    await screen_answer(message, f"{result}\n\nПользователь: <code>{user_id}</code>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]), parse_mode="HTML")

@dp.callback_query(F.data.startswith("admin_listing_actions:"))
async def admin_listing_actions(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    _, listing_id_raw, user_id_raw = call.data.split(":")
    listing_id = int(listing_id_raw); user_id = int(user_id_raw)
    with db() as conn:
        row = conn.execute("SELECT id, seller_id, title, category, item_type, price, description, status, created_at FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not row:
        await call.answer("Объявление не найдено", show_alert=True); return
    lid, seller_id, title, category, item_type, price, description, status, created_at = row
    await show_screen(call, f"""
━━━━━━━━━━━━━━
📦 <b>Админ: объявление #{lid}</b>
━━━━━━━━━━━━━━

👤 Автор: <code>{seller_id}</code>
📌 Статус: <b>{html.escape(status or '—')}</b>
📅 Создано: <code>{html.escape(str(created_at)[:16])}</code>

<b>{html.escape(title or 'Без названия')}</b>
📂 {html.escape(category or '—')} • {html.escape(item_type or '—')}
💰 <b>{price}₽</b>

{html.escape((description or '')[:1200])}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ active", callback_data=f"admin_listing_status:{lid}:active:{user_id}"), InlineKeyboardButton(text="⏳ moderation", callback_data=f"admin_listing_status:{lid}:moderation:{user_id}")],
        [InlineKeyboardButton(text="🚫 blocked", callback_data=f"admin_listing_status:{lid}:blocked:{user_id}"), InlineKeyboardButton(text="🗄 archived", callback_data=f"admin_listing_status:{lid}:archived:{user_id}")],
        [InlineKeyboardButton(text="👤 Автор", callback_data=f"admin_user:{seller_id}"), InlineKeyboardButton(text="⬅️ Объявления", callback_data=f"admin_user_listings:{user_id}")],
    ]), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_listing_status:"))
async def admin_listing_status(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    _, listing_id_raw, status, user_id_raw = call.data.split(":")
    listing_id = int(listing_id_raw); user_id = int(user_id_raw)
    if status not in {"active", "moderation", "blocked", "archived", "rejected"}:
        await call.answer("Неверный статус", show_alert=True); return
    with db() as conn:
        row = conn.execute("SELECT seller_id, title FROM listings WHERE id=?", (listing_id,)).fetchone()
        conn.execute("UPDATE listings SET status=? WHERE id=?", (status, listing_id))
        conn.commit()
    if row:
        seller_id, title = row
        try:
            await bot.send_message(seller_id, f"📦 Статус объявления <b>{html.escape(title or 'Без названия')}</b> изменён: <b>{html.escape(status)}</b>", parse_mode="HTML")
        except Exception:
            pass
    await call.answer(f"Статус: {status}", show_alert=True)
    call.data = f"admin_listing_actions:{listing_id}:{user_id}"
    await admin_listing_actions(call)


@dp.callback_query(F.data.startswith("admin_order_chat:"))
async def admin_order_chat(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    order_id = int(call.data.split(":")[1])
    rows = get_order_chat_history(order_id, limit=30)
    history_text = format_chat_history(rows, current_user_id=None, limit_note="последние 30")

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
👀 <b>Чат заказа #{order_id}</b>
━━━━━━━━━━━━━━

{history_text}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 Открыть заказ", callback_data=f"view_order:{order_id}")],
            [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()

@dp.message(Command("order_chat"))
async def admin_order_chat_command(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await screen_answer(message, "Использование: <code>/order_chat 7</code>", parse_mode="HTML")
        return

    order_id = int(parts[1])
    rows = get_order_chat_history(order_id, limit=30)
    history_text = format_chat_history(rows, current_user_id=None, limit_note="последние 30")
    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
👀 <b>Чат заказа #{order_id}</b>
━━━━━━━━━━━━━━

{history_text}
""",
        parse_mode="HTML",
    )

# ===== АДМИНКА 3/4: ФИНАНСЫ, СДЕЛКИ, БЕЗОПАСНОСТЬ, РАССЫЛКИ =====

def ensure_admin_tables():
    """Дополнительные таблицы для усиленной админки. Безопасно вызывается много раз."""
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            admin_id INTEGER,
            text TEXT,
            created_at TEXT
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
        conn.commit()


def admin_only(call: CallbackQuery) -> bool:
    return is_admin(call.from_user.id)


def rub(value) -> str:
    try:
        return f"{int(value or 0)}₽"
    except Exception:
        return f"{value or 0}₽"


def deal_status_ru(status: str | None) -> str:
    return {
        "waiting_receipt": "🧾 ждёт чек",
        "waiting_admin_confirm": "🧾 чек на проверке",
        "in_work": "🔨 в работе",
        "waiting_buyer_confirm": "👤 ждёт подтверждения покупателя",
        "waiting_payout": "💸 ждёт выплаты",
        "completed": "✅ завершена",
        "cancelled": "❌ отменена",
        "deleted": "🗑 удалена",
        "frozen": "🧊 заморожена",
    }.get(status or "", status or "—")


def admin_deal_buttons(deal_id: int, status: str | None = None):
    rows = [
        [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}"), InlineKeyboardButton(text="👀 Чат", callback_data=f"admin_deal_chat:{deal_id}")],
    ]
    if status == "waiting_admin_confirm":
        rows.append([InlineKeyboardButton(text="✅ Подтвердить оплату", callback_data=f"admin_payment_ok_v2:{deal_id}"), InlineKeyboardButton(text="❌ Отклонить чек", callback_data=f"admin_payment_no_v2:{deal_id}")])
    if status == "waiting_payout":
        rows.append([InlineKeyboardButton(text="💸 Выплата сделана", callback_data=f"admin_payout_done_v2:{deal_id}")])
    if status not in ["completed", "cancelled", "deleted"]:
        rows.append([InlineKeyboardButton(text="🧊 Заморозить", callback_data=f"admin_deal_freeze:{deal_id}"), InlineKeyboardButton(text="❌ Отменить", callback_data=f"admin_deal_cancel:{deal_id}")])
    if status == "frozen":
        rows.append([InlineKeyboardButton(text="▶️ Вернуть в работу", callback_data=f"admin_deal_unfreeze:{deal_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Сделки", callback_data="admin_deals_center"), InlineKeyboardButton(text="⚙️ Админка", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "admin_finance_v2")
async def admin_finance_v2(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    ensure_admin_tables()
    with db() as conn:
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_deals = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*), COALESCE(SUM(amount),0), COALESCE(SUM(commission),0) FROM deals WHERE status='completed'").fetchone()
        waiting_payment = conn.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM deals WHERE status='waiting_admin_confirm'").fetchone()
        in_work = conn.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM deals WHERE status='in_work'").fetchone()
        waiting_payout = conn.execute("SELECT COUNT(*), COALESCE(SUM(payout),0), COALESCE(SUM(commission),0) FROM deals WHERE status='waiting_payout'").fetchone()
        promo_wait = conn.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM promo_payments WHERE status='waiting_admin_confirm'").fetchone()
        promo_done = conn.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM promo_payments WHERE status='approved'").fetchone()
    await show_screen(call, f"""
━━━━━━━━━━━━━━
💰 <b>Финансы LTeam 2.0</b>
━━━━━━━━━━━━━━

👥 Пользователей: <b>{total_users}</b>
🤝 Всего сделок: <b>{total_deals}</b>

🧾 <b>Оплаты</b>
• Чеков на проверке: <b>{waiting_payment[0]}</b> на <b>{rub(waiting_payment[1])}</b>
• В работе: <b>{in_work[0]}</b> на <b>{rub(in_work[1])}</b>

💸 <b>Выплаты</b>
• Ждут выплаты: <b>{waiting_payout[0]}</b> на <b>{rub(waiting_payout[1])}</b>
• Потенциальная комиссия: <b>{rub(waiting_payout[2])}</b>

✅ <b>Завершено</b>
• Сделок: <b>{completed[0]}</b>
• Оборот: <b>{rub(completed[1])}</b>
• Комиссия LTeam: <b>{rub(completed[2])}</b>

🚀 <b>Продвижение</b>
• На проверке: <b>{promo_wait[0]}</b> на <b>{rub(promo_wait[1])}</b>
• Подтверждено: <b>{promo_done[0]}</b> на <b>{rub(promo_done[1])}</b>
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧾 Оплаты на проверке", callback_data="admin_deals_payments"), InlineKeyboardButton(text="💸 Выплаты", callback_data="admin_deals_payouts")],
        [InlineKeyboardButton(text="🔨 В работе", callback_data="admin_deals_inwork"), InlineKeyboardButton(text="✅ Завершённые", callback_data="admin_deals_completed")],
        [InlineKeyboardButton(text="💰 Продвижение", callback_data="admin_promo_pending")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]), parse_mode="HTML")
    await call.answer()


async def show_admin_deals_list(call: CallbackQuery, title: str, status: str | None = None, limit: int = 10):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    where = ""
    params = []
    if status:
        where = "WHERE d.status=?"
        params.append(status)
    with db() as conn:
        rows = conn.execute(f"""
            SELECT d.id, d.buyer_id, d.seller_id, d.amount, d.commission, d.payout, d.status,
                   COALESCE(l.title, 'Заказ/услуга')
            FROM deals d
            LEFT JOIN listings l ON l.id=d.listing_id
            {where}
            ORDER BY d.id DESC
            LIMIT ?
        """, (*params, limit)).fetchall()
    if not rows:
        await show_screen(call, f"━━━━━━━━━━━━━━\n{title}\n━━━━━━━━━━━━━━\n\nПока пусто.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Сделки", callback_data="admin_deals_center")],
            [InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")],
        ]), parse_mode="HTML")
        await call.answer(); return
    text = f"━━━━━━━━━━━━━━\n{title}\n━━━━━━━━━━━━━━\n\n"
    buttons = []
    for deal_id, buyer_id, seller_id, amount, commission, payout, st, title_row in rows:
        text += f"<b>#{deal_id}</b> • {deal_status_ru(st)}\n📦 {html.escape(str(title_row)[:70])}\n👤 Покупатель: <code>{buyer_id}</code> • Исполнитель: <code>{seller_id}</code>\n💰 {rub(amount)} • комиссия {rub(commission)} • выплата {rub(payout)}\n\n"
        buttons.append([InlineKeyboardButton(text=f"Открыть #{deal_id}", callback_data=f"admin_deal_view_v2:{deal_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Сделки", callback_data="admin_deals_center"), InlineKeyboardButton(text="⚙️ Админка", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_deals_center")
async def admin_deals_center(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    with db() as conn:
        counts = {st: conn.execute("SELECT COUNT(*) FROM deals WHERE status=?", (st,)).fetchone()[0] for st in ["waiting_receipt", "waiting_admin_confirm", "in_work", "waiting_buyer_confirm", "waiting_payout", "completed", "frozen"]}
        active_total = conn.execute("SELECT COUNT(*) FROM deals WHERE status NOT IN ('completed','cancelled','deleted')").fetchone()[0]
    await show_screen(call, f"""
━━━━━━━━━━━━━━
🤝 <b>Центр сделок</b>
━━━━━━━━━━━━━━

Активных сделок: <b>{active_total}</b>

🧾 Ждут чек: <b>{counts['waiting_receipt']}</b>
🧾 Чек на проверке: <b>{counts['waiting_admin_confirm']}</b>
🔨 В работе: <b>{counts['in_work']}</b>
👤 Ждут покупателя: <b>{counts['waiting_buyer_confirm']}</b>
💸 Ждут выплаты: <b>{counts['waiting_payout']}</b>
🧊 Заморожены: <b>{counts['frozen']}</b>
✅ Завершены: <b>{counts['completed']}</b>
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🧾 Оплаты ({counts['waiting_admin_confirm']})", callback_data="admin_deals_payments"), InlineKeyboardButton(text=f"💸 Выплаты ({counts['waiting_payout']})", callback_data="admin_deals_payouts")],
        [InlineKeyboardButton(text=f"🔨 В работе ({counts['in_work']})", callback_data="admin_deals_inwork"), InlineKeyboardButton(text=f"👤 Подтверждение ({counts['waiting_buyer_confirm']})", callback_data="admin_deals_buyer_confirm")],
        [InlineKeyboardButton(text=f"🧊 Заморожены ({counts['frozen']})", callback_data="admin_deals_frozen"), InlineKeyboardButton(text="✅ Завершённые", callback_data="admin_deals_completed")],
        [InlineKeyboardButton(text="💰 Финансы", callback_data="admin_finance_v2")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_deals_payments")
async def admin_deals_payments(call: CallbackQuery):
    await show_admin_deals_list(call, "🧾 <b>Оплаты на проверке</b>", "waiting_admin_confirm")

@dp.callback_query(F.data == "admin_deals_payouts")
async def admin_deals_payouts(call: CallbackQuery):
    await show_admin_deals_list(call, "💸 <b>Ожидают выплаты</b>", "waiting_payout")

@dp.callback_query(F.data == "admin_deals_inwork")
async def admin_deals_inwork(call: CallbackQuery):
    await show_admin_deals_list(call, "🔨 <b>Сделки в работе</b>", "in_work")

@dp.callback_query(F.data == "admin_deals_buyer_confirm")
async def admin_deals_buyer_confirm(call: CallbackQuery):
    await show_admin_deals_list(call, "👤 <b>Ждут подтверждения покупателя</b>", "waiting_buyer_confirm")

@dp.callback_query(F.data == "admin_deals_completed")
async def admin_deals_completed(call: CallbackQuery):
    await show_admin_deals_list(call, "✅ <b>Завершённые сделки</b>", "completed")

@dp.callback_query(F.data == "admin_deals_frozen")
async def admin_deals_frozen(call: CallbackQuery):
    await show_admin_deals_list(call, "🧊 <b>Замороженные сделки</b>", "frozen")


@dp.callback_query(F.data.startswith("admin_deal_view_v2:"))
async def admin_deal_view_v2(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    deal_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("""
            SELECT d.id, d.listing_id, d.buyer_id, d.seller_id, d.amount, d.commission, d.payout,
                   d.payment_method, d.status, d.receipt, d.created_at, COALESCE(l.title, 'Заказ/услуга')
            FROM deals d
            LEFT JOIN listings l ON l.id=d.listing_id
            WHERE d.id=?
        """, (deal_id,)).fetchone()
    if not row:
        await call.answer("Сделка не найдена", show_alert=True); return
    deal_id, listing_id, buyer_id, seller_id, amount, commission, payout, method, status, receipt, created_at, title = row
    receipt_text = "есть" if receipt else "нет"
    await show_screen(call, f"""
━━━━━━━━━━━━━━
🤝 <b>Сделка #{deal_id}</b>
━━━━━━━━━━━━━━

📦 Объект: <b>{html.escape(str(title))}</b>
🆔 Listing ID: <code>{listing_id}</code>

👤 Покупатель: {user_contact(buyer_id)}
🛠 Исполнитель: {user_contact(seller_id)}

💰 Сумма: <b>{rub(amount)}</b>
💵 Комиссия: <b>{rub(commission)}</b>
💸 К выплате: <b>{rub(payout)}</b>
💳 Оплата: <b>{html.escape(method or '—')}</b>
🧾 Чек: <b>{receipt_text}</b>
📌 Статус: <b>{deal_status_ru(status)}</b>
📅 Создана: <code>{html.escape(str(created_at or '')[:16])}</code>
""", reply_markup=admin_deal_buttons(deal_id, status), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_payment_ok_v2:"))
async def admin_payment_ok_v2(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    deal_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True); return
        buyer_id, seller_id, status = row
        if status != "waiting_admin_confirm":
            await call.answer("Сделка не ждёт подтверждения оплаты", show_alert=True); return
        conn.execute("UPDATE deals SET status='in_work' WHERE id=?", (deal_id,))
        conn.commit()
    for uid, text in [(buyer_id, f"✅ Оплата по сделке #{deal_id} подтверждена. Исполнитель может начинать работу."), (seller_id, f"✅ Оплата по сделке #{deal_id} подтверждена. Можно начинать выполнение.")]:
        try:
            await bot.send_message(uid, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")]]))
        except Exception:
            pass
    await call.answer("Оплата подтверждена", show_alert=True)
    call.data = f"admin_deal_view_v2:{deal_id}"
    await admin_deal_view_v2(call)


@dp.callback_query(F.data.startswith("admin_payment_no_v2:"))
async def admin_payment_no_v2(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    deal_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True); return
        buyer_id, seller_id = row
        conn.execute("UPDATE deals SET status='waiting_receipt', receipt=NULL WHERE id=?", (deal_id,))
        conn.commit()
    try:
        await bot.send_message(buyer_id, f"❌ Чек по сделке #{deal_id} отклонён. Отправьте корректный чек.")
    except Exception:
        pass
    await call.answer("Чек отклонён", show_alert=True)
    call.data = f"admin_deal_view_v2:{deal_id}"
    await admin_deal_view_v2(call)


@dp.callback_query(F.data.startswith("admin_payout_done_v2:"))
async def admin_payout_done_v2(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    deal_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True); return
        buyer_id, seller_id, status = row
        if status != "waiting_payout":
            await call.answer("Сделка не ожидает выплату", show_alert=True); return
        conn.execute("UPDATE deals SET status='completed' WHERE id=?", (deal_id,))
        conn.commit()
    for uid, text in [(buyer_id, f"✅ Сделка #{deal_id} завершена."), (seller_id, f"✅ Выплата по сделке #{deal_id} отмечена как выполненная.")]:
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass
    try:
        await bot.send_message(buyer_id, f"⭐ Оцените продавца по сделке #{deal_id}:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="1⭐", callback_data=f"review_rating:{deal_id}:1"), InlineKeyboardButton(text="2⭐", callback_data=f"review_rating:{deal_id}:2"), InlineKeyboardButton(text="3⭐", callback_data=f"review_rating:{deal_id}:3"), InlineKeyboardButton(text="4⭐", callback_data=f"review_rating:{deal_id}:4"), InlineKeyboardButton(text="5⭐", callback_data=f"review_rating:{deal_id}:5"),
        ]]))
    except Exception:
        pass
    await call.answer("Выплата закрыта", show_alert=True)
    call.data = f"admin_deal_view_v2:{deal_id}"
    await admin_deal_view_v2(call)


async def admin_set_deal_status(call: CallbackQuery, deal_id: int, status: str, user_text: str):
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True); return
        buyer_id, seller_id = row
        conn.execute("UPDATE deals SET status=? WHERE id=?", (status, deal_id))
        conn.commit()
    for uid in [buyer_id, seller_id]:
        try:
            await bot.send_message(uid, user_text)
        except Exception:
            pass
    await call.answer("Статус обновлён", show_alert=True)
    call.data = f"admin_deal_view_v2:{deal_id}"
    await admin_deal_view_v2(call)

@dp.callback_query(F.data.startswith("admin_deal_freeze:"))
async def admin_deal_freeze(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    await admin_set_deal_status(call, int(call.data.split(":")[1]), "frozen", "🧊 Сделка временно заморожена администрацией LTeam.")

@dp.callback_query(F.data.startswith("admin_deal_unfreeze:"))
async def admin_deal_unfreeze(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    await admin_set_deal_status(call, int(call.data.split(":")[1]), "in_work", "▶️ Сделка возвращена в работу администрацией LTeam.")

@dp.callback_query(F.data.startswith("admin_deal_cancel:"))
async def admin_deal_cancel(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    await admin_set_deal_status(call, int(call.data.split(":")[1]), "cancelled", "❌ Сделка отменена администрацией LTeam.")


@dp.callback_query(F.data == "admin_security_center")
async def admin_security_center(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    ensure_admin_tables()
    with db() as conn:
        new_reports = conn.execute("SELECT COUNT(*) FROM reports WHERE COALESCE(status,'new')='new'").fetchone()[0]
        events = conn.execute("SELECT COUNT(*) FROM security_events WHERE COALESCE(status,'new')='new'").fetchone()[0]
        banned = conn.execute("SELECT COUNT(*) FROM banned_users").fetchone()[0]
        warnings = conn.execute("SELECT COUNT(*) FROM admin_warnings").fetchone()[0]
        tickets = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
    await show_screen(call, f"""
━━━━━━━━━━━━━━
🛡 <b>Центр безопасности</b>
━━━━━━━━━━━━━━

🚨 Новые жалобы: <b>{new_reports}</b>
⚠️ Подозрительные сообщения: <b>{events}</b>
🆘 Открытые обращения: <b>{tickets}</b>
🚫 Забанено пользователей: <b>{banned}</b>
📌 Предупреждений выдано: <b>{warnings}</b>
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🚨 Жалобы ({new_reports})", callback_data="admin_reports"), InlineKeyboardButton(text=f"⚠️ Protect ({events})", callback_data="admin_security_events")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="admin_tickets_v2"), InlineKeyboardButton(text="🚫 Баны", callback_data="admin_bans_list")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_security_events")
async def admin_security_events(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    ensure_admin_tables()
    with db() as conn:
        rows = conn.execute("""
            SELECT id, user_id, event_type, context, text, created_at
            FROM security_events
            WHERE COALESCE(status,'new')='new'
            ORDER BY id DESC
            LIMIT 10
        """).fetchall()
    if not rows:
        await show_screen(call, "⚠️ Новых событий LTeam Protect нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")]]))
        await call.answer(); return
    text = "━━━━━━━━━━━━━━\n⚠️ <b>LTeam Protect</b>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for eid, uid, event_type, context, body, created_at in rows:
        text += f"<b>#{eid}</b> • {html.escape(event_type or 'event')}\n👤 <code>{uid}</code> • {html.escape(str(context or '—'))}\n<code>{html.escape(str(created_at or '')[:16])}</code>\n{html.escape((body or '')[:160])}\n\n"
        buttons.append([InlineKeyboardButton(text=f"✅ Закрыть #{eid}", callback_data=f"admin_security_event_close:{eid}"), InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user:{uid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_security_event_close:"))
async def admin_security_event_close(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    ensure_admin_tables()
    event_id = int(call.data.split(":")[1])
    with db() as conn:
        conn.execute("UPDATE security_events SET status='closed' WHERE id=?", (event_id,))
        conn.commit()
    await call.answer("Событие закрыто", show_alert=True)
    call.data = "admin_security_events"
    await admin_security_events(call)


@dp.callback_query(F.data == "admin_tickets_v2")
async def admin_tickets_v2(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    with db() as conn:
        rows = conn.execute("SELECT id, user_id, text, created_at FROM tickets WHERE status='open' ORDER BY id DESC LIMIT 10").fetchall()
    if not rows:
        await show_screen(call, "🆘 Открытых обращений нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")]]))
        await call.answer(); return
    text="━━━━━━━━━━━━━━\n🆘 <b>Поддержка</b>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for tid, uid, body, created_at in rows:
        text += f"<b>#{tid}</b> от <code>{uid}</code>\n<code>{html.escape(str(created_at or '')[:16])}</code>\n{html.escape((body or '')[:180])}\n\n"
        buttons.append([InlineKeyboardButton(text=f"✅ Закрыть #{tid}", callback_data=f"admin_ticket_close_v2:{tid}"), InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user:{uid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")])
    await show_screen(call,text,reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_ticket_close_v2:"))
async def admin_ticket_close_v2(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    ticket_id=int(call.data.split(":")[1])
    with db() as conn:
        conn.execute("UPDATE tickets SET status='closed' WHERE id=?",(ticket_id,)); conn.commit()
    await call.answer("Обращение закрыто", show_alert=True)
    call.data="admin_tickets_v2"
    await admin_tickets_v2(call)

@dp.callback_query(F.data == "admin_bans_list")
async def admin_bans_list(call: CallbackQuery):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    with db() as conn:
        rows=conn.execute("SELECT user_id, reason, banned_by, created_at FROM banned_users ORDER BY created_at DESC LIMIT 15").fetchall()
    if not rows:
        await show_screen(call,"🚫 Забаненных пользователей нет.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")]])); await call.answer(); return
    text="━━━━━━━━━━━━━━\n🚫 <b>Баны</b>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for uid, reason, banned_by, created_at in rows:
        text += f"👤 <code>{uid}</code>\nПричина: {html.escape(reason or '—')}\nАдмин: <code>{banned_by}</code> • <code>{html.escape(str(created_at or '')[:16])}</code>\n\n"
        buttons.append([InlineKeyboardButton(text=f"✅ Разбанить {uid}", callback_data=f"admin_unban_user:{uid}"), InlineKeyboardButton(text="👤 Открыть", callback_data=f"admin_user:{uid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")])
    await show_screen(call,text,reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),parse_mode="HTML")
    await call.answer()


def broadcast_target_sql(target: str):
    if target == "sellers":
        return "SELECT DISTINCT seller_id FROM listings WHERE seller_id IS NOT NULL", []
    if target == "buyers":
        return "SELECT DISTINCT buyer_id FROM deals WHERE buyer_id IS NOT NULL", []
    if target == "active":
        return """
            SELECT DISTINCT user_id FROM (
                SELECT seller_id AS user_id FROM listings WHERE status='active'
                UNION SELECT buyer_id AS user_id FROM deals WHERE status NOT IN ('completed','cancelled','deleted')
                UNION SELECT seller_id AS user_id FROM deals WHERE status NOT IN ('completed','cancelled','deleted')
            ) WHERE user_id IS NOT NULL
        """, []
    if target == "admins":
        return None, ADMIN_IDS
    return "SELECT user_id FROM users", []


def broadcast_target_name(target: str) -> str:
    return {"all":"всем пользователям", "sellers":"продавцам", "buyers":"покупателям", "active":"активным участникам", "admins":"админам/тест"}.get(target, target)

@dp.callback_query(F.data == "admin_broadcast_target")
async def admin_broadcast_target(call: CallbackQuery, state: FSMContext):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    await state.clear()
    await show_screen(call,"""
━━━━━━━━━━━━━━
📢 <b>Рассылка 2.0</b>
━━━━━━━━━━━━━━

Выберите аудиторию рассылки.
""",reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Всем", callback_data="admin_broadcast_choose:all")],
        [InlineKeyboardButton(text="🛠 Продавцам", callback_data="admin_broadcast_choose:sellers"), InlineKeyboardButton(text="🛒 Покупателям", callback_data="admin_broadcast_choose:buyers")],
        [InlineKeyboardButton(text="🔥 Активным", callback_data="admin_broadcast_choose:active"), InlineKeyboardButton(text="🧪 Админам/тест", callback_data="admin_broadcast_choose:admins")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]),parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("admin_broadcast_choose:"))
async def admin_broadcast_choose(call: CallbackQuery, state: FSMContext):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    target=call.data.split(":",1)[1]
    await state.update_data(broadcast_target=target)
    await state.set_state(BroadcastState.text)
    await show_screen(call,f"📢 Введите текст рассылки для аудитории: <b>{broadcast_target_name(target)}</b>\n\nСообщение сначала будет показано на предпросмотре.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_target")]]),parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "admin_broadcast_send_v2")
async def admin_broadcast_send_v2(call: CallbackQuery, state: FSMContext):
    if not admin_only(call):
        await call.answer("Нет доступа", show_alert=True); return
    ensure_admin_tables()
    data=await state.get_data()
    target=data.get("broadcast_target","all")
    text=(data.get("broadcast_text") or "").strip()
    if not text:
        await call.answer("Текст рассылки потерян", show_alert=True); return
    sql, manual = broadcast_target_sql(target)
    if sql is None:
        users=list(dict.fromkeys(manual))
    else:
        with db() as conn:
            users=list(dict.fromkeys([r[0] for r in conn.execute(sql).fetchall()]))
    sent=0
    for uid in users:
        try:
            await bot.send_message(uid, f"📢 <b>Сообщение от LTeam</b>\n\n{html.escape(text)}", parse_mode="HTML")
            sent += 1
        except Exception:
            pass
    with db() as conn:
        conn.execute("INSERT INTO admin_broadcasts (admin_id, target, text, sent_count, total_count, created_at) VALUES (?, ?, ?, ?, ?, ?)", (call.from_user.id, target, text, sent, len(users), datetime.now().isoformat()))
        conn.commit()
    await state.clear()
    await show_screen(call, f"✅ Рассылка отправлена.\n\n🎯 {broadcast_target_name(target)}\n📨 Отправлено: <b>{sent}/{len(users)}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]]), parse_mode="HTML")
    await call.answer()


# ===== АДМИН-БЕЗОПАСНОСТЬ 2.0: РОЛИ, ЗАПРОСЫ, МУТЫ, ПОЛЬЗОВАТЕЛИ =====

@dp.callback_query(F.data.startswith("admin_req_approve:"))
async def admin_request_approve(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    req_id = int(call.data.split(":")[1])
    with db() as conn:
        req = conn.execute("SELECT request_type, target_id, requested_by, original_admin_id, status FROM admin_action_requests WHERE id=?", (req_id,)).fetchone()
    if not req or req[4] != "pending":
        await call.answer("Запрос уже обработан или не найден", show_alert=True); return
    request_type, target_id, requested_by, original_admin_id, status = req
    if not (is_owner(call.from_user.id) or int(call.from_user.id) == int(original_admin_id)):
        await call.answer("Одобрить может владелец или админ, который сделал исходное действие", show_alert=True); return
    if request_type == "unban":
        with db() as conn:
            conn.execute("DELETE FROM banned_users WHERE user_id=?", (target_id,))
            conn.execute("UPDATE admin_action_requests SET status='approved', resolved_by=?, resolved_at=?, decision=? WHERE id=?", (call.from_user.id, datetime.now().isoformat(), "Одобрено", req_id))
            conn.commit()
        log_admin_action(call.from_user.id, "approve_unban_request", target_id, f"request #{req_id}")
        try: await bot.send_message(requested_by, f"✅ Запрос на разбан пользователя <code>{target_id}</code> одобрен.", parse_mode="HTML")
        except Exception: pass
        await call.answer("Разбан одобрен", show_alert=True)
        await show_screen(call, f"✅ Запрос #{req_id} одобрен. Пользователь <code>{target_id}</code> разбанен.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Открыть пользователя", callback_data=f"admin_user:{target_id}")],[InlineKeyboardButton(text="⬅️ Админка", callback_data="admin_panel")]]), parse_mode="HTML")


@dp.callback_query(F.data.startswith("admin_req_reject:"))
async def admin_request_reject(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    req_id = int(call.data.split(":")[1])
    with db() as conn:
        req = conn.execute("SELECT request_type, target_id, requested_by, original_admin_id, status FROM admin_action_requests WHERE id=?", (req_id,)).fetchone()
    if not req or req[4] != "pending":
        await call.answer("Запрос уже обработан или не найден", show_alert=True); return
    request_type, target_id, requested_by, original_admin_id, status = req
    if not (is_owner(call.from_user.id) or int(call.from_user.id) == int(original_admin_id)):
        await call.answer("Отказать может владелец или админ, который сделал исходное действие", show_alert=True); return
    with db() as conn:
        conn.execute("UPDATE admin_action_requests SET status='rejected', resolved_by=?, resolved_at=?, decision=? WHERE id=?", (call.from_user.id, datetime.now().isoformat(), "Отказано", req_id))
        conn.commit()
    log_admin_action(call.from_user.id, "reject_admin_request", target_id, f"request #{req_id}")
    try: await bot.send_message(requested_by, f"❌ Запрос на действие по пользователю <code>{target_id}</code> отклонён.", parse_mode="HTML")
    except Exception: pass
    await call.answer("Запрос отклонён", show_alert=True)
    await show_screen(call, f"❌ Запрос #{req_id} отклонён.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Админка", callback_data="admin_panel")]]), parse_mode="HTML")


@dp.callback_query(F.data == "admin_roles_panel")
async def admin_roles_panel(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    with db() as conn:
        rows = conn.execute("SELECT user_id, role, assigned_by, created_at FROM staff_roles WHERE role!='user' ORDER BY CASE role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 WHEN 'moderator' THEN 3 ELSE 4 END, user_id LIMIT 50").fetchall()
        pending = conn.execute("SELECT COUNT(*) FROM admin_action_requests WHERE status='pending'").fetchone()[0]
    text = "━━━━━━━━━━━━━━\n👑 <b>Роли и доступы</b>\n━━━━━━━━━━━━━━\n\n"
    text += f"Ваш уровень: <b>{role_badge(call.from_user.id)}</b>\nОжидают решения: <b>{pending}</b>\n\n"
    if rows:
        for uid, role, assigned_by, created_at in rows:
            text += f"• <code>{uid}</code> — <b>{role_badge(uid)}</b> • назначил <code>{assigned_by or 0}</code>\n"
    else:
        text += "Ролей пока нет.\n"
    buttons = []
    if is_owner(call.from_user.id):
        buttons.append([InlineKeyboardButton(text="➕ Назначить роль", callback_data="admin_role_add_start")])
    buttons += [
        [InlineKeyboardButton(text="🔁 Запросы", callback_data="admin_requests_list"), InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users_page:0")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_role_add_start")
async def admin_role_add_start(call: CallbackQuery, state: FSMContext):
    if not is_owner(call.from_user.id):
        await call.answer("Только владелец", show_alert=True); return
    await state.set_state(AdminRoleState.user_id)
    await show_screen(call, "👑 Отправьте Telegram ID пользователя, которому нужно назначить роль:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Роли", callback_data="admin_roles_panel")]]))
    await call.answer()


@dp.message(AdminRoleState.user_id)
async def admin_role_add_id(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await state.clear(); return
    if not message.text or not message.text.strip().isdigit():
        await screen_answer(message, "Отправьте числовой ID."); return
    uid = int(message.text.strip())
    await state.clear()
    await screen_answer(message, f"Выберите роль для <code>{uid}</code>:", reply_markup=role_choose_keyboard(uid), parse_mode="HTML")


def role_choose_keyboard(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 Модератор", callback_data=f"admin_set_role:{user_id}:moderator")],
        [InlineKeyboardButton(text="🛡 Админ", callback_data=f"admin_set_role:{user_id}:admin")],
        [InlineKeyboardButton(text="👑 Владелец", callback_data=f"admin_set_role:{user_id}:owner")],
        [InlineKeyboardButton(text="👤 Снять роль", callback_data=f"admin_set_role:{user_id}:user")],
        [InlineKeyboardButton(text="⬅️ Роли", callback_data="admin_roles_panel")],
    ])


@dp.callback_query(F.data.startswith("admin_role_choose:"))
async def admin_role_choose(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Роли меняет только владелец", show_alert=True); return
    uid = int(call.data.split(":")[1])
    await show_screen(call, f"👑 <b>Смена роли</b>\n\nПользователь: <code>{uid}</code>\nТекущая роль: <b>{role_badge(uid)}</b>", reply_markup=role_choose_keyboard(uid), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_set_role:"))
async def admin_set_role(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Только владелец", show_alert=True); return
    _, uid_raw, role = call.data.split(":")
    uid = int(uid_raw)
    if uid == call.from_user.id and role != "owner":
        await call.answer("Нельзя снять роль самому себе", show_alert=True); return
    if role == "user":
        with db() as conn:
            owners = conn.execute("SELECT COUNT(*) FROM staff_roles WHERE role='owner'").fetchone()[0]
            current = conn.execute("SELECT role FROM staff_roles WHERE user_id=?", (uid,)).fetchone()
            if current and current[0] == "owner" and owners <= 1:
                await call.answer("Нельзя снять последнего владельца", show_alert=True); return
            conn.execute("DELETE FROM staff_roles WHERE user_id=?", (uid,))
            conn.commit()
        log_admin_action(call.from_user.id, "remove_role", uid, "role=user")
        await call.answer("Роль снята", show_alert=True)
    else:
        if role not in ("moderator", "admin", "owner"):
            await call.answer("Неизвестная роль", show_alert=True); return
        with db() as conn:
            conn.execute("INSERT OR REPLACE INTO staff_roles (user_id, role, assigned_by, created_at) VALUES (?, ?, ?, ?)", (uid, role, call.from_user.id, datetime.now().isoformat()))
            conn.commit()
        log_admin_action(call.from_user.id, "set_role", uid, role)
        await call.answer("Роль назначена", show_alert=True)
    call.data = f"admin_user:{uid}"
    await admin_user_profile(call)


@dp.callback_query(F.data == "admin_requests_list")
async def admin_requests_list(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    with db() as conn:
        rows = conn.execute("SELECT id, request_type, target_id, requested_by, original_admin_id, created_at FROM admin_action_requests WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
    if not rows:
        await show_screen(call, "🔁 Активных запросов нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Роли", callback_data="admin_roles_panel")]])); await call.answer(); return
    text = "━━━━━━━━━━━━━━\n🔁 <b>Запросы админов</b>\n━━━━━━━━━━━━━━\n\n"
    buttons=[]
    for rid, typ, target, requested_by, original, created in rows:
        text += f"#{rid} • <b>{html.escape(typ)}</b> • цель <code>{target}</code>\nЗапросил: <code>{requested_by}</code> • решает: <code>{original}</code>\n\n"
        if is_owner(call.from_user.id) or call.from_user.id == original:
            buttons.append([InlineKeyboardButton(text=f"✅ #{rid}", callback_data=f"admin_req_approve:{rid}"), InlineKeyboardButton(text=f"❌ #{rid}", callback_data=f"admin_req_reject:{rid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Роли", callback_data="admin_roles_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_users_page:"))
async def admin_users_page(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    page = int(call.data.split(":")[1])
    limit = 10
    offset = max(page, 0) * limit
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        rows = conn.execute("""
            SELECT u.user_id, COALESCE(u.username,''), u.created_at, COALESCE(u.verified,0),
                   CASE WHEN b.user_id IS NULL THEN 0 ELSE 1 END AS banned
            FROM users u
            LEFT JOIN banned_users b ON b.user_id=u.user_id
            ORDER BY u.created_at DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()
    text = f"━━━━━━━━━━━━━━\n👥 <b>Пользователи</b>\n━━━━━━━━━━━━━━\n\nВсего: <b>{total}</b>\nСтраница: <b>{page+1}</b>\n\n"
    buttons=[]
    for uid, username, created, verified, banned in rows:
        text += f"• <code>{uid}</code> @{html.escape(username or '—')} • {role_badge(uid)} {'🚫' if banned else ''} {'🛡' if verified else ''}\n"
        buttons.append([InlineKeyboardButton(text=f"👤 {uid} @{(username or '—')[:16]}", callback_data=f"admin_user:{uid}")])
    nav=[]
    if page > 0: nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_users_page:{page-1}"))
    if offset + limit < total: nav.append(InlineKeyboardButton(text="➡️ Далее", callback_data=f"admin_users_page:{page+1}"))
    if nav: buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🔎 Поиск", callback_data="admin_find_user"), InlineKeyboardButton(text="⬅️ Админка", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_mute_user:"))
async def admin_mute_user_start(call: CallbackQuery, state: FSMContext):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    uid = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, uid, "mute")
    if not ok:
        await call.answer(reason, show_alert=True); return
    await state.update_data(mute_user_id=uid)
    await show_screen(call, f"🔇 <b>Мут пользователя</b> <code>{uid}</code>\n\nВыберите срок:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="10 минут", callback_data="admin_mute_duration:10"), InlineKeyboardButton(text="1 час", callback_data="admin_mute_duration:60")],
        [InlineKeyboardButton(text="1 день", callback_data="admin_mute_duration:1440"), InlineKeyboardButton(text="7 дней", callback_data="admin_mute_duration:10080")],
        [InlineKeyboardButton(text="⬅️ Профиль", callback_data=f"admin_user:{uid}")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_mute_duration:"))
async def admin_mute_duration(call: CallbackQuery, state: FSMContext):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True); return
    minutes = int(call.data.split(":")[1])
    data = await state.get_data()
    uid = int(data.get("mute_user_id") or 0)
    if not uid:
        await call.answer("Пользователь потерян", show_alert=True); return
    set_mute(uid, minutes, "Мут выдан модерацией", muted_by=call.from_user.id)
    log_admin_action(call.from_user.id, "mute_user", uid, f"{minutes} minutes")
    try: await bot.send_message(uid, f"🔇 Вам выдан мут на {minutes} мин. Причина: модерация LTeam")
    except Exception: pass
    await state.clear()
    await call.answer("Мут выдан", show_alert=True)
    call.data = f"admin_user:{uid}"
    await admin_user_profile(call)


@dp.callback_query(F.data.startswith("report_admin:"))
async def report_admin_start(call: CallbackQuery, state: FSMContext):
    admin_id = int(call.data.split(":")[1])
    await state.update_data(target_type="admin", target_id=admin_id, listing_id=0)
    await state.set_state(ReportState.reason)
    await show_screen(call, "🚨 Опишите жалобу на администратора. Она будет отправлена владельцам и не будет видна этому администратору.")
    await call.answer()
# ===== ЗАПУСК =====

import asyncio

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Проверьте .env")
    init_db()
    await setup_bot_commands()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())


