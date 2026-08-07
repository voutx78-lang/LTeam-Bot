import os
import sqlite3
import html
import re
import json
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
    MenuButtonWebApp,
    BotCommand,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

load_dotenv()  # Эта строка обязательна, она загрузит переменные из .env
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from app.states import (
    AdminBanState, AdminMessageState, AdminMuteState, AdminReasonState,
    AdminRoleState, AdminSearchUserState, AdminUnbanState, AdminUserPickState,
    AdminWarnState, AppealState, BroadcastState, CreateListing, CreateOrder,
    DealChatState, DealFinalPriceState, DisputeState, ListingDiscussionState,
    MarketFilterState, OrderChatState, OrderResponseState, PayoutProfileState,
    ProfileDescriptionState, PromoState, ReceiptState, ReportState, ReviewState,
    SearchState, SupportState, VerificationRequestState, WithdrawalState,
)

from typing import Any, Awaitable, Callable, Dict


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
OWNER_IDS = [int(x.strip()) for x in os.getenv("OWNER_IDS", "").split(",") if x.strip()] or (ADMIN_IDS[:1] if ADMIN_IDS else [])
MODERATOR_IDS = [int(x.strip()) for x in os.getenv("MODERATOR_IDS", "").split(",") if x.strip()]
STAFF_ROLE_LEVELS = {"user": 0, "moderator": 1, "admin": 2, "owner": 3}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
NAV_MENU_MESSAGES: dict[int, tuple[int, int]] = {}

DB_PATH = "market.db"
BANNER_PATH = "Baner.png"
try:
    COMMISSION_PERCENT = max(0, min(100, int(os.getenv("COMMISSION_PERCENT", "10"))))
except (TypeError, ValueError):
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
TG_CHANNEL_URL = os.getenv("TG_CHANNEL_URL", "").strip()
TG_CHANNEL_NAME = os.getenv("TG_CHANNEL_NAME", "").strip() or "Канал LTeam"

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


def money_parts(amount: int) -> tuple[int, int, int]:
    amount = int(amount or 0)
    commission = int(amount * COMMISSION_PERCENT / 100)
    payout = max(0, amount - commission)
    total_to_pay = amount
    return total_to_pay, commission, payout


def lteam_card_payment_text(deal_id: int, amount: int) -> str:
    return f"""
💳 <b>Оплата через гаранта LTeam</b>

Сделка: <b>#{deal_id}</b>
К оплате: <b>{int(amount or 0)}₽</b>

🏦 Банк: <b>{html.escape(SBP_BANK)}</b>
👤 Получатель: <b>{html.escape(SBP_NAME)}</b>
📱 Карта/СБП: <code>{html.escape(SBP_PHONE)}</code>

⚠️ Переводите деньги только по этим реквизитам LTeam.
После оплаты нажмите «✅ Я оплатил» и отправьте чек.
"""


def deal_status_title(status: str | None) -> str:
    return {
        "discussion": "💬 обсуждение",
        "waiting_final_price": "💰 ждёт итоговую цену",
        "waiting_buyer_price_confirm": "🧾 ждёт подтверждение цены",
        "waiting_admin_payment_approval": "🛡 оплату проверяет админ",
        "waiting_payment": "💳 ждёт оплату покупателя",
        "waiting_receipt": "🧾 ждёт чек",
        "waiting_admin_confirm": "🔎 чек на проверке",
        "in_work": "🛠 в работе",
        "waiting_buyer_confirm": "✅ ждёт подтверждение выполнения",
        "waiting_payout": "💸 ждёт зачисление/выплату",
        "completed": "🏁 завершена",
        "payment_rejected": "❌ оплата отклонена",
        "dispute_open": "🚨 открыт спор",
        "dispute_resolved_buyer": "↩️ спор решён в пользу покупателя",
        "dispute_resolved_seller": "✅ спор решён в пользу исполнителя",
        "frozen": "🧊 заморожена",
    }.get(status or "", status or "неизвестно")

def user_public_status(user_id: int) -> str:
    return trust_public_badge(user_id) if 'trust_public_badge' in globals() else ("👑 Official LTeam" if is_admin(user_id) else seller_stats(user_id).get("status", "🆕 Новый пользователь"))


from app.database import db, init_db
def ensure_user_search_columns():
    """Мягкая миграция users для поиска по username / имени / нику."""
    with db() as conn:
        cur = conn.cursor()
        for column_sql in [
            "first_name TEXT DEFAULT ''",
            "last_name TEXT DEFAULT ''",
            "display_name TEXT DEFAULT ''",
        ]:
            try:
                cur.execute(f"ALTER TABLE users ADD COLUMN {column_sql}")
            except sqlite3.OperationalError:
                pass
        conn.commit()


def save_user(message: Message):
    ensure_user_search_columns()

    first_name = message.from_user.first_name or ""
    last_name = message.from_user.last_name or ""
    username = message.from_user.username or ""
    display_name = (f"{first_name} {last_name}".strip() or username or str(message.from_user.id))

    with db() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, created_at, first_name, last_name, display_name)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (message.from_user.id, username, datetime.now().isoformat(), first_name, last_name, display_name),
        )
        conn.execute(
            """
            UPDATE users
            SET username=?, first_name=?, last_name=?, display_name=?
            WHERE user_id=?
            """,
            (username, first_name, last_name, display_name, message.from_user.id),
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
    """Единая модель прав LTeam.

    ВАЖНО:
    - все опасные действия должны проходить через эту функцию;
    - staff нельзя банить/мутить/варнить/понижать обычной админкой;
    - владельца нельзя наказать или изменить через обычную админку;
    - равная роль не может действовать на равную роль;
    - owner-only действия доступны только владельцу.
    """
    actor_id = int(actor_id or 0)
    target_id = int(target_id) if target_id is not None else None

    actor_role = get_role(actor_id)
    actor_level = role_level(actor_id)

    target_role = get_role(target_id) if target_id else "user"
    target_level = role_level(target_id) if target_id else 0

    if actor_level <= 0:
        return False, "Нет доступа."

    if target_id and actor_id == target_id:
        return False, "Нельзя выполнять это действие над самим собой."

    owner_only = {
        "set_role",
        "remove_role",
        "approve_admin_request",
        "reject_admin_request",
        "view_admin_reports",
        "close_admin_report",
        "force_unban",
        "cleanup_staff_punishments",
    }

    admin_plus = {
        "ban",
        "unban",
        "verify",
        "unverify",
        "grant_plus",
        "revoke_plus",
        "broadcast",
        "finance",
        "deal_manage",
        "view_logs",
    }

    moderator_plus = {
        "moderate",
        "warn",
        "mute",
        "view_user",
        "view_reports",
        "view_security",
    }

    protected_staff_actions = {
        "ban",
        "unban",
        "mute",
        "warn",
        "verify",
        "unverify",
        "grant_plus",
        "revoke_plus",
        "deal_manage",
        "set_role",
        "remove_role",
        "force_unban",
    }

    # Важный инвариант безопасности:
    # owner / admin / moderator не получают обычные наказания из админки.
    # Если нужно наказать staff — владелец сначала снимает роль, а уже потом
    # применяется обычный бан / мут / варн к обычному пользователю.
    staff_punishment_actions = {"ban", "mute", "warn"}
    if target_id and target_level >= STAFF_ROLE_LEVELS["moderator"] and action in staff_punishment_actions:
        return False, "Staff-пользователей нельзя банить, мутить или варнить. Если нужен доступный для санкций аккаунт — сначала снимите роль через владельца."

    if action in owner_only and actor_role != "owner":
        return False, "Это действие доступно только владельцу."

    if action in admin_plus and actor_level < STAFF_ROLE_LEVELS["admin"]:
        return False, "Это действие доступно только админу или владельцу."

    if action in moderator_plus and actor_level < STAFF_ROLE_LEVELS["moderator"]:
        return False, "Это действие доступно только модератору, админу или владельцу."

    if target_id and action in protected_staff_actions:
        if target_role == "owner":
            return False, "Владельца нельзя наказывать или менять через обычную админку."

        if actor_level <= target_level:
            return False, "Нельзя выполнять действие над равной или старшей ролью."

    return True, ""

def log_admin_action(actor_id: int, action: str, target_id: int | None = None, details: str = "") -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO admin_action_logs (actor_id, target_id, action, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (actor_id, target_id, action, details, datetime.now().isoformat()),
        )
        conn.commit()





async def require_payout_profile_or_show(call: CallbackQuery, state: FSMContext, next_callback: str = "") -> bool:
    """Совместимость старого сценария.

    Раньше создание заказа/отклик могли требовать реквизиты заранее, из-за чего
    ломалась кнопка «Создать заказ». В новой логике реквизиты нужны только на
    этапе вывода средств, поэтому здесь просто разрешаем продолжить.
    """
    return True


def protected_staff_ids() -> set[int]:
    """Все ID, которые нельзя банить/мутить автоматикой."""
    ids = set(int(x) for x in (OWNER_IDS + ADMIN_IDS + MODERATOR_IDS))
    try:
        with db() as conn:
            rows = conn.execute("SELECT user_id FROM staff_roles WHERE role IN ('owner', 'admin', 'moderator')").fetchall()
        ids.update(int(r[0]) for r in rows)
    except Exception:
        pass
    return ids

def add_column_if_missing(cur, table: str, column_sql: str) -> None:
    """Добавляет колонку, если её ещё нет. Безопасно для повторных запусков."""
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")
    except sqlite3.OperationalError:
        pass


def ensure_admin_tables() -> None:
    """Безопасные миграции для админки, ролей, заявок, логов и жалоб.

    Важно: функция специально совместима со старыми версиями таблиц, которые уже могли
    быть созданы в ранних итерациях main.py.
    """
    with db() as conn:
        cur = conn.cursor()

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
            request_type TEXT,
            action TEXT,
            target_id INTEGER,
            requested_by INTEGER,
            requester_id INTEGER,
            original_admin_id INTEGER,
            reason TEXT,
            details TEXT,
            status TEXT DEFAULT 'pending',
            resolved_by INTEGER,
            reviewer_id INTEGER,
            decision TEXT,
            created_at TEXT,
            resolved_at TEXT,
            reviewed_at TEXT
        )
        """)

        for column_sql in [
            "request_type TEXT",
            "action TEXT",
            "target_id INTEGER",
            "requested_by INTEGER",
            "requester_id INTEGER",
            "original_admin_id INTEGER",
            "reason TEXT",
            "details TEXT",
            "status TEXT DEFAULT 'pending'",
            "resolved_by INTEGER",
            "reviewer_id INTEGER",
            "decision TEXT",
            "created_at TEXT",
            "resolved_at TEXT",
            "reviewed_at TEXT",
        ]:
            add_column_if_missing(cur, "admin_action_requests", column_sql)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS muted_users (
            user_id INTEGER PRIMARY KEY,
            muted_until TEXT,
            reason TEXT,
            muted_by INTEGER DEFAULT 0,
            created_at TEXT
        )
        """)
        add_column_if_missing(cur, "muted_users", "muted_by INTEGER DEFAULT 0")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            admin_id INTEGER,
            reason TEXT,
            text TEXT,
            created_at TEXT
        )
        """)
        add_column_if_missing(cur, "admin_warnings", "reason TEXT")
        add_column_if_missing(cur, "admin_warnings", "text TEXT")

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

        for column_sql in [
            "target_type TEXT DEFAULT 'listing'",
            "target_id INTEGER",
            "status TEXT DEFAULT 'new'",
        ]:
            add_column_if_missing(cur, "reports", column_sql)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS protect_appeals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            reason TEXT,
            status TEXT DEFAULT 'pending',
            reviewer_id INTEGER,
            admin_comment TEXT,
            created_at TEXT,
            reviewed_at TEXT
        )
        """)
        for column_sql in [
            "user_id INTEGER",
            "reason TEXT",
            "status TEXT DEFAULT 'pending'",
            "reviewer_id INTEGER",
            "admin_comment TEXT",
            "created_at TEXT",
            "reviewed_at TEXT",
        ]:
            add_column_if_missing(cur, "protect_appeals", column_sql)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS protect_overrides (
            user_id INTEGER PRIMARY KEY,
            status TEXT DEFAULT 'active',
            trusted_until TEXT,
            reason TEXT,
            created_by INTEGER,
            created_at TEXT
        )
        """)
        for column_sql in [
            "status TEXT DEFAULT 'active'",
            "trusted_until TEXT",
            "reason TEXT",
            "created_by INTEGER",
            "created_at TEXT",
        ]:
            add_column_if_missing(cur, "protect_overrides", column_sql)

        conn.commit()


def create_admin_request(
    request_type: str,
    target_id: int,
    requested_by: int,
    reason: str = "",
    original_admin_id: int | None = None,
    details: str = "",
) -> int:
    """Создаёт заявку на опасное админ-действие."""
    ensure_admin_tables()

    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO admin_action_requests (
                request_type, action, target_id,
                requested_by, requester_id,
                original_admin_id,
                reason, details,
                status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (
                request_type,
                request_type,
                target_id,
                requested_by,
                requested_by,
                original_admin_id,
                reason,
                details,
                datetime.now().isoformat(),
            ),
        )
        request_id = cur.lastrowid
        conn.commit()

    log_admin_action(
        requested_by,
        f"request_{request_type}",
        target_id,
        f"request_id={request_id}; reason={reason}; details={details}",
    )
    return int(request_id)




# ===== LTEAM ADMIN USER RESOLVER =====

def normalize_user_query(value: str) -> str:
    value = (value or "").strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = value.replace(prefix, "", 1)
    if value.startswith("@"):
        value = value[1:]
    return value.strip()


def find_users_for_admin(query: str, limit: int = 8):
    """Ищет пользователя по ID, @username, ссылке t.me или имени/нику."""
    ensure_user_search_columns()
    q = normalize_user_query(query)
    if not q:
        return []
    with db() as conn:
        if q.isdigit():
            rows = conn.execute("""
                SELECT user_id, COALESCE(username,''), COALESCE(display_name,''), COALESCE(first_name,''), COALESCE(last_name,'')
                FROM users WHERE user_id=?
            """, (int(q),)).fetchall()
            if rows:
                return rows
        like = f"%{q.lower()}%"
        return conn.execute("""
            SELECT user_id, COALESCE(username,''), COALESCE(display_name,''), COALESCE(first_name,''), COALESCE(last_name,'')
            FROM users
            WHERE LOWER(COALESCE(username,'')) LIKE ?
               OR LOWER(COALESCE(display_name,'')) LIKE ?
               OR LOWER(COALESCE(first_name,'')) LIKE ?
               OR LOWER(COALESCE(last_name,'')) LIKE ?
            ORDER BY CASE WHEN LOWER(COALESCE(username,''))=LOWER(?) THEN 0 ELSE 1 END, user_id DESC
            LIMIT ?
        """, (like, like, like, like, q, limit)).fetchall()


def user_pick_keyboard(rows, action: str, back_callback: str = "admin_panel"):
    buttons = []
    for user_id, username, display_name, first_name, last_name in rows:
        name = display_name or f"{first_name} {last_name}".strip() or username or str(user_id)
        username_part = f" @{username}" if username else ""
        buttons.append([InlineKeyboardButton(
            text=f"👤 {name}{username_part} • {user_id}",
            callback_data=f"admin_pick_user:{action}:{user_id}",
        )])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def user_action_after_pick_keyboard(action: str, user_id: int, back_callback: str = "admin_panel"):
    callbacks = {
        "profile": f"admin_user:{user_id}",
        "ban": f"admin_ban_user:{user_id}",
        "unban": f"admin_unban_user:{user_id}",
        "role": f"admin_role_choose:{user_id}",
    }
    titles = {
        "profile": "👤 Открыть профиль",
        "ban": "🚫 Забанить",
        "unban": "✅ Разбанить",
        "role": "👑 Изменить роль",
    }
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=titles.get(action, "👤 Открыть профиль"), callback_data=callbacks.get(action, f"admin_user:{user_id}"))],
        [InlineKeyboardButton(text="👤 Профиль", callback_data=f"admin_user:{user_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)],
    ])


async def ask_admin_user_query(call: CallbackQuery, state: FSMContext, action: str, title: str, back_callback: str = "admin_panel"):
    await state.set_state(AdminUserPickState.query)
    await state.update_data(user_pick_action=action, user_pick_back=back_callback)
    await show_screen(call, f"""
{title}

Можно отправить:
• Telegram ID: <code>123456789</code>
• username: <code>@username</code>
• ссылку: <code>t.me/username</code>
• имя / ник из профиля
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)]
    ]), parse_mode="HTML")


@dp.message(AdminUserPickState.query)
async def admin_user_pick_query(message: Message, state: FSMContext):
    if not is_staff(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    action = data.get("user_pick_action", "profile")
    back_callback = data.get("user_pick_back", "admin_panel")
    rows = find_users_for_admin(message.text or "")
    await state.clear()

    if not rows:
        await screen_answer(message, "❌ Пользователь не найден. Он должен хотя бы один раз открыть бота.\n\nПопробуйте ID, @username или часть имени.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Искать ещё", callback_data="admin_find_user")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)],
        ]), parse_mode="HTML")
        return

    if len(rows) == 1:
        uid = int(rows[0][0])
        await screen_answer(message, f"✅ Пользователь найден: <code>{uid}</code>", reply_markup=user_action_after_pick_keyboard(action, uid, back_callback), parse_mode="HTML")
        return

    await screen_answer(message, "Найдено несколько пользователей. Выберите нужного:", reply_markup=user_pick_keyboard(rows, action, back_callback), parse_mode="HTML")


@dp.callback_query(F.data.startswith("admin_pick_user:"))
async def admin_pick_user(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    _, action, uid_raw = call.data.split(":")
    uid = int(uid_raw)
    if action == "profile":
        call.data = f"admin_user:{uid}"
        await admin_user_profile(call)
    elif action == "ban":
        call.data = f"admin_ban_user:{uid}"
        await admin_ban_user_direct(call)
    elif action == "unban":
        call.data = f"admin_unban_user:{uid}"
        await admin_unban_user_direct(call)
    elif action == "role":
        call.data = f"admin_role_choose:{uid}"
        await admin_role_choose(call)
    else:
        await call.answer("Неизвестное действие", show_alert=True)


# ===== LTEAM PROTECT CORE =====

def has_active_protect_override(user_id: int) -> tuple[bool, str]:
    """Проверяет, есть ли у пользователя активное одобрение апелляции Protect."""
    ensure_admin_tables()
    with db() as conn:
        row = conn.execute(
            "SELECT trusted_until, reason FROM protect_overrides WHERE user_id=? AND status='active'",
            (user_id,),
        ).fetchone()
    if not row:
        return False, ""
    trusted_until, reason = row
    if trusted_until:
        try:
            if datetime.fromisoformat(trusted_until) <= datetime.now():
                with db() as conn:
                    conn.execute("UPDATE protect_overrides SET status='expired' WHERE user_id=?", (user_id,))
                    conn.commit()
                return False, ""
        except Exception:
            pass
    return True, reason or "апелляция одобрена"


def create_protect_override(user_id: int, reviewer_id: int, reason: str, days: int = 30) -> None:
    ensure_admin_tables()
    trusted_until = datetime.now() + timedelta(days=days)
    with db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO protect_overrides (user_id, status, trusted_until, reason, created_by, created_at)
            VALUES (?, 'active', ?, ?, ?, ?)
            """,
            (user_id, trusted_until.isoformat(), reason, reviewer_id, datetime.now().isoformat()),
        )
        conn.commit()


def revoke_protect_override(user_id: int, reviewer_id: int, reason: str = "") -> None:
    ensure_admin_tables()
    with db() as conn:
        conn.execute(
            "UPDATE protect_overrides SET status='revoked', reason=? WHERE user_id=?",
            (reason or f"Отозвано владельцем {reviewer_id}", user_id),
        )
        conn.commit()


def protect_policy_for_user(user_id: int) -> dict:
    """Жёсткая политика LTeam Protect.

    high risk = 60+ score:
    - нельзя писать в безопасные чаты;
    - нельзя создавать новые объявления/заказы;
    - нельзя откликаться на заказы;
    - партнёр получает предупреждение уже со среднего риска.
    """
    security = get_user_security_score(user_id)
    score = int(security.get("score", 0))

    if is_staff(user_id):
        return {
            "score": score,
            "badge": "🛡 Staff",
            "level": "staff",
            "block_chats": False,
            "block_create_listing": False,
            "block_create_order": False,
            "block_order_application": False,
            "force_moderation": False,
            "notify_partner": False,
            "reasons": security.get("reasons", []),
        }

    override_active, override_reason = has_active_protect_override(user_id)
    if override_active:
        return {
            "score": score,
            "badge": "🟢 Апелляция одобрена",
            "level": "appeal_approved",
            "block_chats": False,
            "block_create_listing": False,
            "block_create_order": False,
            "block_order_application": False,
            "force_moderation": False,
            "notify_partner": False,
            "reasons": [override_reason] + security.get("reasons", []),
        }

    high_risk = score >= 60
    medium_risk = score >= 25

    return {
        "score": score,
        "badge": security.get("badge", "🟢 Низкий риск"),
        "level": security.get("level", "low"),
        "block_chats": high_risk,
        "block_create_listing": high_risk,
        "block_create_order": high_risk,
        "block_order_application": high_risk,
        "force_moderation": medium_risk,
        "notify_partner": medium_risk,
        "reasons": security.get("reasons", []),
    }


def protect_block_text(user_id: int, action_title: str) -> str:
    policy = protect_policy_for_user(user_id)
    return f"""
🔴 <b>LTeam Protect</b>

Действие заблокировано: <b>{html.escape(action_title)}</b>

Ваш текущий риск: <b>{policy.get('badge')}</b>
Score: <b>{policy.get('score')}/100</b>

Факторы:
{security_reasons_text(policy.get('reasons', []), limit=5)}

Чтобы снять ограничение, подайте апелляцию Protect. Владельцы проверят историю, жалобы и события безопасности.
"""


def register_security_event(user_id: int, event_type: str, context: str, text: str = "", status: str = "new") -> None:
    ensure_admin_tables()
    with db() as conn:
        conn.execute(
            "INSERT INTO security_events (user_id, event_type, context, text, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, event_type, context, (text or "")[:1000], status, datetime.now().isoformat()),
        )
        conn.commit()


def count_recent_security_events(user_id: int, event_type: str, minutes: int = 1440) -> int:
    since = datetime.now() - timedelta(minutes=minutes)
    with db() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM security_events WHERE user_id=? AND event_type=? AND created_at>=?",
            (user_id, event_type, since.isoformat()),
        ).fetchone()[0]


def protect_warning_text(user_id: int) -> str:
    security = get_user_security_score(user_id)
    return f"""
🛡 <b>LTeam Protect</b>

Пользователь: <code>{user_id}</code>
Риск: <b>{security.get('badge')}</b>
Score: <b>{security.get('score')}/100</b>

Факторы:
{security_reasons_text(security.get('reasons', []), limit=5)}

Рекомендация: не переводите оплату напрямую и ведите сделку только через гаранта LTeam.
"""


async def apply_bypass_punishment(user_id: int, context: str, text: str) -> tuple[bool, str]:
    # Защита staff должна работать не только через protect_check_outgoing_message,
    # но и в прямых вызовах этой функции из отдельных сценариев.
    auto_ok, auto_reason = can_auto_punish(user_id)
    if not auto_ok:
        register_security_event(user_id, "bypass_attempt_staff_ignored", context, text, status="ignored")
        log_admin_action(0, "auto_punishment_blocked_staff", user_id, f"context={context}; {auto_reason}")
        return False, ""

    register_security_event(user_id, "bypass_attempt", context, text)
    count = count_recent_security_events(user_id, "bypass_attempt", minutes=1440)

    if count == 1:
        msg = "⚠️ LTeam Protect: нельзя уводить сделку в личку или просить оплату напрямую. Сообщение заблокировано."
    elif count == 2:
        with db() as conn:
            conn.execute(
                "INSERT INTO admin_warnings (user_id, admin_id, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, 0, "Авто-варн LTeam Protect: повторная попытка обхода гаранта", datetime.now().isoformat()),
            )
            conn.commit()
        msg = "⚠️ Повторная попытка обхода гаранта. Выдан автоматический варн. Сообщение заблокировано."
    else:
        set_mute(user_id, 60, "LTeam Protect: 3 попытки обхода гаранта за 24 часа", muted_by=0)
        msg = "🔇 LTeam Protect: 3 попытки обхода гаранта. Авто-мут на 60 минут. Сообщение заблокировано."

    await notify_admins(f"""
🛡 <b>LTeam Protect: обход гаранта</b>

Пользователь: <code>{user_id}</code>
Контекст: <b>{html.escape(context)}</b>
Попыток за 24ч: <b>{count}</b>

Текст:
{html.escape((text or '')[:800])}
""")
    return True, msg


async def protect_check_outgoing_message(user_id: int, text: str, context: str) -> tuple[bool, str]:
    # Staff не должен получать авто-предупреждения, авто-муты или блокировки Protect.
    if is_staff(user_id):
        return True, ""

    policy = protect_policy_for_user(user_id)

    if policy.get("block_chats"):
        register_security_event(user_id, "chat_blocked_high_risk", context, text, status="blocked")
        await notify_admins(f"""
🔴 <b>LTeam Protect заблокировал сообщение высокого риска</b>

Пользователь: <code>{user_id}</code>
Контекст: <b>{html.escape(context)}</b>
Риск: <b>{policy.get('score')}/100</b>

Текст:
{html.escape((text or '')[:800])}
""")
        return False, "🔴 LTeam Protect: ваш аккаунт имеет высокий риск. Сообщение заблокировано до проверки администратором."

    if looks_like_bypass_attempt(text):
        blocked, msg = await apply_bypass_punishment(user_id, context, text)
        return not blocked, msg

    return True, ""


async def protect_notify_partner_if_needed(sender_id: int, receiver_id: int, context: str):
    policy = protect_policy_for_user(sender_id)
    if not policy.get("notify_partner"):
        return
    try:
        await bot.send_message(receiver_id, protect_warning_text(sender_id), parse_mode="HTML")
    except Exception:
        pass


# ===== LTEAM SECURITY SCORE =====

def get_user_security_score(user_id: int) -> dict:
    """
    Риск-профиль пользователя для админки.

    score:
    0-24   — низкий риск
    25-59  — средний риск
    60-100 — высокий риск
    """
    score = 0
    reasons: list[str] = []

    with db() as conn:
        user = conn.execute(
            "SELECT created_at, COALESCE(verified, 0) FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()

        reports_by_user = conn.execute(
            "SELECT COUNT(*) FROM reports WHERE user_id=?",
            (user_id,),
        ).fetchone()[0]

        reports_on_user = conn.execute(
            """
            SELECT COUNT(*)
            FROM reports r
            LEFT JOIN listings l ON l.id = r.listing_id
            LEFT JOIN orders o ON o.id = COALESCE(r.target_id, 0) AND COALESCE(r.target_type, 'listing')='order'
            WHERE COALESCE(r.target_id, 0)=?
               OR l.seller_id=?
               OR o.customer_id=?
            """,
            (user_id, user_id, user_id),
        ).fetchone()[0]

        warnings_count = conn.execute(
            "SELECT COUNT(*) FROM admin_warnings WHERE user_id=?",
            (user_id,),
        ).fetchone()[0]

        active_ban = conn.execute(
            "SELECT 1 FROM banned_users WHERE user_id=?",
            (user_id,),
        ).fetchone() is not None

        active_mute = conn.execute(
            "SELECT muted_until FROM muted_users WHERE user_id=?",
            (user_id,),
        ).fetchone()

        # Даже если в старой базе случайно остался бан/мут staff-пользователя,
        # риск-профиль не должен считать это реальной активной санкцией.
        if is_staff(user_id):
            active_ban = False
            active_mute = None

        security_events_count = conn.execute(
            "SELECT COUNT(*) FROM security_events WHERE user_id=?",
            (user_id,),
        ).fetchone()[0]

        bypass_events_count = conn.execute(
            "SELECT COUNT(*) FROM security_events WHERE user_id=? AND event_type='bypass_attempt'",
            (user_id,),
        ).fetchone()[0]

        admin_ban_events = conn.execute(
            "SELECT COUNT(*) FROM admin_action_logs WHERE target_id=? AND action IN ('ban_user', 'unban_user')",
            (user_id,),
        ).fetchone()[0]

        completed_deals = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE (buyer_id=? OR seller_id=?) AND status='completed'",
            (user_id, user_id),
        ).fetchone()[0]

        reviews_row = conn.execute(
            "SELECT COUNT(*), COALESCE(AVG(rating), 0) FROM reviews WHERE seller_id=?",
            (user_id,),
        ).fetchone()

    reviews_count = int(reviews_row[0] or 0)
    avg_rating = float(reviews_row[1] or 0)

    if is_staff(user_id):
        score -= 15
        reasons.append("staff-пользователь")

    if user:
        created_at, verified = user
        if int(verified or 0):
            score -= 20
            reasons.append("верифицирован LTeam")

        try:
            created_dt = datetime.fromisoformat(created_at)
            account_age_days = (datetime.now() - created_dt).days
            if account_age_days < 1:
                score += 20
                reasons.append("аккаунт создан сегодня")
            elif account_age_days < 7:
                score += 10
                reasons.append("новый аккаунт")
        except Exception:
            score += 5
            reasons.append("неизвестная дата регистрации")
    else:
        score += 15
        reasons.append("пользователь не найден в users")

    if active_ban:
        score += 60
        reasons.append("активный бан")

    if active_mute:
        try:
            muted_until = datetime.fromisoformat(active_mute[0])
            if muted_until > datetime.now():
                score += 20
                reasons.append("активный мут")
        except Exception:
            pass

    if reports_on_user > 0:
        score += min(reports_on_user * 12, 36)
        reasons.append(f"жалобы на пользователя: {reports_on_user}")

    if warnings_count > 0:
        score += min(warnings_count * 10, 30)
        reasons.append(f"предупреждения: {warnings_count}")

    if security_events_count > 0:
        score += min(security_events_count * 8, 32)
        reasons.append(f"security events: {security_events_count}")

    if bypass_events_count > 0:
        score += min(bypass_events_count * 15, 45)
        reasons.append(f"попытки увести сделку: {bypass_events_count}")

    if admin_ban_events > 0:
        score += min(admin_ban_events * 15, 30)
        reasons.append(f"история банов/разбанов: {admin_ban_events}")

    if completed_deals >= 5:
        score -= 20
        reasons.append("есть 5+ завершённых сделок")
    elif completed_deals >= 1:
        score -= 10
        reasons.append("есть завершённые сделки")
    else:
        score += 5
        reasons.append("нет завершённых сделок")

    if reviews_count >= 3 and avg_rating >= 4.5:
        score -= 15
        reasons.append("хороший рейтинг")
    elif reviews_count > 0 and avg_rating < 3.5:
        score += 15
        reasons.append("низкий рейтинг")

    score = max(0, min(100, int(score)))

    if score >= 60:
        level = "high"
        badge = "🔴 Высокий риск"
    elif score >= 25:
        level = "medium"
        badge = "🟡 Средний риск"
    else:
        level = "low"
        badge = "🟢 Низкий риск"

    return {
        "score": score,
        "level": level,
        "badge": badge,
        "reasons": reasons,
        "reports_by_user": reports_by_user,
        "reports_on_user": reports_on_user,
        "warnings_count": warnings_count,
        "security_events_count": security_events_count,
        "bypass_events_count": bypass_events_count,
        "admin_ban_events": admin_ban_events,
        "completed_deals": completed_deals,
        "reviews_count": reviews_count,
        "avg_rating": avg_rating,
    }


def security_reasons_text(reasons: list[str], limit: int = 6) -> str:
    if not reasons:
        return "• факторов риска нет"

    visible = reasons[:limit]
    text = "\n".join(f"• {html.escape(reason)}" for reason in visible)
    if len(reasons) > limit:
        text += f"\n• и ещё {len(reasons) - limit}"
    return text

def admin_user_actions_keyboard(actor_id: int, target_id: int) -> InlineKeyboardMarkup:
    """UX-клавиатура карточки пользователя с учётом ролей и реального состояния."""
    buttons: list[list[InlineKeyboardButton]] = []

    with db() as conn:
        real_banned = conn.execute("SELECT 1 FROM banned_users WHERE user_id=?", (target_id,)).fetchone() is not None
        active_mute = conn.execute(
            "SELECT muted_until FROM muted_users WHERE user_id=? AND muted_until>?",
            (target_id, datetime.now().isoformat())
        ).fetchone() is not None
        warning_count = conn.execute("SELECT COUNT(*) FROM admin_warnings WHERE user_id=?", (target_id,)).fetchone()[0]

    profile = get_profile_info(target_id) if "get_profile_info" in globals() else {}
    plus_active = bool(profile.get("plus_active")) if isinstance(profile, dict) else False
    verified = int(profile.get("verified", 0) or 0) if isinstance(profile, dict) else 0

    buttons.append([
        InlineKeyboardButton(text="📦 Объявления", callback_data=f"admin_user_listings:{target_id}"),
        InlineKeyboardButton(text="📌 Заказы", callback_data=f"admin_user_orders:{target_id}"),
    ])

    buttons.append([
        InlineKeyboardButton(text="💼 Сделки", callback_data=f"admin_user_deals:{target_id}"),
        InlineKeyboardButton(text="🛡 Security", callback_data=f"admin_user_security:{target_id}"),
    ])

    buttons.append([
        InlineKeyboardButton(text=f"⚠️ Предупреждения {warning_count}", callback_data=f"admin_user_warnings:{target_id}"),
        InlineKeyboardButton(text="📜 Логи", callback_data=f"admin_user_logs:{target_id}"),
    ])

    if is_admin(actor_id):
        buttons.append([
            InlineKeyboardButton(text="✉️ Написать", callback_data=f"admin_msg_user:{target_id}"),
            InlineKeyboardButton(text="🔎 Найти другого", callback_data="admin_find_user"),
        ])

    status_row = []
    can_verify, _ = can_act(actor_id, target_id, "verify")
    can_unverify, _ = can_act(actor_id, target_id, "unverify")
    can_plus, _ = can_act(actor_id, target_id, "grant_plus")
    can_revoke_plus, _ = can_act(actor_id, target_id, "revoke_plus")

    if can_verify and not verified:
        status_row.append(InlineKeyboardButton(text="✅ Verified", callback_data=f"admin_verify_user:{target_id}"))
    if can_unverify and verified:
        status_row.append(InlineKeyboardButton(text="❌ Снять Verified", callback_data=f"admin_unverify_user:{target_id}"))
    if status_row:
        buttons.append(status_row)

    plus_row = []
    if can_plus:
        plus_row.append(InlineKeyboardButton(text="💎 Plus 30д", callback_data=f"admin_grant_plus_days:{target_id}:30"))
        plus_row.append(InlineKeyboardButton(text="💎 Plus 90д", callback_data=f"admin_grant_plus_days:{target_id}:90"))
    if can_revoke_plus and plus_active:
        plus_row.append(InlineKeyboardButton(text="🧹 Снять Plus", callback_data=f"admin_revoke_plus:{target_id}"))
    if plus_row:
        buttons.append(plus_row)

    punish_row = []
    can_warn, _ = can_act(actor_id, target_id, "warn")
    can_mute, _ = can_act(actor_id, target_id, "mute")
    if can_warn:
        punish_row.append(InlineKeyboardButton(text="⚠️ Варн", callback_data=f"admin_warn_user:{target_id}"))
    if can_mute and not active_mute:
        punish_row.append(InlineKeyboardButton(text="🔇 Мут", callback_data=f"admin_mute_user:{target_id}"))
    if can_mute and active_mute:
        punish_row.append(InlineKeyboardButton(text="🔊 Снять мут", callback_data=f"admin_unmute_user:{target_id}"))
    if punish_row:
        buttons.append(punish_row)

    ban_row = []
    can_ban, _ = can_act(actor_id, target_id, "ban")
    can_unban, _ = can_act(actor_id, target_id, "unban")
    if can_ban and not real_banned:
        ban_row.append(InlineKeyboardButton(text="🚫 Бан", callback_data=f"admin_ban_user:{target_id}"))
    if can_unban and real_banned:
        ban_row.append(InlineKeyboardButton(text="✅ Разбан", callback_data=f"admin_unban_user:{target_id}"))
    if ban_row:
        buttons.append(ban_row)

    role_ok, _ = can_act(actor_id, target_id, "set_role")
    if role_ok:
        buttons.append([InlineKeyboardButton(text="👑 Изменить роль", callback_data=f"admin_role_choose:{target_id}")])

    if is_owner(actor_id) and is_staff(target_id):
        buttons.append([InlineKeyboardButton(text="🚨 Жалобы на админа", callback_data=f"admin_reports_for_admin:{target_id}")])

    buttons.append([
        InlineKeyboardButton(text="⬅️ Пользователи", callback_data="admin_users_page:0"),
        InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def is_banned(user_id: int) -> bool:
    # Staff не должен блокироваться даже если его случайно занесли в banned_users.
    # Дополнительно удаляем ошибочную запись, чтобы проблема не всплывала снова.
    if is_staff(int(user_id)):
        try:
            with db() as conn:
                deleted = conn.execute("DELETE FROM banned_users WHERE user_id=?", (int(user_id),)).rowcount
                conn.commit()
            if deleted:
                log_admin_action(0, "auto_cleanup_staff_ban", int(user_id), "is_banned skipped protected staff")
        except Exception:
            pass
        return False
    with db() as conn:
        return conn.execute("SELECT 1 FROM banned_users WHERE user_id=?", (user_id,)).fetchone() is not None


def get_mute(user_id: int):
    # Owner/admin/moderator не должны блокироваться мутом, даже если старая запись
    # случайно осталась в базе после прошлых версий кода.
    if is_staff(int(user_id)):
        try:
            with db() as conn:
                deleted = conn.execute("DELETE FROM muted_users WHERE user_id=?", (int(user_id),)).rowcount
                conn.commit()
            if deleted:
                log_admin_action(0, "auto_cleanup_staff_mute", int(user_id), "get_mute skipped protected staff")
        except Exception:
            pass
        return None

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


def is_protected_user(user_id: int) -> bool:
    """Пользователь, которого нельзя наказывать автоматикой и обычной админкой."""
    return is_staff(int(user_id)) or is_owner(int(user_id))


def can_auto_punish(user_id: int) -> tuple[bool, str]:
    """Проверка для автомодерации: авто-мут/авто-бан не трогает staff."""
    user_id = int(user_id)
    if is_owner(user_id):
        return False, "Владелец защищён от автоматических наказаний."
    if is_staff(user_id):
        return False, "Админ/модератор защищён от автоматических наказаний."
    return True, ""


def set_mute(user_id: int, minutes: int, reason: str, muted_by: int = 0) -> bool:
    """Безопасная единая точка выдачи мута.

    muted_by=0 — автомодерация.
    Возвращает True, если мут реально выдан.
    """
    user_id = int(user_id)
    muted_by = int(muted_by or 0)

    if muted_by == 0:
        ok, block_reason = can_auto_punish(user_id)
        if not ok:
            log_admin_action(0, "auto_mute_blocked_protected_user", user_id, block_reason)
            return False
    else:
        ok, block_reason = can_act(muted_by, user_id, "mute")
        if not ok:
            log_admin_action(muted_by, "mute_blocked_by_acl", user_id, block_reason)
            return False

    until = datetime.now() + timedelta(minutes=int(minutes))
    with db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO muted_users
            (user_id, muted_until, reason, muted_by, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, until.isoformat(), reason, muted_by, datetime.now().isoformat()),
        )
        conn.commit()

    log_admin_action(muted_by, "mute_user", user_id, f"minutes={minutes}; reason={reason}")
    return True

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
    trust_badge = trust_public_badge(user_id) if 'trust_public_badge' in globals() else stats.get('status', '🆕 Новый пользователь')
    return f"""
━━━━━━━━━━━━━━
👤 <b>Продавец</b>
━━━━━━━━━━━━━━

🆔 ID: <code>{user_id}</code>
🏷 Профиль: <b>{profile_title(user_id)}</b>
🔗 Username: @{html.escape(stats['username'])}
✅ Статус: <b>{trust_public_badge(user_id)}</b>
📝 Описание: {profile_description_text(user_id)}
⭐ Рейтинг: <b>{stats['rating_text']}</b>
💰 Завершённых продаж: <b>{stats['sales_count']}</b>
📦 Активных объявлений: <b>{stats['active_listings']}</b>
🏷 Статус: <b>{trust_badge}</b>
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
    # Staff не получает авто-предупреждения Protect.
    if is_staff(sender_id):
        return

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



def is_channel_configured() -> bool:
    """Канал показываем только если ссылка задана в .env."""
    return TG_CHANNEL_URL.startswith(("http://", "https://", "tg://"))


def channel_button(text: str = "📢 Канал LTeam") -> InlineKeyboardButton | None:
    if not is_channel_configured():
        return None
    return InlineKeyboardButton(text=text, url=TG_CHANNEL_URL)


def maybe_channel_row(text: str = "📢 Канал LTeam") -> list[InlineKeyboardButton]:
    btn = channel_button(text)
    return [btn] if btn else []


def channel_promo_text(context: str = "default") -> str:
    """Мягкий промо-блок. Не агрессивная реклама, а полезная навигация."""
    if not is_channel_configured():
        return ""

    name = html.escape(TG_CHANNEL_NAME)

    if context == "home":
        return f"\n📢 Новости, обновления и полезные анонсы: <b>{name}</b>\n"
    if context == "about":
        return f"\n\n📢 Следите за развитием LTeam, обновлениями и новостями проекта в канале: <b>{name}</b>"
    if context == "listing_sent":
        return f"\n\n📢 В канале <b>{name}</b> мы публикуем обновления, подборки и новости LTeam."
    if context == "order_sent":
        return f"\n\n📢 Следите за обновлениями LTeam и новыми возможностями в канале: <b>{name}</b>."
    if context == "profile":
        return f"\n\n📢 Канал проекта: <b>{name}</b>"
    return f"\n\n📢 Канал LTeam: <b>{name}</b>"


def add_channel_button(rows: list[list[InlineKeyboardButton]], text: str = "📢 Канал LTeam") -> list[list[InlineKeyboardButton]]:
    row = maybe_channel_row(text)
    if row:
        rows.append(row)
    return rows

def main_menu(user_id: int | None = None):
    buttons = []

    if WEBAPP_URL.startswith("http"):
        buttons.append([
            InlineKeyboardButton(text="🚀 Открыть LTeam App", web_app=WebAppInfo(url=WEBAPP_URL))
        ])

    buttons.extend([
        [InlineKeyboardButton(text="🔎 Каталог услуг", callback_data="market")],
        [
            InlineKeyboardButton(text="📦 Разместить услугу", callback_data="create_listing"),
            InlineKeyboardButton(text="🧾 Создать заказ", callback_data="create_order"),
        ],
        [
            InlineKeyboardButton(text="📦 Мои размещения", callback_data="my_listings"),
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

    add_channel_button(buttons, "📢 Канал LTeam")

    if user_id and is_staff(user_id):
        buttons.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def home_market_keyboard() -> InlineKeyboardMarkup | None:
    """Кнопка MiniApp прямо под приветственным баннером."""
    if not WEBAPP_URL.startswith("http"):
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🚀 Открыть LTeam Market",
            web_app=WebAppInfo(url=WEBAPP_URL),
        )
    ]])


def lteam_reply_menu(user_id: int | None = None) -> ReplyKeyboardMarkup:
    """Постоянное нижнее меню Telegram.

    Это не заменяет inline-кнопки внутри экранов, а даёт быстрый доступ
    к главным разделам из любой точки бота.
    """
    rows = [
        [KeyboardButton(text="🔎 Каталог услуг"), KeyboardButton(text="🧾 Заказы")],
        [KeyboardButton(text="📦 Разместить"), KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="🛡 Гарант"), KeyboardButton(text="⚙️ Меню")],
    ]
    if is_channel_configured():
        rows.append([KeyboardButton(text="📢 Канал LTeam")])
    if user_id and is_staff(user_id):
        rows.append([KeyboardButton(text="🛠 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, input_field_placeholder="Выберите раздел LTeam")


def section_reply_menu(back_text: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Временное нижнее меню раздела: оставляет пользователю понятную кнопку возврата."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=back_text)]],
        resize_keyboard=True,
        input_field_placeholder="Нажмите «Назад», чтобы вернуться в главное меню",
    )


async def clear_nav_menu_message(user_id: int) -> None:
    row = NAV_MENU_MESSAGES.pop(int(user_id), None)
    if not row:
        return
    chat_id, message_id = row
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def set_reply_menu_hint_for_message(
    message: Message,
    reply_markup: ReplyKeyboardMarkup,
    text: str = "🧭 Для возврата используйте кнопку «⬅️ Назад» ниже.",
) -> None:
    await clear_nav_menu_message(message.from_user.id)
    try:
        sent = await message.answer(text, reply_markup=reply_markup, disable_notification=True)
    except TypeError:
        sent = await message.answer(text, reply_markup=reply_markup)
    NAV_MENU_MESSAGES[message.from_user.id] = (sent.chat.id, sent.message_id)


async def set_reply_menu_hint_for_call(
    call: CallbackQuery,
    reply_markup: ReplyKeyboardMarkup,
    text: str = "🧭 Для возврата используйте кнопку ниже.",
) -> None:
    await clear_nav_menu_message(call.from_user.id)
    try:
        sent = await bot.send_message(call.message.chat.id, text, reply_markup=reply_markup, disable_notification=True)
    except TypeError:
        sent = await bot.send_message(call.message.chat.id, text, reply_markup=reply_markup)
    NAV_MENU_MESSAGES[call.from_user.id] = (sent.chat.id, sent.message_id)


def section_back_keyboard(*, home: bool = True) -> InlineKeyboardMarkup:
    rows = []
    if home:
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    await clear_nav_menu_message(message.from_user.id)

    text = """
━━━━━━━━━━━━━━
🚀 <b>LTeam Market</b>
━━━━━━━━━━━━━━

Маркет цифровых услуг, заказов и безопасных сделок через гаранта LTeam.

🔎 <b>Каталог услуг</b> — найти исполнителя или цифровой товар
🧾 <b>Заказы</b> — найти задачу или создать свою
📦 <b>Разместить</b> — опубликовать услугу/товар
👤 <b>Профиль</b> — баланс, сделки, выводы

Нижнее меню включено. Выберите раздел кнопками под полем ввода.
"""

    text += channel_promo_text("home")

    if os.path.exists(BANNER_PATH):
        await message.answer_photo(
            FSInputFile(BANNER_PATH),
            caption=text,
            reply_markup=home_market_keyboard(),
            parse_mode="HTML",
        )
        await set_reply_menu_hint_for_message(
            message,
            lteam_reply_menu(message.from_user.id),
            text="⬇️ Основные разделы доступны в нижнем меню.",
        )
    else:
        await screen_answer(message, text, reply_markup=home_market_keyboard(), parse_mode="HTML")
        await set_reply_menu_hint_for_message(
            message,
            lteam_reply_menu(message.from_user.id),
            text="⬇️ Основные разделы доступны в нижнем меню.",
        )


@dp.message(CommandStart())
async def start(message: Message):
    save_user(message)
    await send_home(message)


@dp.message(F.web_app_data)
async def handle_miniapp_action(message: Message, state: FSMContext):
    """Route MiniApp actions back into the existing Telegram bot flows."""
    save_user(message)
    try:
        payload = json.loads(message.web_app_data.data or "{}")
    except (TypeError, json.JSONDecodeError):
        return

    action = str(payload.get("action", ""))
    if action == "withdraw_start":
        await message.answer(
            "Вывод средств открывается в защищённом сценарии бота.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Открыть вывод", callback_data="withdraw_start")
            ]]),
        )
        return

    if action in {"admin_open", "admin_panel"}:
        if not is_admin(message.from_user.id):
            await message.answer("Этот раздел доступен только администраторам.")
            return
        section = "panel" if action == "admin_panel" else str(payload.get("section", ""))
        sections = {
            "payments": ("Оплаты на проверке", "admin_deals_payments"),
            "payouts": ("Выплаты", "admin_deals_payouts"),
            "disputes": ("Споры", "admin_disputes"),
            "panel": ("Админ-панель", "admin_panel"),
        }
        title, callback = sections.get(section, sections["panel"])
        await message.answer(
            f"{title} открываются в боте.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text=title, callback_data=callback)
            ]]),
        )


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


# ===== НИЖНЕЕ МЕНЮ LTEAM =====

async def show_market_from_message(message: Message):
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
        new_count = conn.execute("SELECT COUNT(*) FROM listings WHERE status='active' AND id >= (SELECT COALESCE(MAX(id),0)-20 FROM listings)").fetchone()[0]

    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
🔎 <b>Каталог услуг</b>
━━━━━━━━━━━━━━

Найдите услугу, цифровой товар или исполнителя под вашу задачу.

📦 Активных объявлений: <b>{total}</b>
🆕 Новых объявлений: <b>{new_count}</b>

Выберите удобный способ поиска:
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_start"), InlineKeyboardButton(text="🎯 Подбор", callback_data="market_filter")],
            [InlineKeyboardButton(text="📂 Категории", callback_data="market_categories"), InlineKeyboardButton(text="🆕 Новые", callback_data="market_new")],
            [InlineKeyboardButton(text="🔥 ТОП", callback_data="market_top"), InlineKeyboardButton(text="🛡 LTeam Verified", callback_data="market_verified")],
            [InlineKeyboardButton(text="📦 Разместить услугу", callback_data="create_listing")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="home")],
        ]),
        parse_mode="HTML",
    )


async def show_orders_from_message(message: Message):
    await screen_answer(
        message,
        """
━━━━━━━━━━━━━━
🧾 <b>Заказы клиентов</b>
━━━━━━━━━━━━━━

Здесь заказчики публикуют задачи, а исполнители откликаются.

Примеры:
• нужен Telegram-бот
• нужен логотип
• нужен монтаж
• нужна настройка AI
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Смотреть заказы", callback_data="orders_list")],
            [InlineKeyboardButton(text="🧾 Создать заказ", callback_data="create_order")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="home")],
        ]),
        parse_mode="HTML",
    )


async def show_place_from_message(message: Message):
    await screen_answer(
        message,
        """
━━━━━━━━━━━━━━
📦 <b>Размещение услуги</b>
━━━━━━━━━━━━━━

Создайте объявление, если вы хотите продавать услугу, цифровой товар, шаблон, бота или доступ.

Что лучше писать:
• что именно вы делаете
• что получит покупатель
• срок выполнения
• что нужно от заказчика

Реквизиты для выплаты указывать не нужно — они запрашиваются только при выводе средств.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Создать размещение", callback_data="create_listing")],
            [InlineKeyboardButton(text="👀 Мои объявления", callback_data="my_listings")],
            [InlineKeyboardButton(text="📜 Правила размещения", callback_data="rules")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="home")],
        ]),
        parse_mode="HTML",
    )


async def show_guarantee_from_message(message: Message):
    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
🛡 <b>Гарант LTeam</b>
━━━━━━━━━━━━━━

1. Покупатель и исполнитель обсуждают задачу в чате LTeam.
2. Исполнитель выставляет итоговую цену.
3. Покупатель подтверждает цену.
4. Админ проверяет сделку и разрешает оплату.
5. Покупатель оплачивает по реквизитам LTeam.
6. Деньги замораживаются до выполнения.
7. После подтверждения выполнения деньги идут на баланс исполнителя.

Комиссия сервиса: <b>{COMMISSION_PERCENT}%</b>.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Правила", callback_data="rules"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="home")],
        ]),
        parse_mode="HTML",
    )


@dp.message(F.text.in_({"⬅️ Назад", "🔙 Назад"}))
async def reply_menu_back(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    await send_home(message)


@dp.message(F.text.in_({"🔎 Каталог услуг", "🛒 Маркет"}))
async def reply_menu_market(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    await show_market_from_message(message)
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Каталог услуг». Для возврата нажмите «⬅️ Назад» ниже.")


@dp.message(F.text.in_({"🧾 Заказы", "📌 Заказы"}))
async def reply_menu_orders(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    await show_orders_from_message(message)
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Заказы». Для возврата нажмите «⬅️ Назад» ниже.")


@dp.message(F.text.in_({"📦 Разместить", "➕ Разместить"}))
async def reply_menu_place(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    await show_place_from_message(message)
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Разместить». Для возврата нажмите «⬅️ Назад» ниже.")


@dp.message(F.text == "👤 Профиль")
async def reply_menu_profile(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    ensure_profile_tables()
    await screen_answer(
        message,
        build_beautiful_profile_text(message.from_user.id),
        reply_markup=beautiful_profile_keyboard(message.from_user.id),
        parse_mode="HTML",
    )
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Профиль». Для возврата нажмите «⬅️ Назад» ниже.")


@dp.message(F.text == "🛡 Гарант")
async def reply_menu_guarantee(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    await show_guarantee_from_message(message)
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Гарант». Для возврата нажмите «⬅️ Назад» ниже.")


@dp.message(F.text == "⚙️ Меню")
async def reply_menu_full(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    await screen_answer(
        message,
        """
━━━━━━━━━━━━━━
⚙️ <b>Меню LTeam</b>
━━━━━━━━━━━━━━

Выберите раздел:
""",
        reply_markup=main_menu(message.from_user.id),
        parse_mode="HTML",
    )
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Меню». Для возврата нажмите «⬅️ Назад» ниже.")



@dp.message(F.text == "📢 Канал LTeam")
async def reply_menu_channel(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    rows = []
    add_channel_button(rows, "📢 Перейти в канал")
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="home")])
    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
📢 <b>Канал LTeam</b>
━━━━━━━━━━━━━━

В канале публикуем обновления проекта, новости маркетплейса, полезные материалы и важные объявления для пользователей.
{channel_promo_text()}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML",
    )
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Канал LTeam». Для возврата нажмите «⬅️ Назад» ниже.")

@dp.message(F.text == "🛠 Админ-панель")
async def reply_menu_admin(message: Message, state: FSMContext):
    await state.clear()
    save_user(message)
    if not is_staff(message.from_user.id):
        await screen_answer(message, "Нет доступа.", reply_markup=main_menu(message.from_user.id), parse_mode="HTML")
        return
    await screen_answer(
        message,
        """
━━━━━━━━━━━━━━
🛠 <b>Админ-панель</b>
━━━━━━━━━━━━━━

Откройте админ-панель через кнопку ниже.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠 Открыть админ-панель", callback_data="admin_panel")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await set_reply_menu_hint_for_message(message, section_reply_menu(), "🧭 Открыт раздел «Админ-панель». Для возврата нажмите «⬅️ Назад» ниже.")


@dp.callback_query(F.data == "home")
async def home(call: CallbackQuery):
    await send_home(call.message)
    await call.answer()
    return
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
    await set_reply_menu_hint_for_call(call, lteam_reply_menu(call.from_user.id), "🏠 Главное меню снова доступно на нижних кнопках.")
    await call.answer()


@dp.callback_query(F.data == "about_company")
async def about_company(call: CallbackQuery):
    await show_screen(
        call,
        """
━━━━━━━━━━━━━━
ℹ️ <b>О компании LTeam</b>
━━━━━━━━━━━━━━

<b>LTeam Market</b> — Telegram-маркетплейс цифровых услуг, заказов и безопасных сделок.

Что есть внутри:
• 📦 размещение услуг и цифровых товаров
• 🧾 заказы от клиентов
• 💬 безопасные чаты внутри бота
• 🛡 гарант LTeam
• ⭐ отзывы и рейтинг
• 💸 баланс исполнителя и вывод средств

Наша цель — построить удобную экосистему Telegram-сервисов, где заказчик и исполнитель могут работать безопасно.
""" + channel_promo_text("about"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=add_channel_button([
            [InlineKeyboardButton(text="🛡 Как работает гарант", callback_data="guarantee")],
            [InlineKeyboardButton(text="📜 Правила", callback_data="rules"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ], "📢 Перейти в канал")),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "rules")
async def rules(call: CallbackQuery):
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
📜 <b>Правила LTeam Market</b>
━━━━━━━━━━━━━━

<b>1. Сделки только через LTeam</b>
Не уводите заказ в личные сообщения и не просите оплату напрямую.

<b>2. Оплата только по реквизитам LTeam</b>
Реквизиты появляются только после проверки админом.

<b>3. Комиссия сервиса</b>
Комиссия LTeam: <b>{COMMISSION_PERCENT}%</b>.

<b>4. Запрещено</b>
Обман, фейковые чеки, спам, запрещённые товары/услуги, обход гаранта.

<b>5. Споры</b>
Если возникла проблема — открывайте спор или пишите в поддержку.

Нарушения могут привести к предупреждению, муту или блокировке.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛡 Как работает гарант", callback_data="guarantee")],
            [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "admin_panel")
async def admin_panel(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    ensure_admin_tables()
    now = datetime.now()
    day_ago = now - timedelta(days=1)

    with db() as conn:
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        active_listings = conn.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
        active_orders = conn.execute("SELECT COUNT(*) FROM orders WHERE status='active'").fetchone()[0]
        listing_moderation = conn.execute("SELECT COUNT(*) FROM listings WHERE status='moderation'").fetchone()[0]
        order_moderation = conn.execute("SELECT COUNT(*) FROM orders WHERE status='moderation'").fetchone()[0]
        active_deals = conn.execute("SELECT COUNT(*) FROM deals WHERE status NOT IN ('completed', 'cancelled', 'deleted')").fetchone()[0]
        waiting_payment = conn.execute("SELECT COUNT(*) FROM deals WHERE status='waiting_admin_confirm'").fetchone()[0]
        waiting_payout = conn.execute("SELECT COUNT(*) FROM deals WHERE status='waiting_payout'").fetchone()[0]
        try:
            ensure_finance_tables()
            pending_withdrawals = conn.execute("SELECT COUNT(*) FROM withdrawal_requests WHERE status='pending'").fetchone()[0]
        except Exception:
            pending_withdrawals = 0
        try:
            ensure_pro_tables()
            open_disputes = conn.execute("SELECT COUNT(*) FROM deal_disputes WHERE status='open'").fetchone()[0]
        except Exception:
            open_disputes = 0
        reports_count = conn.execute("SELECT COUNT(*) FROM reports WHERE COALESCE(status, 'new')='new'").fetchone()[0]
        tickets_count = conn.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
        pending_requests = conn.execute("SELECT COUNT(*) FROM admin_action_requests WHERE status='pending'").fetchone()[0]
        security_24h = conn.execute("SELECT COUNT(*) FROM security_events WHERE created_at>=?", (day_ago.isoformat(),)).fetchone()[0]
        active_mutes = conn.execute("SELECT COUNT(*) FROM muted_users WHERE muted_until>?", (now.isoformat(),)).fetchone()[0]
        active_bans = conn.execute("SELECT COUNT(*) FROM banned_users").fetchone()[0]
        warnings_total = conn.execute("SELECT COUNT(*) FROM admin_warnings").fetchone()[0]
        plus_active = conn.execute("SELECT COUNT(*) FROM user_profile_settings WHERE plus_until>?", (now.isoformat(),)).fetchone()[0] if table_exists("user_profile_settings") else 0
        verified_total = conn.execute("SELECT COUNT(*) FROM users WHERE COALESCE(verified,0)=1").fetchone()[0]
        turnover = conn.execute("SELECT COALESCE(SUM(amount), 0) FROM deals WHERE status IN ('completed', 'waiting_payout')").fetchone()[0]
        commission = conn.execute("SELECT COALESCE(SUM(commission), 0) FROM deals WHERE status='completed'").fetchone()[0]

    moderation_total = listing_moderation + order_moderation
    danger_total = reports_count + security_24h + pending_requests + open_disputes

    buttons = [
        [
            InlineKeyboardButton(text="🔎 Быстрый поиск", callback_data="admin_find_user"),
            InlineKeyboardButton(text=f"👥 Пользователи {users_count}", callback_data="admin_users_page:0"),
        ],
        [
            InlineKeyboardButton(text=f"🛡 Security {security_24h}", callback_data="admin_security_center"),
            InlineKeyboardButton(text=f"🚨 Жалобы {reports_count}", callback_data="admin_reports"),
        ],
        [
            InlineKeyboardButton(text=f"⏳ Модерация {moderation_total}", callback_data="admin_moderation"),
            InlineKeyboardButton(text=f"🆘 Поддержка {tickets_count}", callback_data="admin_tickets_v2"),
        ],
        [
            InlineKeyboardButton(text=f"💼 Сделки {active_deals}", callback_data="admin_deals_center"),
            InlineKeyboardButton(text=f"🚨 Споры {open_disputes}", callback_data="admin_disputes"),
        ],
        [
            InlineKeyboardButton(text="💰 Финансы", callback_data="admin_finance_v2"),
            InlineKeyboardButton(text=f"💸 Выводы {pending_withdrawals}", callback_data="admin_withdrawals"),
        ],
        [
            InlineKeyboardButton(text=f"🧾 Чеки {waiting_payment}", callback_data="admin_deals_center"),
            InlineKeyboardButton(text=f"💼 Выплаты {waiting_payout}", callback_data="admin_deals_center"),
        ],
        [
            InlineKeyboardButton(text=f"🚫 Баны {active_bans}", callback_data="admin_bans_list"),
            InlineKeyboardButton(text=f"🔇 Муты {active_mutes}", callback_data="admin_mutes_list_v5"),
        ],
        [
            InlineKeyboardButton(text=f"⚠️ Варны {warnings_total}", callback_data="admin_warnings_list_v5"),
            InlineKeyboardButton(text=f"📨 Заявки {pending_requests}", callback_data="admin_requests_list"),
        ],
    ]

    if is_admin(call.from_user.id):
        buttons.append([
            InlineKeyboardButton(text=f"💎 Plus {plus_active}", callback_data="admin_plus_center_v5"),
            InlineKeyboardButton(text=f"✅ Verified {verified_total}", callback_data="admin_verified_list_v5"),
        ])
        buttons.append([
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast_start"),
            InlineKeyboardButton(text="📜 Логи", callback_data="admin_logs_page:0"),
        ])

    if is_owner(call.from_user.id):
        buttons.append([
            InlineKeyboardButton(text="👑 Роли", callback_data="admin_roles_panel"),
            InlineKeyboardButton(text="🛡 Staff Safety", callback_data="admin_staff_safety"),
        ])
        buttons.append([
            InlineKeyboardButton(text="🧹 Staff cleanup", callback_data="admin_cleanup_staff_punishments"),
        ])

    buttons.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")])

    health = "🟢 Норма"
    if danger_total >= 10:
        health = "🔴 Требует внимания"
    elif danger_total >= 4:
        health = "🟠 Есть задачи"

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
⚙️ <b>LTeam Admin Center V5</b>
━━━━━━━━━━━━━━

👤 Вы: <b>{role_badge(call.from_user.id)}</b>
📌 Состояние: <b>{health}</b>

<b>Что делать сначала:</b>
1) 🚨 Жалобы и Security
2) 📨 Заявки
3) 💼 Сделки/выплаты
4) 👥 Пользователи

📊 <b>Продукт</b>
• Пользователей: <b>{users_count}</b>
• Активных объявлений: <b>{active_listings}</b>
• Активных заказов: <b>{active_orders}</b>
• На модерации: <b>{moderation_total}</b>

🛡 <b>Безопасность</b>
• Жалобы: <b>{reports_count}</b>
• Открытые споры: <b>{open_disputes}</b>
• Security events за 24ч: <b>{security_24h}</b>
• Активные баны/муты: <b>{active_bans}</b> / <b>{active_mutes}</b>
• Предупреждений всего: <b>{warnings_total}</b>
• Заявки на согласование: <b>{pending_requests}</b>

💎 <b>Статусы</b>
• LTeam Plus активных: <b>{plus_active}</b>
• Verified: <b>{verified_total}</b>

💼 <b>Сделки</b>
• Активные: <b>{active_deals}</b>
• Ожидают оплату: <b>{waiting_payment}</b>
• Ожидают выплату: <b>{waiting_payout}</b>
• Оборот: <b>{turnover}₽</b>
• Комиссия завершённых: <b>{commission}₽</b>
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "admin_ban_start")
async def admin_ban_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await ask_admin_user_query(call, state, "ban", "🚫 <b>Бан пользователя</b>", "admin_panel")
    await call.answer()


@dp.message(AdminBanState.user_id)
async def admin_ban_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    rows = find_users_for_admin(message.text or "")
    if not rows:
        await screen_answer(message, "❌ Пользователь не найден. Введите ID, @username или ник.", parse_mode="HTML")
        return
    if len(rows) > 1:
        await state.clear()
        await screen_answer(message, "Найдено несколько пользователей:", reply_markup=user_pick_keyboard(rows, "ban"), parse_mode="HTML")
        return
    user_id = int(rows[0][0])
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
    await ask_admin_user_query(call, state, "unban", "✅ <b>Разбан пользователя</b>", "admin_panel")
    await call.answer()


@dp.message(AdminUnbanState.user_id)
async def admin_unban_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    rows = find_users_for_admin(message.text or "")
    if not rows:
        await screen_answer(message, "❌ Пользователь не найден. Введите ID, @username или ник.", parse_mode="HTML")
        return
    if len(rows) > 1:
        await state.clear()
        await screen_answer(message, "Найдено несколько пользователей:", reply_markup=user_pick_keyboard(rows, "unban"), parse_mode="HTML")
        return
    user_id = int(rows[0][0])
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

    await show_screen(
        call,
        f"<b>LTeam Market</b>\n━━━━━━━━━━━━\n\n"
        f"Активных объявлений: <b>{total}</b>\n"
        f"Новых объявлений: <b>{new_count}</b>\n\n"
        "Найдите услугу, товар или исполнителя.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Поиск", callback_data="search_start"), InlineKeyboardButton(text="Подбор", callback_data="market_filter")],
            [InlineKeyboardButton(text="Категории", callback_data="market_categories"), InlineKeyboardButton(text="Новые", callback_data="market_new")],
            [InlineKeyboardButton(text="Назад", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()
    return

    progress = {
        "discussion": "1/5 — обсуждение условий",
        "waiting_final_price": "1/5 — ожидается итоговая цена",
        "waiting_buyer_price_confirm": "1/5 — покупатель подтверждает цену",
        "waiting_admin_payment_approval": "2/5 — админ проверяет сделку",
        "waiting_payment": "2/5 — ожидается оплата покупателя",
        "waiting_receipt": "2/5 — ожидается чек",
        "waiting_admin_confirm": "2/5 — админ проверяет поступление",
        "in_work": "3/5 — исполнитель выполняет заказ",
        "waiting_buyer_confirm": "4/5 — покупатель проверяет результат",
        "waiting_payout": "5/5 — ожидается ручная выплата",
        "completed": "5/5 — сделка завершена",
        "payment_rejected": "оплата требует повторной проверки",
        "dispute_open": "спор рассматривает администрация",
    }.get(status, "статус обновляется")

    await show_screen(
        call,
        f"""
<b>Сделка #{deal_id}</b>
━━━━━━━━━━━━━━━━

<b>{html.escape(title or 'Без названия')}</b>
Источник: {'заказ' if source_type == 'order' else 'объявление'}

<b>Этап:</b> {progress}
Цена: <b>{amount} ₽</b>
Комиссия LTeam: <b>{commission} ₽</b>
Исполнителю после завершения: <b>{payout} ₽</b>

Покупатель: <code>{buyer_id}</code>
Исполнитель: <code>{seller_id}</code>

<i>Оплата проходит через реквизиты администратора LTeam.</i>
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()
    return

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

    buttons = []
    if call.from_user.id != seller_id:
        buttons.append([InlineKeyboardButton(text="💬 Обсудить заказ", callback_data=f"ask_seller:{listing_id}")])
        buttons.append([InlineKeyboardButton(text="🛡 Сделка через гаранта", callback_data=f"ask_seller:{listing_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="👁 Как видят покупатели", callback_data=f"view_listing:{listing_id}")])

    buttons.extend([
        [InlineKeyboardButton(text="⭐ Отзывы продавца", callback_data=f"listing_reviews:{seller_id}:{listing_id}")],
        [InlineKeyboardButton(text="👤 Профиль продавца", callback_data=f"seller_profile:{seller_id}")],
        [InlineKeyboardButton(text=fav_text, callback_data=f"fav:{listing_id}")],
        [InlineKeyboardButton(text="🚨 Пожаловаться", callback_data=f"report:{listing_id}")],
    ])

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

    await show_screen(
        call,
        listing_public_text(listing_id, seller_id, title, category, item_type, condition, price, description, delivery_time, is_top, is_highlight),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("ask_seller:"))
async def ask_seller(call: CallbackQuery, state: FSMContext):
    listing_id = int(call.data.split(":")[1])
    buyer_id = call.from_user.id

    with db() as conn:
        row = conn.execute(
            "SELECT seller_id, title, price FROM listings WHERE id=? AND status='active'",
            (listing_id,),
        ).fetchone()

    if not row:
        await call.answer("Объявление не найдено", show_alert=True)
        return

    seller_id, title, price = row
    if seller_id == buyer_id:
        await call.answer("Нельзя создать заявку на своё объявление", show_alert=True)
        return

    await state.update_data(listing_id=listing_id, seller_id=seller_id)
    await state.set_state(ListingDiscussionState.message)

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
💬 <b>Обсуждение заказа</b>
━━━━━━━━━━━━━━

Объявление: <b>{html.escape(title or 'Без названия')}</b>
Цена от: <b>{int(price or 0)}₽</b>

Напишите продавцу, что именно вам нужно:
• задача / детали
• желаемый срок
• важные требования

⚠️ Контакты и оплата напрямую запрещены. Общение и оплата — через LTeam.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К объявлению", callback_data=f"view_listing:{listing_id}")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(ListingDiscussionState.message)
async def listing_discussion_message(message: Message, state: FSMContext):
    data = await state.get_data()
    listing_id = int(data.get("listing_id") or 0)
    seller_id = int(data.get("seller_id") or 0)
    buyer_id = message.from_user.id
    text = (message.text or "").strip()

    if not listing_id or not seller_id:
        await state.clear()
        await screen_answer(message, "❌ Заявка не найдена. Откройте объявление заново.")
        return

    if len(text) < 10:
        await screen_answer(
            message,
            "Напишите чуть подробнее, чтобы продавец понял задачу. Минимум 10 символов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К объявлению", callback_data=f"view_listing:{listing_id}")],
            ]),
        )
        await state.clear()
        return

    ok, reason = moderation_check(text, allow_contacts=False)
    if not ok or looks_like_bypass_attempt(text):
        register_security_event(buyer_id, "listing_request_blocked", f"listing #{listing_id}", text, status="blocked")
        await state.clear()
        await screen_answer(
            message,
            "❌ Сообщение заблокировано LTeam Protect. Нельзя указывать контакты, ссылки или просить оплату напрямую.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💬 Написать заново", callback_data=f"ask_seller:{listing_id}")],
                [InlineKeyboardButton(text="⬅️ К объявлению", callback_data=f"view_listing:{listing_id}")],
            ]),
        )
        return

    with db() as conn:
        listing = conn.execute(
            "SELECT title, price FROM listings WHERE id=? AND status='active'",
            (listing_id,),
        ).fetchone()
        if not listing:
            await state.clear()
            await screen_answer(message, "❌ Объявление уже недоступно.")
            return

        title, price = listing
        cur = conn.execute(
            """
            INSERT INTO listing_discussion_requests
            (listing_id, buyer_id, seller_id, message, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'new', ?, ?)
            """,
            (listing_id, buyer_id, seller_id, text, datetime.now().isoformat(), datetime.now().isoformat()),
        )
        request_id = cur.lastrowid
        conn.commit()

    await state.clear()

    await screen_answer(
        message,
        f"""
✅ <b>Заявка отправлена продавцу</b>

Объявление: <b>{html.escape(title or 'Без названия')}</b>

Продавец сможет принять заявку, отклонить её или открыть сделку для обсуждения внутри LTeam.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 К объявлению", callback_data=f"view_listing:{listing_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )

    try:
        await bot.send_message(
            seller_id,
            f"""
━━━━━━━━━━━━━━
💬 <b>Новая заявка по объявлению</b>
━━━━━━━━━━━━━━

📦 Объявление: <b>{html.escape(title or 'Без названия')}</b>
💰 Цена от: <b>{int(price or 0)}₽</b>
👤 Покупатель: <code>{buyer_id}</code>

Сообщение покупателя:
{html.escape(text)}

Если заявка подходит — нажмите «Откликнуться». После этого создастся сделка для безопасного чата.
""",
            reply_markup=listing_discussion_request_keyboard(request_id, listing_id, buyer_id),
            parse_mode="HTML",
        )
    except Exception as e:
        log_admin_action(0, "listing_request_notify_failed", seller_id, f"request_id={request_id}; error={e}")


@dp.callback_query(F.data.startswith("listing_req_reject:"))
async def listing_request_reject(call: CallbackQuery):
    request_id = int(call.data.split(":")[1])

    with db() as conn:
        row = conn.execute(
            "SELECT listing_id, buyer_id, seller_id, status FROM listing_discussion_requests WHERE id=?",
            (request_id,),
        ).fetchone()
        if not row:
            await call.answer("Заявка не найдена", show_alert=True)
            return

        listing_id, buyer_id, seller_id, status = row
        if call.from_user.id != seller_id and not is_admin(call.from_user.id):
            await call.answer("Нет доступа", show_alert=True)
            return
        if status != "new":
            await call.answer("Заявка уже обработана", show_alert=True)
            return

        conn.execute(
            "UPDATE listing_discussion_requests SET status='rejected', updated_at=? WHERE id=?",
            (datetime.now().isoformat(), request_id),
        )
        conn.commit()

    await bot.send_message(
        buyer_id,
        f"❌ Продавец отклонил заявку по объявлению #{listing_id}. Можно выбрать другое объявление в каталоге.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Каталог услуг", callback_data="market")],
        ]),
    )
    await call.message.edit_text(f"❌ Заявка #{request_id} отклонена.")
    await call.answer("Заявка отклонена")


@dp.callback_query(F.data.startswith("listing_req_accept:"))
async def listing_request_accept(call: CallbackQuery):
    request_id = int(call.data.split(":")[1])
    seller_id = call.from_user.id

    with db() as conn:
        row = conn.execute(
            """
            SELECT r.listing_id, r.buyer_id, r.seller_id, r.message, r.status, l.title, l.price
            FROM listing_discussion_requests r
            JOIN listings l ON l.id = r.listing_id
            WHERE r.id=?
            """,
            (request_id,),
        ).fetchone()

        if not row:
            await call.answer("Заявка не найдена", show_alert=True)
            return

        listing_id, buyer_id, db_seller_id, request_message, status, title, price = row

        if seller_id != db_seller_id and not is_admin(seller_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        if status != "new":
            await call.answer("Заявка уже обработана", show_alert=True)
            return

        commission = int((price or 0) * COMMISSION_PERCENT / 100)
        payout = int(price or 0) - commission

        cur = conn.execute(
            """
            INSERT INTO deals (listing_id, buyer_id, seller_id, amount, commission, payout, payment_method, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (listing_id, buyer_id, db_seller_id, int(price or 0), commission, payout, "manual_admin_card", "discussion", datetime.now().isoformat()),
        )
        deal_id = cur.lastrowid

        conn.execute(
            "UPDATE listing_discussion_requests SET status='accepted', deal_id=?, updated_at=? WHERE id=?",
            (deal_id, datetime.now().isoformat(), request_id),
        )
        conn.execute(
            "INSERT INTO deal_messages (deal_id, sender_id, receiver_id, text, created_at) VALUES (?, ?, ?, ?, ?)",
            (deal_id, buyer_id, db_seller_id, request_message, datetime.now().isoformat()),
        )
        conn.commit()

    log_admin_action(db_seller_id, "listing_request_accepted", buyer_id, f"request_id={request_id}; listing_id={listing_id}; deal_id={deal_id}")

    chat_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Открыть чат сделки", callback_data=f"deal_chat:{deal_id}")],
        [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
    ])

    await bot.send_message(
        buyer_id,
        f"""
✅ <b>Продавец откликнулся</b>

Объявление: <b>{html.escape(title or 'Без названия')}</b>
Сделка: <b>#{deal_id}</b>

Теперь обсудите детали в чате сделки. Оплату не переводите напрямую — только через LTeam.
""",
        reply_markup=chat_kb,
        parse_mode="HTML",
    )

    await call.message.edit_text(
        f"""
✅ <b>Заявка принята</b>

Создана сделка: <b>#{deal_id}</b>
Покупатель получил уведомление.

Откройте чат и обсудите детали заказа.
""",
        reply_markup=chat_kb,
        parse_mode="HTML",
    )
    await call.answer("Сделка создана")


@dp.callback_query(F.data.startswith("listing_reviews:"))
async def listing_reviews(call: CallbackQuery):
    _, seller_raw, listing_raw = call.data.split(":")
    seller_id = int(seller_raw)
    listing_id = int(listing_raw)
    stats = seller_stats(seller_id)

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
⭐ <b>Отзывы продавца</b>
━━━━━━━━━━━━━━

👤 Продавец: <code>{seller_id}</code>
⭐ Рейтинг: <b>{stats.get('rating_text', 'нет отзывов')}</b>
💰 Завершённых продаж: <b>{stats.get('sales_count', 0)}</b>

<b>Последние отзывы:</b>
{seller_reviews_text(seller_id, limit=10)}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К объявлению", callback_data=f"view_listing:{listing_id}")],
            [InlineKeyboardButton(text="👤 Профиль продавца", callback_data=f"seller_profile:{seller_id}")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


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


def seller_reviews_text(seller_id: int, limit: int = 3) -> str:
    with db() as conn:
        rows = conn.execute(
            """
            SELECT rating, text, created_at
            FROM reviews
            WHERE seller_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (seller_id, limit),
        ).fetchall()

    if not rows:
        return "Пока нет отзывов. Будьте первым, кто завершит безопасную сделку и оставит отзыв."

    items = []
    for rating, text, created_at in rows:
        stars = "⭐" * int(rating or 0)
        date = str(created_at or "")[:10]
        items.append(f"• {stars} {html.escape(text or 'Без текста')}" + (f" · {html.escape(date)}" if date else ""))
    return "\n".join(items)


def listing_public_text(listing_id: int, seller_id: int, title: str, category: str, item_type: str, condition: str, price: int, description: str, delivery_time: str, is_top: int = 0, is_highlight: int = 0) -> str:
    stats = seller_stats(seller_id)
    return f"""
━━━━━━━━━━━━━━
📦 <b>{html.escape(title or 'Без названия')}</b>
━━━━━━━━━━━━━━

💰 Цена от: <b>{int(price or 0)}₽</b>
📂 Категория: <b>{html.escape(category or '—')}</b>
📌 Формат: <b>{html.escape(item_type or '—')}</b>
⏳ Срок/получение: <b>{html.escape(delivery_time or 'Не указан')}</b>
🧾 Состояние: <b>{html.escape(condition or '—')}</b>

👤 Продавец: <b>{profile_title(seller_id) if 'profile_title' in globals() else seller_id}</b>
⭐ Рейтинг: <b>{stats.get('rating_text', 'нет отзывов')}</b>
🏷 Статус: <b>{trust_public_badge(seller_id) if 'trust_public_badge' in globals() else stats.get('status', 'Новый продавец')}</b>

📝 <b>Описание:</b>
{html.escape(description or 'Без описания')}

🚀 Продвижение: <b>{promo_marker(is_top, is_highlight) or 'обычное'}</b>

🛡 <b>Безопасность:</b> сначала обсудите заказ в боте. Оплата — только через гаранта LTeam.
"""


def listing_discussion_request_keyboard(request_id: int, listing_id: int, buyer_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Откликнуться", callback_data=f"listing_req_accept:{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"listing_req_reject:{request_id}"),
        ],
        [
            InlineKeyboardButton(text="📦 Объявление", callback_data=f"view_listing:{listing_id}"),
            InlineKeyboardButton(text="👤 Покупатель", callback_data=f"seller_profile:{buyer_id}"),
        ],
    ])


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


def listing_nav_keyboard(back_callback: str | None = None, *, home: bool = False, extra_rows=None):
    """Единая навигация мастера размещения: назад / отмена / главное меню."""
    rows = list(extra_rows or [])
    nav = []
    if back_callback:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback))
    nav.append(InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel"))
    rows.append(nav)
    if home:
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cancel_keyboard(extra_rows=None):
    return listing_nav_keyboard(None, extra_rows=extra_rows)


def category_examples_text(category: str) -> str:
    examples = CATEGORY_EXAMPLES.get(category, ["Создам полезную цифровую услугу"])
    return "\n".join([f"<code>{html.escape(x)}</code>" for x in examples])


def item_types_for_category(category: str) -> list[str]:
    return CATEGORY_ITEM_TYPES.get(category, ITEM_TYPES)


def category_type_hint(category: str) -> str:
    return CATEGORY_TYPE_HINTS.get(category, "Выберите формат, который лучше всего описывает объявление.")


@dp.callback_query(F.data == "create_listing")
async def create_listing(call: CallbackQuery, state: FSMContext):
    policy = protect_policy_for_user(call.from_user.id)
    if policy.get("block_create_listing"):
        register_security_event(call.from_user.id, "listing_create_blocked_high_risk", "create_listing", status="blocked")
        await show_screen(
            call,
            protect_block_text(call.from_user.id, "создание объявления"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚖️ Подать апелляцию", callback_data="protect_appeal_start")],
                [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support"), InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ]),
            parse_mode="HTML",
        )
        await notify_admins(f"""
🔴 <b>LTeam Protect: создание объявления заблокировано</b>

Пользователь: <code>{call.from_user.id}</code>
Риск: <b>{policy.get('badge')}</b> / <b>{policy.get('score')}/100</b>
""")
        await call.answer("Действие заблокировано LTeam Protect", show_alert=True)
        return

    await state.clear()
    await state.set_state(CreateListing.category)

    keyboard = listing_nav_keyboard(
        None,
        home=True,
        extra_rows=[[InlineKeyboardButton(text=cat, callback_data=f"cat_create:{cat}")] for cat in CATEGORIES]
    )

    await show_screen(call,
        """
━━━━━━━━━━━━━━
📦 <b>Разместить услугу</b>
━━━━━━━━━━━━━━

Создайте карточку услуги, товара, шаблона или готового решения.
Покупатель сможет открыть объявление, обсудить задачу и провести сделку через гаранта LTeam.

<b>Шаг 1 из 6</b>
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

<b>Шаг 2 из 6</b>
Напишите короткое и понятное название.

💡 <b>Примеры:</b>
{category_examples_text(category)}
""",
        reply_markup=listing_nav_keyboard("listing_back_category"),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "listing_back_category")
async def listing_back_category(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(CreateListing.category)
    keyboard = listing_nav_keyboard(
        None,
        home=True,
        extra_rows=[[InlineKeyboardButton(text=cat, callback_data=f"cat_create:{cat}")] for cat in CATEGORIES]
    )
    await show_screen(call, """
━━━━━━━━━━━━━━
📦 <b>Разместить услугу</b>
━━━━━━━━━━━━━━

<b>Шаг 1 из 6</b>
Выберите категорию:
""", reply_markup=keyboard, parse_mode="HTML")
    await call.answer()


@dp.message(CreateListing.title)
async def listing_title(message: Message, state: FSMContext):
    title = (message.text or "").strip()

    if len(title) < 3:
        await screen_answer(message, "Название слишком короткое. Напишите понятнее.", reply_markup=listing_nav_keyboard("listing_back_category"))
        return
    ok, reason = moderation_check(title)
    if not ok:
        await screen_answer(message, f"🚫 Название не прошло авто-модерацию: {html.escape(reason)}", reply_markup=listing_nav_keyboard("listing_back_category"), parse_mode="HTML")
        await notify_admins(f"⚠️ <b>Авто-модерация объявления</b>\n\nПользователь: <code>{message.from_user.id}</code>\nПричина: {html.escape(reason)}\nТекст: {html.escape(title)}")
        return

    data = await state.get_data()
    category = data.get("category", "🛠 Другое")

    await state.update_data(title=title)
    await state.set_state(CreateListing.item_type)

    keyboard = listing_nav_keyboard(
        "listing_back_title",
        extra_rows=[[InlineKeyboardButton(text=t, callback_data=f"type_create:{t}")] for t in item_types_for_category(category)]
    )

    await screen_answer(message,
        f"""
━━━━━━━━━━━━━━
📦 <b>Формат объявления</b>
━━━━━━━━━━━━━━

📂 Категория: <b>{html.escape(category)}</b>
📌 Название: <b>{html.escape(title)}</b>

<b>Шаг 3 из 6</b>
{html.escape(category_type_hint(category))}
""",
        reply_markup=keyboard,
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "listing_back_title")
async def listing_back_title(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    category = data.get("category", "🛠 Другое")
    await state.set_state(CreateListing.title)
    await show_screen(call, f"""
━━━━━━━━━━━━━━
📝 <b>Название объявления</b>
━━━━━━━━━━━━━━

📂 Категория: <b>{html.escape(category)}</b>

<b>Шаг 2 из 6</b>
Напишите короткое и понятное название.

💡 <b>Примеры:</b>
{category_examples_text(category)}
""", reply_markup=listing_nav_keyboard("listing_back_category"), parse_mode="HTML")
    await call.answer()


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

<b>Шаг 4 из 6</b>
Выберите срок выполнения или получения товара.
""",
        reply_markup=listing_delivery_keyboard(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "listing_back_type")
async def listing_back_type(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    category = data.get("category", "🛠 Другое")
    title = data.get("title", "")
    await state.set_state(CreateListing.item_type)
    keyboard = listing_nav_keyboard(
        "listing_back_title",
        extra_rows=[[InlineKeyboardButton(text=t, callback_data=f"type_create:{t}")] for t in item_types_for_category(category)]
    )
    await show_screen(call, f"""
━━━━━━━━━━━━━━
📦 <b>Формат объявления</b>
━━━━━━━━━━━━━━

📂 Категория: <b>{html.escape(category)}</b>
📌 Название: <b>{html.escape(title)}</b>

<b>Шаг 3 из 6</b>
{html.escape(category_type_hint(category))}
""", reply_markup=keyboard, parse_mode="HTML")
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

<b>Шаг 5 из 6</b>
Введите цену в рублях.

Лимит: от <b>{MIN_ORDER_BUDGET}₽</b> до <b>{MAX_LISTING_PRICE}₽</b>.
Пример: <code>1500</code>
""",
        reply_markup=listing_nav_keyboard("listing_back_delivery"),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "listing_back_delivery")
async def listing_back_delivery(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    item_type = data.get("item_type", "")
    await state.set_state(CreateListing.delivery_time)
    await show_screen(call, f"""
━━━━━━━━━━━━━━
⏳ <b>Срок / доступность</b>
━━━━━━━━━━━━━━

📌 Формат: <b>{html.escape(item_type)}</b>

<b>Шаг 4 из 6</b>
Выберите срок выполнения или получения товара.
""", reply_markup=listing_delivery_keyboard(), parse_mode="HTML")
    await call.answer()


@dp.message(CreateListing.delivery_time)
async def listing_delivery_time(message: Message, state: FSMContext):
    await screen_answer(
        message,
        "⏳ Срок нужно выбрать кнопкой ниже, а не писать текстом.",
        reply_markup=listing_delivery_keyboard(),
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
    await state.set_state(CreateListing.description)

    await screen_answer(message,
        """
━━━━━━━━━━━━━━
🧾 <b>Описание</b>
━━━━━━━━━━━━━━

<b>Шаг 6 из 6</b>
Опишите объявление по шаблону:

• что именно вы сделаете;
• что получит покупатель;
• что нужно от покупателя;
• что не входит в услугу.

Реквизиты для выплаты здесь указывать не нужно — они запрашиваются только при выводе средств.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить описание", callback_data="skip_desc")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="listing_back_price"), InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel")],
        ]),
        parse_mode="HTML"
    )


@dp.message(CreateListing.payout_details)
async def listing_payout_details_legacy(message: Message, state: FSMContext):
    # Старое состояние больше не используется: реквизиты запрашиваются только при выводе.
    await state.set_state(CreateListing.description)
    await screen_answer(message, "Реквизиты при размещении больше не нужны. Опишите объявление.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить описание", callback_data="skip_desc")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="listing_back_price"), InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel")],
    ]), parse_mode="HTML")


@dp.callback_query(F.data == "listing_back_price")
async def listing_back_price(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    delivery_time = data.get("delivery_time", "")
    await state.set_state(CreateListing.price)
    await show_screen(call, f"""
━━━━━━━━━━━━━━
💰 <b>Цена</b>
━━━━━━━━━━━━━━

⏳ Срок: <b>{html.escape(delivery_time)}</b>

<b>Шаг 5 из 6</b>
Введите цену в рублях.

Лимит: от <b>{MIN_ORDER_BUDGET}₽</b> до <b>{MAX_LISTING_PRICE}₽</b>.
Пример: <code>1500</code>
""", reply_markup=listing_nav_keyboard("listing_back_delivery"), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "skip_desc")
async def skip_description(call: CallbackQuery, state: FSMContext):
    await call.answer("Описание услуги обязательно. Опишите, что получит покупатель.", show_alert=True)


@dp.message(CreateListing.description)
async def listing_description(message: Message, state: FSMContext):
    description = (message.text or "").strip()
    if len(description) < 5:
        await screen_answer(message, "Описание обязательно. Напишите хотя бы несколько слов о вашей услуге.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="listing_back_price"), InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel")]]))
        return
    ok, reason = moderation_check(description)
    if not ok:
        await screen_answer(message, f"🚫 Описание не прошло авто-модерацию: {html.escape(reason)}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="listing_back_price"), InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel")]]), parse_mode="HTML")
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

🛡 <b>Безопасность:</b>
Покупатель сможет обсудить заказ и оплатить только через гаранта LTeam.
Реквизиты продавца запрашиваются отдельно при выводе средств.

Отправляем объявление на модерацию?
"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🖼 Добавить обложку", callback_data="listing_add_cover")],
        [InlineKeyboardButton(text="✅ Отправить на модерацию", callback_data="listing_publish")],
        [InlineKeyboardButton(text="⬅️ Изменить описание", callback_data="listing_back_price")],
        [InlineKeyboardButton(text="✏️ Создать заново", callback_data="create_listing")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel")],
    ])
    return text, keyboard


async def listing_preview(call: CallbackQuery, state: FSMContext):
    text, keyboard = build_listing_preview(await state.get_data())
    await show_screen(call, text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query(F.data == "listing_add_cover")
async def listing_add_cover(call: CallbackQuery, state: FSMContext):
    await state.set_state(CreateListing.cover_photo)
    await show_screen(call, "🖼 <b>Обложка услуги</b>\n\nОтправьте одну картинку. Она обязательна для публикации услуги и будет показана в MiniApp.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К предпросмотру", callback_data="listing_preview")], [InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel")]]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "listing_preview")
async def listing_preview_return(call: CallbackQuery, state: FSMContext):
    await listing_preview(call, state)
    await call.answer()


@dp.message(CreateListing.cover_photo, F.photo)
async def listing_cover_photo(message: Message, state: FSMContext):
    await state.update_data(image_data=f"tg:{message.photo[-1].file_id}")
    row = get_screen(message.from_user.id)
    text, keyboard = build_listing_preview(await state.get_data())
    if row:
        try:
            await bot.edit_message_text(text, chat_id=row[0], message_id=row[1], reply_markup=keyboard, parse_mode="HTML")
            return
        except Exception:
            pass
    sent = await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    save_screen(message.from_user.id, sent.chat.id, sent.message_id)


@dp.message(CreateListing.cover_photo)
async def listing_cover_photo_required(message: Message):
    await screen_answer(message, "Отправьте картинку — она будет обложкой услуги.")


@dp.callback_query(F.data == "listing_publish")
async def listing_publish(call: CallbackQuery, state: FSMContext):
    policy = protect_policy_for_user(call.from_user.id)
    if policy.get("block_create_listing"):
        register_security_event(call.from_user.id, "listing_publish_blocked_high_risk", "listing_publish", status="blocked")
        await state.clear()
        await show_screen(
            call,
            protect_block_text(call.from_user.id, "публикация объявления"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚖️ Подать апелляцию", callback_data="protect_appeal_start")],
                [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support"), InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ]),
            parse_mode="HTML",
        )
        await call.answer("Публикация заблокирована LTeam Protect", show_alert=True)
        return

    data = await state.get_data()
    required = ["title", "category", "item_type", "delivery_time", "price", "description", "image_data"]
    if any(k not in data for k in required):
        await call.answer("Данные объявления не найдены. Создайте заново.", show_alert=True)
        return

    description = data.get("description", "Без описания")
    seller_id = call.from_user.id
    seller_contact = user_contact(seller_id)
    seller_policy = protect_policy_for_user(seller_id)

    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO listings (seller_id, title, category, item_type, condition, price, description, seller_requisites, delivery_time, image_data, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            seller_id,
            data["title"],
            data["category"],
            data["item_type"],
            "—",
            data["price"],
            description,
            "",
            data.get("delivery_time", ""),
            data.get("image_data", ""),
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
""" + channel_promo_text("listing_sent"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=add_channel_button([
            [InlineKeyboardButton(text="📦 Мои объявления", callback_data="my_listings")],
            [InlineKeyboardButton(text="➕ Создать ещё", callback_data="create_listing")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ], "📢 Канал LTeam")),
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
🛡 Риск продавца: <b>{seller_policy.get('badge')}</b> / <b>{seller_policy.get('score')}/100</b>

ℹ️ Реквизиты продавца не запрашиваются при размещении. Они будут обязательны только при выводе средств.
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
    with db() as conn:
        row = conn.execute(
            "SELECT seller_id, title, price FROM listings WHERE id=? AND status='active'",
            (listing_id,),
        ).fetchone()
    if not row:
        await call.answer("Объявление не найдено", show_alert=True)
        return
    seller_id, title, price = row
    if seller_id == call.from_user.id:
        await call.answer("Нельзя купить своё объявление", show_alert=True)
        return

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
🛡 <b>Безопасная сделка LTeam</b>
━━━━━━━━━━━━━━

Объявление: <b>{html.escape(title or 'Без названия')}</b>
Цена от: <b>{int(price or 0)}₽</b>

Теперь покупка начинается не с оплаты, а с обсуждения.
Напишите продавцу детали задачи, после принятия заявки откроется чат сделки.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Обсудить заказ", callback_data=f"ask_seller:{listing_id}")],
            [InlineKeyboardButton(text="⬅️ К объявлению", callback_data=f"view_listing:{listing_id}")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


# ===== СБП =====


@dp.callback_query(F.data.startswith("pay_sbp:"))
async def pay_sbp(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    await show_screen(
        call,
        """
💳 <b>Оплата изменилась</b>

Теперь оплата проходит только через карту/СБП администратора LTeam и только после проверки сделки админом.

Откройте сделку и следуйте кнопкам: итоговая цена → подтверждение → проверка админом → реквизиты.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("pay_crypto:"))
async def pay_crypto(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    await pay_sbp(call)


@dp.callback_query(F.data.startswith("pay_stars:"))
async def pay_stars(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    await pay_sbp(call)



@dp.callback_query(F.data.startswith("receipt:"))
async def receipt_start(call: CallbackQuery, state: FSMContext):
    deal_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        row = conn.execute("SELECT buyer_id, amount, status FROM deals WHERE id=?", (deal_id,)).fetchone()

    if not row:
        await call.answer("Сделка не найдена", show_alert=True)
        return

    buyer_id, amount, status = row
    if user_id != buyer_id:
        await call.answer("Чек может отправить только покупатель.", show_alert=True)
        return

    if status != "waiting_payment":
        await call.answer("Сначала админ должен разрешить оплату.", show_alert=True)
        return

    await state.update_data(deal_id=deal_id)
    await state.set_state(ReceiptState.receipt)

    await show_screen(
        call,
        f"""
📎 <b>Отправьте чек оплаты</b>

Сделка: <b>#{deal_id}</b>
Сумма: <b>{int(amount or 0)}₽</b>

Можно отправить фото, документ или текст с данными перевода.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Сделка", callback_data=f"deal:{deal_id}")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(ReceiptState.receipt)
async def receipt_save(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = int(data["deal_id"])

    if message.photo:
        receipt_text = "📷 Пользователь отправил фото чека"
    elif message.document:
        receipt_text = "📎 Пользователь отправил документ"
    else:
        receipt_text = message.text or "Пользователь отправил чек"

    with db() as conn:
        deal = conn.execute(
            """
            SELECT d.buyer_id, d.seller_id, d.amount, d.commission, d.payout, d.payment_method,
                   COALESCE(d.order_id, 0), COALESCE(l.title, o.title, 'Сделка LTeam')
            FROM deals d
            LEFT JOIN listings l ON l.id=d.listing_id
            LEFT JOIN orders o ON o.id=d.order_id
            WHERE d.id=?
            """,
            (deal_id,),
        ).fetchone()

        if not deal:
            await screen_answer(message, "❌ Сделка не найдена.")
            await state.clear()
            return

        buyer_id, seller_id, amount, commission, payout, method, order_id, title = deal

        if message.from_user.id != buyer_id:
            await screen_answer(message, "❌ Чек может отправить только покупатель.")
            await state.clear()
            return

        conn.execute(
            "UPDATE deals SET receipt=?, status=? WHERE id=?",
            (receipt_text, "waiting_admin_confirm", deal_id),
        )
        conn.commit()

    await state.clear()

    await screen_answer(message,
        f"""
━━━━━━━━━━━━━━
✅ <b>Чек отправлен</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>
Сумма: <b>{int(amount or 0)}₽</b>

⏳ Админ проверит поступление денег. Если сумма пришла полностью, сделка перейдёт в работу.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML"
    )

    admin_text = f"""
━━━━━━━━━━━━━━
🔔 <b>Чек на проверку</b>
━━━━━━━━━━━━━━

📦 Сделка: <b>#{deal_id}</b>
📌 Название: <b>{html.escape(title or 'Без названия')}</b>

👤 Покупатель: <code>{buyer_id}</code>
👷 Исполнитель: <code>{seller_id}</code>

💰 Должно прийти: <b>{int(amount or 0)}₽</b>
🧾 Комиссия LTeam: <b>{int(commission or 0)}₽</b>
💸 После завершения к зачислению исполнителю: <b>{int(payout or 0)}₽</b>

💳 Способ: <b>карта/СБП админу</b>

🧾 Чек / данные оплаты:
{html.escape(receipt_text)}

Проверьте поступление полной суммы. Если пришла не вся сумма — отклоните чек.
"""

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Деньги пришли полностью", callback_data=f"admin_pay_ok:{deal_id}"),
            InlineKeyboardButton(text="❌ Сумма не пришла", callback_data=f"admin_pay_no:{deal_id}"),
        ],
        [
            InlineKeyboardButton(text="👤 Покупатель", callback_data=f"admin_user:{buyer_id}"),
            InlineKeyboardButton(text="👷 Исполнитель", callback_data=f"admin_user:{seller_id}"),
        ],
        ([InlineKeyboardButton(text="👀 Чат заказа", callback_data=f"admin_order_chat:{order_id}")] if int(order_id or 0) else [InlineKeyboardButton(text="👀 Чат сделки", callback_data=f"admin_deal_chat:{deal_id}")]),
        [InlineKeyboardButton(text="📦 Сделка", callback_data=f"admin_deal_v2:{deal_id}")],
    ])

    await notify_admins(admin_text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("admin_pay_ok:"))
async def admin_pay_ok(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id, seller_id, amount, commission, payout, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return

        buyer_id, seller_id, amount, commission, payout, status = deal
        if status != "waiting_admin_confirm":
            await call.answer(f"Сделка сейчас в статусе: {status}", show_alert=True)
            return

        conn.execute("UPDATE deals SET status='in_work' WHERE id=?", (deal_id,))
        conn.commit()

    freeze_deal_funds(seller_id, deal_id, int(payout or 0))
    log_admin_action(call.from_user.id, "payment_confirmed_full_amount", buyer_id, f"deal_id={deal_id}; amount={amount}; commission={commission}; payout={payout}")

    chat_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")],
        [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
    ])

    await bot.send_message(
        buyer_id,
        f"✅ Оплата по сделке #{deal_id} подтверждена. Деньги заморожены у гаранта LTeam. Исполнитель может начинать работу.",
        reply_markup=chat_keyboard,
        parse_mode="HTML",
    )
    await bot.send_message(
        seller_id,
        f"✅ Оплата по сделке #{deal_id} подтверждена. Можно начинать выполнение. После подтверждения покупателем вам будет зачислено <b>{int(payout or 0)}₽</b>.",
        reply_markup=chat_keyboard,
        parse_mode="HTML",
    )
    await call.message.edit_text(
        f"✅ Оплата по сделке #{deal_id} подтверждена.\n\n💰 Получено: {int(amount or 0)}₽\n🧾 Комиссия: {int(commission or 0)}₽\n💸 Исполнителю после завершения: {int(payout or 0)}₽",
        parse_mode="HTML",
    )
    await call.answer("Оплата подтверждена")


@dp.callback_query(F.data.startswith("admin_pay_no:"))
async def admin_pay_no(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    deal_id = int(call.data.split(":")[1])

    with db() as conn:
        deal = conn.execute(
            "SELECT buyer_id, seller_id, amount, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()

        if not deal:
            await call.answer("Сделка не найдена", show_alert=True)
            return

        buyer_id, seller_id, amount, status = deal
        conn.execute("UPDATE deals SET status='waiting_payment', receipt=NULL, payment_admin_comment=? WHERE id=?", ("Чек отклонён: сумма не пришла полностью", deal_id))
        conn.commit()

    log_admin_action(call.from_user.id, "payment_receipt_rejected", buyer_id, f"deal_id={deal_id}; expected_amount={amount}; old_status={status}")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Реквизиты и повторная отправка чека", callback_data=f"deal:{deal_id}")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
    ])

    await bot.send_message(
        buyer_id,
        f"❌ Чек по сделке #{deal_id} отклонён. Админ не увидел полную сумму <b>{int(amount or 0)}₽</b>. Проверьте перевод и отправьте чек повторно.",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await bot.send_message(
        seller_id,
        f"⚠️ По сделке #{deal_id} чек покупателя отклонён: полная сумма ещё не подтверждена. Работу начинать не нужно.",
        parse_mode="HTML",
    )
    await call.message.edit_text(f"❌ Чек по сделке #{deal_id} отклонён. Покупатель уведомлён.", parse_mode="HTML")
    await call.answer("Чек отклонён")


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
                   d.payout, d.payment_method, d.status,
                   COALESCE(l.title, o.title, 'Сделка LTeam') AS title,
                   COALESCE(d.order_id, 0) AS order_id,
                   COALESCE(d.source_type, CASE WHEN COALESCE(d.order_id,0)>0 THEN 'order' ELSE 'listing' END) AS source_type
            FROM deals d
            LEFT JOIN listings l ON l.id = d.listing_id
            LEFT JOIN orders o ON o.id = d.order_id
            WHERE d.id=?
            """,
            (deal_id,),
        ).fetchone()

    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return

    (
        deal_id, buyer_id, seller_id, amount, commission,
        payout, payment_method, status, title, order_id, source_type,
    ) = deal

    if user_id not in [buyer_id, seller_id] and not is_admin(user_id):
        await call.answer("Нет доступа", show_alert=True)
        return

    amount = int(amount or 0)
    commission = int(commission or 0)
    payout = int(payout or 0)
    status_human = deal_status_title(status)

    buttons: list[list[InlineKeyboardButton]] = []

    if int(order_id or 0) > 0:
        other_id = seller_id if user_id == buyer_id else buyer_id
        buttons.append([InlineKeyboardButton(text="💬 Чат заказа", callback_data=f"order_chat:{order_id}:{other_id}")])

    if status in ["discussion", "waiting_final_price", "waiting_buyer_price_confirm", "waiting_admin_payment_approval", "waiting_payment", "waiting_receipt", "waiting_admin_confirm", "in_work", "waiting_buyer_confirm", "waiting_payout", "completed", "payment_rejected"]:
        buttons.append([InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")])

    if user_id == seller_id and status in ["discussion", "waiting_final_price", "payment_rejected"]:
        buttons.append([InlineKeyboardButton(text="💰 Выставить итоговую цену", callback_data=f"deal_set_final_price:{deal_id}")])

    if user_id == buyer_id and status == "waiting_buyer_price_confirm":
        buttons.append([
            InlineKeyboardButton(text="✅ Подтвердить цену", callback_data=f"buyer_confirm_price:{deal_id}"),
            InlineKeyboardButton(text="❌ Обсудить ещё", callback_data=f"buyer_reject_price:{deal_id}"),
        ])

    if user_id == buyer_id and status == "waiting_payment":
        buttons.append([InlineKeyboardButton(text="💳 Реквизиты / Я оплатил", callback_data=f"show_payment_details:{deal_id}")])

    if user_id == seller_id and status == "in_work":
        buttons.append([InlineKeyboardButton(text="📦 Отметить выполненным", callback_data=f"seller_done:{deal_id}")])

    if user_id == buyer_id and status == "waiting_buyer_confirm":
        buttons.append([InlineKeyboardButton(text="✅ Подтвердить выполнение", callback_data=f"buyer_done:{deal_id}")])
        buttons.append([InlineKeyboardButton(text="❌ Есть проблема", callback_data=f"deal_dispute:{deal_id}")])

    if status in ["in_work", "waiting_admin_confirm", "waiting_receipt", "waiting_buyer_confirm", "waiting_payment"]:
        buttons.append([InlineKeyboardButton(text="🚨 Открыть спор", callback_data=f"deal_dispute:{deal_id}")])

    if is_admin(user_id):
        admin_row = [InlineKeyboardButton(text="👀 Админ: читать чат", callback_data=f"admin_deal_chat:{deal_id}")]
        buttons.append(admin_row)
        if status == "waiting_admin_payment_approval":
            buttons.append([
                InlineKeyboardButton(text="✅ Разрешить оплату", callback_data=f"admin_allow_payment:{deal_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment:{deal_id}"),
            ])
        if status == "waiting_admin_confirm":
            buttons.append([
                InlineKeyboardButton(text="✅ Деньги пришли", callback_data=f"admin_pay_ok:{deal_id}"),
                InlineKeyboardButton(text="❌ Сумма не пришла", callback_data=f"admin_pay_no:{deal_id}"),
            ])

    buttons.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="home")])

    if status == "waiting_payment" and user_id == buyer_id:
        payment_hint = lteam_card_payment_text(deal_id, amount)
    else:
        payment_hint = "🛡 Оплата будет доступна только после подтверждения итоговой цены и проверки админом."

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
📦 <b>Сделка #{deal_id}</b>
━━━━━━━━━━━━━━

Источник: <b>{'заказ' if source_type == 'order' else 'объявление'}</b>
Название: <b>{html.escape(title or 'Без названия')}</b>

👤 Покупатель: <code>{buyer_id}</code>
👷 Исполнитель: <code>{seller_id}</code>

💰 Стоимость: <b>{amount}₽</b>
🧾 Комиссия LTeam: <b>{commission}₽</b>
💸 Исполнителю после завершения: <b>{payout}₽</b>

📌 Статус: <b>{html.escape(status_human)}</b>
💳 Оплата: <b>карта/СБП админу</b>

{payment_hint}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("show_payment_details:"))
async def show_payment_details(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, amount, status FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not row:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    buyer_id, seller_id, amount, status = row
    if user_id != buyer_id:
        await call.answer("Реквизиты видит только покупатель.", show_alert=True)
        return
    if status != "waiting_payment":
        await call.answer("Оплата пока недоступна.", show_alert=True)
        return
    await show_screen(call, lteam_card_payment_text(deal_id, int(amount or 0)), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"receipt:{deal_id}")],
        [InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")],
        [InlineKeyboardButton(text="⬅️ Сделка", callback_data=f"deal:{deal_id}")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("deal_set_final_price:"))
async def deal_set_final_price_start(call: CallbackQuery, state: FSMContext):
    deal_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    with db() as conn:
        row = conn.execute("SELECT seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not row:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    seller_id, status = row
    if user_id != seller_id and not is_admin(user_id):
        await call.answer("Итоговую цену выставляет исполнитель.", show_alert=True)
        return
    if status not in ["discussion", "waiting_final_price", "payment_rejected"]:
        await call.answer("Сейчас нельзя менять цену.", show_alert=True)
        return
    await state.update_data(deal_id=deal_id)
    await state.set_state(DealFinalPriceState.amount)
    await show_screen(call, f"💰 <b>Итоговая цена сделки #{deal_id}</b>\n\nВведите сумму в ₽ одним числом. Например: <code>2500</code>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Сделка", callback_data=f"deal:{deal_id}")]
    ]), parse_mode="HTML")
    await call.answer()


@dp.message(DealFinalPriceState.amount)
async def deal_set_final_price_save(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = int(data.get("deal_id") or 0)
    amount = parse_money(message.text or "")
    if amount is None:
        await screen_answer(message, "❌ Введите сумму числом, например: <code>2500</code>", parse_mode="HTML")
        return
    ok, reason = validate_application_price(amount)
    if not ok:
        await screen_answer(message, f"❌ {html.escape(reason)}", parse_mode="HTML")
        return
    total, commission, payout = money_parts(amount)
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await screen_answer(message, "❌ Сделка не найдена.")
            await state.clear()
            return
        buyer_id, seller_id, status = row
        if message.from_user.id != seller_id and not is_admin(message.from_user.id):
            await screen_answer(message, "❌ Итоговую цену выставляет исполнитель.")
            await state.clear()
            return
        conn.execute(
            """
            UPDATE deals
            SET amount=?, commission=?, payout=?, payment_method='admin_card_only',
                status='waiting_buyer_price_confirm', final_price_set_by=?
            WHERE id=?
            """,
            (total, commission, payout, message.from_user.id, deal_id),
        )
        conn.commit()
    await state.clear()
    kb_buyer = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить цену", callback_data=f"buyer_confirm_price:{deal_id}"),
            InlineKeyboardButton(text="❌ Обсудить ещё", callback_data=f"buyer_reject_price:{deal_id}"),
        ],
        [InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")],
        [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")],
    ])
    await bot.send_message(
        buyer_id,
        f"""
💰 <b>Исполнитель выставил итоговую цену</b>

Сделка: <b>#{deal_id}</b>
Стоимость: <b>{total}₽</b>
Комиссия LTeam: <b>{commission}₽</b>
Исполнителю после завершения: <b>{payout}₽</b>

Подтвердите цену, если всё обсудили.
""",
        reply_markup=kb_buyer,
        parse_mode="HTML",
    )
    await screen_answer(message, f"✅ Итоговая цена {total}₽ отправлена покупателю на подтверждение.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")]
    ]), parse_mode="HTML")


@dp.callback_query(F.data.startswith("buyer_confirm_price:"))
async def buyer_confirm_price(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, amount, commission, payout, status, COALESCE(order_id,0) FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True)
            return
        buyer_id, seller_id, amount, commission, payout, status, order_id = row
        if user_id != buyer_id:
            await call.answer("Подтвердить цену может только покупатель.", show_alert=True)
            return
        if status != "waiting_buyer_price_confirm":
            await call.answer("Цена уже обработана или статус изменился.", show_alert=True)
            return
        conn.execute("UPDATE deals SET status='waiting_admin_payment_approval', final_price_confirmed_by=?, payment_requested_by=? WHERE id=?", (user_id, user_id, deal_id))
        conn.commit()

    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Разрешить оплату", callback_data=f"admin_allow_payment:{deal_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment:{deal_id}"),
        ],
        [
            InlineKeyboardButton(text="👤 Покупатель", callback_data=f"admin_user:{buyer_id}"),
            InlineKeyboardButton(text="👷 Исполнитель", callback_data=f"admin_user:{seller_id}"),
        ],
        ([InlineKeyboardButton(text="👀 Чат заказа", callback_data=f"admin_order_chat:{order_id}")] if int(order_id or 0) else [InlineKeyboardButton(text="👀 Чат сделки", callback_data=f"admin_deal_chat:{deal_id}")]),
        [InlineKeyboardButton(text="📦 Сделка", callback_data=f"admin_deal_v2:{deal_id}")],
    ])
    await notify_admins(f"""
🛡 <b>Запрос на разрешение оплаты</b>

Сделка: <b>#{deal_id}</b>
Покупатель: <code>{buyer_id}</code>
Исполнитель: <code>{seller_id}</code>

💰 Покупатель должен оплатить: <b>{int(amount or 0)}₽</b>
🧾 Комиссия LTeam: <b>{int(commission or 0)}₽</b>
💸 Исполнителю после завершения: <b>{int(payout or 0)}₽</b>

Проверьте чат и профили. Если всё нормально — разрешите оплату.
""", reply_markup=admin_kb)
    await bot.send_message(seller_id, f"✅ Покупатель подтвердил цену по сделке #{deal_id}. Ожидаем разрешение оплаты админом.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")]]))
    await show_screen(call, "⏳ Цена подтверждена. Запрос оплаты отправлен админу LTeam. После проверки вам придут реквизиты.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")]]), parse_mode="HTML")
    await call.answer("Отправлено админу")


@dp.callback_query(F.data.startswith("buyer_reject_price:"))
async def buyer_reject_price(call: CallbackQuery):
    deal_id = int(call.data.split(":")[1])
    user_id = call.from_user.id
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True)
            return
        buyer_id, seller_id, status = row
        if user_id != buyer_id:
            await call.answer("Нет доступа", show_alert=True)
            return
        conn.execute("UPDATE deals SET status='discussion' WHERE id=?", (deal_id,))
        conn.commit()
    await bot.send_message(seller_id, f"↩️ Покупатель хочет ещё обсудить цену по сделке #{deal_id}.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")], [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")]]))
    await show_screen(call, "Ок, вернули сделку в обсуждение. Напишите исполнителю в чат, что нужно изменить.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")]]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_allow_payment:"))
async def admin_allow_payment(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    deal_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, amount, commission, payout, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True)
            return
        buyer_id, seller_id, amount, commission, payout, status = row
        if status != "waiting_admin_payment_approval":
            await call.answer(f"Нельзя разрешить оплату в статусе: {status}", show_alert=True)
            return
        conn.execute("UPDATE deals SET status='waiting_payment', payment_approved_by=?, payment_approved_at=? WHERE id=?", (call.from_user.id, datetime.now().isoformat(), deal_id))
        conn.commit()
    log_admin_action(call.from_user.id, "payment_allowed", buyer_id, f"deal_id={deal_id}; amount={amount}; commission={commission}; payout={payout}")
    buyer_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"receipt:{deal_id}")],
        [InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}"), InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")],
    ])
    await bot.send_message(buyer_id, f"✅ <b>Оплата разрешена</b>\n\n{lteam_card_payment_text(deal_id, int(amount or 0))}", reply_markup=buyer_kb, parse_mode="HTML")
    await bot.send_message(seller_id, f"⏳ По сделке #{deal_id} покупателю выданы реквизиты LTeam. Начинайте работу только после подтверждения оплаты админом.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")]]), parse_mode="HTML")
    await call.message.edit_text(f"✅ Оплата по сделке #{deal_id} разрешена. Покупателю отправлены реквизиты.\n\n💰 К оплате: {int(amount or 0)}₽\n🧾 Комиссия: {int(commission or 0)}₽\n💸 Исполнителю после завершения: {int(payout or 0)}₽", parse_mode="HTML")
    await call.answer("Оплата разрешена")


@dp.callback_query(F.data.startswith("admin_reject_payment:"))
async def admin_reject_payment(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    deal_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, amount, status FROM deals WHERE id=?", (deal_id,)).fetchone()
        if not row:
            await call.answer("Сделка не найдена", show_alert=True)
            return
        buyer_id, seller_id, amount, status = row
        conn.execute("UPDATE deals SET status='payment_rejected', payment_rejected_by=?, payment_rejected_at=?, payment_admin_comment=? WHERE id=?", (call.from_user.id, datetime.now().isoformat(), "Админ отклонил разрешение оплаты", deal_id))
        conn.commit()
    log_admin_action(call.from_user.id, "payment_request_rejected", buyer_id, f"deal_id={deal_id}; amount={amount}; old_status={status}")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")], [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")]])
    await bot.send_message(buyer_id, f"❌ Оплата по сделке #{deal_id} пока отклонена админом. Деньги отправлять не нужно. Обсудите детали с исполнителем или обратитесь в поддержку.", reply_markup=kb, parse_mode="HTML")
    await bot.send_message(seller_id, f"❌ Оплата по сделке #{deal_id} отклонена админом. Работу начинать не нужно. Можно обсудить детали и выставить цену заново.", reply_markup=kb, parse_mode="HTML")
    await call.message.edit_text(f"❌ Оплата по сделке #{deal_id} отклонена. Участники уведомлены.", parse_mode="HTML")
    await call.answer("Отклонено")


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
    ensure_finance_tables()
    credited = False

    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
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

        updated = conn.execute(
            "UPDATE deals SET status=? WHERE id=? AND status='waiting_buyer_confirm'",
            ("completed", deal_id),
        ).rowcount
        if updated != 1:
            await call.answer("Статус сделки уже изменился. Обновите экран.", show_alert=True)
            return

        credited = _credit_seller_balance_in_conn(
            conn,
            seller_id,
            deal_id,
            int(payout or 0),
            comment=f"Сделка #{deal_id} завершена покупателем",
        )
        conn.commit()

    if credited:
        log_admin_action(0, "seller_balance_credited", seller_id, f"deal_id={deal_id}; payout={int(payout or 0)}")
    else:
        log_admin_action(0, "seller_balance_credit_duplicate_skipped", seller_id, f"deal_id={deal_id}; payout={int(payout or 0)}")

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
✅ <b>Сделка завершена</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>

Вы подтвердили выполнение. Деньги зачислены на баланс исполнителя.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⭐ Оценить исполнителя", callback_data=f"review_rating:{deal_id}:5")],
            [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )

    await bot.send_message(
        seller_id,
        f"""
✅ <b>Покупатель подтвердил выполнение</b>

Сделка: <b>#{deal_id}</b>
На ваш баланс зачислено: <b>{int(payout or 0)}₽</b>

Вы можете вывести средства в профиле. Выплата вручную, срок до <b>2 дней</b>.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 Баланс", callback_data="balance")],
            [InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw_start")],
            [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")],
        ]),
        parse_mode="HTML",
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



# ===== PRO CORE: СПОРЫ, УМНЫЕ УВЕДОМЛЕНИЯ, ПРОДАВЦЫ =====

def ensure_pro_tables() -> None:
    """PRO-механики LTeam: споры, решения админов, улучшенный аудит."""
    with db() as conn:
        cur = conn.cursor()
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS seller_levels_cache (
            user_id INTEGER PRIMARY KEY,
            level_code TEXT,
            level_title TEXT,
            score INTEGER DEFAULT 0,
            updated_at TEXT
        )
        """)
        conn.commit()


def seller_level(user_id: int) -> dict:
    """Уровень исполнителя для карточек и админ-уведомлений."""
    with db() as conn:
        completed = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE seller_id=? AND status='completed'",
            (user_id,),
        ).fetchone()[0]
        rating_row = conn.execute(
            "SELECT COUNT(*), COALESCE(AVG(rating), 0) FROM reviews WHERE seller_id=?",
            (user_id,),
        ).fetchone()
        disputes = conn.execute(
            "SELECT COUNT(*) FROM deal_disputes WHERE seller_id=? AND status!='resolved_seller'",
            (user_id,),
        ).fetchone()[0] if table_exists('deal_disputes') else 0
        reports = conn.execute(
            """
            SELECT COUNT(*)
            FROM reports r
            LEFT JOIN listings l ON l.id=r.listing_id
            WHERE l.seller_id=?
            """,
            (user_id,),
        ).fetchone()[0]

    reviews_count = int(rating_row[0] or 0)
    avg_rating = float(rating_row[1] or 0)
    score = completed * 10 + reviews_count * 3 + int(avg_rating * 5) - disputes * 12 - reports * 8

    if is_staff(user_id):
        code, title = "official", "👑 Official LTeam"
    elif completed >= 20 and avg_rating >= 4.8 and disputes == 0:
        code, title = "top", "🏆 Топ исполнитель"
    elif completed >= 5 and avg_rating >= 4.5:
        code, title = "trusted", "✅ Проверенный исполнитель"
    elif completed >= 1:
        code, title = "seller", "📈 Исполнитель"
    else:
        code, title = "new", "🆕 Новичок"

    return {
        "code": code,
        "title": title,
        "score": max(0, int(score)),
        "completed": int(completed or 0),
        "reviews_count": reviews_count,
        "avg_rating": avg_rating,
        "disputes": int(disputes or 0),
        "reports": int(reports or 0),
    }


def deal_admin_summary(deal_id: int) -> dict | None:
    with db() as conn:
        row = conn.execute(
            """
            SELECT d.id, d.buyer_id, d.seller_id, d.amount, d.commission, d.payout,
                   d.status, COALESCE(l.title, o.title, 'Сделка LTeam') AS title,
                   COALESCE(d.order_id, 0) AS order_id
            FROM deals d
            LEFT JOIN listings l ON l.id=d.listing_id
            LEFT JOIN orders o ON o.id=d.order_id
            WHERE d.id=?
            """,
            (deal_id,),
        ).fetchone()
    if not row:
        return None
    did, buyer_id, seller_id, amount, commission, payout, status, title, order_id = row
    buyer_risk = get_user_security_score(buyer_id)
    seller_risk = get_user_security_score(seller_id)
    seller_lvl = seller_level(seller_id)
    return {
        "deal_id": did,
        "buyer_id": buyer_id,
        "seller_id": seller_id,
        "amount": int(amount or 0),
        "commission": int(commission or 0),
        "payout": int(payout or 0),
        "status": status,
        "title": title,
        "order_id": int(order_id or 0),
        "buyer_risk": buyer_risk,
        "seller_risk": seller_risk,
        "seller_level": seller_lvl,
    }


def smart_admin_deal_keyboard(deal_id: int, buyer_id: int, seller_id: int, *, order_id: int = 0, dispute_id: int | None = None) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="👤 Покупатель", callback_data=f"admin_user:{buyer_id}"),
            InlineKeyboardButton(text="👷 Исполнитель", callback_data=f"admin_user:{seller_id}"),
        ],
        [
            InlineKeyboardButton(text="📦 Сделка", callback_data=f"admin_deal_view_v2:{deal_id}"),
            InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"admin_deal_chat:{deal_id}"),
        ],
    ]
    if order_id:
        rows.append([InlineKeyboardButton(text="👀 Чат заказа", callback_data=f"admin_order_chat:{order_id}")])
    if dispute_id:
        rows.append([
            InlineKeyboardButton(text="↩️ Вернуть покупателю", callback_data=f"admin_dispute_refund:{dispute_id}"),
            InlineKeyboardButton(text="✅ Отдать исполнителю", callback_data=f"admin_dispute_seller:{dispute_id}"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def notify_admins_deal_event(title: str, deal_id: int, body: str = "", *, important: bool = False, dispute_id: int | None = None):
    info = deal_admin_summary(deal_id)
    if not info:
        await notify_admins(f"{title}\n\nСделка #{deal_id} не найдена.")
        return
    prefix = "🚨 <b>ВАЖНО</b>\n\n" if important else ""
    text = f"""
{prefix}{title}

📦 Сделка: <b>#{deal_id}</b>
📌 Название: <b>{html.escape(info['title'] or 'Без названия')}</b>
📍 Статус: <b>{html.escape(deal_status_title(info['status']))}</b>

👤 Покупатель: <code>{info['buyer_id']}</code>
Риск: <b>{html.escape(info['buyer_risk'].get('badge', ''))}</b> · Score: <b>{info['buyer_risk'].get('score', 0)}/100</b>

👷 Исполнитель: <code>{info['seller_id']}</code>
Уровень: <b>{html.escape(info['seller_level']['title'])}</b>
Риск: <b>{html.escape(info['seller_risk'].get('badge', ''))}</b> · Score: <b>{info['seller_risk'].get('score', 0)}/100</b>

💰 Сумма: <b>{info['amount']}₽</b>
🧾 Комиссия: <b>{info['commission']}₽</b>
💸 Исполнителю: <b>{info['payout']}₽</b>

{body}
"""
    await notify_admins(
        text,
        reply_markup=smart_admin_deal_keyboard(
            deal_id,
            info["buyer_id"],
            info["seller_id"],
            order_id=info.get("order_id", 0),
            dispute_id=dispute_id,
        ),
    )


def open_deal_dispute(deal_id: int, opened_by: int, reason: str) -> tuple[bool, str, int | None, tuple | None]:
    ensure_pro_tables()
    with db() as conn:
        row = conn.execute(
            "SELECT buyer_id, seller_id, status FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()
        if not row:
            return False, "Сделка не найдена.", None, None
        buyer_id, seller_id, status = row
        if opened_by not in (buyer_id, seller_id) and not is_admin(opened_by):
            return False, "Нет доступа к сделке.", None, None
        if status in ("completed", "cancelled", "deleted"):
            return False, "По завершённой или отменённой сделке спор открыть нельзя.", None, None
        exists = conn.execute(
            "SELECT id FROM deal_disputes WHERE deal_id=? AND status='open'",
            (deal_id,),
        ).fetchone()
        if exists:
            return False, f"По этой сделке уже открыт спор #{exists[0]}.", int(exists[0]), (buyer_id, seller_id, status)
        cur = conn.execute(
            """
            INSERT INTO deal_disputes (deal_id, opened_by, buyer_id, seller_id, reason, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'open', ?)
            """,
            (deal_id, opened_by, buyer_id, seller_id, reason, datetime.now().isoformat()),
        )
        dispute_id = int(cur.lastrowid)
        conn.execute("UPDATE deals SET status='dispute_open' WHERE id=?", (deal_id,))
        conn.commit()
    log_admin_action(opened_by, "deal_dispute_opened", deal_id, f"dispute_id={dispute_id}; reason={reason}")
    return True, "ok", dispute_id, (buyer_id, seller_id, status)


@dp.callback_query(F.data.startswith("deal_dispute:"))
async def deal_dispute(call: CallbackQuery, state: FSMContext):
    deal_id = int(call.data.split(":")[1])
    with db() as conn:
        row = conn.execute("SELECT buyer_id, seller_id, status FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not row:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    buyer_id, seller_id, status = row
    if call.from_user.id not in (buyer_id, seller_id) and not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await state.update_data(dispute_deal_id=deal_id)
    await state.set_state(DisputeState.reason)
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
🚨 <b>Открыть спор</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>
Статус: <b>{html.escape(deal_status_title(status))}</b>

Опишите проблему одним сообщением:
• что было обещано;
• что пошло не так;
• какой результат вы хотите получить.

Админ увидит чат сделки, профили сторон и риск-профили участников.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Сделка", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(DisputeState.reason)
async def deal_dispute_reason_save(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = int(data.get("dispute_deal_id") or 0)
    reason = (message.text or "").strip()
    if len(reason) < 10:
        await screen_answer(message, "❌ Опишите проблему подробнее. Минимум 10 символов.", parse_mode="HTML")
        return
    if len(reason) > 1500:
        reason = reason[:1500]
    ok, msg, dispute_id, participants = open_deal_dispute(deal_id, message.from_user.id, reason)
    await state.clear()
    if not ok:
        await screen_answer(
            message,
            f"❌ {html.escape(msg)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ]),
            parse_mode="HTML",
        )
        return
    buyer_id, seller_id, old_status = participants
    await screen_answer(
        message,
        f"""
🚨 <b>Спор открыт</b>

Сделка: <b>#{deal_id}</b>
Спор: <b>#{dispute_id}</b>

Администратор получил уведомление и проверит чат сделки.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    other_id = seller_id if message.from_user.id == buyer_id else buyer_id
    try:
        await bot.send_message(
            other_id,
            f"🚨 По сделке #{deal_id} открыт спор. Админ LTeam проверит ситуацию.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📦 Сделка", callback_data=f"deal:{deal_id}")],
                [InlineKeyboardButton(text="💬 Чат сделки", callback_data=f"deal_chat:{deal_id}")],
            ]),
            parse_mode="HTML",
        )
    except Exception:
        pass
    await notify_admins_deal_event(
        "🚨 <b>Открыт спор по сделке</b>",
        deal_id,
        body=f"<b>Причина:</b>\n{html.escape(reason)}",
        important=True,
        dispute_id=dispute_id,
    )


@dp.callback_query(F.data == "admin_disputes")
async def admin_disputes(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    ensure_pro_tables()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, deal_id, opened_by, buyer_id, seller_id, reason, created_at
            FROM deal_disputes
            WHERE status='open'
            ORDER BY id ASC
            LIMIT 20
            """,
        ).fetchall()
    if not rows:
        await show_screen(call, "🚨 <b>Споры</b>\n\nОткрытых споров нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]]), parse_mode="HTML")
        await call.answer()
        return
    text = "🚨 <b>Открытые споры</b>\n\n"
    buttons = []
    for did, deal_id, opened_by, buyer_id, seller_id, reason, created_at in rows:
        text += f"• <b>#{did}</b> · сделка <b>#{deal_id}</b> · открыл <code>{opened_by}</code> · <code>{short_time(created_at)}</code>\n"
        buttons.append([InlineKeyboardButton(text=f"#{did} · сделка #{deal_id}", callback_data=f"admin_dispute_view:{did}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_dispute_view:"))
async def admin_dispute_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    dispute_id = int(call.data.split(":")[1])
    ensure_pro_tables()
    with db() as conn:
        row = conn.execute(
            """
            SELECT id, deal_id, opened_by, buyer_id, seller_id, reason, status, created_at
            FROM deal_disputes
            WHERE id=?
            """,
            (dispute_id,),
        ).fetchone()
    if not row:
        await call.answer("Спор не найден", show_alert=True)
        return
    did, deal_id, opened_by, buyer_id, seller_id, reason, status, created_at = row
    risk_buyer = get_user_security_score(buyer_id)
    risk_seller = get_user_security_score(seller_id)
    history = format_chat_history(get_deal_chat_history(deal_id, limit=8), current_user_id=call.from_user.id, limit_note="последние 8")
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
🚨 <b>Спор #{did}</b>
━━━━━━━━━━━━━━

Сделка: <b>#{deal_id}</b>
Открыл: <code>{opened_by}</code>
Статус: <b>{html.escape(status or '')}</b>
Создан: <code>{short_time(created_at)}</code>

👤 Покупатель: <code>{buyer_id}</code> · {html.escape(risk_buyer.get('badge', ''))} · {risk_buyer.get('score', 0)}/100
👷 Исполнитель: <code>{seller_id}</code> · {html.escape(risk_seller.get('badge', ''))} · {risk_seller.get('score', 0)}/100

<b>Причина:</b>
{html.escape(reason or 'Не указана')}

<b>Чат сделки:</b>
{history}
""",
        reply_markup=smart_admin_deal_keyboard(deal_id, buyer_id, seller_id, dispute_id=did),
        parse_mode="HTML",
    )
    await call.answer()


def resolve_dispute(dispute_id: int, admin_id: int, resolution: str) -> tuple[bool, str, tuple | None]:
    ensure_pro_tables()
    ensure_finance_tables()
    credited = False

    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT deal_id, buyer_id, seller_id, status FROM deal_disputes WHERE id=?",
            (dispute_id,),
        ).fetchone()
        if not row:
            return False, "Спор не найден.", None

        deal_id, buyer_id, seller_id, status = row
        if status != "open":
            return False, "Спор уже обработан.", row

        deal = conn.execute("SELECT payout FROM deals WHERE id=?", (deal_id,)).fetchone()
        payout = int(deal[0] or 0) if deal else 0
        new_dispute_status = "resolved_buyer" if resolution == "buyer" else "resolved_seller"
        new_deal_status = "dispute_resolved_buyer" if resolution == "buyer" else "completed"

        conn.execute(
            """
            UPDATE deal_disputes
            SET status=?, resolved_by=?, resolution=?, resolved_at=?
            WHERE id=? AND status='open'
            """,
            (new_dispute_status, admin_id, resolution, datetime.now().isoformat(), dispute_id),
        )
        conn.execute("UPDATE deals SET status=? WHERE id=?", (new_deal_status, deal_id))

        if resolution == "seller" and payout > 0:
            credited = _credit_seller_balance_in_conn(
                conn,
                seller_id,
                deal_id,
                payout,
                comment=f"Спор #{dispute_id} решён в пользу исполнителя",
            )

        conn.commit()

    log_admin_action(
        admin_id,
        f"dispute_resolved_{resolution}",
        deal_id,
        f"dispute_id={dispute_id}; payout={payout}; seller_credit={'yes' if credited else 'no'}",
    )
    return True, "ok", (deal_id, buyer_id, seller_id, payout)

@dp.callback_query(F.data.startswith("admin_dispute_refund:"))
async def admin_dispute_refund(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    dispute_id = int(call.data.split(":")[1])
    ok, msg, row = resolve_dispute(dispute_id, call.from_user.id, "buyer")
    if not ok:
        await call.answer(msg, show_alert=True)
        return
    deal_id, buyer_id, seller_id, payout = row
    await bot.send_message(buyer_id, f"↩️ Спор #{dispute_id} по сделке #{deal_id} решён в вашу пользу. Админ LTeam свяжется по возврату, если оплата уже была внесена.")
    await bot.send_message(seller_id, f"🚨 Спор #{dispute_id} по сделке #{deal_id} решён в пользу покупателя. Средства исполнителю не зачислены.")
    await show_screen(call, f"↩️ Спор #{dispute_id} решён в пользу покупателя.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚨 Все споры", callback_data="admin_disputes")], [InlineKeyboardButton(text="📦 Сделка", callback_data=f"admin_deal_view_v2:{deal_id}")]]), parse_mode="HTML")
    await call.answer("Решено")


@dp.callback_query(F.data.startswith("admin_dispute_seller:"))
async def admin_dispute_seller(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    dispute_id = int(call.data.split(":")[1])
    ok, msg, row = resolve_dispute(dispute_id, call.from_user.id, "seller")
    if not ok:
        await call.answer(msg, show_alert=True)
        return
    deal_id, buyer_id, seller_id, payout = row
    await bot.send_message(buyer_id, f"✅ Спор #{dispute_id} по сделке #{deal_id} решён в пользу исполнителя. Сделка закрыта.")
    await bot.send_message(seller_id, f"✅ Спор #{dispute_id} по сделке #{deal_id} решён в вашу пользу. На баланс зачислено: <b>{int(payout or 0)}₽</b>.", parse_mode="HTML")
    await show_screen(call, f"✅ Спор #{dispute_id} решён в пользу исполнителя. Зачислено: <b>{int(payout or 0)}₽</b>.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚨 Все споры", callback_data="admin_disputes")], [InlineKeyboardButton(text="📦 Сделка", callback_data=f"admin_deal_view_v2:{deal_id}")]]), parse_mode="HTML")
    await call.answer("Решено")


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

    if status not in ["discussion", "waiting_final_price", "waiting_buyer_price_confirm", "waiting_admin_payment_approval", "waiting_payment", "waiting_receipt", "waiting_admin_confirm", "in_work", "waiting_buyer_confirm", "waiting_payout", "completed", "payment_rejected"]:
        await call.answer("Чат сделки пока недоступен", show_alert=True)
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

    protect_ok, protect_reason = await protect_check_outgoing_message(message.from_user.id, text, f"deal #{deal_id}")
    if not protect_ok:
        await screen_answer(
            message,
            html.escape(protect_reason),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💬 Написать заново", callback_data=f"deal_chat:{deal_id}")],
                [InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")],
            ]),
            parse_mode="HTML",
        )
        return

    await protect_notify_partner_if_needed(message.from_user.id, receiver_id, f"deal #{deal_id}")

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
            [InlineKeyboardButton(text="💬 Центр чатов", callback_data="admin_chat_hint")],
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
━━━━━━━━━━━━━━
🆘 <b>Поддержка LTeam</b>
━━━━━━━━━━━━━━

Здесь можно создать обращение по оплате, сделке, объявлению или спору.

Активных обращений: <b>{open_count}</b>

Если LTeam Protect ограничил действие, используйте отдельную апелляцию — так владельцы увидят её быстрее.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать обращение", callback_data="ticket_create")],
            [InlineKeyboardButton(text="📂 Мои обращения", callback_data="my_tickets")],
            [InlineKeyboardButton(text="⚖️ Апелляция Protect", callback_data="protect_appeal_start")],
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





# ===== LTEAM PROFILE SYSTEM V1 =====

FREE_PROFILE_EMOJIS = ["👤", "🚀", "💼", "🎨", "🤖", "🧠", "✍️", "🛠", "📦", "⭐"]
PREMIUM_PROFILE_EMOJIS = ["💎", "🔥", "👑", "⚡", "🦾", "🧩", "🌟", "🏆", "🛡", "🧬"]
LTEAM_PLUS_DAYS_DEFAULT = 30


def ensure_profile_tables() -> None:
    """Миграции для профилей, подписки, эмодзи и заявок на галочку."""
    with db() as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_profile_settings (
            user_id INTEGER PRIMARY KEY,
            profile_emoji TEXT DEFAULT '👤',
            display_name TEXT,
            profile_description TEXT,
            plus_until TEXT,
            updated_at TEXT
        )
        """)

        for column_sql in [
            "profile_emoji TEXT DEFAULT '👤'",
            "display_name TEXT",
            "profile_description TEXT",
            "plus_until TEXT",
            "updated_at TEXT",
        ]:
            add_column_if_missing(cur, "user_profile_settings", column_sql)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS verification_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            reason TEXT,
            status TEXT DEFAULT 'pending',
            reviewed_by INTEGER,
            decision TEXT,
            created_at TEXT,
            reviewed_at TEXT
        )
        """)

        for column_sql in [
            "user_id INTEGER",
            "reason TEXT",
            "status TEXT DEFAULT 'pending'",
            "reviewed_by INTEGER",
            "decision TEXT",
            "created_at TEXT",
            "reviewed_at TEXT",
        ]:
            add_column_if_missing(cur, "verification_requests", column_sql)

        try:
            cur.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
        except sqlite3.OperationalError:
            pass

        conn.commit()


def get_profile_info(user_id: int) -> dict:
    ensure_profile_tables()
    now = datetime.now()
    with db() as conn:
        user_row = conn.execute("""
            SELECT COALESCE(username, ''), COALESCE(verified, 0), COALESCE(display_name, '')
            FROM users
            WHERE user_id=?
        """, (user_id,)).fetchone()

        row = conn.execute("""
            SELECT COALESCE(profile_emoji, '👤'), COALESCE(display_name, ''),
                   COALESCE(profile_description, ''), plus_until
            FROM user_profile_settings
            WHERE user_id=?
        """, (user_id,)).fetchone()

    username = user_row[0] if user_row else ""
    verified = int(user_row[1] or 0) if user_row else 0
    user_display_name = user_row[2] if user_row else ""

    if row:
        emoji, display_name, description, plus_until = row
    else:
        emoji, display_name, description, plus_until = "👤", "", "", None

    if not display_name:
        display_name = user_display_name or (f"@{username}" if username else f"ID {user_id}")

    plus_active = False
    plus_days_left = 0
    if plus_until:
        try:
            plus_dt = datetime.fromisoformat(plus_until)
            plus_active = plus_dt > now
            plus_days_left = max(0, (plus_dt - now).days)
        except Exception:
            plus_active = False

    return {
        "user_id": user_id,
        "username": username,
        "verified": verified,
        "emoji": emoji or "👤",
        "display_name": display_name,
        "description": description or "",
        "plus_until": plus_until,
        "plus_active": plus_active,
        "plus_days_left": plus_days_left,
        "plus_badge": "💎 LTeam Plus" if plus_active else "⚪ Обычный профиль",
    }


def profile_title(user_id: int) -> str:
    info = get_profile_info(user_id)
    check = " ✅" if info["verified"] else ""
    return f"{info['emoji']} {html.escape(info['display_name'])}{check}"


def can_use_premium_profile_features(user_id: int) -> bool:
    info = get_profile_info(user_id)
    return bool(info.get("plus_active") or is_staff(user_id))


def set_user_plus(user_id: int, days: int, actor_id: int | None = None) -> str:
    ensure_profile_tables()
    base = datetime.now()
    info = get_profile_info(user_id)
    if info.get("plus_until"):
        try:
            current_until = datetime.fromisoformat(info["plus_until"])
            if current_until > base:
                base = current_until
        except Exception:
            pass
    until = base + timedelta(days=days)
    with db() as conn:
        conn.execute("""
            INSERT INTO user_profile_settings (user_id, plus_until, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET plus_until=excluded.plus_until, updated_at=excluded.updated_at
        """, (user_id, until.isoformat(), datetime.now().isoformat()))
        conn.commit()
    if actor_id:
        log_admin_action(actor_id, "grant_lteam_plus", user_id, f"days={days}; until={until.isoformat()}")
    return until.isoformat()


def profile_description_text(user_id: int) -> str:
    info = get_profile_info(user_id)
    if info["description"]:
        return html.escape(info["description"])
    if can_use_premium_profile_features(user_id):
        return "Описание ещё не заполнено."
    return "Описание доступно с подпиской LTeam Plus."


def profile_settings_keyboard(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎭 Выбрать эмодзи", callback_data="profile_emoji")],
        [InlineKeyboardButton(text="📝 Описание профиля", callback_data="profile_description_start")],
        [InlineKeyboardButton(text="✅ Подать на галочку", callback_data="verification_request_start")],
        [InlineKeyboardButton(text="💎 LTeam Plus", callback_data="lteam_plus_info")],
        [InlineKeyboardButton(text="⬅️ Профиль", callback_data="profile")],
    ])


def emoji_keyboard(user_id: int):
    info = get_profile_info(user_id)
    plus = bool(info.get("plus_active") or is_staff(user_id))
    rows = []
    current = info.get("emoji") or "👤"

    free_buttons = []
    for emoji in FREE_PROFILE_EMOJIS:
        label = f"{emoji} ✓" if emoji == current else emoji
        free_buttons.append(InlineKeyboardButton(text=label, callback_data=f"profile_set_emoji:{emoji}"))
        if len(free_buttons) == 5:
            rows.append(free_buttons)
            free_buttons = []
    if free_buttons:
        rows.append(free_buttons)

    premium_buttons = []
    for emoji in PREMIUM_PROFILE_EMOJIS:
        label = f"{emoji} ✓" if emoji == current else emoji
        cb = f"profile_set_emoji:{emoji}" if plus else "lteam_plus_info"
        premium_buttons.append(InlineKeyboardButton(text=label, callback_data=cb))
        if len(premium_buttons) == 5:
            rows.append(premium_buttons)
            premium_buttons = []
    if premium_buttons:
        rows.append(premium_buttons)

    rows.append([InlineKeyboardButton(text="⬅️ Настройки профиля", callback_data="profile_settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_beautiful_profile_text(user_id: int) -> str:
    info = get_profile_info(user_id)
    security = get_user_security_score(user_id)
    policy = protect_policy_for_user(user_id)

    with db() as conn:
        listings_count = conn.execute("SELECT COUNT(*) FROM listings WHERE seller_id=? AND status='active'", (user_id,)).fetchone()[0]
        purchases_count = conn.execute("SELECT COUNT(*) FROM deals WHERE buyer_id=?", (user_id,)).fetchone()[0]
        sales_count = conn.execute("SELECT COUNT(*) FROM deals WHERE seller_id=? AND status='completed'", (user_id,)).fetchone()[0]
        rating_row = conn.execute("SELECT AVG(rating), COUNT(*) FROM reviews WHERE seller_id=?", (user_id,)).fetchone()
        pending_verification = conn.execute("SELECT id FROM verification_requests WHERE user_id=? AND status='pending' ORDER BY id DESC LIMIT 1", (user_id,)).fetchone()

    avg_rating, reviews_count = rating_row
    rating_text = "нет отзывов" if avg_rating is None else f"{avg_rating:.1f} ⭐ ({reviews_count})"
    username_text = f"@{html.escape(info['username'])}" if info.get("username") else "не указан"
    verification_text = "✅ Верифицирован" if info["verified"] else ("⏳ Заявка на проверке" if pending_verification else "❌ Нет галочки")

    return f"""
━━━━━━━━━━━━━━
{profile_title(user_id)}
━━━━━━━━━━━━━━

🆔 ID: <code>{user_id}</code>
🔗 Username: <b>{username_text}</b>
✅ Галочка: <b>{verification_text}</b>
💎 Подписка: <b>{info['plus_badge']}</b>{f" — {info['plus_days_left']} дн." if info['plus_active'] else ""}
🏷 Trust Badge: <b>{trust_public_badge(user_id)}</b>
🛡 Protect: <b>{policy.get('badge')}</b>
📊 Trust Score: <b>{security.get('score')}/100</b>

━━━━━━━━━━━━━━
📝 <b>Описание</b>
━━━━━━━━━━━━━━
{profile_description_text(user_id)}

━━━━━━━━━━━━━━
📈 <b>Активность</b>
━━━━━━━━━━━━━━
🛒 Покупок: <b>{purchases_count}</b>
🏪 Продаж: <b>{sales_count}</b>
📌 Активных объявлений: <b>{listings_count}</b>
⭐ Рейтинг продавца: <b>{rating_text}</b>
💰 Баланс к выводу: <b>{get_user_balance(user_id)['available']}₽</b>
🧊 В обработке: <b>{get_user_balance(user_id)['frozen']}₽</b>{channel_promo_text("profile")}
"""


def beautiful_profile_keyboard(user_id: int):
    rows = [
        [InlineKeyboardButton(text="⚙️ Настроить профиль", callback_data="profile_settings")],
        [InlineKeyboardButton(text="🛡 Trust Passport", callback_data="trust_passport")],
        [InlineKeyboardButton(text="🛡 Статус Protect", callback_data="protect_status"), InlineKeyboardButton(text="⚖️ Апелляция", callback_data="protect_appeal_start")],
        [InlineKeyboardButton(text="📦 Мои покупки", callback_data="my_purchases"), InlineKeyboardButton(text="⭐ Избранное", callback_data="favorites")],
        [InlineKeyboardButton(text="💰 Баланс", callback_data="balance"), InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw_start")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
    ]
    add_channel_button(rows, "📢 Канал LTeam")
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ===== LTEAM TRUST PASSPORT V1 =====

def trust_score_bar(score: int) -> str:
    score = max(0, min(100, int(score or 0)))
    filled = max(0, min(10, round(score / 10)))
    return "█" * filled + "░" * (10 - filled)


def trust_public_badge(user_id: int) -> str:
    """Короткий публичный бейдж доверия для карточек, объявлений и заказов."""
    try:
        info = get_profile_info(user_id)
        if int(info.get("verified") or 0):
            return "✅ Верифицирован"
    except Exception:
        pass

    if is_staff(user_id):
        return "👑 Official LTeam"

    policy = protect_policy_for_user(user_id)
    level = policy.get("level")
    if level == "appeal_approved":
        return "🟢 Protect approved"
    if level == "high":
        return "🔴 Высокий риск"
    if level == "medium":
        return "🟡 Требует внимания"
    return "🟢 Надёжный"


def trust_recommendations(user_id: int, security: dict | None = None) -> list[str]:
    security = security or get_user_security_score(user_id)
    tips = []

    if security.get("deals_completed", 0) <= 0:
        tips.append("завершите первую сделку через гаранта LTeam")
    if security.get("reviews_count", 0) < 3:
        tips.append("получите несколько честных отзывов")
    if security.get("bypass_events_count", 0) > 0:
        tips.append("не отправляйте контакты и не уводите сделки в личку")
    if security.get("warnings_count", 0) > 0:
        tips.append("избегайте нарушений правил и спорных формулировок")
    if not is_staff(user_id):
        try:
            if not int(seller_stats(user_id).get("verified") or 0):
                tips.append("поддерживайте чистую историю, чтобы получить доверие LTeam")
        except Exception:
            pass

    if not tips:
        tips.append("продолжайте проводить сделки только через LTeam")
    return tips[:5]


def trust_passport_text(user_id: int, *, admin_view: bool = False) -> str:
    security = get_user_security_score(user_id)
    policy = protect_policy_for_user(user_id)
    stats = seller_stats(user_id)
    override_active, override_reason = has_active_protect_override(user_id)

    score = int(security.get("score", 0))
    public_badge = trust_public_badge(user_id)
    rating = stats.get("rating_text", "нет отзывов")
    sales_count = int(stats.get("sales_count") or 0)
    active_listings = int(stats.get("active_listings") or 0)
    verified = int(stats.get("verified") or 0)

    reasons = security.get("reasons") or []
    reasons_text = security_reasons_text(reasons, limit=6)
    tips_text = "\n".join([f"• {html.escape(tip)}" for tip in trust_recommendations(user_id, security)])

    protect_status = policy.get("badge", security.get("badge", "🟢 Низкий риск"))
    if override_active:
        protect_status = f"🟢 Апелляция одобрена — {html.escape(override_reason or 'временное доверие')}"

    title = "🛡 <b>Trust Passport LTeam</b>" if not admin_view else "🛡 <b>Admin Trust View</b>"

    admin_extra = ""
    if admin_view:
        admin_extra = f"""
━━━━━━━━━━━━━━
🧩 <b>Админ-данные</b>
━━━━━━━━━━━━━━

🚨 Жалобы: <b>{security.get('reports_count', 0)}</b>
⚠️ Предупреждения: <b>{security.get('warnings_count', 0)}</b>
🛡 Security events: <b>{security.get('security_events_count', 0)}</b>
🚫 Попытки обхода гаранта: <b>{security.get('bypass_events_count', 0)}</b>
"""

    return f"""
━━━━━━━━━━━━━━
{title}
━━━━━━━━━━━━━━

👤 Пользователь: <code>{user_id}</code>
🏷 Публичный бейдж: <b>{public_badge}</b>
🛡 Protect: <b>{protect_status}</b>
📊 Trust Score: <b>{score}/100</b>
<code>{trust_score_bar(score)}</code>

━━━━━━━━━━━━━━
📈 <b>Репутация</b>
━━━━━━━━━━━━━━

⭐ Рейтинг: <b>{html.escape(str(rating))}</b>
✅ Завершённых продаж: <b>{sales_count}</b>
📦 Активных объявлений: <b>{active_listings}</b>
🛡 Verified: <b>{'да' if verified else 'нет'}</b>

━━━━━━━━━━━━━━
🧠 <b>Факторы доверия</b>
━━━━━━━━━━━━━━

{reasons_text}

━━━━━━━━━━━━━━
💡 <b>Как повысить доверие</b>
━━━━━━━━━━━━━━

{tips_text}
{admin_extra}
"""


def trust_passport_keyboard(user_id: int, *, admin_view: bool = False):
    if admin_view:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Админ-логи", callback_data=f"admin_user_logs:{user_id}"), InlineKeyboardButton(text="🛡 Security events", callback_data=f"admin_user_security:{user_id}")],
            [InlineKeyboardButton(text="🚨 Жалобы", callback_data=f"admin_user_reports:{user_id}"), InlineKeyboardButton(text="💬 Чаты", callback_data=f"admin_user_chats:{user_id}")],
            [InlineKeyboardButton(text="👤 Карточка пользователя", callback_data=f"admin_user:{user_id}")],
            [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="trust_passport")],
        [InlineKeyboardButton(text="🛡 Статус Protect", callback_data="protect_status"), InlineKeyboardButton(text="⚖️ Апелляция", callback_data="protect_appeal_start")],
        [InlineKeyboardButton(text="⬅️ Профиль", callback_data="profile")],
    ])


@dp.callback_query(F.data == "trust_passport")
async def trust_passport(call: CallbackQuery):
    await show_screen(
        call,
        trust_passport_text(call.from_user.id, admin_view=False),
        reply_markup=trust_passport_keyboard(call.from_user.id, admin_view=False),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin_trust_passport:"))
async def admin_trust_passport(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "view_user")
    if not ok:
        await call.answer(reason, show_alert=True)
        return
    await show_screen(
        call,
        trust_passport_text(user_id, admin_view=True),
        reply_markup=trust_passport_keyboard(user_id, admin_view=True),
        parse_mode="HTML",
    )
    await call.answer()


# ===== LTEAM ADMIN CHAT CENTER =====

def admin_recent_deal_chats(limit: int = 10):
    with db() as conn:
        return conn.execute("""
            SELECT d.id,
                   d.buyer_id,
                   d.seller_id,
                   d.status,
                   COUNT(m.id) AS messages_count,
                   MAX(m.created_at) AS last_message_at
            FROM deals d
            JOIN deal_messages m ON m.deal_id = d.id
            GROUP BY d.id
            ORDER BY MAX(m.id) DESC
            LIMIT ?
        """, (limit,)).fetchall()


def admin_recent_order_chats(limit: int = 10):
    with db() as conn:
        return conn.execute("""
            SELECT o.id,
                   o.customer_id,
                   o.executor_id,
                   o.status,
                   COUNT(m.id) AS messages_count,
                   MAX(m.created_at) AS last_message_at
            FROM orders o
            JOIN order_messages m ON m.order_id = o.id
            GROUP BY o.id
            ORDER BY MAX(m.id) DESC
            LIMIT ?
        """, (limit,)).fetchall()


def admin_user_chat_summary(user_id: int, limit: int = 10):
    with db() as conn:
        deal_rows = conn.execute("""
            SELECT d.id, d.buyer_id, d.seller_id, d.status, COUNT(m.id), MAX(m.created_at)
            FROM deals d
            JOIN deal_messages m ON m.deal_id=d.id
            WHERE d.buyer_id=? OR d.seller_id=?
            GROUP BY d.id
            ORDER BY MAX(m.id) DESC
            LIMIT ?
        """, (user_id, user_id, limit)).fetchall()
        order_rows = conn.execute("""
            SELECT o.id, o.customer_id, o.executor_id, o.status, COUNT(m.id), MAX(m.created_at)
            FROM orders o
            JOIN order_messages m ON m.order_id=o.id
            WHERE o.customer_id=? OR o.executor_id=? OR m.sender_id=? OR m.receiver_id=?
            GROUP BY o.id
            ORDER BY MAX(m.id) DESC
            LIMIT ?
        """, (user_id, user_id, user_id, user_id, limit)).fetchall()
    return deal_rows, order_rows


def short_time(value: str | None) -> str:
    if not value:
        return "—"
    return str(value).replace("T", " ")[:16]


@dp.callback_query(F.data == "admin_chat_hint")
async def admin_chat_hint(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    deal_rows = admin_recent_deal_chats(limit=8)
    order_rows = admin_recent_order_chats(limit=8)

    text = """
━━━━━━━━━━━━━━
💬 <b>Центр чатов LTeam</b>
━━━━━━━━━━━━━━

Здесь админы могут быстро открыть последние чаты сделок и заказов без ручного ввода ID.

<b>Сделки:</b>
"""
    buttons = []

    if deal_rows:
        for deal_id, buyer_id, seller_id, status, count, last_at in deal_rows:
            text += f"\n• Сделка <b>#{deal_id}</b> — {count} сообщ. — <code>{short_time(last_at)}</code>"
            buttons.append([InlineKeyboardButton(text=f"🤝 Сделка #{deal_id} · {count} сообщ.", callback_data=f"admin_deal_chat:{deal_id}")])
    else:
        text += "\n• пока нет чатов сделок"

    text += "\n\n<b>Заказы:</b>\n"
    if order_rows:
        for order_id, customer_id, executor_id, status, count, last_at in order_rows:
            text += f"\n• Заказ <b>#{order_id}</b> — {count} сообщ. — <code>{short_time(last_at)}</code>"
            buttons.append([InlineKeyboardButton(text=f"📌 Заказ #{order_id} · {count} сообщ.", callback_data=f"admin_order_chat:{order_id}")])
    else:
        text += "\n• пока нет чатов заказов"

    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_chat_hint")])
    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])

    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_user_chats:"))
async def admin_user_chats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "view_user")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    deal_rows, order_rows = admin_user_chat_summary(user_id, limit=8)

    text = f"""
━━━━━━━━━━━━━━
💬 <b>Чаты пользователя</b>
━━━━━━━━━━━━━━

Пользователь: <code>{user_id}</code>
Бейдж: <b>{trust_public_badge(user_id)}</b>

<b>Сделки:</b>
"""
    buttons = []

    if deal_rows:
        for deal_id, buyer_id, seller_id, status, count, last_at in deal_rows:
            text += f"\n• Сделка <b>#{deal_id}</b> — {count} сообщ. — <code>{short_time(last_at)}</code>"
            buttons.append([InlineKeyboardButton(text=f"🤝 Сделка #{deal_id} · {count} сообщ.", callback_data=f"admin_deal_chat:{deal_id}")])
    else:
        text += "\n• нет чатов сделок"

    text += "\n\n<b>Заказы:</b>\n"
    if order_rows:
        for order_id, customer_id, executor_id, status, count, last_at in order_rows:
            text += f"\n• Заказ <b>#{order_id}</b> — {count} сообщ. — <code>{short_time(last_at)}</code>"
            buttons.append([InlineKeyboardButton(text=f"📌 Заказ #{order_id} · {count} сообщ.", callback_data=f"admin_order_chat:{order_id}")])
    else:
        text += "\n• нет чатов заказов"

    buttons.append([InlineKeyboardButton(text="🛡 Trust Passport", callback_data=f"admin_trust_passport:{user_id}")])
    buttons.append([InlineKeyboardButton(text="👤 Карточка пользователя", callback_data=f"admin_user:{user_id}")])

    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()



# ===== LTEAM FINANCE: БАЛАНС И ВЫВОДЫ V1 =====

class WithdrawalState(StatesGroup):
    amount = State()
    requisites = State()


def ensure_finance_tables() -> None:
    """Финансовый контур: баланс исполнителя, заморозка, история и заявки на вывод."""
    with db() as conn:
        cur = conn.cursor()
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
        for table, columns in {
            "user_balances": [
                "available INTEGER DEFAULT 0",
                "frozen INTEGER DEFAULT 0",
                "total_earned INTEGER DEFAULT 0",
                "total_withdrawn INTEGER DEFAULT 0",
                "updated_at TEXT",
            ],
            "balance_transactions": [
                "user_id INTEGER",
                "deal_id INTEGER",
                "withdrawal_id INTEGER",
                "tx_type TEXT",
                "amount INTEGER",
                "balance_after INTEGER DEFAULT 0",
                "comment TEXT",
                "created_at TEXT",
            ],
            "withdrawal_requests": [
                "user_id INTEGER",
                "amount INTEGER",
                "requisites TEXT",
                "status TEXT DEFAULT 'pending'",
                "admin_id INTEGER",
                "admin_comment TEXT",
                "created_at TEXT",
                "resolved_at TEXT",
            ],
        }.items():
            for column_sql in columns:
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")
                except sqlite3.OperationalError:
                    pass
        conn.commit()


def ensure_user_balance(user_id: int) -> None:
    ensure_finance_tables()
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO user_balances (user_id, available, frozen, total_earned, total_withdrawn, updated_at) VALUES (?, 0, 0, 0, 0, ?)",
            (int(user_id), datetime.now().isoformat()),
        )
        conn.commit()


def get_user_balance(user_id: int) -> dict:
    ensure_user_balance(user_id)
    with db() as conn:
        row = conn.execute(
            "SELECT available, frozen, total_earned, total_withdrawn FROM user_balances WHERE user_id=?",
            (int(user_id),),
        ).fetchone()
    available, frozen, total_earned, total_withdrawn = row or (0, 0, 0, 0)
    return {
        "available": int(available or 0),
        "frozen": int(frozen or 0),
        "total_earned": int(total_earned or 0),
        "total_withdrawn": int(total_withdrawn or 0),
    }


def add_balance_tx(user_id: int, tx_type: str, amount: int, *, deal_id: int | None = None, withdrawal_id: int | None = None, comment: str = "") -> None:
    ensure_user_balance(user_id)
    balance = get_user_balance(user_id)
    with db() as conn:
        conn.execute(
            """
            INSERT INTO balance_transactions (user_id, deal_id, withdrawal_id, tx_type, amount, balance_after, comment, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(user_id), deal_id, withdrawal_id, tx_type, int(amount or 0), balance.get("available", 0), comment, datetime.now().isoformat()),
        )
        conn.commit()


def freeze_deal_funds(seller_id: int, deal_id: int, amount: int) -> None:
    """Логируем, что деньги у гаранта. В доступный баланс они попадут только после подтверждения выполнения."""
    add_balance_tx(seller_id, "escrow_frozen", int(amount or 0), deal_id=deal_id, comment="Оплата подтверждена админом, деньги у гаранта LTeam")


def _credit_seller_balance_in_conn(
    conn: sqlite3.Connection,
    seller_id: int,
    deal_id: int,
    amount: int,
    *,
    comment: str = "Сделка завершена покупателем",
) -> bool:
    """Идемпотентно зачисляет выплату в уже открытой транзакции."""
    seller_id = int(seller_id)
    deal_id = int(deal_id)
    amount = int(amount or 0)

    conn.execute(
        """
        INSERT OR IGNORE INTO user_balances
        (user_id, available, frozen, total_earned, total_withdrawn, updated_at)
        VALUES (?, 0, 0, 0, 0, ?)
        """,
        (seller_id, datetime.now().isoformat()),
    )

    already_credited = conn.execute(
        """
        SELECT 1
        FROM balance_transactions
        WHERE deal_id=? AND tx_type='deal_credit'
        LIMIT 1
        """,
        (deal_id,),
    ).fetchone()
    if already_credited:
        return False

    now = datetime.now().isoformat()
    conn.execute(
        """
        UPDATE user_balances
        SET available = COALESCE(available,0) + ?,
            total_earned = COALESCE(total_earned,0) + ?,
            updated_at = ?
        WHERE user_id=?
        """,
        (amount, amount, now, seller_id),
    )
    balance_after_row = conn.execute(
        "SELECT COALESCE(available,0) FROM user_balances WHERE user_id=?",
        (seller_id,),
    ).fetchone()
    balance_after = int(balance_after_row[0] or 0) if balance_after_row else 0
    conn.execute(
        """
        INSERT INTO balance_transactions
        (user_id, deal_id, withdrawal_id, tx_type, amount, balance_after, comment, created_at)
        VALUES (?, ?, NULL, 'deal_credit', ?, ?, ?, ?)
        """,
        (seller_id, deal_id, amount, balance_after, comment, now),
    )
    return True


def credit_seller_balance(seller_id: int, deal_id: int, amount: int) -> bool:
    """Безопасно и идемпотентно зачисляет выплату исполнителю."""
    ensure_finance_tables()
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        credited = _credit_seller_balance_in_conn(
            conn,
            seller_id,
            deal_id,
            amount,
            comment=f"Сделка #{int(deal_id)} завершена покупателем",
        )
        conn.commit()
    return credited

def reserve_withdrawal(user_id: int, amount: int, requisites: str) -> tuple[bool, str, int | None]:
    """Атомарно резервирует деньги под вывод и пишет запись в финансовый журнал."""
    ensure_finance_tables()
    user_id = int(user_id)
    amount = int(amount or 0)
    if amount <= 0:
        return False, "Сумма должна быть больше нуля.", None

    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT OR IGNORE INTO user_balances
            (user_id, available, frozen, total_earned, total_withdrawn, updated_at)
            VALUES (?, 0, 0, 0, 0, ?)
            """,
            (user_id, datetime.now().isoformat()),
        )
        balance_row = conn.execute(
            "SELECT COALESCE(available,0) FROM user_balances WHERE user_id=?",
            (user_id,),
        ).fetchone()
        available = int(balance_row[0] or 0) if balance_row else 0
        if amount > available:
            return False, f"Недостаточно средств. Доступно: {available}₽.", None

        cur = conn.execute(
            """
            INSERT INTO withdrawal_requests (user_id, amount, requisites, status, created_at)
            VALUES (?, ?, ?, 'pending', ?)
            """,
            (user_id, amount, requisites, datetime.now().isoformat()),
        )
        withdrawal_id = int(cur.lastrowid)
        updated = conn.execute(
            """
            UPDATE user_balances
            SET available = COALESCE(available,0) - ?,
                frozen = COALESCE(frozen,0) + ?,
                updated_at = ?
            WHERE user_id=? AND COALESCE(available,0) >= ?
            """,
            (amount, amount, datetime.now().isoformat(), user_id, amount),
        ).rowcount
        if updated != 1:
            conn.rollback()
            return False, "Баланс изменился. Обновите экран и попробуйте снова.", None

        balance_after_row = conn.execute(
            "SELECT COALESCE(available,0) FROM user_balances WHERE user_id=?",
            (user_id,),
        ).fetchone()
        balance_after = int(balance_after_row[0] or 0) if balance_after_row else 0
        conn.execute(
            """
            INSERT INTO balance_transactions
            (user_id, deal_id, withdrawal_id, tx_type, amount, balance_after, comment, created_at)
            VALUES (?, NULL, ?, 'withdrawal_reserved', ?, ?, ?, ?)
            """,
            (user_id, withdrawal_id, -amount, balance_after, "Средства зарезервированы под вывод", datetime.now().isoformat()),
        )
        conn.commit()

    return True, "ok", withdrawal_id

def complete_withdrawal(withdrawal_id: int, admin_id: int) -> tuple[bool, str, tuple | None]:
    ensure_finance_tables()
    with db() as conn:
        row = conn.execute(
            "SELECT user_id, amount, requisites, status FROM withdrawal_requests WHERE id=?",
            (int(withdrawal_id),),
        ).fetchone()
        if not row:
            return False, "Заявка не найдена.", None
        user_id, amount, requisites, status = row
        if status != "pending":
            return False, f"Заявка уже обработана. Статус: {status}", row
        conn.execute(
            """
            UPDATE withdrawal_requests
            SET status='paid', admin_id=?, admin_comment='Выплата выполнена вручную', resolved_at=?
            WHERE id=?
            """,
            (int(admin_id), datetime.now().isoformat(), int(withdrawal_id)),
        )
        conn.execute(
            """
            UPDATE user_balances
            SET frozen = MAX(COALESCE(frozen,0) - ?, 0),
                total_withdrawn = COALESCE(total_withdrawn,0) + ?,
                updated_at=?
            WHERE user_id=?
            """,
            (int(amount or 0), int(amount or 0), datetime.now().isoformat(), int(user_id)),
        )
        conn.commit()
    add_balance_tx(user_id, "withdrawal_paid", -int(amount or 0), withdrawal_id=withdrawal_id, comment=f"Выплата подтверждена админом {admin_id}")
    return True, "ok", row


def reject_withdrawal(withdrawal_id: int, admin_id: int, comment: str = "Отклонено админом") -> tuple[bool, str, tuple | None]:
    ensure_finance_tables()
    with db() as conn:
        row = conn.execute(
            "SELECT user_id, amount, requisites, status FROM withdrawal_requests WHERE id=?",
            (int(withdrawal_id),),
        ).fetchone()
        if not row:
            return False, "Заявка не найдена.", None
        user_id, amount, requisites, status = row
        if status != "pending":
            return False, f"Заявка уже обработана. Статус: {status}", row
        conn.execute(
            """
            UPDATE withdrawal_requests
            SET status='rejected', admin_id=?, admin_comment=?, resolved_at=?
            WHERE id=?
            """,
            (int(admin_id), comment, datetime.now().isoformat(), int(withdrawal_id)),
        )
        conn.execute(
            """
            UPDATE user_balances
            SET available = COALESCE(available,0) + ?,
                frozen = MAX(COALESCE(frozen,0) - ?, 0),
                updated_at=?
            WHERE user_id=?
            """,
            (int(amount or 0), int(amount or 0), datetime.now().isoformat(), int(user_id)),
        )
        conn.commit()
    add_balance_tx(user_id, "withdrawal_rejected", int(amount or 0), withdrawal_id=withdrawal_id, comment=comment)
    return True, "ok", row


def validate_withdrawal_requisites(value: str) -> tuple[bool, str, str]:
    raw = (value or "").strip()
    digits = re.sub(r"\D+", "", raw)
    if not raw:
        return False, "", "Введите карту или номер телефона для перевода."
    if any(x in raw.lower() for x in ["t.me/", "@", "http://", "https://", "vk.com", "discord"]):
        return False, "", "В реквизитах вывода нужна только карта или номер телефона, без ссылок и username."
    if digits and 10 <= len(digits) <= 19:
        return True, raw, ""
    return False, "", "Похоже, реквизиты некорректны. Укажите карту или номер телефона, например: 2200 0000 0000 0000 или +79990000000."


def balance_text(user_id: int) -> str:
    b = get_user_balance(user_id)
    return f"""
━━━━━━━━━━━━━━
💰 <b>Баланс LTeam</b>
━━━━━━━━━━━━━━

✅ Доступно к выводу: <b>{b['available']}₽</b>
🧊 Зарезервировано/в обработке: <b>{b['frozen']}₽</b>
📈 Всего заработано: <b>{b['total_earned']}₽</b>
💸 Всего выведено: <b>{b['total_withdrawn']}₽</b>

Средства попадают на баланс после того, как покупатель подтвердит выполнение сделки.
Вывод обрабатывается вручную админом LTeam в течение <b>2 дней</b>.
"""


def balance_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Вывести средства", callback_data="withdraw_start")],
        [InlineKeyboardButton(text="📜 История баланса", callback_data="balance_history")],
        [InlineKeyboardButton(text="⬅️ Профиль", callback_data="profile")],
    ])


@dp.callback_query(F.data == "balance")
async def balance_screen(call: CallbackQuery):
    ensure_finance_tables()
    await show_screen(call, balance_text(call.from_user.id), reply_markup=balance_keyboard(), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "balance_history")
async def balance_history(call: CallbackQuery):
    ensure_finance_tables()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT tx_type, amount, comment, created_at
            FROM balance_transactions
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT 12
            """,
            (call.from_user.id,),
        ).fetchall()
    if not rows:
        text = "📜 <b>История баланса</b>\n\nПока операций нет."
    else:
        lines = []
        for tx_type, amount, comment, created_at in rows:
            sign = "+" if int(amount or 0) > 0 else ""
            lines.append(f"• <b>{html.escape(tx_type or '')}</b>: {sign}{int(amount or 0)}₽ — {html.escape(comment or '')} <code>{short_time(created_at)}</code>")
        text = "📜 <b>История баланса</b>\n\n" + "\n".join(lines)
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Баланс", callback_data="balance")]]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "withdraw_start")
async def withdraw_start(call: CallbackQuery, state: FSMContext):
    b = get_user_balance(call.from_user.id)
    if b["available"] <= 0:
        await call.answer("Нет средств для вывода", show_alert=True)
        await show_screen(call, balance_text(call.from_user.id), reply_markup=balance_keyboard(), parse_mode="HTML")
        return
    await state.set_state(WithdrawalState.amount)
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
💸 <b>Вывод средств</b>
━━━━━━━━━━━━━━

Доступно: <b>{b['available']}₽</b>

Введите сумму вывода числом.

⚠️ Выплата выполняется вручную админом LTeam в течение <b>2 дней</b>.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Баланс", callback_data="balance")]]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(WithdrawalState.amount)
async def withdraw_amount_save(message: Message, state: FSMContext):
    amount = parse_money(message.text or "")
    b = get_user_balance(message.from_user.id)
    if not amount or amount <= 0:
        await screen_answer(message, "❌ Введите сумму числом, например: <code>1500</code>", parse_mode="HTML")
        return
    if amount > b["available"]:
        await screen_answer(message, f"❌ Недостаточно средств. Доступно: <b>{b['available']}₽</b>", parse_mode="HTML")
        return
    await state.update_data(withdraw_amount=int(amount))
    await state.set_state(WithdrawalState.requisites)
    await screen_answer(
        message,
        f"""
💳 <b>Реквизиты для вывода</b>

Сумма: <b>{amount}₽</b>

Отправьте карту или номер телефона, куда админ переведёт деньги.

⚠️ Выплата выполняется в течение <b>2 дней</b>.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Баланс", callback_data="balance")]]),
        parse_mode="HTML",
    )


@dp.message(WithdrawalState.requisites)
async def withdraw_requisites_save(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = int(data.get("withdraw_amount") or 0)
    ok, requisites, error = validate_withdrawal_requisites(message.text or "")
    if not ok:
        await screen_answer(message, f"❌ {html.escape(error)}", parse_mode="HTML")
        return
    success, msg, withdrawal_id = reserve_withdrawal(message.from_user.id, amount, requisites)
    if not success:
        await state.clear()
        await screen_answer(message, f"❌ {html.escape(msg)}", reply_markup=balance_keyboard(), parse_mode="HTML")
        return
    await state.clear()
    await screen_answer(
        message,
        f"""
✅ <b>Заявка на вывод создана</b>

ID заявки: <b>#{withdrawal_id}</b>
Сумма: <b>{amount}₽</b>

Средства зарезервированы. Админ LTeam выполнит перевод вручную в течение <b>2 дней</b>.
""",
        reply_markup=balance_keyboard(),
        parse_mode="HTML",
    )
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Выплачено", callback_data=f"admin_withdraw_paid:{withdrawal_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_withdraw_reject:{withdrawal_id}"),
        ],
        [InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user:{message.from_user.id}")],
        [InlineKeyboardButton(text="💸 Все выводы", callback_data="admin_withdrawals")],
    ])
    await notify_admins(
        f"""
💸 <b>Новая заявка на вывод</b>

ID: <b>#{withdrawal_id}</b>
Пользователь: <code>{message.from_user.id}</code>
Сумма: <b>{amount}₽</b>

Реквизиты:
<code>{html.escape(requisites)}</code>

⚠️ Выполнить вручную в течение <b>2 дней</b>.
""",
        reply_markup=admin_kb,
    )


@dp.callback_query(F.data == "admin_withdrawals")
async def admin_withdrawals(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    ensure_finance_tables()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, user_id, amount, requisites, created_at
            FROM withdrawal_requests
            WHERE status='pending'
            ORDER BY id ASC
            LIMIT 20
            """,
        ).fetchall()
    if not rows:
        await show_screen(call, "💸 <b>Заявки на вывод</b>\n\nАктивных заявок нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]]), parse_mode="HTML")
        await call.answer()
        return
    text = "💸 <b>Заявки на вывод</b>\n\n"
    buttons = []
    for wid, user_id, amount, requisites, created_at in rows:
        text += f"• <b>#{wid}</b> — <code>{user_id}</code> — <b>{amount}₽</b> — <code>{short_time(created_at)}</code>\n"
        buttons.append([InlineKeyboardButton(text=f"#{wid} · {amount}₽ · {user_id}", callback_data=f"admin_withdraw_view:{wid}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_withdraw_view:"))
async def admin_withdraw_view(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[1])
    ensure_finance_tables()
    with db() as conn:
        row = conn.execute(
            "SELECT user_id, amount, requisites, status, created_at FROM withdrawal_requests WHERE id=?",
            (wid,),
        ).fetchone()
    if not row:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    user_id, amount, requisites, status, created_at = row
    await show_screen(
        call,
        f"""
💸 <b>Заявка на вывод #{wid}</b>

Пользователь: <code>{user_id}</code>
Сумма: <b>{amount}₽</b>
Статус: <b>{html.escape(status or '')}</b>
Создана: <code>{short_time(created_at)}</code>

Реквизиты:
<code>{html.escape(requisites or '')}</code>

⚠️ Выплата вручную, срок до <b>2 дней</b>.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Выплачено", callback_data=f"admin_withdraw_paid:{wid}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_withdraw_reject:{wid}")],
            [InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user:{user_id}")],
            [InlineKeyboardButton(text="⬅️ Все выводы", callback_data="admin_withdrawals")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin_withdraw_paid:"))
async def admin_withdraw_paid(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[1])
    ok, msg, row = complete_withdrawal(wid, call.from_user.id)
    if not ok:
        await call.answer(msg, show_alert=True)
        return
    user_id, amount, requisites, status = row
    log_admin_action(call.from_user.id, "withdrawal_paid", user_id, f"withdrawal_id={wid}; amount={amount}")
    await bot.send_message(user_id, f"✅ Вывод #{wid} на сумму <b>{int(amount or 0)}₽</b> выполнен админом LTeam.", parse_mode="HTML")
    await call.message.edit_text(f"✅ Вывод #{wid} отмечен как выплаченный.\n\nПользователь: <code>{user_id}</code>\nСумма: <b>{int(amount or 0)}₽</b>", parse_mode="HTML")
    await call.answer("Выплата отмечена")


@dp.callback_query(F.data.startswith("admin_withdraw_reject:"))
async def admin_withdraw_reject(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    wid = int(call.data.split(":")[1])
    ok, msg, row = reject_withdrawal(wid, call.from_user.id, "Отклонено админом. Средства возвращены на баланс.")
    if not ok:
        await call.answer(msg, show_alert=True)
        return
    user_id, amount, requisites, status = row
    log_admin_action(call.from_user.id, "withdrawal_rejected", user_id, f"withdrawal_id={wid}; amount={amount}")
    await bot.send_message(user_id, f"❌ Вывод #{wid} на сумму <b>{int(amount or 0)}₽</b> отклонён. Средства возвращены на баланс.", parse_mode="HTML")
    await call.message.edit_text(f"❌ Вывод #{wid} отклонён. Средства возвращены пользователю <code>{user_id}</code>.", parse_mode="HTML")
    await call.answer("Отклонено")

# ===== ПРОФИЛЬ =====

@dp.callback_query(F.data == "profile")
async def profile(call: CallbackQuery):
    ensure_profile_tables()
    await show_screen(
        call,
        build_beautiful_profile_text(call.from_user.id),
        reply_markup=beautiful_profile_keyboard(call.from_user.id),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "profile_settings")
async def profile_settings(call: CallbackQuery):
    ensure_profile_tables()
    info = get_profile_info(call.from_user.id)
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
⚙️ <b>Настройки профиля</b>
━━━━━━━━━━━━━━

Профиль: <b>{profile_title(call.from_user.id)}</b>
Подписка: <b>{info['plus_badge']}</b>

🎭 <b>Эмодзи</b> — значок возле профиля.
✅ <b>Галочка</b> — ручная верификация владельцами.
📝 <b>Описание</b> — доступно с LTeam Plus.
""",
        reply_markup=profile_settings_keyboard(call.from_user.id),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "profile_emoji")
async def profile_emoji(call: CallbackQuery):
    info = get_profile_info(call.from_user.id)
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
🎭 <b>Эмодзи профиля</b>
━━━━━━━━━━━━━━

Текущий эмодзи: <b>{html.escape(info['emoji'])}</b>

🆓 Верхние эмодзи доступны всем.
💎 Премиальные эмодзи доступны с LTeam Plus.
""",
        reply_markup=emoji_keyboard(call.from_user.id),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("profile_set_emoji:"))
async def profile_set_emoji(call: CallbackQuery):
    emoji = call.data.split(":", 1)[1]
    if emoji in PREMIUM_PROFILE_EMOJIS and not can_use_premium_profile_features(call.from_user.id):
        await call.answer("Премиальные эмодзи доступны с LTeam Plus", show_alert=True)
        return
    if emoji not in FREE_PROFILE_EMOJIS and emoji not in PREMIUM_PROFILE_EMOJIS:
        await call.answer("Этот эмодзи недоступен", show_alert=True)
        return
    ensure_profile_tables()
    with db() as conn:
        conn.execute("""
            INSERT INTO user_profile_settings (user_id, profile_emoji, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET profile_emoji=excluded.profile_emoji, updated_at=excluded.updated_at
        """, (call.from_user.id, emoji, datetime.now().isoformat()))
        conn.commit()
    await call.answer("Эмодзи обновлён", show_alert=True)
    await profile_emoji(call)


@dp.callback_query(F.data == "profile_description_start")
async def profile_description_start(call: CallbackQuery, state: FSMContext):
    if not can_use_premium_profile_features(call.from_user.id):
        await show_screen(
            call,
            """
━━━━━━━━━━━━━━
📝 <b>Описание профиля</b>
━━━━━━━━━━━━━━

Описание профиля доступно с <b>💎 LTeam Plus</b>.

С подпиской можно добавить короткое описание: чем вы занимаетесь, какие услуги оказываете и почему вам можно доверять.
""",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Узнать про LTeam Plus", callback_data="lteam_plus_info")],
                [InlineKeyboardButton(text="⬅️ Настройки профиля", callback_data="profile_settings")],
            ]),
            parse_mode="HTML",
        )
        await call.answer()
        return

    await state.set_state(ProfileDescriptionState.text)
    await show_screen(
        call,
        """
━━━━━━━━━━━━━━
📝 <b>Описание профиля</b>
━━━━━━━━━━━━━━

Отправьте описание профиля одним сообщением.

Правила:
• до 400 символов;
• без контактов;
• без запрещённых тем;
• по делу: навыки, опыт, услуги.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Настройки профиля", callback_data="profile_settings")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(ProfileDescriptionState.text)
async def profile_description_save(message: Message, state: FSMContext):
    if not can_use_premium_profile_features(message.from_user.id):
        await state.clear()
        await screen_answer(message, "Описание доступно только с LTeam Plus.", reply_markup=back_home())
        return
    text = (message.text or "").strip()
    if len(text) < 20:
        await screen_answer(message, "Описание слишком короткое. Напишите хотя бы 20 символов.")
        return
    if len(text) > 400:
        await screen_answer(message, "Описание слишком длинное. Максимум 400 символов.")
        return
    ok, reason = moderation_check(text, allow_contacts=False)
    if not ok or looks_like_bypass_attempt(text):
        await screen_answer(message, f"❌ Описание не прошло модерацию: {html.escape(reason or 'контакты/обход гаранта')}", parse_mode="HTML")
        return
    ensure_profile_tables()
    with db() as conn:
        conn.execute("""
            INSERT INTO user_profile_settings (user_id, profile_description, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET profile_description=excluded.profile_description, updated_at=excluded.updated_at
        """, (message.from_user.id, text, datetime.now().isoformat()))
        conn.commit()
    await state.clear()
    await screen_answer(message, "✅ Описание профиля обновлено.", reply_markup=beautiful_profile_keyboard(message.from_user.id), parse_mode="HTML")


@dp.callback_query(F.data == "lteam_plus_info")
async def lteam_plus_info(call: CallbackQuery):
    info = get_profile_info(call.from_user.id)
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
💎 <b>LTeam Plus</b>
━━━━━━━━━━━━━━

Текущий статус: <b>{info['plus_badge']}</b>{f" — осталось {info['plus_days_left']} дн." if info['plus_active'] else ""}

Что даёт подписка:
• 💎 премиальные эмодзи профиля;
• 📝 описание профиля;
• 🏷 статус <b>LTeam Plus</b> в профиле;
• будущие Plus-функции без переделки аккаунта.

Оплата подписки будет проходить через LTeam. Пока включение подписки доступно через владельцев/админов.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🆘 Написать в поддержку", callback_data="support")],
            [InlineKeyboardButton(text="⬅️ Настройки профиля", callback_data="profile_settings")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "verification_request_start")
async def verification_request_start(call: CallbackQuery, state: FSMContext):
    ensure_profile_tables()
    info = get_profile_info(call.from_user.id)
    if info.get("verified"):
        await call.answer("У вас уже есть галочка", show_alert=True)
        return
    with db() as conn:
        pending = conn.execute("SELECT id FROM verification_requests WHERE user_id=? AND status='pending' ORDER BY id DESC LIMIT 1", (call.from_user.id,)).fetchone()
    if pending:
        await show_screen(call, f"⏳ Ваша заявка на галочку уже на проверке. Номер заявки: <b>#{pending[0]}</b>", reply_markup=profile_settings_keyboard(call.from_user.id), parse_mode="HTML")
        await call.answer()
        return
    await state.set_state(VerificationRequestState.reason)
    await show_screen(
        call,
        """
━━━━━━━━━━━━━━
✅ <b>Заявка на галочку</b>
━━━━━━━━━━━━━━

Галочка выдаётся вручную владельцами LTeam.

Напишите, почему вам можно выдать верификацию:
• чем занимаетесь;
• есть ли завершённые сделки;
• почему вам можно доверять;
• какую пользу даёте пользователям LTeam.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Настройки профиля", callback_data="profile_settings")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(VerificationRequestState.reason)
async def verification_request_save(message: Message, state: FSMContext):
    reason = (message.text or "").strip()
    if len(reason) < 20:
        await screen_answer(message, "Напишите подробнее: минимум 20 символов.")
        return
    if len(reason) > 1000:
        await screen_answer(message, "Слишком длинно. Максимум 1000 символов.")
        return
    ok, mod_reason = moderation_check(reason, allow_contacts=True)
    if not ok:
        await screen_answer(message, f"❌ Текст не прошёл модерацию: {html.escape(mod_reason)}", parse_mode="HTML")
        return
    ensure_profile_tables()
    with db() as conn:
        cur = conn.execute("""
            INSERT INTO verification_requests (user_id, reason, status, created_at)
            VALUES (?, ?, 'pending', ?)
        """, (message.from_user.id, reason, datetime.now().isoformat()))
        req_id = cur.lastrowid
        conn.commit()
    await state.clear()
    for owner_id in OWNER_IDS:
        try:
            await bot.send_message(
                owner_id,
                f"""
✅ <b>Новая заявка на галочку</b>

Заявка: <b>#{req_id}</b>
Пользователь: <code>{message.from_user.id}</code>
Профиль: <b>{profile_title(message.from_user.id)}</b>

Причина:
{html.escape(reason[:700])}
""",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"verify_req_approve:{req_id}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"verify_req_reject:{req_id}")],
                    [InlineKeyboardButton(text="👤 Профиль", callback_data=f"admin_user:{message.from_user.id}")],
                ]),
                parse_mode="HTML",
            )
        except Exception:
            pass
    await screen_answer(message, f"✅ Заявка на галочку отправлена владельцам. Номер заявки: <b>#{req_id}</b>", reply_markup=beautiful_profile_keyboard(message.from_user.id), parse_mode="HTML")


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

    ok, reason = can_act(call.from_user.id, user_id, "view_user")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    stats = seller_stats(user_id)
    risk = get_user_security_score(user_id)

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
            WHERE COALESCE(r.target_id, 0)=? OR l.seller_id=? OR o.customer_id=?
        """, (user_id, user_id, user_id)).fetchone()[0]
        warnings_count = conn.execute("SELECT COUNT(*) FROM admin_warnings WHERE user_id=?", (user_id,)).fetchone()[0]
        banned = conn.execute("SELECT reason, created_at, banned_by FROM banned_users WHERE user_id=?", (user_id,)).fetchone()
        mute = conn.execute("SELECT muted_until, reason, muted_by FROM muted_users WHERE user_id=?", (user_id,)).fetchone()
        last_deal = conn.execute("SELECT id, status, amount, created_at FROM deals WHERE buyer_id=? OR seller_id=? ORDER BY id DESC LIMIT 1", (user_id, user_id)).fetchone()

    username = (user_row[0] if user_row and user_row[0] else stats.get("username", "не указан"))
    created_at = (user_row[1] if user_row and user_row[1] else stats.get("created_at", "неизвестно"))
    ban_status = "🚫 Забанен" if banned else "✅ Активен"
    ban_line = ""
    if banned:
        ban_line = f"\nПричина: <b>{html.escape(banned[0] or 'не указана')}</b>\nДата: <code>{html.escape(str(banned[1])[:16])}</code> • Админ: <code>{banned[2]}</code>"

    mute_line = ""
    if mute:
        try:
            muted_until = datetime.fromisoformat(mute[0])
            if muted_until > datetime.now():
                mute_line = f"\n🔇 Мут до: <code>{muted_until.strftime('%d.%m %H:%M')}</code> • {html.escape(mute[1] or 'без причины')}"
        except Exception:
            pass

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
🎚 Роль: <b>{role_badge(user_id)}</b>
🛡 Верификация: <b>{verify_status}</b>
🚦 Доступ: <b>{ban_status}</b>{ban_line}{mute_line}

🛡 <b>Security Score</b>
Уровень: <b>{risk['badge']}</b>
Балл риска: <b>{risk['score']}/100</b>
Главные факторы:
{security_reasons_text(risk['reasons'], limit=4)}

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
        reply_markup=admin_user_actions_keyboard(call.from_user.id, user_id),
        parse_mode="HTML",
    )
    await call.answer()

@dp.callback_query(F.data.startswith("admin_user_security:"))
async def admin_user_security(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "view_user")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    risk = get_user_security_score(user_id)

    with db() as conn:
        events = conn.execute("""
            SELECT event_type, context, text, status, created_at
            FROM security_events
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT 10
        """, (user_id,)).fetchall()

    lines = []
    for event_type, context, text, status, created_at in events:
        lines.append(
            f"• <b>{html.escape(event_type or 'event')}</b> "
            f"<code>{html.escape(str(created_at or '')[:16])}</code>\n"
            f"Контекст: {html.escape(context or '—')}\n"
            f"Статус: {html.escape(status or '—')}\n"
            f"Текст: {html.escape((text or '')[:250])}"
        )

    events_text = "\n\n".join(lines) if lines else "Событий безопасности пока нет."

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
🛡 <b>Security пользователя</b>
━━━━━━━━━━━━━━

Пользователь: <code>{user_id}</code>
Уровень: <b>{risk['badge']}</b>
Балл риска: <b>{risk['score']}/100</b>

<b>Факторы:</b>
{security_reasons_text(risk['reasons'], limit=12)}

<b>Метрики:</b>
🚨 Жалоб на пользователя: <b>{risk['reports_on_user']}</b>
⚠️ Предупреждений: <b>{risk['warnings_count']}</b>
🛡 Security events: <b>{risk['security_events_count']}</b>
↪️ Попыток увести сделку: <b>{risk['bypass_events_count']}</b>
✅ Завершённых сделок: <b>{risk['completed_deals']}</b>
⭐ Отзывов: <b>{risk['reviews_count']}</b>, рейтинг: <b>{risk['avg_rating']:.1f}</b>

<b>Последние события:</b>
{events_text}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Карточка пользователя", callback_data=f"admin_user:{user_id}")],
            [InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin_user_logs:"))
async def admin_user_logs(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "view_reports")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    with db() as conn:
        rows = conn.execute("""
            SELECT actor_id, action, details, created_at
            FROM admin_action_logs
            WHERE target_id=?
            ORDER BY id DESC
            LIMIT 15
        """, (user_id,)).fetchall()

    if not rows:
        text = f"📜 По пользователю <code>{user_id}</code> пока нет админ-логов."
    else:
        items = []
        for actor_id, action, details, created_at in rows:
            items.append(
                f"• <code>{html.escape(str(created_at or '')[:16])}</code>\n"
                f"Админ: <code>{actor_id}</code>\n"
                f"Действие: <b>{html.escape(action or '—')}</b>\n"
                f"Детали: {html.escape((details or '—')[:300])}"
            )
        log_items_text = "\n\n".join(items)
        text = f"""
━━━━━━━━━━━━━━
📜 <b>Админ-логи пользователя</b>
━━━━━━━━━━━━━━

Пользователь: <code>{user_id}</code>

{log_items_text}
"""

    await show_screen(call,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Карточка пользователя", callback_data=f"admin_user:{user_id}")],
            [InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")],
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
    await show_screen(call, """
━━━━━━━━━━━━━━
❌ <b>Создание заказа отменено</b>
━━━━━━━━━━━━━━

Вы можете вернуться в меню или начать заново в любой момент.
""", reply_markup=main_menu(call.from_user.id), parse_mode="HTML")
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


def listing_delivery_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚡ Срочно / сегодня", callback_data="listing_delivery:Срочно / сегодня"),
            InlineKeyboardButton(text="1 день", callback_data="listing_delivery:1 день"),
        ],
        [
            InlineKeyboardButton(text="2-3 дня", callback_data="listing_delivery:2-3 дня"),
            InlineKeyboardButton(text="до 1 недели", callback_data="listing_delivery:до 1 недели"),
        ],
        [
            InlineKeyboardButton(text="1-2 недели", callback_data="listing_delivery:1-2 недели"),
            InlineKeyboardButton(text="до 1 месяца", callback_data="listing_delivery:до 1 месяца"),
        ],
        [InlineKeyboardButton(text="🤝 По договорённости", callback_data="listing_delivery:по договорённости")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="listing_back_type"), InlineKeyboardButton(text="❌ Отменить", callback_data="listing_cancel")],
    ])


def delivery_time_keyboard():
    # оставляем старое имя для совместимости, но теперь с кнопкой назад
    return listing_delivery_keyboard()


def order_deadline_keyboard():
    return deadline_options_keyboard("order_deadline_pick", "order_cancel")


def order_cancel_keyboard(extra_rows=None):
    rows = list(extra_rows or [])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="order_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def order_nav_keyboard(back_callback: str | None = None, extra_rows=None) -> InlineKeyboardMarkup:
    rows = list(extra_rows or [])
    nav = []
    if back_callback:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback))
    nav.append(InlineKeyboardButton(text="❌ Отменить", callback_data="order_cancel"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def order_category_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=cat, callback_data=f"order_cat:{cat}")] for cat in CATEGORIES]
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home"), InlineKeyboardButton(text="❌ Отменить", callback_data="order_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def order_deadline_keyboard_v2() -> InlineKeyboardMarkup:
    base = deadline_options_keyboard("order_deadline_pick", "order_cancel").inline_keyboard[:-1]
    base.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="order_back_budget"), InlineKeyboardButton(text="❌ Отменить", callback_data="order_cancel")])
    base.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=base)


def order_title_prompt(category: str) -> str:
    examples = ORDER_EXAMPLES.get(category, ["Нужна помощь с задачей", "Нужна цифровая услуга", "Нужен исполнитель"])
    examples_text = "\n".join(f"• <code>{html.escape(example)}</code>" for example in examples)
    return f"""
━━━━━━━━━━━━━━
🧾 <b>Создание заказа</b>
━━━━━━━━━━━━━━

<b>Шаг 2 из 5 — название</b>

📂 Категория: <b>{html.escape(category)}</b>

Напишите коротко, что нужно сделать.

<b>Примеры:</b>
{examples_text}
"""


def order_budget_prompt(title: str) -> str:
    return f"""
━━━━━━━━━━━━━━
💰 <b>Бюджет заказа</b>
━━━━━━━━━━━━━━

<b>Шаг 3 из 5</b>

📌 Заказ: <b>{html.escape(title)}</b>

Введите бюджет числом в рублях.

Диапазон: <b>{MIN_ORDER_BUDGET}₽ — {MAX_ORDER_BUDGET}₽</b>
Пример: <code>1500</code>
"""


@dp.callback_query(F.data == "create_order")
async def create_order(call: CallbackQuery, state: FSMContext):
    policy = protect_policy_for_user(call.from_user.id)
    if policy.get("block_create_order"):
        register_security_event(call.from_user.id, "order_create_blocked_high_risk", "create_order", status="blocked")
        await show_screen(
            call,
            protect_block_text(call.from_user.id, "создание заказа"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚖️ Подать апелляцию", callback_data="protect_appeal_start")],
                [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support"), InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ]),
            parse_mode="HTML",
        )
        await notify_admins(f"""
🔴 <b>LTeam Protect: создание заказа заблокировано</b>

Пользователь: <code>{call.from_user.id}</code>
Риск: <b>{policy.get('badge')}</b> / <b>{policy.get('score')}/100</b>
""")
        await call.answer("Действие заблокировано LTeam Protect", show_alert=True)
        return

    # Реквизиты нужны исполнителю только при выводе средств,
    # заказчик не должен заполнять их при создании заказа.

    await state.clear()
    await state.set_state(CreateOrder.category)

    await show_screen(call,
        """
━━━━━━━━━━━━━━
🧾 <b>Создание заказа</b>
━━━━━━━━━━━━━━

Здесь заказчик публикует задачу, а исполнители откликаются.

<b>Шаг 1 из 5 — категория</b>

Выберите направление, чтобы заказ попал к нужным исполнителям.
""",
        reply_markup=order_category_keyboard(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("order_cat:"))
async def order_category(call: CallbackQuery, state: FSMContext):
    category = call.data.split(":", 1)[1]
    await state.update_data(category=category)
    await state.set_state(CreateOrder.title)

    await show_screen(
        call,
        order_title_prompt(category),
        reply_markup=order_nav_keyboard("order_back_category"),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "order_back_category")
async def order_back_category(call: CallbackQuery, state: FSMContext):
    await state.set_state(CreateOrder.category)
    await show_screen(
        call,
        """
━━━━━━━━━━━━━━
🧾 <b>Создание заказа</b>
━━━━━━━━━━━━━━

<b>Шаг 1 из 5 — категория</b>

Выберите категорию заказа.
""",
        reply_markup=order_category_keyboard(),
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

    await screen_answer(
        message,
        order_budget_prompt(title),
        reply_markup=order_nav_keyboard("order_back_title"),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "order_back_title")
async def order_back_title(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    category = data.get("category", "")
    await state.set_state(CreateOrder.title)
    await show_screen(
        call,
        order_title_prompt(category),
        reply_markup=order_nav_keyboard("order_back_category"),
        parse_mode="HTML"
    )
    await call.answer()


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
        reply_markup=order_deadline_keyboard_v2(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "order_back_budget")
async def order_back_budget(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(CreateOrder.budget)
    await show_screen(
        call,
        order_budget_prompt(data.get("title", "")),
        reply_markup=order_nav_keyboard("order_back_title"),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data == "order_back_deadline")
async def order_back_deadline(call: CallbackQuery, state: FSMContext):
    await state.set_state(CreateOrder.deadline)
    await show_screen(
        call,
        """
━━━━━━━━━━━━━━
⏳ <b>Срок выполнения</b>
━━━━━━━━━━━━━━

<b>Шаг 4 из 5</b>

Выберите желаемый срок выполнения.
""",
        reply_markup=order_deadline_keyboard_v2(),
        parse_mode="HTML"
    )
    await call.answer()


@dp.callback_query(F.data.startswith("order_deadline_pick:"))
async def order_deadline_pick(call: CallbackQuery, state: FSMContext):
    deadline = call.data.split(":", 1)[1]
    await state.update_data(deadline=deadline)
    await state.set_state(CreateOrder.description)

    await show_screen(call,
        f"""
━━━━━━━━━━━━━━
📝 <b>Описание заказа</b>
━━━━━━━━━━━━━━

<b>Шаг 5 из 5</b>

⏳ Срок: <b>{html.escape(deadline)}</b>

Опишите задачу подробно:
• что нужно сделать
• какие функции нужны
• какой результат ожидаете
• примеры или пожелания

Чем подробнее описание, тем лучше будут отклики.
""",
        reply_markup=order_nav_keyboard("order_back_deadline"),
        parse_mode="HTML"
    )
    await call.answer()


@dp.message(CreateOrder.deadline)
async def order_deadline(message: Message, state: FSMContext):
    await screen_answer(
        message,
        "⏳ Срок нужно выбрать кнопкой ниже, а не писать текстом.",
        reply_markup=order_deadline_keyboard_v2(),
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
        [InlineKeyboardButton(text="✏️ Изменить описание", callback_data="order_back_deadline")],
        [InlineKeyboardButton(text="🔄 Создать заново", callback_data="create_order")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home"), InlineKeyboardButton(text="❌ Отменить", callback_data="order_cancel")],
    ])
    return text, keyboard


@dp.callback_query(F.data == "order_publish")
async def order_publish(call: CallbackQuery, state: FSMContext):
    policy = protect_policy_for_user(call.from_user.id)
    if policy.get("block_create_order"):
        register_security_event(call.from_user.id, "order_publish_blocked_high_risk", "order_publish", status="blocked")
        await state.clear()
        await show_screen(
            call,
            protect_block_text(call.from_user.id, "публикация заказа"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚖️ Подать апелляцию", callback_data="protect_appeal_start")],
                [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support"), InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ]),
            parse_mode="HTML",
        )
        await call.answer("Публикация заблокирована LTeam Protect", show_alert=True)
        return

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
""" + channel_promo_text("order_sent"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=add_channel_button([
            [InlineKeyboardButton(text="📋 Смотреть заказы", callback_data="orders_list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ], "📢 Канал LTeam")),
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
        await bot.send_message(
            seller_id,
            f"""
━━━━━━━━━━━━━━
✅ <b>Объявление опубликовано</b>
━━━━━━━━━━━━━━

Ваше объявление прошло модерацию и теперь видно в каталоге.

📦 Объявление: <b>#{listing_id}</b>
🧾 Название: <b>{html.escape(title or 'Без названия')}</b>
""",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=add_channel_button([
                [InlineKeyboardButton(text="👀 Перейти к объявлению", callback_data=f"view_listing:{listing_id}")],
                [InlineKeyboardButton(text="🔎 Каталог услуг", callback_data="market")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ], "📢 Канал LTeam")),
            parse_mode="HTML"
        )
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
        await bot.send_message(
            customer_id,
            f"""
━━━━━━━━━━━━━━
✅ <b>Заказ опубликован</b>
━━━━━━━━━━━━━━

Ваш заказ успешно прошёл модерацию и теперь доступен исполнителям.

📌 Заказ: <b>#{order_id}</b>
🧾 Название: <b>{html.escape(title or 'Без названия')}</b>

Теперь исполнители смогут откликаться на ваш заказ.
""",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=add_channel_button([
                [InlineKeyboardButton(text="👀 Перейти к заказу", callback_data=f"view_order:{order_id}")],
                [InlineKeyboardButton(text="📌 Лента заказов", callback_data="orders_list")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ], "📢 Канал LTeam")),
            parse_mode="HTML"
        )
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

    policy = protect_policy_for_user(executor_id)
    if policy.get("block_order_application"):
        register_security_event(executor_id, "order_application_blocked_high_risk", f"order #{order_id}", status="blocked")
        await show_screen(
            call,
            protect_block_text(executor_id, "отклик на заказ"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
                [InlineKeyboardButton(text="📌 Назад к заказу", callback_data=f"view_order:{order_id}")],
            ]),
            parse_mode="HTML",
        )
        await notify_admins(f"""
🔴 <b>LTeam Protect: отклик на заказ заблокирован</b>

Пользователь: <code>{executor_id}</code>
Заказ: <code>#{order_id}</code>
Риск: <b>{policy.get('badge')}</b> / <b>{policy.get('score')}/100</b>
""")
        await call.answer("Отклик заблокирован LTeam Protect", show_alert=True)
        return

    # Реквизиты исполнителя больше не требуются на этапе отклика.
    # Они понадобятся только при выводе средств после завершения сделки.

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

        # Исполнитель укажет реквизиты только при выводе средств.
        # На этапе отклика мы не требуем карту/кошелёк, чтобы не ломать воронку.
        executor_card_mask = "не указан — будет запрошен при выводе"
        executor_ton_mask = ""

        conn.execute(
            """
            INSERT INTO order_applications (
                order_id, executor_id, customer_id, price, deadline, comment,
                status, created_at, updated_at, executor_card_mask, executor_ton_mask
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id, executor_id) DO UPDATE SET
                price=excluded.price,
                deadline=excluded.deadline,
                comment=excluded.comment,
                status='new',
                updated_at=excluded.updated_at,
                executor_card_mask=excluded.executor_card_mask,
                executor_ton_mask=excluded.executor_ton_mask
            """,
            (
                order_id, executor_id, customer_id, price, deadline, comment,
                "new", datetime.now().isoformat(), datetime.now().isoformat(),
                executor_card_mask, executor_ton_mask,
            )
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
        conn.execute("BEGIN IMMEDIATE")
        app = conn.execute(
            "SELECT order_id, executor_id, customer_id, status FROM order_applications WHERE id=?",
            (app_id,),
        ).fetchone()
        if not app:
            await call.answer("Отклик не найден", show_alert=True)
            return

        order_id, executor_id, customer_id, status = app
        if user_id != customer_id and not is_admin(user_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        if status != "new":
            if status == "accepted":
                await call.answer("Принятый отклик нельзя отклонить.", show_alert=True)
            elif status == "rejected":
                await call.answer("Этот отклик уже отклонён.", show_alert=True)
            else:
                await call.answer("Статус отклика уже изменён.", show_alert=True)
            return

        updated = conn.execute(
            "UPDATE order_applications SET status='rejected', updated_at=? WHERE id=? AND status='new'",
            (datetime.now().isoformat(), app_id),
        ).rowcount
        if updated != 1:
            await call.answer("Отклик уже был обработан.", show_alert=True)
            return
        conn.commit()

    try:
        await bot.send_message(executor_id, f"❌ Ваш отклик на заказ #{order_id} отклонён.")
    except Exception:
        pass

    await show_screen(
        call,
        "✅ Отклик отклонён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Все отклики", callback_data=f"order_apps:{order_id}")]
        ]),
    )
    await call.answer()

@dp.callback_query(F.data.startswith("accept_app:"))
async def accept_app(call: CallbackQuery):
    app_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        app = conn.execute(
            """
            SELECT
                a.order_id,
                a.executor_id,
                a.customer_id,
                a.price,
                COALESCE(a.deadline,''),
                COALESCE(a.comment,''),
                COALESCE(a.status,'new'),
                o.title,
                COALESCE(o.status,''),
                COALESCE(o.executor_id, 0)
            FROM order_applications a
            JOIN orders o ON o.id=a.order_id
            WHERE a.id=?
            """,
            (app_id,),
        ).fetchone()
        if not app:
            await call.answer("Отклик не найден", show_alert=True)
            return

        order_id, executor_id, customer_id, price, deadline, comment, app_status, title, order_status, selected_executor_id = app
        if user_id != customer_id and not is_admin(user_id):
            await call.answer("Нет доступа", show_alert=True)
            return

        if app_status != "new":
            if app_status == "accepted":
                await call.answer("Этот отклик уже принят.", show_alert=True)
            elif app_status == "rejected":
                await call.answer("Отклик уже отклонён.", show_alert=True)
            else:
                await call.answer("Отклик уже обработан.", show_alert=True)
            return

        if order_status != "active" or int(selected_executor_id or 0) != 0:
            await call.answer("Исполнитель по этому заказу уже выбран или заказ больше не активен.", show_alert=True)
            return

        commission = int(int(price or 0) * COMMISSION_PERCENT / 100)
        payout = int(price or 0) - commission
        now = datetime.now().isoformat()

        accepted = conn.execute(
            "UPDATE order_applications SET status='accepted', updated_at=? WHERE id=? AND status='new'",
            (now, app_id),
        ).rowcount
        if accepted != 1:
            await call.answer("Отклик уже был обработан.", show_alert=True)
            return

        conn.execute(
            "UPDATE order_applications SET status='rejected', updated_at=? WHERE order_id=? AND id<>? AND status='new'",
            (now, order_id, app_id),
        )

        # После принятия отклика стороны переходят в безопасное обсуждение.
        order_updated = conn.execute(
            "UPDATE orders SET executor_id=?, status='discussion' WHERE id=? AND status='active' AND COALESCE(executor_id,0)=0",
            (executor_id, order_id),
        ).rowcount
        if order_updated != 1:
            # Заказ изменился параллельно — отменяем всю транзакцию целиком.
            conn.rollback()
            await call.answer("Исполнитель уже выбран. Обновите список откликов.", show_alert=True)
            return

        existing = conn.execute(
            """
            SELECT id FROM deals
            WHERE order_id=? AND buyer_id=? AND seller_id=?
              AND status IN ('discussion','waiting_final_price','waiting_buyer_price_confirm','waiting_admin_payment_approval','waiting_payment','waiting_receipt','waiting_admin_confirm','in_work')
            """,
            (order_id, customer_id, executor_id),
        ).fetchone()

        if existing:
            deal_id = int(existing[0])
            conn.execute(
                """
                UPDATE deals
                SET amount=?, commission=?, payout=?, payment_method='admin_card_only', status='discussion', source_type='order'
                WHERE id=?
                """,
                (price, commission, payout, deal_id),
            )
        else:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO deals (
                    listing_id, order_id, buyer_id, seller_id, amount, commission, payout,
                    payment_method, status, source_type, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (0, order_id, customer_id, executor_id, price, commission, payout, "admin_card_only", "discussion", "order", now),
            )
            deal_id = int(cur.lastrowid)

        conn.commit()

    log_admin_action(
        user_id,
        "order_application_accepted_discussion_started",
        executor_id,
        f"order_id={order_id}; app_id={app_id}; deal_id={deal_id}; price={price}; commission={commission}; payout={payout}",
    )

    buyer_text = f"""
━━━━━━━━━━━━━━
✅ <b>Исполнитель выбран</b>
━━━━━━━━━━━━━━

Заказ: <b>{html.escape(title or 'Без названия')}</b>
Исполнитель: <code>{executor_id}</code>
Отклик: <b>#{app_id}</b>

💰 Предварительная цена: <b>{int(price or 0)}₽</b>
💬 Теперь можно безопасно обсудить детали внутри LTeam.
После обсуждения исполнитель предложит итоговую цену сделки.
"""

    seller_text = f"""
━━━━━━━━━━━━━━
🎉 <b>Ваш отклик принят</b>
━━━━━━━━━━━━━━

Заказ: <b>{html.escape(title or 'Без названия')}</b>
Заказчик: <code>{customer_id}</code>

💬 Обсудите детали в безопасном чате LTeam.
После согласования сможете отправить итоговую цену сделки.
"""

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Чат заказа", callback_data=f"order_chat:{order_id}:{executor_id}")],
        [InlineKeyboardButton(text="🤝 Открыть сделку", callback_data=f"deal:{deal_id}")],
        [InlineKeyboardButton(text="⬅️ К заказу", callback_data=f"view_order:{order_id}")],
    ])

    await show_screen(call, buyer_text, reply_markup=keyboard, parse_mode="HTML")
    try:
        await bot.send_message(executor_id, seller_text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Исполнитель выбран")

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

    deal_id = 0
    with db() as conn:
        deal_row = conn.execute(
            """
            SELECT id FROM deals
            WHERE order_id=? AND ((buyer_id=? AND seller_id=?) OR (buyer_id=? AND seller_id=?))
            ORDER BY id DESC LIMIT 1
            """,
            (order_id, sender_id, receiver_id, receiver_id, sender_id),
        ).fetchone()
        if deal_row:
            deal_id = int(deal_row[0])

    await state.update_data(order_id=order_id, receiver_id=receiver_id, order_budget=budget, deal_id=deal_id)
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
        reply_markup=InlineKeyboardMarkup(inline_keyboard=(
            ([[InlineKeyboardButton(text="📦 Открыть сделку", callback_data=f"deal:{deal_id}")]] if deal_id else []) +
            [
                [InlineKeyboardButton(text="🔄 Обновить чат", callback_data=f"order_chat:{order_id}:{receiver_id}")],
                [InlineKeyboardButton(text="⬅️ Открыть заказ", callback_data=f"view_order:{order_id}")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
            ]
        )),
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
        if looks_like_bypass_attempt(text):
            await apply_bypass_punishment(message.from_user.id, f"order #{order_id}", text)
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

    protect_ok, protect_reason = await protect_check_outgoing_message(message.from_user.id, text, f"order #{order_id}")
    if not protect_ok:
        await screen_answer(
            message,
            html.escape(protect_reason),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💬 Написать заново", callback_data=f"order_chat:{order_id}:{receiver_id}")],
                [InlineKeyboardButton(text="📌 Открыть заказ", callback_data=f"view_order:{order_id}")],
            ]),
            parse_mode="HTML",
        )
        return

    await protect_notify_partner_if_needed(message.from_user.id, receiver_id, f"order #{order_id}")

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
    await ask_admin_user_query(call, state, "profile", "🔎 <b>Поиск пользователя</b>", "admin_panel")
    await call.answer()

@dp.message(AdminSearchUserState.user_id)
async def admin_find_user_result(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    rows = find_users_for_admin(message.text or "")
    await state.clear()
    if not rows:
        await screen_answer(message, "❌ Пользователь не найден. Введите ID, @username или ник.", parse_mode="HTML")
        return
    if len(rows) == 1:
        user_id = int(rows[0][0])
        await screen_answer(message, "Откройте профиль пользователя:", reply_markup=user_action_after_pick_keyboard("profile", user_id), parse_mode="HTML")
        return
    await screen_answer(message, "Найдено несколько пользователей:", reply_markup=user_pick_keyboard(rows, "profile"), parse_mode="HTML")

@dp.callback_query(F.data == "admin_reports")
async def admin_reports(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    ensure_admin_tables()
    if is_owner(call.from_user.id):
        where_sql = "COALESCE(status, 'new')='new'"
    else:
        # Жалобы на админов видят только владельцы.
        where_sql = "COALESCE(status, 'new')='new' AND COALESCE(target_type, 'listing')!='admin'"

    with db() as conn:
        rows = conn.execute(f"""
            SELECT id, user_id, COALESCE(target_type, 'listing'), COALESCE(target_id, listing_id), reason
            FROM reports
            WHERE {where_sql}
            ORDER BY id DESC
            LIMIT 10
        """).fetchall()

    if not rows:
        await show_screen(
            call,
            "🚨 Новых жалоб нет.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]
            ]),
        )
        await call.answer()
        return

    text = "━━━━━━━━━━━━━━\n🚨 <b>Новые жалобы</b>\n━━━━━━━━━━━━━━\n\n"
    buttons = []

    for report_id, reporter_id, target_type, target_id, reason in rows:
        is_admin_report = target_type == "admin"
        marker = "👑 Жалоба на админа" if is_admin_report else "🚨 Жалоба"
        text += f"""
<b>#{report_id}</b> • {marker}
Тип: <b>{html.escape(target_type or '—')}</b>
Цель: <code>{target_id}</code>
От: <code>{reporter_id}</code>
Причина: {html.escape((reason or '')[:240])}

"""

        if is_admin_report:
            if is_owner(call.from_user.id):
                buttons.append([
                    InlineKeyboardButton(text=f"✅ Закрыть #{report_id}", callback_data=f"admin_close_report:{report_id}"),
                    InlineKeyboardButton(text=f"👤 Админ {target_id}", callback_data=f"admin_user:{target_id}"),
                ])
        else:
            buttons.append([
                InlineKeyboardButton(text=f"✅ Закрыть #{report_id}", callback_data=f"admin_close_report:{report_id}")
            ])

    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_close_report:"))
async def admin_close_report(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    ensure_admin_tables()
    report_id = int(call.data.split(":")[1])

    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(target_type, 'listing'), COALESCE(target_id, listing_id)
            FROM reports
            WHERE id=?
            """,
            (report_id,),
        ).fetchone()

    if not row:
        await call.answer("Жалоба не найдена", show_alert=True)
        return

    target_type, target_id = row

    if target_type == "admin":
        ok, reason = can_act(call.from_user.id, target_id, "close_admin_report")
    else:
        ok, reason = can_act(call.from_user.id, target_id, "view_reports")

    if not ok:
        await call.answer(reason, show_alert=True)
        return

    with db() as conn:
        conn.execute("UPDATE reports SET status='closed' WHERE id=?", (report_id,))
        conn.commit()

    log_admin_action(call.from_user.id, "close_report", target_id, f"report_id={report_id}; target_type={target_type}")

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
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "warn")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    await state.update_data(admin_target_user_id=user_id)
    await state.set_state(AdminWarnState.reason)
    await show_screen(call, f"⚠️ Введите причину предупреждения для пользователя <code>{user_id}</code>.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Профиль пользователя", callback_data=f"admin_user:{user_id}")]]), parse_mode="HTML")
    await call.answer()

@dp.message(AdminWarnState.reason)
async def admin_warn_user_finish(message: Message, state: FSMContext):
    if not is_staff(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    user_id = int(data.get("admin_target_user_id") or 0)
    reason = (message.text or "").strip()

    if not user_id or len(reason) < 3:
        await screen_answer(message, "Причина слишком короткая.")
        return

    ok, reason_text = can_act(message.from_user.id, user_id, "warn")
    if not ok:
        await state.clear()
        await screen_answer(message, f"❌ {html.escape(reason_text)}")
        return

    with db() as conn:
        conn.execute(
            "INSERT INTO admin_warnings (user_id, admin_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (user_id, message.from_user.id, reason, datetime.now().isoformat())
        )
        conn.commit()

    log_admin_action(message.from_user.id, "warn_user", user_id, reason)

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
            [InlineKeyboardButton(text="💬 Центр чатов", callback_data="admin_chat_hint")],
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



# ===== LTEAM APPEAL UX =====

def get_user_restrictions(user_id: int) -> dict:
    """Возвращает человекочитаемое состояние ограничений LTeam Protect.

    Функция нужна для UX-экранов Protect и апелляций. Источник истины —
    protect_policy_for_user(), поэтому ограничения не расходятся с реальной
    логикой блокировок в чатах, объявлениях, заказах и откликах.
    """
    policy = protect_policy_for_user(int(user_id))
    blocked_actions: list[str] = []

    if policy.get("block_chats"):
        blocked_actions.append("безопасные чаты")
    if policy.get("block_create_listing"):
        blocked_actions.append("создание объявлений")
    if policy.get("block_create_order"):
        blocked_actions.append("создание заказов")
    if policy.get("block_order_application"):
        blocked_actions.append("отклики на заказы")

    if blocked_actions:
        reason = "Заблокированы: " + ", ".join(blocked_actions) + ". Подайте апелляцию Protect для ручной проверки."
        return {
            "blocked": True,
            "reason": reason,
            "actions": blocked_actions,
            "level": policy.get("level", "high"),
            "score": int(policy.get("score", 0) or 0),
        }

    if policy.get("force_moderation"):
        return {
            "blocked": False,
            "reason": "Критичных блокировок нет, но новые действия могут проходить усиленную модерацию.",
            "actions": [],
            "level": policy.get("level", "medium"),
            "score": int(policy.get("score", 0) or 0),
        }

    return {
        "blocked": False,
        "reason": "Критичных ограничений сейчас нет.",
        "actions": [],
        "level": policy.get("level", "low"),
        "score": int(policy.get("score", 0) or 0),
    }


def appeal_intro_text(user_id: int) -> str:
    risk = get_user_security_score(user_id)
    restrictions = get_user_restrictions(user_id)
    status_line = "🔒 Есть ограничения" if restrictions.get("blocked") else "✅ Критичных ограничений нет"
    return f"""
━━━━━━━━━━━━━━
⚖️ <b>Апелляция LTeam Protect</b>
━━━━━━━━━━━━━━

🛡 <b>Ваш статус</b>
{risk['badge']} · <b>{risk['score']}/100</b>
{status_line}

<b>Когда нужна апелляция?</b>
Если Protect ограничил действие ошибочно, владельцы вручную проверят историю аккаунта, жалобы, предупреждения и события безопасности.

<b>Что написать:</b>
• какое действие было заблокировано;
• почему сработал Protect;
• что вы готовы исправить;
• почему аккаунту можно доверять.

━━━━━━━━━━━━━━
✍️ Отправьте объяснение одним сообщением.
"""


@dp.callback_query(F.data == "protect_status")
async def protect_status(call: CallbackQuery):
    risk = get_user_security_score(call.from_user.id)
    restrictions = get_user_restrictions(call.from_user.id)
    restriction_text = restrictions.get("reason") or "Критичных ограничений сейчас нет."
    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
🛡 <b>Мой статус LTeam Protect</b>
━━━━━━━━━━━━━━

Статус: <b>{risk['badge']}</b>
Score: <b>{risk['score']}/100</b>

<b>Ограничения:</b>
{html.escape(restriction_text)}

<b>Факторы:</b>
{security_reasons_text(risk.get('reasons', []), limit=6)}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚖️ Апелляция Protect", callback_data="protect_appeal_start")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()

# ===== LTEAM PROTECT APPEALS =====

async def notify_owners(text: str, reply_markup=None):
    owner_ids = OWNER_IDS or ADMIN_IDS[:1]
    for owner_id in owner_ids:
        try:
            await bot.send_message(owner_id, text, reply_markup=reply_markup, parse_mode="HTML")
        except Exception:
            pass


def pending_appeal_for_user(user_id: int):
    ensure_admin_tables()
    with db() as conn:
        return conn.execute(
            """
            SELECT id, reason, created_at
            FROM protect_appeals
            WHERE user_id=? AND status='pending'
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()


@dp.message(Command("appeal"))
async def command_appeal(message: Message, state: FSMContext):
    save_user(message)
    await state.set_state(AppealState.reason)
    risk = get_user_security_score(message.from_user.id)
    await screen_answer(
        message,
        appeal_intro_text(message.from_user.id),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="protect_appeal_cancel")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        ]),
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "protect_appeal_start")
async def protect_appeal_start(call: CallbackQuery, state: FSMContext):
    existing = pending_appeal_for_user(call.from_user.id)
    if existing:
        appeal_id, reason, created_at = existing
        await show_screen(
            call,
            f"""
━━━━━━━━━━━━━━
⚖️ <b>Апелляция уже на проверке</b>
━━━━━━━━━━━━━━

Заявка: <b>#{appeal_id}</b>
Создана: <code>{html.escape((created_at or '')[:16])}</code>

Причина:
{html.escape((reason or '')[:800])}

Дождитесь решения владельцев. Новую апелляцию можно будет создать после обработки текущей.
""",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛡 Мой статус Protect", callback_data="protect_status")],
                [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
            ]),
            parse_mode="HTML",
        )
        await call.answer("Апелляция уже создана", show_alert=True)
        return
    risk = get_user_security_score(call.from_user.id)
    await state.set_state(AppealState.reason)
    await show_screen(
        call,
        appeal_intro_text(call.from_user.id),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="protect_appeal_cancel")],
            [InlineKeyboardButton(text="🛡 Мой статус", callback_data="protect_status")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "protect_appeal_cancel")
async def protect_appeal_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_screen(
        call,
        "⚖️ Апелляция отменена. Вернуться к ней можно из профиля или поддержки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
            [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(AppealState.reason)
async def protect_appeal_save(message: Message, state: FSMContext):
    save_user(message)
    text = (message.text or "").strip()
    if len(text) < 20:
        await screen_answer(message, "Напишите подробнее: минимум 20 символов. Объясните, почему ограничение нужно снять.", parse_mode="HTML")
        return
    if len(text) > 1500:
        await screen_answer(message, "Апелляция слишком длинная. Максимум 1500 символов.", parse_mode="HTML")
        return
    existing = pending_appeal_for_user(message.from_user.id)
    if existing:
        await state.clear()
        await screen_answer(
            message,
            f"⚖️ У вас уже есть апелляция на проверке: <b>#{existing[0]}</b>.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛡 Мой статус Protect", callback_data="protect_status")],
                [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
            ]),
            parse_mode="HTML",
        )
        return
    ensure_admin_tables()
    with db() as conn:
        cur = conn.execute(
            """INSERT INTO protect_appeals (user_id, reason, status, created_at) VALUES (?, ?, 'pending', ?)""",
            (message.from_user.id, text, datetime.now().isoformat()),
        )
        appeal_id = cur.lastrowid
        conn.commit()
    register_security_event(message.from_user.id, "protect_appeal_created", f"appeal #{appeal_id}", text, status="new")
    log_admin_action(message.from_user.id, "protect_appeal_created", message.from_user.id, f"appeal_id={appeal_id}")
    risk = get_user_security_score(message.from_user.id)
    await notify_owners(
        f"""
⚖️ <b>Новая апелляция Protect</b>

Заявка: <b>#{appeal_id}</b>
Пользователь: <code>{message.from_user.id}</code>
Риск: <b>{risk['badge']}</b> / <b>{risk['score']}/100</b>

Текст:
{html.escape(text[:1000])}
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_appeal_approve:{appeal_id}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_appeal_reject:{appeal_id}")],
            [InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user:{message.from_user.id}"), InlineKeyboardButton(text="⚖️ Все апелляции", callback_data="admin_protect_appeals")],
        ]),
    )
    await state.clear()
    await screen_answer(
        message,
        f"""
━━━━━━━━━━━━━━
✅ <b>Апелляция отправлена</b>
━━━━━━━━━━━━━━

🧾 Номер заявки: <b>#{appeal_id}</b>
⏳ Статус: <b>на проверке у владельцев</b>

Владельцы проверят историю аккаунта, жалобы, предупреждения и события Protect. После решения вы получите уведомление в боте.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛡 Мой статус Protect", callback_data="protect_status")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        ]),
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "admin_protect_appeals")
async def admin_protect_appeals(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Апелляции видят только владельцы", show_alert=True)
        return
    ensure_admin_tables()
    with db() as conn:
        rows = conn.execute("""
            SELECT id, user_id, reason, created_at
            FROM protect_appeals
            WHERE status='pending'
            ORDER BY id DESC
            LIMIT 10
        """).fetchall()
    if not rows:
        await show_screen(call, "⚖️ Новых апелляций Protect нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")]]), parse_mode="HTML")
        await call.answer()
        return
    text = "━━━━━━━━━━━━━━\n⚖️ <b>Апелляции Protect</b>\n━━━━━━━━━━━━━━\n\n"
    buttons = []
    for appeal_id, user_id, reason, created_at in rows:
        risk = get_user_security_score(user_id)
        text += f"""
<b>#{appeal_id}</b> • Пользователь: <code>{user_id}</code>
Риск: <b>{risk['badge']}</b> / <b>{risk['score']}/100</b>
Дата: <code>{html.escape((created_at or '')[:16])}</code>
Текст: {html.escape((reason or '')[:220])}

"""
        buttons.append([InlineKeyboardButton(text=f"✅ Одобрить #{appeal_id}", callback_data=f"admin_appeal_approve:{appeal_id}"), InlineKeyboardButton(text=f"❌ Отклонить #{appeal_id}", callback_data=f"admin_appeal_reject:{appeal_id}")])
        buttons.append([InlineKeyboardButton(text=f"👤 Пользователь {user_id}", callback_data=f"admin_user:{user_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Безопасность", callback_data="admin_security_center")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_appeal_approve:"))
async def admin_appeal_approve(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Только владелец может одобрять апелляции", show_alert=True)
        return
    appeal_id = int(call.data.split(":")[1])
    ensure_admin_tables()
    with db() as conn:
        row = conn.execute("SELECT user_id, reason, status FROM protect_appeals WHERE id=?", (appeal_id,)).fetchone()
    if not row:
        await call.answer("Апелляция не найдена", show_alert=True)
        return
    user_id, reason, status = row
    if status != "pending":
        await call.answer("Апелляция уже обработана", show_alert=True)
        return
    create_protect_override(user_id, call.from_user.id, f"Апелляция #{appeal_id} одобрена", days=30)
    with db() as conn:
        conn.execute("""
            UPDATE protect_appeals
            SET status='approved', reviewer_id=?, admin_comment=?, reviewed_at=?
            WHERE id=?
        """, (call.from_user.id, "Ограничения Protect сняты на 30 дней", datetime.now().isoformat(), appeal_id))
        conn.commit()
    log_admin_action(call.from_user.id, "protect_appeal_approved", user_id, f"appeal_id={appeal_id}")
    register_security_event(user_id, "protect_appeal_approved", f"appeal #{appeal_id}", status="closed")
    try:
        await bot.send_message(user_id, f"""
✅ <b>Апелляция Protect одобрена</b>

Заявка: <b>#{appeal_id}</b>
Ограничения LTeam Protect сняты на <b>30 дней</b>.

Важно: если снова будут попытки обхода гаранта, жалобы или нарушения, ограничения вернутся.
""", parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Апелляция одобрена", show_alert=True)
    call.data = "admin_protect_appeals"
    await admin_protect_appeals(call)


@dp.callback_query(F.data.startswith("admin_appeal_reject:"))
async def admin_appeal_reject(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Только владелец может отклонять апелляции", show_alert=True)
        return
    appeal_id = int(call.data.split(":")[1])
    ensure_admin_tables()
    with db() as conn:
        row = conn.execute("SELECT user_id, reason, status FROM protect_appeals WHERE id=?", (appeal_id,)).fetchone()
    if not row:
        await call.answer("Апелляция не найдена", show_alert=True)
        return
    user_id, reason, status = row
    if status != "pending":
        await call.answer("Апелляция уже обработана", show_alert=True)
        return
    with db() as conn:
        conn.execute("""
            UPDATE protect_appeals
            SET status='rejected', reviewer_id=?, admin_comment=?, reviewed_at=?
            WHERE id=?
        """, (call.from_user.id, "Апелляция отклонена владельцем", datetime.now().isoformat(), appeal_id))
        conn.commit()
    log_admin_action(call.from_user.id, "protect_appeal_rejected", user_id, f"appeal_id={appeal_id}")
    register_security_event(user_id, "protect_appeal_rejected", f"appeal #{appeal_id}", status="closed")
    try:
        await bot.send_message(user_id, f"""
❌ <b>Апелляция Protect отклонена</b>

Заявка: <b>#{appeal_id}</b>
Ограничения остаются активными.

Вы можете снизить риск: не нарушать правила, не уводить сделки в личку, работать через гаранта и дождаться повторной проверки.
""", parse_mode="HTML")
    except Exception:
        pass
    await call.answer("Апелляция отклонена", show_alert=True)
    call.data = "admin_protect_appeals"
    await admin_protect_appeals(call)


@dp.callback_query(F.data == "admin_security_center")
async def admin_security_center(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    ensure_admin_tables()
    now = datetime.now()
    day_ago = now - timedelta(days=1)

    with db() as conn:
        new_reports = conn.execute("SELECT COUNT(*) FROM reports WHERE COALESCE(status,'new')='new'").fetchone()[0]
        events_new = conn.execute("SELECT COUNT(*) FROM security_events WHERE COALESCE(status,'new')='new'").fetchone()[0]
        events_24h = conn.execute("SELECT COUNT(*) FROM security_events WHERE created_at>=?", (day_ago.isoformat(),)).fetchone()[0]
        appeals = conn.execute("SELECT COUNT(*) FROM protect_appeals WHERE status='pending'").fetchone()[0]
        banned = conn.execute("SELECT COUNT(*) FROM banned_users").fetchone()[0]
        active_mutes = conn.execute("SELECT COUNT(*) FROM muted_users WHERE muted_until>?", (now.isoformat(),)).fetchone()[0]
        warnings = conn.execute("SELECT COUNT(*) FROM admin_warnings").fetchone()[0]
        last_events = conn.execute("""
            SELECT user_id, event_type, context, status, created_at
            FROM security_events
            ORDER BY id DESC
            LIMIT 7
        """).fetchall()

    events_text = ""
    if last_events:
        for uid, event_type, context, status, created_at in last_events:
            events_text += (
                f"• <code>{uid}</code> — <b>{html.escape(event_type or 'event')}</b>\n"
                f"  {html.escape(context or '—')} • <code>{html.escape(status or 'new')}</code> • {html.escape((created_at or '')[:16])}\n"
            )
    else:
        events_text = "Событий пока нет."

    await show_screen(call, f"""
━━━━━━━━━━━━━━
🛡 <b>Security Center</b>
━━━━━━━━━━━━━━

🚨 Новые жалобы: <b>{new_reports}</b>
⚠️ Новые security events: <b>{events_new}</b>
🕒 Events за 24ч: <b>{events_24h}</b>
⚖️ Апелляции Protect: <b>{appeals}</b>
🚫 Баны: <b>{banned}</b>
🔇 Активные муты: <b>{active_mutes}</b>
📌 Всего предупреждений: <b>{warnings}</b>

<b>Последние события:</b>
{events_text}
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🚨 Жалобы ({new_reports})", callback_data="admin_reports"), InlineKeyboardButton(text=f"⚠️ Events ({events_new})", callback_data="admin_security_events")],
        [InlineKeyboardButton(text=f"⚖️ Апелляции ({appeals})", callback_data="admin_protect_appeals")],
        [InlineKeyboardButton(text="🚫 Баны", callback_data="admin_bans_list"), InlineKeyboardButton(text="📜 Логи", callback_data="admin_logs_page:0")],
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
        await call.answer("Только владелец", show_alert=True)
        return
    await ask_admin_user_query(call, state, "role", "👑 <b>Назначение роли</b>", "admin_roles_panel")
    await call.answer()


@dp.message(AdminRoleState.user_id)
async def admin_role_add_id(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await state.clear(); return
    rows = find_users_for_admin(message.text or "")
    if not rows:
        await screen_answer(message, "❌ Пользователь не найден. Введите ID, @username или ник.", parse_mode="HTML")
        return
    if len(rows) > 1:
        await state.clear()
        await screen_answer(message, "Найдено несколько пользователей:", reply_markup=user_pick_keyboard(rows, "role", "admin_roles_panel"), parse_mode="HTML")
        return
    uid = int(rows[0][0])
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

        # Если пользователя повысили до staff после старых санкций,
        # очищаем ошибочные бан/мут сразу, не дожидаясь следующего сообщения.
        cleaned = cleanup_protected_punishments()
        log_admin_action(call.from_user.id, "set_role", uid, f"role={role}; cleanup={cleaned}")
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
        await call.answer("Нет доступа", show_alert=True)
        return

    minutes = int(call.data.split(":")[1])
    data = await state.get_data()
    uid = int(data.get("mute_user_id") or 0)

    if not uid:
        await call.answer("Пользователь потерян", show_alert=True)
        return

    ok, reason = can_act(call.from_user.id, uid, "mute")
    if not ok:
        await state.clear()
        await call.answer(reason, show_alert=True)
        return

    muted = set_mute(uid, minutes, "Мут выдан модерацией", muted_by=call.from_user.id)
    if not muted:
        await state.clear()
        await call.answer("Мут не выдан: пользователь защищён системой ролей.", show_alert=True)
        return

    try:
        await bot.send_message(uid, f"🔇 Вам выдан мут на {minutes} мин. Причина: модерация LTeam")
    except Exception:
        pass

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


# ===== LTEAM PROFILE ADMIN TOOLS =====

@dp.callback_query(F.data == "admin_verification_requests")
async def admin_verification_requests(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Только владельцы", show_alert=True)
        return
    ensure_profile_tables()
    with db() as conn:
        rows = conn.execute("""
            SELECT id, user_id, reason, created_at
            FROM verification_requests
            WHERE status='pending'
            ORDER BY id DESC
            LIMIT 15
        """).fetchall()
    if not rows:
        await show_screen(call, "✅ Заявок на галочку нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")]]), parse_mode="HTML")
        await call.answer()
        return
    text = "━━━━━━━━━━━━━━\n✅ <b>Заявки на галочку</b>\n━━━━━━━━━━━━━━\n\n"
    buttons = []
    for req_id, user_id, reason, created_at in rows:
        text += f"<b>#{req_id}</b> • <code>{user_id}</code> • {profile_title(user_id)}\n"
        text += f"Причина: {html.escape((reason or '')[:180])}\n\n"
        buttons.append([InlineKeyboardButton(text=f"✅ Одобрить #{req_id}", callback_data=f"verify_req_approve:{req_id}"), InlineKeyboardButton(text=f"❌ Отклонить #{req_id}", callback_data=f"verify_req_reject:{req_id}")])
        buttons.append([InlineKeyboardButton(text=f"👤 Открыть пользователя {user_id}", callback_data=f"admin_user:{user_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("verify_req_approve:"))
async def verify_req_approve(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Только владельцы", show_alert=True)
        return
    req_id = int(call.data.split(":")[1])
    ensure_profile_tables()
    with db() as conn:
        row = conn.execute("SELECT user_id, status FROM verification_requests WHERE id=?", (req_id,)).fetchone()
        if not row:
            await call.answer("Заявка не найдена", show_alert=True)
            return
        user_id, status = row
        if status != "pending":
            await call.answer("Заявка уже обработана", show_alert=True)
            return
        conn.execute("UPDATE users SET verified=1 WHERE user_id=?", (user_id,))
        conn.execute("""
            UPDATE verification_requests
            SET status='approved', reviewed_by=?, decision='approved', reviewed_at=?
            WHERE id=?
        """, (call.from_user.id, datetime.now().isoformat(), req_id))
        conn.commit()
    log_admin_action(call.from_user.id, "approve_verification", user_id, f"request_id={req_id}")
    try:
        await bot.send_message(user_id, "✅ Ваша заявка на галочку одобрена. Теперь в профиле отображается верификация LTeam.")
    except Exception:
        pass
    await call.answer("Галочка выдана", show_alert=True)
    await admin_verification_requests(call)


@dp.callback_query(F.data.startswith("verify_req_reject:"))
async def verify_req_reject(call: CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Только владельцы", show_alert=True)
        return
    req_id = int(call.data.split(":")[1])
    ensure_profile_tables()
    with db() as conn:
        row = conn.execute("SELECT user_id, status FROM verification_requests WHERE id=?", (req_id,)).fetchone()
        if not row:
            await call.answer("Заявка не найдена", show_alert=True)
            return
        user_id, status = row
        if status != "pending":
            await call.answer("Заявка уже обработана", show_alert=True)
            return
        conn.execute("""
            UPDATE verification_requests
            SET status='rejected', reviewed_by=?, decision='rejected', reviewed_at=?
            WHERE id=?
        """, (call.from_user.id, datetime.now().isoformat(), req_id))
        conn.commit()
    log_admin_action(call.from_user.id, "reject_verification", user_id, f"request_id={req_id}")
    try:
        await bot.send_message(user_id, "❌ Заявка на галочку отклонена. Вы можете улучшить профиль, завершить сделки и подать заявку позже.")
    except Exception:
        pass
    await call.answer("Заявка отклонена", show_alert=True)
    await admin_verification_requests(call)


@dp.callback_query(F.data.startswith("admin_grant_plus:"))
async def admin_grant_plus(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])

    ok, reason = can_act(call.from_user.id, user_id, "grant_plus")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    until = set_user_plus(user_id, LTEAM_PLUS_DAYS_DEFAULT, actor_id=call.from_user.id)

    try:
        await bot.send_message(
            user_id,
            f"💎 Вам активирована подписка LTeam Plus на {LTEAM_PLUS_DAYS_DEFAULT} дней."
        )
    except Exception:
        pass

    log_admin_action(call.from_user.id, "grant_lteam_plus", user_id, f"until={until}")
    await call.answer("LTeam Plus активирован", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


# ===== LTEAM ADMIN CENTER V2 ADDONS =====

def cleanup_protected_punishments() -> int:
    """Удаляет старые ошибочные баны/муты владельцев, админов и модераторов."""
    ensure_admin_tables()
    protected_ids = protected_staff_ids()

    with db() as conn:
        changed = 0
        for uid in protected_ids:
            ban_deleted = conn.execute("DELETE FROM banned_users WHERE user_id=?", (uid,)).rowcount
            mute_deleted = conn.execute("DELETE FROM muted_users WHERE user_id=?", (uid,)).rowcount

            if ban_deleted or mute_deleted:
                changed += ban_deleted + mute_deleted
                conn.execute(
                    """
                    INSERT INTO admin_action_logs
                    (actor_id, target_id, action, details, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (0, uid, "cleanup_protected_punishment", "removed ban/mute from protected staff", datetime.now().isoformat())
                )

        conn.commit()

    return changed


def staff_safety_snapshot() -> dict:
    """Сводка по защищённым staff-аккаунтам и ошибочным старым санкциям."""
    ensure_admin_tables()
    protected_ids = sorted(protected_staff_ids())

    if not protected_ids:
        return {
            "protected_count": 0,
            "staff_rows": [],
            "banned_rows": [],
            "muted_rows": [],
            "env_owner_count": len(OWNER_IDS),
            "env_admin_count": len(ADMIN_IDS),
            "env_moderator_count": len(MODERATOR_IDS),
        }

    placeholders = ",".join("?" for _ in protected_ids)
    with db() as conn:
        staff_rows = conn.execute(
            f"""
            SELECT user_id, role, COALESCE(assigned_by, 0), COALESCE(created_at, '')
            FROM staff_roles
            WHERE user_id IN ({placeholders})
            ORDER BY CASE role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 WHEN 'moderator' THEN 2 ELSE 3 END, user_id
            """,
            protected_ids,
        ).fetchall()
        banned_rows = conn.execute(
            f"SELECT user_id, COALESCE(reason,''), COALESCE(banned_by,0) FROM banned_users WHERE user_id IN ({placeholders}) ORDER BY user_id",
            protected_ids,
        ).fetchall()
        muted_rows = conn.execute(
            f"SELECT user_id, COALESCE(muted_until,''), COALESCE(reason,''), COALESCE(muted_by,0) FROM muted_users WHERE user_id IN ({placeholders}) ORDER BY user_id",
            protected_ids,
        ).fetchall()

    return {
        "protected_count": len(protected_ids),
        "staff_rows": staff_rows,
        "banned_rows": banned_rows,
        "muted_rows": muted_rows,
        "env_owner_count": len(OWNER_IDS),
        "env_admin_count": len(ADMIN_IDS),
        "env_moderator_count": len(MODERATOR_IDS),
    }


@dp.callback_query(F.data == "admin_staff_safety")
async def admin_staff_safety(call: CallbackQuery):
    ok, reason = can_act(call.from_user.id, None, "cleanup_staff_punishments")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    snap = staff_safety_snapshot()
    banned_count = len(snap.get("banned_rows", []))
    muted_count = len(snap.get("muted_rows", []))
    state_badge = "🟢 Норма" if banned_count == 0 and muted_count == 0 else "🟠 Есть старые санкции для очистки"

    staff_preview = []
    for uid, role, assigned_by, created_at in snap.get("staff_rows", [])[:12]:
        staff_preview.append(f"• <code>{uid}</code> — <b>{html.escape(role or 'user')}</b>")
    if not staff_preview:
        staff_preview.append("• роли из БД пока не назначены")

    await show_screen(
        call,
        f"""
━━━━━━━━━━━━━━
🛡 <b>Staff Safety Center</b>
━━━━━━━━━━━━━━

Состояние: <b>{state_badge}</b>

<b>Защищённые аккаунты:</b> <b>{snap.get('protected_count', 0)}</b>
• OWNER_IDS в .env: <b>{snap.get('env_owner_count', 0)}</b>
• ADMIN_IDS в .env: <b>{snap.get('env_admin_count', 0)}</b>
• MODERATOR_IDS в .env: <b>{snap.get('env_moderator_count', 0)}</b>

<b>Проверка санкций:</b>
• ошибочных банов staff: <b>{banned_count}</b>
• ошибочных мутов staff: <b>{muted_count}</b>

<b>Staff из базы:</b>
{chr(10).join(staff_preview)}

Правило LTeam:
<b>owner / admin / moderator нельзя банить, мутить или варнить через обычную админку.</b>
Если нужно наказать staff — владелец сначала снимает роль, затем применяет обычное действие.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🧹 Очистить старые санкции", callback_data="admin_cleanup_staff_punishments")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_staff_safety")],
            [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data == "admin_cleanup_staff_punishments")
async def admin_cleanup_staff_punishments(call: CallbackQuery):
    ok, reason = can_act(call.from_user.id, None, "cleanup_staff_punishments")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    changed = cleanup_protected_punishments()
    log_admin_action(call.from_user.id, "manual_cleanup_staff_punishments", None, f"changed={changed}")

    await call.answer(f"Готово. Исправлено записей: {changed}", show_alert=True)
    await admin_panel(call)


@dp.callback_query(F.data.startswith("admin_logs_page:"))
async def admin_logs_page(call: CallbackQuery):
    ok, reason = can_act(call.from_user.id, None, "view_logs")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    page = int(call.data.split(":")[1])
    limit = 10
    offset = page * limit

    with db() as conn:
        rows = conn.execute("""
            SELECT actor_id, target_id, action, details, created_at
            FROM admin_action_logs
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()

    text = """
━━━━━━━━━━━━━━
📜 <b>Админ-логи</b>
━━━━━━━━━━━━━━

"""

    if not rows:
        text += "Логов пока нет."
    else:
        for actor_id, target_id, action, details, created_at in rows:
            text += (
                f"• <b>{html.escape(action or 'action')}</b>\n"
                f"  Кто: <code>{actor_id}</code>\n"
                f"  Цель: <code>{target_id or '—'}</code>\n"
                f"  Детали: {html.escape((details or '—')[:180])}\n"
                f"  Время: <code>{html.escape((created_at or '')[:16])}</code>\n\n"
            )

    buttons = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_logs_page:{page - 1}"))
    if len(rows) == limit:
        nav.append(InlineKeyboardButton(text="➡️ Далее", callback_data=f"admin_logs_page:{page + 1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])

    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_subscriptions_center")
async def admin_subscriptions_center(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    await show_screen(call, """
━━━━━━━━━━━━━━
💎 <b>Plus / Verified Center</b>
━━━━━━━━━━━━━━

Рабочий сценарий:
1. Найдите пользователя.
2. Откройте карточку.
3. Выдайте LTeam Plus или Verified.
4. Действие будет проверено через роли и записано в логи.

Защита:
• админ не может выдать/снять статус равной или старшей роли;
• владельца нельзя изменить через обычную админку;
• все действия пишутся в admin_action_logs.
""", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Найти пользователя", callback_data="admin_find_user")],
        [InlineKeyboardButton(text="📜 Логи", callback_data="admin_logs_page:0")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]), parse_mode="HTML")
    await call.answer()



# ===== LTEAM MARKET V7: ГИБКИЕ РЕКВИЗИТЫ ВЫПЛАТ =====

class PayoutProfileState(StatesGroup):
    card_or_phone = State()
    ton_wallet = State()


def ensure_payout_profile_tables() -> None:
    """Профиль выплат пользователя: карта/номер и TON можно хранить независимо."""
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_payout_profiles (
            user_id INTEGER PRIMARY KEY,
            card_or_phone TEXT,
            ton_wallet TEXT,
            preferred_method TEXT DEFAULT '',
            created_at TEXT,
            updated_at TEXT
        )
        """)
        for column_sql in [
            "card_or_phone TEXT",
            "ton_wallet TEXT",
            "preferred_method TEXT DEFAULT ''",
            "created_at TEXT",
            "updated_at TEXT",
        ]:
            try:
                cur.execute(f"ALTER TABLE user_payout_profiles ADD COLUMN {column_sql}")
            except sqlite3.OperationalError:
                pass
        conn.commit()


def get_payout_profile(user_id: int) -> dict:
    ensure_payout_profile_tables()
    with db() as conn:
        row = conn.execute("""
            SELECT card_or_phone, ton_wallet, preferred_method, created_at, updated_at
            FROM user_payout_profiles
            WHERE user_id=?
        """, (user_id,)).fetchone()

    if not row:
        return {
            "card_or_phone": "",
            "ton_wallet": "",
            "preferred_method": "",
            "has_any": False,
            "created_at": "",
            "updated_at": "",
        }

    card_or_phone, ton_wallet, preferred_method, created_at, updated_at = row
    return {
        "card_or_phone": card_or_phone or "",
        "ton_wallet": ton_wallet or "",
        "preferred_method": preferred_method or "",
        "has_any": bool(card_or_phone or ton_wallet),
        "created_at": created_at or "",
        "updated_at": updated_at or "",
    }


def mask_card_or_phone(value: str) -> str:
    value = (value or "").strip()
    digits = re.sub(r"\D+", "", value)

    if len(digits) >= 12:
        return f"**** **** **** {digits[-4:]}"
    if len(digits) >= 7:
        return f"+*** *** {digits[-4:]}"
    if value:
        return html.escape(value[:3] + "***")
    return "не привязано"


def mask_ton_wallet(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return "не привязано"
    if len(value) <= 12:
        return html.escape(value)
    return html.escape(f"{value[:6]}...{value[-6:]}")


def payout_profile_text(user_id: int) -> str:
    profile = get_payout_profile(user_id)
    preferred = profile.get("preferred_method") or "не выбран"

    preferred_text = {
        "card": "Карта/номер",
        "ton": "TON-кошелёк",
        "": "не выбран",
    }.get(preferred, preferred)

    return f"""
━━━━━━━━━━━━━━
💳 <b>Реквизиты выплат</b>
━━━━━━━━━━━━━━

Основной способ: <b>{html.escape(preferred_text)}</b>

💳 Карта/номер: <code>{mask_card_or_phone(profile.get('card_or_phone', ''))}</code>
💎 TON: <code>{mask_ton_wallet(profile.get('ton_wallet', ''))}</code>

Реквизиты нужны только для вывода средств после завершённых сделок.
Создать заказ и откликнуться можно без привязки реквизитов.

Можно привязать только один способ сейчас, а второй добавить позже в профиле.
"""


def payout_profile_keyboard(next_callback: str = "profile") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💳 Привязать карту/номер", callback_data=f"payout_bind_card:{next_callback}"),
        ],
        [
            InlineKeyboardButton(text="💎 Привязать TON", callback_data=f"payout_bind_ton:{next_callback}"),
        ],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=next_callback),
        ],
    ])


def payout_has_any_method(user_id: int) -> bool:
    return bool(get_payout_profile(user_id).get("has_any"))


def payout_missing_text(action_title: str) -> str:
    return f"""
💳 <b>Нужно привязать реквизиты</b>

Чтобы продолжить: <b>{html.escape(action_title)}</b>, выберите один способ выплат:

• <b>Карта/номер</b> — можно указать карту или телефон для перевода.
• <b>TON</b> — кошелёк Telegram/TON.

Это нужно сделать только один раз. Второй способ можно добавить позже в профиле.
"""


def normalize_card_or_phone(value: str) -> tuple[bool, str, str]:
    raw = (value or "").strip()
    digits = re.sub(r"\D+", "", raw)

    if not raw:
        return False, "", "Введите карту или номер телефона."

    # Карта: 13-19 цифр, телефон: 10-15 цифр.
    if digits and 10 <= len(digits) <= 19:
        return True, raw, ""

    return False, "", "Некорректный формат. Введите карту или номер телефона, например: 2200 0000 0000 0000 или +79990000000."


def normalize_ton_wallet(value: str) -> tuple[bool, str, str]:
    raw = (value or "").strip()

    if not raw:
        return False, "", "Введите TON-кошелёк."

    if len(raw) < 24:
        return False, "", "TON-кошелёк выглядит слишком коротким."

    if " " in raw:
        return False, "", "TON-кошелёк не должен содержать пробелы."

    # Не делаем слишком жёсткую проверку, потому что TON-адреса бывают user-friendly/base64url/raw.
    allowed = re.match(r"^[A-Za-z0-9_\-:]+$", raw) is not None
    if not allowed:
        return False, "", "TON-кошелёк содержит недопустимые символы."

    return True, raw, ""


def save_payout_card_or_phone(user_id: int, value: str) -> None:
    ensure_payout_profile_tables()
    now = datetime.now().isoformat()
    with db() as conn:
        existing = conn.execute("SELECT user_id FROM user_payout_profiles WHERE user_id=?", (user_id,)).fetchone()
        if existing:
            conn.execute("""
                UPDATE user_payout_profiles
                SET card_or_phone=?, preferred_method=CASE
                    WHEN COALESCE(preferred_method,'')='' THEN 'card'
                    ELSE preferred_method
                END, updated_at=?
                WHERE user_id=?
            """, (value, now, user_id))
        else:
            conn.execute("""
                INSERT INTO user_payout_profiles
                (user_id, card_or_phone, ton_wallet, preferred_method, created_at, updated_at)
                VALUES (?, ?, '', 'card', ?, ?)
            """, (user_id, value, now, now))
        conn.commit()


def save_payout_ton_wallet(user_id: int, value: str) -> None:
    ensure_payout_profile_tables()
    now = datetime.now().isoformat()
    with db() as conn:
        existing = conn.execute("SELECT user_id FROM user_payout_profiles WHERE user_id=?", (user_id,)).fetchone()
        if existing:
            conn.execute("""
                UPDATE user_payout_profiles
                SET ton_wallet=?, preferred_method=CASE
                    WHEN COALESCE(preferred_method,'')='' THEN 'ton'
                    ELSE preferred_method
                END, updated_at=?
                WHERE user_id=?
            """, (value, now, user_id))
        else:
            conn.execute("""
                INSERT INTO user_payout_profiles
                (user_id, card_or_phone, ton_wallet, preferred_method, created_at, updated_at)
                VALUES (?, '', ?, 'ton', ?, ?)
            """, (user_id, value, now, now))
        conn.commit()


def payout_continue_keyboard(next_callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Продолжить", callback_data=next_callback)],
        [InlineKeyboardButton(text="💳 Реквизиты", callback_data=f"payout_profile:{next_callback}")],
        [InlineKeyboardButton(text="⬅️ В профиль", callback_data="profile")],
    ])


async def require_payout_or_ask(call: CallbackQuery, action_title: str, next_callback: str) -> bool:
    """Возвращает True, если реквизиты есть. Иначе показывает UX привязки."""
    if payout_has_any_method(call.from_user.id):
        return True

    await show_screen(
        call,
        payout_missing_text(action_title),
        reply_markup=payout_profile_keyboard(next_callback),
        parse_mode="HTML",
    )
    await call.answer("Сначала привяжите реквизиты", show_alert=True)
    return False


@dp.callback_query(F.data.startswith("payout_profile:"))
async def payout_profile_open(call: CallbackQuery):
    next_callback = call.data.split(":", 1)[1] if ":" in call.data else "profile"
    await show_screen(
        call,
        payout_profile_text(call.from_user.id),
        reply_markup=payout_profile_keyboard(next_callback),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("payout_bind_card:"))
async def payout_bind_card_start(call: CallbackQuery, state: FSMContext):
    next_callback = call.data.split(":", 1)[1] if ":" in call.data else "profile"
    await state.set_state(PayoutProfileState.card_or_phone)
    await state.update_data(payout_next_callback=next_callback)

    await show_screen(
        call,
        """
💳 <b>Привязка карты/номера</b>

Отправьте одним сообщением:
• номер карты;
или
• номер телефона для перевода.

Примеры:
<code>2200 0000 0000 0000</code>
<code>+79990000000</code>

TON можно будет добавить позже в профиле.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"payout_profile:{next_callback}")]
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(PayoutProfileState.card_or_phone)
async def payout_bind_card_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    next_callback = data.get("payout_next_callback", "profile")

    ok, value, error = normalize_card_or_phone(message.text or "")
    if not ok:
        await screen_answer(
            message,
            f"❌ {html.escape(error)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Попробовать снова", callback_data=f"payout_bind_card:{next_callback}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"payout_profile:{next_callback}")],
            ]),
            parse_mode="HTML",
        )
        return

    save_payout_card_or_phone(message.from_user.id, value)
    await state.clear()

    await screen_answer(
        message,
        f"""
✅ <b>Карта/номер привязаны</b>

Сохранено: <code>{mask_card_or_phone(value)}</code>

TON можно добавить позже в профиле.
""",
        reply_markup=payout_continue_keyboard(next_callback),
        parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("payout_bind_ton:"))
async def payout_bind_ton_start(call: CallbackQuery, state: FSMContext):
    next_callback = call.data.split(":", 1)[1] if ":" in call.data else "profile"
    await state.set_state(PayoutProfileState.ton_wallet)
    await state.update_data(payout_next_callback=next_callback)

    await show_screen(
        call,
        """
💎 <b>Привязка TON-кошелька</b>

Отправьте TON-адрес одним сообщением.

Подойдут адреса формата:
<code>UQ...</code>, <code>EQ...</code> или raw-адрес.

Карту/номер можно будет добавить позже в профиле.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"payout_profile:{next_callback}")]
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.message(PayoutProfileState.ton_wallet)
async def payout_bind_ton_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    next_callback = data.get("payout_next_callback", "profile")

    ok, value, error = normalize_ton_wallet(message.text or "")
    if not ok:
        await screen_answer(
            message,
            f"❌ {html.escape(error)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Попробовать снова", callback_data=f"payout_bind_ton:{next_callback}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"payout_profile:{next_callback}")],
            ]),
            parse_mode="HTML",
        )
        return

    save_payout_ton_wallet(message.from_user.id, value)
    await state.clear()

    await screen_answer(
        message,
        f"""
✅ <b>TON-кошелёк привязан</b>

Сохранено: <code>{mask_ton_wallet(value)}</code>

Карту/номер можно добавить позже в профиле.
""",
        reply_markup=payout_continue_keyboard(next_callback),
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "create_order_payout_gate_disabled")
async def create_order_payout_gate(call: CallbackQuery, state: FSMContext):
    if not await require_payout_or_ask(call, "создать заказ", "create_order_continue"):
        return
    call.data = "create_order_continue"
    await create_order_start_v7(call, state)


@dp.callback_query(F.data == "create_order_continue")
async def create_order_start_v7(call: CallbackQuery, state: FSMContext):
    """Старт создания заказа после проверки реквизитов."""
    if protect_policy_for_user(call.from_user.id).get("block_create_order"):
        await show_screen(call, protect_block_text(call.from_user.id, "создание заказа"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛡 Подать апелляцию", callback_data="protect_appeal")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="home")],
        ]), parse_mode="HTML")
        await call.answer()
        return

    await state.set_state(CreateOrder.category)
    await show_screen(
        call,
        """
📌 <b>Создание заказа</b>

Шаг 1 из 5: выберите категорию.

Совет: чем точнее категория, тем быстрее исполнители найдут ваш заказ.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            *[[InlineKeyboardButton(text=category, callback_data=f"order_cat:{category}") ] for category in CATEGORIES],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="home")],
        ]),
        parse_mode="HTML",
    )
    await call.answer()


@dp.callback_query(F.data.startswith("apply_order:"))
async def apply_order_payout_gate(call: CallbackQuery, state: FSMContext):
    order_id = int(call.data.split(":")[1])
    if not await require_payout_or_ask(call, "откликнуться на заказ", f"apply_order_continue:{order_id}"):
        return
    call.data = f"apply_order_continue:{order_id}"
    await apply_order_start_v7(call, state)


@dp.callback_query(F.data.startswith("apply_order_continue:"))
async def apply_order_start_v7(call: CallbackQuery, state: FSMContext):
    """Старт отклика после проверки реквизитов."""
    order_id = int(call.data.split(":")[1])

    if protect_policy_for_user(call.from_user.id).get("block_order_application"):
        await show_screen(call, protect_block_text(call.from_user.id, "отклик на заказ"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛡 Подать апелляцию", callback_data="protect_appeal")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"order:{order_id}")],
        ]), parse_mode="HTML")
        await call.answer()
        return

    with db() as conn:
        order = conn.execute("SELECT customer_id, title, budget, status FROM orders WHERE id=?", (order_id,)).fetchone()

    if not order:
        await call.answer("Заказ не найден", show_alert=True)
        return

    customer_id, title, budget, status = order
    if int(customer_id) == int(call.from_user.id):
        await call.answer("Нельзя откликаться на свой заказ", show_alert=True)
        return

    if status != "active":
        await call.answer("Заказ уже не активен", show_alert=True)
        return

    await state.set_state(OrderResponseState.price)
    await state.update_data(order_id=order_id)

    await show_screen(
        call,
        f"""
📨 <b>Отклик на заказ</b>

Заказ: <b>{html.escape(title or 'Без названия')}</b>
Бюджет заказчика: <b>{budget or 0}₽</b>

Шаг 1 из 3: отправьте вашу цену в ₽.
""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к заказу", callback_data=f"order:{order_id}")]
        ]),
        parse_mode="HTML",
    )
    await call.answer()


# Профиль: отдельная кнопка реквизитов.
@dp.callback_query(F.data == "profile_payouts")
async def profile_payouts(call: CallbackQuery):
    await show_screen(
        call,
        payout_profile_text(call.from_user.id),
        reply_markup=payout_profile_keyboard("profile"),
        parse_mode="HTML",
    )
    await call.answer()


# ===== ЗАПУСК =====

import asyncio

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Проверь .env файл")

    init_db()
    ensure_admin_tables()
    ensure_profile_tables()
    ensure_payout_profile_tables()
    cleanup_protected_punishments()

    print("✅ База данных проверена")
    print("🚀 Запускаю LTeam Market...")

    try:
        await setup_bot_commands()
        if WEBAPP_URL.startswith("https://"):
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="Open LTeam Market",
                    web_app=WebAppInfo(url=WEBAPP_URL),
                )
            )
        print("✅ Команды бота установлены")
    except Exception as e:
        print(f"⚠️ Команды бота не установлены: {e}")

    try:
        await dp.start_polling(bot)
    except Exception as e:
        print(f"❌ Ошибка polling: {e}")
        raise
    finally:
        await bot.session.close()



# ===== LTEAM ADMIN CENTER V5 ADDONS =====

def table_exists(table_name: str) -> bool:
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
    return row is not None


async def notify_admins_v5(title: str, body: str, level: str = "info", reply_markup=None):
    """Красивые уведомления staff по уровню важности."""
    level_map = {
        "critical": ("🔴", "CRITICAL"),
        "important": ("🟠", "IMPORTANT"),
        "info": ("⚪", "INFO"),
    }
    emoji, label = level_map.get(level, ("⚪", "INFO"))

    text = f"""
{emoji} <b>LTeam Admin Notice</b> • <code>{label}</code>

<b>{html.escape(title)}</b>

{body}
"""
    targets = OWNER_IDS if level == "critical" else list(dict.fromkeys(OWNER_IDS + ADMIN_IDS))
    for admin_id in targets:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception:
            pass


def revoke_user_plus(user_id: int, actor_id: int) -> None:
    ensure_profile_tables()
    with db() as conn:
        conn.execute("""
            INSERT INTO user_profile_settings (user_id, plus_until, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET plus_until=excluded.plus_until, updated_at=excluded.updated_at
        """, (user_id, datetime.now().isoformat(), datetime.now().isoformat()))
        conn.commit()
    log_admin_action(actor_id, "revoke_lteam_plus", user_id, "plus revoked")


@dp.callback_query(F.data.startswith("admin_grant_plus_days:"))
async def admin_grant_plus_days(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    _, user_id_raw, days_raw = call.data.split(":")
    user_id = int(user_id_raw)
    days = int(days_raw)

    ok, reason = can_act(call.from_user.id, user_id, "grant_plus")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    until = set_user_plus(user_id, days, actor_id=call.from_user.id)

    try:
        until_dt = datetime.fromisoformat(until)
        until_text = until_dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        until_text = str(until)

    try:
        await bot.send_message(
            user_id,
            f"""
💎 <b>LTeam Plus активирован</b>

Срок: <b>{days} дней</b>
До: <code>{html.escape(until_text)}</code>
""",
            parse_mode="HTML",
        )
    except Exception:
        pass

    await notify_admins_v5(
        "Выдан LTeam Plus",
        f"Админ: <code>{call.from_user.id}</code>\nПользователь: <code>{user_id}</code>\nСрок: <b>{days} дней</b>",
        level="info",
    )

    await call.answer(f"💎 Plus выдан на {days} дней", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


@dp.callback_query(F.data.startswith("admin_revoke_plus:"))
async def admin_revoke_plus(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "revoke_plus")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    revoke_user_plus(user_id, call.from_user.id)

    try:
        await bot.send_message(user_id, "💎 Подписка LTeam Plus была снята администрацией.")
    except Exception:
        pass

    await call.answer("Plus снят", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


@dp.callback_query(F.data.startswith("admin_unmute_user:"))
async def admin_unmute_user(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    ok, reason = can_act(call.from_user.id, user_id, "mute")
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    with db() as conn:
        conn.execute("DELETE FROM muted_users WHERE user_id=?", (user_id,))
        conn.commit()

    log_admin_action(call.from_user.id, "unmute_user", user_id, "manual unmute")
    await call.answer("Мут снят", show_alert=True)
    call.data = f"admin_user:{user_id}"
    await admin_user_profile(call)


@dp.callback_query(F.data.startswith("admin_user_warnings:"))
async def admin_user_warnings(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    user_id = int(call.data.split(":")[1])
    with db() as conn:
        rows = conn.execute("""
            SELECT id, admin_id, reason, created_at
            FROM admin_warnings
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT 15
        """, (user_id,)).fetchall()

    text = f"""
━━━━━━━━━━━━━━
⚠️ <b>Предупреждения пользователя</b>
━━━━━━━━━━━━━━

👤 Пользователь: <code>{user_id}</code>

"""
    if not rows:
        text += "Предупреждений нет."
    else:
        for wid, admin_id, reason, created_at in rows:
            text += f"<b>#{wid}</b> от <code>{admin_id}</code> • <code>{html.escape(str(created_at or '')[:16])}</code>\n{html.escape(reason or '—')}\n\n"

    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Назад к пользователю", callback_data=f"admin_user:{user_id}")],
        [InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")],
    ]), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_mutes_list_v5")
async def admin_mutes_list_v5(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    now = datetime.now().isoformat()
    with db() as conn:
        rows = conn.execute("""
            SELECT user_id, muted_until, reason, muted_by, created_at
            FROM muted_users
            WHERE muted_until>?
            ORDER BY muted_until DESC
            LIMIT 15
        """, (now,)).fetchall()

    text = """
━━━━━━━━━━━━━━
🔇 <b>Активные муты</b>
━━━━━━━━━━━━━━

"""
    buttons = []
    if not rows:
        text += "Активных мутов нет."
    else:
        for uid, until, reason, muted_by, created_at in rows:
            text += f"👤 <code>{uid}</code>\nДо: <code>{html.escape(str(until)[:16])}</code>\nКем: <code>{muted_by}</code>\nПричина: {html.escape(reason or '—')}\n\n"
            buttons.append([
                InlineKeyboardButton(text=f"🔊 Снять мут {uid}", callback_data=f"admin_unmute_user:{uid}"),
                InlineKeyboardButton(text="👤 Профиль", callback_data=f"admin_user:{uid}"),
            ])

    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_warnings_list_v5")
async def admin_warnings_list_v5(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    with db() as conn:
        rows = conn.execute("""
            SELECT user_id, COUNT(*) AS cnt, MAX(created_at)
            FROM admin_warnings
            GROUP BY user_id
            ORDER BY cnt DESC, MAX(created_at) DESC
            LIMIT 15
        """).fetchall()

    text = """
━━━━━━━━━━━━━━
⚠️ <b>Пользователи с предупреждениями</b>
━━━━━━━━━━━━━━

"""
    buttons = []
    if not rows:
        text += "Предупреждений нет."
    else:
        for uid, cnt, last_at in rows:
            text += f"👤 <code>{uid}</code> • предупреждений: <b>{cnt}</b> • <code>{html.escape(str(last_at or '')[:16])}</code>\n"
            buttons.append([
                InlineKeyboardButton(text=f"⚠️ История {uid}", callback_data=f"admin_user_warnings:{uid}"),
                InlineKeyboardButton(text="👤 Профиль", callback_data=f"admin_user:{uid}"),
            ])

    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_plus_center_v5")
async def admin_plus_center_v5(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    now = datetime.now().isoformat()
    with db() as conn:
        rows = conn.execute("""
            SELECT user_id, plus_until
            FROM user_profile_settings
            WHERE plus_until>?
            ORDER BY plus_until DESC
            LIMIT 15
        """, (now,)).fetchall() if table_exists("user_profile_settings") else []

    text = """
━━━━━━━━━━━━━━
💎 <b>LTeam Plus Center</b>
━━━━━━━━━━━━━━

Что можно:
• выдать Plus из карточки пользователя;
• снять Plus;
• посмотреть активные подписки;
• найти пользователя по ID/@username.

"""
    buttons = [[InlineKeyboardButton(text="🔎 Найти пользователя", callback_data="admin_find_user")]]

    if not rows:
        text += "\nАктивных Plus-подписок пока нет."
    else:
        text += "\n<b>Активные Plus:</b>\n"
        for uid, until in rows:
            text += f"• <code>{uid}</code> до <code>{html.escape(str(until)[:16])}</code>\n"
            buttons.append([
                InlineKeyboardButton(text=f"👤 {uid}", callback_data=f"admin_user:{uid}"),
                InlineKeyboardButton(text="🧹 Снять", callback_data=f"admin_revoke_plus:{uid}"),
            ])

    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "admin_verified_list_v5")
async def admin_verified_list_v5(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    with db() as conn:
        rows = conn.execute("""
            SELECT user_id, COALESCE(username,''), COALESCE(display_name,'')
            FROM users
            WHERE COALESCE(verified,0)=1
            ORDER BY user_id DESC
            LIMIT 20
        """).fetchall()

    text = """
━━━━━━━━━━━━━━
✅ <b>Verified пользователи</b>
━━━━━━━━━━━━━━

"""
    buttons = [[InlineKeyboardButton(text="🔎 Найти пользователя", callback_data="admin_find_user")]]

    if not rows:
        text += "Verified пользователей пока нет."
    else:
        for uid, username, display_name in rows:
            name = display_name or (f"@{username}" if username else str(uid))
            text += f"• <code>{uid}</code> — {html.escape(name)}\n"
            buttons.append([
                InlineKeyboardButton(text=f"👤 {uid}", callback_data=f"admin_user:{uid}"),
                InlineKeyboardButton(text="❌ Снять", callback_data=f"admin_unverify_user:{uid}"),
            ])

    buttons.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin_panel")])
    await show_screen(call, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data.startswith("admin_bulk_plus:"))
async def admin_bulk_plus_v5(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    # Формат: admin_bulk_plus:123,456,789:30
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer("Неверный формат bulk Plus", show_alert=True)
        return

    user_ids = [int(u) for u in parts[1].split(",") if u.strip().isdigit()]
    days = int(parts[2])
    success = 0
    skipped = 0

    for uid in user_ids:
        ok, _ = can_act(call.from_user.id, uid, "grant_plus")
        if not ok:
            skipped += 1
            continue
        set_user_plus(uid, days, actor_id=call.from_user.id)
        success += 1

    log_admin_action(call.from_user.id, "bulk_grant_plus", None, f"days={days}; success={success}; skipped={skipped}; ids={user_ids[:20]}")
    await call.answer(f"Plus: {success} выдано, {skipped} пропущено", show_alert=True)


@dp.callback_query(F.data.startswith("admin_bulk_mute:"))
async def admin_bulk_mute_v5(call: CallbackQuery):
    if not is_staff(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    # Формат: admin_bulk_mute:123,456,789:10
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer("Неверный формат bulk mute", show_alert=True)
        return

    user_ids = [int(u) for u in parts[1].split(",") if u.strip().isdigit()]
    minutes = int(parts[2])
    success = 0
    skipped = 0

    for uid in user_ids:
        if set_mute(uid, minutes, "Bulk mute by admin", muted_by=call.from_user.id):
            success += 1
        else:
            skipped += 1

    log_admin_action(call.from_user.id, "bulk_mute", None, f"minutes={minutes}; success={success}; skipped={skipped}; ids={user_ids[:20]}")
    await call.answer(f"Мут: {success} выдано, {skipped} пропущено", show_alert=True)


@dp.callback_query(F.data.startswith("admin_bulk_warn:"))
async def admin_bulk_warn_v5(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    # Формат: admin_bulk_warn:123,456,789
    parts = call.data.split(":")
    user_ids = [int(u) for u in parts[1].split(",") if u.strip().isdigit()]
    reason = "Массовое предупреждение администрации LTeam"
    success = 0
    skipped = 0

    for uid in user_ids:
        ok, _ = can_act(call.from_user.id, uid, "warn")
        if not ok:
            skipped += 1
            continue
        with db() as conn:
            conn.execute("""
                INSERT INTO admin_warnings(user_id, admin_id, reason, created_at)
                VALUES (?, ?, ?, ?)
            """, (uid, call.from_user.id, reason, datetime.now().isoformat()))
            conn.commit()
        log_admin_action(call.from_user.id, "warn_user", uid, reason)
        success += 1

    await call.answer(f"Варн: {success} выдано, {skipped} пропущено", show_alert=True)


# ===== ЗАПУСК =====

import asyncio

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Проверь .env файл")

    init_db()
    ensure_admin_tables()
    ensure_profile_tables()
    ensure_payout_profile_tables()
    cleanup_protected_punishments()

    print("✅ База данных проверена")
    print("🚀 Запускаю LTeam Market...")

    try:
        await setup_bot_commands()
        print("✅ Команды бота установлены")
    except Exception as e:
        print(f"⚠️ Команды бота не установлены: {e}")

    try:
        await dp.start_polling(bot)
    except Exception as e:
        print(f"❌ Ошибка polling: {e}")
        raise
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
