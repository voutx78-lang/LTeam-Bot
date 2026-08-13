"""Product and safety policy shared by the bot and MiniApp API.

The early-access marketplace is deliberately limited to digital work that the
team can reasonably moderate. Money movement is opt-in and disabled by default
until a legal payment provider is connected.
"""

from __future__ import annotations

import os

MARKETPLACE_BETA = os.getenv("MARKETPLACE_BETA", "true").strip().lower() not in {"0", "false", "no"}
PAYMENTS_ENABLED = os.getenv("MARKET_PAYMENTS_ENABLED", "false").strip().lower() in {"1", "true", "yes"}

ALLOWED_CATEGORIES = (
    "Telegram-боты и Mini Apps",
    "Дизайн Telegram",
    "Монтаж и контент",
    "AI-автоматизация",
    "Тексты для каналов и бизнеса",
)

DISALLOWED_MARKET_TERMS = (
    "аккаунт",
    "аккаунты",
    "доступ к аккаунту",
    "продам доступ",
    "взлом",
    "накрутка",
    "кардинг",
    "паспорт",
    "карта",
    "реквизиты",
)


def validate_category(category: str) -> str | None:
    """Return a clear user-facing error when a new listing is out of scope."""
    if category not in ALLOWED_CATEGORIES:
        return "Выберите одно из направлений раннего доступа LTeam Market."
    return None


def validate_market_text(text: str) -> str | None:
    value = (text or "").lower()
    if any(term in value for term in DISALLOWED_MARKET_TERMS):
        return "В раннем доступе запрещены аккаунты, доступы, чужие данные и сомнительные цифровые товары."
    return None
