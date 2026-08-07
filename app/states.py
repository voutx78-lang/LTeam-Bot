"""Состояния диалогов Telegram-бота."""

from aiogram.fsm.state import State, StatesGroup


class CreateListing(StatesGroup):
    category = State()
    title = State()
    item_type = State()
    delivery_time = State()
    price = State()
    payout_details = State()
    description = State()
    cover_photo = State()


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
    reference_photo = State()


class ReceiptState(StatesGroup):
    receipt = State()


class DealFinalPriceState(StatesGroup):
    amount = State()


class SupportState(StatesGroup):
    text = State()
    admin_reply = State()


class AppealState(StatesGroup):
    reason = State()


class ReviewState(StatesGroup):
    rating = State()
    text = State()


class ReportState(StatesGroup):
    reason = State()


class DisputeState(StatesGroup):
    reason = State()


class DealChatState(StatesGroup):
    text = State()


class ListingDiscussionState(StatesGroup):
    message = State()


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


class AdminUserPickState(StatesGroup):
    query = State()


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


class ProfileDescriptionState(StatesGroup):
    text = State()


class PayoutProfileState(StatesGroup):
    card = State()
    ton_wallet = State()


class WithdrawalState(StatesGroup):
    amount = State()
    requisites = State()


class VerificationRequestState(StatesGroup):
    reason = State()
