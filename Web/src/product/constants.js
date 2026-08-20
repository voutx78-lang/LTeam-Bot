export const FALLBACK_CATEGORIES = [
  "Telegram-боты и Mini Apps",
  "Дизайн Telegram",
  "Монтаж и контент",
  "AI-автоматизация",
  "Тексты для каналов и бизнеса",
];

export const CATEGORY_META = {
  "Telegram-боты и Mini Apps": { short: "Боты и Mini Apps", icon: "grid", className: "bots" },
  "Дизайн Telegram": { short: "Дизайн", icon: "spark", className: "design" },
  "Монтаж и контент": { short: "Монтаж", icon: "image", className: "video" },
  "AI-автоматизация": { short: "AI и автоматизация", icon: "spark", className: "ai" },
  "Тексты для каналов и бизнеса": { short: "Тексты", icon: "file", className: "text" },
};

export const DEAL_STATUS = {
  discussion: ["Обсуждение", "neutral"],
  waiting_final_price: ["Согласование условий", "warning"],
  waiting_buyer_price_confirm: ["Подтвердите условия", "warning"],
  terms_confirmed: ["Условия согласованы", "accent"],
  waiting_admin_payment_approval: ["Проверка оплаты", "warning"],
  in_work: ["В работе", "accent"],
  in_revision: ["На правке", "warning"],
  waiting_buyer_confirm: ["Результат отправлен", "success"],
  completed: ["Завершён", "success"],
  dispute_open: ["Открыт спор", "danger"],
  cancelled: ["Отменён", "muted"],
};
