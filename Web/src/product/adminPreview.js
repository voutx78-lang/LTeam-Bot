const now = new Date();
const ago = (minutes) => new Date(now.getTime() - minutes * 60_000).toISOString();

const summary = {
  users: 1284,
  active_listings: 186,
  active_orders: 43,
  completed: 627,
  moderation: 7,
  tickets: 4,
  disputes: 2,
  reports: 3,
  star_pending: 2,
  star_paid: 1840,
  runtime_errors: 1,
};

const analytics = {
  totals: { users: 1284, listings: 342, orders: 711, deals: 684, completed: 627, reviews: 518, active_orders: 43, open_disputes: 2, open_tickets: 4, paid_stars: 1840, completion_rate: 91.7, dispute_rate: 0.3 },
  days: Array.from({ length: 7 }, (_, index) => ({
    day: new Date(now.getTime() - (6 - index) * 86_400_000).toISOString().slice(0, 10),
    users: [8, 13, 11, 18, 16, 25, 21][index],
    listings: [3, 5, 4, 7, 6, 9, 8][index],
    orders: [5, 6, 8, 10, 7, 13, 12][index],
    deals: [2, 4, 5, 6, 5, 9, 8][index],
  })),
};

const moderation = [
  { id: 201, item_type: "listing", author_id: 2201, author_name: "Botcraft Studio", title: "Настрою AI-ассистента для отдела продаж", category: "AI-автоматизация", description: "Подключу заявки, подготовлю сценарии, базу знаний и аналитику. Передам инструкцию команде и две недели помогу после запуска.", amount: 24000, status: "moderation", created_at: ago(18) },
  { id: 202, item_type: "order", author_id: 2202, author_name: "Анна К.", title: "Нужен дизайн Telegram-канала", category: "Дизайн и оформление Telegram", description: "Нужны аватар, обложки для пяти рубрик и шаблоны постов. Референсы и брендбук приложены к задаче.", amount: 9000, status: "moderation", created_at: ago(46) },
];

const tickets = [
  { id: 31, title: "Обращение в поддержку", author_id: 2311, author_name: "Илья Морозов", status: "open", note: "Исполнитель отправил новую версию, но кнопка принятия результата не появилась. Помогите проверить заказ.", created_at: ago(12) },
  { id: 30, title: "Обращение в поддержку", author_id: 2310, author_name: "Design Lab", status: "answered", note: "Хочу изменить название активной услуги после модерации.", created_at: ago(92) },
];

const disputes = [
  { id: 14, deal_id: 804, title: "Разработка Mini App для школы", author_id: 2401, author_name: "Мария Соколова", buyer_id: 2401, buyer_name: "Мария Соколова", seller_id: 2402, seller_name: "DevFlow", amount: 36000, status: "dispute_open", note: "Исполнитель задержал финальную версию и не передал инструкцию по запуску.", created_at: ago(65) },
];

const reports = [
  { id: 51, title: "Жалоба пользователя", author_id: 2501, author_name: "Алексей", target_type: "listing", target_id: 490, target_title: "Продам готовый аккаунт", status: "new", note: "Объявление похоже на продажу аккаунта и нарушает правила площадки.", created_at: ago(24) },
];

const users = [
  { user_id: 101, display_name: "Алексей Воронцов", username: "alex_lt", verified: 1, is_admin: true, banned: 0, completed_count: 38, warnings_count: 0, created_at: ago(80_000) },
  { user_id: 2201, display_name: "Botcraft Studio", username: "botcraft", verified: 1, is_admin: false, banned: 0, completed_count: 26, warnings_count: 0, created_at: ago(60_000) },
  { user_id: 2207, display_name: "Quick Media", username: "quickmedia", verified: 0, is_admin: false, banned: 0, completed_count: 4, warnings_count: 1, created_at: ago(12_000) },
  { user_id: 2214, display_name: "Test User", username: "test_user", verified: 0, is_admin: false, banned: 1, completed_count: 0, warnings_count: 2, created_at: ago(5_000) },
];

const audit = [
  { id: 1, actor_id: 101, actor_name: "Алексей Воронцов", target_id: 2208, target_name: "Мария", action: "moderation_approve_listing", details: "#198 Оформлю Telegram-канал", created_at: ago(8) },
  { id: 2, actor_id: 101, actor_name: "Алексей Воронцов", target_id: 2214, target_name: "Test User", action: "miniapp_warn", details: "Повторная публикация дубликатов", created_at: ago(31) },
  { id: 3, actor_id: 101, actor_name: "Алексей Воронцов", target_id: 804, target_name: "Сделка #804", action: "ticket_reply", details: "Пользователю отправлена инструкция", created_at: ago(74) },
];

const finance = {
  can_refund: true,
  totals: { operations: 57, paid_stars: 1840, refunded_stars: 75, pending: 2 },
  items: [
    { id: 91, user_id: 2201, user_name: "Botcraft Studio", listing_id: 201, listing_title: "AI-ассистент для отдела продаж", product_code: "promo_top", stars: 75, currency: "XTR", status: "paid", created_at: ago(120), paid_at: ago(118) },
    { id: 90, user_id: 2207, user_name: "Quick Media", listing_id: 175, listing_title: "Монтаж Reels", product_code: "promo_highlight", stars: 45, currency: "XTR", status: "pending", created_at: ago(180) },
    { id: 89, user_id: 2210, user_name: "Text Maker", listing_id: 168, listing_title: "Тексты для Telegram", product_code: "promo_bump", stars: 25, currency: "XTR", status: "refunded", created_at: ago(1440), refunded_at: ago(900) },
  ],
};

const system = {
  health: { ok: true, product: "LT Market", version: "2026.08-admin-console", payments_enabled: false, stars_enabled: true, storage: "cloud_snapshot" },
  errors: [{ reference: "D0D218", created_at: ago(38), error_type: "TelegramBadRequest", message: "Message is not modified", kind: "callback", user_id: 2214, command: "/start", traceback: "TelegramBadRequest: Message is not modified\n  at update_handler(...)" }],
};

export function adminPreview(path) {
  if (path === "/api/admin/summary") return summary;
  if (path === "/api/admin/analytics") return analytics;
  if (path === "/api/admin/moderation") return moderation;
  if (path.startsWith("/api/admin/queues/tickets")) return tickets;
  if (path.startsWith("/api/admin/queues/disputes")) return disputes;
  if (path.startsWith("/api/admin/queues/reports")) return reports;
  if (path.startsWith("/api/admin/users/")) {
    const id = Number(path.split("/").pop());
    const user = users.find((item) => item.user_id === id) || users[1];
    return { ...user, avatar_url: "", bio: "Разрабатываю Telegram-ботов и Mini Apps для бизнеса.", market_role: "both", listings_count: 5, orders_count: 2, deals_count: 31, reports_count: user.warnings_count, warnings: user.warnings_count ? [{ id: 1, reason: "Дубликат объявления", created_at: ago(1500) }] : [] };
  }
  if (path.startsWith("/api/admin/users")) return users;
  if (path === "/api/admin/audit") return audit;
  if (path === "/api/admin/payments/stars") return finance;
  if (path === "/api/health") return system.health;
  if (path === "/api/admin/runtime-errors") return { errors: system.errors };
  if (path.startsWith("/api/admin/disputes/")) return { dispute: { ...disputes[0], deal_status: "dispute_open", reason: disputes[0].note }, messages: [{ id: 1, sender_id: 2401, sender_name: "Мария Соколова", text: "Инструкция и исходники не переданы.", created_at: ago(190) }, { id: 2, sender_id: 2402, sender_name: "DevFlow", text: "Готовлю архив и документацию.", created_at: ago(175) }], deliveries: [{ id: 1, sender_id: 2402, version: 1, comment: "Первая версия Mini App", created_at: ago(360) }] };
  return {};
}

export const isAdminPreview = () => ["localhost", "127.0.0.1"].includes(window.location.hostname) && new URLSearchParams(window.location.search).get("preview") === "admin";
