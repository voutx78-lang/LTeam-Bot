const local = ["localhost", "127.0.0.1"].includes(window.location.hostname);
const previewParams = new URLSearchParams(window.location.search);
const enabled = local && previewParams.has("preview");
if (enabled && previewParams.get("device") === "phone") document.documentElement.dataset.previewPhone = "true";

export const preview = enabled ? {
  route: previewParams.get("preview") || "home",
  me: { authenticated: true, id: 101, name: "Алексей Воронцов", username: "alex_lt", photo_url: "", is_admin: true, role: "both", unread_notifications: 3 },
  config: { beta: true, payments_enabled: false, stars_enabled: true, star_products: [
    { code: "promo_bump", promo_type: "bump", title: "Поднять объявление", description: "Поднять объявление выше в каталоге и свежей выдаче.", stars: 25, days: 0 },
    { code: "promo_highlight", promo_type: "highlight", title: "Выделить на 7 дней", description: "Добавить заметное оформление карточки на семь дней.", stars: 45, days: 7 },
    { code: "promo_top", promo_type: "top", title: "ТОП на 7 дней", description: "Закрепить объявление в приоритетной выдаче на семь дней.", stars: 75, days: 7 },
  ], categories: ["Telegram-боты и Mini Apps", "Дизайн и оформление Telegram", "Монтаж и создание контента", "AI-автоматизация", "Тексты для каналов и бизнеса"] },
  preferences: { role: "both", theme: previewParams.get("theme") || "system", notifications: { messages: true, orders: true, recommendations: true }, display: { animations: true, haptics: true, compact_cards: false, language: "ru", accent: previewParams.get("accent") || "violet" } },
  listings: [
    { id: 101, seller_id: 101, seller_name: "Алексей Воронцов", seller_username: "alex_lt", seller_verified: 1, title: "Создам AI-бота для обработки заявок", category: "Telegram-боты и Mini Apps", price: 12000, delivery_time: "5 дней", avg_rating: 4.9, reviews_count: 8, completed_orders: 11, status: "active" },
    { id: 1, seller_id: 201, seller_name: "Botcraft Studio", seller_username: "botcraft", seller_verified: 1, title: "Разработаю Telegram-бота и Mini App под ключ", category: "Telegram-боты и Mini Apps", price: 18000, delivery_time: "7 дней", avg_rating: 5, reviews_count: 14, completed_orders: 26, is_favorite: true },
    { id: 2, seller_id: 202, seller_name: "Мария Соколова", seller_username: "maria.design", seller_verified: 1, title: "Оформлю Telegram-канал в едином стиле", category: "Дизайн и оформление Telegram", price: 4500, delivery_time: "3 дня", avg_rating: 4.9, reviews_count: 31, completed_orders: 54 },
    { id: 3, seller_id: 203, seller_name: "Frame Lab", seller_username: "framelab", title: "Смонтирую динамичный ролик для Reels", category: "Монтаж и создание контента", price: 2500, delivery_time: "2 дня", avg_rating: 4.6, reviews_count: 8, completed_orders: 17 },
    { id: 4, seller_id: 204, seller_name: "AI Flow", seller_username: "aiflow", title: "Автоматизирую рутину бизнеса с помощью AI", category: "AI-автоматизация", price: 12000, delivery_time: "5 дней", avg_rating: 0, reviews_count: 0, completed_orders: 0 },
  ],
  orders: [
    { id: 11, customer_id: 301, customer_name: "Илья", title: "Нужен бот для записи клиентов", category: "Telegram-боты и Mini Apps", description: "Запись на услуги, напоминания и простая панель администратора.", budget: 30000, deadline: "10 дней", status: "active" },
    { id: 12, customer_id: 302, customer_name: "Анна", title: "Оформить канал онлайн-школы", category: "Дизайн и оформление Telegram", description: "Нужны аватар, шаблоны постов и обложки для рубрик.", budget: 9000, deadline: "5 дней", status: "active" },
  ],
  deals: [],
  notifications: [
    { id: 1, event_type: "message", title: "Новое сообщение", body: "Исполнитель уточнил детали задачи", route: "orders", is_read: 0, created_at: new Date().toISOString() },
  ],
} : null;
