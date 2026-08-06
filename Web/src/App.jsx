import { useEffect, useMemo, useState } from "react";
import "./App.css";
import "./Form.css";
import "./Polish.css";
import "./Interactions.css";

const tg = window.Telegram?.WebApp;
const API_BASE = import.meta.env.VITE_API_URL || "https://lteam-botminiapp.onrender.com";

async function apiFetch(path) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: tg?.initData ? { "X-Telegram-Init-Data": tg.initData } : {},
  });
  if (!response.ok) throw new Error(`API ${response.status}`);
  return response.json();
}

async function apiRequest(path, method, body) {
  const response = await fetch(`${API_BASE}${path}`, {
    method,
    headers: { "Content-Type": "application/json", ...(tg?.initData ? { "X-Telegram-Init-Data": tg.initData } : {}) },
    body: JSON.stringify(body),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.message || "Не удалось выполнить действие.");
  return data;
}

const demoListings = [
  { id: 14, title: "Telegram-бот под ключ", category: "Разработка", price: 3500, seller: "@northdev", rating: "4.9", orders: 27, accent: "bot" },
  { id: 8, title: "Оформление Telegram-канала", category: "Дизайн", price: 1500, seller: "@mira_design", rating: "5.0", orders: 43, accent: "design" },
  { id: 21, title: "Тексты для карточек и постов", category: "Копирайтинг", price: 700, seller: "@writewave", rating: "4.8", orders: 19, accent: "text" },
];

const dealStages = ["Цена", "Оплата", "Работа", "Проверка", "Выплата"];

function sendToBot(action, data = {}) {
  const payload = { action, ...data };
  if (tg?.sendData) tg.sendData(JSON.stringify(payload));
  else console.info("MiniApp action", payload);
}

function Icon({ name }) {
  const icons = { home: "⌂", search: "⌕", orders: "▣", wallet: "◈", profile: "●", admin: "◉", bell: "◌", plus: "+", arrow: "›", shield: "✦", star: "★", chat: "◌", eye: "◉", filter: "≡", check: "✓" };
  return <span className={`icon icon-${name}`}>{icons[name] ?? "•"}</span>;
}

function ListingCard({ item, onOpen, favorite = false, onFavorite }) {
  return <article className="listing-card" onClick={() => onOpen(item.id)}>
    {onFavorite && <button className={`favorite-button ${favorite ? "saved" : ""}`} onClick={(event) => { event.stopPropagation(); onFavorite(item.id); }} aria-label="В избранное">{favorite ? "♥" : "♡"}</button>}
    <div className={`listing-cover ${item.accent}`}><span>{item.category.slice(0, 1)}</span></div>
    <div className="listing-copy">
      <p className="eyebrow">{item.category}</p>
      <h3>{item.title}</h3>
      <div className="seller"><span className="avatar small">{item.seller.slice(1, 2).toUpperCase()}</span>{item.seller}<span className="rating"><Icon name="star" /> {item.rating}</span></div>
      <div className="listing-footer"><b>{item.price.toLocaleString("ru-RU")} ₽</b><span>{item.orders} заказов <Icon name="arrow" /></span></div>
    </div>
  </article>
}

function DealCard({ deal }) {
  const title = deal?.title || "Сделка LTeam";
  const amount = Number(deal?.amount || 0).toLocaleString("ru-RU");
  const status = deal?.status === "completed" ? "Завершена" : deal?.status === "waiting_buyer_confirm" ? "Проверка" : "В работе";
  return <article className="deal-card">
    <div className="deal-head"><div><p className="eyebrow">Сделка #{deal?.id}</p><h3>{title}</h3></div><span className="status working">{status}</span></div>
    <div className="deal-people"><span>Безопасная сделка LTeam</span></div>
    <div className="timeline">{dealStages.map((stage, index) => <div className={index < 3 ? "done" : index === 3 ? "current" : ""} key={stage}><i>{index < 3 ? "✓" : index + 1}</i><span>{stage}</span></div>)}</div>
    <div className="deal-bottom"><span>К оплате <b>{amount} ₽</b></span><button className="text-button" onClick={() => sendToBot("open_deal", { deal_id: deal?.id })}>Открыть <Icon name="arrow" /></button></div>
  </article>
}

function AdminPanel() {
  return <section className="admin-panel">
    <div className="section-heading"><div><p className="eyebrow">Управление</p><h2>Админ-панель</h2></div><span className="admin-badge">ADMIN</span></div>
    <div className="admin-stats"><div><span>На проверке</span><b>12</b></div><div><span>Споры</span><b>3</b></div><div><span>Выплаты</span><b>7</b></div></div>
    <div className="admin-actions">
      <button onClick={() => sendToBot("admin_open", { section: "payments" })}><Icon name="wallet" /><span>Проверить чеки</span><Icon name="arrow" /></button>
      <button onClick={() => sendToBot("admin_open", { section: "disputes" })}><Icon name="shield" /><span>Открытые споры</span><Icon name="arrow" /></button>
      <button onClick={() => sendToBot("admin_open", { section: "payouts" })}><Icon name="check" /><span>Подтвердить выплаты</span><Icon name="arrow" /></button>
    </div>
  </section>
}

function SettingsSheet({ theme, setTheme, onClose }) {
  const themes = [
    ["light", "Светлая", "Светлый фон и мягкие акценты"],
    ["dark", "Тёмная", "Комфортно вечером"],
    ["midnight", "Неон", "Глубокий фон и яркий акцент"],
  ];
  return <div className="settings-sheet" role="dialog" aria-modal="true">
    <div className="sheet-backdrop" onClick={onClose} />
    <section className="sheet-card">
      <div className="sheet-grab" />
      <div className="sheet-title"><div><p className="eyebrow">Персонализация</p><h2>Настройки</h2></div><button className="round-button" onClick={onClose}>×</button></div>
      <p className="settings-caption">Тема сохраняется только для вашего профиля в LTeam Market.</p>
      <div className="theme-options">{themes.map(([id, title, note]) => <button key={id} className={`theme-option ${theme === id ? "selected" : ""}`} onClick={() => setTheme(id)}><span className={`theme-preview ${id}`}><i /><i /><i /></span><span><b>{title}</b><small>{note}</small></span><em>{theme === id ? "✓" : ""}</em></button>)}</div>
      <div className="settings-block"><button onClick={() => sendToBot("profile_settings")}><Icon name="profile" /><span>Настройки профиля</span><Icon name="arrow" /></button><button onClick={() => sendToBot("support")}><Icon name="chat" /><span>Помощь и поддержка</span><Icon name="arrow" /></button></div>
    </section>
  </div>
}

export default function App() {
  const [tab, setTab] = useState("home");
  const [query, setQuery] = useState("");
  const [activeCategory, setActiveCategory] = useState("Все");
  const [theme, setTheme] = useState(() => localStorage.getItem("lteam-theme") || "light");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [favorites, setFavorites] = useState(() => JSON.parse(localStorage.getItem("lteam-favorites") || "[]"));
  const [toast, setToast] = useState("");
  const [isAdmin, setIsAdmin] = useState(false);
  const [catalogListings, setCatalogListings] = useState([]);
  const [dealItems, setDealItems] = useState([]);
  const [balance, setBalance] = useState({ available: 0, frozen: 0 });
  const [listingForm, setListingForm] = useState({ title: "", category: "Дизайн", price: "", description: "" });
  const [formMessage, setFormMessage] = useState("");
  const [profile, setProfile] = useState({ name: tg?.initDataUnsafe?.user?.first_name || "Гость", username: tg?.initDataUnsafe?.user?.username ? `@${tg.initDataUnsafe.user.username}` : "LTeam user" });

  useEffect(() => {
    tg?.ready();
    tg?.expand();
    tg?.setHeaderColor?.("#f7f8fc");
    tg?.setBackgroundColor?.("#f7f8fc");

    // Роль приходит только с защищённого API. Клиент не имеет права сам выдавать доступ.
    apiFetch("/api/me")
      .then((data) => {
        if (!data) return;
        setIsAdmin(Boolean(data.is_admin));
        setProfile({ name: data.name || "Пользователь", username: data.username ? `@${data.username}` : "LTeam user" });
      })
      .catch(() => {});
    apiFetch("/api/listings").then((items) => setCatalogListings(items.map((item) => ({ ...item, seller: "Исполнитель LTeam", rating: "—", orders: 0, accent: "bot" })))).catch(() => setCatalogListings([]));
    apiFetch("/api/deals").then(setDealItems).catch(() => setDealItems([]));
    apiFetch("/api/balance").then(setBalance).catch(() => {});
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("lteam-theme", theme);
    tg?.setHeaderColor?.(theme === "dark" || theme === "midnight" ? "#151525" : "#f7f8fc");
    tg?.setBackgroundColor?.(theme === "dark" || theme === "midnight" ? "#151525" : "#f7f8fc");
  }, [theme]);

  useEffect(() => {
    localStorage.setItem("lteam-favorites", JSON.stringify(favorites));
  }, [favorites]);

  useEffect(() => {
    if (!toast) return undefined;
    const timer = window.setTimeout(() => setToast(""), 2600);
    return () => window.clearTimeout(timer);
  }, [toast]);

  const listings = useMemo(() => catalogListings.filter((item) => {
    const matchesSearch = `${item.title} ${item.category} ${item.seller}`.toLowerCase().includes(query.toLowerCase());
    const matchesCategory = activeCategory === "Все" || item.category.toLowerCase().includes(activeCategory.toLowerCase());
    return matchesSearch && matchesCategory;
  }), [query, activeCategory, catalogListings]);
  const toggleFavorite = (id) => {
    setFavorites((current) => current.includes(id) ? current.filter((value) => value !== id) : [...current, id]);
    setToast(favorites.includes(id) ? "Удалено из избранного" : "Добавлено в избранное");
  };
  const navItems = [
    ["home", "home", "Главная"], ["catalog", "search", "Каталог"], ["orders", "orders", "Заказы"], ["wallet", "wallet", "Баланс"], ["profile", "profile", "Профиль"],
  ];

  return <main className="app-shell">
    <header className="topbar"><button className="brand" onClick={() => setTab("home")}><span className="brand-mark">L</span><span>LTeam <b>Market</b></span></button><button className="round-button" onClick={() => sendToBot("open_notifications")} aria-label="Уведомления"><Icon name="bell" /></button></header>

    <button className="floating-settings" onClick={() => setSettingsOpen(true)} aria-label="Настройки">◐</button>
    {tab === "home" && <>
      <section className="hero-card"><div className="hero-orb one" /><div className="hero-orb two" /><p className="eyebrow">Безопасные сделки в Telegram</p><h1>Находите исполнителей.<br /><em>Работайте спокойно.</em></h1><p className="hero-copy">Оплата проходит через гаранта LTeam, а деньги исполнитель получает после вашего подтверждения.</p><div className="hero-buttons"><button className="primary" onClick={() => setTab("catalog")}>Смотреть каталог <Icon name="arrow" /></button><button className="ghost" onClick={() => setTab("create")}><Icon name="plus" /> Разместить</button></div></section>
      <section className="trust-row"><div><Icon name="shield" /><span><b>Гарант-сделки</b><small>Оплата через администратора</small></span></div><div><Icon name="check" /><span><b>Проверенные отзывы</b><small>Только после заказа</small></span></div></section>
      <section><div className="section-heading"><div><p className="eyebrow">Популярное</p><h2>Услуги для старта</h2></div><button className="text-button" onClick={() => setTab("catalog")}>Все <Icon name="arrow" /></button></div><div className="listing-grid">{catalogListings.slice(0, 2).map((item) => <ListingCard key={item.id} item={item} onOpen={(listing_id) => sendToBot("open_listing", { listing_id })} />)}</div></section>
      <section className="quick-grid"><button onClick={() => { setTab("orders"); sendToBot("open_orders"); }}><span className="quick-icon purple">⌁</span><b>Найти исполнителя</b><small>Создать заказ</small></button><button onClick={() => sendToBot("open_guarantee")}><span className="quick-icon mint">✦</span><b>Как работает гарант</b><small>6 простых шагов</small></button></section>
    </>}

    {tab === "catalog" && <section><div className="page-title"><div><p className="eyebrow">Маркетплейс</p><h1>Каталог услуг</h1></div><button className="round-button" onClick={() => setQuery("")}><Icon name="filter" /></button></div><label className="search-box"><Icon name="search" /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Что вы ищете?" /></label><div className="chips">{["Все", "Дизайн", "Разработка", "Тексты", "Монтаж"].map((category) => <button className={activeCategory === category ? "selected" : ""} onClick={() => setActiveCategory(category)} key={category}>{category}</button>)}</div>{listings.length ? <div className="listing-grid">{listings.map((item) => <ListingCard key={item.id} item={item} favorite={favorites.includes(item.id)} onFavorite={toggleFavorite} onOpen={(listing_id) => sendToBot("open_listing", { listing_id })} />)}</div> : <div className="empty-note"><Icon name="search" /><div><b>Ничего не найдено</b><span>Попробуйте изменить запрос или выберите другую категорию.</span></div></div>}</section>}

    {tab === "orders" && <section><div className="page-title"><div><p className="eyebrow">Ваши задачи</p><h1>Заказы и сделки</h1></div><button className="round-button" onClick={() => sendToBot("create_order")}><Icon name="plus" /></button></div><button className="create-order" onClick={() => sendToBot("create_order")}><span><Icon name="plus" /></span><div><b>Создать заказ</b><small>Исполнители откликнутся сами</small></div><Icon name="arrow" /></button>{dealItems.length ? dealItems.map((deal) => <DealCard key={deal.id} deal={deal} />) : <div className="empty-note"><Icon name="chat" /><div><b>Сделок пока нет</b><span>Создайте заказ или выберите услугу в каталоге.</span></div></div>}<div className="empty-note"><Icon name="chat" /><div><b>Все обсуждения внутри сделки</b><span>Так гарант сможет помочь при споре.</span></div></div></section>}

    {tab === "wallet" && <section><div className="page-title"><div><p className="eyebrow">Финансы</p><h1>Баланс</h1></div><button className="round-button" onClick={() => sendToBot("balance_history")}><Icon name="orders" /></button></div><div className="balance-card"><p>Доступно к выводу</p><h2>{Number(balance.available || 0).toLocaleString("ru-RU")} ₽</h2><span>В обработке у гаранта: {Number(balance.frozen || 0).toLocaleString("ru-RU")} ₽</span><button className="primary" onClick={() => sendToBot("withdraw_start")}>Вывести средства <Icon name="arrow" /></button></div><div className="list-row"><span className="list-icon mint"><Icon name="wallet" /></span><div><b>История операций</b><small>Пополнения, сделки и выплаты</small></div><Icon name="arrow" /></div></section>}

    {tab === "profile" && <section><div className="profile-card"><span className="avatar large">{profile.name.slice(0, 1).toUpperCase()}</span><div><p className="eyebrow">Профиль LTeam</p><h1>{profile.name}</h1><span>{profile.username}</span></div><button className="round-button" onClick={() => sendToBot("profile_settings")}>⚙</button></div><div className="profile-stats"><div><b>0</b><span>Заказов</span></div><div><b>0</b><span>Продаж</span></div><div><b>—</b><span>Рейтинг</span></div></div><div className="settings-list"><button onClick={() => sendToBot("my_listings")}><Icon name="orders" /><span>Мои объявления</span><Icon name="arrow" /></button><button onClick={() => sendToBot("favorites")}><Icon name="star" /><span>Избранное</span><Icon name="arrow" /></button><button onClick={() => sendToBot("support")}><Icon name="chat" /><span>Поддержка</span><Icon name="arrow" /></button></div>{isAdmin && <AdminPanel />}</section>}

    {tab === "create" && <section><div className="page-title"><div><p className="eyebrow">Новая услуга</p><h1>Разместить объявление</h1></div></div><form className="listing-form" onSubmit={async (event) => { event.preventDefault(); setFormMessage(""); try { await apiRequest("/api/listings", "POST", listingForm); setFormMessage("Объявление отправлено на проверку."); setListingForm({ title: "", category: "Дизайн", price: "", description: "" }); } catch (error) { setFormMessage(error.message); } }}><label>Название<input required value={listingForm.title} onChange={(event) => setListingForm({ ...listingForm, title: event.target.value })} placeholder="Например, дизайн Telegram-канала" /></label><label>Категория<select value={listingForm.category} onChange={(event) => setListingForm({ ...listingForm, category: event.target.value })}><option>Дизайн</option><option>Разработка</option><option>Тексты</option><option>Монтаж</option><option>Другое</option></select></label><label>Цена, ₽<input required inputMode="numeric" value={listingForm.price} onChange={(event) => setListingForm({ ...listingForm, price: event.target.value })} placeholder="1500" /></label><label>Описание<textarea required value={listingForm.description} onChange={(event) => setListingForm({ ...listingForm, description: event.target.value })} placeholder="Расскажите, что получит покупатель" /></label><button className="primary" type="submit">Отправить на проверку <Icon name="arrow" /></button>{formMessage && <p className="form-message">{formMessage}</p>}</form></section>}

    <nav className="bottom-nav">{navItems.map(([id, icon, label]) => <button className={tab === id ? "active" : ""} key={id} onClick={() => setTab(id)}><Icon name={icon} /><span>{label}</span></button>)}</nav>
    {settingsOpen && <SettingsSheet theme={theme} setTheme={setTheme} onClose={() => setSettingsOpen(false)} />}
    {toast && <div className="toast"><span>✓</span>{toast}</div>}
  </main>;
}
