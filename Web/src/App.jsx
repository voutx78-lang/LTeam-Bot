import { useEffect, useMemo, useState } from "react";
import "./App.css";
import "./Form.css";
import "./Polish.css";
import "./Interactions.css";
import "./Experience.css";
import "./Status.css";
import "./Chat.css";
import "./Brand.css";
import "./Home.css";
import "./HomeRefine.css";
import "./Guide.css";
import "./Catalog.css";
import "./CatalogPlatform.css";
import "./CatalogV3.css";
import "./CatalogFix.css";
import "./CatalogPolish.css";
import "./Marketplace.css";
import "./ListingComposer.css";
import "./OrdersWorkspace.css";
import "./MarketplaceFix.css";
import "./SellerProfile.css";
import CatalogV3 from "./CatalogV3";
import OrdersWorkspace from "./OrdersWorkspace";

const tg = window.Telegram?.WebApp;
const API_BASE = import.meta.env.VITE_API_URL || "https://lteam-botminiapp.onrender.com";

function telegramApp() {
  return window.Telegram?.WebApp;
}

async function apiFetch(path) {
  const initData = telegramApp()?.initData || "";
  const response = await fetch(`${API_BASE}${path}`, {
    headers: initData ? { "X-Telegram-Init-Data": initData } : {},
  });
  if (!response.ok) throw new Error(`API ${response.status}`);
  return response.json();
}

async function apiRequest(path, method, body) {
  const initData = telegramApp()?.initData || "";
  const response = await fetch(`${API_BASE}${path}`, {
    method,
    headers: { "Content-Type": "application/json", ...(initData ? { "X-Telegram-Init-Data": initData } : {}) },
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
  if (telegramApp()?.sendData) telegramApp().sendData(JSON.stringify(payload));
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

function AdminPanel({ summary = {} }) {
  return <section className="admin-panel">
    <div className="section-heading"><div><p className="eyebrow">Управление</p><h2>Админ-панель</h2></div><span className="admin-badge">ADMIN</span></div>
    <div className="admin-stats"><div><span>На проверке</span><b>{summary.payments ?? 0}</b></div><div><span>Споры</span><b>{summary.disputes ?? 0}</b></div><div><span>Выплаты</span><b>{summary.payouts ?? 0}</b></div></div>
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

function WorkspaceCard({ profile, dealsCount, onNavigate }) {
  const hour = new Date().getHours();
  const greeting = hour < 12 ? "Доброе утро" : hour < 18 ? "Добрый день" : "Добрый вечер";
  return <section className="workspace-card">
    <div className="workspace-glow" />
    <div className="workspace-copy"><p>{greeting}, {profile.name || "пользователь"}</p><h2>Ваш рабочий стол</h2><span>{dealsCount ? `Активных сделок: ${dealsCount}` : "Создайте заказ или разместите первую услугу"}</span></div>
    <div className="workspace-actions"><button onClick={() => onNavigate("create")}><b>＋</b><span>Разместить</span></button><button onClick={() => onNavigate("orders")}><b>↗</b><span>Заказы</span></button><button onClick={() => onNavigate("wallet")}><b>₽</b><span>Баланс</span></button></div>
  </section>
}

function HomeScreen({ profile, dealsCount, onNavigate }) {
  return <section className="home-screen">
    <div className="home-hero"><div className="home-grid" /><div className="home-signal"><span /><span /><span /></div><p className="eyebrow">LTEAM MARKET · БЕЗОПАСНЫЕ СДЕЛКИ</p><h1>Работа в Telegram,<br /><em>как на полноценной платформе.</em></h1><p>Находите исполнителей, публикуйте задачи и ведите сделки через гаранта LTeam.</p><div className="home-hero-actions"><button className="primary" onClick={() => onNavigate("catalog")}>Найти исполнителя <span>→</span></button><button className="secondary-action" onClick={() => onNavigate("create-order")}>Создать заказ</button></div></div>
    <div className="focus-card"><div><p className="eyebrow">ВАШ ЦЕНТР УПРАВЛЕНИЯ</p><h2>{dealsCount ? `Активных сделок: ${dealsCount}` : `Добро пожаловать, ${profile.name || "пользователь"}`}</h2><span>{dealsCount ? "Откройте сделки, чтобы проверить статусы и переписку." : "Начните с каталога или опубликуйте первую задачу."}</span></div><button onClick={() => onNavigate(dealsCount ? "orders" : "catalog")} aria-label="Открыть">→</button></div>
    <section className="how-it-works"><div className="section-heading"><div><p className="eyebrow">КАК ЭТО РАБОТАЕТ</p><h2>Три шага до результата</h2></div></div><div className="guide-steps"><article><b>01</b><div><h3>Выберите</h3><p>Услугу из каталога или создайте свой заказ.</p></div></article><article><b>02</b><div><h3>Обсудите</h3><p>Все условия и сообщения хранятся внутри сделки.</p></div></article><article><b>03</b><div><h3>Подтвердите</h3><p>Гарант помогает провести оплату безопасно.</p></div></article></div></section>
    <button className="guide-link" onClick={() => onNavigate("guide")}><span className="guide-mark">?</span><span><b>Короткий гид по гаранту</b><small>Понятно о сделках, оплате и защите</small></span><i>→</i></button>
  </section>
}

function GuidePage({ onClose, onOrders }) {
  return <section className="guide-page"><div className="page-title"><div><p className="eyebrow">БЕЗОПАСНАЯ СДЕЛКА</p><h1>Гарант LTeam</h1></div><button className="round-button" onClick={onClose}>×</button></div><p className="guide-intro">Деньги не передаются исполнителю, пока вы не подтвердите результат. Администратор помогает провести оплату и выплату.</p><div className="guide-flow"><article><b>1</b><div><h3>Согласуйте условия</h3><p>Обсудите задачу, цену и срок в чате сделки.</p></div></article><article><b>2</b><div><h3>Оплатите по реквизитам LTeam</h3><p>Реквизиты выдаются только после проверки сделки.</p></div></article><article><b>3</b><div><h3>Получите результат</h3><p>Проверьте работу и подтвердите выполнение.</p></div></article><article><b>4</b><div><h3>Выплата исполнителю</h3><p>После подтверждения администратор переводит деньги исполнителю.</p></div></article></div><button className="primary guide-primary" onClick={onOrders}>Перейти к заказам <span>→</span></button></section>
}

function CatalogOverlay({ items, favorites, onFavorite, onNavigate }) {
  const [search, setSearch] = useState("");
  const [category, setCategory] = useState("Все");
  const [sort, setSort] = useState("new");
  const [selected, setSelected] = useState(null);
  const categories = ["Все", "Дизайн", "Разработка", "Тексты", "Монтаж", "Другое"];
  const visible = useMemo(() => items.filter((item) => `${item.title} ${item.category} ${item.description || ""}`.toLowerCase().includes(search.toLowerCase()) && (category === "Все" || item.category === category)).sort((a, b) => sort === "price-low" ? Number(a.price) - Number(b.price) : sort === "price-high" ? Number(b.price) - Number(a.price) : Number(b.id) - Number(a.id)), [items, search, category, sort]);
  return <section className="catalog-overlay"><header className="catalog-top"><button className="catalog-brand" onClick={() => onNavigate("home")}><span className="catalog-brand-mark">L</span><span>Каталог</span></button><button className="catalog-order" onClick={() => onNavigate("create-order")}>Создать заказ</button></header><div className="catalog-heading"><p>УСЛУГИ И ЦИФРОВЫЕ ТОВАРЫ</p><h1>Найдите нужного<br />исполнителя.</h1></div><label className="catalog-search"><span>⌕</span><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Поиск по услугам" />{search && <button onClick={() => setSearch("")}>×</button>}</label><div className="catalog-toolbar"><div className="catalog-categories">{categories.map((item) => <button className={category === item ? "active" : ""} key={item} onClick={() => setCategory(item)}>{item}</button>)}</div><select value={sort} onChange={(event) => setSort(event.target.value)} aria-label="Сортировка"><option value="new">Сначала новые</option><option value="price-low">Сначала дешевле</option><option value="price-high">Сначала дороже</option></select></div><div className="catalog-result"><span>{visible.length} {visible.length === 1 ? "предложение" : "предложений"}</span><button onClick={() => { setSearch(""); setCategory("Все"); setSort("new"); }}>Сбросить</button></div><div className="catalog-list">{visible.length ? visible.map((item) => <article className="market-card" key={item.id} onClick={() => setSelected(item)}><div className="market-card-accent"><span>{String(item.category || "L").slice(0, 1)}</span></div><div className="market-card-copy"><p>{item.category || "Услуга"}</p><h3>{item.title}</h3><small>{item.delivery_time || "Срок обсуждается с исполнителем"}</small><b>от {Number(item.price || 0).toLocaleString("ru-RU")} ₽</b></div><button className={favorites.includes(item.id) ? "market-save saved" : "market-save"} onClick={(event) => { event.stopPropagation(); onFavorite(item.id); }} aria-label="В избранное">{favorites.includes(item.id) ? "♥" : "♡"}</button></article>) : <div className="catalog-empty"><b>Ничего не найдено</b><span>Попробуйте другой запрос или категорию.</span><button onClick={() => { setSearch(""); setCategory("Все"); }}>Показать все услуги</button></div>}</div><nav className="catalog-dock"><button onClick={() => onNavigate("home")}><span>⌂</span>Главная</button><button className="active" onClick={() => {}}><span>⌕</span>Каталог</button><button onClick={() => onNavigate("orders")}><span>▣</span>Заказы</button><button onClick={() => onNavigate("profile")}><span>◉</span>Профиль</button></nav>{selected && <div className="listing-sheet"><div className="sheet-backdrop" onClick={() => setSelected(null)} /><section><button className="sheet-close" onClick={() => setSelected(null)}>×</button><p className="eyebrow">{selected.category || "Услуга"}</p><h2>{selected.title}</h2><p className="listing-description">{selected.description || "Описание будет уточнено исполнителем в переписке."}</p><div className="listing-details"><span>Стоимость <b>от {Number(selected.price || 0).toLocaleString("ru-RU")} ₽</b></span><span>Срок <b>{selected.delivery_time || "По договорённости"}</b></span></div><button className="primary listing-cta" onClick={() => { setSelected(null); onNavigate("create-order"); }}>Создать заказ по услуге <span>→</span></button><button className="sheet-favorite" onClick={() => onFavorite(selected.id)}>{favorites.includes(selected.id) ? "Убрать из избранного" : "Сохранить в избранное"}</button></section></div>}</section>
}

function CatalogPlatform({ items, orders, favorites, onFavorite, onNavigate }) {
  const [mode, setMode] = useState("services");
  const [search, setSearch] = useState("");
  const [category, setCategory] = useState("Все");
  const [sort, setSort] = useState("new");
  const [selected, setSelected] = useState(null);
  const [application, setApplication] = useState({ price: "", deadline: "", comment: "" });
  const [notice, setNotice] = useState("");
  const source = mode === "services" ? items : orders.filter((order) => ["active", "open", "approved"].includes(order.status));
  const visible = useMemo(() => source.filter((item) => `${item.title} ${item.category} ${item.description || ""}`.toLowerCase().includes(search.toLowerCase()) && (category === "Все" || item.category === category)).sort((a, b) => sort === "low" ? Number(a.price ?? a.budget) - Number(b.price ?? b.budget) : sort === "high" ? Number(b.price ?? b.budget) - Number(a.price ?? a.budget) : Number(b.id) - Number(a.id)), [source, search, category, sort]);
  const submitApplication = async (event) => { event.preventDefault(); try { await apiRequest(`/api/orders/${selected.id}/applications`, "POST", application); setNotice("Отклик отправлен заказчику."); setSelected(null); setApplication({ price: "", deadline: "", comment: "" }); } catch (error) { setNotice(error.message); } };
  return <section className="catalog-overlay platform-catalog"><header className="catalog-top"><button className="catalog-brand" onClick={() => onNavigate("home")}><span className="catalog-brand-mark">L</span><span>Маркет</span></button><button className="catalog-order" onClick={() => onNavigate("create-order")}>Создать заказ</button></header><div className="catalog-heading"><p>LTEAM MARKETPLACE</p><h1>{mode === "services" ? "Предлагают услуги" : "Ищут исполнителя"}</h1></div><div className="market-switch"><button className={mode === "services" ? "active" : ""} onClick={() => { setMode("services"); setSelected(null); }}>Услуги</button><button className={mode === "orders" ? "active" : ""} onClick={() => { setMode("orders"); setSelected(null); }}>Заказы</button></div><label className="catalog-search"><span>⌕</span><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={mode === "services" ? "Найти услугу" : "Найти заказ"} />{search && <button onClick={() => setSearch("")}>×</button>}</label><div className="market-filter-row"><select value={category} onChange={(event) => setCategory(event.target.value)}>{["Все", "Дизайн", "Разработка", "Тексты", "Монтаж", "Другое"].map((item) => <option key={item}>{item}</option>)}</select><select value={sort} onChange={(event) => setSort(event.target.value)}><option value="new">Сначала новые</option><option value="low">Сначала дешевле</option><option value="high">Сначала дороже</option></select></div>{notice && <div className="market-notice">{notice}<button onClick={() => setNotice("")}>×</button></div>}<div className="catalog-result"><span>{visible.length} {mode === "services" ? "услуг" : "заказов"}</span><button onClick={() => { setSearch(""); setCategory("Все"); setSort("new"); }}>Сбросить</button></div><div className="catalog-list">{visible.length ? visible.map((item) => mode === "services" ? <article className="market-card" key={item.id} onClick={() => setSelected(item)}><div className="market-card-accent"><span>{String(item.category || "L").slice(0, 1)}</span></div><div className="market-card-copy"><p>{item.category || "Услуга"}</p><h3>{item.title}</h3><small>{item.delivery_time || "Срок обсуждается с исполнителем"}</small><b>от {Number(item.price || 0).toLocaleString("ru-RU")} ₽</b></div><button className={favorites.includes(item.id) ? "market-save saved" : "market-save"} onClick={(event) => { event.stopPropagation(); onFavorite(item.id); }}>{favorites.includes(item.id) ? "♥" : "♡"}</button></article> : <article className="order-market-card" key={item.id} onClick={() => setSelected(item)}><div className="order-market-top"><span>{item.category || "Заказ"}</span><b>до {Number(item.budget || 0).toLocaleString("ru-RU")} ₽</b></div><h3>{item.title}</h3><p>{item.description || "Подробности — в карточке заказа"}</p><footer><small>{item.deadline || "Срок обсуждается"}</small><button onClick={(event) => { event.stopPropagation(); setSelected(item); }}>Откликнуться</button></footer></article>) : <div className="catalog-empty"><b>Ничего не найдено</b><span>Попробуйте изменить поиск или категорию.</span><button onClick={() => { setSearch(""); setCategory("Все"); }}>Показать всё</button></div>}</div><nav className="catalog-dock"><button onClick={() => onNavigate("home")}><span>⌂</span>Главная</button><button className="active"><span>⌕</span>Маркет</button><button onClick={() => onNavigate("orders")}><span>▣</span>Мои</button><button onClick={() => onNavigate("profile")}><span>◉</span>Профиль</button></nav>{selected && (mode === "services" ? <div className="listing-sheet"><div className="sheet-backdrop" onClick={() => setSelected(null)} /><section><button className="sheet-close" onClick={() => setSelected(null)}>×</button><p className="eyebrow">{selected.category || "Услуга"}</p><h2>{selected.title}</h2><p className="listing-description">{selected.description || "Описание будет уточнено исполнителем в переписке."}</p><div className="listing-details"><span>Стоимость <b>от {Number(selected.price || 0).toLocaleString("ru-RU")} ₽</b></span><span>Срок <b>{selected.delivery_time || "По договорённости"}</b></span></div><button className="primary listing-cta" onClick={() => { setSelected(null); onNavigate("create-order"); }}>Создать заказ по услуге <span>→</span></button><button className="sheet-favorite" onClick={() => onFavorite(selected.id)}>{favorites.includes(selected.id) ? "Убрать из избранного" : "Сохранить в избранное"}</button></section></div> : <div className="listing-sheet"><div className="sheet-backdrop" onClick={() => setSelected(null)} /><section><button className="sheet-close" onClick={() => setSelected(null)}>×</button><p className="eyebrow">ЗАКАЗ · {selected.category || "Другое"}</p><h2>{selected.title}</h2><p className="listing-description">{selected.description || ""}</p><form className="application-form" onSubmit={submitApplication}><div className="form-split"><label>Ваша цена<input required inputMode="numeric" value={application.price} onChange={(event) => setApplication({ ...application, price: event.target.value })} placeholder={selected.budget || "0"} /></label><label>Срок<input required value={application.deadline} onChange={(event) => setApplication({ ...application, deadline: event.target.value })} placeholder="Например, 3 дня" /></label></div><label>Комментарий<textarea required value={application.comment} onChange={(event) => setApplication({ ...application, comment: event.target.value })} placeholder="Коротко расскажите, как выполните задачу" /></label><button className="primary listing-cta" type="submit">Отправить отклик <span>→</span></button></form></section></div>)}</section>
}

function OrderCard({ order, onOpen }) {
  const budget = Number(order.budget || 0).toLocaleString("ru-RU");
  return <article className="order-card" onClick={() => onOpen(order)}><div className="order-card-top"><span className="order-icon">◈</span><span className="status working">{order.status || "active"}</span></div><p className="eyebrow">{order.category || "Заказ"}</p><h3>{order.title}</h3><p className="order-description">{order.description || "Описание заказа"}</p><div className="order-card-bottom"><b>до {budget} ₽</b><span>Открыть чат ›</span></div></article>
}

function ChatSheet({ chat, currentUserId, onClose }) {
  const [messages, setMessages] = useState([]);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const basePath = chat.kind === "deal" ? `/api/deals/${chat.item.id}/messages` : `/api/orders/${chat.item.id}/messages`;
  const loadMessages = () => apiFetch(basePath).then(setMessages).catch(() => setMessages([]));
  useEffect(() => { loadMessages(); const timer = window.setInterval(loadMessages, 7000); return () => window.clearInterval(timer); }, [chat.item.id, chat.kind]);
  const submit = async (event) => { event.preventDefault(); if (!draft.trim() || sending) return; setSending(true); try { await apiRequest(basePath, "POST", { text: draft }); setDraft(""); await loadMessages(); } finally { setSending(false); } };
  return <div className="chat-sheet"><div className="sheet-backdrop" onClick={onClose} /><section className="chat-card"><header className="chat-header"><button className="round-button" onClick={onClose}>‹</button><div><p className="eyebrow">{chat.kind === "deal" ? "Безопасная сделка" : "Заказ"}</p><b>{chat.item.title || `Диалог #${chat.item.id}`}</b></div><span className="chat-secure">● защищён</span></header><div className="chat-notice">Переписка хранится в сделке. При споре гарант сможет помочь.</div><div className="messages">{messages.length ? messages.map((message) => <div className={Number(message.sender_id) === Number(currentUserId) ? "message mine" : "message"} key={message.id}><span>{message.text}</span><small>{String(message.created_at || "").slice(11, 16)}</small></div>) : <div className="chat-empty">Пока нет сообщений. Начните обсуждение условий.</div>}</div><form className="chat-compose" onSubmit={submit}><input value={draft} onChange={(event) => setDraft(event.target.value)} placeholder="Написать сообщение..." maxLength="1200" /><button type="submit" disabled={!draft.trim() || sending}>↑</button></form></section></div>
}

function ListingComposer({ initial, onSubmit, busy, message }) {
  const [form, setForm] = useState(initial);
  const [preview, setPreview] = useState(initial.image_data || "");
  const update = (key, value) => setForm((current) => ({ ...current, [key]: value }));
  const selectImage = (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    if (!file.type.startsWith("image/") || file.size > 650000) { event.target.value = ""; return; }
    const reader = new FileReader();
    reader.onload = () => { const image = String(reader.result || ""); setPreview(image); update("image_data", image); };
    reader.readAsDataURL(file);
  };
  return <section className="listing-composer"><div className="page-title"><div><p className="eyebrow">Новая услуга</p><h1>Разместить объявление</h1><span>Покажите работу, сроки и понятную цену.</span></div></div><form className="listing-form" onSubmit={(event) => { event.preventDefault(); onSubmit(form, () => { setForm(initial); setPreview(""); }); }}><label className="listing-cover-upload">{preview ? <img src={preview} alt="Превью работы" /> : <><b>＋</b><span>Добавить обложку работы</span><small>JPG, PNG или WebP до 650 КБ</small></>}<input type="file" accept="image/*" onChange={selectImage} /></label><label>Название услуги<input required value={form.title} onChange={(event) => update("title", event.target.value)} placeholder="Например, дизайн Telegram-канала" /></label><div className="form-split"><label>Категория<select value={form.category} onChange={(event) => update("category", event.target.value)}><option>Дизайн</option><option>Разработка</option><option>Тексты</option><option>Монтаж</option><option>Другое</option></select></label><label>Срок<select value={form.delivery_time} onChange={(event) => update("delivery_time", event.target.value)}><option>По договорённости</option><option>Сегодня</option><option>1–3 дня</option><option>До недели</option><option>Больше недели</option></select></label></div><label>Цена, ₽<input required inputMode="numeric" value={form.price} onChange={(event) => update("price", event.target.value)} placeholder="1500" /></label><label>Описание<textarea required value={form.description} onChange={(event) => update("description", event.target.value)} placeholder="Что получит покупатель, что входит в услугу и что нужно от него?" /></label><div className="form-tip"><Icon name="shield" /> Объявление проверит модератор LTeam перед публикацией.</div><button className="primary" type="submit" disabled={busy}>{busy ? "Отправляем…" : "Отправить на проверку"} <Icon name="arrow" /></button>{message && <p className="form-message">{message}</p>}</form></section>;
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
  const [isSynced, setIsSynced] = useState(false);
  const [catalogListings, setCatalogListings] = useState([]);
  const [orderItems, setOrderItems] = useState([]);
  const [dealItems, setDealItems] = useState([]);
  const [chat, setChat] = useState(null);
  const [adminSummary, setAdminSummary] = useState({});
  const [balance, setBalance] = useState({ available: 0, frozen: 0 });
  const [listingForm, setListingForm] = useState({ title: "", category: "Дизайн", price: "", delivery_time: "По договорённости", description: "", image_data: "" });
  const [orderForm, setOrderForm] = useState({ title: "", category: "Разработка", budget: "", deadline: "По договорённости", description: "" });
  const [formMessage, setFormMessage] = useState("");
  const [profile, setProfile] = useState({ name: tg?.initDataUnsafe?.user?.first_name || "Гость", username: tg?.initDataUnsafe?.user?.username ? `@${tg.initDataUnsafe.user.username}` : "LTeam user" });

  useEffect(() => {
    const telegram = telegramApp();
    telegram?.ready();
    telegram?.expand();
    telegram?.setHeaderColor?.("#f7f8fc");
    telegram?.setBackgroundColor?.("#f7f8fc");
    const telegramUser = telegram?.initDataUnsafe?.user;
    if (telegramUser) {
      setProfile({ name: [telegramUser.first_name, telegramUser.last_name].filter(Boolean).join(" ") || "Пользователь", username: telegramUser.username ? `@${telegramUser.username}` : "LTeam user" });
    }

    // Роль приходит только с защищённого API. Клиент не имеет права сам выдавать доступ.
    apiFetch("/api/me")
      .then((data) => {
        if (!data?.authenticated) return;
        setIsSynced(true);
        setIsAdmin(Boolean(data.is_admin));
        setProfile({ id: data.id, name: data.name || "Пользователь", username: data.username ? `@${data.username}` : "LTeam user" });
      })
      .catch(() => setIsSynced(false));
    const retryProfileSync = () => apiFetch("/api/me")
      .then((data) => {
        if (!data?.authenticated) return;
        setIsSynced(true);
        setIsAdmin(Boolean(data.is_admin));
        setProfile({ id: data.id, name: data.name || "Пользователь", username: data.username ? `@${data.username}` : "LTeam user" });
      })
      .catch(() => setIsSynced(false));
    const profileRetry = window.setTimeout(retryProfileSync, 1200);
    apiFetch("/api/listings").then((items) => setCatalogListings(items.map((item) => ({ ...item, seller: "Исполнитель LTeam", rating: "—", orders: 0, accent: "bot" })))).catch(() => setCatalogListings([]));
    apiFetch("/api/orders").then(setOrderItems).catch(() => setOrderItems([]));
    apiFetch("/api/deals").then(setDealItems).catch(() => setDealItems([]));
    apiFetch("/api/balance").then(setBalance).catch(() => {});
    apiFetch("/api/admin/summary").then(setAdminSummary).catch(() => setAdminSummary({}));
    return () => window.clearTimeout(profileRetry);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("lteam-theme", theme);
    telegramApp()?.setHeaderColor?.(theme === "dark" || theme === "midnight" ? "#151525" : "#f7f8fc");
    telegramApp()?.setBackgroundColor?.(theme === "dark" || theme === "midnight" ? "#151525" : "#f7f8fc");
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
    <header className="topbar"><button className="brand" onClick={() => setTab("home")}><span className="brand-mark"><i>L</i><i>T</i></span><span className="brand-copy"><strong>LTEAM</strong><b>MARKET</b></span></button><span className="secure-pill"><Icon name="shield" /> Гарант</span><button className="round-button notification-button" onClick={() => sendToBot("open_notifications")} aria-label="Уведомления"><Icon name="bell" /><i /></button></header>
    {tab === "home" && <><HomeScreen profile={profile} dealsCount={dealItems.length} onNavigate={setTab} /><div className="legacy-home">
      <section className="hero-card"><div className="hero-orb one" /><div className="hero-orb two" /><p className="eyebrow">Безопасные сделки в Telegram</p><h1>Находите исполнителей.<br /><em>Работайте спокойно.</em></h1><p className="hero-copy">Оплата проходит через гаранта LTeam, а деньги исполнитель получает после вашего подтверждения.</p><div className="hero-buttons"><button className="primary" onClick={() => setTab("catalog")}>Смотреть каталог <Icon name="arrow" /></button><button className="ghost" onClick={() => setTab("create")}><Icon name="plus" /> Разместить</button></div></section>
      <section className="trust-row"><div><Icon name="shield" /><span><b>Гарант-сделки</b><small>Оплата через администратора</small></span></div><div><Icon name="check" /><span><b>Проверенные отзывы</b><small>Только после заказа</small></span></div></section>
      <section><div className="section-heading"><div><p className="eyebrow">Популярное</p><h2>Услуги для старта</h2></div><button className="text-button" onClick={() => setTab("catalog")}>Все <Icon name="arrow" /></button></div><div className="listing-grid">{catalogListings.slice(0, 2).map((item) => <ListingCard key={item.id} item={item} onOpen={(listing_id) => sendToBot("open_listing", { listing_id })} />)}</div></section>
      <WorkspaceCard profile={profile} dealsCount={dealItems.length} onNavigate={setTab} />
      <section className="quick-grid"><button onClick={() => { setTab("orders"); sendToBot("open_orders"); }}><span className="quick-icon purple">⌁</span><b>Найти исполнителя</b><small>Создать заказ</small></button><button onClick={() => sendToBot("open_guarantee")}><span className="quick-icon mint">✦</span><b>Как работает гарант</b><small>6 простых шагов</small></button></section>
    </div></>}

    {tab === "guide" && <GuidePage onClose={() => setTab("home")} onOrders={() => setTab("orders")} />}

    {tab === "catalog" && <section><div className="page-title"><div><p className="eyebrow">Маркетплейс</p><h1>Каталог услуг</h1></div><button className="round-button" onClick={() => setQuery("")}><Icon name="filter" /></button></div><label className="search-box"><Icon name="search" /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Что вы ищете?" /></label><div className="chips">{["Все", "Дизайн", "Разработка", "Тексты", "Монтаж"].map((category) => <button className={activeCategory === category ? "selected" : ""} onClick={() => setActiveCategory(category)} key={category}>{category}</button>)}</div>{listings.length ? <div className="listing-grid">{listings.map((item) => <ListingCard key={item.id} item={item} favorite={favorites.includes(item.id)} onFavorite={toggleFavorite} onOpen={(listing_id) => sendToBot("open_listing", { listing_id })} />)}</div> : <div className="empty-note"><Icon name="search" /><div><b>Ничего не найдено</b><span>Попробуйте изменить запрос или выберите другую категорию.</span></div></div>}</section>}

    {tab === "orders" && <section><div className="page-title"><div><p className="eyebrow">Ваши задачи</p><h1>Заказы и сделки</h1></div><button className="round-button" onClick={() => setTab("create-order")}><Icon name="plus" /></button></div><button className="create-order" onClick={() => setTab("create-order")}><span><Icon name="plus" /></span><div><b>Создать заказ</b><small>Исполнители откликнутся сами</small></div><Icon name="arrow" /></button><div className="subsection-title"><b>Заказы</b><span>{orderItems.length}</span></div>{orderItems.length ? <div className="order-grid">{orderItems.map((order) => <OrderCard key={order.id} order={order} onOpen={(item) => setChat({ kind: "order", item })} />)}</div> : <div className="empty-note"><Icon name="orders" /><div><b>Заказов пока нет</b><span>Здесь появятся все заказы, созданные в боте или MiniApp.</span></div></div>}<div className="subsection-title"><b>Сделки</b><span>{dealItems.length}</span></div>{dealItems.length ? dealItems.map((deal) => <div className="deal-with-chat" key={deal.id}><DealCard deal={deal} /><button className="deal-chat-button" onClick={() => setChat({ kind: "deal", item: deal })}>Открыть чат сделки</button></div>) : <div className="empty-note"><Icon name="chat" /><div><b>Сделок пока нет</b><span>Все обсуждения и статусы сделок будут здесь.</span></div></div>}</section>}

    {tab === "create-order" && <section><div className="page-title"><div><p className="eyebrow">Новая задача</p><h1>Создать заказ</h1></div><button className="round-button" onClick={() => setTab("orders")}>×</button></div><form className="listing-form" onSubmit={async (event) => { event.preventDefault(); setFormMessage(""); try { const result = await apiRequest("/api/orders", "POST", orderForm); setOrderItems((items) => [{ ...orderForm, id: result.order_id, status: "moderation" }, ...items]); setOrderForm({ title: "", category: "Разработка", budget: "", deadline: "По договорённости", description: "" }); setFormMessage("Заказ отправлен на модерацию. После проверки он станет виден исполнителям."); } catch (error) { setFormMessage(error.message); } }}><label>Что нужно сделать?<input required value={orderForm.title} onChange={(event) => setOrderForm({ ...orderForm, title: event.target.value })} placeholder="Например, разработать Telegram-бота" /></label><label>Категория<select value={orderForm.category} onChange={(event) => setOrderForm({ ...orderForm, category: event.target.value })}><option>Разработка</option><option>Дизайн</option><option>Тексты</option><option>Монтаж</option><option>Другое</option></select></label><div className="form-split"><label>Бюджет, ₽<input required inputMode="numeric" value={orderForm.budget} onChange={(event) => setOrderForm({ ...orderForm, budget: event.target.value })} placeholder="5000" /></label><label>Срок<select value={orderForm.deadline} onChange={(event) => setOrderForm({ ...orderForm, deadline: event.target.value })}><option>По договорённости</option><option>Сегодня</option><option>1–3 дня</option><option>До недели</option><option>Больше недели</option></select></label></div><label>Опишите задачу<textarea required value={orderForm.description} onChange={(event) => setOrderForm({ ...orderForm, description: event.target.value })} placeholder="Каким должен быть результат, что уже есть и что важно учесть?" /></label><div className="form-tip"><Icon name="shield" /> Перед публикацией заказ проверит модератор LTeam.</div><button className="primary" type="submit">Отправить на модерацию <Icon name="arrow" /></button>{formMessage && <p className="form-message">{formMessage}</p>}</form></section>}

    {tab === "wallet" && <section><div className="page-title"><div><p className="eyebrow">Финансы</p><h1>Баланс</h1></div><button className="round-button" onClick={() => sendToBot("balance_history")}><Icon name="orders" /></button></div><div className="balance-card"><p>Доступно к выводу</p><h2>{Number(balance.available || 0).toLocaleString("ru-RU")} ₽</h2><span>В обработке у гаранта: {Number(balance.frozen || 0).toLocaleString("ru-RU")} ₽</span><button className="primary" onClick={() => sendToBot("withdraw_start")}>Вывести средства <Icon name="arrow" /></button></div><div className="list-row"><span className="list-icon mint"><Icon name="wallet" /></span><div><b>История операций</b><small>Пополнения, сделки и выплаты</small></div><Icon name="arrow" /></div></section>}

    {tab === "profile" && <section><div className="profile-card"><span className="avatar large">{profile.name.slice(0, 1).toUpperCase()}</span><div><p className="eyebrow">Профиль LTeam</p><h1>{profile.name}</h1><span>{profile.username}</span><small className={`sync-status ${isSynced ? "online" : "offline"}`}>{isSynced ? "● Синхронизирован с ботом" : "○ Требуется синхронизация с ботом"}</small></div><button className="round-button" onClick={() => setSettingsOpen(true)}>⚙</button></div><div className="profile-stats"><div><b>{orderItems.length}</b><span>Заказов</span></div><div><b>{dealItems.length}</b><span>Сделок</span></div><div><b>{catalogListings.filter((item) => Number(item.seller_id) === Number(profile.id)).length}</b><span>Услуг</span></div></div><div className="settings-list"><button onClick={() => sendToBot("my_listings")}><Icon name="orders" /><span>Мои объявления</span><Icon name="arrow" /></button><button onClick={() => setToast(favorites.length ? `В избранном: ${favorites.length}` : "В избранном пока ничего нет")}><Icon name="star" /><span>Избранное</span><Icon name="arrow" /></button><button onClick={() => sendToBot("support")}><Icon name="chat" /><span>Поддержка</span><Icon name="arrow" /></button></div>{isAdmin && <AdminPanel summary={adminSummary} />}</section>}

    {tab === "create" && <ListingComposer initial={listingForm} message={formMessage} onSubmit={async (form, reset) => { setFormMessage(""); try { await apiRequest("/api/listings", "POST", form); setFormMessage("Объявление отправлено на проверку."); const clean = { title: "", category: "Дизайн", price: "", delivery_time: "По договорённости", description: "", image_data: "" }; setListingForm(clean); reset(); } catch (error) { setFormMessage(error.message); } }} />}

    {tab === "catalog" && <CatalogV3 items={catalogListings} orders={orderItems} favorites={favorites} onFavorite={toggleFavorite} onNavigate={setTab} request={apiRequest} fetchData={apiFetch} />}
    {tab === "orders" && <OrdersWorkspace orders={orderItems} deals={dealItems} profile={profile} fetchData={apiFetch} onNavigate={setTab} onChat={setChat} />}
    <nav className="bottom-nav">{navItems.map(([id, icon, label]) => <button className={tab === id ? "active" : ""} key={id} onClick={() => setTab(id)}><Icon name={icon} /><span>{label}</span></button>)}</nav>
    {settingsOpen && <SettingsSheet theme={theme} setTheme={setTheme} onClose={() => setSettingsOpen(false)} />}
    {toast && <div className="toast"><span>✓</span>{toast}</div>}
    {chat && <ChatSheet chat={chat} currentUserId={profile.id} onClose={() => setChat(null)} />}
  </main>;
}
