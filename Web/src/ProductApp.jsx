import { useCallback, useEffect, useState } from "react";
import CatalogV3 from "./CatalogV3";
import OrdersWorkspace from "./OrdersWorkspace";
import MyListingsWorkspace from "./MyListingsWorkspace";
import AdminWorkspace from "./AdminWorkspace";
import WalletWorkspace from "./WalletWorkspace";
import FavoritesWorkspace from "./FavoritesWorkspace";
import SupportWorkspace from "./SupportWorkspace";
import SettingsWorkspace from "./SettingsWorkspace";
import ProfileWorkspace from "./ProfileWorkspace";
import OrderComposer from "./OrderComposer";
import { ListingComposer } from "./App";
import workspaceCover from "./assets/market-workspace-cover.png";
import "./CatalogV3.css";
import "./Marketplace.css";
import "./MarketplaceFix.css";
import "./OrdersWorkspace.css";
import "./OrderComposer.css";
import "./MyListingsWorkspace.css";
import "./AdminWorkspace.css";
import "./AdminModeration.css";
import "./WalletWorkspace.css";
import "./FavoritesWorkspace.css";
import "./SupportWorkspace.css";
import "./SettingsWorkspace.css";
import "./ProfileWorkspace.css";
import "./ReviewCards.css";
import "./ReviewSheet.css";
import "./ThemeBridge.css";
import "./ProfessionalApp.css";

const API = import.meta.env.VITE_API_URL || "https://lteam-botminiapp.onrender.com";
const tg = () => window.Telegram?.WebApp;
async function call(path, options = {}) {
  const initData = tg()?.initData || "";
  const response = await fetch(API + path, { ...options, headers: { ...(options.body ? { "Content-Type": "application/json" } : {}), ...(initData ? { "X-Telegram-Init-Data": initData } : {}) } });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || "Request failed");
  return payload;
}
const nav = [["home", "Главная", "home"], ["catalog", "Каталог", "grid"], ["create", "Создать", "plus"], ["orders", "Заказы", "briefcase"], ["profile", "Профиль", "user"]];
function Icon({ name, size = 20 }) { const common = { width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.9, strokeLinecap: "round", strokeLinejoin: "round", "aria-hidden": true }; const paths = { home: <><path d="m3 10 9-7 9 7v10a1 1 0 0 1-1 1h-5v-6H9v6H4a1 1 0 0 1-1-1Z" /></>, grid: <><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></>, plus: <><path d="M12 5v14M5 12h14"/></>, briefcase: <><rect x="3" y="7" width="18" height="13" rx="2"/><path d="M8 7V5a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2M3 12h18M10 12v2h4v-2"/></>, user: <><circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/></>, bell: <><path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4"/></>, search: <><circle cx="11" cy="11" r="6"/><path d="m20 20-4.2-4.2"/></>, arrow: <><path d="M5 12h14M13 6l6 6-6 6"/></>, spark: <><path d="m12 3 1.7 5.3L19 10l-5.3 1.7L12 17l-1.7-5.3L5 10l5.3-1.7Z"/></> }; return <svg {...common}>{paths[name] || paths.spark}</svg>; }
function Logo() { return <span className="pro-logo">LT</span>; }
function ProductNav({ active, onNavigate, onCreate }) { return <nav className="pro-nav">{nav.map(([id, text, icon]) => <button key={id} className={`${active === id ? "active" : ""} ${id === "create" ? "pro-nav-create" : ""}`} onClick={() => id === "create" ? onCreate() : onNavigate(id)}><i><Icon name={icon} size={id === "create" ? 22 : 19} /></i><span>{text}</span></button>)}</nav>; }
function MiniServiceCard({ item, onClick }) { return <button className="home-service-card" onClick={onClick}><div className="home-service-cover">{item.image_data ? <img src={item.image_data} alt="" /> : <span>{String(item.category || "LT").slice(0, 2)}</span>}</div><div><b>{item.title}</b><small>{item.seller_name || item.seller_username || "Исполнитель LTeam"}</small><footer><span>★ {item.reviews_count ? Number(item.avg_rating || 0).toFixed(1) : "Новый"}</span><strong>от {Number(item.price || 0).toLocaleString("ru-RU")} ₽</strong></footer></div></button>; }
function Home({ profile, listings, orders, onNavigate, isAdmin, onCreate }) {
  const categoryNames = ["Telegram-боты", "Дизайн", "Монтаж", "AI"], featured = listings.slice(0, 4), openOrders = orders.slice(0, 2);
  return <section className="pro-home">
    <header className="pro-topbar"><button className="pro-brand" onClick={() => onNavigate("home")}><Logo /><b>LT <em>Market</em></b><small>Бета</small></button><div className="pro-top-actions">{isAdmin && <button className="pro-admin-button" onClick={() => onNavigate("admin")}>Команда</button>}<button className="pro-bell" aria-label="Уведомления" onClick={() => onNavigate("notifications")}><Icon name="bell" /></button><button className="pro-avatar" onClick={() => onNavigate("profile")}>{String(profile.name || "L").slice(0, 1).toUpperCase()}</button></div></header>
    <button className="home-search" onClick={() => onNavigate("catalog")}><Icon name="search" /><span>Найти услугу или исполнителя</span></button>
    <section className="home-categories"><div className="home-section-title"><h1>Что нужно сделать?</h1><button onClick={() => onNavigate("catalog")}>Все категории</button></div><div>{categoryNames.map((name, index) => <button key={name} onClick={() => onNavigate("catalog")}><i>{["◈", "◐", "▷", "✦"][index]}</i><span>{name}</span></button>)}</div></section>
    <section className="home-prompt"><img src={workspaceCover} alt=""/><div><span>РАБОЧЕЕ ПРОСТРАНСТВО В TELEGRAM</span><h2>Найдите исполнителя<br/>для своей задачи</h2><p>Сравните портфолио, договоритесь об условиях и ведите работу в одном месте.</p></div><button onClick={() => onCreate()}>Создать заказ <Icon name="arrow" /></button></section>
    <section className="home-section"><div className="home-section-title"><div><span>РЕКОМЕНДАЦИИ</span><h2>Новые услуги</h2></div><button onClick={() => onNavigate("catalog")}>Смотреть все</button></div>{featured.length ? <div className="home-service-list">{featured.map((item) => <MiniServiceCard key={item.id} item={item} onClick={() => onNavigate("catalog")} />)}</div> : <div className="home-empty"><Icon name="spark" /><div><b>Каталог только запускается</b><span>Создайте заказ — подходящие исполнители смогут откликнуться.</span></div><button onClick={onCreate}>Создать заказ</button></div>}</section>
    <section className="home-section home-orders"><div className="home-section-title"><div><span>ДЛЯ ИСПОЛНИТЕЛЕЙ</span><h2>Новые заказы</h2></div><button onClick={() => onNavigate("catalog")}>Все заказы</button></div>{openOrders.length ? openOrders.map((order) => <button className="home-order-row" key={order.id} onClick={() => onNavigate("catalog")}><span>{order.category}</span><b>{order.title}</b><small>до {Number(order.budget || 0).toLocaleString("ru-RU")} ₽ · {order.deadline || "Срок обсуждается"}</small><i><Icon name="arrow" /></i></button>) : <p className="home-muted">Здесь появятся задачи, на которые можно откликнуться.</p>}</section>
    <p className="home-beta-note">LT Market работает в бета-режиме. Площадка пока не принимает и не хранит оплату за сделки.</p>
  </section>;
}
function CreateSheet({ onClose, onChoose }) { return <div className="create-sheet-backdrop" onMouseDown={onClose}><section className="create-sheet" onMouseDown={(event) => event.stopPropagation()}><div className="sheet-handle"/><button className="create-sheet-close" onClick={onClose}>×</button><span>СОЗДАТЬ</span><h2>Что хотите разместить?</h2><p>Выберите формат — черновик сохранится, пока вы заполняете данные.</p><button onClick={() => onChoose("create-order")}><i><Icon name="briefcase" /></i><div><b>Создать заказ</b><small>Найдите исполнителя под задачу</small></div><Icon name="arrow" /></button><button onClick={() => onChoose("create")}><i><Icon name="grid" /></i><div><b>Создать услугу</b><small>Покажите работу и портфолио</small></div><Icon name="arrow" /></button></section></div>; }
function Notifications({ onNavigate }) { return <section className="notifications-page"><header><button onClick={() => onNavigate("home")}>←</button><div><span>ЦЕНТР СОБЫТИЙ</span><h1>Уведомления</h1></div></header><div className="notifications-empty"><i><Icon name="bell" size={27}/></i><h2>Пока тихо</h2><p>Здесь появятся важные события: отклики, сообщения, изменения условий и статусы заказов.</p><button onClick={() => onNavigate("catalog")}>Перейти в каталог</button></div></section>; }
function Guide({ onNavigate }) { return <section className="pro-guide"><button className="pro-back" onClick={() => onNavigate("home")}>← Назад</button><span className="pro-eyebrow">КРАТКИЙ ГИД</span><h1>Три шага до результата</h1><div className="pro-guide-steps">{[["01","Выберите","Откройте каталог услуг или создайте заказ."],["02","Обсудите","Зафиксируйте задачу, стоимость, срок и формат результата."],["03","Завершите","Передайте результат, запросите правки или оставьте отзыв."]].map(([id,title,text]) => <article key={id}><i>{id}</i><div><b>{title}</b><p>{text}</p></div></article>)}</div><button className="pro-primary" onClick={() => onNavigate("catalog")}>Перейти в каталог →</button></section>; }

export default function ProductApp() {
  const telegramUser = tg()?.initDataUnsafe?.user;
  const [route, setRoute] = useState("home");
  const [theme, setTheme] = useState(() => localStorage.getItem("lteam-theme") || "dark");
  const [profile, setProfile] = useState({ name: [telegramUser?.first_name, telegramUser?.last_name].filter(Boolean).join(" ") || "Пользователь", username: telegramUser?.username ? "@" + telegramUser.username : "LTeam user" });
  const [listings, setListings] = useState([]), [orders, setOrders] = useState([]), [deals, setDeals] = useState([]), [balance, setBalance] = useState({});
  const [favorites, setFavorites] = useState(() => JSON.parse(localStorage.getItem("lteam-favorites") || "[]"));
  const [isAdmin, setIsAdmin] = useState(false), [adminSummary, setAdminSummary] = useState({}), [settings, setSettings] = useState(false), [notice, setNotice] = useState(""), [createSheet, setCreateSheet] = useState(false);
  const refresh = useCallback(async () => {
    const [nextListings, nextOrders, nextDeals, nextBalance] = await Promise.all([call("/api/listings"), call("/api/orders"), call("/api/deals"), call("/api/balance")]);
    setListings(nextListings); setOrders(nextOrders); setDeals(nextDeals); setBalance(nextBalance);
  }, []);
  useEffect(() => {
    tg()?.ready?.(); tg()?.expand?.();
    const bootTimer = window.setTimeout(() => { refresh().catch(() => {}); }, 0);
    call("/api/me").then((me) => {
      if (me.authenticated) setProfile((value) => ({ id: me.id, name: me.name || value.name, username: me.username ? "@" + me.username : value.username }));
      setIsAdmin(Boolean(me.is_admin));
      if (me.is_admin) call("/api/admin/summary").then(setAdminSummary).catch(() => {});
    }).catch(() => {});
    return () => window.clearTimeout(bootTimer);
  }, [refresh]);
  useEffect(() => { document.documentElement.dataset.theme = theme; localStorage.setItem("lteam-theme", theme); }, [theme]);
  useEffect(() => localStorage.setItem("lteam-favorites", JSON.stringify(favorites)), [favorites]);
  const request = (path, method, body) => call(path, { method, body: JSON.stringify(body) });
  const toggleFavorite = (id) => setFavorites((value) => value.includes(id) ? value.filter((entry) => entry !== id) : [...value, id]);
  let content;
  if (route === "home") content = <Home profile={profile} listings={listings} orders={orders} onNavigate={setRoute} isAdmin={isAdmin} onCreate={() => setCreateSheet(true)} />;
  else if (route === "guide") content = <Guide onNavigate={setRoute} />;
  else if (route === "notifications") content = <Notifications onNavigate={setRoute} />;
  else if (route === "catalog") content = <CatalogV3 items={listings} orders={orders} favorites={favorites} onFavorite={toggleFavorite} onNavigate={setRoute} request={request} fetchData={call} />;
  else if (route === "orders") content = <OrdersWorkspace orders={orders} deals={deals} profile={profile} fetchData={call} request={request} onNavigate={setRoute} onChat={() => setNotice("Чаты открываются из карточки сделки.")} onDealsChanged={setDeals} />;
  else if (route === "create-order") content = <OrderComposer initial={{ title: "", category: "Telegram-боты и Mini Apps", budget: "", deadline: "По договорённости", description: "" }} onClose={() => setRoute("orders")} message={notice} onSubmit={async (form, reset) => { try { await request("/api/orders", "POST", form); reset(); setNotice("Заказ отправлен на модерацию."); refresh(); } catch (error) { setNotice(error.message); } }} />;
  else if (route === "create") content = <ListingComposer initial={{ title: "", category: "Дизайн Telegram", price: "", delivery_time: "По договорённости", description: "", image_data: "", portfolio_data: [] }} message={notice} onSubmit={async (form, reset) => { try { await request("/api/listings", "POST", form); reset(); setNotice("Услуга отправлена на модерацию."); refresh(); setRoute("my-listings"); } catch (error) { setNotice(error.message); } }} />;
  else if (route === "profile") content = <ProfileWorkspace profile={profile} listings={listings} orders={orders} deals={deals} balance={balance} isSynced isAdmin={isAdmin} onNavigate={setRoute} onSettings={() => setSettings(true)} />;
  else if (route === "wallet") content = <WalletWorkspace balance={balance} fetchData={call} onNavigate={setRoute} onWithdraw={() => tg()?.sendData?.("withdraw_start")} />;
  else if (route === "my-listings") content = <MyListingsWorkspace fetchData={call} request={request} onNavigate={setRoute} onOpenDeal={() => { refresh(); setRoute("orders"); }} />;
  else if (route === "favorites") content = <FavoritesWorkspace items={listings} favorites={favorites} onToggle={toggleFavorite} onNavigate={setRoute} />;
  else if (route === "support") content = <SupportWorkspace fetchData={call} request={request} onNavigate={setRoute} />;
  else if (route === "admin" && isAdmin) content = <AdminWorkspace summary={adminSummary} fetchData={call} request={request} onNavigate={setRoute} onOpenBot={(section) => tg()?.sendData?.(section || "admin_panel")} />;
  else content = <Home profile={profile} listings={listings} orders={orders} onNavigate={setRoute} isAdmin={isAdmin} onCreate={() => setCreateSheet(true)} />;
  return <main className="product-app">{content}{route !== "guide" && <ProductNav active={route} onNavigate={setRoute} onCreate={() => setCreateSheet(true)} />}{createSheet && <CreateSheet onClose={() => setCreateSheet(false)} onChoose={(nextRoute) => { setCreateSheet(false); setRoute(nextRoute); }} />}{settings && <SettingsWorkspace theme={theme} setTheme={setTheme} onClose={() => setSettings(false)} />}{notice && <button className="pro-notice" onClick={() => setNotice("")}>{notice}<i>×</i></button>}</main>;
}
