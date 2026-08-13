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
const nav = [["home", "Главная", "⌂"], ["catalog", "Маркет", "▦"], ["orders", "Заказы", "▣"], ["profile", "Профиль", "○"]];
function Logo() { return <span className="pro-logo">LT</span>; }
function ProductNav({ active, onNavigate }) { return <nav className="pro-nav">{nav.map(([id, text, icon]) => <button key={id} className={active === id ? "active" : ""} onClick={() => onNavigate(id)}><i>{icon}</i><span>{text}</span></button>)}</nav>; }
function Home({ profile, listings, orders, deals, onNavigate, isAdmin }) {
  const activeDeals = deals.filter((deal) => !["completed", "cancelled"].includes(deal.status)).length;
  return <section className="pro-home">
    <header className="pro-topbar"><button className="pro-brand" onClick={() => onNavigate("home")}><Logo /><b>LTeam <em>Market</em></b></button><div className="pro-top-actions">{isAdmin && <button className="pro-admin-button" onClick={() => onNavigate("admin")}>Admin</button>}<button className="pro-avatar" onClick={() => onNavigate("profile")}>{String(profile.name || "L").slice(0, 1).toUpperCase()}</button></div></header>
    <section className="pro-hero"><span className="pro-eyebrow">БЕЗОПАСНЫЙ МАРКЕТПЛЕЙС УСЛУГ</span><h1>Работайте <i>спокойно.</i><br />Закроем сделку.</h1><p>Искать исполнителя, продавать услуги и обсуждать условия — всё в Telegram, с гарантом LTeam.</p><div className="pro-hero-actions"><button className="pro-primary" onClick={() => onNavigate("catalog")}>Найти исполнителя <b>→</b></button><button className="pro-secondary" onClick={() => onNavigate("create-order")}>Создать заказ</button></div><div className="pro-safety"><span>✓</span><div><b>Оплата через гаранта</b><small>Деньги передаются исполнителю после вашего подтверждения</small></div></div></section>
    <section className="pro-section-head"><div><span>ВОЗМОЖНОСТИ</span><h2>Всё для работы в одном месте</h2></div><button onClick={() => onNavigate("guide")}>Как это работает? →</button></section>
    <div className="pro-feature-grid"><button onClick={() => onNavigate("catalog")}><i>⌕</i><b>Каталог услуг</b><span>{listings.length || 0} активных предложений</span></button><button onClick={() => onNavigate("orders")}><i>▣</i><b>Заказы и сделки</b><span>{activeDeals ? activeDeals + " в работе" : "Новые отклики и диалоги"}</span></button><button onClick={() => onNavigate("create")}><i>+</i><b>Продать услугу</b><span>Создайте витрину с портфолио</span></button></div>
    <section className="pro-activity"><div><span>ВАША РАБОЧАЯ ОБЛАСТЬ</span><h2>{profile.name}</h2></div><div><b>{orders.length}</b><small>заказов</small></div><div><b>{deals.length}</b><small>сделок</small></div><button onClick={() => onNavigate("profile")}>Открыть профиль →</button></section>
  </section>;
}
function Guide({ onNavigate }) { return <section className="pro-guide"><button className="pro-back" onClick={() => onNavigate("home")}>← Назад</button><span className="pro-eyebrow">КРАТКИЙ ГИД</span><h1>Три шага до результата</h1><div className="pro-guide-steps">{[["01","Выберите","Откройте каталог услуг или создайте заказ."],["02","Обсудите","Детали и сообщения хранятся в защищённой сделке."],["03","Подтвердите","Гарант контролирует оплату и завершение."]].map(([id,title,text]) => <article key={id}><i>{id}</i><div><b>{title}</b><p>{text}</p></div></article>)}</div><button className="pro-primary" onClick={() => onNavigate("catalog")}>Перейти в каталог →</button></section>; }

export default function ProductApp() {
  const telegramUser = tg()?.initDataUnsafe?.user;
  const [route, setRoute] = useState("home");
  const [theme, setTheme] = useState(() => localStorage.getItem("lteam-theme") || "dark");
  const [profile, setProfile] = useState({ name: [telegramUser?.first_name, telegramUser?.last_name].filter(Boolean).join(" ") || "Пользователь", username: telegramUser?.username ? "@" + telegramUser.username : "LTeam user" });
  const [listings, setListings] = useState([]), [orders, setOrders] = useState([]), [deals, setDeals] = useState([]), [balance, setBalance] = useState({});
  const [favorites, setFavorites] = useState(() => JSON.parse(localStorage.getItem("lteam-favorites") || "[]"));
  const [isAdmin, setIsAdmin] = useState(false), [adminSummary, setAdminSummary] = useState({}), [settings, setSettings] = useState(false), [notice, setNotice] = useState("");
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
  if (route === "home") content = <Home profile={profile} listings={listings} orders={orders} deals={deals} onNavigate={setRoute} isAdmin={isAdmin} />;
  else if (route === "guide") content = <Guide onNavigate={setRoute} />;
  else if (route === "catalog") content = <CatalogV3 items={listings} orders={orders} favorites={favorites} onFavorite={toggleFavorite} onNavigate={setRoute} request={request} fetchData={call} />;
  else if (route === "orders") content = <OrdersWorkspace orders={orders} deals={deals} profile={profile} fetchData={call} request={request} onNavigate={setRoute} onChat={() => setNotice("Чаты открываются из карточки сделки.")} onDealsChanged={setDeals} />;
  else if (route === "create-order") content = <OrderComposer initial={{ title: "", category: "Разработка", budget: "", deadline: "По договорённости", description: "" }} onClose={() => setRoute("orders")} message={notice} onSubmit={async (form, reset) => { try { await request("/api/orders", "POST", form); reset(); setNotice("Заказ отправлен на модерацию."); refresh(); } catch (error) { setNotice(error.message); } }} />;
  else if (route === "create") content = <ListingComposer initial={{ title: "", category: "Дизайн", price: "", delivery_time: "По договорённости", description: "", image_data: "", portfolio_data: [] }} message={notice} onSubmit={async (form, reset) => { try { await request("/api/listings", "POST", form); reset(); setNotice("Услуга отправлена на модерацию."); refresh(); setRoute("my-listings"); } catch (error) { setNotice(error.message); } }} />;
  else if (route === "profile") content = <ProfileWorkspace profile={profile} listings={listings} orders={orders} deals={deals} balance={balance} isSynced isAdmin={isAdmin} onNavigate={setRoute} onSettings={() => setSettings(true)} />;
  else if (route === "wallet") content = <WalletWorkspace balance={balance} fetchData={call} onNavigate={setRoute} onWithdraw={() => tg()?.sendData?.("withdraw_start")} />;
  else if (route === "my-listings") content = <MyListingsWorkspace fetchData={call} request={request} onNavigate={setRoute} onOpenDeal={() => { refresh(); setRoute("orders"); }} />;
  else if (route === "favorites") content = <FavoritesWorkspace items={listings} favorites={favorites} onToggle={toggleFavorite} onNavigate={setRoute} />;
  else if (route === "support") content = <SupportWorkspace fetchData={call} request={request} onNavigate={setRoute} />;
  else if (route === "admin" && isAdmin) content = <AdminWorkspace summary={adminSummary} fetchData={call} request={request} onNavigate={setRoute} onOpenBot={(section) => tg()?.sendData?.(section || "admin_panel")} />;
  else content = <Home profile={profile} listings={listings} orders={orders} deals={deals} onNavigate={setRoute} isAdmin={isAdmin} />;
  return <main className="product-app">{content}{route !== "guide" && <ProductNav active={route} onNavigate={setRoute} />}{settings && <SettingsWorkspace theme={theme} setTheme={setTheme} onClose={() => setSettings(false)} />}{notice && <button className="pro-notice" onClick={() => setNotice("")}>{notice}<i>×</i></button>}</main>;
}
