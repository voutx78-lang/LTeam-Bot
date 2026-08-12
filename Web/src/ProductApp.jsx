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
const nav = [["home", "\u0413\u043b\u0430\u0432\u043d\u0430\u044f", "\u2302"], ["catalog", "\u041c\u0430\u0440\u043a\u0435\u0442", "\u25a6"], ["orders", "\u0417\u0430\u043a\u0430\u0437\u044b", "\u25a3"], ["profile", "\u041f\u0440\u043e\u0444\u0438\u043b\u044c", "\u25cb"]];
function Logo() { return <span className="pro-logo">LT</span>; }
function ProductNav({ active, onNavigate }) { return <nav className="pro-nav">{nav.map(([id, text, icon]) => <button key={id} className={active === id ? "active" : ""} onClick={() => onNavigate(id)}><i>{icon}</i><span>{text}</span></button>)}</nav>; }
function Home({ profile, listings, orders, deals, onNavigate, isAdmin }) {
  const activeDeals = deals.filter((deal) => !["completed", "cancelled"].includes(deal.status)).length;
  return <section className="pro-home">
    <header className="pro-topbar"><button className="pro-brand" onClick={() => onNavigate("home")}><Logo /><b>LTeam <em>Market</em></b></button><div className="pro-top-actions">{isAdmin && <button className="pro-admin-button" onClick={() => onNavigate("admin")}>Admin</button>}<button className="pro-avatar" onClick={() => onNavigate("profile")}>{String(profile.name || "L").slice(0, 1).toUpperCase()}</button></div></header>
    <section className="pro-hero"><span className="pro-eyebrow">\u0411\u0415\u0417\u041e\u041f\u0410\u0421\u041d\u042b\u0419 \u041c\u0410\u0420\u041a\u0415\u0422\u041f\u041b\u0415\u0419\u0421 \u0423\u0421\u041b\u0423\u0413</span><h1>\u0420\u0430\u0431\u043e\u0442\u0430\u0439\u0442\u0435 <i>\u0441\u043f\u043e\u043a\u043e\u0439\u043d\u043e.</i><br />\u0417\u0430\u043a\u0440\u043e\u0435\u043c \u0441\u0434\u0435\u043b\u043a\u0443.</h1><p>\u0418\u0441\u043a\u0430\u0442\u044c \u0438\u0441\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044f, \u043f\u0440\u043e\u0434\u0430\u0432\u0430\u0442\u044c \u0443\u0441\u043b\u0443\u0433\u0438 \u0438 \u043e\u0431\u0441\u0443\u0436\u0434\u0430\u0442\u044c \u0443\u0441\u043b\u043e\u0432\u0438\u044f \u2014 \u0432\u0441\u0451 \u0432 Telegram, \u0441 \u0433\u0430\u0440\u0430\u043d\u0442\u043e\u043c LTeam.</p><div className="pro-hero-actions"><button className="pro-primary" onClick={() => onNavigate("catalog")}>\u041d\u0430\u0439\u0442\u0438 \u0438\u0441\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044f <b>\u2192</b></button><button className="pro-secondary" onClick={() => onNavigate("create-order")}>\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0437\u0430\u043a\u0430\u0437</button></div><div className="pro-safety"><span>\u2713</span><div><b>\u041e\u043f\u043b\u0430\u0442\u0430 \u0447\u0435\u0440\u0435\u0437 \u0433\u0430\u0440\u0430\u043d\u0442\u0430</b><small>\u0414\u0435\u043d\u044c\u0433\u0438 \u043f\u0435\u0440\u0435\u0434\u0430\u044e\u0442\u0441\u044f \u0438\u0441\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044e \u043f\u043e\u0441\u043b\u0435 \u0432\u0430\u0448\u0435\u0433\u043e \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f</small></div></div></section>
    <section className="pro-section-head"><div><span>\u0412\u041e\u0417\u041c\u041e\u0416\u041d\u041e\u0421\u0422\u0418</span><h2>\u0412\u0441\u0451 \u0434\u043b\u044f \u0440\u0430\u0431\u043e\u0442\u044b \u0432 \u043e\u0434\u043d\u043e\u043c \u043c\u0435\u0441\u0442\u0435</h2></div><button onClick={() => onNavigate("guide")}>\u041a\u0430\u043a \u044d\u0442\u043e \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442? \u2192</button></section>
    <div className="pro-feature-grid"><button onClick={() => onNavigate("catalog")}><i>\u2315</i><b>\u041a\u0430\u0442\u0430\u043b\u043e\u0433 \u0443\u0441\u043b\u0443\u0433</b><span>{listings.length || 0} \u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u043f\u0440\u0435\u0434\u043b\u043e\u0436\u0435\u043d\u0438\u0439</span></button><button onClick={() => onNavigate("orders")}><i>\u25a3</i><b>\u0417\u0430\u043a\u0430\u0437\u044b \u0438 \u0441\u0434\u0435\u043b\u043a\u0438</b><span>{activeDeals ? activeDeals + " \u0432 \u0440\u0430\u0431\u043e\u0442\u0435" : "\u041d\u043e\u0432\u044b\u0435 \u043e\u0442\u043a\u043b\u0438\u043a\u0438 \u0438 \u0434\u0438\u0430\u043b\u043e\u0433\u0438"}</span></button><button onClick={() => onNavigate("create")}><i>+</i><b>\u041f\u0440\u043e\u0434\u0430\u0442\u044c \u0443\u0441\u043b\u0443\u0433\u0443</b><span>\u0421\u043e\u0437\u0434\u0430\u0439\u0442\u0435 \u0432\u0438\u0442\u0440\u0438\u043d\u0443 \u0441 \u043f\u043e\u0440\u0442\u0444\u043e\u043b\u0438\u043e</span></button></div>
    <section className="pro-activity"><div><span>\u0412\u0410\u0428\u0410 \u0420\u0410\u0411\u041e\u0427\u0410\u042f \u041e\u0411\u041b\u0410\u0421\u0422\u042c</span><h2>{profile.name}</h2></div><div><b>{orders.length}</b><small>\u0437\u0430\u043a\u0430\u0437\u043e\u0432</small></div><div><b>{deals.length}</b><small>\u0441\u0434\u0435\u043b\u043e\u043a</small></div><button onClick={() => onNavigate("profile")}>\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u043f\u0440\u043e\u0444\u0438\u043b\u044c \u2192</button></section>
  </section>;
}
function Guide({ onNavigate }) { return <section className="pro-guide"><button className="pro-back" onClick={() => onNavigate("home")}>\u2190 \u041d\u0430\u0437\u0430\u0434</button><span className="pro-eyebrow">\u041a\u0420\u0410\u0422\u041a\u0418\u0419 \u0413\u0418\u0414</span><h1>\u0422\u0440\u0438 \u0448\u0430\u0433\u0430 \u0434\u043e \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u0430</h1><div className="pro-guide-steps">{[["01","\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435","\u041e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 \u043a\u0430\u0442\u0430\u043b\u043e\u0433 \u0443\u0441\u043b\u0443\u0433 \u0438\u043b\u0438 \u0441\u043e\u0437\u0434\u0430\u0439\u0442\u0435 \u0437\u0430\u043a\u0430\u0437."],["02","\u041e\u0431\u0441\u0443\u0434\u0438\u0442\u0435","\u0414\u0435\u0442\u0430\u043b\u0438 \u0438 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f \u0445\u0440\u0430\u043d\u044f\u0442\u0441\u044f \u0432 \u0437\u0430\u0449\u0438\u0449\u0451\u043d\u043d\u043e\u0439 \u0441\u0434\u0435\u043b\u043a\u0435."],["03","\u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u0442\u0435","\u0413\u0430\u0440\u0430\u043d\u0442 \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u0438\u0440\u0443\u0435\u0442 \u043e\u043f\u043b\u0430\u0442\u0443 \u0438 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0438\u0435."]].map(([id,title,text]) => <article key={id}><i>{id}</i><div><b>{title}</b><p>{text}</p></div></article>)}</div><button className="pro-primary" onClick={() => onNavigate("catalog")}>\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u0432 \u043a\u0430\u0442\u0430\u043b\u043e\u0433 \u2192</button></section>; }

export default function ProductApp() {
  const telegramUser = tg()?.initDataUnsafe?.user;
  const [route, setRoute] = useState("home");
  const [theme, setTheme] = useState(() => localStorage.getItem("lteam-theme") || "dark");
  const [profile, setProfile] = useState({ name: [telegramUser?.first_name, telegramUser?.last_name].filter(Boolean).join(" ") || "\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c", username: telegramUser?.username ? "@" + telegramUser.username : "LTeam user" });
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
  else if (route === "orders") content = <OrdersWorkspace orders={orders} deals={deals} profile={profile} fetchData={call} request={request} onNavigate={setRoute} onChat={() => setNotice("\u0427\u0430\u0442\u044b \u043e\u0442\u043a\u0440\u044b\u0432\u0430\u044e\u0442\u0441\u044f \u0438\u0437 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438 \u0441\u0434\u0435\u043b\u043a\u0438.")} onDealsChanged={setDeals} />;
  else if (route === "create-order") content = <OrderComposer initial={{ title: "", category: "\u0420\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u043a\u0430", budget: "", deadline: "\u041f\u043e \u0434\u043e\u0433\u043e\u0432\u043e\u0440\u0451\u043d\u043d\u043e\u0441\u0442\u0438", description: "" }} onClose={() => setRoute("orders")} message={notice} onSubmit={async (form, reset) => { try { await request("/api/orders", "POST", form); reset(); setNotice("\u0417\u0430\u043a\u0430\u0437 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d \u043d\u0430 \u043c\u043e\u0434\u0435\u0440\u0430\u0446\u0438\u044e."); refresh(); } catch (error) { setNotice(error.message); } }} />;
  else if (route === "profile") content = <ProfileWorkspace profile={profile} listings={listings} orders={orders} deals={deals} balance={balance} isSynced isAdmin={isAdmin} onNavigate={setRoute} onSettings={() => setSettings(true)} />;
  else if (route === "wallet") content = <WalletWorkspace balance={balance} fetchData={call} onNavigate={setRoute} onWithdraw={() => tg()?.sendData?.("withdraw_start")} />;
  else if (route === "my-listings") content = <MyListingsWorkspace fetchData={call} request={request} onNavigate={setRoute} onOpenDeal={() => { refresh(); setRoute("orders"); }} />;
  else if (route === "favorites") content = <FavoritesWorkspace items={listings} favorites={favorites} onToggle={toggleFavorite} onNavigate={setRoute} />;
  else if (route === "support") content = <SupportWorkspace fetchData={call} request={request} onNavigate={setRoute} />;
  else if (route === "admin" && isAdmin) content = <AdminWorkspace summary={adminSummary} fetchData={call} request={request} onNavigate={setRoute} onOpenBot={(section) => tg()?.sendData?.(section || "admin_panel")} />;
  else content = <Home profile={profile} listings={listings} orders={orders} deals={deals} onNavigate={setRoute} isAdmin={isAdmin} />;
  return <main className="product-app">{content}{route !== "guide" && <ProductNav active={route} onNavigate={setRoute} />}{settings && <SettingsWorkspace theme={theme} setTheme={setTheme} onClose={() => setSettings(false)} />}{notice && <button className="pro-notice" onClick={() => setNotice("")}>{notice}<i>\u00d7</i></button>}</main>;
}
