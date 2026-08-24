import { useCallback, useEffect, useRef, useState } from "react";
import HomeScreen from "./product/screens/HomeScreen";
import CatalogScreen, { ListingDetail, OrderDetail } from "./product/screens/CatalogScreen";
import CreateScreen from "./product/screens/CreateScreen";
import OrdersScreen, { DealWorkspace } from "./product/screens/OrdersScreen";
import AdminScreen from "./product/screens/AdminScreen";
import PromotionsScreen from "./product/screens/PromotionsScreen";
import { GuideScreen, LegalScreen, NotificationsScreen, ProfileEditSheet, ProfileScreen, SellerProfileScreen, SupportScreen } from "./product/screens/AccountScreens";
import SettingsSheet from "./product/screens/SettingsSheet";
import CatalogLaunch from "./product/components/CatalogLaunch";
import Icon from "./product/icons";
import { BottomNav, Brand, EmptyState, Loading, ServiceCard, Sheet, Toast } from "./product/components";
import { FALLBACK_CATEGORIES } from "./product/constants";
import { api, haptic, send, telegram } from "./product/api";
import { preview } from "./product/preview";
import "./product-ui.css";

const initialTelegramUser = telegram()?.initDataUnsafe?.user || {};
const initialMe = {
  authenticated: false,
  id: initialTelegramUser.id || 0,
  name: [initialTelegramUser.first_name, initialTelegramUser.last_name].filter(Boolean).join(" ") || "Пользователь LT",
  username: initialTelegramUser.username || "",
  photo_url: initialTelegramUser.photo_url || "",
  is_admin: false,
  role: "both",
  unread_notifications: 0,
};

export default function ProductApp() {
  const [route, setRoute] = useState({ name: preview?.route || "home", params: {} });
  const [, setStack] = useState([]);
  const [me, setMe] = useState(preview?.me || initialMe);
  const [config, setConfig] = useState(preview?.config || { categories: FALLBACK_CATEGORIES, payments_enabled: false, beta: true });
  const [listings, setListings] = useState(preview?.listings || []);
  const [orders, setOrders] = useState(preview?.orders || []);
  const [deals, setDeals] = useState(preview?.deals || []);
  const [notifications, setNotifications] = useState(preview?.notifications || []);
  const [preferences, setPreferences] = useState(preview?.preferences || { role: "both", theme: "system", notifications: { messages: true, orders: true, recommendations: true }, display: { animations: true, haptics: true, compact_cards: false, language: "ru", accent: "violet" } });
  const [loading, setLoading] = useState(!preview);
  const [bootError, setBootError] = useState("");
  const [toast, setToast] = useState({ message: "", tone: "default" });
  const [settingsOpen, setSettingsOpen] = useState(() => Boolean(preview && new URLSearchParams(window.location.search).get("open") === "settings"));
  const [profileEditOpen, setProfileEditOpen] = useState(false);
  const [transition, setTransition] = useState("forward");
  const [requestSheet, setRequestSheet] = useState(null);
  const [detail, setDetail] = useState(null);
  const [seller, setSeller] = useState(null);
  const [sellerReviews, setSellerReviews] = useState([]);
  const [catalogLaunch, setCatalogLaunch] = useState(null);
  const catalogLaunchTimers = useRef([]);

  const notify = useCallback((message, tone = "default") => {
    setToast({ message, tone });
    window.clearTimeout(window.__ltToastTimer);
    window.__ltToastTimer = window.setTimeout(() => setToast({ message: "", tone: "default" }), 3600);
  }, []);

  const refresh = useCallback(async () => {
    const nextMe = await api("/api/me");
    setBootError("");
    setMe((current) => ({ ...current, ...nextMe }));
    if (!nextMe.authenticated) { setLoading(false); return false; }
    const [nextConfig, nextListings, nextOrders, nextDeals, nextNotifications, nextPreferences] = await Promise.all([
      api("/api/market/config"), api("/api/listings"), api("/api/orders"), api("/api/deals"), api("/api/notifications"), api("/api/preferences"),
    ]);
    setConfig(nextConfig); setListings(nextListings); setOrders(nextOrders); setDeals(nextDeals); setNotifications(nextNotifications); setPreferences(nextPreferences);
    setMe((current) => ({ ...current, unread_notifications: nextNotifications.filter((item) => !item.is_read).length, role: nextPreferences.role }));
    setLoading(false);
    return true;
  }, []);

  useEffect(() => {
    if (preview) return;
    const webApp = telegram();
    webApp?.ready?.(); webApp?.expand?.();
    webApp?.setHeaderColor?.("bg_color"); webApp?.setBackgroundColor?.("bg_color"); webApp?.setBottomBarColor?.("bg_color");
    refresh().catch((error) => { setLoading(false); setBootError(error.message || "Сервис временно недоступен"); });
  }, [refresh, notify]);

  useEffect(() => {
    const webApp = telegram();
    const applyTheme = () => {
      const selected = preferences.theme || "system";
      const systemTheme = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
      const effective = selected === "system" ? (preview ? systemTheme : (webApp?.colorScheme || systemTheme)) : selected;
      document.documentElement.dataset.theme = effective;
      document.documentElement.style.colorScheme = effective;
      const chrome = effective === "dark"
        ? { header: "#0b0f1a", background: "#0b0f1a", bottom: "#101522" }
        : { header: "#f5f6fb", background: "#f5f6fb", bottom: "#ffffff" };
      try {
        webApp?.setHeaderColor?.(chrome.header);
        webApp?.setBackgroundColor?.(chrome.background);
        webApp?.setBottomBarColor?.(chrome.bottom);
      } catch { /* Older Telegram clients keep their native chrome colors. */ }
      document.querySelector('meta[name="theme-color"]')?.setAttribute("content", chrome.header);
    };
    applyTheme(); webApp?.onEvent?.("themeChanged", applyTheme);
    return () => webApp?.offEvent?.("themeChanged", applyTheme);
  }, [preferences.theme]);

  useEffect(() => {
    const root = document.documentElement;
    root.dataset.motion = preferences.display?.animations === false ? "reduced" : "full";
    root.dataset.density = preferences.display?.compact_cards ? "compact" : "comfortable";
    root.dataset.accent = preferences.display?.accent || "violet";
  }, [preferences.display]);

  useEffect(() => {
    const webApp = telegram();
    const applyInsets = () => {
      const safe = webApp?.safeAreaInset || {};
      const content = webApp?.contentSafeAreaInset || {};
      const root = document.documentElement;
      root.style.setProperty("--tg-safe-top", `${Math.max(Number(safe.top || 0), Number(content.top || 0))}px`);
      root.style.setProperty("--tg-safe-bottom", `${Math.max(Number(safe.bottom || 0), Number(content.bottom || 0))}px`);
    };
    applyInsets();
    webApp?.onEvent?.("safeAreaChanged", applyInsets);
    webApp?.onEvent?.("contentSafeAreaChanged", applyInsets);
    return () => { webApp?.offEvent?.("safeAreaChanged", applyInsets); webApp?.offEvent?.("contentSafeAreaChanged", applyInsets); };
  }, []);

  const navigate = useCallback((name, params = {}, replace = false) => {
    haptic("selection", preferences.display?.haptics !== false);
    setTransition("forward");
    setRoute((current) => {
      const roots = ["home", "catalog", "create", "orders", "profile"];
      if (!replace && current.name !== name && !(roots.includes(current.name) && roots.includes(name))) setStack((history) => [...history.slice(-9), current]);
      return { name, params };
    });
    window.scrollTo({ top: 0, behavior: "instant" });
  }, [preferences.display?.haptics]);

  const launchCatalog = useCallback((bounds) => {
    const reduced = preferences.display?.animations === false || window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (reduced) { navigate("catalog", { focus: true }); return; }
    catalogLaunchTimers.current.forEach((timer) => window.clearTimeout(timer));
    const appRect = document.querySelector(".market-app")?.getBoundingClientRect() || { left: 0 };
    const rect = { top: bounds.top, left: bounds.left - appRect.left, width: bounds.width, height: bounds.height };
    haptic("medium", preferences.display?.haptics !== false);
    setCatalogLaunch({ phase: "expand", rect });
    catalogLaunchTimers.current = [
      window.setTimeout(() => {
        navigate("catalog", { focus: true, focusDelay: 820, entrance: "search" });
        setCatalogLaunch({ phase: "ready", rect });
        haptic("success", preferences.display?.haptics !== false);
      }, 410),
      window.setTimeout(() => setCatalogLaunch({ phase: "exit", rect }), 820),
      window.setTimeout(() => setCatalogLaunch(null), 1120),
    ];
  }, [navigate, preferences.display?.animations, preferences.display?.haptics]);

  useEffect(() => {
    if (!preview || route.name !== "home" || new URLSearchParams(window.location.search).get("open") !== "search") return undefined;
    const timer = window.setTimeout(() => {
      const search = document.querySelector(".global-search");
      if (search) launchCatalog(search.getBoundingClientRect());
    }, 120);
    return () => window.clearTimeout(timer);
  }, [launchCatalog, route.name]);

  useEffect(() => {
    if (!preview || new URLSearchParams(window.location.search).get("open") !== "select") return undefined;
    const timer = window.setTimeout(() => document.querySelector(".mobile-select-trigger")?.click(), 160);
    return () => window.clearTimeout(timer);
  }, [route.name]);

  useEffect(() => () => catalogLaunchTimers.current.forEach((timer) => window.clearTimeout(timer)), []);

  const openCreate = useCallback((type = "") => {
    const safeType = ["listing", "order"].includes(type) ? type : "";
    navigate("create", safeType ? { type: safeType } : {});
  }, [navigate]);

  const goBack = useCallback(() => {
    haptic("selection", preferences.display?.haptics !== false);
    setTransition("back");
    setStack((history) => {
      const previous = history.at(-1) || { name: "home", params: {} };
      setRoute(previous);
      return history.slice(0, -1);
    });
    window.scrollTo({ top: 0, behavior: "instant" });
  }, [preferences.display?.haptics]);

  useEffect(() => {
    const back = telegram()?.BackButton;
    if (!back) return;
    if (["home", "catalog", "orders", "profile"].includes(route.name)) back.hide(); else back.show();
    back.onClick(goBack);
    return () => back.offClick(goBack);
  }, [route.name, goBack]);

  useEffect(() => {
    if (route.name !== "listing") { setDetail(null); return; }
    api(`/api/listings/${route.params.id}`).then(setDetail).catch((error) => { notify(error.message, "error"); goBack(); });
  }, [route.name, route.params.id, notify, goBack]);

  useEffect(() => {
    if (route.name !== "seller") { setSeller(null); setSellerReviews([]); return; }
    Promise.all([api(`/api/users/${route.params.id}/public`), api(`/api/users/${route.params.id}/reviews`)]).then(([nextSeller, nextReviews]) => { setSeller(nextSeller); setSellerReviews(nextReviews); }).catch((error) => { notify(error.message, "error"); goBack(); });
  }, [route.name, route.params.id, notify, goBack]);

  async function toggleFavorite(item) {
    const favorite = !item.is_favorite;
    setListings((current) => current.map((entry) => entry.id === item.id ? { ...entry, is_favorite: favorite } : entry));
    if (detail?.id === item.id) setDetail((current) => ({ ...current, is_favorite: favorite }));
    try { await send("/api/favorites", favorite ? "POST" : "DELETE", { listing_id: item.id }); }
    catch (error) { notify(error.message, "error"); setListings((current) => current.map((entry) => entry.id === item.id ? { ...entry, is_favorite: !favorite } : entry)); }
  }

  async function savePreferences(next) {
    try { const saved = await send("/api/preferences", "PUT", next); setPreferences(saved); setMe((current) => ({ ...current, role: saved.role })); setSettingsOpen(false); haptic("success", saved.display?.haptics !== false); notify("Настройки сохранены"); }
    catch (error) { notify(error.message, "error"); }
  }

  async function saveProfile(next) {
    try {
      const saved = await send("/api/profile", "PUT", next);
      setMe((current) => ({ ...current, name: saved.display_name, bio: saved.bio, skills: saved.skills }));
      setProfileEditOpen(false);
      haptic("success", preferences.display?.haptics !== false);
      notify("Профиль обновлён");
    } catch (error) { notify(error.message, "error"); }
  }

  async function clearRecent() {
    try { await send("/api/recently-viewed", "DELETE", {}); notify("История просмотров очищена"); }
    catch (error) { notify(error.message, "error"); }
  }

  const readAllNotifications = useCallback(async () => {
    setNotifications((current) => current.map((item) => ({ ...item, is_read: 1 })));
    setMe((current) => ({ ...current, unread_notifications: 0 }));
    await send("/api/notifications/read", "POST", {}).catch(() => {});
  }, []);

  async function openNotification(item) {
    if (!item.is_read) {
      setNotifications((current) => current.map((entry) => entry.id === item.id ? { ...entry, is_read: 1 } : entry));
      setMe((current) => ({ ...current, unread_notifications: Math.max(0, Number(current.unread_notifications || 0) - 1) }));
      await send("/api/notifications/read", "POST", { ids: [item.id] }).catch(() => {});
    }
    const target = String(item.route || "notifications");
    if (target.startsWith("deal:")) navigate("deal", { id: Number(target.split(":")[1]) });
    else if (target.startsWith("listing:")) navigate("listing", { id: Number(target.split(":")[1]) });
    else if (target.startsWith("order:")) navigate("order", { id: Number(target.split(":")[1]) });
    else if (target.startsWith("profile:") || target.startsWith("seller:")) navigate("seller", { id: Number(target.split(":")[1]) });
    else if (["home", "catalog", "create", "orders", "profile", "support", "admin"].includes(target)) navigate(target);
    else navigate("orders");
  }

  async function retryBoot() {
    setBootError("");
    setLoading(true);
    try { await refresh(); }
    catch (error) { setLoading(false); setBootError(error.message || "Сервис временно недоступен"); }
  }

  if (loading) return <main className="market-app"><Loading/></main>;
  if (bootError) return <main className="market-app"><section className="telegram-gate connection-state"><Brand/><div className="connection-orbit"><i/><span><Icon name="shield" size={32}/></span><i/></div><small>СОЕДИНЕНИЕ С LT MARKET</small><h1>Не удалось загрузить данные</h1><p>{bootError}. На бесплатном сервере первый запуск иногда занимает до минуты.</p><button className="primary-button" onClick={retryBoot}>Попробовать снова <Icon name="arrow"/></button></section></main>;
  if (!me.authenticated) return <main className="market-app"><section className="telegram-gate"><Brand/><div className="gate-visual"><span><Icon name="shield" size={34}/></span><i/><i/></div><small>TELEGRAM MINI APP</small><h1>Откройте LT Market<br/>в Telegram</h1><p>Профиль, публикации и заказы доступны после безопасной авторизации через бота.</p><a className="primary-button" href="https://t.me/lteam_marketbot?startapp=market">Открыть в Telegram <Icon name="arrow"/></a></section></main>;

  const categories = config.categories?.length ? config.categories : FALLBACK_CATEGORIES;
  const selectedDeal = route.name === "deal" ? deals.find((item) => Number(item.id) === Number(route.params.id)) : null;
  const selectedOrder = route.name === "order" ? orders.find((item) => Number(item.id) === Number(route.params.id)) : null;
  const favorites = listings.filter((item) => item.is_favorite);
  let content;
  if (route.name === "home") content = <HomeScreen me={me} categories={categories} listings={listings} orders={orders} onNavigate={navigate} onSearchLaunch={launchCatalog} onCreate={openCreate} onFavorite={toggleFavorite}/>;
  else if (route.name === "catalog") content = <CatalogScreen listings={listings} orders={orders} categories={categories} initial={route.params} onNavigate={navigate} onFavorite={toggleFavorite}/>;
  else if (route.name === "listing") content = <ListingDetail item={detail} loading={!detail} onBack={goBack} onSeller={() => navigate("seller", { id: detail.seller_id })} onFavorite={toggleFavorite} onRequest={(selectedPackage) => setRequestSheet({ type: "listing", item: detail, selectedPackage })}/>;
  else if (route.name === "order") content = <OrderDetail item={selectedOrder} onBack={goBack} onApply={() => setRequestSheet({ type: "order", item: selectedOrder })}/>;
  else if (route.name === "create") content = <CreateScreen initialType={route.params.type || ""} categories={categories} onBack={goBack} notify={notify} onDone={async () => { await refresh(); navigate("orders", { tab: "published" }, true); }}/>;
  else if (route.name === "orders") content = <OrdersScreen me={me} deals={deals} orders={orders} onNavigate={navigate} notify={notify} refresh={refresh}/>;
  else if (route.name === "deal") content = selectedDeal ? <DealWorkspace me={me} deal={selectedDeal} onBack={goBack} notify={notify} onRefresh={refresh}/> : <section className="screen"><Loading label="Открываем заказ"/></section>;
  else if (route.name === "profile") content = <ProfileScreen me={me} listings={listings} orders={orders} deals={deals} onNavigate={navigate} onSettings={() => setSettingsOpen(true)} onEdit={() => setProfileEditOpen(true)}/>;
  else if (route.name === "promotions") content = <PromotionsScreen me={me} listings={listings} products={config.star_products || []} initialListingId={route.params.listing_id} onBack={goBack} notify={notify} onUpdated={refresh}/>;
  else if (route.name === "seller") content = <SellerProfileScreen profile={seller} reviews={sellerReviews} onBack={goBack} onListing={(id) => navigate("listing", { id })} onFavorite={toggleFavorite}/>;
  else if (route.name === "notifications") content = <NotificationsScreen items={notifications} onBack={goBack} onOpen={openNotification} onReadAll={readAllNotifications}/>;
  else if (route.name === "favorites") content = <section className="screen favorites-screen"><header className="simple-head"><button onClick={goBack}><Icon name="back"/></button><div><small>СОХРАНЁННОЕ</small><h1>Избранное</h1></div></header>{favorites.length ? <div className="catalog-grid">{favorites.map((item) => <ServiceCard key={item.id} item={item} onOpen={() => navigate("listing", { id: item.id })} onFavorite={toggleFavorite}/>)}</div> : <EmptyState icon="heart" title="В избранном пусто" text="Сохраняйте интересные услуги, чтобы быстро вернуться к ним." action="Открыть каталог" onAction={() => navigate("catalog")}/>}</section>;
  else if (route.name === "guide") content = <GuideScreen onBack={goBack} onCatalog={() => navigate("catalog")}/>;
  else if (route.name === "legal") content = <LegalScreen onBack={goBack} onSupport={() => navigate("support")}/>;
  else if (route.name === "support") content = <SupportScreen onBack={goBack} notify={notify}/>;
  else if (route.name === "admin" && me.is_admin) content = <AdminScreen onBack={goBack} notify={notify}/>;
  else content = <HomeScreen me={me} categories={categories} listings={listings} orders={orders} onNavigate={navigate} onSearchLaunch={launchCatalog} onCreate={openCreate} onFavorite={toggleFavorite}/>;

  const navActive = route.name === "listing" || route.name === "order" ? "catalog" : route.name === "deal" ? "orders" : route.name === "seller" ? "profile" : route.name;
  const showNav = ["home", "catalog", "create", "orders", "profile"].includes(route.name);
  return <main className="market-app"><div key={`${route.name}-${JSON.stringify(route.params)}`} className={`route-stage route-${transition}`}>{content}</div>{showNav && <BottomNav active={navActive} onNavigate={navigate} onCreate={openCreate}/>}<CatalogLaunch launch={catalogLaunch}/><RequestSheet data={requestSheet} onClose={() => setRequestSheet(null)} notify={notify} onDone={async () => { setRequestSheet(null); await refresh(); navigate("orders"); }}/><SettingsSheet open={settingsOpen} preferences={preferences} onClose={() => setSettingsOpen(false)} onSave={savePreferences} onClearRecent={clearRecent}/><ProfileEditSheet open={profileEditOpen} me={me} onClose={() => setProfileEditOpen(false)} onSave={saveProfile}/><Toast message={toast.message} tone={toast.tone} onClose={() => setToast({ message: "", tone: "default" })}/></main>;
}

function RequestSheet({ data, onClose, notify, onDone }) {
  const [form, setForm] = useState({ message: "", price: "", deadline: "По договорённости" });
  useEffect(() => { if (data) setForm({ message: data.type === "listing" ? `Здравствуйте! Хочу обсудить тариф «${data.selectedPackage?.title || "Базовый"}». ` : "", price: data.item?.budget || "", deadline: data.item?.deadline || "По договорённости" }); }, [data]);
  if (!data) return null;
  const submit = async () => { try { if (data.type === "listing") await send(`/api/listings/${data.item.id}/requests`, "POST", { message: form.message, package_key: data.selectedPackage?.package_key }); else await send(`/api/orders/${data.item.id}/applications`, "POST", { comment: form.message, price: Number(form.price), deadline: form.deadline }); notify(data.type === "listing" ? "Запрос отправлен исполнителю" : "Отклик отправлен заказчику"); onDone(); } catch (error) { notify(error.message, "error"); } };
  return <Sheet open title={data.type === "listing" ? "Обсудить услугу" : "Откликнуться на задачу"} onClose={onClose}><div className="request-form">{data.type === "order" && <div className="request-conditions"><label className="field"><span>Ваша цена, ₽</span><input type="number" value={form.price} onChange={(event) => setForm({ ...form, price: event.target.value })}/></label><label className="field"><span>Срок</span><input value={form.deadline} onChange={(event) => setForm({ ...form, deadline: event.target.value })}/></label></div>}<label className="field"><span>{data.type === "listing" ? "Опишите задачу" : "Ваше предложение"}</span><textarea value={form.message} minLength={10} onChange={(event) => setForm({ ...form, message: event.target.value })} placeholder="Коротко расскажите о задаче, опыте и важных деталях"/></label><p><Icon name="shield" size={17}/> Контакты и условия лучше обсуждать внутри заказа — так история сохранится.</p><button className="primary-button wide" disabled={form.message.trim().length < 10 || (data.type === "order" && !Number(form.price))} onClick={submit}>{data.type === "listing" ? "Отправить запрос" : "Отправить отклик"}</button></div></Sheet>;
}
