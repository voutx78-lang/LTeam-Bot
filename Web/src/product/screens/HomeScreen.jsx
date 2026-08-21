import Icon from "../icons";
import { Avatar, Brand, EmptyState, OrderCard, ServiceCard } from "../components";
import { CATEGORY_META } from "../constants";

export default function HomeScreen({ me, categories, listings, orders, onNavigate, onCreate, onFavorite }) {
  const recommended = listings.slice(0, 4);
  const urgentOrders = orders.filter((item) => ["active", "open", "approved"].includes(item.status)).slice(0, 3);
  return <section className="screen home-screen">
    <header className="home-header"><button onClick={() => onNavigate("home")}><Brand/></button><div><button className="notification-button" onClick={() => onNavigate("notifications")} aria-label="Уведомления"><Icon name="bell"/>{me.unread_notifications > 0 && <i>{Math.min(99, me.unread_notifications)}</i>}</button><button onClick={() => onNavigate("profile")}><Avatar src={me.photo_url} name={me.name} size="md"/></button></div></header>
    <button className="global-search" onClick={() => onNavigate("catalog", { focus: true })}><Icon name="search"/><span>Услуга, специалист или задача</span><kbd>Найти</kbd></button>

    <section className="home-intro"><div><small>ЦИФРОВЫЕ ЗАДАЧИ В TELEGRAM</small><h1>От идеи<br/>до результата.</h1><p>Исполнители, понятные условия и вся работа в одном пространстве.</p><div><button className="primary-button" onClick={() => onNavigate("catalog")}>Найти исполнителя <Icon name="arrow"/></button><button className="ghost-button" onClick={() => onCreate("order")}>Разместить задачу</button></div></div><span className="intro-visual" aria-hidden="true"><i/><i/><i/><b>LT</b></span></section>

    <section className="home-block"><header className="section-heading"><div><small>НАПРАВЛЕНИЯ</small><h2>Что нужно сделать?</h2></div><button onClick={() => onNavigate("catalog")}>Все</button></header><div className="category-rail">{categories.map((category) => { const meta = CATEGORY_META[category] || { short: category, icon: "grid", className: "other" }; return <button key={category} className={meta.className} onClick={() => onNavigate("catalog", { category })}><i><Icon name={meta.icon}/></i><span>{meta.short}</span><small>Открыть</small></button>; })}</div></section>

    <section className="home-block"><header className="section-heading"><div><small>ПОДБОРКА ДЛЯ ВАС</small><h2>Новые услуги</h2></div><button onClick={() => onNavigate("catalog", { type: "services" })}>Смотреть все</button></header>{recommended.length ? <div className="service-rail">{recommended.map((item) => <ServiceCard key={item.id} item={item} dense onOpen={() => onNavigate("listing", { id: item.id })} onFavorite={onFavorite}/>)}</div> : <EmptyState title="Каталог наполняется" text="Опишите задачу — исполнители смогут предложить решение." action="Создать заказ" onAction={() => onCreate("order")}/>}</section>

    <section className="home-block"><header className="section-heading"><div><small>ДЛЯ ИСПОЛНИТЕЛЕЙ</small><h2>Свежие задачи</h2></div><button onClick={() => onNavigate("catalog", { type: "orders" })}>Все задачи</button></header>{urgentOrders.length ? <div className="order-list">{urgentOrders.map((item) => <OrderCard key={item.id} item={item} onOpen={() => onNavigate("order", { id: item.id })}/>)}</div> : <p className="muted-panel">Новые задачи появятся здесь после модерации.</p>}</section>

    <section className="trust-strip"><div><Icon name="shield"/><span><b>Прозрачные условия</b><small>Цена, срок и результат сохраняются в заказе</small></span></div><button onClick={() => onNavigate("guide")}>Как это работает <Icon name="chevron" size={17}/></button></section>
    <footer className="early-access">Ранний доступ · LT Market пока не принимает и не хранит оплату за заказы.</footer>
  </section>;
}
