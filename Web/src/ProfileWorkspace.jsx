function initials(value) {
  return String(value || "L").trim().slice(0, 1).toUpperCase();
}

function Row({ icon, title, description, badge, onClick }) {
  return <button className="account-row" onClick={onClick}>
    <span className="account-row-icon">{icon}</span>
    <span className="account-row-copy"><b>{title}</b><small>{description}</small></span>
    {badge && <span className="account-row-badge">{badge}</span>}
    <i>›</i>
  </button>;
}

export default function ProfileWorkspace({ profile = {}, listings = [], orders = [], deals = [], isSynced, isAdmin, onNavigate, onSettings }) {
  const ownListings = listings.filter((item) => Number(item.seller_id) === Number(profile.id));
  const activeDeals = deals.filter((deal) => !["completed", "cancelled"].includes(deal.status)).length;
  return <section className="account-page">
    <header className="account-head"><button className="account-mark" onClick={() => onNavigate("home")}>LT</button><div><span>ЛИЧНЫЙ КАБИНЕТ</span><h1>Профиль</h1></div><button className="account-settings" onClick={onSettings} aria-label="Настройки">⚙</button></header>
    <section className="account-hero"><div className="account-avatar">{initials(profile.name)}</div><div className="account-identity"><div><h2>{profile.name || "Пользователь LTeam"}</h2><span>{profile.username || "LTeam user"}</span></div><small className={isSynced ? "online" : ""}><i />{isSynced ? "Профиль синхронизирован" : "Подключите Telegram"}</small></div><button onClick={onSettings}>Изменить</button></section>
    <section className="account-stats"><div><b>{ownListings.length}</b><span>Услуг</span></div><div><b>{orders.length}</b><span>Заказов</span></div><div><b>{activeDeals}</b><span>В работе</span></div></section>
    <section className="account-section"><p>МОЯ РАБОТА</p><div className="account-list"><Row icon="▦" title="Мои услуги" description="Витрина, модерация и заявки" badge={ownListings.length || null} onClick={() => onNavigate("my-listings")} /><Row icon="▤" title="Заказы и сделки" description="Отклики, договорённости и чат" badge={activeDeals || null} onClick={() => onNavigate("orders")} /><Row icon="♡" title="Избранное" description="Сохранённые услуги исполнителей" onClick={() => onNavigate("favorites")} /></div></section>
    <section className="account-section"><p>ПОДДЕРЖКА</p><div className="account-list"><Row icon="?" title="Поддержка LTeam" description="Вопросы, обращения и спорные ситуации" onClick={() => onNavigate("support")} /><Row icon="i" title="Как это работает" description="Краткий гид по заказам и услугам" onClick={() => onNavigate("guide")} /></div></section>
    {isAdmin && <button className="account-admin" onClick={() => onNavigate("admin")}><span>◆</span><div><small>УПРАВЛЕНИЕ ПЛАТФОРМОЙ</small><b>Админ-центр LTeam</b></div><i>→</i></button>}
  </section>;
}
