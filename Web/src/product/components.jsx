import Icon from "./icons";
import { initials, money } from "./api";

export function Brand({ compact = false }) {
  return <span className={`lt-brand ${compact ? "compact" : ""}`}><span className="lt-logo" aria-hidden="true"><i>L</i><i>T</i></span>{!compact && <span><b>LT Market</b><small>digital services</small></span>}</span>;
}

export function Avatar({ src, name, size = "md", verified = false }) {
  return <span className={`lt-avatar ${size}`} aria-label={name || "Пользователь"}>{src ? <img src={src} alt="" onError={(event) => { event.currentTarget.hidden = true; }} /> : null}<span>{initials(name)}</span>{verified && <i><Icon name="check" size={10} strokeWidth={3}/></i>}</span>;
}

export function Rating({ rating = 0, count = 0, compact = false }) {
  const value = Number(rating || 0);
  const tone = !count ? "new" : value >= 4.8 ? "great" : value >= 4 ? "good" : "low";
  return <span className={`lt-rating ${tone} ${compact ? "compact" : ""}`}><Icon name="star" size={15}/><b>{count ? value.toFixed(1) : "Новый"}</b>{!compact && <small>{count ? `${count} ${count === 1 ? "отзыв" : "отзывов"}` : "без отзывов"}</small>}</span>;
}

export function PageHeader({ title, eyebrow, onBack, action, children }) {
  return <header className={`page-header ${onBack ? "has-back" : "root-header"}`}>{onBack && <button className="icon-button" onClick={onBack} aria-label="Назад"><Icon name="back"/></button>}<div><small>{eyebrow}</small><h1>{title}</h1></div>{action || <span/>}{children}</header>;
}

export function EmptyState({ icon = "spark", title, text, action, onAction }) {
  return <section className="empty-state"><i><Icon name={icon} size={28}/></i><h3>{title}</h3><p>{text}</p>{action && <button className="secondary-button" onClick={onAction}>{action}</button>}</section>;
}

export function Loading({ label = "Загружаем LT Market" }) {
  return <section className="app-loading"><span className="loading-mark"><i/><i/><i/></span><b>{label}</b></section>;
}

export function BottomNav({ active, onNavigate, onCreate }) {
  const items = [["home", "Главная", "home"], ["catalog", "Каталог", "grid"], ["create", "Создать", "plus"], ["orders", "Заказы", "briefcase"], ["profile", "Профиль", "user"]];
  const activeIndex = Math.max(0, items.findIndex(([id]) => id === active));
  return <nav className={`bottom-nav ${active === "create" ? "nav-create-active" : ""}`} aria-label="Основная навигация" style={{ "--nav-index": activeIndex }}><i className="nav-indicator" aria-hidden="true"/>{items.map(([id, label, icon]) => <button type="button" key={id} aria-current={active === id ? "page" : undefined} className={`${active === id ? "active" : ""} ${id === "create" ? "create" : ""}`} onClick={(event) => id === "create" ? onCreate("", event.currentTarget.getBoundingClientRect()) : onNavigate(id)}><i><Icon name={icon} size={id === "create" ? 24 : 20}/></i><span>{label}</span></button>)}</nav>;
}

export function ServiceCard({ item, onOpen, onFavorite, dense = false }) {
  return <article className={`service-card ${dense ? "dense" : ""}`} onClick={onOpen} tabIndex={0} role="button">
    <div className="service-cover">{item.image_data ? <img src={item.image_data} alt=""/> : <span><Brand compact/></span>}<button className={`favorite-button ${item.is_favorite ? "active" : ""}`} onClick={(event) => { event.stopPropagation(); onFavorite?.(item); }} aria-label="Добавить в избранное"><Icon name="heart" size={19}/></button></div>
    <div className="service-body"><div className="seller-line"><Avatar src={item.avatar_url} name={item.seller_name || item.seller_username} size="sm" verified={Boolean(item.seller_verified)}/><div><b>{item.seller_name || item.seller_username || "Исполнитель LT"}</b><Rating rating={item.avg_rating} count={item.reviews_count} compact/></div></div><h3>{item.title}</h3><div className="service-tags"><span>{item.category}</span><span><Icon name="clock" size={14}/>{item.delivery_time || "По договорённости"}</span></div><footer><strong>{money(item.price, "от ")}</strong><span>Подробнее <Icon name="chevron" size={16}/></span></footer></div>
  </article>;
}

export function OrderCard({ item, onOpen }) {
  return <article className="order-card" onClick={onOpen} tabIndex={0} role="button"><header><span>{item.category}</span><strong>{money(item.budget, "до ")}</strong></header><h3>{item.title}</h3><p>{item.description}</p><div className="order-author"><Avatar src={item.customer_avatar_url} name={item.customer_name || item.customer_username} size="sm"/><div><b>{item.customer_name || item.customer_username || "Заказчик LT"}</b><small>{item.deadline || "Срок обсуждается"}</small></div><Icon name="chevron" size={18}/></div></article>;
}

export function Sheet({ open, title, onClose, children, className = "" }) {
  if (!open) return null;
  return <div className="sheet-backdrop" onMouseDown={onClose}><section className={`sheet ${className}`} onMouseDown={(event) => event.stopPropagation()}><div className="sheet-handle"/><header><h2>{title}</h2><button className="icon-button" onClick={onClose} aria-label="Закрыть"><Icon name="close"/></button></header>{children}</section></div>;
}

export function Toast({ message, tone = "default", onClose }) {
  if (!message) return null;
  return <button className={`toast ${tone}`} onClick={onClose}><i><Icon name={tone === "error" ? "alert" : "check"} size={18}/></i><span>{message}</span><Icon name="close" size={16}/></button>;
}

export function Price({ value, from = false }) { return <strong>{money(value, from ? "от " : "")}</strong>; }
