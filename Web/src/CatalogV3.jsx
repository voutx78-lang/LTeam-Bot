import { useMemo, useState } from "react";

const CATEGORIES = ["Все", "Дизайн", "Разработка", "Тексты", "Монтаж", "Другое"];
const SORTS = [
  ["new", "Сначала новые"],
  ["rating", "По рейтингу"],
  ["low", "Сначала дешевле"],
  ["high", "Сначала дороже"],
];

const price = (value, prefix = "от ") => `${prefix}${Number(value || 0).toLocaleString("ru-RU")} ₽`;
const sellerName = (item) => item?.seller_name || item?.seller_username || "Исполнитель LTeam";
const sellerNick = (item) => item?.seller_username || "lteam_user";

function Rating({ rating, count }) {
  const value = Number(rating || 0);
  const reviews = Number(count || 0);
  const tone = !reviews ? "neutral" : value < 3.5 ? "low" : value < 4.9 ? "mid" : "high";
  return (
    <span className={`market-rating ${tone}`}>
      <span aria-hidden="true">★</span>
      {reviews ? `${value.toFixed(1)} · ${reviews} ${reviews === 1 ? "отзыв" : "отзывов"}` : "Нет отзывов"}
    </span>
  );
}

function Avatar({ name, image }) {
  return <span className="market-avatar">{image ? <img src={image} alt="" /> : (name || "L").slice(0, 1).toUpperCase()}</span>;
}

function Picker({ label, value, options, onChange, open, setOpen }) {
  const current = options.find((option) => Array.isArray(option) ? option[0] === value : option === value);
  const title = Array.isArray(current) ? current[1] : current;
  return <div className="market-picker">
    <button type="button" className="market-picker-trigger" onClick={() => setOpen(!open)} aria-expanded={open}>
      <span>{title}</span><span className="market-chevron">⌄</span>
    </button>
    {open && <div className="market-picker-menu" role="menu" aria-label={label}>
      {options.map((option) => {
        const key = Array.isArray(option) ? option[0] : option;
        const text = Array.isArray(option) ? option[1] : option;
        return <button type="button" key={key} className={value === key ? "chosen" : ""} onClick={() => { onChange(key); setOpen(false); }}>{text}</button>;
      })}
    </div>}
  </div>;
}

export default function CatalogV3({ items = [], orders = [], favorites = [], onFavorite, onNavigate, request, fetchData }) {
  const [mode, setMode] = useState("services");
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState("Все");
  const [sort, setSort] = useState("new");
  const [categoryOpen, setCategoryOpen] = useState(false);
  const [sortOpen, setSortOpen] = useState(false);
  const [detail, setDetail] = useState(null);
  const [profile, setProfile] = useState(null);
  const [application, setApplication] = useState({ price: "", deadline: "", comment: "" });
  const [notice, setNotice] = useState("");
  const [sending, setSending] = useState(false);

  const source = mode === "services" ? items : orders;
  const filtered = useMemo(() => source
    .filter((item) => {
      const matchesCategory = category === "Все" || item.category === category;
      const text = `${item.title || ""} ${item.description || ""} ${item.category || ""}`.toLowerCase();
      return matchesCategory && text.includes(query.trim().toLowerCase());
    })
    .sort((a, b) => {
      if (sort === "rating") return Number(b.avg_rating || 0) - Number(a.avg_rating || 0);
      if (sort === "low") return Number(a.price ?? a.budget ?? 0) - Number(b.price ?? b.budget ?? 0);
      if (sort === "high") return Number(b.price ?? b.budget ?? 0) - Number(a.price ?? a.budget ?? 0);
      return Number(b.id || 0) - Number(a.id || 0);
    }), [source, category, query, sort]);

  const openProfile = async (id) => {
    if (!id || !fetchData) return;
    try {
      const result = await fetchData(`/api/users/${id}/public`);
      if (result?.id) setProfile(result);
    } catch {
      setNotice("Профиль пока недоступен. Попробуйте ещё раз.");
    }
  };

  const submitApplication = async () => {
    if (!detail?.id) return;
    setSending(true);
    try {
      await request(`/api/orders/${detail.id}/applications`, application);
      setNotice("Отклик отправлен заказчику.");
      setDetail(null);
      setApplication({ price: "", deadline: "", comment: "" });
    } catch (error) {
      setNotice(error?.message || "Не удалось отправить отклик.");
    } finally {
      setSending(false);
    }
  };

  if (profile?.id) {
    const profileName = sellerName(profile);
    return <section className="seller-profile marketplace-shell">
      <header className="market-topbar"><button type="button" className="market-back" onClick={() => setProfile(null)}>‹</button><b>Профиль исполнителя</b><span /></header>
      <div className="seller-profile-hero">
        <Avatar name={profileName} image={profile.avatar_url} />
        <div><p className="market-eyebrow">ПРОВЕРЕННЫЙ ПРОФИЛЬ</p><h1>{profileName}</h1><span>@{profile.username || "lteam_user"}</span></div>
        <Rating rating={profile.rating} count={profile.reviews_count} />
      </div>
      <section className="seller-stat-row"><div><b>{profile.reviews_count || 0}</b><span>отзывов</span></div><div><b>{Number(profile.rating || 0).toFixed(1)}</b><span>рейтинг</span></div><div><b>{profile.listings?.length || 0}</b><span>услуг</span></div></section>
      <section className="seller-services"><div className="market-section-title"><div><p className="market-eyebrow">ВИТРИНА</p><h2>Услуги исполнителя</h2></div></div>
        {!profile.listings?.length && <div className="market-empty">У этого исполнителя пока нет активных услуг.</div>}
        {profile.listings?.map((listing) => <article className="seller-service-row" key={listing.id}>
          <div className="seller-mini-cover">{listing.image_data ? <img src={listing.image_data} alt="" /> : "LT"}</div>
          <div><span>{listing.category || "Услуга"}</span><b>{listing.title}</b><small>{listing.delivery_time || "Срок обсуждается"}</small></div>
          <strong>{price(listing.price)}</strong>
        </article>)}
      </section>
    </section>;
  }

  return <section className="catalog-v3 marketplace-shell">
    <header className="market-topbar">
      <button type="button" className="market-brand" onClick={() => onNavigate("home")}><i>LT</i><b>Маркет</b></button>
      <button type="button" className="market-create" onClick={() => onNavigate("create-order")}>+ Заказ</button>
    </header>
    <main className="market-content">
      <div className="market-heading"><p className="market-eyebrow">LTEAM MARKETPLACE</p><h1>{mode === "services" ? "Услуги" : "Заказы"}</h1><span>{mode === "services" ? "Исполнители предлагают свою работу" : "Найдите задачу и откликнитесь"}</span></div>
      <nav className="market-segment" aria-label="Тип каталога"><button type="button" className={mode === "services" ? "active" : ""} onClick={() => setMode("services")}>Предлагаю услугу</button><button type="button" className={mode === "orders" ? "active" : ""} onClick={() => setMode("orders")}>Ищу исполнителя</button></nav>
      <label className="market-search"><span>⌕</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Поиск по каталогу" /><button type="button" aria-label="Очистить поиск" className={query ? "visible" : ""} onClick={() => setQuery("")}>×</button></label>
      <div className="market-toolbar">
        <Picker label="Категория" value={category} options={CATEGORIES} onChange={setCategory} open={categoryOpen} setOpen={setCategoryOpen} />
        <Picker label="Сортировка" value={sort} options={SORTS} onChange={setSort} open={sortOpen} setOpen={setSortOpen} />
        <span className="market-found">{filtered.length} {filtered.length === 1 ? "найдено" : "найдено"}</span>
      </div>
      <div className={`market-grid ${mode}`}>
        {!filtered.length && <div className="market-empty"><b>Ничего не найдено</b><span>Измените поиск или выберите другую категорию.</span></div>}
        {filtered.map((item) => mode === "services" ? <ServiceCard key={item.id} item={item} favorites={favorites} onFavorite={onFavorite} onDetail={() => setDetail({ ...item, type: "service" })} onProfile={openProfile} /> : <OrderCard key={item.id} item={item} onDetail={() => setDetail({ ...item, type: "order" })} />)}
      </div>
    </main>
    <nav className="market-dock"><button type="button" onClick={() => onNavigate("home")}>⌂<span>Главная</span></button><button type="button" className="active">▦<span>Маркет</span></button><button type="button" onClick={() => onNavigate("orders")}>▤<span>Мои заказы</span></button><button type="button" onClick={() => onNavigate("profile")}>♙<span>Профиль</span></button></nav>
    {notice && <button className="market-notice" type="button" onClick={() => setNotice("")}>{notice}<span>×</span></button>}
    {detail && <DetailSheet item={detail} onClose={() => setDetail(null)} onProfile={openProfile} onFavorite={onFavorite} favorites={favorites} onCreate={() => { setDetail(null); onNavigate("create-order"); }} application={application} setApplication={setApplication} sending={sending} onApply={submitApplication} />}
  </section>;
}

function ServiceCard({ item, favorites, onFavorite, onDetail, onProfile }) {
  const name = sellerName(item);
  const isFavorite = favorites.includes(item.id);
  return <article className="market-card service-card">
    <button type="button" className="service-cover" onClick={onDetail}>{item.image_data ? <img src={item.image_data} alt="" /> : <><span>LT</span><small>{item.category || "Услуга"}</small></>}</button>
    <button type="button" className={`market-heart ${isFavorite ? "active" : ""}`} onClick={() => onFavorite(item.id)} aria-label="В избранное">♥</button>
    <div className="service-card-body">
      <button type="button" className="seller-line" onClick={() => onProfile(item.seller_id)}><Avatar name={name} image={item.avatar_url} /><span><b>{name}</b><small>@{sellerNick(item)}</small></span></button>
      <Rating rating={item.avg_rating} count={item.reviews_count} />
      <button type="button" className="service-title" onClick={onDetail}>{item.title}</button>
      <div className="market-badges"><span>{item.category || "Услуга"}</span><span>◷ {item.delivery_time || "Срок обсуждается"}</span></div>
      <footer><strong>{price(item.price)}</strong><button type="button" onClick={onDetail}>Подробнее <span>›</span></button></footer>
    </div>
  </article>;
}

function OrderCard({ item, onDetail }) {
  return <article className="market-card order-card">
    <div className="order-card-top"><span>{item.category || "Заказ"}</span><strong>{price(item.budget, "до ")}</strong></div>
    <h3>{item.title}</h3><p>{item.description || "Описание заказа появится здесь."}</p>
    <div className="order-client"><Avatar name={item.customer_name || item.customer_username || "Заказчик"} /><span><b>{item.customer_name || item.customer_username || "Заказчик LTeam"}</b><small>@{item.customer_username || "lteam_user"}</small></span></div>
    <footer><span>◷ {item.deadline || "Срок обсуждается"}</span><button type="button" onClick={onDetail}>Откликнуться</button></footer>
  </article>;
}

function DetailSheet({ item, onClose, onProfile, onFavorite, favorites, onCreate, application, setApplication, sending, onApply }) {
  const isService = item.type === "service";
  const name = sellerName(item);
  return <div className="market-modal-backdrop" role="presentation" onMouseDown={onClose}>
    <section className="market-detail-sheet" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
      <div className="sheet-handle" /><header><span>{item.category || (isService ? "Услуга" : "Заказ")}</span><button type="button" onClick={onClose}>×</button></header>
      {item.image_data && <img className="detail-image" src={item.image_data} alt="" />}
      <h2>{item.title}</h2>
      {isService && <button type="button" className="detail-seller" onClick={() => onProfile(item.seller_id)}><Avatar name={name} image={item.avatar_url} /><span><b>{name}</b><small>@{sellerNick(item)}</small></span><Rating rating={item.avg_rating} count={item.reviews_count} /><i>›</i></button>}
      <p className="detail-description">{item.description || "Исполнитель уточнит детали после создания заказа."}</p>
      <div className="detail-facts"><div><span>{isService ? "Стоимость" : "Бюджет"}</span><b>{price(isService ? item.price : item.budget, isService ? "от " : "до ")}</b></div><div><span>Срок</span><b>{isService ? item.delivery_time || "Обсуждается" : item.deadline || "Обсуждается"}</b></div></div>
      {isService ? <div className="detail-actions"><button type="button" className="secondary" onClick={() => onFavorite(item.id)}>{favorites.includes(item.id) ? "♥ В избранном" : "♡ В избранное"}</button><button type="button" className="primary" onClick={onCreate}>Создать заказ</button></div> : <div className="application-form"><h3>Отклик на заказ</h3><div><input value={application.price} onChange={(event) => setApplication({ ...application, price: event.target.value })} inputMode="numeric" placeholder="Ваша цена, ₽" /><input value={application.deadline} onChange={(event) => setApplication({ ...application, deadline: event.target.value })} placeholder="Срок выполнения" /></div><textarea value={application.comment} onChange={(event) => setApplication({ ...application, comment: event.target.value })} placeholder="Коротко расскажите, как выполните задачу" /><button type="button" className="primary" disabled={sending} onClick={onApply}>{sending ? "Отправляем…" : "Отправить отклик"}</button></div>}
    </section>
  </div>;
}
