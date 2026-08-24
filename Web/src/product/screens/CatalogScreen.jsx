import { useEffect, useMemo, useRef, useState } from "react";
import Icon from "../icons";
import { Avatar, EmptyState, OrderCard, PageHeader, Price, Rating, ServiceCard, Sheet } from "../components";
import { CATEGORY_META } from "../constants";
import { money } from "../api";
import MobileSelect from "../components/MobileSelect";

const sorters = {
  relevant: () => 0,
  new: (a, b) => Number(b.id) - Number(a.id),
  rating: (a, b) => Number(b.avg_rating || 0) - Number(a.avg_rating || 0) || Number(b.reviews_count || 0) - Number(a.reviews_count || 0),
  popular: (a, b) => Number(b.completed_orders || b.reviews_count || 0) - Number(a.completed_orders || a.reviews_count || 0),
  price_up: (a, b) => Number(a.price || a.budget || 0) - Number(b.price || b.budget || 0),
  price_down: (a, b) => Number(b.price || b.budget || 0) - Number(a.price || a.budget || 0),
};

const SORT_OPTIONS = [
  { value: "relevant", label: "По релевантности", description: "Сначала наиболее подходящие", icon: "spark" },
  { value: "new", label: "Сначала новые", description: "Свежие публикации выше", icon: "clock" },
  { value: "popular", label: "Популярные", description: "Больше заказов и интереса", icon: "chart" },
  { value: "rating", label: "По рейтингу", description: "Сначала лучшие оценки", icon: "star", servicesOnly: true },
  { value: "price_up", label: "Сначала дешевле", description: "Цена по возрастанию", icon: "arrow" },
  { value: "price_down", label: "Сначала дороже", description: "Цена по убыванию", icon: "list" },
];

export default function CatalogScreen({ listings, orders, categories, initial = {}, onNavigate, onCreate, onFavorite }) {
  const [type, setType] = useState(initial.type || "services");
  const [switchDirection, setSwitchDirection] = useState("next");
  const [query, setQuery] = useState(initial.query || "");
  const [selectedCategories, setSelectedCategories] = useState(() => {
    const linkedCategories = new URLSearchParams(window.location.search).getAll("category");
    if (linkedCategories.length) return linkedCategories;
    return Array.isArray(initial.category) ? initial.category : (initial.category ? [initial.category] : []);
  });
  const [sort, setSort] = useState("relevant");
  const [filtersOpen, setFiltersOpen] = useState(() => new URLSearchParams(window.location.search).get("open") === "filters");
  const [reviewedOnly, setReviewedOnly] = useState(false);
  const [fastOnly, setFastOnly] = useState(false);
  const searchRef = useRef(null);
  const sortOptions = useMemo(() => SORT_OPTIONS.filter((option) => !option.servicesOnly || type === "services"), [type]);
  useEffect(() => {
    if (!initial.focus) return undefined;
    const timer = window.setTimeout(() => searchRef.current?.focus(), Number(initial.focusDelay || 120));
    return () => window.clearTimeout(timer);
  }, [initial.focus, initial.focusDelay]);
  const source = type === "services" ? listings : orders;
  const selectType = (nextType) => {
    if (nextType === type) return;
    setSwitchDirection(nextType === "orders" ? "next" : "prev");
    setType(nextType);
  };
  const toggleCategory = (value) => {
    setSelectedCategories((current) => current.includes(value) ? current.filter((item) => item !== value) : [...current, value]);
  };
  const visible = useMemo(() => source.filter((item) => {
    const haystack = `${item.title || ""} ${item.description || ""} ${item.category || ""} ${item.seller_name || item.customer_name || ""}`.toLowerCase();
    if (query.trim() && !haystack.includes(query.trim().toLowerCase())) return false;
    if (selectedCategories.length && !selectedCategories.includes(item.category)) return false;
    if (type === "services" && reviewedOnly && !Number(item.reviews_count || 0)) return false;
    if (fastOnly && !String(item.delivery_time || item.deadline || "").match(/1|2|3|день|дня/)) return false;
    return true;
  }).sort(sorters[sort] || sorters.relevant), [source, query, selectedCategories, reviewedOnly, fastOnly, sort, type]);
  const activeFilterCount = selectedCategories.length + Number(type === "services" && reviewedOnly) + Number(fastOnly);
  return <section className={`screen catalog-screen catalog-type-${type} ${initial.entrance === "search" ? "search-arrival" : ""}`}><PageHeader eyebrow="МАРКЕТПЛЕЙС" title={<span key={type} className={`catalog-title-swap ${switchDirection}`}>{type === "services" ? "Каталог услуг" : "Задачи заказчиков"}</span>} action={<button className="header-create" onClick={(event) => onCreate(type === "services" ? "listing" : "order", event.currentTarget.getBoundingClientRect())}><Icon name="plus" size={18}/> Создать</button>}/>
    <div className={`catalog-switch ${type}`}><i className="catalog-switch-indicator" aria-hidden="true"/><button className={type === "services" ? "active" : ""} aria-pressed={type === "services"} onClick={() => selectType("services")}><b>Услуги</b><span>Выбрать готовое предложение</span></button><button className={type === "orders" ? "active" : ""} aria-pressed={type === "orders"} onClick={() => selectType("orders")}><b>Задачи</b><span>Найти проект и откликнуться</span></button></div>
    <div key={type} className={`catalog-view-swap ${switchDirection}`}>
      <div className="catalog-search-row"><label><Icon name="search"/><input ref={searchRef} value={query} onChange={(event) => setQuery(event.target.value)} placeholder={type === "services" ? "Название услуги или исполнитель" : "Что нужно сделать?"}/>{query && <button onClick={() => setQuery("")} aria-label="Очистить"><Icon name="close" size={17}/></button>}</label><button className={activeFilterCount ? "active" : ""} onClick={() => setFiltersOpen(true)} aria-label={`Фильтры${activeFilterCount ? `: выбрано ${activeFilterCount}` : ""}`}><Icon name="filter"/>{activeFilterCount > 0 && <i className="filter-count">{activeFilterCount}</i>}</button></div>
      <div className="category-chips" aria-label="Категории"><button className={!selectedCategories.length ? "active" : ""} aria-pressed={!selectedCategories.length} onClick={() => setSelectedCategories([])}>Все</button>{categories.map((item) => <button className={selectedCategories.includes(item) ? "active" : ""} aria-pressed={selectedCategories.includes(item)} key={item} onClick={() => toggleCategory(item)}>{selectedCategories.includes(item) && <Icon name="check" size={13}/>} {CATEGORY_META[item]?.short || item}</button>)}</div>
      <div className="catalog-toolbar"><span>{visible.length} {visible.length === 1 ? "результат" : "результатов"}</span><MobileSelect variant="toolbar" value={sort} onChange={setSort} options={sortOptions} title="Как показать результаты" eyebrow="СОРТИРОВКА"/></div>
      {visible.length ? (
        type === "services"
          ? <div className="catalog-grid">{visible.map((item) => <ServiceCard key={item.id} item={item} onOpen={() => onNavigate("listing", { id: item.id })} onFavorite={onFavorite}/>)}</div>
          : <div className="order-list catalog-orders">{visible.map((item) => <OrderCard key={item.id} item={item} onOpen={() => onNavigate("order", { id: item.id })}/>)}</div>
      ) : <EmptyState
        icon="search"
        title="Ничего не найдено"
        text="Попробуйте убрать часть фильтров или изменить запрос."
        action="Сбросить фильтры"
        onAction={() => { setQuery(""); setSelectedCategories([]); setReviewedOnly(false); setFastOnly(false); }}
      />}
    </div>
    <Sheet open={filtersOpen} title="Фильтры" onClose={() => setFiltersOpen(false)}><div className="filter-sheet"><section className="filter-category-picker"><header><span><b>Категории</b><small>Можно выбрать несколько</small></span>{selectedCategories.length > 0 && <button onClick={() => setSelectedCategories([])}>Сбросить</button>}</header><div>{categories.map((item) => <button className={selectedCategories.includes(item) ? "active" : ""} aria-pressed={selectedCategories.includes(item)} key={item} onClick={() => toggleCategory(item)}><span>{CATEGORY_META[item]?.short || item}</span><i>{selectedCategories.includes(item) && <Icon name="check" size={13}/>}</i></button>)}</div></section>{type === "services" && <label className="toggle-row"><span><b>Только с отзывами</b><small>Показывать исполнителей с историей</small></span><input type="checkbox" checked={reviewedOnly} onChange={(event) => setReviewedOnly(event.target.checked)}/></label>}<label className="toggle-row"><span><b>Быстрый срок</b><small>До трёх дней</small></span><input type="checkbox" checked={fastOnly} onChange={(event) => setFastOnly(event.target.checked)}/></label><button className="primary-button wide" onClick={() => setFiltersOpen(false)}>Показать {visible.length}</button></div></Sheet>
  </section>;
}

export function ListingDetail({ item, loading, onBack, onSeller, onFavorite, onRequest }) {
  const [packageIndex, setPackageIndex] = useState(0);
  if (loading || !item) return <section className="screen detail-screen"><PageHeader title="Услуга" onBack={onBack}/><div className="detail-skeleton"/></section>;
  const packages = item.packages?.length ? item.packages : [{ title: "Базовый", price: item.price, delivery_time: item.delivery_time, revisions: item.revisions, description: item.result_description || item.description }];
  const activePackage = packages[Math.min(packageIndex, packages.length - 1)];
  const gallery = [item.image_data, ...(item.portfolio_data || [])].filter(Boolean);
  return <section className="screen detail-screen"><PageHeader eyebrow={item.category} title="Услуга" onBack={onBack} action={<button className={`icon-button ${item.is_favorite ? "active" : ""}`} onClick={() => onFavorite(item)}><Icon name="heart"/></button>}/>
    <div className="detail-gallery">{gallery.length ? gallery.map((image, index) => <img key={`${image.slice?.(0, 30)}-${index}`} src={image} alt={index ? "Пример работы" : item.title}/>) : <div/>}</div>
    <section className="detail-main"><div className="detail-title"><span>{item.category}</span><h1>{item.title}</h1><button onClick={onSeller}><Avatar src={item.avatar_url} name={item.seller_name || item.seller_username} verified={Boolean(item.seller_verified)}/><div><b>{item.seller_name || item.seller_username || "Исполнитель LT"}</b><Rating rating={item.avg_rating} count={item.reviews_count}/></div><Icon name="chevron"/></button></div>
      <div className="package-tabs">{packages.map((entry, index) => <button key={entry.package_key || entry.title} className={packageIndex === index ? "active" : ""} onClick={() => setPackageIndex(index)}>{entry.title}</button>)}</div><article className="package-card"><header><div><small>СТОИМОСТЬ</small><Price value={activePackage.price}/></div><div><small>СРОК</small><b>{activePackage.delivery_time || "По договорённости"}</b></div><div><small>ПРАВКИ</small><b>{Number(activePackage.revisions || 0) ? activePackage.revisions : "Без правок"}</b></div></header><p>{activePackage.description || item.description}</p></article>
      <article className="detail-section"><h2>Об услуге</h2><p>{item.description}</p></article>{item.result_description && <article className="detail-section"><h2>Что вы получите</h2><p>{item.result_description}</p></article>}{item.requirements && <article className="detail-section"><h2>Что потребуется от вас</h2><p>{item.requirements}</p></article>}
      <article className="detail-section reviews-preview"><header><h2>Отзывы</h2><span>{item.reviews?.length || item.reviews_count || 0}</span></header>{item.reviews?.length ? item.reviews.slice(0, 3).map((review) => <div key={review.id}><Avatar name={review.reviewer_name} size="sm"/><span><b>{review.reviewer_name}</b><Rating rating={review.rating} count={1} compact/><p>{review.text}</p></span></div>) : <p>У исполнителя ещё нет отзывов. После завершённых заказов они появятся здесь.</p>}</article>
    </section><footer className="sticky-action"><div><small>Выбранный тариф</small><Price value={activePackage.price}/></div><button className="primary-button" onClick={() => onRequest(activePackage)}>Обсудить заказ <Icon name="arrow"/></button></footer>
  </section>;
}

export function OrderDetail({ item, onBack, onApply }) {
  if (!item) return null;
  return <section className="screen detail-screen order-detail"><PageHeader eyebrow="ЗАДАЧА" title="Нужен исполнитель" onBack={onBack}/>{item.reference_image_data && <img className="order-reference" src={item.reference_image_data} alt="Пример заказчика"/>}<article className="order-detail-card"><span>{item.category}</span><h1>{item.title}</h1><p>{item.description}</p><div><span><small>Бюджет</small><b>{money(item.budget, "до ")}</b></span><span><small>Срок</small><b>{item.deadline || "Обсуждается"}</b></span></div></article><button className="customer-card" onClick={() => {}}><Avatar src={item.customer_avatar_url} name={item.customer_name || item.customer_username}/><div><small>ЗАКАЗЧИК</small><b>{item.customer_name || item.customer_username || "Пользователь LT"}</b></div></button><footer className="sticky-action"><div><small>Опубликовал заказчик</small><b>Предложите цену и срок</b></div><button className="primary-button" onClick={onApply}>Откликнуться <Icon name="arrow"/></button></footer></section>;
}
