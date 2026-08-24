import { useEffect, useMemo, useRef, useState } from "react";
import Icon from "../icons";
import { PageHeader } from "../components";
import { api, compressImage, send } from "../api";
import MobileSelect from "../components/MobileSelect";
import { CATEGORY_META } from "../constants";

const packageTemplates = [
  { package_key: "basic", title: "Базовый", description: "Основной результат без дополнительных опций", price: "", delivery_time: "3 дня", revisions: 1 },
  { package_key: "standard", title: "Стандарт", description: "Расширенный вариант и дополнительные материалы", price: "", delivery_time: "5 дней", revisions: 2 },
  { package_key: "premium", title: "Премиум", description: "Полный комплекс работ и сопровождение", price: "", delivery_time: "7 дней", revisions: 3 },
];

const blank = {
  listing: { title: "", category: "Telegram-боты и Mini Apps", description: "", result_description: "", requirements: "", price: "", delivery_time: "3 дня", revisions: 1, image_data: "", portfolio_data: [], packages: packageTemplates },
  order: { title: "", category: "Telegram-боты и Mini Apps", description: "", budget: "", deadline: "По договорённости", reference_image_data: "" },
};

export default function CreateScreen({ initialType = "", categories, onBack, onDone, notify }) {
  const safeInitialType = ["listing", "order"].includes(initialType) ? initialType : "";
  const [type, setType] = useState(safeInitialType);
  const [step, setStep] = useState(1);
  const [form, setForm] = useState(blank[safeInitialType] || null);
  const [saving, setSaving] = useState(false);
  const hydrated = useRef(false);
  const totalSteps = type === "listing" ? 4 : 3;
  const categoryOptions = useMemo(() => categories.map((category) => ({ value: category, label: CATEGORY_META[category]?.short || category, description: category, icon: CATEGORY_META[category]?.icon || "grid" })), [categories]);
  useEffect(() => {
    if (!type) return;
    setForm((value) => value || structuredClone(blank[type]));
    api(`/api/drafts/${type}`).then((data) => {
      const payload = data?.payload;
      if (payload) setForm((value) => ({ ...value, ...payload }));
      hydrated.current = true;
    }).catch(() => { hydrated.current = true; });
  }, [type]);
  useEffect(() => {
    if (!type || !form || !hydrated.current) return;
    const timer = window.setTimeout(() => send(`/api/drafts/${type}`, "PUT", form).catch(() => {}), 700);
    return () => window.clearTimeout(timer);
  }, [type, form]);
  const valid = useMemo(() => {
    if (!form) return false;
    if (step === 1) return form.title?.trim().length >= 5 && form.category;
    if (step === 2) return form.description?.trim().length >= 30;
    if (type === "listing" && step === 3) return Boolean(form.image_data) && Number(form.price || form.packages?.[0]?.price || 0) > 0;
    if (type === "order" && step === 3) return Number(form.budget || 0) > 0;
    return true;
  }, [form, step, type]);
  const update = (key, value) => setForm((current) => ({ ...current, [key]: value }));
  const updatePackage = (index, key, value) => setForm((current) => ({ ...current, packages: current.packages.map((entry, position) => position === index ? { ...entry, [key]: value } : entry) }));
  async function loadImage(file, key, portfolio = false) {
    try {
      const data = await compressImage(file, portfolio ? 1100 : 1440, portfolio ? 0.7 : 0.78);
      if (portfolio) update("portfolio_data", [...form.portfolio_data, data].slice(0, 4)); else update(key, data);
    } catch (error) { notify(error.message, "error"); }
  }
  async function publish() {
    setSaving(true);
    try {
      if (type === "listing") {
        const packages = form.packages.filter((entry) => Number(entry.price || 0));
        const payload = { ...form, price: Number(form.price || packages[0]?.price || 0), packages: packages.map((entry) => ({ ...entry, price: Number(entry.price), revisions: Number(entry.revisions || 0) })) };
        await send("/api/listings", "POST", payload);
        notify("Услуга отправлена на модерацию");
      } else {
        await send("/api/orders", "POST", { ...form, budget: Number(form.budget) });
        notify("Заказ отправлен на модерацию");
      }
      onDone(type);
    } catch (error) { notify(error.message, "error"); }
    finally { setSaving(false); }
  }
  if (!type) return <section className="screen create-screen"><PageHeader eyebrow="НОВАЯ ПУБЛИКАЦИЯ" title="Что размещаем?" onBack={onBack}/><div className="create-choice"><button onClick={() => { setType("order"); setForm(structuredClone(blank.order)); }}><i><Icon name="briefcase"/></i><span><b>Задачу</b><small>Опишите работу и получите отклики исполнителей</small></span><Icon name="chevron"/></button><button onClick={() => { setType("listing"); setForm(structuredClone(blank.listing)); }}><i><Icon name="grid"/></i><span><b>Услугу</b><small>Оформите предложение, тарифы и портфолио</small></span><Icon name="chevron"/></button></div></section>;
  return <section className="screen create-screen"><PageHeader eyebrow={type === "listing" ? "НОВАЯ УСЛУГА" : "НОВАЯ ЗАДАЧА"} title={step === totalSteps ? "Предпросмотр" : `Шаг ${step} из ${totalSteps}`} onBack={() => step > 1 ? setStep(step - 1) : onBack()}/><div className="step-progress">{Array.from({ length: totalSteps }, (_, index) => <i className={index + 1 <= step ? "active" : ""} key={index}/>)}</div>
    <div className="composer-body">
      {step === 1 && <><div className="composer-copy"><small>ОСНОВА</small><h2>{type === "listing" ? "Какую услугу вы предлагаете?" : "Какого результата вы ждёте?"}</h2><p>Короткое и конкретное название лучше работает в поиске.</p></div><label className="field"><span>Название</span><input value={form.title} maxLength={120} onChange={(event) => update("title", event.target.value)} placeholder={type === "listing" ? "Разработаю Telegram-бота под ключ" : "Нужен дизайн Telegram-канала"}/><small>{form.title.length}/120</small></label><div className="field"><span>Категория</span><MobileSelect value={form.category} onChange={(value) => update("category", value)} options={categoryOptions} title="Выберите направление" eyebrow="КАТЕГОРИЯ" searchable/></div></>}
      {step === 2 && <><div className="composer-copy"><small>ПОДРОБНОСТИ</small><h2>{type === "listing" ? "Расскажите о результате" : "Составьте понятное задание"}</h2><p>Описание обязательно. Укажите объём, формат результата и важные ограничения.</p></div><label className="field"><span>Описание</span><textarea value={form.description} maxLength={2000} onChange={(event) => update("description", event.target.value)} placeholder={type === "listing" ? "Что входит в услугу, как проходит работа и для кого она подходит" : "Что уже есть, что нужно сделать и каким должен быть результат"}/><small>{form.description.length}/2000 · минимум 30</small></label>{type === "listing" && <><label className="field"><span>Что получит заказчик</span><textarea value={form.result_description} maxLength={1200} onChange={(event) => update("result_description", event.target.value)} placeholder="Например: исходный код, инструкция и 14 дней поддержки"/></label><label className="field"><span>Что потребуется от заказчика</span><textarea value={form.requirements} maxLength={1200} onChange={(event) => update("requirements", event.target.value)} placeholder="Материалы, доступы и информация для старта"/></label></>}</>}
      {type === "listing" && step === 3 && <><div className="composer-copy"><small>ВИТРИНА И ЦЕНА</small><h2>Покажите работу</h2><p>Обложка обязательна. Добавьте до четырёх примеров в портфолио.</p></div><div className="upload-grid"><label className={`cover-upload ${form.image_data ? "filled" : ""}`}>{form.image_data ? <img src={form.image_data} alt="Обложка"/> : <><Icon name="image"/><b>Добавить обложку</b><small>Обязательно · горизонтальное фото</small></>}<input type="file" accept="image/*" onChange={(event) => loadImage(event.target.files?.[0], "image_data")}/></label><div className="portfolio-upload">{form.portfolio_data.map((image, index) => <button key={index} onClick={() => update("portfolio_data", form.portfolio_data.filter((_, position) => position !== index))}><img src={image} alt=""/><Icon name="close" size={16}/></button>)}{form.portfolio_data.length < 4 && <label><Icon name="plus"/><span>Пример работы</span><input type="file" accept="image/*" onChange={(event) => loadImage(event.target.files?.[0], "portfolio_data", true)}/></label>}</div></div><div className="base-price-row"><label className="field"><span>Цена от, ₽</span><input type="number" min="1" value={form.price} onChange={(event) => update("price", event.target.value)}/></label><label className="field"><span>Срок</span><input value={form.delivery_time} onChange={(event) => update("delivery_time", event.target.value)}/></label></div></>}
      {type === "listing" && step === 4 && <><div className="composer-copy"><small>ТАРИФЫ</small><h2>Предложите варианты</h2><p>Можно оставить один тариф или заполнить все три.</p></div><div className="package-editor">{form.packages.map((entry, index) => <article key={entry.package_key}><header><b>{entry.title}</b><small>{index === 0 ? "ОБЯЗАТЕЛЬНЫЙ" : "ПО ЖЕЛАНИЮ"}</small></header><label className="field"><span>Цена, ₽</span><input type="number" min="0" value={entry.price} onChange={(event) => updatePackage(index, "price", event.target.value)}/></label><label className="field"><span>Что входит</span><textarea value={entry.description} onChange={(event) => updatePackage(index, "description", event.target.value)}/></label><div><label className="field"><span>Срок</span><input value={entry.delivery_time} onChange={(event) => updatePackage(index, "delivery_time", event.target.value)}/></label><label className="field"><span>Правки</span><input type="number" min="0" max="20" value={entry.revisions} onChange={(event) => updatePackage(index, "revisions", event.target.value)}/></label></div></article>)}</div></>}
      {type === "order" && step === 3 && <><div className="composer-copy"><small>УСЛОВИЯ</small><h2>Бюджет, срок и пример</h2><p>Исполнители смогут предложить другую цену в отклике.</p></div><div className="base-price-row"><label className="field"><span>Бюджет до, ₽</span><input type="number" min="1" value={form.budget} onChange={(event) => update("budget", event.target.value)}/></label><label className="field"><span>Желаемый срок</span><input value={form.deadline} onChange={(event) => update("deadline", event.target.value)}/></label></div><label className={`reference-upload ${form.reference_image_data ? "filled" : ""}`}>{form.reference_image_data ? <img src={form.reference_image_data} alt="Референс"/> : <><Icon name="image"/><b>Добавить пример</b><small>Необязательно · покажите, что вам нравится</small></>}<input type="file" accept="image/*" onChange={(event) => loadImage(event.target.files?.[0], "reference_image_data")}/></label></>}
      {step === totalSteps && <article className="publish-summary"><Icon name="shield"/><div><b>Перед публикацией всё проверит модератор</b><span>Мы проверяем описание, обложку и соответствие выбранной категории.</span></div></article>}
    </div><footer className="composer-actions"><span>Черновик сохранён</span>{step < totalSteps ? <button className="primary-button" disabled={!valid} onClick={() => setStep(step + 1)}>Продолжить <Icon name="arrow"/></button> : <button className="primary-button" disabled={saving || !valid} onClick={publish}>{saving ? "Публикуем…" : "Отправить на модерацию"}</button>}</footer>
  </section>;
}
