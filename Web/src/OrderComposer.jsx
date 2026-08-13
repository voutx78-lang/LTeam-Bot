import { useState } from "react";

const categories = ["Telegram-боты и Mini Apps", "Дизайн Telegram", "Монтаж и контент", "AI-автоматизация", "Тексты для каналов и бизнеса"];
const deadlines = ["По договорённости", "Сегодня", "1–3 дня", "До недели", "Больше недели"];

export default function OrderComposer({ initial, onSubmit, onClose, message }) {
  const [form, setForm] = useState(initial);
  const [reference, setReference] = useState(() => sessionStorage.getItem("lteam-order-reference") || "");
  const [error, setError] = useState("");
  const update = (key, value) => setForm((current) => ({ ...current, [key]: value }));
  const chooseReference = (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    if (!file.type.startsWith("image/") || file.size > 650000) {
      setError("Подойдёт изображение до 650 КБ.");
      event.target.value = "";
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      const value = String(reader.result || "");
      sessionStorage.setItem("lteam-order-reference", value);
      setReference(value);
      setError("");
    };
    reader.readAsDataURL(file);
  };
  const removeReference = () => {
    sessionStorage.removeItem("lteam-order-reference");
    setReference("");
  };
  const submit = (event) => {
    event.preventDefault();
    if (form.description.trim().length < 12) {
      setError("Опишите задачу чуть подробнее — минимум 12 символов.");
      return;
    }
    setError("");
    onSubmit({ ...form, reference_image_data: reference }, () => {
      setForm(initial);
      removeReference();
    });
  };
  return <section className="order-composer">
    <header className="composer-head">
      <button type="button" className="composer-back" onClick={onClose}>←</button>
      <div><p>НОВАЯ ЗАДАЧА</p><h1>Найти исполнителя</h1></div>
      <span className="composer-step">1 из 1</span>
    </header>
    <div className="composer-intro"><span>✦</span><div><b>Расскажите о задаче</b><small>После модерации она появится в каталоге для исполнителей.</small></div></div>
    <form onSubmit={submit} className="order-composer-form">
      <label>Что нужно сделать?<input required value={form.title} onChange={(event) => update("title", event.target.value)} placeholder="Например, разработать Telegram-бота" maxLength="110" /></label>
      <div className="composer-row">
        <label>Категория<select value={form.category} onChange={(event) => update("category", event.target.value)}>{categories.map((item) => <option key={item}>{item}</option>)}</select></label>
        <label>Срок<select value={form.deadline} onChange={(event) => update("deadline", event.target.value)}>{deadlines.map((item) => <option key={item}>{item}</option>)}</select></label>
      </div>
      <label>Бюджет, ₽<input required inputMode="numeric" value={form.budget} onChange={(event) => update("budget", event.target.value.replace(/\D/g, ""))} placeholder="Например, 5 000" /></label>
      <label>Опишите задачу<textarea required minLength="12" value={form.description} onChange={(event) => update("description", event.target.value)} placeholder="Какой результат нужен, что уже есть и что важно учесть исполнителю?" maxLength="1600" /><small className="field-hint">{form.description.length}/1600 · описание обязательно</small></label>
      <div className="reference-block"><div><b>Пример для исполнителя <small>необязательно</small></b><span>Прикрепите референс, скриншот или набросок желаемого результата.</span></div>{reference ? <button type="button" className="reference-image" onClick={removeReference}><img src={reference} alt="Референс задачи" /><i>×</i></button> : <label className="reference-upload">＋<input type="file" accept="image/*" onChange={chooseReference} /></label>}</div>
      <div className="composer-security"><span>✓</span><p><b>Публикация через модерацию</b><small>Администратор проверит заказ перед показом в каталоге.</small></p></div>
      <button className="composer-submit" type="submit">Отправить на модерацию <span>→</span></button>
      {(error || message) && <p className="composer-message">{error || message}</p>}
    </form>
  </section>;
}
