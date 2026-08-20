import { useEffect, useMemo, useState } from "react";
import "./DealActions.css";

const money = (value) => `${Number(value || 0).toLocaleString("ru-RU")} ₽`;
const titleFor = (item) => item.title || "Заказ LTeam";

export default function OrdersWorkspace({ orders = [], deals = [], profile, fetchData, request, onNavigate, onChat, onDealsChanged }) {
  const [view, setView] = useState("tasks");
  const [applications, setApplications] = useState([]);
  const [notice, setNotice] = useState("");
  const [selectedTask, setSelectedTask] = useState(null);
  const [taskApplications, setTaskApplications] = useState([]);
  const [choosing, setChoosing] = useState(false);
  const [reviewDeal, setReviewDeal] = useState(null);
  const ownOrders = useMemo(() => orders.filter((order) => Number(order.customer_id) === Number(profile?.id)), [orders, profile]);

  useEffect(() => {
    fetchData?.("/api/applications/mine").then(setApplications).catch(() => setApplications([]));
  }, [fetchData]);

  const active = view === "tasks" ? ownOrders : view === "responses" ? applications : deals;
  return <section className="orders-workspace marketplace-shell">
    <header className="market-topbar"><button type="button" className="market-brand" onClick={() => onNavigate("home")}><i>LT</i><b>Мои заказы</b></button><button type="button" className="market-create" onClick={() => onNavigate("create-order")}>+ Создать</button></header>
    <main className="orders-content">
      <div className="orders-intro"><p className="market-eyebrow">РАБОЧЕЕ ПРОСТРАНСТВО</p><h1>Заказы и работа</h1><span>Условия, отклики, чат и результат — в одном месте.</span></div>
      <div className="orders-summary"><div><b>{ownOrders.length}</b><span>моих задач</span></div><div><b>{applications.length}</b><span>моих откликов</span></div><div><b>{deals.length}</b><span>сделок</span></div></div>
      <nav className="orders-tabs"><button className={view === "tasks" ? "active" : ""} onClick={() => setView("tasks")}>Мои задачи</button><button className={view === "responses" ? "active" : ""} onClick={() => setView("responses")}>Отклики</button><button className={view === "deals" ? "active" : ""} onClick={() => setView("deals")}>Сделки</button></nav>
      <section className="orders-list">
        {!active.length && <div className="orders-empty"><b>{view === "tasks" ? "Нет созданных задач" : view === "responses" ? "Откликов пока нет" : "Сделок пока нет"}</b><span>{view === "tasks" ? "Создайте заказ — исполнители смогут откликнуться." : "Здесь появится история работы на LTeam."}</span>{view === "tasks" && <button onClick={() => onNavigate("create-order")}>Создать заказ</button>}</div>}
        {view === "tasks" && active.map((order) => <TaskRow key={order.id} order={order} onOpen={() => onChat({ kind: "order", item: order })} onApplications={async () => { try { const data = await fetchData(`/api/orders/${order.id}/applications`); setTaskApplications(data); setSelectedTask(order); } catch { setNotice("Не удалось загрузить отклики."); } }} />)}
        {view === "responses" && active.map((application) => <ApplicationRow key={application.id} item={application} onOpen={() => onChat({ kind: "order", item: { id: application.order_id, title: application.title } })} />)}
        {view === "deals" && active.map((deal) => <DealRow key={deal.id} deal={deal} profile={profile} onOpen={() => onChat({ kind: "deal", item: deal })} onAction={async (action, payload = {}) => { try { const result = await request(`/api/deals/${deal.id}/action`, "POST", { action, ...payload }); const refreshed = await fetchData("/api/deals"); onDealsChanged?.(refreshed); setNotice(result.status === "waiting_admin_payment_approval" ? "Цена подтверждена. Запрос оплаты отправлен администратору." : "Статус сделки обновлён."); } catch (error) { setNotice(error?.message || "Не удалось обновить сделку."); } }} onReview={Number(deal.buyer_id) === Number(profile?.id) && deal.status === "completed" ? () => setReviewDeal(deal) : null} />)}
      </section>
    </main>
    {notice && <button className="market-notice" onClick={() => setNotice("")}>{notice}<span>×</span></button>}
    {selectedTask && <ApplicantsSheet order={selectedTask} applications={taskApplications} onClose={() => setSelectedTask(null)} onChoose={async (application) => { if (choosing) return; setChoosing(true); try { const data = await request(`/api/orders/${selectedTask.id}/applications/${application.id}/accept`, "POST", {}); const refreshedDeals = await fetchData("/api/deals"); onDealsChanged?.(refreshedDeals); setView("deals"); setNotice(`Исполнитель выбран. Сделка #${data.deal_id} открыта для согласования деталей.`); setSelectedTask(null); } catch (error) { setNotice(error?.message || "Не удалось выбрать исполнителя."); } finally { setChoosing(false); } }} choosing={choosing} />}
    {reviewDeal && <ReviewSheet deal={reviewDeal} request={request} onClose={() => setReviewDeal(null)} onDone={() => { setNotice("Спасибо! Отзыв опубликован в профиле исполнителя."); setReviewDeal(null); }} />}
    <nav className="market-dock"><button onClick={() => onNavigate("home")}>⌂<span>Главная</span></button><button onClick={() => onNavigate("catalog")}>▦<span>Маркет</span></button><button className="active">▤<span>Мои заказы</span></button><button onClick={() => onNavigate("profile")}>♙<span>Профиль</span></button></nav>
  </section>;
}

function ReviewSheet({ deal, request, onClose, onDone }) {
  const [rating, setRating] = useState(5); const [text, setText] = useState(""); const [error, setError] = useState("");
  const submit = async (event) => { event.preventDefault(); try { await request(`/api/deals/${deal.id}/review`, "POST", { rating, text }); onDone(); } catch (err) { setError(err?.message || "Не удалось опубликовать отзыв."); } };
  return <div className="review-backdrop" onMouseDown={onClose}><form className="review-sheet" onMouseDown={(event) => event.stopPropagation()} onSubmit={submit}><div className="sheet-handle" /><button type="button" className="review-close" onClick={onClose}>×</button><p>ЗАВЕРШЁННАЯ СДЕЛКА</p><h2>Как прошла работа?</h2><span>Ваша оценка появится в профиле исполнителя.</span><div className="rating-pick">{[1,2,3,4,5].map((value) => <button key={value} type="button" className={value <= rating ? "active" : ""} onClick={() => setRating(value)}>★</button>)}</div><textarea required minLength="3" value={text} onChange={(event) => setText(event.target.value)} placeholder="Коротко опишите опыт работы" /><button className="review-submit">Опубликовать отзыв</button>{error && <small className="review-error">{error}</small>}</form></div>;
}

function ApplicantsSheet({ order, applications, onClose, onChoose, choosing }) {
  return <div className="applicants-backdrop" onMouseDown={onClose}><section className="applicants-sheet" onMouseDown={(event) => event.stopPropagation()}><div className="sheet-handle" /><header><div><small>ОТКЛИКИ НА ЗАКАЗ</small><h2>{titleFor(order)}</h2></div><button onClick={onClose}>×</button></header><p className="applicants-caption">Выберите исполнителя. После этого появится рабочее пространство для согласования условий, чата и передачи результата.</p>{!applications.length && <div className="applicants-empty">Откликов пока нет — исполнитель увидит заказ после модерации.</div>}<div className="applicants-list">{applications.map((item) => <article key={item.id} className="applicant-card"><div className="applicant-avatar">{(item.executor_name || item.executor_username || "L").slice(0, 1).toUpperCase()}</div><div className="applicant-copy"><b>{item.executor_name || item.executor_username || "Исполнитель LTeam"}</b><small>@{item.executor_username || "lteam_user"}</small><p>{item.comment}</p><span>{money(item.price)} · {item.deadline}</span></div><button disabled={choosing || item.status !== "new"} onClick={() => onChoose(item)}>{item.status === "new" ? (choosing ? "Создаём…" : "Выбрать") : "Выбран"}</button></article>)}</div></section></div>;
}

function TaskRow({ order, onOpen, onApplications }) {
  const active = ["active", "open", "approved"].includes(order.status);
  return <article className="work-row"><div className="work-row-head"><span className={active ? "work-status active" : "work-status"}>{active ? "Идёт поиск" : "На модерации"}</span><small>{order.category || "Заказ"}</small></div><h3>{titleFor(order)}</h3><p>{order.description || "Описание задачи"}</p><div className="work-meta"><b>до {money(order.budget)}</b><span>◷ {order.deadline || "Срок обсуждается"}</span></div><footer><button onClick={onApplications}>Отклики</button><button className="work-primary" onClick={onOpen}>Открыть чат</button></footer></article>;
}

function ApplicationRow({ item, onOpen }) {
  return <article className="work-row application-row"><div className="work-row-head"><span className="work-status">{item.status === "new" ? "Отправлен" : item.status}</span><small>{item.category || "Заказ"}</small></div><h3>{titleFor(item)}</h3><p>{item.comment || "Ваш отклик"}</p><div className="work-meta"><b>ваша цена {money(item.price)}</b><span>◷ {item.deadline || "Срок обсуждается"}</span></div><footer><span className="work-client">Заказчик: {item.customer_name || item.customer_username || "LTeam"}</span><button className="work-primary" onClick={onOpen}>Чат</button></footer></article>;
}

function DealRow({ deal, profile, onOpen, onAction, onReview }) {
  const label = {
    discussion: "Согласование деталей",
    waiting_final_price: "Ожидаем цену",
    waiting_buyer_price_confirm: "Подтвердите цену",
    terms_confirmed: "Условия согласованы",
    waiting_admin_payment_approval: "Оплата не подключена",
    waiting_payment: "Ожидаем оплату",
    waiting_receipt: "Проверяем оплату",
    waiting_admin_confirm: "Проверяем оплату",
    payment_rejected: "Оплата отклонена — согласуйте сумму заново",
    in_work: "В работе",
    waiting_buyer_confirm: "Ожидаем подтверждение",
    dispute_open: "Открыт спор",
    completed: "Завершена",
    cancelled: "Отменена",
  }[deal.status] || "Сделка";
  const isBuyer = Number(deal.buyer_id) === Number(profile?.id);
  const isSeller = Number(deal.seller_id) === Number(profile?.id);
  const setPrice = () => {
    const value = window.prompt("Итоговая цена в ₽", String(deal.amount || ""));
    if (value?.trim()) onAction("set_final_price", { amount: value.trim() });
  };
  const openDispute = () => {
    const reason = window.prompt("Коротко опишите проблему");
    if (reason?.trim()) onAction("open_dispute", { reason: reason.trim() });
  };
  return <article className="work-row deal-row"><div className="work-row-head"><span className="work-status active">{label}</span><small>Рабочая сделка</small></div><h3>{titleFor(deal)}</h3><div className="work-meta"><b>{money(deal.amount)}</b><span>Оплата площадкой пока не подключена</span></div><footer><span className="work-client">Все сообщения сохранены в сделке</span><div className="deal-row-actions">{isSeller && ["discussion", "waiting_final_price", "payment_rejected"].includes(deal.status) && <button className="work-review" onClick={setPrice}>{deal.status === "payment_rejected" ? "Изменить цену" : "Выставить цену"}</button>}{isBuyer && deal.status === "waiting_buyer_price_confirm" && <button className="work-review" onClick={() => onAction("confirm_price")}>Подтвердить цену</button>}{(isBuyer || isSeller) && deal.status === "terms_confirmed" && <button className="work-review" onClick={() => onAction("start_work")}>Начать работу</button>}{isSeller && deal.status === "in_work" && <button className="work-review" onClick={() => onAction("mark_done")}>Работа готова</button>}{isBuyer && deal.status === "waiting_buyer_confirm" && <button className="work-review" onClick={() => onAction("confirm_done")}>Подтвердить</button>}{!['completed','cancelled','dispute_open'].includes(deal.status) && <button className="work-review" onClick={openDispute}>Спор</button>}{onReview && <button className="work-review" onClick={onReview}>Отзыв</button>}<button className="work-primary" onClick={onOpen}>Чат сделки</button></div></footer></article>;
}
