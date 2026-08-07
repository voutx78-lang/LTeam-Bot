import { useEffect, useMemo, useState } from "react";

const money = (value) => `${Number(value || 0).toLocaleString("ru-RU")} ₽`;
const titleFor = (item) => item.title || "Заказ LTeam";

export default function OrdersWorkspace({ orders = [], deals = [], profile, fetchData, onNavigate, onChat }) {
  const [view, setView] = useState("tasks");
  const [applications, setApplications] = useState([]);
  const [notice, setNotice] = useState("");
  const ownOrders = useMemo(() => orders.filter((order) => Number(order.customer_id) === Number(profile?.id)), [orders, profile]);

  useEffect(() => {
    fetchData?.("/api/applications/mine").then(setApplications).catch(() => setApplications([]));
  }, [fetchData]);

  const active = view === "tasks" ? ownOrders : view === "responses" ? applications : deals;
  return <section className="orders-workspace marketplace-shell">
    <header className="market-topbar"><button type="button" className="market-brand" onClick={() => onNavigate("home")}><i>LT</i><b>Мои заказы</b></button><button type="button" className="market-create" onClick={() => onNavigate("create-order")}>+ Создать</button></header>
    <main className="orders-content">
      <div className="orders-intro"><p className="market-eyebrow">РАБОЧЕЕ ПРОСТРАНСТВО</p><h1>Задачи и сделки</h1><span>Обсуждение, отклики и оплата — в одном месте.</span></div>
      <div className="orders-summary"><div><b>{ownOrders.length}</b><span>моих задач</span></div><div><b>{applications.length}</b><span>моих откликов</span></div><div><b>{deals.length}</b><span>сделок</span></div></div>
      <nav className="orders-tabs"><button className={view === "tasks" ? "active" : ""} onClick={() => setView("tasks")}>Мои задачи</button><button className={view === "responses" ? "active" : ""} onClick={() => setView("responses")}>Отклики</button><button className={view === "deals" ? "active" : ""} onClick={() => setView("deals")}>Сделки</button></nav>
      <section className="orders-list">
        {!active.length && <div className="orders-empty"><b>{view === "tasks" ? "Нет созданных задач" : view === "responses" ? "Откликов пока нет" : "Сделок пока нет"}</b><span>{view === "tasks" ? "Создайте заказ — исполнители смогут откликнуться." : "Здесь появится история работы на LTeam."}</span>{view === "tasks" && <button onClick={() => onNavigate("create-order")}>Создать заказ</button>}</div>}
        {view === "tasks" && active.map((order) => <TaskRow key={order.id} order={order} onOpen={() => onChat({ kind: "order", item: order })} onApplications={async () => { try { const data = await fetchData(`/api/orders/${order.id}/applications`); setNotice(data.length ? `Откликов: ${data.length}` : "На заказ пока нет откликов."); } catch { setNotice("Не удалось загрузить отклики."); } }} />)}
        {view === "responses" && active.map((application) => <ApplicationRow key={application.id} item={application} onOpen={() => onChat({ kind: "order", item: { id: application.order_id, title: application.title } })} />)}
        {view === "deals" && active.map((deal) => <DealRow key={deal.id} deal={deal} onOpen={() => onChat({ kind: "deal", item: deal })} />)}
      </section>
    </main>
    {notice && <button className="market-notice" onClick={() => setNotice("")}>{notice}<span>×</span></button>}
    <nav className="market-dock"><button onClick={() => onNavigate("home")}>⌂<span>Главная</span></button><button onClick={() => onNavigate("catalog")}>▦<span>Маркет</span></button><button className="active">▤<span>Мои заказы</span></button><button onClick={() => onNavigate("profile")}>♙<span>Профиль</span></button></nav>
  </section>;
}

function TaskRow({ order, onOpen, onApplications }) {
  const active = ["active", "open", "approved"].includes(order.status);
  return <article className="work-row"><div className="work-row-head"><span className={active ? "work-status active" : "work-status"}>{active ? "Идёт поиск" : "На модерации"}</span><small>{order.category || "Заказ"}</small></div><h3>{titleFor(order)}</h3><p>{order.description || "Описание задачи"}</p><div className="work-meta"><b>до {money(order.budget)}</b><span>◷ {order.deadline || "Срок обсуждается"}</span></div><footer><button onClick={onApplications}>Отклики</button><button className="work-primary" onClick={onOpen}>Открыть чат</button></footer></article>;
}

function ApplicationRow({ item, onOpen }) {
  return <article className="work-row application-row"><div className="work-row-head"><span className="work-status">{item.status === "new" ? "Отправлен" : item.status}</span><small>{item.category || "Заказ"}</small></div><h3>{titleFor(item)}</h3><p>{item.comment || "Ваш отклик"}</p><div className="work-meta"><b>ваша цена {money(item.price)}</b><span>◷ {item.deadline || "Срок обсуждается"}</span></div><footer><span className="work-client">Заказчик: {item.customer_name || item.customer_username || "LTeam"}</span><button className="work-primary" onClick={onOpen}>Чат</button></footer></article>;
}

function DealRow({ deal, onOpen }) {
  const label = { waiting_payment: "Ожидаем оплату", in_progress: "В работе", checking: "Проверка", completed: "Завершена" }[deal.status] || "Сделка";
  return <article className="work-row deal-row"><div className="work-row-head"><span className="work-status active">{label}</span><small>Гарант LTeam</small></div><h3>{titleFor(deal)}</h3><div className="work-meta"><b>{money(deal.amount)}</b><span>Безопасная сделка</span></div><footer><span className="work-client">Все сообщения сохранены в сделке</span><button className="work-primary" onClick={onOpen}>Чат сделки</button></footer></article>;
}
