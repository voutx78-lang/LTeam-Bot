import { useCallback, useEffect, useState } from "react";
import Icon from "../icons";
import { EmptyState, PageHeader } from "../components";
import { api, dateLabel, money, send } from "../api";

const queues = [
  ["moderation", "Модерация", "grid"],
  ["tickets", "Поддержка", "message"],
  ["disputes", "Споры", "shield"],
];

export default function AdminScreen({ onBack, notify }) {
  const [summary, setSummary] = useState({});
  const [queue, setQueue] = useState("moderation");
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const load = useCallback(async (nextQueue = queue) => {
    setLoading(true);
    try {
      const [nextSummary, nextItems] = await Promise.all([api("/api/admin/summary"), api(nextQueue === "moderation" ? "/api/admin/moderation" : `/api/admin/queues/${nextQueue}`)]);
      setSummary(nextSummary); setItems(nextItems);
    } catch (error) { notify(error.message, "error"); }
    finally { setLoading(false); }
  }, [queue, notify]);
  useEffect(() => { load(queue); }, [queue, load]);
  async function moderate(item, action) {
    const note = action === "reject" ? window.prompt("Комментарий автору") || "" : "";
    try { await send(`/api/admin/moderation/${item.item_type}/${item.id}`, "POST", { action, note }); notify(action === "approve" ? "Публикация одобрена" : "Публикация отклонена"); await load(); } catch (error) { notify(error.message, "error"); }
  }
  return <section className="screen admin-screen"><PageHeader eyebrow="ТОЛЬКО ДЛЯ КОМАНДЫ" title="Центр управления" onBack={onBack}/><section className="admin-overview"><div><small>ТРЕБУЮТ ВНИМАНИЯ</small><b>{Number(summary.moderation || 0) + Number(summary.tickets || 0) + Number(summary.disputes || 0)}</b><span>задач в активных очередях</span></div><Icon name="shield" size={46}/></section><div className="admin-metrics"><span><b>{summary.moderation || 0}</b><small>модерация</small></span><span><b>{summary.tickets || 0}</b><small>поддержка</small></span><span><b>{summary.disputes || 0}</b><small>споры</small></span></div><nav className="admin-tabs">{queues.map(([id, label, icon]) => <button className={queue === id ? "active" : ""} key={id} onClick={() => setQueue(id)}><Icon name={icon}/><span>{label}</span></button>)}</nav>{loading ? <div className="admin-loading">Обновляем очередь…</div> : items.length ? <div className="admin-queue">{items.map((item) => <article key={`${queue}-${item.item_type || "item"}-${item.id}`}><header><span>{queue === "moderation" ? (item.item_type === "listing" ? "УСЛУГА" : "ЗАДАЧА") : `#${item.id}`}</span><small>{dateLabel(item.created_at)}</small></header><h3>{item.title || "Обращение пользователя"}</h3><p>{item.description || item.note || "Подробности доступны в журнале."}</p>{item.category && <div><span>{item.category}</span>{item.amount ? <b>{money(item.amount)}</b> : null}</div>}{queue === "moderation" && <footer><button onClick={() => moderate(item, "reject")}>Отклонить</button><button className="primary-button" onClick={() => moderate(item, "approve")}>Опубликовать</button></footer>}</article>)}</div> : <EmptyState icon="check" title="Очередь разобрана" text="Новых элементов, требующих решения, нет."/>}<section className="admin-note"><Icon name="shield"/><span><b>Действия администраторов журналируются</b><small>Решения должны быть объяснимыми и не зависеть от личных договорённостей.</small></span></section></section>;
}
