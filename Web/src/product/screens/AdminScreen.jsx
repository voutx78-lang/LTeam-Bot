import { useCallback, useEffect, useState } from "react";
import Icon from "../icons";
import { Avatar, EmptyState, PageHeader, Sheet } from "../components";
import { api, dateLabel, money, send } from "../api";

const sections = [
  ["overview", "Обзор", "chart"],
  ["moderation", "Модерация", "grid"],
  ["users", "Пользователи", "users"],
  ["tickets", "Поддержка", "message"],
  ["disputes", "Споры", "shield"],
  ["reports", "Жалобы", "alert"],
  ["audit", "Журнал", "list"],
];

const rejectionTemplates = [
  "Недостаточно информации: дополните описание и ожидаемый результат.",
  "Материалы или обложка не соответствуют правилам площадки.",
  "Публикация дублирует уже активное предложение.",
  "Категория выбрана неверно — исправьте её и отправьте повторно.",
  "Услуга не входит в разрешённые направления LT Market.",
];

export default function AdminScreen({ onBack, notify }) {
  const [section, setSection] = useState("overview");
  const [summary, setSummary] = useState({});
  const [analytics, setAnalytics] = useState({ totals: {}, days: [] });
  const [items, setItems] = useState([]);
  const [users, setUsers] = useState([]);
  const [audit, setAudit] = useState([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [userStatus, setUserStatus] = useState("all");
  const [selectedUser, setSelectedUser] = useState(null);
  const [decision, setDecision] = useState(null);
  const [userAction, setUserAction] = useState(null);

  const loadSummary = useCallback(async () => {
    const [nextSummary, nextAnalytics] = await Promise.all([api("/api/admin/summary"), api("/api/admin/analytics")]);
    setSummary(nextSummary); setAnalytics(nextAnalytics);
  }, []);

  const loadSection = useCallback(async (nextSection = section) => {
    setLoading(true);
    try {
      await loadSummary();
      if (nextSection === "moderation") setItems(await api("/api/admin/moderation"));
      else if (["tickets", "disputes", "reports"].includes(nextSection)) setItems(await api(`/api/admin/queues/${nextSection}`));
      else if (nextSection === "users") setUsers(await api(`/api/admin/users?query=${encodeURIComponent(query)}&status=${userStatus}`));
      else if (nextSection === "audit") setAudit(await api("/api/admin/audit"));
    } catch (error) { notify(error.message, "error"); }
    finally { setLoading(false); }
  }, [section, query, userStatus, loadSummary, notify]);

  useEffect(() => { loadSection(section); }, [section, loadSection]);

  async function moderate(payload) {
    try {
      await send(`/api/admin/moderation/${payload.item.item_type}/${payload.item.id}`, "POST", { action: payload.action, note: payload.note });
      setDecision(null); notify(payload.action === "approve" ? "Публикация одобрена" : "Решение отправлено автору"); await loadSection("moderation");
    } catch (error) { notify(error.message, "error"); }
  }

  async function openUser(userId) {
    try { setSelectedUser(await api(`/api/admin/users/${userId}`)); }
    catch (error) { notify(error.message, "error"); }
  }

  async function applyUserAction(payload) {
    try {
      await send(`/api/admin/users/${payload.user.user_id}/action`, "POST", { action: payload.action, reason: payload.reason });
      setUserAction(null); setSelectedUser(null); notify(payload.action === "unban" ? "Доступ восстановлен" : "Решение применено"); await loadSection("users");
    } catch (error) { notify(error.message, "error"); }
  }

  async function closeQueueItem(item) {
    try { await send(`/api/admin/queues/${section}/${item.id}/action`, "POST", { action: "close", note: "Обработано в Mini App" }); notify("Элемент закрыт"); await loadSection(section); }
    catch (error) { notify(error.message, "error"); }
  }

  const attention = Number(summary.moderation || 0) + Number(summary.tickets || 0) + Number(summary.disputes || 0) + Number(summary.reports || 0);
  return <section className="screen admin-screen">
    <PageHeader eyebrow="ЗАКРЫТЫЙ РАЗДЕЛ · ADMIN" title="Центр управления" onBack={onBack} action={<button className="icon-button" onClick={() => loadSection(section)} aria-label="Обновить"><Icon name="refresh"/></button>}/>
    <section className="admin-command"><div><span><i/> Система работает</span><small>ТРЕБУЮТ РЕШЕНИЯ</small><b>{attention}</b><p>Единая очередь публикаций, обращений и рисков.</p></div><i><Icon name="shield" size={36}/></i></section>
    <nav className="admin-sections" aria-label="Разделы админ-панели">{sections.map(([id, label, icon]) => <button className={section === id ? "active" : ""} key={id} onClick={() => setSection(id)}><Icon name={icon} size={18}/><span>{label}</span>{queueCount(id, summary) > 0 && <em>{queueCount(id, summary)}</em>}</button>)}</nav>
    {loading ? <AdminLoading/> : section === "overview" ? <Overview summary={summary} analytics={analytics} onOpen={setSection}/> : section === "users" ? <UsersView users={users} query={query} setQuery={setQuery} status={userStatus} setStatus={setUserStatus} onOpen={openUser}/> : section === "audit" ? <AuditView items={audit}/> : <QueueView section={section} items={items} onDecision={setDecision} onCloseItem={closeQueueItem}/>}
    <section className="admin-integrity"><Icon name="lock"/><div><b>Каждое действие сохраняется</b><small>Администратор, цель, причина и время доступны в журнале.</small></div></section>
    <ModerationDecision data={decision} onClose={() => setDecision(null)} onSubmit={moderate}/>
    <UserDetail data={selectedUser} onClose={() => setSelectedUser(null)} onAction={(action) => setUserAction({ user: selectedUser, action })}/>
    <UserAction data={userAction} onClose={() => setUserAction(null)} onSubmit={applyUserAction}/>
  </section>;
}

function queueCount(id, summary) {
  return ({ moderation: summary.moderation, tickets: summary.tickets, disputes: summary.disputes, reports: summary.reports })[id] || 0;
}

function AdminLoading() { return <div className="admin-skeleton"><i/><i/><i/></div>; }

function Overview({ summary, analytics, onOpen }) {
  const max = Math.max(1, ...analytics.days.map((day) => Number(day.users || 0) + Number(day.listings || 0) + Number(day.orders || 0)));
  const totals = analytics.totals || {};
  return <div className="admin-dashboard">
    <section className="admin-kpis"><article><small>ПОЛЬЗОВАТЕЛИ</small><b>{totals.users || 0}</b><span>всего аккаунтов</span></article><article><small>АКТИВНЫЙ КАТАЛОГ</small><b>{summary.active_listings || 0}</b><span>услуг опубликовано</span></article><article><small>ЗАВЕРШЕНО</small><b>{totals.completed || 0}</b><span>{totals.completion_rate || 0}% от сделок</span></article><article><small>ОТЗЫВЫ</small><b>{totals.reviews || 0}</b><span>после заказов</span></article></section>
    <section className="admin-chart"><header><div><small>ПОСЛЕДНИЕ 7 ДНЕЙ</small><h2>Активность площадки</h2></div><span>пользователи и публикации</span></header><div>{analytics.days.map((day) => { const value = Number(day.users || 0) + Number(day.listings || 0) + Number(day.orders || 0); return <span key={day.day}><i style={{ height: `${Math.max(8, value / max * 100)}%` }}/><b>{value}</b><small>{new Date(`${day.day}T00:00:00`).toLocaleDateString("ru-RU", { weekday: "short" })}</small></span>; })}</div></section>
    <section className="admin-priority"><header><div><small>ОЧЕРЕДИ</small><h2>Приоритеты сегодня</h2></div></header>{[["moderation", "Проверить публикации", summary.moderation, "grid"], ["tickets", "Ответить поддержке", summary.tickets, "message"], ["disputes", "Разобрать споры", summary.disputes, "shield"], ["reports", "Проверить жалобы", summary.reports, "alert"]].map(([id, title, count, icon]) => <button key={id} onClick={() => onOpen(id)}><i><Icon name={icon}/></i><span><b>{title}</b><small>{count ? `${count} ждут решения` : "Очередь разобрана"}</small></span><em className={count ? "hot" : ""}>{count || <Icon name="check" size={15}/>}</em><Icon name="chevron" size={17}/></button>)}</section>
  </div>;
}

function QueueView({ section, items, onDecision, onCloseItem }) {
  const labels = { moderation: ["Модерация", "Проверяйте содержание, категорию и полноту карточки."], tickets: ["Поддержка", "Открытые обращения пользователей."], disputes: ["Споры", "Фиксируйте решение после изучения истории заказа."], reports: ["Жалобы", "Сигналы о публикациях и пользователях."] };
  const [title, text] = labels[section] || ["Очередь", "Элементы, требующие внимания."];
  return <section className="admin-worklist"><header><div><small>РАБОЧАЯ ОЧЕРЕДЬ</small><h2>{title}</h2><p>{text}</p></div><span>{items.length}</span></header>{items.length ? items.map((item) => <article key={`${section}-${item.item_type || "item"}-${item.id}`}><header><div><span>{item.item_type === "listing" ? "УСЛУГА" : item.item_type === "order" ? "ЗАДАЧА" : `#${item.id}`}</span><small>{dateLabel(item.created_at)}</small></div>{item.amount ? <b>{money(item.amount)}</b> : null}</header><h3>{item.title || "Обращение пользователя"}</h3>{item.author_name && <div className="admin-author"><Avatar name={item.author_name} size="sm"/><span><b>{item.author_name}</b><small>ID {item.author_id}</small></span></div>}<p>{item.description || item.note || "Подробности не указаны."}</p>{item.category && <div className="admin-item-meta"><span>{item.category}</span><em>{item.status}</em></div>}{section === "moderation" ? <footer><button onClick={() => onDecision({ item, action: "reject" })}>Отклонить</button><button className="primary-button" onClick={() => onDecision({ item, action: "approve" })}>Опубликовать</button></footer> : ["tickets", "reports"].includes(section) ? <footer className="single"><button className="primary-button" onClick={() => onCloseItem(item)}>Отметить обработанным</button></footer> : <div className="admin-readonly"><Icon name="shield" size={17}/> Решение по спору принимается после проверки всей истории сделки в Telegram-админке.</div>}</article>) : <EmptyState icon="check" title="Очередь разобрана" text="Новых элементов, требующих решения, нет."/>}</section>;
}

function UsersView({ users, query, setQuery, status, setStatus, onOpen }) {
  return <section className="admin-users"><header><div><small>ПОЛЬЗОВАТЕЛИ</small><h2>Контроль аккаунтов</h2></div><span>{users.length}</span></header><label className="admin-search"><Icon name="search"/><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Имя, username или Telegram ID"/></label><div className="admin-user-filters">{[["all", "Все"], ["active", "Активные"], ["banned", "Заблокированные"]].map(([id, label]) => <button className={status === id ? "active" : ""} key={id} onClick={() => setStatus(id)}>{label}</button>)}</div>{users.length ? <div className="admin-user-list">{users.map((user) => <button key={user.user_id} onClick={() => onOpen(user.user_id)}><Avatar src={user.avatar_url} name={user.display_name} verified={Boolean(user.verified)}/><span><b>{user.display_name}{user.is_admin && <em className="admin-badge">Admin</em>}</b><small>{user.username ? `@${user.username}` : `ID ${user.user_id}`}</small><i>{user.completed_count} завершено · {user.warnings_count} предупреждений</i></span><em className={user.banned ? "banned" : "active"}>{user.banned ? "Блок" : "Активен"}</em><Icon name="chevron" size={17}/></button>)}</div> : <EmptyState icon="users" title="Ничего не найдено" text="Измените запрос или фильтр."/>}</section>;
}

function AuditView({ items }) {
  return <section className="admin-audit"><header><div><small>ЖУРНАЛ ДЕЙСТВИЙ</small><h2>История решений</h2><p>Неизменяемый след действий команды.</p></div><span>{items.length}</span></header>{items.length ? <div>{items.map((item) => <article key={item.id}><i><Icon name={auditIcon(item.action)} size={17}/></i><span><b>{auditLabel(item.action)}</b><p>{item.actor_name} → {item.target_name}</p>{item.details && <small>{item.details}</small>}</span><time>{dateLabel(item.created_at)}</time></article>)}</div> : <EmptyState icon="list" title="Журнал пуст" text="Действия администраторов появятся здесь."/>}</section>;
}

function auditIcon(action = "") { return action.includes("ban") ? "lock" : action.includes("warn") || action.includes("reject") ? "alert" : action.includes("approve") ? "check" : "shield"; }
function auditLabel(action = "") { const map = { miniapp_warn: "Предупреждение", miniapp_ban: "Блокировка", miniapp_unban: "Разблокировка" }; return map[action] || action.replaceAll("_", " "); }

function ModerationDecision({ data, onClose, onSubmit }) {
  const [note, setNote] = useState("");
  useEffect(() => setNote(""), [data]);
  if (!data) return null;
  const rejecting = data.action === "reject";
  return <Sheet open title={rejecting ? "Причина отклонения" : "Подтвердить публикацию"} onClose={onClose} className="admin-decision-sheet"><div className="admin-decision"><section><Icon name={rejecting ? "alert" : "check"}/><div><b>{data.item.title}</b><small>{data.item.item_type === "listing" ? "Услуга" : "Задача"} · #{data.item.id}</small></div></section>{rejecting ? <><p>Выберите понятную причину — автор получит её в Telegram.</p><div className="decision-templates">{rejectionTemplates.map((template) => <button className={note === template ? "active" : ""} key={template} onClick={() => setNote(template)}>{template}</button>)}</div><label className="field"><span>Комментарий</span><textarea value={note} onChange={(event) => setNote(event.target.value)} placeholder="Что нужно исправить?"/></label></> : <p>Карточка станет видна всем пользователям каталога. Решение сохранится в журнале.</p>}<button className={`wide ${rejecting ? "danger-button" : "primary-button"}`} disabled={rejecting && note.trim().length < 5} onClick={() => onSubmit({ ...data, note })}>{rejecting ? "Отклонить и уведомить" : "Одобрить публикацию"}</button></div></Sheet>;
}

function UserDetail({ data, onClose, onAction }) {
  if (!data) return null;
  return <Sheet open title="Карточка пользователя" onClose={onClose} className="admin-user-sheet"><div className="admin-user-detail"><section className="user-detail-head"><Avatar src={data.avatar_url} name={data.display_name} size="lg" verified={Boolean(data.verified)}/><div><small>{data.is_admin ? "КОМАНДА LT MARKET" : "ПОЛЬЗОВАТЕЛЬ"}</small><h2>{data.display_name}{data.is_admin && <em className="admin-badge">Admin</em>}</h2><p>{data.username ? `@${data.username}` : `Telegram ID ${data.user_id}`}</p></div></section><div className="user-detail-state"><span className={data.banned ? "banned" : "active"}><i/>{data.banned ? "Доступ ограничен" : "Аккаунт активен"}</span><small>Регистрация: {dateLabel(data.created_at)}</small></div><section className="user-detail-metrics"><span><b>{data.listings_count}</b><small>услуг</small></span><span><b>{data.orders_count}</b><small>задач</small></span><span><b>{data.completed_count}</b><small>завершено</small></span><span><b>{data.reports_count}</b><small>жалоб</small></span></section>{data.bio && <article className="user-detail-bio"><small>О ПРОФИЛЕ</small><p>{data.bio}</p></article>}<section className="user-risk"><header><b>История рисков</b><span>{data.warnings_count}</span></header>{data.ban_reason && <p><Icon name="lock" size={16}/> {data.ban_reason}</p>}{data.warnings?.length ? data.warnings.map((warning) => <p key={warning.id}><Icon name="alert" size={16}/> {warning.reason}</p>) : <small>Предупреждений нет.</small>}</section>{!data.is_admin && <footer><button onClick={() => onAction("warn")}>Предупредить</button><button className={data.banned ? "primary-button" : "danger-button"} onClick={() => onAction(data.banned ? "unban" : "ban")}>{data.banned ? "Восстановить доступ" : "Заблокировать"}</button></footer>}</div></Sheet>;
}

function UserAction({ data, onClose, onSubmit }) {
  const [reason, setReason] = useState("");
  useEffect(() => setReason(""), [data]);
  if (!data) return null;
  const needsReason = data.action !== "unban";
  const title = { warn: "Вынести предупреждение", ban: "Ограничить доступ", unban: "Восстановить доступ" }[data.action];
  return <Sheet open title={title} onClose={onClose} className="admin-action-sheet"><div className="admin-user-action"><section><Avatar src={data.user.avatar_url} name={data.user.display_name}/><div><b>{data.user.display_name}</b><small>ID {data.user.user_id}</small></div></section><p>{data.action === "warn" ? "Предупреждение появится в истории и будет отправлено пользователю." : data.action === "ban" ? "Пользователь потеряет доступ к боту и маркетплейсу до разблокировки." : "Пользователь снова сможет работать с LT Market."}</p>{needsReason && <label className="field"><span>Причина</span><textarea value={reason} onChange={(event) => setReason(event.target.value)} placeholder="Опишите нарушение понятным языком"/></label>}<button className={`wide ${data.action === "ban" ? "danger-button" : "primary-button"}`} disabled={needsReason && reason.trim().length < 5} onClick={() => onSubmit({ ...data, reason })}>{title}</button></div></Sheet>;
}
