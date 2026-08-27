import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "../../AdminConsole.css";
import Icon from "../icons";
import { Avatar, EmptyState, PageHeader, Sheet } from "../components";
import { api, dateLabel, money, send } from "../api";
import { adminPreview, isAdminPreview } from "../adminPreview";

const sections = [
  ["overview", "Обзор", "chart"],
  ["moderation", "Модерация", "grid"],
  ["users", "Люди", "users"],
  ["tickets", "Поддержка", "message"],
  ["disputes", "Споры", "shield"],
  ["reports", "Жалобы", "alert"],
  ["finance", "Stars", "star"],
  ["system", "Система", "settings"],
  ["audit", "Журнал", "list"],
];

const rejectionTemplates = [
  "Дополните описание и укажите конкретный результат работы.",
  "Материалы или обложка не соответствуют правилам площадки.",
  "Публикация дублирует уже активное предложение.",
  "Выберите подходящую категорию и отправьте карточку повторно.",
  "Услуга не входит в разрешённые направления LT Market.",
];

const readAdmin = (path) => isAdminPreview() ? Promise.resolve(adminPreview(path)) : api(path);
const writeAdmin = (path, method = "POST", payload = {}) => isAdminPreview() ? Promise.resolve({ ok: true }) : send(path, method, payload);

export default function AdminScreen({ onBack, notify }) {
  const [section, setSection] = useState("overview");
  const [summary, setSummary] = useState({});
  const [analytics, setAnalytics] = useState({ totals: {}, days: [] });
  const [items, setItems] = useState([]);
  const [users, setUsers] = useState([]);
  const [audit, setAudit] = useState([]);
  const [finance, setFinance] = useState({ totals: {}, items: [] });
  const [system, setSystem] = useState({ health: {}, errors: [] });
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [busy, setBusy] = useState(false);
  const [query, setQuery] = useState("");
  const [userStatus, setUserStatus] = useState("all");
  const [selectedUser, setSelectedUser] = useState(null);
  const [decision, setDecision] = useState(null);
  const [userAction, setUserAction] = useState(null);
  const [queueAction, setQueueAction] = useState(null);
  const [dispute, setDispute] = useState(null);
  const [refund, setRefund] = useState(null);
  const [updatedAt, setUpdatedAt] = useState(null);
  const activeSectionRef = useRef(null);

  const loadSummary = useCallback(async () => {
    const [nextSummary, nextAnalytics] = await Promise.all([
      readAdmin("/api/admin/summary"), readAdmin("/api/admin/analytics"),
    ]);
    setSummary(nextSummary || {});
    setAnalytics(nextAnalytics || { totals: {}, days: [] });
  }, []);

  const loadSection = useCallback(async (nextSection = section, silent = false) => {
    if (silent) setRefreshing(true); else setLoading(true);
    try {
      await loadSummary();
      if (nextSection === "moderation") setItems(await readAdmin("/api/admin/moderation"));
      else if (["tickets", "disputes", "reports"].includes(nextSection)) setItems(await readAdmin(`/api/admin/queues/${nextSection}`));
      else if (nextSection === "users") setUsers(await readAdmin(`/api/admin/users?query=${encodeURIComponent(query)}&status=${userStatus}`));
      else if (nextSection === "audit") setAudit(await readAdmin("/api/admin/audit"));
      else if (nextSection === "finance") setFinance(await readAdmin("/api/admin/payments/stars"));
      else if (nextSection === "system") {
        const [health, runtime] = await Promise.all([readAdmin("/api/health"), readAdmin("/api/admin/runtime-errors")]);
        setSystem({ health: health || {}, errors: runtime?.errors || [] });
      }
      setUpdatedAt(new Date());
    } catch (error) { notify(error.message, "error"); }
    finally { setLoading(false); setRefreshing(false); }
  }, [section, query, userStatus, loadSummary, notify]);

  useEffect(() => {
    const timer = window.setTimeout(() => loadSection(section), section === "users" ? 220 : 0);
    return () => window.clearTimeout(timer);
  }, [section, loadSection]);

  useEffect(() => {
    activeSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "center" });
  }, [section]);

  async function execute(action, success, reload = section) {
    setBusy(true);
    try { await action(); notify(success); await loadSection(reload, true); return true; }
    catch (error) { notify(error.message, "error"); return false; }
    finally { setBusy(false); }
  }

  async function moderate(payload) {
    const ok = await execute(
      () => writeAdmin(`/api/admin/moderation/${payload.item.item_type}/${payload.item.id}`, "POST", { action: payload.action, note: payload.note }),
      payload.action === "approve" ? "Публикация одобрена" : "Правки отправлены автору",
      "moderation",
    );
    if (ok) setDecision(null);
  }

  async function openUser(userId) {
    try { setSelectedUser(await readAdmin(`/api/admin/users/${userId}`)); }
    catch (error) { notify(error.message, "error"); }
  }

  async function applyUserAction(payload) {
    const labels = { warn: "Предупреждение отправлено", ban: "Доступ ограничен", unban: "Доступ восстановлен", verify: "Профиль подтверждён", unverify: "Отметка снята" };
    const ok = await execute(
      () => writeAdmin(`/api/admin/users/${payload.user.user_id}/action`, "POST", { action: payload.action, reason: payload.reason }),
      labels[payload.action], "users",
    );
    if (ok) { setUserAction(null); setSelectedUser(null); }
  }

  async function applyQueueAction(payload) {
    const ok = await execute(
      () => writeAdmin(`/api/admin/queues/${payload.section}/${payload.item.id}/action`, "POST", { action: payload.action, note: payload.note }),
      payload.action === "reply" ? "Ответ отправлен в Telegram" : "Элемент закрыт",
      payload.section,
    );
    if (ok) setQueueAction(null);
  }

  async function openDispute(item) {
    setBusy(true);
    try { setDispute(await readAdmin(`/api/admin/disputes/${item.id}`)); }
    catch (error) { notify(error.message, "error"); }
    finally { setBusy(false); }
  }

  async function resolveDispute(payload) {
    const ok = await execute(
      () => writeAdmin(`/api/admin/disputes/${payload.id}/resolve`, "POST", { outcome: payload.outcome, note: payload.note }),
      "Решение сохранено и отправлено участникам", "disputes",
    );
    if (ok) setDispute(null);
  }

  async function refundStars(payment) {
    const ok = await execute(
      () => writeAdmin(`/api/admin/payments/stars/${payment.id}/refund`, "POST", {}),
      "Stars возвращены пользователю", "finance",
    );
    if (ok) setRefund(null);
  }

  const attention = Number(summary.moderation || 0) + Number(summary.tickets || 0) + Number(summary.disputes || 0) + Number(summary.reports || 0) + Number(summary.star_pending || 0);
  return <section className="screen admin-screen admin-console">
    <PageHeader eyebrow="PRIVATE · LT MARKET" title="Центр управления" onBack={onBack} action={<button className={`icon-button ${refreshing ? "spinning" : ""}`} onClick={() => loadSection(section, true)} aria-label="Обновить"><Icon name="refresh"/></button>}/>
    <section className="admin-command admin-command-v2">
      <div><span><i/> Система на связи</span><small>ТРЕБУЮТ РЕШЕНИЯ</small><b>{attention}</b><p>Контроль публикаций, обращений, рисков и операций в одном месте.</p></div>
      <i><Icon name="shield" size={34}/></i>
    </section>
    <div className="admin-livebar"><span><i/> Данные защищены</span><small>{updatedAt ? `обновлено ${updatedAt.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}` : "загрузка"}</small></div>
    <nav className="admin-sections admin-sections-v2" aria-label="Разделы админ-панели">
      {sections.map(([id, label, icon]) => <button ref={section === id ? activeSectionRef : undefined} className={section === id ? "active" : ""} key={id} onClick={() => setSection(id)}><Icon name={icon} size={17}/><span>{label}</span>{queueCount(id, summary) > 0 && <em>{queueCount(id, summary)}</em>}</button>)}
    </nav>
    <div className="admin-section-stage" key={section}>
      {loading ? <AdminLoading/> : section === "overview" ? <Overview summary={summary} analytics={analytics} onOpen={setSection}/> : section === "users" ? <UsersView users={users} query={query} setQuery={setQuery} status={userStatus} setStatus={setUserStatus} onOpen={openUser}/> : section === "audit" ? <AuditView items={audit}/> : section === "finance" ? <FinanceView data={finance} onRefund={setRefund}/> : section === "system" ? <SystemView data={system}/> : <QueueView section={section} items={items} busy={busy} onDecision={setDecision} onAction={setQueueAction} onDispute={openDispute}/>}
    </div>
    <section className="admin-integrity"><Icon name="lock"/><div><b>Действия не остаются без следа</b><small>Решение, причина, администратор и время фиксируются в журнале.</small></div></section>
    <ModerationDecision data={decision} busy={busy} onClose={() => setDecision(null)} onSubmit={moderate}/>
    <UserDetail data={selectedUser} onClose={() => setSelectedUser(null)} onAction={(action) => setUserAction({ user: selectedUser, action })}/>
    <UserAction data={userAction} busy={busy} onClose={() => setUserAction(null)} onSubmit={applyUserAction}/>
    <QueueAction data={queueAction} busy={busy} onClose={() => setQueueAction(null)} onSubmit={applyQueueAction}/>
    <DisputeDetail data={dispute} busy={busy} onClose={() => setDispute(null)} onResolve={resolveDispute}/>
    <RefundSheet data={refund} busy={busy} onClose={() => setRefund(null)} onSubmit={refundStars}/>
  </section>;
}

function queueCount(id, summary) {
  return ({ moderation: summary.moderation, tickets: summary.tickets, disputes: summary.disputes, reports: summary.reports, finance: summary.star_pending, system: summary.runtime_errors })[id] || 0;
}

function AdminLoading() { return <div className="admin-skeleton"><i/><i/><i/></div>; }

function Overview({ summary, analytics, onOpen }) {
  const days = analytics.days || [];
  const totals = analytics.totals || {};
  const max = Math.max(1, ...days.map((day) => Number(day.users || 0) + Number(day.listings || 0) + Number(day.orders || 0)));
  const priorities = [
    ["moderation", "Проверить публикации", summary.moderation, "grid"],
    ["tickets", "Ответить пользователям", summary.tickets, "message"],
    ["disputes", "Разобрать споры", summary.disputes, "shield"],
    ["reports", "Проверить жалобы", summary.reports, "alert"],
  ];
  return <div className="admin-dashboard">
    <section className="admin-kpis admin-kpis-v2">
      <article><small>ПОЛЬЗОВАТЕЛИ</small><b>{totals.users || 0}</b><span>всего аккаунтов</span></article>
      <article><small>АКТИВНЫЙ КАТАЛОГ</small><b>{summary.active_listings || 0}</b><span>услуг опубликовано</span></article>
      <article><small>ЗАВЕРШЕНО</small><b>{totals.completed || 0}</b><span>{totals.completion_rate || 0}% от сделок</span></article>
      <article><small>TELEGRAM STARS</small><b>{totals.paid_stars || 0}</b><span>успешно оплачено</span></article>
    </section>
    <section className="admin-health-grid">
      <article><Icon name="briefcase"/><span><b>{totals.active_orders || 0}</b><small>активных задач</small></span></article>
      <article><Icon name="shield"/><span><b>{totals.dispute_rate || 0}%</b><small>доля споров</small></span></article>
      <article><Icon name="message"/><span><b>{totals.open_tickets || 0}</b><small>обращений</small></span></article>
    </section>
    <section className="admin-chart"><header><div><small>ПОСЛЕДНИЕ 7 ДНЕЙ</small><h2>Пульс площадки</h2></div><span>новые пользователи и публикации</span></header><div>{days.map((day) => { const value = Number(day.users || 0) + Number(day.listings || 0) + Number(day.orders || 0); return <span key={day.day}><i style={{ height: `${Math.max(8, value / max * 100)}%` }}/><b>{value}</b><small>{new Date(`${day.day}T00:00:00`).toLocaleDateString("ru-RU", { weekday: "short" })}</small></span>; })}</div></section>
    <section className="admin-priority"><header><div><small>ОЧЕРЕДИ</small><h2>Фокус на сегодня</h2></div></header>{priorities.map(([id, title, count, icon]) => <button key={id} onClick={() => onOpen(id)}><i><Icon name={icon}/></i><span><b>{title}</b><small>{count ? `${count} ждут решения` : "Очередь разобрана"}</small></span><em className={count ? "hot" : ""}>{count || <Icon name="check" size={15}/>}</em><Icon name="chevron" size={17}/></button>)}</section>
  </div>;
}

function QueueView({ section, items, busy, onDecision, onAction, onDispute }) {
  const [search, setSearch] = useState("");
  const [kind, setKind] = useState("all");
  const labels = { moderation: ["Модерация", "Оцените содержание, категорию и полноту карточки."], tickets: ["Поддержка", "Ответьте пользователю прямо из панели."], disputes: ["Споры", "Изучите факты и зафиксируйте обоснованное решение."], reports: ["Жалобы", "Проверьте сигнал и связанную публикацию."] };
  const [title, text] = labels[section] || ["Очередь", "Элементы, требующие внимания."];
  const filtered = useMemo(() => items.filter((item) => {
    const haystack = `${item.title || ""} ${item.author_name || ""} ${item.description || item.note || ""}`.toLowerCase();
    return haystack.includes(search.toLowerCase()) && (kind === "all" || item.item_type === kind);
  }), [items, search, kind]);
  return <section className="admin-worklist admin-worklist-v2">
    <header><div><small>РАБОЧАЯ ОЧЕРЕДЬ</small><h2>{title}</h2><p>{text}</p></div><span>{filtered.length}</span></header>
    {items.length > 2 && <label className="admin-search"><Icon name="search"/><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Найти в очереди"/></label>}
    {section === "moderation" && <div className="admin-user-filters">{[["all", "Все"], ["listing", "Услуги"], ["order", "Задачи"]].map(([id, label]) => <button className={kind === id ? "active" : ""} key={id} onClick={() => setKind(id)}>{label}</button>)}</div>}
    {filtered.length ? filtered.map((item) => <QueueCard key={`${section}-${item.item_type || "item"}-${item.id}`} section={section} item={item} busy={busy} onDecision={onDecision} onAction={onAction} onDispute={onDispute}/>) : <EmptyState icon="check" title="Очередь разобрана" text="Новых элементов, требующих решения, нет."/>}
  </section>;
}

function QueueCard({ section, item, busy, onDecision, onAction, onDispute }) {
  const copy = item.description || item.note || "Подробности не указаны.";
  const completion = Math.min(100, [item.title, copy, item.category, item.amount].filter(Boolean).length * 25);
  return <article>
    <header><div><span>{item.item_type === "listing" ? "УСЛУГА" : item.item_type === "order" ? "ЗАДАЧА" : `#${item.id}`}</span><small>{dateLabel(item.created_at)}</small></div>{item.amount ? <b>{money(item.amount)}</b> : <em className={`admin-state ${item.status}`}>{statusLabel(item.status)}</em>}</header>
    <h3>{item.title || "Обращение пользователя"}</h3>
    {item.author_name && <div className="admin-author"><Avatar name={item.author_name} size="sm"/><span><b>{item.author_name}</b><small>Telegram ID {item.author_id}</small></span></div>}
    <p>{copy}</p>
    {section === "moderation" && <div className="moderation-quality"><span><i style={{ width: `${completion}%` }}/></span><small>Карточка заполнена на {completion}%</small></div>}
    {(item.category || item.target_title) && <div className="admin-item-meta"><span>{item.category || item.target_title}</span><em>{statusLabel(item.status)}</em></div>}
    {section === "moderation" ? <footer><button disabled={busy} onClick={() => onDecision({ item, action: "reject" })}>На доработку</button><button disabled={busy} className="primary-button" onClick={() => onDecision({ item, action: "approve" })}>Опубликовать</button></footer> : section === "tickets" ? <footer><button disabled={busy} onClick={() => onAction({ section, item, action: "close" })}>Закрыть</button><button disabled={busy} className="primary-button" onClick={() => onAction({ section, item, action: "reply" })}>Ответить</button></footer> : section === "reports" ? <footer className="single"><button disabled={busy} className="primary-button" onClick={() => onAction({ section, item, action: "close" })}>Рассмотреть и закрыть</button></footer> : <footer className="single"><button disabled={busy} className="primary-button" onClick={() => onDispute(item)}>Открыть материалы спора</button></footer>}
  </article>;
}

function UsersView({ users, query, setQuery, status, setStatus, onOpen }) {
  return <section className="admin-users"><header><div><small>ПОЛЬЗОВАТЕЛИ</small><h2>Аккаунты и доверие</h2></div><span>{users.length}</span></header><label className="admin-search"><Icon name="search"/><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Имя, username или Telegram ID"/></label><div className="admin-user-filters">{[["all", "Все"], ["active", "Активные"], ["banned", "Заблокированные"]].map(([id, label]) => <button className={status === id ? "active" : ""} key={id} onClick={() => setStatus(id)}>{label}</button>)}</div>{users.length ? <div className="admin-user-list">{users.map((user) => <button key={user.user_id} onClick={() => onOpen(user.user_id)}><Avatar src={user.avatar_url} name={user.display_name} verified={Boolean(user.verified)}/><span><b>{user.display_name}{user.is_admin && <em className="admin-badge">Admin</em>}</b><small>{user.username ? `@${user.username}` : `ID ${user.user_id}`}</small><i>{user.completed_count} завершено · {user.warnings_count} предупреждений</i></span><em className={user.banned ? "banned" : "active"}>{user.banned ? "Блок" : user.verified ? "Verified" : "Активен"}</em><Icon name="chevron" size={17}/></button>)}</div> : <EmptyState icon="users" title="Ничего не найдено" text="Измените запрос или фильтр."/>}</section>;
}

function FinanceView({ data, onRefund }) {
  const totals = data.totals || {};
  return <section className="admin-finance"><header className="admin-view-heading"><div><small>TELEGRAM STARS</small><h2>Операции и возвраты</h2><p>Stars используются только для продвижения объявлений.</p></div><span>{totals.operations || 0}</span></header><div className="finance-kpis"><article><Icon name="star"/><b>{totals.paid_stars || 0}</b><small>получено</small></article><article><Icon name="clock"/><b>{totals.pending || 0}</b><small>ожидают</small></article><article><Icon name="refresh"/><b>{totals.refunded_stars || 0}</b><small>возвращено</small></article></div><div className="finance-list">{data.items?.length ? data.items.map((item) => <article key={item.id}><header><Avatar name={item.user_name} size="sm"/><span><b>{item.user_name}</b><small>{item.listing_title}</small></span><strong>{item.stars} <Icon name="star" size={13}/></strong></header><footer><span className={`payment-status ${item.status}`}>{paymentStatus(item.status)}</span><small>{dateLabel(item.paid_at || item.created_at)}</small>{item.status === "paid" && data.can_refund && <button onClick={() => onRefund(item)}>Вернуть</button>}</footer></article>) : <EmptyState icon="star" title="Операций пока нет" text="Покупки продвижения появятся здесь."/>}</div><aside className="admin-readonly"><Icon name="lock" size={17}/> Возврат выполняется через Telegram API и доступен только владельцу.</aside></section>;
}

function SystemView({ data }) {
  const health = data.health || {};
  const [expanded, setExpanded] = useState(null);
  const cards = [["API", health.ok ? "Работает" : "Недоступен", "shield", health.ok], ["Telegram Stars", health.stars_enabled ? "Подключены" : "Отключены", "star", health.stars_enabled], ["Хранилище", health.storage || "неизвестно", "file", Boolean(health.storage)], ["Версия", health.version || "—", "settings", true]];
  return <section className="admin-system"><header className="admin-view-heading"><div><small>СОСТОЯНИЕ СЕРВИСА</small><h2>Диагностика</h2><p>Ключевые компоненты и последние ошибки приложения.</p></div><span>{data.errors?.length || 0}</span></header><div className="system-grid">{cards.map(([title, value, icon, ok]) => <article key={title}><i className={ok ? "ok" : "bad"}><Icon name={icon}/></i><span><small>{title}</small><b>{value}</b></span></article>)}</div><section className="runtime-errors"><header><div><small>ПОСЛЕДНИЕ ОШИБКИ</small><h3>Журнал диагностики</h3></div><em>{data.errors?.length || 0}</em></header>{data.errors?.length ? data.errors.map((error) => <article key={error.reference || error.id}><button onClick={() => setExpanded(expanded === error.reference ? null : error.reference)}><i><Icon name="alert" size={17}/></i><span><b>{error.error_type || "Runtime error"}</b><small>Код {error.reference} · {dateLabel(error.created_at)}</small></span><Icon name="chevron" className={expanded === error.reference ? "rotate" : ""}/></button>{expanded === error.reference && <div><p>{error.message}</p><small>{error.kind} · ID {error.user_id || "—"} · {error.command || "без команды"}</small>{error.traceback && <pre>{error.traceback}</pre>}</div>}</article>) : <EmptyState icon="check" title="Ошибок нет" text="За последнее время критических событий не зафиксировано."/>}</section></section>;
}

function AuditView({ items }) {
  const [query, setQuery] = useState("");
  const filtered = items.filter((item) => `${item.actor_name} ${item.target_name} ${item.action} ${item.details}`.toLowerCase().includes(query.toLowerCase()));
  return <section className="admin-audit"><header><div><small>ЖУРНАЛ ДЕЙСТВИЙ</small><h2>История решений</h2><p>След действий команды нельзя скрыть из интерфейса.</p></div><span>{filtered.length}</span></header><label className="admin-search"><Icon name="search"/><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Администратор, действие или цель"/></label>{filtered.length ? <div className="audit-list">{filtered.map((item) => <article key={item.id}><i><Icon name={auditIcon(item.action)} size={17}/></i><span><b>{auditLabel(item.action)}</b><p>{item.actor_name} → {item.target_name}</p>{item.details && <small>{item.details}</small>}</span><time>{dateLabel(item.created_at)}</time></article>)}</div> : <EmptyState icon="list" title="Записей не найдено" text="Измените поисковый запрос."/>}</section>;
}

function ModerationDecision({ data, busy, onClose, onSubmit }) {
  const [note, setNote] = useState("");
  useEffect(() => setNote(""), [data]);
  if (!data) return null;
  const rejecting = data.action === "reject";
  return <Sheet open title={rejecting ? "Вернуть на доработку" : "Опубликовать карточку"} onClose={onClose} className="admin-decision-sheet"><div className="admin-decision"><section><Icon name={rejecting ? "alert" : "check"}/><div><b>{data.item.title}</b><small>{data.item.item_type === "listing" ? "Услуга" : "Задача"} · #{data.item.id}</small></div></section>{rejecting ? <><p>Автор увидит причину в Telegram и сможет исправить карточку.</p><div className="decision-templates">{rejectionTemplates.map((template) => <button className={note === template ? "active" : ""} key={template} onClick={() => setNote(template)}>{template}</button>)}</div><label className="field"><span>Комментарий</span><textarea value={note} onChange={(event) => setNote(event.target.value)} placeholder="Что именно нужно исправить?"/></label></> : <p>Карточка станет доступна в каталоге. Решение сохранится в журнале действий.</p>}<button className={`wide ${rejecting ? "danger-button" : "primary-button"}`} disabled={busy || (rejecting && note.trim().length < 5)} onClick={() => onSubmit({ ...data, note })}>{busy ? "Сохраняем…" : rejecting ? "Отправить на доработку" : "Подтвердить публикацию"}</button></div></Sheet>;
}

function UserDetail({ data, onClose, onAction }) {
  if (!data) return null;
  return <Sheet open title="Карточка пользователя" onClose={onClose} className="admin-user-sheet"><div className="admin-user-detail"><section className="user-detail-head"><Avatar src={data.avatar_url} name={data.display_name} size="lg" verified={Boolean(data.verified)}/><div><small>{data.is_admin ? "КОМАНДА LT MARKET" : data.verified ? "ПРОФИЛЬ ПОДТВЕРЖДЁН" : "ПОЛЬЗОВАТЕЛЬ"}</small><h2>{data.display_name}{data.is_admin && <em className="admin-badge">Admin</em>}</h2><p>{data.username ? `@${data.username}` : `Telegram ID ${data.user_id}`}</p></div></section><div className="user-detail-state"><span className={data.banned ? "banned" : "active"}><i/>{data.banned ? "Доступ ограничен" : "Аккаунт активен"}</span><small>{dateLabel(data.created_at)}</small></div><section className="user-detail-metrics"><span><b>{data.listings_count || 0}</b><small>услуг</small></span><span><b>{data.orders_count || 0}</b><small>задач</small></span><span><b>{data.completed_count || 0}</b><small>сделок</small></span><span><b>{data.reports_count || 0}</b><small>жалоб</small></span></section>{data.bio && <article className="user-detail-bio"><small>О ПРОФИЛЕ</small><p>{data.bio}</p></article>}<section className="user-risk"><header><b>История рисков</b><span>{data.warnings_count || 0}</span></header>{data.ban_reason && <p><Icon name="lock" size={16}/> {data.ban_reason}</p>}{data.warnings?.length ? data.warnings.map((warning) => <p key={warning.id}><Icon name="alert" size={16}/> {warning.reason}</p>) : <small>Подтверждённых нарушений нет.</small>}</section>{!data.is_admin && <><div className="user-trust-action"><span><b>Знак доверия</b><small>Отметка видна в профиле и карточках услуг.</small></span><button onClick={() => onAction(data.verified ? "unverify" : "verify")}>{data.verified ? "Снять" : "Подтвердить"}</button></div><footer><button onClick={() => onAction("warn")}>Предупредить</button><button className={data.banned ? "primary-button" : "danger-button"} onClick={() => onAction(data.banned ? "unban" : "ban")}>{data.banned ? "Восстановить" : "Заблокировать"}</button></footer></>}</div></Sheet>;
}

function UserAction({ data, busy, onClose, onSubmit }) {
  const [reason, setReason] = useState("");
  useEffect(() => setReason(""), [data]);
  if (!data) return null;
  const needsReason = ["warn", "ban"].includes(data.action);
  const titles = { warn: "Вынести предупреждение", ban: "Ограничить доступ", unban: "Восстановить доступ", verify: "Подтвердить профиль", unverify: "Снять отметку доверия" };
  const descriptions = { warn: "Предупреждение появится в истории пользователя и придёт в Telegram.", ban: "Пользователь потеряет доступ к LT Market до ручной разблокировки.", unban: "Пользователь снова сможет работать с площадкой.", verify: "В карточках и профиле появится отметка подтверждённого пользователя.", unverify: "Отметка будет снята, остальные данные профиля сохранятся." };
  return <Sheet open title={titles[data.action]} onClose={onClose} className="admin-action-sheet"><div className="admin-user-action"><section><Avatar src={data.user.avatar_url} name={data.user.display_name}/><div><b>{data.user.display_name}</b><small>ID {data.user.user_id}</small></div></section><p>{descriptions[data.action]}</p>{needsReason && <label className="field"><span>Причина</span><textarea value={reason} onChange={(event) => setReason(event.target.value)} placeholder="Опишите решение понятным языком"/></label>}<button className={`wide ${data.action === "ban" ? "danger-button" : "primary-button"}`} disabled={busy || (needsReason && reason.trim().length < 5)} onClick={() => onSubmit({ ...data, reason })}>{busy ? "Сохраняем…" : titles[data.action]}</button></div></Sheet>;
}

function QueueAction({ data, busy, onClose, onSubmit }) {
  const [note, setNote] = useState("");
  useEffect(() => setNote(""), [data]);
  if (!data) return null;
  const reply = data.action === "reply";
  return <Sheet open title={reply ? "Ответить пользователю" : "Закрыть обращение"} onClose={onClose}><div className="admin-queue-action"><section><Icon name={reply ? "message" : "check"}/><div><b>{data.item.author_name || "Пользователь LT"}</b><small>Обращение #{data.item.id}</small></div></section><p>{reply ? "Ответ будет доставлен как важное уведомление в Telegram." : "Укажите, что было сделано. Запись останется в журнале."}</p><label className="field"><span>{reply ? "Ответ" : "Комментарий администратора"}</span><textarea autoFocus value={note} onChange={(event) => setNote(event.target.value)} placeholder={reply ? "Напишите понятное решение или уточняющий вопрос" : "Например: нарушение не подтвердилось"}/></label><button className="primary-button wide" disabled={busy || note.trim().length < 3} onClick={() => onSubmit({ ...data, note })}>{busy ? "Отправляем…" : reply ? "Отправить в Telegram" : "Закрыть и сохранить"}</button></div></Sheet>;
}

function DisputeDetail({ data, busy, onClose, onResolve }) {
  const [outcome, setOutcome] = useState("");
  const [note, setNote] = useState("");
  useEffect(() => { setOutcome(""); setNote(""); }, [data]);
  if (!data) return null;
  const item = data.dispute || {};
  return <Sheet open title={`Спор #${item.id}`} onClose={onClose} className="admin-dispute-sheet"><div className="dispute-console"><section className="dispute-summary"><small>СДЕЛКА #{item.deal_id}</small><h3>{item.title}</h3><strong>{money(item.amount)}</strong><p>{item.reason || item.note}</p></section><div className="dispute-parties"><article><Avatar name={item.buyer_name} size="sm"/><span><small>ЗАКАЗЧИК</small><b>{item.buyer_name}</b></span></article><Icon name="arrow"/><article><Avatar name={item.seller_name} size="sm"/><span><small>ИСПОЛНИТЕЛЬ</small><b>{item.seller_name}</b></span></article></div><section className="dispute-evidence"><header><b>Материалы сделки</b><span>{(data.messages?.length || 0) + (data.deliveries?.length || 0)}</span></header>{data.deliveries?.map((delivery) => <article key={`delivery-${delivery.id}`}><i><Icon name="file" size={15}/></i><span><b>Версия {delivery.version}</b><small>{delivery.comment || "Результат работы"} · {dateLabel(delivery.created_at)}</small></span></article>)}{data.messages?.slice(-5).map((message) => <article key={`message-${message.id}`}><i><Icon name="message" size={15}/></i><span><b>{message.sender_name}</b><small>{message.text} · {dateLabel(message.created_at)}</small></span></article>)}</section><section className="dispute-outcome"><small>РЕШЕНИЕ</small>{[["buyer", "В пользу заказчика", "Зафиксировать невыполнение условий"], ["seller", "В пользу исполнителя", "Работа соответствует согласованным условиям"], ["mutual", "Взаимное закрытие", "Закрыть сделку по соглашению сторон"]].map(([id, title, text]) => <button className={outcome === id ? "active" : ""} key={id} onClick={() => setOutcome(id)}><i>{outcome === id && <Icon name="check" size={14}/>}</i><span><b>{title}</b><small>{text}</small></span></button>)}</section><label className="field"><span>Обоснование решения</span><textarea value={note} onChange={(event) => setNote(event.target.value)} placeholder="Какие факты и материалы повлияли на решение?"/><small>{note.trim().length}/10 минимум</small></label><aside className="admin-readonly"><Icon name="alert" size={17}/> Решение меняет статус сделки, но не выполняет банковский перевод и не имитирует эскроу.</aside><button className="primary-button wide" disabled={busy || !outcome || note.trim().length < 10} onClick={() => onResolve({ id: item.id, outcome, note })}>{busy ? "Сохраняем решение…" : "Закрыть спор и уведомить стороны"}</button></div></Sheet>;
}

function RefundSheet({ data, busy, onClose, onSubmit }) {
  if (!data) return null;
  return <Sheet open title="Возврат Telegram Stars" onClose={onClose}><div className="refund-confirm"><i><Icon name="star" size={30}/></i><small>ОПЕРАЦИЯ #{data.id}</small><h3>{data.stars} Stars</h3><p>Вернуть пользователю <b>{data.user_name}</b> оплату продвижения «{data.listing_title}»?</p><aside><Icon name="alert" size={17}/> Возврат необратим и будет записан в журнале.</aside><button className="danger-button wide" disabled={busy} onClick={() => onSubmit(data)}>{busy ? "Выполняем возврат…" : `Вернуть ${data.stars} Stars`}</button></div></Sheet>;
}

function statusLabel(status = "") { return ({ open: "Открыто", answered: "Есть ответ", new: "Новое", moderation: "На проверке", pending: "Ожидает", dispute_open: "Открыт спор", active: "Активно", closed: "Закрыто" })[status] || status || "Новое"; }
function paymentStatus(status) { return ({ paid: "Оплачено", pending: "Ожидает", refunded: "Возвращено", cancelled: "Отменено" })[status] || status; }
function auditIcon(action = "") { return action.includes("ban") ? "lock" : action.includes("warn") || action.includes("reject") ? "alert" : action.includes("approve") || action.includes("verify") ? "check" : action.includes("refund") ? "star" : "shield"; }
function auditLabel(action = "") { const map = { miniapp_warn: "Предупреждение", miniapp_ban: "Блокировка", miniapp_unban: "Разблокировка", miniapp_verify: "Профиль подтверждён", miniapp_unverify: "Отметка снята", refund_stars: "Возврат Stars", ticket_reply: "Ответ поддержки" }; return map[action] || action.replaceAll("_", " "); }
