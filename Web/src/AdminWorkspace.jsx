import { useEffect, useState } from "react";

const queues = [
  ["moderation", "Модерация", "Новые услуги и заказы", "П"],
  ["payments", "Оплаты", "Чеки и поступления", "₽"],
  ["payouts", "Выплаты", "Заявки исполнителей", "↑"],
  ["disputes", "Споры", "Проблемные сделки", "!"],
  ["tickets", "Поддержка", "Новые обращения пользователей", "?"],
];

const labels = {
  moderation: ["ОЧЕРЕДЬ МОДЕРАЦИИ", "Публикации на проверке"],
  payments: ["ОПЛАТЫ", "Сделки на проверке"],
  payouts: ["ВЫВОДЫ", "Заявки на выплату"],
  disputes: ["СПОРЫ", "Открытые обращения"],
  tickets: ["ПОДДЕРЖКА", "Обращения пользователей"],
};

function money(value) {
  return Number(value || 0).toLocaleString("ru-RU") + " ₽";
}

export default function AdminWorkspace({ summary = {}, onNavigate, onOpenBot, fetchData, request }) {
  const [queue, setQueue] = useState([]);
  const [mode, setMode] = useState("moderation");
  const [opened, setOpened] = useState(false);
  const [notice, setNotice] = useState("");
  const [liveCounts, setLiveCounts] = useState({});

  useEffect(() => {
    // Parent refreshes the server summary asynchronously; mirror it for
    // optimistic queue counters without changing the first paint.
    setLiveCounts(summary);
  }, [summary]);
  const counts = { moderation: liveCounts.moderation ?? 0, payments: liveCounts.payments ?? 0, payouts: liveCounts.payouts ?? 0, disputes: liveCounts.disputes ?? 0, tickets: liveCounts.tickets ?? 0 };
  const total = Object.values(counts).reduce((sum, value) => sum + Number(value || 0), 0);

  async function openQueue(nextMode) {
    setNotice("");
    try {
      const endpoint = nextMode === "moderation" ? "/api/admin/moderation" : `/api/admin/queues/${nextMode}`;
      setQueue(await fetchData(endpoint));
      setMode(nextMode);
      setOpened(true);
    } catch (error) {
      setNotice(error?.message || "Не удалось загрузить очередь. Повторите попытку.");
    }
  }

  async function decide(item, action, note = "") {
    try {
      await request(`/api/admin/moderation/${item.item_type}/${item.id}`, "POST", { action, note });
      setQueue((current) => current.filter((row) => row.id !== item.id || row.item_type !== item.item_type));
      setLiveCounts((current) => ({ ...current, moderation: Math.max(0, Number(current.moderation || 0) - 1) }));
      setNotice(action === "approve" ? "Публикация одобрена — автор уже получил уведомление." : "Публикация отклонена, комментарий отправлен автору.");
    } catch (error) {
      setNotice(error?.message || "Не удалось обработать публикацию.");
    }
  }

  return <section className="admin-workspace">
    <header className="admin-workspace-head">
      <button className="admin-back" onClick={() => onNavigate("profile")} aria-label="Назад">‹</button>
      <div><p>LTEAM CONTROL</p><h1>Админ-центр</h1></div>
      <span>ADMIN</span>
    </header>

    <section className="admin-brief">
      <div className="admin-brief-number"><b>{total}</b><span>задач требуют внимания</span></div>
      <p>Проверяйте публикации здесь, а финансовые операции и решения по спорам подтверждайте в защищённой панели бота.</p>
    </section>

    <section className="admin-queue">
      {queues.map(([key, title, caption, icon]) => <button key={key} onClick={() => openQueue(key)}>
        <i aria-hidden="true">{icon}</i><span><b>{title}</b><small>{caption}</small></span><strong>{counts[key]}</strong><em>›</em>
      </button>)}
    </section>

    <section className="admin-tools">
      <p>РАСШИРЕННОЕ УПРАВЛЕНИЕ</p>
      <button onClick={() => onOpenBot("admin_panel")}><span>Открыть админ-панель бота</span><b>→</b></button>
      <small>Бот используется для выплат, подтверждения переводов, решения споров и действий с аккаунтами.</small>
    </section>

    {notice && <button className="admin-notice" onClick={() => setNotice("")}><span>✓</span>{notice}<b>×</b></button>}
    {opened && <QueueSheet queue={queue} mode={mode} onClose={() => setOpened(false)} onDecide={decide} onOpenBot={onOpenBot} />}
  </section>;
}

function QueueSheet({ queue, mode, onClose, onDecide, onOpenBot }) {
  const [rejecting, setRejecting] = useState(null);
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState("");
  const [eyebrow, title] = labels[mode] || ["LTEAM", "Очередь"];

  async function submitDecision(item, action) {
    setBusy(true);
    await onDecide(item, action, action === "reject" ? note.trim() : "");
    setBusy(false); setRejecting(null); setNote("");
  }

  return <div className="moderation-backdrop" onMouseDown={onClose}>
    <section className="moderation-sheet" onMouseDown={(event) => event.stopPropagation()}>
      <div className="sheet-handle" />
      <header><div><p>{eyebrow}</p><h2>{title}</h2></div><button onClick={onClose} aria-label="Закрыть">×</button></header>
      {!queue.length && <div className="moderation-empty"><b>В очереди пока пусто</b><span>Новые задачи появятся здесь автоматически.</span></div>}
      <div className="moderation-list">{queue.map((item) => <article key={`${mode}-${item.id}`}>
        <div className="admin-item-top"><span>{mode === "moderation" ? (item.item_type === "listing" ? "УСЛУГА" : "ЗАКАЗ") : `#${item.id}`}</span><small>{item.created_at ? new Date(item.created_at).toLocaleDateString("ru-RU") : "LTeam"}</small></div>
        <h3>{item.title || "Без названия"}</h3>
        <p>{item.description || item.note || "Требуется проверка администратора."}</p>
        <div className="admin-item-meta"><span>{item.category || "LTeam"}</span><b>{money(item.amount)}</b></div>
        <footer>{mode === "moderation" ? <>
          <button className="admin-reject" onClick={() => { setRejecting(item); setNote(""); }}>Отклонить</button>
          <button className="admin-approve" disabled={busy} onClick={() => submitDecision(item, "approve")}>Одобрить</button>
        </> : <button className="admin-bot-action" onClick={() => { setToast("Открываю защищённое действие в боте…"); onOpenBot(mode); }}>Открыть в боте <b>→</b></button>}</footer>
      </article>)}</div>
      {toast && <div className="admin-sheet-toast">{toast}</div>}
      {rejecting && <div className="admin-reject-backdrop"><section className="admin-reject-dialog"><p>КОММЕНТАРИЙ АВТОРУ</p><h3>Почему публикация не подходит?</h3><textarea autoFocus value={note} maxLength={500} placeholder="Например: уточните сроки и добавьте описание результата" onChange={(event) => setNote(event.target.value)} /><small>Комментарий будет отправлен пользователю в Telegram.</small><footer><button onClick={() => setRejecting(null)}>Отмена</button><button disabled={busy} onClick={() => submitDecision(rejecting, "reject")}>Отклонить</button></footer></section></div>}
    </section>
  </div>;
}
