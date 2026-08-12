import { useEffect, useState } from "react";

const themes = [["light", "Светлая", "Мягкий светлый интерфейс"], ["dark", "Тёмная", "Комфортная в любое время"], ["midnight", "Неон", "Глубокий фон и яркие акценты"]];

export default function SettingsWorkspace({ theme, setTheme, onClose }) {
  const [compact, setCompact] = useState(() => localStorage.getItem("lteam-compact") === "true");
  const [haptics, setHaptics] = useState(() => localStorage.getItem("lteam-haptics") !== "false");
  useEffect(() => { localStorage.setItem("lteam-compact", String(compact)); document.documentElement.dataset.compact = compact ? "true" : "false"; }, [compact]);
  useEffect(() => { localStorage.setItem("lteam-haptics", String(haptics)); }, [haptics]);
  return <section className="settings-workspace"><header className="settings-workspace-head"><button onClick={onClose}>←</button><div><p>ПЕРСОНАЛИЗАЦИЯ</p><h1>Настройки</h1></div></header><section className="settings-group"><p>ОФОРМЛЕНИЕ</p>{themes.map(([id, title, caption]) => <button className={`theme-choice ${theme === id ? "active" : ""}`} key={id} onClick={() => setTheme(id)}><i className={`theme-preview ${id}`}><span /><span /><span /></i><span><b>{title}</b><small>{caption}</small></span><em>{theme === id ? "✓" : ""}</em></button>)}</section><section className="settings-group"><p>ИНТЕРФЕЙС</p><Toggle title="Компактный режим" caption="Меньше отступов в списках и карточках" value={compact} onChange={setCompact} /><Toggle title="Тактильный отклик" caption="Лёгкая вибрация при действиях в MiniApp" value={haptics} onChange={setHaptics} /></section><section className="settings-note"><i>i</i><div><b>Уведомления сделок</b><span>Новые сообщения и статусы приходят через Telegram. Управлять их звуком и показом можно в настройках чата Telegram.</span></div></section><button className="settings-done" onClick={onClose}>Готово</button></section>;
}

function Toggle({ title, caption, value, onChange }) { return <button className="settings-toggle" onClick={() => onChange(!value)}><span><b>{title}</b><small>{caption}</small></span><i className={value ? "on" : ""}><em /></i></button>; }
