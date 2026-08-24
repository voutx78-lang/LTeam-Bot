import { useEffect, useMemo, useState } from "react";
import { Sheet } from "../components";
import Icon from "../icons";
import { telegram } from "../api";

const DEFAULTS = {
  role: "both",
  theme: "system",
  notifications: { messages: true, orders: true, recommendations: true },
  display: { animations: true, haptics: true, compact_cards: false, language: "ru", accent: "violet" },
};

const ROLES = [
  { id: "customer", icon: "search", title: "Заказываю", text: "Каталог и задачи" },
  { id: "executor", icon: "briefcase", title: "Выполняю", text: "Заказы и отклики" },
  { id: "both", icon: "grid", title: "Обе роли", text: "Все возможности" },
];

const THEMES = [
  { id: "system", title: "Telegram", text: "Как в приложении" },
  { id: "light", title: "Светлая", text: "Чистая и лёгкая" },
  { id: "dark", title: "Тёмная", text: "Комфортно вечером" },
];

const ACCENTS = [
  { id: "violet", title: "Фиолетовый" },
  { id: "ocean", title: "Синий" },
  { id: "mint", title: "Мятный" },
  { id: "sunset", title: "Коралловый" },
];

function normalize(value = {}) {
  return {
    ...DEFAULTS,
    ...value,
    notifications: { ...DEFAULTS.notifications, ...(value.notifications || {}) },
    display: { ...DEFAULTS.display, ...(value.display || {}) },
  };
}

function effectiveTheme(selected) {
  if (selected !== "system") return selected;
  return telegram()?.colorScheme || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
}

function applyVisual(value) {
  const root = document.documentElement;
  const theme = effectiveTheme(value.theme || "system");
  root.dataset.theme = theme;
  root.dataset.accent = value.display?.accent || "violet";
  root.dataset.motion = value.display?.animations === false ? "reduced" : "full";
  root.dataset.density = value.display?.compact_cards ? "compact" : "comfortable";
  root.style.colorScheme = theme;
}

function SwitchRow({ icon, title, text, checked, onChange, important = false }) {
  return <button type="button" className="setting-switch-row" onClick={() => onChange(!checked)} role="switch" aria-checked={checked}>
    <i><Icon name={icon}/></i><span><b>{title}</b><small>{text}</small>{important && <em>ВАЖНОЕ</em>}</span><strong className={checked ? "on" : ""}><i/></strong>
  </button>;
}

export default function SettingsSheet({ open, preferences, onClose, onSave, onClearRecent }) {
  const [form, setForm] = useState(() => normalize(preferences));
  useEffect(() => { if (open) setForm(normalize(preferences)); }, [preferences, open]);
  useEffect(() => {
    if (!open) return undefined;
    applyVisual(form);
    return () => applyVisual(normalize(preferences));
  }, [form, open, preferences]);

  const saved = useMemo(() => normalize(preferences), [preferences]);
  const dirty = JSON.stringify(form) !== JSON.stringify(saved);
  const patchDisplay = (patch) => setForm((current) => ({ ...current, display: { ...current.display, ...patch } }));
  const patchNotifications = (patch) => setForm((current) => ({ ...current, notifications: { ...current.notifications, ...patch } }));
  const reset = () => setForm(normalize(DEFAULTS));

  return <Sheet open={open} title="Настройки" onClose={onClose} className="settings-sheet settings-sheet-v2">
    <div className="settings-content-v2">
      <section className="settings-hero-v2">
        <div><small>ВАШ LT MARKET</small><h2>Настройте всё<br/>под себя</h2><p>Изменения сразу видны в интерфейсе.</p><span><i/> Синхронизируется с профилем</span></div>
        <div className="settings-device-preview"><i/><header><b/><span/></header><main><strong/><strong/><strong/></main><footer><em/><em/><em/></footer></div>
      </section>

      <SettingsSection number="01" eyebrow="РЕЖИМ" title="Как вы используете площадку">
        <div className="settings-role-grid">{ROLES.map((role) => <button type="button" className={form.role === role.id ? "active" : ""} key={role.id} onClick={() => setForm({ ...form, role: role.id })}><i><Icon name={role.icon}/></i><b>{role.title}</b><small>{role.text}</small><span><Icon name="check" size={13}/></span></button>)}</div>
      </SettingsSection>

      <SettingsSection number="02" eyebrow="ВНЕШНИЙ ВИД" title="Тема оформления">
        <div className="settings-theme-grid">{THEMES.map((theme) => <button type="button" className={`${theme.id} ${form.theme === theme.id ? "active" : ""}`} key={theme.id} onClick={() => setForm({ ...form, theme: theme.id })}><i className="theme-demo"><span/><b/><em/></i><strong>{theme.title}</strong><small>{theme.text}</small><Icon name="check" size={14}/></button>)}</div>
        <div className="settings-accent"><header><div><b>Цвет акцента</b><small>Кнопки, ссылки и активные элементы</small></div><em>{ACCENTS.find((item) => item.id === form.display.accent)?.title}</em></header><div>{ACCENTS.map((accent) => <button type="button" aria-label={accent.title} title={accent.title} className={`${accent.id} ${form.display.accent === accent.id ? "active" : ""}`} key={accent.id} onClick={() => patchDisplay({ accent: accent.id })}><i/><Icon name="check" size={13}/></button>)}</div></div>
        <div className="settings-density"><header><b>Плотность каталога</b><small>Размер карточек и количество информации</small></header><div><button type="button" className={!form.display.compact_cards ? "active" : ""} onClick={() => patchDisplay({ compact_cards: false })}><Icon name="grid"/> Комфортно</button><button type="button" className={form.display.compact_cards ? "active" : ""} onClick={() => patchDisplay({ compact_cards: true })}><Icon name="list"/> Компактно</button></div></div>
      </SettingsSection>

      <SettingsSection number="03" eyebrow="ПОВЕДЕНИЕ" title="Ощущение от интерфейса">
        <div className="settings-switch-list"><SwitchRow icon="spark" title="Плавные переходы" text="Анимация страниц, окон и кнопок" checked={form.display.animations !== false} onChange={(value) => patchDisplay({ animations: value })}/><SwitchRow icon="refresh" title="Тактильный отклик" text="Лёгкая вибрация при действиях" checked={form.display.haptics !== false} onChange={(value) => patchDisplay({ haptics: value })}/></div>
      </SettingsSection>

      <SettingsSection number="04" eyebrow="УВЕДОМЛЕНИЯ" title="Что не хочется пропустить">
        <div className="settings-switch-list"><SwitchRow icon="message" title="Сообщения и отклики" text="Новые сообщения и предложения" checked={form.notifications.messages !== false} onChange={(value) => patchNotifications({ messages: value })} important/><SwitchRow icon="briefcase" title="Статусы заказов" text="Сроки, результаты и правки" checked={form.notifications.orders !== false} onChange={(value) => patchNotifications({ orders: value })} important/><SwitchRow icon="spark" title="Рекомендации" text="Подходящие услуги и новые задачи" checked={form.notifications.recommendations !== false} onChange={(value) => patchNotifications({ recommendations: value })}/></div>
        <p className="settings-notice"><Icon name="shield" size={17}/> Системные сообщения по активной сделке остаются включёнными для безопасности.</p>
      </SettingsSection>

      <SettingsSection number="05" eyebrow="ДАННЫЕ" title="Приложение и история">
        <div className="settings-tools"><button type="button" onClick={onClearRecent}><i><Icon name="clock"/></i><span><b>Очистить историю просмотров</b><small>Убрать недавно открытые услуги</small></span><Icon name="chevron"/></button><div><i><Icon name="message"/></i><span><b>Язык интерфейса</b><small>Другие языки появятся позже</small></span><em>Русский · RU</em></div></div>
      </SettingsSection>

      <footer className="settings-savebar"><button type="button" className="settings-reset" onClick={reset}>Сбросить</button><div><small>{dirty ? "Есть несохранённые изменения" : "Все изменения сохранены"}</small><button type="button" className="primary-button" disabled={!dirty} onClick={() => onSave(form)}>{dirty ? "Сохранить" : "Готово"}<Icon name="check" size={17}/></button></div></footer>
    </div>
  </Sheet>;
}

function SettingsSection({ number, eyebrow, title, children }) {
  return <section className="settings-section-v2"><header><span>{number}</span><div><small>{eyebrow}</small><h3>{title}</h3></div></header>{children}</section>;
}
