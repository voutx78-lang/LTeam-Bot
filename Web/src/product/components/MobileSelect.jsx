import { useEffect, useId, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import Icon from "../icons";
import { haptic } from "../api";

function normalizeOption(option) {
  return typeof option === "object" ? option : { value: option, label: String(option) };
}

export default function MobileSelect({ value, onChange, options, title = "Выберите вариант", eyebrow = "ВЫБОР", variant = "form", className = "", searchable = false, disabled = false }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const titleId = useId();
  const triggerRef = useRef(null);
  const sheetRef = useRef(null);
  const normalized = useMemo(() => options.map(normalizeOption), [options]);
  const selected = normalized.find((option) => String(option.value) === String(value)) || normalized[0];
  const visible = useMemo(() => normalized.filter((option) => `${option.label} ${option.description || ""}`.toLowerCase().includes(query.trim().toLowerCase())), [normalized, query]);

  useEffect(() => {
    if (!open) return undefined;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const closeOnEscape = (event) => { if (event.key === "Escape") setOpen(false); };
    window.addEventListener("keydown", closeOnEscape);
    const focusFrame = window.requestAnimationFrame(() => sheetRef.current?.querySelector('[role="radio"][aria-checked="true"]')?.focus());
    return () => { document.body.style.overflow = previous; window.removeEventListener("keydown", closeOnEscape); window.cancelAnimationFrame(focusFrame); };
  }, [open]);

  const hide = () => {
    setOpen(false);
    window.setTimeout(() => triggerRef.current?.focus({ preventScroll: true }), 0);
  };

  const show = () => {
    if (disabled) return;
    setQuery("");
    setOpen(true);
    haptic("selection");
  };
  const choose = (option) => {
    onChange(option.value);
    haptic("selection");
    hide();
  };

  return <>
    <button ref={triggerRef} type="button" className={`mobile-select-trigger ${variant} ${className}`} onClick={show} disabled={disabled} aria-haspopup="dialog" aria-expanded={open}>
      {selected?.icon ? <i><Icon name={selected.icon} size={variant === "toolbar" ? 15 : 19}/></i> : null}
      <span>{selected?.label || "Выбрать"}</span><Icon name="chevron" size={16}/>
    </button>
    {open ? createPortal(<div className="mobile-select-backdrop" onMouseDown={hide}>
      <section ref={sheetRef} className="mobile-select-sheet" role="dialog" aria-modal="true" aria-labelledby={titleId} onMouseDown={(event) => event.stopPropagation()}>
        <div className="mobile-select-handle"/>
        <header><div><small>{eyebrow}</small><h2 id={titleId}>{title}</h2><p>{normalized.length} {normalized.length === 1 ? "вариант" : "вариантов"}</p></div><button type="button" className="icon-button" onClick={hide} aria-label="Закрыть"><Icon name="close"/></button></header>
        {searchable && normalized.length > 7 ? <label className="mobile-select-search"><Icon name="search" size={18}/><input autoFocus value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Найти вариант"/></label> : null}
        <div className="mobile-select-options" role="radiogroup">
          {visible.map((option, index) => {
            const active = String(option.value) === String(value);
            return <button type="button" role="radio" aria-checked={active} className={active ? "active" : ""} key={`${option.value}-${index}`} onClick={() => choose(option)}>
              <i><Icon name={option.icon || "spark"} size={19}/></i><span><b>{option.label}</b>{option.description ? <small>{option.description}</small> : null}</span><em>{active ? <Icon name="check" size={16} strokeWidth={2.6}/> : null}</em>
            </button>;
          })}
        </div>
        <footer><Icon name="shield" size={15}/> Выбор сохранится сразу после нажатия</footer>
      </section>
    </div>, document.body) : null}
  </>;
}
