const API_BASE = import.meta.env.VITE_API_URL || "https://lteam-botminiapp.onrender.com";

export const telegram = () => window.Telegram?.WebApp;

export function haptic(kind = "selection", enabled = true) {
  if (!enabled) return;
  const feedback = telegram()?.HapticFeedback;
  try {
    if (kind === "success" || kind === "error" || kind === "warning") feedback?.notificationOccurred?.(kind);
    else if (kind === "light" || kind === "medium" || kind === "heavy") feedback?.impactOccurred?.(kind);
    else feedback?.selectionChanged?.();
  } catch { /* Telegram haptics are optional. */ }
}

export async function api(path, options = {}) {
  const localPreview = ["localhost", "127.0.0.1"].includes(window.location.hostname) ? new URLSearchParams(window.location.search).get("initData") || "" : "";
  const initData = telegram()?.initData || localPreview;
  const headers = {
    ...(options.body ? { "Content-Type": "application/json" } : {}),
    ...(initData ? { "X-Telegram-Init-Data": initData } : {}),
    ...(options.headers || {}),
  };
  const response = await fetch(`${API_BASE}${path}`, { ...options, headers });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(payload.message || payload.error || "Не удалось выполнить действие");
    error.status = response.status;
    error.payload = payload;
    throw error;
  }
  return payload;
}

export function send(path, method = "POST", payload = {}) {
  return api(path, { method, body: JSON.stringify(payload) });
}

export function money(value, prefix = "") {
  const amount = Number(value || 0).toLocaleString("ru-RU");
  return `${prefix}${amount} ₽`;
}

export function dateLabel(value) {
  if (!value) return "Недавно";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Недавно";
  const diff = Date.now() - date.getTime();
  if (diff < 60_000) return "Только что";
  if (diff < 3_600_000) return `${Math.max(1, Math.round(diff / 60_000))} мин назад`;
  if (diff < 86_400_000) return `${Math.max(1, Math.round(diff / 3_600_000))} ч назад`;
  return date.toLocaleDateString("ru-RU", { day: "numeric", month: "short" });
}

export function initials(name = "LT") {
  const words = String(name).trim().split(/\s+/).filter(Boolean);
  return (words.slice(0, 2).map((word) => word[0]).join("") || "LT").toUpperCase();
}

export async function compressImage(file, maxWidth = 1440, quality = 0.78) {
  if (!file?.type?.startsWith("image/")) throw new Error("Выберите изображение");
  const source = await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
  const image = await new Promise((resolve, reject) => {
    const element = new Image();
    element.onload = () => resolve(element);
    element.onerror = reject;
    element.src = source;
  });
  const ratio = Math.min(1, maxWidth / image.width);
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.round(image.width * ratio));
  canvas.height = Math.max(1, Math.round(image.height * ratio));
  canvas.getContext("2d").drawImage(image, 0, 0, canvas.width, canvas.height);
  return canvas.toDataURL("image/jpeg", quality);
}

export function normalizeArray(value) {
  if (Array.isArray(value)) return value;
  try { return JSON.parse(value || "[]"); } catch { return []; }
}
