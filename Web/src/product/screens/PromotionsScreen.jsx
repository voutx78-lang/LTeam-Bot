import { useEffect, useMemo, useState } from "react";
import { EmptyState, PageHeader } from "../components";
import Icon from "../icons";
import { api, dateLabel, send, telegram } from "../api";

const PRODUCT_ICONS = { promo_bump: "arrow", promo_highlight: "spark", promo_top: "star" };
const PRODUCT_LABELS = { promo_bump: "Быстрый старт", promo_highlight: "Больше внимания", promo_top: "Максимум показов" };
const STATUS_LABELS = {
  pending: "Ожидает оплаты",
  paid: "Оплачено",
  refunded: "Возвращено",
  invoice_failed: "Счёт не создан",
};

export default function PromotionsScreen({ me, listings, products = [], initialListingId, onBack, notify, onUpdated }) {
  const ownListings = useMemo(
    () => listings.filter((item) => Number(item.seller_id) === Number(me.id) && (!item.status || item.status === "active")),
    [listings, me.id],
  );
  const [listingId, setListingId] = useState(() => Number(initialListingId || ownListings[0]?.id || 0));
  const [payments, setPayments] = useState([]);
  const [buying, setBuying] = useState("");

  const loadPayments = async () => {
    try { setPayments(await api("/api/payments/stars")); }
    catch { /* History is supplementary; invoice errors are shown separately. */ }
  };

  useEffect(() => { loadPayments(); }, []);
  useEffect(() => {
    if (!ownListings.some((item) => Number(item.id) === Number(listingId))) setListingId(Number(ownListings[0]?.id || 0));
  }, [ownListings, listingId]);

  const waitForPayment = async (paymentId) => {
    for (let attempt = 0; attempt < 6; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 900 + attempt * 250));
      const payment = await api(`/api/payments/stars/${paymentId}`);
      if (payment.status !== "pending") return payment;
    }
    return null;
  };

  const buy = async (product) => {
    if (!listingId || buying) return;
    const webApp = telegram();
    if (!webApp?.openInvoice) {
      notify("Оплата Stars открывается только внутри Telegram", "error");
      return;
    }
    setBuying(product.code);
    try {
      const invoice = await send("/api/payments/stars/invoice", "POST", { product_code: product.code, listing_id: Number(listingId) });
      webApp.openInvoice(invoice.invoice_link, async (status) => {
        try {
          if (status === "paid" || status === "pending") {
            const payment = await waitForPayment(invoice.payment_id);
            if (payment?.status === "paid") {
              notify("Продвижение оплачено и уже включено");
              await loadPayments();
              await onUpdated?.();
            } else {
              notify("Telegram обрабатывает платёж. Статус обновится автоматически");
              await loadPayments();
            }
          } else if (status === "failed") notify("Telegram не провёл платёж", "error");
        } catch (error) {
          notify(error.message || "Проверяем платёж через Telegram", "error");
        } finally {
          setBuying("");
        }
      });
    } catch (error) {
      setBuying("");
      notify(error.message, "error");
    }
  };

  return <section className="screen promotions-screen">
    <PageHeader eyebrow="TELEGRAM STARS" title="Продвижение" onBack={onBack}/>
    <section className="stars-hero">
      <div className="stars-orbit"><Icon name="star" size={32}/><i/><i/><i/></div>
      <div><small>ОФИЦИАЛЬНАЯ ОПЛАТА TELEGRAM</small><h2>Больше внимания<br/>к вашей работе</h2><p>Поднимайте свои услуги в каталоге. Активация происходит только после подтверждения платежа Telegram.</p></div>
    </section>

    {ownListings.length ? <>
      <label className="promotion-listing-picker">
        <span>КАКУЮ УСЛУГУ ПРОДВИГАТЬ</span>
        <div><Icon name="grid"/><select value={listingId} onChange={(event) => setListingId(Number(event.target.value))}>{ownListings.map((item) => <option value={item.id} key={item.id}>#{item.id} · {item.title}</option>)}</select><Icon name="chevron" size={18}/></div>
      </label>
      <div className="star-products">
        {products.map((product, index) => <article className={`star-product ${product.promo_type || ""}`} key={product.code}>
          <header><i><Icon name={PRODUCT_ICONS[product.code] || "spark"}/></i><span>{PRODUCT_LABELS[product.code] || `Вариант ${index + 1}`}</span>{product.promo_type === "top" && <em>ПОПУЛЯРНО</em>}</header>
          <h3>{product.title}</h3><p>{product.description}</p>
          <footer><strong><Icon name="star" size={17}/>{product.stars}</strong><button disabled={Boolean(buying)} onClick={() => buy(product)}>{buying === product.code ? "Открываем…" : "Выбрать"}<Icon name="arrow" size={17}/></button></footer>
        </article>)}
      </div>
    </> : <EmptyState icon="grid" title="Сначала опубликуйте услугу" text="Продвижение доступно для ваших активных объявлений после модерации."/>}

    <section className="stars-safety">
      <Icon name="shield"/><div><b>Платёж защищён Telegram</b><p>LT Market не видит данные карты или кошелька. При проблеме напишите боту команду /paysupport.</p></div>
    </section>

    {payments.length > 0 && <section className="star-history"><header><div><small>ВАШИ ОПЕРАЦИИ</small><h2>История Stars</h2></div><button onClick={loadPayments}><Icon name="refresh" size={17}/> Обновить</button></header><div>{payments.slice(0, 12).map((payment) => <article key={payment.id}><i className={payment.status}><Icon name={payment.status === "paid" ? "check" : payment.status === "refunded" ? "back" : "clock"}/></i><span><b>Операция #{payment.id}</b><small>Объявление #{payment.listing_id} · {dateLabel(payment.paid_at || payment.created_at)}</small></span><em className={payment.status}>{STATUS_LABELS[payment.status] || payment.status}</em><strong>{payment.stars} <Icon name="star" size={13}/></strong></article>)}</div></section>}

    <p className="stars-footnote">Stars используются только для функций самой площадки. Оплата работы исполнителя через LT Market пока не принимается.</p>
  </section>;
}
