import { useEffect, useState } from "react";

const money = (value) => `${Number(value || 0).toLocaleString("ru-RU")} ₽`;
const transactionTitle = (type) => ({ deal_income: "Оплата за сделку", withdrawal: "Вывод средств", withdrawal_rejected: "Возврат по заявке", adjustment: "Корректировка баланса" }[type] || "Операция LTeam");

export default function WalletWorkspace({ balance = {}, fetchData, onNavigate, onWithdraw }) {
  const [history, setHistory] = useState([]);
  useEffect(() => { fetchData?.("/api/balance/history").then(setHistory).catch(() => setHistory([])); }, [fetchData]);
  return <section className="wallet-workspace">
    <header className="wallet-head"><button onClick={() => onNavigate("profile")}>←</button><b>Кошелёк</b><span>● защищён</span></header>
    <section className="wallet-hero"><p>ДОСТУПНО К ВЫВОДУ</p><h1>{money(balance.available)}</h1><span>В сделках у гаранта: {money(balance.frozen)}</span><button onClick={onWithdraw}>Вывести средства <i>→</i></button></section>
    <section className="wallet-stat-grid"><div><span>Заработано</span><b>{money(balance.total_earned)}</b></div><div><span>Выведено</span><b>{money(balance.total_withdrawn)}</b></div></section>
    <section className="wallet-info"><i>✓</i><div><b>Как работают выплаты</b><span>После успешной сделки гарант подтверждает оплату и средства становятся доступны к выводу.</span></div></section>
    <section className="wallet-history"><header><div><p>ИСТОРИЯ</p><h2>Операции</h2></div><span>{history.length}</span></header>{history.length ? <div>{history.map((item) => <article key={item.id}><i className={Number(item.amount) < 0 ? "out" : "in"}>{Number(item.amount) < 0 ? "↗" : "↓"}</i><div><b>{transactionTitle(item.tx_type)}</b><small>{item.comment || "Операция в LTeam Market"}</small></div><strong className={Number(item.amount) < 0 ? "out" : "in"}>{Number(item.amount) > 0 ? "+" : ""}{money(item.amount)}</strong></article>)}</div> : <div className="wallet-empty"><b>Операций пока нет</b><span>Здесь появится история сделок, начислений и выводов.</span></div>}</section>
  </section>;
}
