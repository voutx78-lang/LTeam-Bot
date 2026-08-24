import Icon from "../icons";

export default function CreateLaunch({ launch }) {
  if (!launch) return null;
  const kind = launch.type === "order" ? "order" : launch.type === "listing" ? "listing" : "choice";
  const copy = kind === "order"
    ? { eyebrow: "НОВАЯ ЗАДАЧА", title: "Собираем ваш запрос", text: "Три коротких шага — и исполнители увидят задачу." }
    : kind === "listing"
      ? { eyebrow: "НОВАЯ УСЛУГА", title: "Открываем витрину", text: "Покажите результат, тарифы и примеры работ." }
      : { eyebrow: "НОВАЯ ПУБЛИКАЦИЯ", title: "Что создадим?", text: "Задачу для исполнителей или услугу для заказчиков." };
  const style = { "--create-x": `${launch.point.x}px`, "--create-y": `${launch.point.y}px` };
  return <div className={`create-launch phase-${launch.phase} kind-${kind}`} style={style} aria-live="polite" aria-label="Открываем создание публикации">
    <div className="create-launch-aurora" aria-hidden="true"><i/><i/><i/></div>
    <div className="create-launch-core">
      <span><Icon name={launch.phase === "ready" ? "check" : "plus"} size={30}/></span>
      <small>{copy.eyebrow}</small>
      <h2>{launch.phase === "ready" ? "Всё готово" : copy.title}</h2>
      <p>{launch.phase === "ready" ? "Открываем удобный редактор публикации" : copy.text}</p>
    </div>
    <div className="create-launch-options" aria-hidden="true">
      <article className={kind === "order" ? "active" : ""}><i><Icon name="briefcase" size={19}/></i><span><b>Задача</b><small>Получить отклики</small></span><em/></article>
      <article className={kind === "listing" ? "active" : ""}><i><Icon name="grid" size={19}/></i><span><b>Услуга</b><small>Найти заказчиков</small></span><em/></article>
    </div>
    <footer><i/><i className="active"/><i/></footer>
  </div>;
}
