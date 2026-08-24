import Icon from "../icons";

export default function CatalogLaunch({ launch }) {
  if (!launch) return null;
  const style = {
    "--launch-top": `${launch.rect.top}px`,
    "--launch-left": `${launch.rect.left}px`,
    "--launch-width": `${launch.rect.width}px`,
    "--launch-height": `${launch.rect.height}px`,
  };
  return <div className={`catalog-launch phase-${launch.phase}`} style={style} aria-live="polite" aria-label="Открываем каталог">
    <div className="catalog-launch-glow"><i/><i/><i/></div>
    <div className="catalog-launch-pill">
      <span><Icon name="search" size={25}/></span>
      <div><small>LT MARKET</small><b>{launch.phase === "ready" ? "Каталог готов" : "Подбираем лучшее"}</b></div>
      <em><i/><i/><i/></em>
    </div>
    <div className="catalog-launch-preview" aria-hidden="true">
      <i><span/><b/></i><i><span/><b/></i><i><span/><b/></i>
    </div>
    <p>{launch.phase === "ready" ? "Услуги и исполнители уже здесь" : "Собираем предложения под ваш запрос"}</p>
  </div>;
}
