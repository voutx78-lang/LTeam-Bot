const tg = window.Telegram.WebApp;

tg.expand();

function sendAction(action) {
  tg.sendData(JSON.stringify({
    action: action
  }));

  tg.close();
}