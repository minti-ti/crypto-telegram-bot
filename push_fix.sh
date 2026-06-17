#!/bin/bash
cd /c/Users/Ян/Downloads/crypto_bot || exit 1

echo "=== проверка bot.py ==="
if grep -q "kb_alerts()" bot.py; then
  echo "ОШИБКА: bot.py всё ещё старый! Замени файл на bot_fixed.py"
  echo "Найдено kb_alerts() вызовов: $(grep -c "kb_alerts()" bot.py)"
  exit 1
fi
echo "bot.py OK, kb_alerts() не найдено"

git add -A
git -c user.name="minti-ti" -c user.email="minti-ti@users.noreply.github.com" commit -m "fix: replace kb_alerts/kb_news with kb_alerts_list/kb_subs_current, remove duplicate handlers"
git push origin main
