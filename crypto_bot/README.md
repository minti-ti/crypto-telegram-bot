# 🤖 Crypto Telegram Bot

Телеграм-бот для ежедневной крипто-сводки, алертов на цены и фильтрованных новостей.

## ✨ Что умеет

### Утренняя сводка (по расписанию, по умолчанию 09:00 Europe/Moscow)
- 😱 **Fear & Greed Index** (Alternative.me)
- 💰 Цены топ-монет с изменением за 24ч (BTC, ETH, BNB, SOL, TON, XRP и др.)
- 📊 **Доминация BTC** и общая капитализация (CoinGecko)
- 💸 **Funding rate** по фьючерсам BTC и ETH (Binance)
- 🐋 **Whale-alert** — крупные транзакции > $500k (Whale Alert API)
- 📈 **Объёмы торгов** по топ-биржам (Binance)

### По запросу
- `/price BTC` — текущая цена любой монеты (Binance)
- `/fg` — индекс страха и жадности
- `/funding` — funding rate BTC/ETH
- `/whales` — последние крупные транзакции
- `/news BTC,ETH` — свежие новости по монетам (фильтр + перевод на русский)
- `/top` — топ-рост/падение за 24ч
- `/convert 1 BTC USD` — конвертер
- `/settime HH:MM` — изменить время утренней сводки (без рестарта бота)

> К каждому ответу бот крепит **inline-панель быстрых действий** — все основные команды доступны в один тап, без набора текста.

### Алерты
- `/alert BTCUSDT above 70000` — сработает, когда BTC/USDT поднимется выше 70000
- `/alert ETHUSDT below 3000` — сработает, когда ETH/USDT упадёт ниже 3000
- `/alerts` — список твоих активных алертов
- `/delalert <id>` — удалить алерт

### Подписка на новости
- `/subscribe BTC,ETH,SOL` — бот будет присылать значимые новости по этим монетам
- `/unsubscribe` — отписаться
- `/mysubs` — мои подписки

## 🚀 Установка

### 1. Получи API-ключи
| Сервис | Зачем | Где взять |
|---|---|---|
| Telegram Bot Token | Бот | [@BotFather](https://t.me/BotFather) |
| Whale Alert API Key | Крупные транзакции | [whale-alert.io](https://whale-alert.io/) (бесплатно, 60 req/день) |
| CoinGecko API Key *(опц.)* | Расширенные лимиты | [coingecko.com/api/pricing](https://www.coingecko.com/en/api/pricing) |
| OpenRouter API Key *(опц.)* | LLM-фильтрация новостей | [openrouter.ai/keys](https://openrouter.ai/keys) (бесплатные модели есть) |

> API-ключи Binance и Alternative.me **не нужны** (публичные endpoints).
> CryptoPanic в июне 2026 закрыл бесплатный план — поэтому используем RSS + опционально OpenRouter для умной фильтрации. Заголовки автоматически переводятся на русский (через LLM-промпт или Google Translate fallback).

### Источники новостей
- **RSS**: CoinDesk, Cointelegraph, The Block, Decrypt (списком в `NEWS_RSS_FEEDS`, без ключей).
- **LLM-фильтр (опционально)**: если задан `OPENROUTER_API_KEY`, бот пропускает все новости через бесплатную LLM, которая оставляет только значимые события (SEC/ETF/взломы/листинги/макро) и привязывает к тикерам. Без ключа — простая эвристика по ключевым словам.
| Whale Alert API Key | Крупные транзакции | [whale-alert.io](https://whale-alert.io/) (бесплатно, 60 req/день) |
| CoinGecko API Key *(опц.)* | Расширенные лимиты | [coingecko.com/api/pricing](https://www.coingecko.com/en/api/pricing) |

> API-ключи Binance, Alternative.me, Etherscan — **не нужны** (публичные endpoints).

### 2. Локальный запуск
```bash
cd crypto_bot
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# заполни .env своими ключами

python bot.py
```

### 3. Деплой на Render (бесплатно, **Background Worker**)
1. Залей репозиторий на GitHub.
2. На [render.com](https://render.com) → **New** → **Background Worker**.
3. Подключи репозиторий.
4. Render автоматически найдёт `render.yaml` либо задай:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python bot.py`
5. В разделе **Environment Variables** добавь ключи из `.env`.
6. ⚠️ **Не выбирай Web Service** — он засыпает. Нужен именно **Background Worker**, он работает 24/7.

> Если хочешь Web Service (например, для webhook), добавь пинг `/healthz` через cron-job.org раз в 5 минут.

### 4. Деплой на Railway
1. Залей на GitHub.
2. На [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub**.
3. Задай **Start Command**: `python bot.py`.
4. Добавь переменные окружения из `.env`.
5. Railway даёт $5 бесплатных кредитов в месяц — обычно хватает.

## 🗂 Структура
```
crypto_bot/
├── bot.py              # точка входа, хендлеры, планировщик
├── config.py           # настройки из env
├── db.py               # SQLite: пользователи, алерты, подписки
├── services.py         # все API-клиенты
├── keyboards.py        # inline-кнопки
├── requirements.txt
├── Dockerfile
├── render.yaml
└── .env.example
```

## ⚙️ Переменные окружения (.env)
См. `.env.example`. Обязателен только `BOT_TOKEN`. Остальные опциональны (бот работает без них, но соответствующая фича будет ограничена).

## 📝 Лицензия
MIT
