"""Конфигурация бота. Все секреты — только из env."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def _get(key: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and not val:
        raise RuntimeError(
            f"Не задана обязательная переменная окружения {key}. "
            f"См. .env.example"
        )
    return val or ""


# ---- Telegram ----
BOT_TOKEN: str = _get("BOT_TOKEN", required=True)

# ---- Расписание ----
MORNING_TIME: str = _get("MORNING_TIME", "09:00")  # HH:MM по Europe/Moscow
TZ = "Europe/Moscow"

# ---- Внешние API ----
BLOCKCHAIR_API_KEY: str = _get("BLOCKCHAIR_API_KEY")
COINGECKO_API_KEY: str = _get("COINGECKO_API_KEY")

# ---- Новости: RSS + OpenRouter (опциональная LLM-фильтрация) ----
# Если OPENROUTER_API_KEY не задан, бот рассылает все новости из RSS as-is.
OPENROUTER_API_KEY: str = _get("OPENROUTER_API_KEY")
OPENROUTER_MODEL: str = _get(
    "OPENROUTER_MODEL", "openai/gpt-oss-120b:free"
)
NEWS_RSS_FEEDS: list[str] = [
    u.strip() for u in _get(
        "NEWS_RSS_FEEDS",
        "https://www.coindesk.com/arc/outboundfeeds/rss/,"
        "https://cointelegraph.com/rss,"
        "https://www.theblock.co/rss.xml,"
        "https://decrypt.co/feed",
    ).split(",") if u.strip()
]
NEWS_LOOKBACK_HOURS: int = int(_get("NEWS_LOOKBACK_HOURS", "24"))
NEWS_MAX_PER_LLM: int = int(_get("NEWS_MAX_PER_LLM", "30"))

# ---- Параметры ----
TOP_SYMBOLS: list[str] = [
    s.strip().upper()
    for s in _get(
        "TOP_SYMBOLS",
        "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,TONUSDT,ADAUSDT,DOGEUSDT",
    ).split(",")
    if s.strip()
]

FUNDING_SYMBOLS: list[str] = [
    s.strip().upper()
    for s in _get("FUNDING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
    if s.strip()
]

# ---- БД (PostgreSQL через Neon.tech, бесплатно) ----
DATABASE_URL: str = _get("DATABASE_URL")
# Если не задана — бот упадёт при init_db() с понятной ошибкой.
