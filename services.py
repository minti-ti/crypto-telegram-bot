"""Все клиенты к внешним API. Асинхронные, с таймаутом и обработкой ошибок."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp
import feedparser  # type: ignore

try:
    from deep_translator import GoogleTranslator
    _HAS_TRANSLATOR = True
except ImportError:
    _HAS_TRANSLATOR = False

import config

log = logging.getLogger(__name__)

TIMEOUT = aiohttp.ClientTimeout(total=20)
HEADERS = {"User-Agent": "crypto-tg-bot/1.0"}


# ─────────────────────────── утилиты ───────────────────────────
async def _get_json(
    session: aiohttp.ClientSession,
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
) -> Any:
    h = {**HEADERS, **(headers or {})}
    try:
        async with session.get(url, params=params, headers=h, timeout=TIMEOUT) as r:
            if r.status == 429:
                log.warning("Rate limit on %s", url)
                return None
            if r.status >= 400:
                log.warning("HTTP %s on %s", r.status, url)
                return None
            return await r.json()
    except asyncio.TimeoutError:
        log.warning("Timeout on %s", url)
        return None
    except Exception as e:
        log.warning("Error on %s: %s", url, e)
        return None


def _shared_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(timeout=TIMEOUT)


# ─────────────────────────── Fear & Greed ───────────────────────
async def get_fear_greed(session: aiohttp.ClientSession) -> dict | None:
    """
    Возвращает:
      { value: int, classification: str, timestamp: int }
    """
    data = await _get_json(
        session, "https://api.alternative.me/fng/", {"limit": 1}
    )
    if not data or "data" not in data or not data["data"]:
        return None
    d = data["data"][0]
    return {
        "value": int(d["value"]),
        "classification": d["value_classification"],
        "timestamp": int(d["timestamp"]),
    }


def fear_greed_emoji(value: int) -> str:
    if value < 25:
        return "😱 Extreme Fear"
    if value < 45:
        return "😨 Fear"
    if value < 55:
        return "😐 Neutral"
    if value < 75:
        return "😏 Greed"
    return "🤑 Extreme Greed"


# ─────────────────────────── Binance: цены ──────────────────────
async def get_24h_tickers(
    session: aiohttp.ClientSession, symbols: list[str]
) -> dict[str, dict]:
    """
    Возвращает {symbol: {...}} с lastPrice, priceChangePercent, volume, quoteVolume, ...
    Делает один запрос ко всем тикерам и фильтрует локально.
    """
    data = await _get_json(
        session, "https://api.binance.com/api/v3/ticker/24hr"
    )
    if not data:
        return {}
    wanted = {s.upper() for s in symbols}
    return {
        t["symbol"]: t
        for t in data
        if t["symbol"] in wanted
    }


async def get_price(
    session: aiohttp.ClientSession, symbol: str
) -> float | None:
    """Текущая цена одной монеты (USDT-пара)."""
    data = await _get_json(
        session,
        "https://api.binance.com/api/v3/ticker/price",
        {"symbol": symbol.upper()},
    )
    if data and "price" in data:
        try:
            return float(data["price"])
        except (TypeError, ValueError):
            return None
    return None


# ─────────────────────────── Binance: funding ───────────────────
async def get_funding_rate(
    session: aiohttp.ClientSession, symbol: str
) -> dict | None:
    """
    Текущий funding rate по фьючерсу.
    Возвращает {symbol, markPrice, indexPrice, lastFundingRate, nextFundingTime}.
    """
    data = await _get_json(
        session,
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        {"symbol": symbol.upper()},
    )
    if not data:
        return None
    try:
        return {
            "symbol": data["symbol"],
            "markPrice": float(data["markPrice"]),
            "indexPrice": float(data["indexPrice"]),
            "lastFundingRate": float(data["lastFundingRate"]),
            "nextFundingTime": int(data["nextFundingTime"]),
        }
    except (KeyError, TypeError, ValueError):
        return None


# ─────────────────────────── CoinGecko global ───────────────────
async def get_global_market(session: aiohttp.ClientSession) -> dict | None:
    """
    Возвращает {'total_market_cap_usd', 'btc_dominance', 'eth_dominance', ...}
    """
    params: dict[str, Any] = {}
    headers: dict[str, Any] = {}
    if config.COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = config.COINGECKO_API_KEY
    data = await _get_json(
        session, "https://api.coingecko.com/api/v3/global", params, headers
    )
    if not data or "data" not in data:
        return None
    d = data["data"]
    return {
        "total_market_cap_usd": d.get("total_market_cap", {}).get("usd"),
        "btc_dominance": d.get("market_cap_percentage", {}).get("btc"),
        "eth_dominance": d.get("market_cap_percentage", {}).get("eth"),
        "active_cryptocurrencies": d.get("active_cryptocurrencies"),
        "market_cap_change_24h": d.get(
            "market_cap_change_percentage_24h_usd"
        ),
    }


# ─────────────────────────── форматирование ─────────────────────
def fmt_usd(v: float | None, digits: int = 2) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1:
        return f"${v:,.{digits}f}"
    return f"${v:.4f}"


def fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    arrow = "🟢" if v >= 0 else "🔴"
    return f"{arrow} {v:+.2f}%"


def fmt_volume(v: float | None) -> str:
    if v is None:
        return "—"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.2f}M"
    if v >= 1e3:
        return f"${v / 1e3:.2f}K"
    return f"${v:.0f}"






def symbol_to_coin(symbol: str) -> str:
    """BTCUSDT -> BTC"""
    s = symbol.upper()
    for suf in ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH"):
        if s.endswith(suf) and s != suf:
            return s[: -len(suf)]
    return s


# ─────────────────────────── утренняя сводка ────────────────────
async def build_morning_briefing() -> str:
    """Собирает утреннюю сводку в одно сообщение."""
    lines: list[str] = []
    today = datetime.now(timezone.utc).strftime("%d.%m.%Y")
    lines.append(f"☀️ *Утренняя сводка — {today}*")
    lines.append("")

    async with _shared_session() as s:
        fg, tickers, global_m, funding = await asyncio.gather(
            get_fear_greed(s),
            get_24h_tickers(s, config.TOP_SYMBOLS),
            get_global_market(s),
            asyncio.gather(*[get_funding_rate(s, x) for x in config.FUNDING_SYMBOLS]),
        )

    # ─ Fear & Greed
    if fg:
        lines.append(
            f"😱 *Fear & Greed Index:* `{fg['value']}` — "
            f"{fear_greed_emoji(fg['value'])}"
        )
        lines.append("")

    # ─ Глобальный рынок
    if global_m:
        if global_m.get("btc_dominance"):
            lines.append(f"📊 *Доминация BTC:* {global_m['btc_dominance']:.1f}%")
        if global_m.get("total_market_cap_usd"):
            lines.append(
                f"🌐 *Капитализация:* {fmt_volume(global_m['total_market_cap_usd'])}"
            )
        if global_m.get("market_cap_change_24h") is not None:
            lines.append(
                f"📈 *Изм. кап. 24ч:* {fmt_pct(global_m['market_cap_change_24h'])}"
            )
        lines.append("")

    # ─ Топ-цены
    if tickers:
        lines.append("💰 *Топ-монеты (24ч):*")
        for sym in config.TOP_SYMBOLS:
            t = tickers.get(sym)
            if not t:
                continue
            coin = symbol_to_coin(sym)
            try:
                price = float(t["lastPrice"])
                change = float(t["priceChangePercent"])
                vol = float(t.get("quoteVolume", 0))
            except (KeyError, TypeError, ValueError):
                continue
            lines.append(
                f"  • *{coin}*: {fmt_usd(price, 2)}  "
                f"{fmt_pct(change)}  _vol {fmt_volume(vol)}_"
            )
        lines.append("")

    # ─ Funding
    if isinstance(funding, list) and funding:
        valid = [f for f in funding if f]
        if valid:
            lines.append("💸 *Funding rate (фьючерсы):*")
            for f in valid:
                coin = symbol_to_coin(f["symbol"])
                rate_pct = f["lastFundingRate"] * 100
                arrow = "🟢" if rate_pct >= 0 else "🔴"
                lines.append(
                    f"  • *{coin}*: {arrow} {rate_pct:+.4f}%  "
                    f"_mark {fmt_usd(f['markPrice'])}_"
                )
            lines.append("")


    if len(lines) == 2:
        lines.append("⚠️ Не удалось получить данные. Проверь API-ключи.")

    return "\n".join(lines)
