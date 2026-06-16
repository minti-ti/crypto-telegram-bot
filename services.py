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


# ─────────────────────────── Binance: Open Interest ───────────
async def get_open_interest(
    session: aiohttp.ClientSession, symbol: str
) -> dict | None:
    """
    Текущий Open Interest по фьючерсу.
    Возвращает {symbol, open_interest (в монетах), time}.
    """
    data = await _get_json(
        session,
        "https://fapi.binance.com/fapi/v1/openInterest",
        {"symbol": symbol.upper()},
    )
    if not data:
        return None
    try:
        return {
            "symbol": data["symbol"],
            "open_interest": float(data["openInterest"]),
            "time": int(data["time"]),
        }
    except (KeyError, TypeError, ValueError):
        return None


async def get_oi_change_24h(
    session: aiohttp.ClientSession, symbol: str
) -> dict | None:
    """
    Изменение Open Interest за 24 часа.
    Возвращает {current_oi, oi_24h_ago, change_pct, change_usd}.
    """
    current = await get_open_interest(session, symbol)
    if not current:
        return None
    end_ms = current["time"]
    start_ms = end_ms - 24 * 3600 * 1000
    hist = await _get_json(
        session,
        "https://fapi.binance.com/futures/data/openInterestHist",
        {
            "symbol": symbol.upper(),
            "period": "5m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1,
        },
    )
    if not hist or not isinstance(hist, list) or not hist:
        return None
    try:
        old_oi = float(hist[0]["sumOpenInterest"])
    except (KeyError, TypeError, ValueError, IndexError):
        return None
    if old_oi <= 0:
        return None
    change = current["open_interest"] - old_oi
    change_pct = (change / old_oi) * 100
    price = await get_price(session, symbol)
    change_usd = change * price if price else None
    return {
        "current_oi": current["open_interest"],
        "oi_24h_ago": old_oi,
        "change": change,
        "change_pct": change_pct,
        "change_usd": change_usd,
        "price": price,
    }


# ─────────────────────────── RSS-новости ────────────────────────
async def _fetch_one_rss(
    session: aiohttp.ClientSession, url: str
) -> list[dict]:
    try:
        async with session.get(url, timeout=TIMEOUT, headers=HEADERS) as r:
            if r.status >= 400:
                log.warning("RSS HTTP %s on %s", r.status, url)
                return []
            text = await r.text()
    except asyncio.TimeoutError:
        log.warning("RSS timeout on %s", url)
        return []
    except Exception as e:
        log.warning("RSS error on %s: %s", url, e)
        return []

    loop = asyncio.get_running_loop()

    def _parse() -> list[dict]:
        d = feedparser.parse(text)
        out: list[dict] = []
        for e in d.entries[:50]:
            published = e.get("published") or e.get("updated") or ""
            try:
                dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                published_iso = dt.isoformat()
            except Exception:
                published_iso = published
            out.append({
                "title": (e.get("title") or "").strip(),
                "url": (e.get("link") or "").strip(),
                "source": (d.feed.get("title") or url).strip(),
                "published": published_iso,
            })
        return out

    return await loop.run_in_executor(None, _parse)


def _parse_date(s: str) -> float | None:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.timestamp()
        except Exception:
            return None


async def fetch_rss_feeds(
    session: aiohttp.ClientSession | None = None,
    lookback_hours: int | None = None,
) -> list[dict]:
    lookback = lookback_hours or config.NEWS_LOOKBACK_HOURS
    cutoff = time.time() - lookback * 3600
    own_session = session is None
    if own_session:
        session = _shared_session()
    try:
        results = await asyncio.gather(
            *[_fetch_one_rss(session, u) for u in config.NEWS_RSS_FEEDS],
            return_exceptions=True,
        )
    finally:
        if own_session:
            await session.close()

    seen: set[str] = set()
    items: list[dict] = []
    for r in results:
        if isinstance(r, Exception):
            continue
        for it in r:
            if not it.get("url") or it["url"] in seen:
                continue
            ts = _parse_date(it.get("published", ""))
            if ts is not None and ts < cutoff:
                continue
            seen.add(it["url"])
            items.append({**it, "ts": ts})
    items.sort(key=lambda x: x["ts"] or 0, reverse=True)
    return items


# ─────────────────────────── LLM-фильтрация новостей ────────────
NEWS_LLM_SYSTEM = (
    "Ты — фильтр крипто-новостей. Тебе дают сырой список новостей из разных "
    "RSS-источников (CoinDesk, Cointelegraph, The Block, Decrypt и др.).\n\n"
    "Твоя задача — выбрать ТОЛЬКО ЗНАЧИМЫЕ события, которые реально могут "
    "повлиять на рынок или требуют внимания инвестора/трейдера.\n\n"
    "**ЗНАЧИМЫМИ считаются** (примеры, не исчерпывающе):\n"
    "• Регуляторные действия: SEC, CFTC, MiCA, запреты, одобрения, штрафы\n"
    "• ETF: одобрение/отклонение спотовых BTC/ETH/Altcoin-ETF, притоки/оттоки\n"
    "• Листинги/делистинги на крупных CEX (Binance, Coinbase, Kraken)\n"
    "• Взломы протоколов и бирж, эксплойты, потери средств\n"
    "• Крупные партнёрства, M&A, инвестиции раундов $100M+\n"
    "• Макро-новости, влияющие на крипто: ставки ФРС, CPI, геополитика\n"
    "• Важные обновления протоколов: хардфорки, токеномика, эмиссия, сжигание\n"
    "• Институциональные шаги (BlackRock, Fidelity, MicroStrategy и т.п.)\n\n"
    "**НЕ значимые** (отбрасывай):\n"
    "• Аналитика/прогнозы без конкретного события\n"
    "• Обычные ценовые апдейты и реклама icos/airdrops\n"
    "• Переводные/дублированные новости\n"
    "• Мусор, кликбейт без фактов\n\n"
    "Для каждой значимой новости определи тикеры монет, которых она касается "
    "(BTC, ETH, SOL, TON и т.д. — строкой в верхнем регистре). Если новость "
    "касается рынка в целом — ставь пустой массив.\n\n"
    "Дополнительно: для каждой новости добавь поле \"title_ru\" — перевод "
    "заголовка на русский.\n\n"
    "Верни СТРОГО валидный JSON (без markdown-обёрток ```json) — массив объектов:\n"
    "[\n"
    "  {\n"
    '    "url": "точная ссылка из входа",\n'
    '    "title_ru": "перевод оригинального заголовка на русский (1 строка)",\n'
    '    "reason": "1 предложение на русском: почему это важно",\n'
    '    "coins": ["BTC"],\n'
    '    "importance": "high" | "medium"\n'
    "  },\n"
    "  ...\n"
    "]\n\n"
    "Если значимых новостей нет — верни []."
)


async def filter_news_with_llm(
    items: list[dict], target_coins: list[str] | None = None,
) -> list[dict]:
    if not items:
        return []

    if not config.OPENROUTER_API_KEY:
        # Fallback: эвристика + перевод через Google Translate
        out: list[dict] = []
        candidates = items[:config.NEWS_MAX_PER_LLM]
        translations = await _translate_titles([it["title"] for it in candidates])
        for it, title_ru in zip(candidates, translations):
            coins = _extract_coins_from_text(it["title"])
            if target_coins and not (set(coins) & set(target_coins)):
                if not coins and not _has_crypto_keywords(it["title"]):
                    continue
            out.append({
                **it, "coins": coins, "reason": "", "title_ru": title_ru,
            })
        return out

    payload_items = [
        {
            "url": it["url"],
            "title": it["title"],
            "source": it.get("source", ""),
            "published": it.get("published", ""),
        }
        for it in items[: config.NEWS_MAX_PER_LLM]
    ]
    user_msg = "Список новостей (JSON):\n" + json.dumps(payload_items, ensure_ascii=False)
    if target_coins:
        user_msg += f"\n\nОсобое внимание новостям про: {', '.join(target_coins)}."

    body = {
        "model": config.OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": NEWS_LLM_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.1,
        "max_tokens": 2000,
    }
    headers = {
        "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/crypto-tg-bot",
        "X-Title": "crypto-tg-bot",
    }
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
            async with s.post(
                "https://openrouter.ai/api/v1/chat/completions",
                json=body, headers=headers,
            ) as r:
                if r.status >= 400:
                    log.warning("OpenRouter HTTP %s: %s", r.status, await r.text())
                    return []
                data = await r.json()
    except Exception as e:
        log.warning("OpenRouter error: %s", e)
        return []

    content = (
        data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
    )
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content)
    try:
        filtered = json.loads(content)
    except Exception as e:
        log.warning("OpenRouter JSON parse error: %s; raw: %s", e, content[:200])
        return []

    by_url = {it["url"]: it for it in items}
    out: list[dict] = []
    for f in filtered:
        if not isinstance(f, dict) or "url" not in f:
            continue
        orig = by_url.get(f["url"])
        if not orig:
            continue
        title_ru = (f.get("title_ru") or "").strip()
        if not title_ru and _HAS_TRANSLATOR:
            try:
                title_ru = GoogleTranslator(
                    source="auto", target="ru"
                ).translate(orig["title"][:500])
            except Exception:
                title_ru = ""
        out.append({
            **orig,
            "coins": [c.upper() for c in f.get("coins", []) if isinstance(c, str)],
            "reason": f.get("reason", ""),
            "importance": f.get("importance", "medium"),
            "title_ru": title_ru,
        })
    return out


async def _translate_titles(titles: list[str]) -> list[str]:
    if not _HAS_TRANSLATOR or not titles:
        return list(titles)
    out: list[str] = []
    loop = asyncio.get_running_loop()
    for t in titles:
        if not t:
            out.append("")
            continue
        try:
            tr = await loop.run_in_executor(
                None,
                lambda txt=t: GoogleTranslator(
                    source="auto", target="ru"
                ).translate(txt[:500]),
            )
            out.append(tr or t)
        except Exception as e:
            log.debug("Translate failed: %s", e)
            out.append(t)
    return out


_CRYPTO_KW = (
    "btc", "bitcoin", "eth", "ethereum", "crypto", "blockchain",
    "altcoin", "token", "coin", "defi", "nft", "web3", "stable",
    "exchange", "binance", "coinbase", "sec", "etf", "halving",
    "mining", "wallet", "ledger", "tether", "usdt", "solana", "sol",
    "ton", "tron", "trx", "cardano", "ada", "ripple", "xrp",
    "dogecoin", "doge", "shib", "avax", "polkadot", "dot",
)


def _has_crypto_keywords(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in _CRYPTO_KW)


_KNOWN_COINS = [
    "BTC", "ETH", "SOL", "TON", "BNB", "XRP", "ADA", "DOGE",
    "AVAX", "DOT", "MATIC", "LINK", "TRX", "LTC", "BCH", "ATOM",
    "NEAR", "APT", "SUI", "ARB", "OP", "INJ", "RNDR", "FET",
    "PEPE", "SHIB", "WIF", "BONK", "JUP", "PYTH", "JTO",
]


def _extract_coins_from_text(text: str) -> list[str]:
    found: set[str] = set()
    upper = text.upper()
    for coin in _KNOWN_COINS:
        if re.search(rf"(?<![A-Z]){coin}(?![A-Z])", upper):
            found.add(coin)
    return sorted(found)


# ─────────────────────────── CoinGecko global ───────────────────
async def get_global_market(session: aiohttp.ClientSession) -> dict | None:
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
    s = symbol.upper()
    for suf in ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH"):
        if s.endswith(suf) and s != suf:
            return s[: -len(suf)]
    return s


# ─────────────────────────── утренняя сводка ────────────────────
async def build_morning_briefing() -> str:
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

    if fg:
        lines.append(
            f"😱 *Fear & Greed Index:* `{fg['value']}` — "
            f"{fear_greed_emoji(fg['value'])}"
        )
        lines.append("")

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
