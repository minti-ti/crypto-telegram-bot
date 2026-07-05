"""Все клиенты к внешним API. Асинхронные, с таймаутом и обработкой ошибок.

Источники данных:
  - data-api.binance.vision — spot цены, 24ч тикеры (обходит блокировку EU)
  - CoinGecko derivatives  — funding rate, open interest
  - CoinGlass API           — ликвидации (требует COINGLASS_API_KEY)
  - alternative.me          — Fear & Greed Index
  - RSS + LLM / Google Translate — новости
  - MQL5 / Finnhub          — экономический календарь
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
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

# Binance spot (EU-safe mirror)
BINANCE_SPOT = "https://data-api.binance.vision/api/v3"


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


# ─────────────────────────── Binance Spot: цены ─────────────────
async def get_24h_tickers(
    session: aiohttp.ClientSession, symbols: list[str]
) -> dict[str, dict]:
    """Все 24ч тикеры со spot (data-api.binance.vision)."""
    data = await _get_json(session, f"{BINANCE_SPOT}/ticker/24hr")
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
        f"{BINANCE_SPOT}/ticker/price",
        {"symbol": symbol.upper()},
    )
    if data and "price" in data:
        try:
            return float(data["price"])
        except (TypeError, ValueError):
            return None
    return None


# ─────────────────────────── CoinGecko derivatives ──────────────
_COINGECKO_DERIV_CACHE: dict[str, Any] = {"data": None, "ts": 0.0}
_COINGECKO_DERIV_TTL = 120  # секунд


async def _get_coingecko_derivatives(session: aiohttp.ClientSession) -> list[dict]:
    """Кэшированный запрос CoinGecko /derivatives."""
    now = time.time()
    if _COINGECKO_DERIV_CACHE["data"] and (now - _COINGECKO_DERIV_CACHE["ts"]) < _COINGECKO_DERIV_TTL:
        return _COINGECKO_DERIV_CACHE["data"]

    headers: dict[str, Any] = {}
    if config.COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = config.COINGECKO_API_KEY

    data = await _get_json(
        session,
        "https://api.coingecko.com/api/v3/derivatives",
        headers=headers,
    )
    if data and isinstance(data, list):
        _COINGECKO_DERIV_CACHE["data"] = data
        _COINGECKO_DERIV_CACHE["ts"] = now
        return data
    return _COINGECKO_DERIV_CACHE["data"] or []


async def get_funding_rate(
    session: aiohttp.ClientSession, symbol: str
) -> dict | None:
    """Funding rate через CoinGecko derivatives (Binance Futures)."""
    coin = symbol_to_coin(symbol).upper()
    derivs = await _get_coingecko_derivatives(session)
    if not derivs:
        return None

    # Ищем Binance Futures perpetual для нужной монеты
    for d in derivs:
        market = (d.get("market") or "").lower()
        sym = (d.get("symbol") or "").upper().replace("_", "").replace("/", "")
        contract = (d.get("contract_type") or "").lower()
        if "binance" not in market:
            continue
        if contract != "perpetual":
            continue
        # Сравниваем символы: BTCUSDT, BTC_USDT, BTC/USDT → BTCUSDT
        if sym in (symbol.upper(), f"{coin}USDT"):
            try:
                funding_raw = d.get("funding_rate")
                # CoinGecko возвращает funding_rate в % (0.01 = 0.01%)
                # Нужно перевести в дробный формат (0.0001)
                if funding_raw is None:
                    continue
                funding_pct = float(funding_raw)
                # CoinGecko: если значение > 1, это уже в % (напр. 0.01 = 0.01%)
                # В Binance: 0.0001 = 0.01%
                # CoinGecko даёт 0.01 как процент → делим на 100 для дробного
                if abs(funding_pct) < 1:
                    # Значение уже в процентах (0.01 = 0.01%)
                    funding_frac = funding_pct / 100
                else:
                    funding_frac = funding_pct / 100

                price = float(d.get("price") or 0)
                index_price = float(d.get("index") or 0)

                # nextFundingTime: CoinGecko не даёт точное время,
                # Binance funding каждые 8ч (00:00, 08:00, 16:00 UTC)
                now_utc = datetime.now(timezone.utc)
                next_hour = ((now_utc.hour // 8) + 1) * 8
                next_dt = now_utc.replace(hour=next_hour % 24, minute=0, second=0, microsecond=0)
                if next_hour >= 24:
                    next_dt += timedelta(days=1)
                next_funding_ms = int(next_dt.timestamp() * 1000)

                return {
                    "symbol": symbol.upper(),
                    "markPrice": price,
                    "indexPrice": index_price,
                    "lastFundingRate": funding_frac,
                    "nextFundingTime": next_funding_ms,
                }
            except (KeyError, TypeError, ValueError) as e:
                log.debug("Funding parse error for %s: %s", symbol, e)
                continue

    # Fallback: любая биржа для этой монеты
    for d in derivs:
        sym = (d.get("symbol") or "").upper().replace("_", "").replace("/", "")
        contract = (d.get("contract_type") or "").lower()
        if contract != "perpetual":
            continue
        if sym in (symbol.upper(), f"{coin}USDT"):
            try:
                funding_raw = d.get("funding_rate")
                if funding_raw is None:
                    continue
                funding_pct = float(funding_raw)
                funding_frac = funding_pct / 100
                price = float(d.get("price") or 0)
                index_price = float(d.get("index") or 0)

                now_utc = datetime.now(timezone.utc)
                next_hour = ((now_utc.hour // 8) + 1) * 8
                next_dt = now_utc.replace(hour=next_hour % 24, minute=0, second=0, microsecond=0)
                if next_hour >= 24:
                    next_dt += timedelta(days=1)
                next_funding_ms = int(next_dt.timestamp() * 1000)

                return {
                    "symbol": symbol.upper(),
                    "markPrice": price,
                    "indexPrice": index_price,
                    "lastFundingRate": funding_frac,
                    "nextFundingTime": next_funding_ms,
                }
            except (KeyError, TypeError, ValueError):
                continue
    return None


# ─────────────────────────── Open Interest ──────────────────────
async def get_open_interest(
    session: aiohttp.ClientSession, symbol: str
) -> dict | None:
    """Open Interest через CoinGecko derivatives (в USD)."""
    coin = symbol_to_coin(symbol).upper()
    derivs = await _get_coingecko_derivatives(session)
    if not derivs:
        return None

    # Ищем Binance Futures
    for d in derivs:
        market = (d.get("market") or "").lower()
        sym = (d.get("symbol") or "").upper().replace("_", "").replace("/", "")
        contract = (d.get("contract_type") or "").lower()
        if "binance" not in market or contract != "perpetual":
            continue
        if sym in (symbol.upper(), f"{coin}USDT"):
            try:
                oi_usd = float(d.get("open_interest") or 0)
                if oi_usd <= 0:
                    continue
                price = float(d.get("price") or 0)
                # Конвертируем USD → монеты
                oi_coins = oi_usd / price if price > 0 else 0
                return {
                    "symbol": symbol.upper(),
                    "open_interest": oi_coins,
                    "open_interest_usd": oi_usd,
                    "price": price,
                    "time": int(time.time() * 1000),
                }
            except (KeyError, TypeError, ValueError):
                continue

    # Fallback: любая биржа
    for d in derivs:
        sym = (d.get("symbol") or "").upper().replace("_", "").replace("/", "")
        contract = (d.get("contract_type") or "").lower()
        if contract != "perpetual":
            continue
        if sym in (symbol.upper(), f"{coin}USDT"):
            try:
                oi_usd = float(d.get("open_interest") or 0)
                if oi_usd <= 0:
                    continue
                price = float(d.get("price") or 0)
                oi_coins = oi_usd / price if price > 0 else 0
                return {
                    "symbol": symbol.upper(),
                    "open_interest": oi_coins,
                    "open_interest_usd": oi_usd,
                    "price": price,
                    "time": int(time.time() * 1000),
                }
            except (KeyError, TypeError, ValueError):
                continue
    return None


async def get_oi_change_24h(
    session: aiohttp.ClientSession, symbol: str
) -> dict | None:
    """
    Изменение Open Interest. CoinGecko не даёт историю,
    поэтому показываем текущий OI и цену.
    """
    current = await get_open_interest(session, symbol)
    if not current:
        return None

    price = current.get("price")
    if not price:
        price = await get_price(session, symbol)

    oi_usd = current.get("open_interest_usd")
    if not oi_usd:
        oi_usd = current["open_interest"] * price if price else None

    return {
        "current_oi": current["open_interest"],
        "oi_24h_ago": None,
        "change": None,
        "change_pct": None,
        "change_usd": None,
        "price": price,
        "oi_usd": oi_usd,
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


async def _filter_news_heuristic(
    items: list[dict], target_coins: list[str] | None = None,
) -> list[dict]:
    """Fallback-фильтр важных новостей без LLM.
    Используется, если OpenRouter не задан/упал/вернул невалидный JSON.
    """
    out: list[dict] = []
    target_set = {c.upper() for c in (target_coins or [])}
    candidates: list[tuple[dict, list[str], str, str]] = []
    for it in items[:config.NEWS_MAX_PER_LLM]:
        title = it.get("title", "")
        is_important, importance, reason = _classify_news_importance(title)
        if not is_important:
            continue
        coins = _extract_coins_from_text(title)
        # Для конкретной монеты показываем новости по ней + обще-рыночные.
        if target_set and coins and not (set(coins) & target_set):
            continue
        candidates.append((it, coins, importance, reason))

    translations = await _translate_titles([it["title"] for it, *_ in candidates])
    for (it, coins, importance, reason), title_ru in zip(candidates, translations):
        out.append({
            **it,
            "coins": coins,
            "reason": reason,
            "importance": importance,
            "title_ru": title_ru,
        })
    out.sort(key=lambda x: (0 if x.get("importance") == "high" else 1, -(x.get("ts") or 0)))
    return out[:8]


async def filter_news_with_llm(
    items: list[dict], target_coins: list[str] | None = None,
) -> list[dict]:
    if not items:
        return []

    if not config.OPENROUTER_API_KEY:
        # Без LLM используем строгий локальный фильтр, а не весь RSS.
        return await _filter_news_heuristic(items, target_coins)


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
                    return await _filter_news_heuristic(items, target_coins)
                data = await r.json()
    except Exception as e:
        log.warning("OpenRouter error: %s", e)
        return await _filter_news_heuristic(items, target_coins)

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
        return await _filter_news_heuristic(items, target_coins)

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
    if out:
        return out[:8]
    return await _filter_news_heuristic(items, target_coins)


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


_HIGH_IMPORTANCE_KW = (
    "sec", "cftc", "fomc", "federal reserve", "fed ", "interest rate",
    "cpi", "inflation", "nonfarm", "payroll", "jobs report",
    "etf", "blackrock", "fidelity", "microstrategy", "strategy buys",
    "hack", "hacked", "exploit", "exploited", "stolen", "breach",
    "lawsuit", "sues", "settlement", "settles", "fine", "charged", "charges",
    "ban", "bans", "approval", "approves", "approved", "rejects", "rejected",
    "delist", "delisting", "lists", "listing on binance", "coinbase lists",
    "bankruptcy", "insolvency", "default", "reserve", "depeg", "stablecoin",
)

_MEDIUM_IMPORTANCE_KW = (
    "mainnet", "hard fork", "upgrade", "token unlock", "airdrop",
    "merger", "acquisition", "raises", "funding round", "partnership",
    "treasury", "buyback", "burn", "staking", "validator",
    "etp", "futures", "options", "open interest", "liquidation",
)

_CLICKBAIT_OR_LOW_VALUE_KW = (
    "price prediction", "could reach", "will reach", "analyst says",
    "top 3", "best crypto", "to buy", "presale", "sponsored",
    "opinion", "how to", "guide", "what is", "learn",
)


def _classify_news_importance(title: str) -> tuple[bool, str, str]:
    """Грубый fallback-фильтр новостей, когда нет OPENROUTER_API_KEY.
    Возвращает: (важная ли, high/medium, причина на русском).
    """
    t = (title or "").lower()
    if not t or any(k in t for k in _CLICKBAIT_OR_LOW_VALUE_KW):
        return False, "medium", ""
    if any(k in t for k in _HIGH_IMPORTANCE_KW):
        return True, "high", "Событие может заметно повлиять на рынок: регуляторы, ETF, безопасность, листинг или макро."
    if any(k in t for k in _MEDIUM_IMPORTANCE_KW) and _has_crypto_keywords(title):
        return True, "medium", "Важное отраслевое событие: обновление, токеномика, инфраструктура или деривативы."
    return False, "medium", ""


_KNOWN_COINS = [
    "BTC", "ETH", "SOL", "TON", "BNB", "XRP", "ADA", "DOGE",
    "AVAX", "DOT", "MATIC", "LINK", "TRX", "LTC", "BCH", "ATOM",
    "NEAR", "APT", "SUI", "ARB", "OP", "INJ", "RNDR", "FET",
    "PEPE", "SHIB", "WIF", "BONK", "JUP", "PYTH", "JTO",
]


_COIN_ALIASES = {
    "BTC": ("bitcoin", "btc"),
    "ETH": ("ethereum", "ether", "eth"),
    "SOL": ("solana", "sol"),
    "TON": ("toncoin", "the open network", "ton"),
    "BNB": ("bnb", "binance coin"),
    "XRP": ("xrp", "ripple"),
    "ADA": ("cardano", "ada"),
    "DOGE": ("dogecoin", "doge"),
    "TRX": ("tron", "trx"),
    "LINK": ("chainlink", "link"),
    "AVAX": ("avalanche", "avax"),
    "DOT": ("polkadot", "dot"),
    "SUI": ("sui",),
}


def _extract_coins_from_text(text: str) -> list[str]:
    found: set[str] = set()
    upper = text.upper()
    lower = text.lower()
    for coin in _KNOWN_COINS:
        if re.search(rf"(?<![A-Z0-9]){coin}(?![A-Z0-9])", upper):
            found.add(coin)
    for coin, aliases in _COIN_ALIASES.items():
        for alias in aliases:
            if re.search(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", lower):
                found.add(coin)
                break
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


# ─────────────────────────── экономический календарь (Finnhub) ──
US_ECON_KEYWORDS = (
    "CPI", "NFP", "NONFARM", "FOMC", "FED", "GDP", "PPI",
    "UNEMPLOYMENT", "INTEREST RATE", "JOBLESS", "EMPLOYMENT",
    "INFLATION", "RETAIL SALES", "INDUSTRIAL PRODUCTION",
    "PMI", "ISM", "SERVICES", "MANUFACTURING", "JOLTS", "PAYROLL",
)

HIGH_IMPACT_KEYWORDS = (
    "CPI", "NFP", "NONFARM", "FOMC", "FED INTEREST RATE", "GDP", "PPI",
    "UNEMPLOYMENT RATE", "RETAIL SALES", "CORE RETAIL SALES",
    "ISM NON-MANUFACTURING", "ISM MANUFACTURING", "S&P GLOBAL SERVICES PMI",
    "S&P GLOBAL COMPOSITE PMI", "JOLTS", "FED GOVERNOR", "FED CHAIR", "FED SPEECH",
)


async def fetch_economic_calendar(
    session: aiohttp.ClientSession,
    from_date: str,
    to_date: str,
) -> list[dict]:
    """Finnhub economic calendar. from/to в формате YYYY-MM-DD."""
    if config.FINNHUB_API_KEY:
        try:
            data = await _get_json(
                session,
                "https://finnhub.io/api/v1/calendar/economic",
                {
                    "from": from_date,
                    "to": to_date,
                    "token": config.FINNHUB_API_KEY,
                },
            )
            if data and "economicCalendar" in data:
                return list(data["economicCalendar"])
        except Exception as e:
            log.warning("Finnhub calendar failed: %s", e)

    # Fallback: MQL5 economic calendar (не требует ключа)
    return await fetch_mql5_calendar(session, from_date, to_date)


async def fetch_mql5_calendar(
    session: aiohttp.ClientSession,
    from_date: str,
    to_date: str,
) -> list[dict]:
    """Парсим MQL5 economic calendar. Возвращает события в формате Finnhub."""
    url = "https://www.mql5.com/en/economic-calendar"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as r:
            if r.status >= 400:
                log.warning("MQL5 calendar HTTP %s", r.status)
                return []
            text = await r.text()
    except Exception as e:
        log.warning("MQL5 calendar error: %s", e)
        return []

    log.info("MQL5 calendar: fetched HTML length %s", len(text))

    events: list[dict] = []
    pattern = re.compile(
        r'<div class="ec-table__item ec-table__item_inline">'
        r'(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}),\s+([A-Z]{3}),\s+(.*?)</div>',
        re.S,
    )
    for m in pattern.finditer(text):
        date_str, time_str, currency, rest = m.groups()
        name_match = re.search(r'<a[^>]*>(.*?)</a>', rest)
        event_name = name_match.group(1) if name_match else rest
        event_name = html.unescape(re.sub(r'<[^>]+>', '', event_name)).strip()

        actual = estimate = previous = ""
        for label in ("Actual:", "Forecast:", "Previous:"):
            if label in rest:
                idx = rest.index(label)
                part = rest[idx:]
                val_end = part.find(",", len(label))
                if val_end == -1:
                    val_end = len(part)
                val = part[len(label):val_end].strip()
                if label == "Actual:":
                    actual = val
                elif label == "Forecast:":
                    estimate = val
                elif label == "Previous:":
                    previous = val

        try:
            dt = datetime.strptime(date_str, "%Y.%m.%d")
            iso_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            iso_date = date_str

        events.append({
            "date": iso_date,
            "time": time_str,
            "country": currency,
            "event": event_name,
            "actual": actual or None,
            "estimate": estimate or None,
            "previous": previous or None,
            "impact": "",
        })

    out = [e for e in events if from_date <= e["date"] <= to_date]
    log.info("MQL5 calendar: parsed %s events, filtered %s", len(events), len(out))
    return out


def _is_high_impact_event(name: str) -> bool:
    """Только самые важные макро-события."""
    n = name.upper()
    return any(k in n for k in HIGH_IMPACT_KEYWORDS)


async def _translate_events(events: list[dict]) -> list[dict]:
    """Переводит названия событий на русский через Google Translate."""
    if not _HAS_TRANSLATOR:
        return events
    try:
        loop = asyncio.get_running_loop()
        names = [e.get("event", "") for e in events]
        try:
            translated = await loop.run_in_executor(
                None,
                lambda: GoogleTranslator(source="auto", target="ru").translate_batch(names),
            )
        except Exception:
            translated = []
            for n in names:
                try:
                    tr = await loop.run_in_executor(
                        None,
                        lambda txt=n: GoogleTranslator(source="auto", target="ru").translate(txt),
                    )
                    translated.append(tr)
                except Exception:
                    translated.append(n)
        for e, tr in zip(events, translated):
            if tr and tr.strip():
                e["event_ru"] = tr.strip()
    except Exception as e:
        log.warning("Event translation failed: %s", e)
    return events


def _format_event_time_msk(e: dict) -> str:
    """Время события в московском часовом поясе (UTC+3)."""
    dt = _parse_event_datetime(e)
    if not dt:
        return "TBA"
    msk = dt + timedelta(hours=3)
    return msk.strftime("%H:%M MSK")


def _filter_us_macro_events(events: list[dict]) -> list[dict]:
    """Оставляем только US macro events, которые влияют на рынок."""
    out: list[dict] = []
    for e in events:
        country = (e.get("country") or "").upper()
        if country not in ("US", "USD", "UNITED STATES", "USA"):
            continue
        event_name = (e.get("event") or "").upper()

        impact_raw = e.get("impact")
        impact_val = 0
        if isinstance(impact_raw, (int, float)):
            impact_val = int(impact_raw)
        elif isinstance(impact_raw, str) and impact_raw.strip():
            impact_raw = impact_raw.strip().lower()
            if impact_raw.isdigit():
                impact_val = int(impact_raw)
            else:
                impact_val = {"low": 1, "medium": 2, "high": 3}.get(impact_raw, 0)

        if any(kw in event_name for kw in US_ECON_KEYWORDS):
            out.append(e)
            continue

        if impact_val >= config.ECON_MIN_IMPORTANCE:
            out.append(e)
    return out


def _parse_event_datetime(e: dict) -> datetime | None:
    """Парсим date + time в datetime UTC. MQL5 возвращает UTC."""
    date_s = e.get("date") or ""
    time_s = e.get("time") or ""
    if not date_s:
        return None
    try:
        dt = datetime.strptime(date_s, "%Y-%m-%d")
    except ValueError:
        return None
    if time_s and time_s != "TBA":
        try:
            hh, mm = map(int, time_s.split(":"))
            dt = dt.replace(hour=hh, minute=mm)
        except Exception:
            pass
    return dt.replace(tzinfo=timezone.utc)


def _event_id(e: dict) -> str:
    return f"{e.get('date', '')}|{e.get('time', '')}|{e.get('country', '')}|{e.get('event', '')}"


def _fmt_event(e: dict) -> str:
    event = e.get("event_ru") or e.get("event", "")
    time_s = _format_event_time_msk(e)
    actual = e.get("actual")
    estimate = e.get("estimate")
    previous = e.get("previous")
    impact = (e.get("impact") or "").lower()
    emoji = "🔥" if impact == "3" or impact == "high" else "⚡" if impact == "2" or impact == "medium" else "•"
    parts = [f"{emoji} *{event}* — `{time_s}`"]
    vals = []
    if estimate is not None:
        vals.append(f"прогноз `{estimate}`")
    if previous is not None:
        vals.append(f"предыдущее `{previous}`")
    if actual is not None:
        vals.append(f"факт `{actual}`")
    if vals:
        parts.append(f"  _{' · '.join(vals)}_")
    return "\n".join(parts)


async def build_econ_calendar_text(days: int = 1) -> str:
    """Собрать текст календаря на ближайшие дни: предстоящие, важные, на русском, MSK."""
    now = datetime.now(timezone.utc)
    from_date = now.strftime("%Y-%m-%d")
    to_date = (now + timedelta(days=days)).strftime("%Y-%m-%d")
    async with _shared_session() as s:
        events = await fetch_economic_calendar(s, from_date, to_date)
    events = _filter_us_macro_events(events)
    # Только предстоящие
    events = [e for e in events if _parse_event_datetime(e) and _parse_event_datetime(e) > now]
    if not events:
        return "📅 *Нет предстоящих US macro-событий на ближайшие дни.*"

    # Сначала показываем самые важные. Если их нет — показываем ближайшие
    # средне-важные USD-события, чтобы календарь не выглядел пустым.
    high_events = [e for e in events if _is_high_impact_event(e.get("event", ""))]
    title = "📅 *Важные предстоящие US-события (MSK)*\n"
    if high_events:
        events = high_events
    else:
        title = "📅 *Ближайшие US macro-события (MSK)*\n"

    # Сортировка по времени
    events.sort(key=lambda e: _parse_event_datetime(e) or datetime.min.replace(tzinfo=timezone.utc))
    # Перевод на русский
    events = await _translate_events(events)
    lines = [title]
    for e in events[:12]:
        lines.append(_fmt_event(e))
    return "\n\n".join(lines)



async def get_upcoming_macro_events(
    session: aiohttp.ClientSession,
    within_minutes: int = 60,
) -> list[dict]:
    """Вернуть события, которые произойдут в ближайшие within_minutes."""
    now = datetime.now(timezone.utc)
    from_date = now.strftime("%Y-%m-%d")
    to_date = (now + timedelta(days=2)).strftime("%Y-%m-%d")
    events = await fetch_economic_calendar(session, from_date, to_date)
    events = _filter_us_macro_events(events)
    out: list[dict] = []
    for e in events:
        dt = _parse_event_datetime(e)
        if not dt:
            continue
        delta = (dt - now).total_seconds() / 60
        if 0 < delta <= within_minutes:
            out.append(e)
    return out


# ─────────────────────────── ликвидации (CoinGlass API) ─────────
_LIQ_HISTORY: deque[dict] = deque()
_LIQ_MAX_AGE_HOURS = 24


async def fetch_liquidations(
    session: aiohttp.ClientSession,
    hours: int = 24,
) -> dict[str, dict]:
    """
    Получает агрегированные ликвидации через CoinGlass API v4.
    Возвращает {symbol: {total_usd, long_usd, short_usd}}.

    Важно: у CoinGlass API нет бесплатного тарифа. Для Hobbyist минимальный
    interval обычно 4h, поэтому для сводки за 24ч используем 4h как безопасный
    интервал; если тариф выше — это всё равно работает.
    """
    if not config.COINGLASS_API_KEY:
        return {}

    results: dict[str, dict] = {}
    interval = "4h" if hours >= 4 else "1h"
    limit = max(1, min(1000, int(hours / 4) if interval == "4h" else hours))
    exchanges = "Binance,OKX,Bybit"

    for sym in config.LIQ_SYMBOLS:
        coin = symbol_to_coin(sym)
        try:
            data = await _get_json(
                session,
                "https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-history",
                {
                    "exchange_list": exchanges,
                    "symbol": coin,
                    "interval": interval,
                    "limit": limit,
                },
                headers={
                    "accept": "application/json",
                    "CG-API-KEY": config.COINGLASS_API_KEY,
                },
            )
            if not data or str(data.get("code")) != "0" or not data.get("data"):
                log.warning("CoinGlass v4: no data for %s: %s", coin, data)
                continue

            items = data.get("data") or []
            total_usd = 0.0
            long_usd = 0.0
            short_usd = 0.0
            for item in items:
                # v4 fields
                l = float(item.get("aggregated_long_liquidation_usd") or 0)
                sh = float(item.get("aggregated_short_liquidation_usd") or 0)
                long_usd += l
                short_usd += sh
                total_usd += l + sh

            results[sym.upper()] = {
                "total_usd": total_usd,
                "long_usd": long_usd,
                "short_usd": short_usd,
            }

        except Exception as e:
            log.warning("CoinGlass error for %s: %s", coin, e)

    return results


async def build_liquidations_text(hours: int = 24) -> str:
    """Текст с ликвидациями за последние hours часов через CoinGlass."""
    async with _shared_session() as s:
        data = await fetch_liquidations(s, hours=hours)

    if not data:
        if not config.COINGLASS_API_KEY:
            return (
                "💥 *Ликвидации*\n\n"
                "⚠️ Не задан `COINGLASS_API_KEY` в `.env`.\n"
                "У CoinGlass сейчас нет бесплатного API-тарифа; минимальный план — Hobbyist.\n"
                "После оплаты добавь ключ в переменную `COINGLASS_API_KEY`.\n\n"
                "_Без ключа данные о ликвидациях недоступны._"
            )
        return (
            f"💥 *Ликвидации за {hours}ч*\n\n"
            f"Нет данных по `{', '.join(config.LIQ_SYMBOLS)}`.\n"
            "_Попробуй позже._"
        )

    lines = [f"💥 *Ликвидации за {hours}ч*\n"]
    total_all = 0.0
    for sym in sorted(data, key=lambda s: data[s]["total_usd"], reverse=True):
        info = data[sym]
        total = info["total_usd"]
        total_all += total
        coin = symbol_to_coin(sym)
        long_usd = info.get("long_usd", 0)
        short_usd = info.get("short_usd", 0)

        lines.append(f"*{coin}* — всего `{fmt_volume(total)}`")
        if long_usd or short_usd:
            lines.append(f"  🟢 Лонги: `{fmt_volume(long_usd)}`")
            lines.append(f"  🔴 Шорты: `{fmt_volume(short_usd)}`")
        lines.append("")

    if total_all > 0:
        lines.append(f"_Всего: {fmt_volume(total_all)}_")

    return "\n".join(lines)


# ─────────────────────────── утренняя сводка ────────────────────
async def build_morning_briefing() -> str:
    lines: list[str] = []
    now_utc = datetime.now(timezone.utc)
    today = now_utc.strftime("%d.%m.%Y")
    today_api = now_utc.strftime("%Y-%m-%d")
    lines.append(f"☀️ *Утренняя сводка — {today}*")
    lines.append("")

    async with _shared_session() as s:
        fg, tickers, global_m, funding, cal_events = await asyncio.gather(
            get_fear_greed(s),
            get_24h_tickers(s, config.TOP_SYMBOLS),
            get_global_market(s),
            asyncio.gather(*[get_funding_rate(s, x) for x in config.FUNDING_SYMBOLS]),
            fetch_economic_calendar(s, today_api, today_api),
        )

    if cal_events:
        us_events = _filter_us_macro_events(cal_events)
        us_events = [e for e in us_events if _is_high_impact_event(e.get("event", ""))]
        if us_events:
            us_events = await _translate_events(us_events)
            lines.append("📅 *Важные US-события сегодня:*")
            for e in us_events[:5]:
                lines.append(_fmt_event(e))
            lines.append("")

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
