"""Все клиенты к внешним API. Асинхронные, с таймаутом и обработкой ошибок."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import time
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
        session, "https://fapi.binance.com/fapi/v1/ticker/24hr"
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
        "https://fapi.binance.com/fapi/v1/ticker/price",
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


# ─────────────────────────── экономический календарь (Finnhub) ──
US_ECON_KEYWORDS = (
    "CPI", "NFP", "NONFARM", "FOMC", "FED", "GDP", "PPI",
    "UNEMPLOYMENT", "INTEREST RATE", "JOBLESS", "EMPLOYMENT",
    "INFLATION", "RETAIL SALES", "INDUSTRIAL PRODUCTION",
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

    # MQL5 рендерит события в div'ах вида:
    # <div class="ec-table__item ec-table__item_inline">2026.06.17 18:00, USD, <a href="...">FOMC Statement</a></div>
    events: list[dict] = []
    pattern = re.compile(
        r'<div class="ec-table__item ec-table__item_inline">'
        r'(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}),\s+([A-Z]{3}),\s+(.*?)</div>',
        re.S,
    )
    for m in pattern.finditer(text):
        date_str, time_str, currency, rest = m.groups()
        # Извлекаем название события из ссылки
        name_match = re.search(r'<a[^>]*>(.*?)</a>', rest)
        event_name = name_match.group(1) if name_match else rest
        # Убираем HTML-теги
        event_name = html.unescape(re.sub(r'<[^>]+>', '', event_name)).strip()

        # Парсим Actual/Forecast/Previous из rest
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

        # Переводим дату в YYYY-MM-DD
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
            "impact": "",  # MQL5 не даёт impact
        })

    # Фильтруем по датам
    out = [e for e in events if from_date <= e["date"] <= to_date]
    log.info("MQL5 calendar: parsed %s events, filtered %s", len(events), len(out))
    return out


def _filter_us_macro_events(events: list[dict]) -> list[dict]:
    """Оставляем только US macro events, которые влияют на рынок."""
    out: list[dict] = []
    for e in events:
        country = (e.get("country") or "").upper()
        if country not in ("US", "USD", "UNITED STATES", "USA"):
            continue
        event_name = (e.get("event") or "").upper()

        # impact у Finnhub — строка "1" / "2" / "3" (1=low, 2=medium, 3=high)
        # у MQL5 impact нет, считаем его неизвестным
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

        # Важные ключевые слова — всегда пропускаем
        if any(kw in event_name for kw in US_ECON_KEYWORDS):
            out.append(e)
            continue

        # Если impact известен и достаточно высокий — тоже пропускаем
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
    event = e.get("event", "")
    time_s = e.get("time") or "TBA"
    actual = e.get("actual")
    estimate = e.get("estimate")
    previous = e.get("previous")
    impact = (e.get("impact") or "").lower()
    emoji = "🔥" if impact == "3" or impact == "high" else "⚡" if impact == "2" or impact == "medium" else "•"
    parts = [f"{emoji} *{event}* — `{time_s} UTC`"]
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
    """Собрать текст календаря на ближайшие дни."""
    today = datetime.now(timezone.utc)
    from_date = today.strftime("%Y-%m-%d")
    to_date = (today + timedelta(days=days)).strftime("%Y-%m-%d")
    async with _shared_session() as s:
        events = await fetch_economic_calendar(s, from_date, to_date)
    events = _filter_us_macro_events(events)
    if not events:
        return "📅 *Экономический календарь*\n\nНет важных US-событий на ближайшие дни."
    lines = [f"📅 *Экономический календарь (US)*\n_Время UTC_\n"]
    for e in events[:20]:
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


# ─────────────────────────── ликвидации (Binance forceOrders) ───
async def fetch_force_orders(
    session: aiohttp.ClientSession,
    symbol: str | None = None,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
) -> list[dict]:
    """Получить forceOrders с Binance Futures."""
    params: dict[str, Any] = {"limit": limit}
    if symbol:
        params["symbol"] = symbol.upper()
    if start_time_ms:
        params["startTime"] = start_time_ms
    if end_time_ms:
        params["endTime"] = end_time_ms
    data = await _get_json(
        session,
        "https://fapi.binance.com/fapi/v1/forceOrders",
        params,
    )
    if not data or not isinstance(data, list):
        return []
    return data


async def fetch_all_force_orders(
    session: aiohttp.ClientSession,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
) -> list[dict]:
    """Все ликвидации за период."""
    params: dict[str, Any] = {"limit": limit}
    if start_time_ms:
        params["startTime"] = start_time_ms
    if end_time_ms:
        params["endTime"] = end_time_ms
    data = await _get_json(
        session,
        "https://fapi.binance.com/fapi/v1/allForceOrders",
        params,
    )
    if not data or not isinstance(data, list):
        return []
    return data


def aggregate_liquidations(
    orders: list[dict],
    symbols: list[str],
    window_minutes: int = 60,
) -> dict[str, float]:
    """Сумма USD-ликвидаций за window_minutes по указанным символам."""
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - window_minutes * 60 * 1000
    wanted = {s.upper() for s in symbols}
    totals: dict[str, float] = {}
    for o in orders:
        sym = (o.get("symbol") or "").upper()
        if wanted and sym not in wanted:
            continue
        ts = o.get("time") or 0
        if ts < cutoff_ms:
            continue
        try:
            price = float(o.get("price", 0))
            qty = float(o.get("executedQty", 0))
        except (TypeError, ValueError):
            continue
        if price <= 0 or qty <= 0:
            continue
        totals[sym] = totals.get(sym, 0.0) + price * qty
    return totals


def aggregate_liquidations_by_bucket(
    orders: list[dict],
    symbols: list[str],
    bucket_hours: int = 6,
    total_hours: int = 24,
) -> tuple[dict[tuple[str, int], float], dict[str, float]]:
    """Группировка ликвидаций по bucket_hours-часовым интервалам."""
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - total_hours * 3600 * 1000
    wanted = {s.upper() for s in symbols}
    bucket_ms = bucket_hours * 3600 * 1000
    buckets: dict[tuple[str, int], float] = {}
    totals: dict[str, float] = {}
    for o in orders:
        sym = (o.get("symbol") or "").upper()
        if wanted and sym not in wanted:
            continue
        ts = o.get("time") or 0
        if ts < cutoff_ms or ts > now_ms:
            continue
        try:
            price = float(o.get("price", 0))
            qty = float(o.get("executedQty", 0))
        except (TypeError, ValueError):
            continue
        if price <= 0 or qty <= 0:
            continue
        val = price * qty
        bucket_idx = int((now_ms - ts) // bucket_ms)
        key = (sym, bucket_idx)
        buckets[key] = buckets.get(key, 0.0) + val
        totals[sym] = totals.get(sym, 0.0) + val
    return buckets, totals


async def build_liquidations_text(hours: int = 24) -> str:
    """Текст с ликвидациями за последние hours часов с разбивкой по интервалам."""
    now_ms = int(time.time() * 1000)
    bucket_hours = 6
    all_orders: list[dict] = []
    async with _shared_session() as s:
        # Разбиваем на 6-часовые интервалы, чтобы не терять данные из-за limit=1000
        for i in range(hours // bucket_hours):
            end_ms = now_ms - i * bucket_hours * 3600 * 1000
            start_ms = end_ms - bucket_hours * 3600 * 1000
            orders = await fetch_all_force_orders(
                s, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000
            )
            all_orders.extend(orders)
    buckets, totals = aggregate_liquidations_by_bucket(
        all_orders, config.LIQ_SYMBOLS, bucket_hours=bucket_hours, total_hours=hours
    )
    if not totals:
        return (
            f"💥 *Ликвидации за {hours}ч*\n\n"
            f"Нет данных по `{', '.join(config.LIQ_SYMBOLS)}`."
        )
    labels = ["0-6ч", "6-12ч", "12-18ч", "18-24ч"]
    lines = [f"💥 *Ликвидации за {hours}ч*\n"]
    total_all = 0.0
    for sym in sorted(totals, key=totals.get, reverse=True):
        val = totals[sym]
        total_all += val
        coin = symbol_to_coin(sym)
        lines.append(f"*{coin}* — всего `{fmt_volume(val)}`")
        for idx, label in enumerate(labels):
            bucket_val = buckets.get((sym, idx), 0.0)
            lines.append(f"  {label}: `{fmt_volume(bucket_val)}`")
        lines.append("")
    lines.append(f"_Всего: {fmt_volume(total_all)}_")
    return "\n".join(lines)


# ─────────────────────────── утренняя сводка ────────────────────
async def build_morning_briefing() -> str:
    lines: list[str] = []
    today = datetime.now(timezone.utc).strftime("%d.%m.%Y")
    lines.append(f"☀️ *Утренняя сводка — {today}*")
    lines.append("")

    async with _shared_session() as s:
        fg, tickers, global_m, funding, cal_events = await asyncio.gather(
            get_fear_greed(s),
            get_24h_tickers(s, config.TOP_SYMBOLS),
            get_global_market(s),
            asyncio.gather(*[get_funding_rate(s, x) for x in config.FUNDING_SYMBOLS]),
            fetch_economic_calendar(s, today, today),
        )

    if cal_events:
        us_events = _filter_us_macro_events(cal_events)
        if us_events:
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
