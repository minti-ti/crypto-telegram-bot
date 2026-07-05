"""Точка входа: хендлеры, планировщик и новости."""
from __future__ import annotations

import asyncio
import logging
import re
import sys
from datetime import datetime, timezone
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.types import InlineKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

import config
import db
import services
from keyboards import (
    main_menu_kb,
    kb_price, kb_price_symbols, kb_market, kb_news_my_subs, kb_news_after,
    kb_subs_current, kb_settime, kb_calendar, kb_liq,
    cancel_kb,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("crypto-bot")

bot = Bot(
    token=config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Глобальный scheduler (для reschedule из /settime)
_scheduler: AsyncIOScheduler | None = None
_health_runner: web.AppRunner | None = None


# ─────────────────────────── helper: edit-in-place ───────────────
async def _send_or_edit(
    target: types.Message | types.CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    disable_web_page_preview: bool = False,
    parse_mode: str = ParseMode.MARKDOWN,
) -> types.Message:
    """Редактирует сообщение, если target — CallbackQuery, иначе отправляет новое."""
    msg = target if isinstance(target, types.Message) else target.message
    if not msg:
        raise ValueError("No message to edit or send")
    try:
        return await msg.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
    except Exception:
        return await msg.answer(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )


def _md(text: str | None) -> str:
    """Минимально экранирует пользовательский/внешний текст для Telegram Markdown."""
    if not text:
        return ""
    return (
        str(text)
        .replace("\\", "\\\\")
        .replace("*", "")
        .replace("`", "")
        .replace("[", "(")
        .replace("]", ")")
        .replace("_", " ")
    )


# ─────────────────────────── FSM ───────────────────────────────
class ConvertState(StatesGroup):
    waiting_input = State()


class NewsCustomCoin(StatesGroup):
    waiting_coin = State()


class SubCustomCoin(StatesGroup):
    waiting_coin = State()


class PriceCustomState(StatesGroup):
    waiting_coin = State()


class RiskCalc(StatesGroup):
    deposit = State()  # депозит в USD
    entry = State()    # цена входа
    stop = State()     # стоп-лосс


# ─────────────────────────── /start и меню ─────────────────────
HELP_TEXT = (
    "🤖 *Crypto-бот*\n\n"
    "*Основные команды:*\n"
    "`/price <тикер>` — цена (напр. `/price BTC`)\n"
    "`/fg` — Fear & Greed Index\n"
    "`/funding` — funding rate BTC/ETH/SOL\n"
    "`/oi BTC` — Open Interest за 24ч\n"
    "`/news [BTC,ETH,...]` — важные новости по монетам\n"
    "`/top` — топ рост/падение за 24ч\n"
    "`/convert` — конвертер криптовалют\n"
    "`/calendar` — экономический календарь (US)\n"
    "`/liq` — ликвидации за час (BTC/ETH)\n"
    "`/calc` — калькулятор позиции\n\n"
    "*Подписки на новости:*\n"
    "`/subscribe BTC,ETH,SOL` — присылать важные новости\n"
    "`/unsubscribe BTC`\n"
    "`/mysubs`\n\n"
    "*Прочее:*\n"
    "`/morning on|off` — утренняя сводка\n"
    "`/briefing` — прислать сводку сейчас\n"
    "`/settime HH:MM` — время сводки\n"
    "`/help` — эта справка"
)


@router.message(Command("start"))
async def cmd_start(m: types.Message) -> None:
    await db.upsert_user(m.from_user.id)
    current_time = await db.get_setting("morning_time") or config.MORNING_TIME
    await m.answer(
        f"Привет, *{m.from_user.first_name or 'друг'}*! 👋\n\n"
        "Я — твой персональный крипто-ассистент.\n"
        f"Каждое утро в *{current_time}* (Europe/Moscow) присылаю сводку рынка.\n"
        "Нажми кнопку или набери /help.",
        reply_markup=main_menu_kb(),
    )


@router.message(Command("help"))
async def cmd_help(m: types.Message) -> None:
    await m.answer(HELP_TEXT, reply_markup=kb_market())


# ─────────────────────────── callback'и от меню ─────────────────
@router.callback_query(F.data.startswith("cmd:"))
async def cb_menu(c: types.CallbackQuery, state: FSMContext) -> None:
    cmd = c.data.split(":", 1)[1]
    await c.answer()

    if cmd == "menu":
        await _send_or_edit(c, "🏠 *Главное меню*", reply_markup=main_menu_kb())
        return
    if cmd == "fg":
        await _send_fg(c)
        return
    if cmd == "funding":
        await _send_funding(c)
        return
    if cmd == "subs":
        await _show_subs(c)
        return
    if cmd == "top":
        await _send_top(c)
        return
    if cmd == "briefing":
        await _send_briefing(c)
        return
    if cmd == "oi":
        await _send_oi_top(c)
        return
    if cmd == "calc":
        await state.set_state(RiskCalc.deposit)
        await _send_or_edit(
            c,
            "📊 *Калькулятор позиции*\n\n"
            "Риск фиксирован — *1%* от депозита.\n"
            "Введи *размер депозита в USD*:",
            reply_markup=cancel_kb(),
        )
        return
    if cmd == "settime":
        current = await db.get_setting("morning_time") or config.MORNING_TIME
        await _send_or_edit(
            c,
            f"⏰ *Текущее время утренней сводки:* `{current}` MSK\n\n"
            "Изменить: `/settime HH:MM` (например `/settime 08:30`)\n"
            "Или нажми кнопку ниже:",
            reply_markup=kb_settime(current),
        )
        return
    if cmd == "news":
        subs = await db.get_user_subs(c.from_user.id)
        # Если есть подписки — показываем меню. Если нет — сразу все новости
        if subs:
            await _send_or_edit(
                c,
                "📰 *Новости за 24ч — выбери монету:*\n"
                "_Твои подписки сверху. Если нужна другая — нажми 'Другая монета'._",
                reply_markup=kb_news_my_subs(subs),
            )
        else:
            await _show_news(c, [], lookback_hours=24)
        return
    if cmd == "price":
        await _send_or_edit(
            c,
            "💰 *Цена* — выбери монету или введи тикер вручную:",
            reply_markup=kb_price_symbols(),
        )
        return
    if cmd == "morning":
        new_val = await db.toggle_morning(c.from_user.id)
        await _send_or_edit(
            c,
            f"🌅 Утренняя сводка: {'включена ✅' if new_val else 'выключена ❌'}",
            reply_markup=kb_market(),
        )
        return
    if cmd == "calendar":
        await _send_calendar(c)
        return
    if cmd == "liq":
        await _send_liq(c)
        return


@router.callback_query(F.data == "noop")
async def cb_noop(c: types.CallbackQuery) -> None:
    """Заглушка для информационных кнопок-разделителей."""
    await c.answer()


@router.callback_query(F.data == "cancel")
async def cb_cancel(c: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await c.answer("Отменено")
    await _send_or_edit(c, "❌ Отменено.", reply_markup=main_menu_kb())


# ─────────────────────────── /price ─────────────────────────────
async def _send_price(target: types.Message | types.CallbackQuery, symbol: str) -> None:
    sym = symbol.upper().replace("/", "")
    if not sym.endswith("USDT") and sym not in ("BTCUSDT", "ETHUSDT"):
        sym = sym + "USDT"
    async with services._shared_session() as s:
        ticker = await services.get_market_ticker(s, sym)
    if not ticker:
        await _send_or_edit(target, f"❌ Не нашёл `{sym}` в источниках данных OKX/CoinGecko. Проверь тикер.")
        return
    try:
        price = float(ticker["lastPrice"])
        ch = float(ticker["priceChangePercent"])
        hi = float(ticker["highPrice"])
        lo = float(ticker["lowPrice"])
        vol = float(ticker.get("quoteVolume", 0))
    except (KeyError, TypeError, ValueError):
        await _send_or_edit(target, "⚠️ Не удалось распарсить ответ биржи.")
        return
    coin = services.symbol_to_coin(sym)
    await _send_or_edit(
        target,
        f"💰 *{coin}*\n"
        f"Цена: *{services.fmt_usd(price, 2)}*\n"
        f"24ч: {services.fmt_pct(ch)}\n"
        f"High: {services.fmt_usd(hi)} · Low: {services.fmt_usd(lo)}\n"
        f"Объём: {services.fmt_volume(vol)}",
        reply_markup=kb_price(sym),
    )


@router.message(Command("price"))
async def cmd_price(m: types.Message, command: CommandObject, state: FSMContext) -> None:
    if not command.args:
        await _send_or_edit(
            m,
            "💰 *Цена* — выбери монету или введи тикер вручную:",
            reply_markup=kb_price_symbols(),
        )
        return
    await _send_price(m, command.args.strip())


@router.callback_query(F.data.startswith("price:"))
async def cb_price_coin(c: types.CallbackQuery, state: FSMContext) -> None:
    await c.answer()
    coin = c.data.split(":", 1)[1].upper()
    if coin == "CUSTOM":
        await state.set_state(PriceCustomState.waiting_coin)
        await _send_or_edit(
            c,
            "✏️ Введи тикер монеты (например `BTC`, `ETH`, `SOL`):",
            reply_markup=cancel_kb(),
        )
        return
    await _send_price(c, coin)


@router.message(PriceCustomState.waiting_coin)
async def price_custom_coin(m: types.Message, state: FSMContext) -> None:
    await state.clear()
    await _send_price(m, m.text.strip())


# ─────────────────────────── /fg ────────────────────────────────
async def _send_fg(target: types.Message | types.CallbackQuery) -> None:
    async with services._shared_session() as s:
        fg = await services.get_fear_greed(s)
    if not fg:
        await _send_or_edit(target, "⚠️ Не удалось получить Fear & Greed Index.", reply_markup=kb_market())
        return
    await _send_or_edit(
        target,
        f"😱 *Fear & Greed Index*\n"
        f"Значение: *{fg['value']}* — {services.fear_greed_emoji(fg['value'])}\n"
        f"_Обновлено: {datetime.fromtimestamp(fg['timestamp'], tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}_",
        reply_markup=kb_market(),
    )


@router.message(Command("fg"))
async def cmd_fg(m: types.Message) -> None:
    await _send_fg(m)


# ─────────────────────────── /funding ──────────────────────────
async def _send_funding(target: types.Message | types.CallbackQuery) -> None:
    async with services._shared_session() as s:
        results = await asyncio.gather(
            *[services.get_funding_rate(s, x) for x in config.FUNDING_SYMBOLS]
        )
    valid = [r for r in results if r]
    if not valid:
        await _send_or_edit(target, "⚠️ Не удалось получить funding rate.", reply_markup=kb_market())
        return
    lines = ["💸 *Funding rate (текущий):*"]
    for r in valid:
        coin = services.symbol_to_coin(r["symbol"])
        pct = r["lastFundingRate"] * 100
        arrow = "🟢" if pct >= 0 else "🔴"
        next_dt = datetime.fromtimestamp(
            r["nextFundingTime"] / 1000, tz=timezone.utc
        ).strftime("%H:%M UTC")
        lines.append(
            f"• *{coin}*: {arrow} {pct:+.4f}%  "
            f"_mark {services.fmt_usd(r['markPrice'])}_\n"
            f"  Следующее списание: {next_dt}"
        )
    await _send_or_edit(target, "\n".join(lines), reply_markup=kb_market())


@router.message(Command("funding"))
async def cmd_funding(m: types.Message) -> None:
    await _send_funding(m)


# ─────────────────────────── /news ──────────────────────────────
def _format_news_message(items: list[dict], coins_filter: list[str] | None = None) -> str:
    """Форматирует список важных новостей. Заголовки всегда на русском,
    оригинал — второй строкой, если отличается. Внешний текст экранируется.
    """
    if not items:
        return (
            "📰 Свежих важных новостей нет"
            + (f" по `{', '.join(coins_filter)}`" if coins_filter else "")
            + " за последние 24ч."
        )

    header = "📰 *Самые важные новости за 24ч"
    if coins_filter:
        header += f" ({', '.join(coins_filter)})"
    header += ":*\n"
    lines = [header]

    for it in items[:8]:
        title_orig = _md(it.get("title", ""))
        title_ru = _md((it.get("title_ru") or "").strip())
        if not title_ru:
            title_ru = title_orig

        if title_ru and title_orig and title_ru.lower() != title_orig.lower():
            title_part = f"*{title_ru}*\n  _{title_orig}_"
        else:
            title_part = f"*{title_orig}*"

        coins = [c for c in it.get("coins", []) if isinstance(c, str)]
        coin_part = f" `[{_md(' '.join(coins[:4]))}]`" if coins else ""
        fire = "🔥 " if it.get("importance") == "high" else ""
        reason = _md((it.get("reason") or "").strip())
        reason_part = f"\n  _{reason}_" if reason else ""
        source = _md(it.get("source") or "source")
        url = it.get("url", "")

        lines.append(
            f"• {fire}{title_part}{coin_part}{reason_part}\n"
            f"  [↗ {source}]({url})"
        )
    return "\n\n".join(lines)


@router.message(Command("news"))
async def cmd_news(m: types.Message, command: CommandObject) -> None:
    if command.args:
        currencies = [
            c.strip().upper() for c in re.split(r"[,\s]+", command.args) if c.strip()
        ]
        await _show_news(m, currencies, lookback_hours=24)
        return
    # /news без аргументов — все новости за 24ч
    await _show_news(m, [], lookback_hours=24)


async def _show_news(
    target: types.Message | types.CallbackQuery,
    currencies: list[str],
    lookback_hours: int = 24,
) -> None:
    """Собрать новости и отправить/отредактировать."""
    await _send_or_edit(target, "⏳ Собираю новости...")
    raw = await services.fetch_rss_feeds(lookback_hours=lookback_hours)
    if not raw:
        await _send_or_edit(
            target,
            "⚠️ Не удалось получить новости из RSS.",
            reply_markup=kb_news_my_subs([]),
        )
        return
    items = await services.filter_news_with_llm(raw, target_coins=currencies)
    text = _format_news_message(items, currencies)
    coin = currencies[0] if currencies and currencies[0] != "ALL" else None
    if coin:
        await _send_or_edit(target, text, disable_web_page_preview=True,
                            reply_markup=kb_news_after(coin))
    else:
        await _send_or_edit(target, text, disable_web_page_preview=True,
                            reply_markup=kb_news_my_subs([]))


# ─────────────────────────── /oi (Open Interest) ────────────────
async def _send_oi(target: types.Message | types.CallbackQuery, symbol: str) -> None:
    """Показать Open Interest для монеты (через CoinGecko derivatives)."""
    sym = symbol.upper()
    if not sym.endswith("USDT"):
        sym += "USDT"
    async with services._shared_session() as s:
        result = await services.get_oi_change_24h(s, sym)
    if not result:
        await _send_or_edit(
            target,
            f"⚠️ Не удалось получить OI для `{sym}`.",
            reply_markup=kb_market(),
        )
        return

    coin = services.symbol_to_coin(sym)
    current = result["current_oi"]
    price = result.get("price")
    oi_usd = result.get("oi_usd")

    price_str = f"${price:,.2f}" if price else "—"
    oi_usd_str = services.fmt_volume(oi_usd) if oi_usd else "—"

    msg = (
        f"📊 *Open Interest — {coin}*\n\n"
        f"Текущий OI: `{current:,.0f} {coin}`\n"
        f"OI в USD: `{oi_usd_str}`\n"
        f"Цена: `{price_str}`\n\n"
        f"_💡 Трактовка:_\n"
        f"• Высокий OI = больше позиций открыто\n"
        f"• Рост OI + рост цены = сильный бычий тренд\n"
        f"• Рост OI + падение цены = медвежье давление\n"
        f"• Падение OI = позиции закрываются"
    )

    await _send_or_edit(target, msg, reply_markup=kb_market())


async def _send_oi_top(target: types.Message | types.CallbackQuery) -> None:
    """Показать OI топ-3 для кнопки меню."""
    async with services._shared_session() as s:
        results = await asyncio.gather(
            *[services.get_oi_change_24h(s, sym)
              for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]],
            return_exceptions=True,
        )
    valid = [r for r in results if r and not isinstance(r, Exception)]
    if not valid:
        await _send_or_edit(
            target,
            "⚠️ Не удалось получить OI.",
            reply_markup=kb_market(),
        )
        return
    lines = ["📊 *Open Interest (топ-3):*\n"]
    coins = ["BTC", "ETH", "SOL"]
    for i, r in enumerate(valid):
        if i >= len(coins):
            break
        coin = coins[i]
        oi_usd = r.get("oi_usd")
        oi_str = services.fmt_volume(oi_usd) if oi_usd else "—"
        lines.append(
            f"• *{coin}*: `{oi_str}`"
        )
    lines.append("\n_Конкретная монета: `/oi BTC` или `/oi ETH`_")
    await _send_or_edit(target, "\n".join(lines), reply_markup=kb_market())


@router.message(Command("oi"))
async def cmd_oi(m: types.Message, command: CommandObject) -> None:
    if not command.args:
        await _send_oi_top(m)
        return
    await _send_oi(m, command.args.strip())


# ─────────────────────────── /top ───────────────────────────────
async def _send_top(target: types.Message | types.CallbackQuery) -> None:
    async with services._shared_session() as s:
        data = await services.get_all_market_tickers(s)
    if not data:
        await _send_or_edit(target, "⚠️ Не удалось получить данные из OKX/CoinGecko.", reply_markup=kb_market())
        return
    # фильтруем USDT-пары с достаточным объёмом
    rows = []
    for t in data:
        if not t["symbol"].endswith("USDT"):
            continue
        try:
            rows.append((
                services.symbol_to_coin(t["symbol"]),
                float(t["lastPrice"]),
                float(t["priceChangePercent"]),
                float(t.get("quoteVolume", 0)),
            ))
        except (KeyError, TypeError, ValueError):
            continue
    rows = [r for r in rows if r[3] >= 1_000_000]  # > $1M объём
    rows.sort(key=lambda r: r[2], reverse=True)
    gainers = rows[:5]
    losers = rows[-5:][::-1]
    lines = ["🏆 *Топ-5 роста за 24ч:*"]
    for coin, price, ch, _vol in gainers:
        lines.append(f"  • *{coin}*: {services.fmt_usd(price)} {services.fmt_pct(ch)}")
    lines.append("\n📉 *Топ-5 падения за 24ч:*")
    for coin, price, ch, _vol in losers:
        lines.append(f"  • *{coin}*: {services.fmt_usd(price)} {services.fmt_pct(ch)}")
    await _send_or_edit(target, "\n".join(lines), reply_markup=kb_market())


@router.message(Command("top"))
async def cmd_top(m: types.Message) -> None:
    await _send_top(m)


# ─────────────────────────── /convert ───────────────────────────
@router.message(Command("convert"))
async def cmd_convert(m: types.Message, command: CommandObject) -> None:
    if not command.args:
        await m.answer(
            "Формат: `/convert 1 BTC USD`\n"
            "Поддерживаются тикеры Binance (BTC, ETH, TON, ...) и фиат USD, EUR, RUB."
        )
        return
    try:
        amount, frm, to = command.args.upper().split()
    except ValueError:
        await m.answer("Формат: `/convert 1 BTC USD`")
        return
    try:
        amount_f = float(amount)
    except ValueError:
        await m.answer("Сумма должна быть числом.")
        return

    async with services._shared_session() as s:
        # крипта → USDT через Binance
        def _pair(sym: str) -> str:
            return sym + "USDT" if not sym.endswith("USDT") else sym

        async def price_usdt(sym: str) -> float | None:
            p = await services.get_price(s, _pair(sym))
            return p

        prices = await asyncio.gather(
            price_usdt(frm) if frm != "USD" else _zero(),
            price_usdt(to) if to not in ("USD", "EUR", "RUB") else _zero(),
        )
        frm_usd, to_usd = prices

        # фиатные курсы к USD
        fiat_usd = {"USD": 1.0, "EUR": None, "RUB": None}
        if to in ("EUR", "RUB") or frm in ("EUR", "RUB"):
            data = await services._get_json(
                s,
                "https://api.exchangerate-api.com/v4/latest/USD",
            )
            if data and "rates" in data:
                r = data["rates"]
                fiat_usd["EUR"] = r.get("EUR")
                fiat_usd["RUB"] = r.get("RUB")

        # пересчёт
        if frm == "USD":
            frm_to_usd = 1.0
        elif frm in fiat_usd:
            if not fiat_usd[frm]:
                await m.answer(f"⚠️ Не удалось получить курс {frm}/USD.")
                return
            frm_to_usd = 1.0 / fiat_usd[frm]
        elif frm_usd:
            frm_to_usd = frm_usd
        else:
            await m.answer(f"⚠️ Не нашёл цену `{frm}`.")
            return

        if to == "USD":
            to_to_usd = 1.0
        elif to in fiat_usd:
            if not fiat_usd[to]:
                await m.answer(f"⚠️ Не удалось получить курс {to}/USD.")
                return
            to_to_usd = 1.0 / fiat_usd[to]
        elif to_usd:
            to_to_usd = to_usd
        else:
            await m.answer(f"⚠️ Не нашёл цену `{to}`.")
            return

        result = amount_f * frm_to_usd / to_to_usd

    await m.answer(
        f"💱 *{amount_f} {frm} = {result:,.6f} {to}*\n"
        f"_1 {frm} = {frm_to_usd:,.4f} USD; 1 {to} = {to_to_usd:,.4f} USD_",
        reply_markup=kb_market(),
    )


async def _zero() -> float:
    return 1.0


# ─────────────────────────── подписки на новости ─────────────────
@router.message(Command("subscribe"))
async def cmd_subscribe(m: types.Message, command: CommandObject) -> None:
    if not command.args:
        await m.answer("Формат: `/subscribe BTC,ETH,SOL`")
        return
    coins = [c.strip().upper() for c in re.split(r"[,\s]+", command.args) if c.strip()]
    if not coins:
        await m.answer("Не указаны монеты.")
        return
    added = await db.add_subs(m.from_user.id, coins)
    subs = await db.get_user_subs(m.from_user.id)
    await m.answer(
        f"✅ Подписки обновлены. Добавлено новых: *{added}*.\n"
        f"Бот будет присылать важные новости по этим монетам.",
        reply_markup=kb_subs_current(subs),
    )


@router.message(Command("unsubscribe"))
async def cmd_unsubscribe(m: types.Message, command: CommandObject) -> None:
    if not command.args:
        await m.answer("Формат: `/unsubscribe BTC` (или `BTC,ETH`)")
        return
    coins = [c.strip().upper() for c in re.split(r"[,\s]+", command.args) if c.strip()]
    removed = await db.remove_subs(m.from_user.id, coins)
    subs = await db.get_user_subs(m.from_user.id)
    await m.answer(f"🗑 Удалено подписок: *{removed}*.", reply_markup=kb_subs_current(subs))


async def _show_subs(target: types.Message | types.CallbackQuery) -> None:
    user_id = target.from_user.id if hasattr(target, "from_user") else target.chat.id
    subs = await db.get_user_subs(user_id)
    if not subs:
        await _send_or_edit(
            target,
            "Нет подписок. Добавь: `/subscribe BTC,ETH`",
            reply_markup=kb_subs_current([]),
        )
        return
    await _send_or_edit(
        target,
        "🔔 *Твои подписки:*\n" + ", ".join(f"`{s}`" for s in subs),
        reply_markup=kb_subs_current(subs),
    )


@router.message(Command("mysubs"))
async def cmd_mysubs(m: types.Message) -> None:
    await _show_subs(m)


# ─────────────────────────── утренняя сводка ────────────────────
@router.message(Command("morning"))
async def cmd_morning(m: types.Message, command: CommandObject) -> None:
    if not command.args:
        await m.answer(
            "Включить/выключить утреннюю сводку:\n"
            "`/morning on` или `/morning off`"
        )
        return
    arg = command.args.strip().lower()
    if arg in ("on", "1", "yes", "вкл"):
        await db.set_morning(m.from_user.id, True)
        await m.answer(f"🌅 Утренняя сводка *включена* ({config.MORNING_TIME} MSK).",
                       reply_markup=kb_market())
    elif arg in ("off", "0", "no", "выкл"):
        await db.set_morning(m.from_user.id, False)
        await m.answer("🌅 Утренняя сводка *выключена*.", reply_markup=kb_market())
    else:
        await m.answer("Аргумент: `on` или `off`.", reply_markup=kb_market())


async def _send_briefing(target: types.Message | types.CallbackQuery) -> None:
    await _send_or_edit(target, "⏳ Собираю сводку...")
    text = await services.build_morning_briefing()
    await _send_or_edit(target, text, disable_web_page_preview=True, reply_markup=kb_market())


@router.message(Command("briefing"))
async def cmd_briefing(m: types.Message) -> None:
    await _send_briefing(m)


# ─────────────────────────── /calendar ──────────────────────────
async def _send_calendar(target: types.Message | types.CallbackQuery) -> None:
    await _send_or_edit(target, "⏳ Загружаю календарь...")
    text = await services.build_econ_calendar_text(days=2)
    await _send_or_edit(target, text, reply_markup=kb_calendar(), disable_web_page_preview=True)


@router.message(Command("calendar"))
async def cmd_calendar(m: types.Message) -> None:
    await _send_calendar(m)


# ─────────────────────────── /liq ─────────────────────────────────
async def _send_liq(target: types.Message | types.CallbackQuery) -> None:
    await _send_or_edit(target, "⏳ Считаю ликвидации...")
    text = await services.build_liquidations_text(hours=24)
    await _send_or_edit(target, text, reply_markup=kb_liq())


@router.message(Command("liq"))
async def cmd_liq(m: types.Message) -> None:
    await _send_liq(m)


# ─────────────────────────── периодические задачи ───────────────
async def morning_job() -> None:
    log.info("Morning briefing job started")
    try:
        text = await services.build_morning_briefing()
        users = await db.all_morning_users()
        for uid in users:
            try:
                await bot.send_message(uid, text, disable_web_page_preview=True)
            except Exception as e:
                log.warning("Failed to send briefing to %s: %s", uid, e)
        log.info("Morning briefing sent to %s users", len(users))
    except Exception as e:
        log.exception("Morning job error: %s", e)


async def check_news_job() -> None:
    """Каждые 5 минут проверяет RSS на НОВЫЕ новости, фильтрует LLM,
    шлёт подписчикам. LLM вызывается ТОЛЬКО если есть новые URL
    (экономит ~70% расходов)."""
    subs_by_coin = await db.all_subs_by_coin()
    if not subs_by_coin:
        return
    target_coins = list(subs_by_coin.keys())

    # 1. Скачиваем RSS
    raw = await services.fetch_rss_feeds(lookback_hours=config.NEWS_LOOKBACK_HOURS)
    if not raw:
        log.info("News job: no RSS items fetched")
        return

    # 2. Фильтруем только НОВЫЕ (URL которых ещё нет в БД)
    new_items: list[dict] = []
    for it in raw[: config.NEWS_MAX_PER_LLM]:
        if not await db.is_news_sent(it["url"]):
            new_items.append(it)

    # 3. Если новых нет — выходим без вызова LLM
    if not new_items:
        log.info("News job: no new items, skipping LLM (saved API calls)")
        return

    log.info("News job: %s new items, calling LLM", len(new_items))

    # 4. Помечаем все новые URL как seen ДО LLM. Это страховка от дублей,
    #    если бот упадёт пока LLM отвечает. Повторно эти URL уже не возьмём.
    for it in new_items:
        await db.mark_news_sent(it["url"])

    # 5. LLM фильтрует ТОЛЬКО новые (не все 30, а только новые)
    items = await services.filter_news_with_llm(
        new_items, target_coins=target_coins
    )
    if not items:
        log.info("News job: LLM found nothing significant in %s new items",
                 len(new_items))
        return

    # 6. Рассылаем подписчикам
    sent_count = 0
    for it in items:
        url = it["url"]
        target_users: set[int] = set()
        for c in it.get("coins", []):
            target_users.update(subs_by_coin.get(c, []))
        if not it.get("coins"):
            for users in subs_by_coin.values():
                target_users.update(users)
        if not target_users:
            continue
        title_orig = _md(it.get("title", ""))
        title_ru = _md((it.get("title_ru") or "").strip()) or title_orig
        coins = [c for c in it.get("coins", []) if isinstance(c, str)]
        coin_part = f" `[{_md(' '.join(coins[:4]))}]`" if coins else ""
        fire = "🔥 " if it.get("importance") == "high" else ""
        reason = _md((it.get("reason") or "").strip())
        # Перевод на русский если есть
        if title_ru and title_orig and title_ru.lower() != title_orig.lower():
            title_part = f"*{title_ru}*\n  _{title_orig}_"
        else:
            title_part = f"*{title_orig}*"
        reason_part = f"\n_{reason}_" if reason else ""
        source = _md(it.get("source") or "source")
        text = (
            f"📰 {fire}{title_part}{coin_part}{reason_part}\n"
            f"[↗ {source}]({url})"
        )
        for uid in target_users:
            try:
                await bot.send_message(
                    uid, text, disable_web_page_preview=True
                )
                sent_count += 1
            except Exception as e:
                log.warning("Failed to send news to %s: %s", uid, e)
    log.info("News job: sent %s messages for %s significant items "
             "(of %s new)",
             sent_count, len(items), len(new_items))


async def econ_calendar_job() -> None:
    """Алерт за 60 минут до high-impact US macro-событий.
    Работает и без Finnhub: если ключа нет, используется бесплатный MQL5 fallback.
    """
    try:
        async with services._shared_session() as s:
            events = await services.get_upcoming_macro_events(
                s, within_minutes=config.ECON_NOTIFY_MINUTES
            )
        if not events:
            return
        users = await db.all_morning_users()
        if not users:
            return
        for e in events:
            event_id = services._event_id(e)
            if await db.is_econ_event_sent(event_id, "60min"):
                continue
            await db.mark_econ_event_sent(event_id, "60min")
            text = (
                f"📅 *Важное событие через {config.ECON_NOTIFY_MINUTES} мин*\n\n"
                f"{services._fmt_event(e)}\n\n"
                f"_Будьте внимательны к волатильности._"
            )
            for uid in users:
                try:
                    await bot.send_message(uid, text, disable_web_page_preview=True)
                except Exception as ex:
                    log.warning("Failed to send econ alert to %s: %s", uid, ex)
        log.info("Econ calendar job: %s events, %s users", len(events), len(users))
    except Exception as e:
        log.exception("Econ calendar job error: %s", e)


async def setup_scheduler() -> AsyncIOScheduler:
    global _scheduler
    sched = AsyncIOScheduler(timezone=config.TZ)
    # Время из БД (если меняли через /settime), иначе из .env
    saved = await db.get_setting("morning_time") or config.MORNING_TIME
    hh, mm = saved.split(":")
    sched.add_job(
        morning_job,
        CronTrigger(hour=int(hh), minute=int(mm), timezone=config.TZ),
        id="morning",
        replace_existing=True,
    )
    sched.add_job(
        check_news_job,
        IntervalTrigger(minutes=5),
        id="news",
        replace_existing=True,
    )
    sched.add_job(
        econ_calendar_job,
        IntervalTrigger(minutes=10),
        id="econ_calendar",
        replace_existing=True,
    )
    return sched


# ─────────────────────────── /settime ────────────────────────────
@router.message(Command("settime"))
async def cmd_settime(m: types.Message, command: CommandObject) -> None:
    """Показать текущее время или изменить: /settime HH:MM"""
    current = await db.get_setting("morning_time") or config.MORNING_TIME
    if not command.args:
        await _send_or_edit(
            m,
            f"⏰ *Текущее время утренней сводки:* `{current}` MSK\n\n"
            "Изменить: `/settime HH:MM` (например `/settime 08:30`)\n"
            "Или нажми кнопку ниже:",
            reply_markup=kb_settime(current),
        )
        return
    try:
        parts = command.args.strip().split(":")
        if len(parts) != 2:
            raise ValueError
        hh, mm = int(parts[0]), int(parts[1])
        if not (0 <= hh < 24 and 0 <= mm < 60):
            raise ValueError
    except ValueError:
        await _send_or_edit(
            m,
            "❌ Неверный формат. Используй HH:MM, например `/settime 08:30`",
            reply_markup=kb_settime(current),
        )
        return
    new_time = f"{hh:02d}:{mm:02d}"
    await db.set_setting("morning_time", new_time)
    # Reschedule без рестарта (remove + add — надёжнее reschedule_job)
    if _scheduler:
        try:
            _scheduler.remove_job("morning")
            _scheduler.add_job(
                morning_job,
                CronTrigger(hour=hh, minute=mm, timezone=config.TZ),
                id="morning",
                replace_existing=True,
            )
            log.info("Morning job rescheduled to %s", new_time)
        except Exception as e:
            log.warning("reschedule failed: %s", e)
    await _send_or_edit(
        m,
        f"✅ Утренняя сводка теперь в *{new_time}* MSK.\n"
        f"_Изменения применены сразу, без рестарта._",
        reply_markup=kb_market(),
    )


@router.callback_query(F.data.startswith("settime:"))
async def cb_settime(c: types.CallbackQuery) -> None:
    """Быстрый выбор времени кнопкой."""
    time_str = c.data.split(":", 1)[1]
    try:
        hh, mm = map(int, time_str.split(":"))
    except ValueError:
        await c.answer("Неверный формат")
        return
    await db.set_setting("morning_time", f"{hh:02d}:{mm:02d}")
    if _scheduler:
        try:
            _scheduler.remove_job("morning")
            _scheduler.add_job(
                morning_job,
                CronTrigger(hour=hh, minute=mm, timezone=config.TZ),
                id="morning",
                replace_existing=True,
            )
        except Exception as e:
            log.warning("reschedule failed: %s", e)
    await c.answer(f"✅ Утренняя сводка теперь в {hh:02d}:{mm:02d} MSK")
    await _send_or_edit(
        c,
        f"✅ Утренняя сводка теперь в *{hh:02d}:{mm:02d}* MSK.\n"
        f"_Изменения применены сразу._"
    )


async def healthz(request: web.Request) -> web.Response:
    """Простой health-check endpoint чтобы Render Web Service не засыпал."""
    return web.Response(text="ok")


async def start_health_server() -> web.AppRunner | None:
    import os
    port = os.getenv("PORT", "8000")
    app = web.Application()
    app.router.add_get("/healthz", healthz)
    runner = web.AppRunner(app)
    try:
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(port))
        await site.start()
        log.info("Health server started on port %s", port)
        return runner
    except Exception as e:
        log.warning("Health server failed: %s (non-fatal)", e)
        return None


# ─────────────────────────── News callbacks ────────────────────
@router.callback_query(F.data.startswith("news:"))
async def cb_news_coin(c: types.CallbackQuery, state: FSMContext) -> None:
    """Обработка кнопок новостей: news:BTC, news:ETH, news:ALL, news:custom."""
    await c.answer()
    coin = c.data.split(":", 1)[1].upper()
    if coin == "CUSTOM":
        await state.set_state(NewsCustomCoin.waiting_coin)
        await _send_or_edit(
            c,
            "✏️ *Введи тикер монеты* (например `BTC`, `ETH`, `SOL`):",
            reply_markup=cancel_kb(),
        )
        return
    # ALL — все новости без фильтра
    if coin == "ALL":
        await _show_news(c, [], lookback_hours=24)
        return
    await _show_news(c, [coin], lookback_hours=24)


@router.message(NewsCustomCoin.waiting_coin)
async def news_custom_coin(m: types.Message, state: FSMContext) -> None:
    await state.clear()
    coin = m.text.strip().upper()
    if not coin or not coin.isalpha():
        await m.answer("❌ Не похоже на тикер. Попробуй ещё раз, например `BTC`")
        return
    await _show_news(m, [coin], lookback_hours=24)


# ─────────────────────────── Subscribe callbacks ────────────────
@router.callback_query(F.data.startswith("sub:"))
async def cb_sub_coin(c: types.CallbackQuery, state: FSMContext) -> None:
    """Кнопки подписки: sub:BTC, sub:ETH, sub:SOL, sub:custom."""
    coin = c.data.split(":", 1)[1].upper()
    if coin == "CUSTOM":
        await state.set_state(SubCustomCoin.waiting_coin)
        await c.answer()
        await _send_or_edit(
            c,
            "✏️ *Введи тикер монеты* для подписки:",
            reply_markup=cancel_kb(),
        )
        return
    added = await db.add_subs(c.from_user.id, [coin])
    subs = await db.get_user_subs(c.from_user.id)
    await c.answer(f"✅ Подписка на {coin}")
    await _send_or_edit(
        c,
        f"✅ Подписка на *{coin}* активна.\n"
        f"Всего подписок: {len(subs)}.",
        reply_markup=kb_subs_current(subs),
    )


@router.message(SubCustomCoin.waiting_coin)
async def sub_custom_coin(m: types.Message, state: FSMContext) -> None:
    await state.clear()
    coin = m.text.strip().upper()
    if not coin or not coin.isalpha():
        await m.answer("❌ Не похоже на тикер. Попробуй ещё раз:")
        return
    await db.add_subs(m.from_user.id, [coin])
    subs = await db.get_user_subs(m.from_user.id)
    await m.answer(
        f"✅ Подписка на *{coin}* активна.\n"
        f"Всего подписок: {len(subs)}.",
        reply_markup=kb_subs_current(subs),
    )


@router.callback_query(F.data.startswith("unsub:"))
async def cb_unsub_coin(c: types.CallbackQuery) -> None:
    """Кнопка отписки."""
    coin = c.data.split(":", 1)[1].upper()
    await db.remove_subs(c.from_user.id, [coin])
    subs = await db.get_user_subs(c.from_user.id)
    await c.answer(f"🗑 Отписался от {coin}")
    await _send_or_edit(
        c,
        f"🗑 Отписался от *{coin}*.\nОсталось подписок: {len(subs)}",
        reply_markup=kb_subs_current(subs),
    )


# ─────────────────────────── Калькулятор позиции ────────────────
@router.message(Command("calc"))
async def cmd_calc(m: types.Message, state: FSMContext) -> None:
    await state.set_state(RiskCalc.deposit)
    await _send_or_edit(
        m,
        "📊 *Калькулятор позиции*\n\n"
        "Риск фиксирован — *1%* от депозита.\n"
        "Введи *размер депозита в USD*:",
        reply_markup=cancel_kb(),
    )


@router.message(RiskCalc.deposit)
async def risk_deposit(m: types.Message, state: FSMContext) -> None:
    try:
        v = float(m.text.replace(",", ".").replace(" ", ""))
        if v <= 0:
            raise ValueError
    except ValueError:
        await m.answer("❌ Введи положительное число, например `1000`:")
        return
    await state.update_data(deposit=v)
    await state.set_state(RiskCalc.entry)
    await m.answer("Введи *цену входа*:\n_(например `64270`)_")


@router.message(RiskCalc.entry)
async def risk_entry(m: types.Message, state: FSMContext) -> None:
    try:
        v = float(m.text.replace(",", ".").replace(" ", ""))
        if v <= 0:
            raise ValueError
    except ValueError:
        await m.answer("❌ Введи положительное число:")
        return
    await state.update_data(entry=v)
    await state.set_state(RiskCalc.stop)
    await m.answer("Введи *цену стоп-лосса*:\n_(где закрываешь убыточную сделку)_")


@router.message(RiskCalc.stop)
async def risk_stop(m: types.Message, state: FSMContext) -> None:
    try:
        stop = float(m.text.replace(",", ".").replace(" ", ""))
        if stop <= 0:
            raise ValueError
    except ValueError:
        await m.answer("❌ Введи положительное число:")
        return
    data = await state.get_data()
    entry = data["entry"]
    if abs(stop - entry) < 1e-9:
        await m.answer("❌ Стоп не может равняться входу. Попробуй ещё:")
        return
    await state.clear()

    deposit = data["deposit"]
    risk_pct = 1.0  # фиксированный риск
    risk_usd = deposit * (risk_pct / 100)

    stop_pct = abs(entry - stop) / entry * 100
    position_value = risk_usd / (stop_pct / 100) if stop_pct > 0 else 0
    qty = position_value / entry if entry > 0 else 0

    # Bybit futures taker ~0.06% (открытие + закрытие)
    fee = 0.06
    fee_amount = position_value * (fee / 100) * 2

    msg = (
        f"📊 *Калькулятор позиции*\n\n"
        f"📥 *Вводные:*\n"
        f"Депозит: `{deposit:,.2f} USD`\n"
        f"Риск: `{risk_pct}%` = `{risk_usd:,.2f} USD`\n"
        f"Вход: `{entry:,.2f}`\n"
        f"Стоп: `{stop:,.2f}` ({stop_pct:.2f}% от входа)\n\n"
        f"💼 *Расчёт:*\n"
        f"Объём позиции: `{position_value:,.2f} USD`\n"
        f"Размер в монетах: `{qty:.6f}`\n"
        f"Комиссия Bybit (~{fee}% × 2): `{fee_amount:,.2f} USD`\n\n"
        f"_Расчёт приблизительный. Фиксирован 1% риска от депозита._"
    )
    await m.answer(msg, reply_markup=kb_market())


async def on_startup() -> None:
    global _scheduler, _health_runner
    log.info("Bot starting...")
    await db.init_db()
    _scheduler = await setup_scheduler()
    _scheduler.start()
    log.info(
        "Scheduler started: morning at %s MSK, news every 5min",
        config.MORNING_TIME,
    )
    # Health-сервер (для Render Web Service)
    _health_runner = await start_health_server()
    me = await bot.get_me()
    log.info("Bot ready: @%s (%s)", me.username, me.full_name)


async def main() -> None:
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        if _health_runner:
            await _health_runner.cleanup()
        await db.close_db()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped")
        sys.exit(0)
