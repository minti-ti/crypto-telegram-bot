"""Точка входа: хендлеры, планировщик, проверка алертов и новостей."""
from __future__ import annotations

import asyncio
import logging
import re
import sys
from datetime import datetime, timezone
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router, types
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
    kb_price, kb_market, kb_news, kb_alerts, kb_settime,
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


# ─────────────────────────── FSM ───────────────────────────────
class AddAlert(StatesGroup):
    waiting_symbol = State()
    waiting_direction = State()
    waiting_price = State()


class ConvertState(StatesGroup):
    waiting_input = State()


# ─────────────────────────── /start и меню ─────────────────────
HELP_TEXT = (
    "🤖 *Crypto-бот*\n\n"
    "*Основные команды:*\n"
    "`/price <тикер>` — цена (напр. `/price BTC`)\n"
    "`/fg` — Fear & Greed Index\n"
    "`/funding` — funding rate BTC/ETH/SOL\n"
    "`/news [BTC,ETH,...]` — важные новости по монетам\n"
    "`/top` — топ рост/падение за 24ч\n"
    "`/convert` — конвертер криптовалют\n\n"
    "*Алерты:*\n"
    "`/alert BTCUSDT above 70000` — сработает при пробое 70000\n"
    "`/alerts` — список\n"
    "`/delalert <id>` — удалить\n\n"
    "*Подписки на новости:*\n"
    "`/subscribe BTC,ETH,SOL` — присылать важные новости\n"
    "`/unsubscribe BTC`\n"
    "`/mysubs`\n\n"
    "*Прочее:*\n"
    "`/morning on|off` — утренняя сводка\n"
    "`/briefing` — прислать сводку сейчас\n"
    "`/help` — эта справка"
)


@router.message(Command("start"))
async def cmd_start(m: types.Message) -> None:
    db.upsert_user(m.from_user.id)
    await m.answer(
        f"Привет, *{m.from_user.first_name or 'друг'}*! 👋\n\n"
        "Я — твой персональный крипто-ассистент.\n"
        "Каждое утро в *" + config.MORNING_TIME + "* (Europe/Moscow) присылаю сводку рынка.\n"
        "Нажми кнопку или набери /help.",
        reply_markup=main_menu_kb(),
    )


@router.message(Command("help"))
async def cmd_help(m: types.Message) -> None:
    await m.answer(HELP_TEXT, reply_markup=kb_market())


# ─────────────────────────── callback'и от меню ─────────────────
@router.callback_query(F.data.startswith("cmd:"))
async def cb_menu(c: types.CallbackQuery) -> None:
    cmd = c.data.split(":", 1)[1]
    await c.answer()
    # Простые команды — вызываем соответствующий хендлер
    if cmd == "fg":
        await _send_fg(c.message)
        return
    if cmd == "funding":
        await _send_funding(c.message)
        return
    if cmd == "alerts":
        await cmd_alerts(c.message)
        return
    if cmd == "subs":
        await cmd_mysubs(c.message)
        return
    if cmd == "top":
        await cmd_top(c.message)
        return
    if cmd == "briefing":
        await cmd_briefing(c.message)
        return
    if cmd == "menu":
        await cmd_start(c.message)
        return
    if cmd == "news":
        await c.message.answer(
            "Введи монеты через запятую, например: `BTC,ETH`, "
            "или просто `/news` для важных по рынку.",
            reply_markup=kb_market(),
        )
        return
    if cmd == "price":
        await c.message.answer(
            "Укажи тикер. Примеры:\n"
            "• `/price BTC`\n"
            "• `/price BTCUSDT`",
            reply_markup=kb_market(),
        )
        return
    if cmd == "morning":
        new_val = db.toggle_morning(c.from_user.id)
        await c.message.answer(
            f"🌅 Утренняя сводка: {'включена ✅' if new_val else 'выключена ❌'}",
            reply_markup=kb_market(),
        )
        return


@router.callback_query(F.data == "cancel")
async def cb_cancel(c: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await c.answer("Отменено")
    await c.message.edit_text("❌ Отменено.")


# ─────────────────────────── /price ─────────────────────────────
@router.message(Command("price"))
async def cmd_price(m: types.Message, command: CommandObject) -> None:
    if not command.args:
        await m.answer(
            "Укажи тикер. Примеры:\n"
            "• `/price BTC`\n"
            "• `/price BTCUSDT`\n"
            "• `/price eth`"
        )
        return
    sym = command.args.strip().upper().replace("/", "")
    if not sym.endswith("USDT") and sym not in ("BTCUSDT", "ETHUSDT"):
        # попробуем добавить USDT
        sym = sym + "USDT"
    async with services._shared_session() as s:
        ticker = await services._get_json(
            s,
            "https://api.binance.com/api/v3/ticker/24hr",
            {"symbol": sym},
        )
    if not ticker:
        await m.answer(f"❌ Не нашёл `{sym}` на Binance. Проверь тикер.")
        return
    try:
        price = float(ticker["lastPrice"])
        ch = float(ticker["priceChangePercent"])
        hi = float(ticker["highPrice"])
        lo = float(ticker["lowPrice"])
        vol = float(ticker.get("quoteVolume", 0))
    except (KeyError, TypeError, ValueError):
        await m.answer("⚠️ Не удалось распарсить ответ биржи.")
        return
    coin = services.symbol_to_coin(sym)
    await m.answer(
        f"💰 *{coin}*\n"
        f"Цена: *{services.fmt_usd(price, 2)}*\n"
        f"24ч: {services.fmt_pct(ch)}\n"
        f"High: {services.fmt_usd(hi)} · Low: {services.fmt_usd(lo)}\n"
        f"Объём: {services.fmt_volume(vol)}",
        reply_markup=kb_price(sym),
    )


# ─────────────────────────── /fg ────────────────────────────────
async def _send_fg(m: types.Message) -> None:
    async with services._shared_session() as s:
        fg = await services.get_fear_greed(s)
    if not fg:
        await m.answer("⚠️ Не удалось получить Fear & Greed Index.", reply_markup=kb_market())
        return
    await m.answer(
        f"😱 *Fear & Greed Index*\n"
        f"Значение: *{fg['value']}* — {services.fear_greed_emoji(fg['value'])}\n"
        f"_Обновлено: {datetime.fromtimestamp(fg['timestamp'], tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}_",
        reply_markup=kb_market(),
    )


@router.message(Command("fg"))
async def cmd_fg(m: types.Message) -> None:
    await _send_fg(m)


# ─────────────────────────── /funding ──────────────────────────
async def _send_funding(m: types.Message) -> None:
    async with services._shared_session() as s:
        results = await asyncio.gather(
            *[services.get_funding_rate(s, x) for x in config.FUNDING_SYMBOLS]
        )
    valid = [r for r in results if r]
    if not valid:
        await m.answer("⚠️ Не удалось получить funding rate.", reply_markup=kb_market())
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
    await m.answer("\n".join(lines), reply_markup=kb_market())


@router.message(Command("funding"))
async def cmd_funding(m: types.Message) -> None:
    await _send_funding(m)





# ─────────────────────────── /news ──────────────────────────────
def _format_news_message(items: list[dict], coins_filter: list[str] | None = None) -> str:
    """Форматирует список отфильтрованных новостей в одно сообщение.
    Использует title_ru (перевод) если есть, иначе оригинальный заголовок."""
    if not items:
        return (
            "📰 Свежих важных новостей нет"
            + (f" по `{', '.join(coins_filter)}`" if coins_filter else "")
            + "."
        )
    header = "📰 *Важные новости"
    if coins_filter:
        header += f" ({', '.join(coins_filter)})"
    header += ":*\n"
    lines = [header]
    for it in items:
        title_ru = (it.get("title_ru") or "").strip()
        title_orig = it["title"].replace("*", "").replace("`", "")
        if title_ru and title_ru.lower() != title_orig.lower():
            title_part = f"{title_ru}*\n  _{title_orig}_"
        else:
            title_part = f"{title_orig}*"
        coins = it.get("coins", [])
        coin_part = f" `[{' '.join(coins[:4])}]`" if coins else ""
        fire = "🔥 " if it.get("importance") == "high" else ""
        reason = it.get("reason", "").strip()
        reason_part = f"\n  _{reason}_" if reason else ""
        source = it.get("source") or "source"
        lines.append(
            f"• {fire}{title_part}{coin_part}{reason_part}\n"
            f"  [↗ {source}]({it['url']})"
        )
    return "\n\n".join(lines)


@router.message(Command("news"))
async def cmd_news(m: types.Message, command: CommandObject) -> None:
    currencies: list[str] | None = None
    if command.args:
        currencies = [
            c.strip().upper() for c in re.split(r"[,\s]+", command.args) if c.strip()
        ]
    await m.answer("⏳ Собираю новости...")
    raw = await services.fetch_rss_feeds(lookback_hours=24)
    if not raw:
        await m.answer(
            "⚠️ Не удалось получить новости из RSS. Проверь интернет.",
            reply_markup=kb_news(currencies),
        )
        return
    items = await services.filter_news_with_llm(raw, target_coins=currencies)
    text = _format_news_message(items, currencies)
    await m.answer(text, disable_web_page_preview=True, reply_markup=kb_news(currencies))


# ─────────────────────────── /top ───────────────────────────────
@router.message(Command("top"))
async def cmd_top(m: types.Message) -> None:
    async with services._shared_session() as s:
        data = await services._get_json(
            s, "https://api.binance.com/api/v3/ticker/24hr"
        )
    if not data:
        await m.answer("⚠️ Не удалось получить данные.", reply_markup=kb_market())
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
    await m.answer("\n".join(lines), reply_markup=kb_market())


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


# ─────────────────────────── /alert ─────────────────────────────
@router.message(Command("alert"))
async def cmd_alert(m: types.Message, command: CommandObject) -> None:
    """
    Поддерживает:
      /alert BTCUSDT above 70000
      /alert ETH above 3000
      /alert (без аргументов → интерактивный режим)
    """
    if not command.args:
        await m.answer(
            "Формат:\n"
            "`/alert BTCUSDT above 70000`\n"
            "`/alert ETH below 3000`\n\n"
            "Или нажми кнопку ниже для пошагового создания.",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(
                    text="➕ Создать алерт (пошагово)",
                    callback_data="alert:start",
                )]
            ]),
        )
        return
    parts = command.args.upper().split()
    if len(parts) != 3:
        await m.answer(
            "❌ Неверный формат. Пример:\n"
            "`/alert BTCUSDT above 70000`"
        )
        return
    sym, direction, price_s = parts
    if direction not in ("ABOVE", "BELOW"):
        await m.answer("Направление должно быть `above` или `below`.")
        return
    try:
        price = float(price_s.replace(",", "."))
    except ValueError:
        await m.answer("Цена должна быть числом.")
        return
    if not sym.endswith("USDT"):
        sym = sym + "USDT"
    alert_id = db.add_alert(m.from_user.id, sym, direction.lower(), price)
    await m.answer(
        f"✅ Алерт создан (id *{alert_id}*):\n"
        f"`{services.symbol_to_coin(sym)}` {direction.lower()} {price:g}\n"
        f"Бот пришлёт сообщение, когда условие сработает.",
        reply_markup=kb_alerts(),
    )


@router.callback_query(F.data == "alert:start")
async def cb_alert_start(c: types.CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AddAlert.waiting_symbol)
    await c.message.answer(
        "Введи тикер монеты (например `BTC` или `BTCUSDT`):"
    )
    await c.answer()


@router.message(AddAlert.waiting_symbol)
async def alert_symbol(m: types.Message, state: FSMContext) -> None:
    sym = m.text.strip().upper().replace("/", "")
    if not sym.endswith("USDT"):
        sym += "USDT"
    await state.update_data(symbol=sym)
    await state.set_state(AddAlert.waiting_direction)
    await m.answer(
        f"Тикер: *{sym}*\n"
        "Теперь выбери направление:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="📈 Above (выше)", callback_data="alert:dir:above"
                ),
                types.InlineKeyboardButton(
                    text="📉 Below (ниже)", callback_data="alert:dir:below"
                ),
            ],
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")],
        ]),
    )


@router.callback_query(F.data.startswith("alert:dir:"))
async def cb_alert_dir(c: types.CallbackQuery, state: FSMContext) -> None:
    direction = c.data.split(":")[2]
    await state.update_data(direction=direction)
    await state.set_state(AddAlert.waiting_price)
    await c.message.answer("Введи целевую цену (число):")
    await c.answer()


@router.message(AddAlert.waiting_price)
async def alert_price(m: types.Message, state: FSMContext) -> None:
    try:
        price = float(m.text.replace(",", "."))
    except ValueError:
        await m.answer("❌ Это не число. Попробуй ещё раз:")
        return
    data = await state.get_data()
    alert_id = db.add_alert(
        m.from_user.id, data["symbol"], data["direction"], price
    )
    await state.clear()
    await m.answer(
        f"✅ Алерт создан (id *{alert_id}*):\n"
        f"`{services.symbol_to_coin(data['symbol'])}` "
        f"{data['direction']} {price:g}",
        reply_markup=kb_alerts(),
    )


@router.message(Command("alerts"))
async def cmd_alerts(m: types.Message) -> None:
    rows = db.list_user_alerts(m.from_user.id)
    if not rows:
        await m.answer("У тебя нет алертов. Создай: `/alert BTCUSDT above 70000`",
                       reply_markup=kb_alerts())
        return
    lines = ["🚨 *Твои алерты:*"]
    for r in rows:
        status = "✅ сработал" if r["triggered"] else "⏳ активен"
        coin = services.symbol_to_coin(r["symbol"])
        lines.append(
            f"• *#{r['id']}* `{coin}` {r['direction']} {r['price']:g} — {status}"
        )
    lines.append("\nУдалить: `/delalert <id>`")
    await m.answer("\n".join(lines), reply_markup=kb_alerts())


@router.message(Command("delalert"))
async def cmd_delalert(m: types.Message, command: CommandObject) -> None:
    if not command.args or not command.args.isdigit():
        await m.answer("Формат: `/delalert 5`")
        return
    if db.delete_alert(m.from_user.id, int(command.args)):
        await m.answer(f"🗑 Алерт #{command.args} удалён.", reply_markup=kb_alerts())
    else:
        await m.answer("❌ Алерт не найден или не принадлежит тебе.", reply_markup=kb_alerts())


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
    added = db.add_subs(m.from_user.id, coins)
    await m.answer(
        f"✅ Подписки обновлены. Добавлено новых: *{added}*.\n"
        f"Бот будет присылать важные новости по этим монетам.",
        reply_markup=kb_news(coins),
    )


@router.message(Command("unsubscribe"))
async def cmd_unsubscribe(m: types.Message, command: CommandObject) -> None:
    if not command.args:
        await m.answer("Формат: `/unsubscribe BTC` (или `BTC,ETH`)")
        return
    coins = [c.strip().upper() for c in re.split(r"[,\s]+", command.args) if c.strip()]
    removed = db.remove_subs(m.from_user.id, coins)
    await m.answer(f"🗑 Удалено подписок: *{removed}*.", reply_markup=kb_news())


@router.message(Command("mysubs"))
async def cmd_mysubs(m: types.Message) -> None:
    subs = db.get_user_subs(m.from_user.id)
    if not subs:
        await m.answer("Нет подписок. Добавь: `/subscribe BTC,ETH`",
                       reply_markup=kb_news())
        return
    await m.answer(
        "🔔 *Твои подписки:*\n" + ", ".join(f"`{s}`" for s in subs),
        reply_markup=kb_news(),
    )


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


@router.message(Command("briefing"))
async def cmd_briefing(m: types.Message) -> None:
    await m.answer("⏳ Собираю сводку...")
    text = await services.build_morning_briefing()
    await m.answer(text, disable_web_page_preview=True, reply_markup=kb_market())


# ─────────────────────────── периодические задачи ───────────────
async def morning_job() -> None:
    log.info("Morning briefing job started")
    try:
        text = await services.build_morning_briefing()
        users = db.all_morning_users()
        for uid in users:
            try:
                await bot.send_message(uid, text, disable_web_page_preview=True)
            except Exception as e:
                log.warning("Failed to send briefing to %s: %s", uid, e)
        log.info("Morning briefing sent to %s users", len(users))
    except Exception as e:
        log.exception("Morning job error: %s", e)


async def check_alerts_job() -> None:
    """Проверяет активные алерты каждые 30 секунд."""
    alerts = db.active_alerts()
    if not alerts:
        return
    # группируем по символам, чтобы делать 1 запрос на символ
    by_symbol: dict[str, list] = {}
    for a in alerts:
        by_symbol.setdefault(a["symbol"], []).append(a)

    async with services._shared_session() as s:
        for sym, items in by_symbol.items():
            price = await services.get_price(s, sym)
            if price is None:
                continue
            for a in items:
                hit = (
                    (a["direction"] == "above" and price >= a["price"]) or
                    (a["direction"] == "below" and price <= a["price"])
                )
                if not hit:
                    continue
                db.mark_triggered(a["id"])
                coin = services.symbol_to_coin(sym)
                arrow = "📈" if a["direction"] == "above" else "📉"
                text = (
                    f"{arrow} *АЛЕРТ #{a['id']}*\n"
                    f"`{coin}` достиг(ла) *{services.fmt_usd(price)}*\n"
                    f"Условие: {a['direction']} {a['price']:g}"
                )
                try:
                    await bot.send_message(a["user_id"], text)
                    log.info("Alert %s triggered for user %s", a["id"], a["user_id"])
                except Exception as e:
                    log.warning("Failed to send alert: %s", e)


async def check_news_job() -> None:
    """Каждые 5 минут собирает RSS, фильтрует LLM, шлёт подписчикам."""
    subs_by_coin = db.all_subs_by_coin()
    if not subs_by_coin:
        return
    target_coins = list(subs_by_coin.keys())

    raw = await services.fetch_rss_feeds(lookback_hours=24)
    if not raw:
        log.info("News job: no RSS items fetched")
        return
    items = await services.filter_news_with_llm(raw, target_coins=target_coins)
    if not items:
        log.info("News job: nothing significant after filter")
        return

    sent_count = 0
    for it in items:
        url = it["url"]
        if db.is_news_sent(url):
            continue
        # для каждой монеты в новости ищем подписчиков
        target_users: set[int] = set()
        for c in it.get("coins", []):
            target_users.update(subs_by_coin.get(c, []))
        # если монеты не указаны — шлём всем подписчикам (общая новость)
        if not it.get("coins"):
            for users in subs_by_coin.values():
                target_users.update(users)
        if not target_users:
            continue
        title = it["title"].replace("*", "").replace("`", "")
        coins = it.get("coins", [])
        coin_part = f" `[{' '.join(coins[:4])}]`" if coins else ""
        fire = "🔥 " if it.get("importance") == "high" else ""
        reason = it.get("reason", "").strip()
        reason_part = f"\n_{reason}_" if reason else ""
        source = it.get("source") or "source"
        text = (
            f"📰 {fire}*{title}*{coin_part}{reason_part}\n"
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
        db.mark_news_sent(url)
    if sent_count:
        log.info("News job: sent %s messages for %s items",
                 sent_count, len(items))


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
        check_alerts_job,
        IntervalTrigger(seconds=30),
        id="alerts",
        replace_existing=True,
    )
    sched.add_job(
        check_news_job,
        IntervalTrigger(minutes=5),
        id="news",
        replace_existing=True,
    )
    return sched


# ─────────────────────────── main ───────────────────────────────
# ─────────────────────────── /settime ────────────────────────────
@router.message(Command("settime"))
async def cmd_settime(m: types.Message, command: CommandObject) -> None:
    """Показать текущее время или изменить: /settime HH:MM"""
    current = db.get_setting("morning_time") or config.MORNING_TIME
    if not command.args:
        await m.answer(
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
        await m.answer(
            "❌ Неверный формат. Используй HH:MM, например `/settime 08:30`",
            reply_markup=kb_settime(current),
        )
        return
    new_time = f"{hh:02d}:{mm:02d}"
    db.set_setting("morning_time", new_time)
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
    await m.answer(
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
    db.set_setting("morning_time", f"{hh:02d}:{mm:02d}")
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
    await c.message.edit_text(
        f"✅ Утренняя сводка теперь в *{hh:02d}:{mm:02d}* MSK.\n"
        f"_Изменения применены сразу._"
    )


@router.callback_query(F.data.startswith("news:"))
async def cb_news_for_coin(c: types.CallbackQuery) -> None:
    """Новости по конкретной монете (из кнопки kb_price)."""
    coin = c.data.split(":", 1)[1].upper()
    await c.answer()
    await c.message.answer(f"⏳ Ищу новости по {coin}...")
    # Имитируем /news
    raw = await services.fetch_rss_feeds(lookback_hours=24)
    if not raw:
        await c.message.answer("⚠️ Не удалось получить RSS.", reply_markup=kb_market())
        return
    items = await services.filter_news_with_llm(raw, target_coins=[coin])
    text = _format_news_message(items, [coin])
    await c.message.answer(text, disable_web_page_preview=True,
                           reply_markup=kb_news([coin]))


@router.callback_query(F.data.startswith("sub:"))
async def cb_sub_for_coin(c: types.CallbackQuery) -> None:
    """Подписаться на монету из кнопки kb_news."""
    coin = c.data.split(":", 1)[1].upper()
    added = db.add_subs(c.from_user.id, [coin])
    await c.answer(f"✅ Подписка на {coin}: добавлено {added}")
    await c.message.answer(
        f"✅ Подписка на *{coin}* активна.\n"
        f"Бот будет присылать важные новости.",
        reply_markup=kb_news(),
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


async def on_startup() -> None:
    global _scheduler, _health_runner
    log.info("Bot starting...")
    await db.init_db()
    _scheduler = await setup_scheduler()
    _scheduler.start()
    log.info(
        "Scheduler started: morning at %s MSK, alerts every 30s, news every 5min",
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
