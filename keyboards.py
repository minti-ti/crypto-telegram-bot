"""Inline-кнопки: главное меню + контекстные панели для каждой команды.
Все подменю — максимально на кнопках, минимум текста."""
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


# ─────────────────────────── главное меню ───────────────────────
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Калькулятор позиции", callback_data="cmd:calc"),
        ],
        [
            InlineKeyboardButton(text="📅 Календарь", callback_data="cmd:calendar"),
            InlineKeyboardButton(text="💥 Ликвидации", callback_data="cmd:liq"),
        ],
        [
            InlineKeyboardButton(text="📰 Новости", callback_data="cmd:news"),
            InlineKeyboardButton(text="💰 Цена", callback_data="cmd:price"),
        ],
        [
            InlineKeyboardButton(text="😱 F&G", callback_data="cmd:fg"),
            InlineKeyboardButton(text="💸 Funding", callback_data="cmd:funding"),
            InlineKeyboardButton(text="📊 OI", callback_data="cmd:oi"),
        ],
        [
            InlineKeyboardButton(text="🏆 Топ", callback_data="cmd:top"),
            InlineKeyboardButton(text="📋 Сводка", callback_data="cmd:briefing"),
        ],
        [
            InlineKeyboardButton(text="🔔 Подписки", callback_data="cmd:subs"),
        ],
        [
            InlineKeyboardButton(text="⚙️ Время сводки", callback_data="cmd:settime"),
        ],
    ])


# ─────────────────────────── /price ───────────────────────────────
PRICE_SYMBOLS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "TON"]


def kb_price_symbols() -> InlineKeyboardMarkup:
    """Выбор монеты для цены."""
    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(PRICE_SYMBOLS), 4):
        chunk = PRICE_SYMBOLS[i:i+4]
        rows.append([
            InlineKeyboardButton(text=c, callback_data=f"price:{c}")
            for c in chunk
        ])
    rows.append([InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="price:custom")])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_price(symbol: str) -> InlineKeyboardMarkup:
    coin = _symbol_to_coin(symbol)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"📈 Funding {coin}", callback_data="cmd:funding"),
            InlineKeyboardButton(text=f"📰 Новости {coin}", callback_data=f"news:{coin}"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


# ─────────────────────────── /fg /funding /top /briefing ─────────
def kb_market(cmd: str | None = None) -> InlineKeyboardMarkup:
    """Упрощённая клавиатура: только Обновить + Меню, без лишней навигации."""
    rows: list[list[InlineKeyboardButton]] = []
    if cmd:
        rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"cmd:{cmd}")])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────── /news ──────────────────────────────
def kb_news_my_subs(coins: list[str]) -> InlineKeyboardMarkup:
    """Панель новостей: твои подписки как кнопки + 'Другая монета'."""
    rows: list[list[InlineKeyboardButton]] = []
    if coins:
        # Кнопки с подписками — по 3 в ряд
        for i in range(0, len(coins), 3):
            chunk = coins[i:i+3]
            rows.append([
                InlineKeyboardButton(text=c, callback_data=f"news:{c}")
                for c in chunk
            ])
        rows.append([
            InlineKeyboardButton(text="📋 Все новости (без фильтра)", callback_data="news:ALL"),
        ])
    else:
        rows.append([
            InlineKeyboardButton(text="🔔 У меня нет подписок — добавить", callback_data="cmd:subs"),
        ])
    rows.append([
        InlineKeyboardButton(text="✏️ Другая монета", callback_data="news:custom"),
    ])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_news_after(coin: str) -> InlineKeyboardMarkup:
    """Панель ПОСЛЕ получения новостей по монете."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔔 Подписаться", callback_data=f"sub:{coin}"),
            InlineKeyboardButton(text="✏️ Другая монета", callback_data="news:custom"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


# ─────────────────────────── /subscribe /mysubs ──────────────────
def kb_subs_current(coins: list[str]) -> InlineKeyboardMarkup:
    """Управление подписками на новости."""
    rows: list[list[InlineKeyboardButton]] = []
    if coins:
        rows.append([InlineKeyboardButton(text="─── Твои подписки ───", callback_data="noop")])
        for c in coins:
            rows.append([
                InlineKeyboardButton(text=f"🗑 Отписаться от {c}", callback_data=f"unsub:{c}"),
            ])
    rows.append([
        InlineKeyboardButton(text="➕ Подписаться на BTC", callback_data="sub:BTC"),
        InlineKeyboardButton(text="➕ ETH", callback_data="sub:ETH"),
        InlineKeyboardButton(text="➕ SOL", callback_data="sub:SOL"),
    ])
    rows.append([
        InlineKeyboardButton(text="✏️ Другая монета", callback_data="sub:custom"),
    ])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────── /calendar ───────────────────────────
def kb_calendar() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="cmd:calendar"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


# ─────────────────────────── /liq ────────────────────────────────
def kb_liq() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="cmd:liq"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


# ─────────────────────────── /settime ────────────────────────────
def kb_settime(current: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⏰ 07:00", callback_data="settime:07:00"),
            InlineKeyboardButton(text="⏰ 08:00", callback_data="settime:08:00"),
            InlineKeyboardButton(text="⏰ 09:00", callback_data="settime:09:00"),
        ],
        [
            InlineKeyboardButton(text="⏰ 10:00", callback_data="settime:10:00"),
            InlineKeyboardButton(text="⏰ 12:00", callback_data="settime:12:00"),
            InlineKeyboardButton(text="⏰ 21:00", callback_data="settime:21:00"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


# ─────────────────────────── Калькулятор сделки ──────────────────
def kb_calc() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Лонг (Long)", callback_data="calc:long"),
            InlineKeyboardButton(text="🔴 Шорт (Short)", callback_data="calc:short"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


# ─────────────────────────── FSM helper ──────────────────────────
def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])


# ─────────────────────────── helpers ─────────────────────────────
def _symbol_to_coin(symbol: str) -> str:
    s = symbol.upper()
    for suf in ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH"):
        if s.endswith(suf) and s != suf:
            return s[: -len(suf)]
    return s
