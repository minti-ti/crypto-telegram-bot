"""Inline-кнопки для бота: главное меню + контекстные панели для каждой команды."""
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


# ─────────────────────────── главное меню ───────────────────────
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💰 Цена", callback_data="cmd:price"),
            InlineKeyboardButton(text="😱 F&G", callback_data="cmd:fg"),
        ],
        [
            InlineKeyboardButton(text="💸 Funding", callback_data="cmd:funding"),
            InlineKeyboardButton(text="🏆 Топ", callback_data="cmd:top"),
        ],
        [
            InlineKeyboardButton(text="📰 Новости", callback_data="cmd:news"),
            InlineKeyboardButton(text="🚨 Алерты", callback_data="cmd:alerts"),
        ],
        [
            InlineKeyboardButton(text="🔔 Подписки", callback_data="cmd:subs"),
            InlineKeyboardButton(text="⚙️ Время", callback_data="cmd:settime"),
        ],
        [InlineKeyboardButton(text="📋 Сводка", callback_data="cmd:briefing")],
    ])


# ─────────────────────────── контекстные панели ─────────────────
def kb_price(symbol: str) -> InlineKeyboardMarkup:
    """Панель для /price <symbol>."""
    coin = _symbol_to_coin(symbol)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"📈 Funding {coin}", callback_data="cmd:funding"),
            InlineKeyboardButton(text=f"📰 Новости {coin}", callback_data=f"news:{coin}"),
        ],
        [
            InlineKeyboardButton(text="🚨 Создать алерт", callback_data="alert:start"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


def kb_market() -> InlineKeyboardMarkup:
    """Панель для /fg, /funding, /top, /briefing.
    📰 Новости — большая сверху. Funding — компактная как другие."""
    return InlineKeyboardMarkup(inline_keyboard=[
        # Новости — на всю ширину
        [InlineKeyboardButton(text="📰 Новости", callback_data="cmd:news")],
        # Рыночные команды равные
        [
            InlineKeyboardButton(text="💰 Цена", callback_data="cmd:price"),
            InlineKeyboardButton(text="😱 F&G", callback_data="cmd:fg"),
            InlineKeyboardButton(text="💸 Funding", callback_data="cmd:funding"),
        ],
        [InlineKeyboardButton(text="🏆 Топ", callback_data="cmd:top")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


def kb_news(coins: list[str] | None = None) -> InlineKeyboardMarkup:
    """Панель для /news, /subscribe, /mysubs."""
    if coins:
        first_row = [
            InlineKeyboardButton(
                text=f"🔔 Подписаться на {c}", callback_data=f"sub:{c}"
            )
            for c in coins[:3]
        ]
        rows = []
        if first_row:
            rows.append(first_row)
        rows.append([
            InlineKeyboardButton(text="🔔 Мои подписки", callback_data="cmd:subs"),
            InlineKeyboardButton(text="💰 Цена", callback_data="cmd:price"),
        ])
        rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")])
        return InlineKeyboardMarkup(inline_keyboard=rows)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔔 Подписки", callback_data="cmd:subs"),
            InlineKeyboardButton(text="💰 Цена", callback_data="cmd:price"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


def kb_alerts() -> InlineKeyboardMarkup:
    """Панель для /alerts, /delalert."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Создать алерт", callback_data="alert:start"),
            InlineKeyboardButton(text="💰 Цена", callback_data="cmd:price"),
        ],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="cmd:menu")],
    ])


def kb_settime(current: str) -> InlineKeyboardMarkup:
    """Панель для /settime — кнопки быстрого выбора."""
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


# ─────────────────────────── утилиты ────────────────────────────
def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])


def _symbol_to_coin(symbol: str) -> str:
    s = symbol.upper()
    for suf in ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH"):
        if s.endswith(suf) and s != suf:
            return s[: -len(suf)]
    return s
