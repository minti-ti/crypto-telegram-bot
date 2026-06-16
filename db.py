"""PostgreSQL-хранилище через asyncpg: пользователи, алерты, подписки, settings."""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import asyncpg

import config

log = logging.getLogger(__name__)

# Глобальный пул соединений (создаётся в init_db)
_pool: Optional[asyncpg.Pool] = None


# ─────────────────────────── инициализация ──────────────────────
async def init_db() -> None:
    """Создать пул и таблицы."""
    global _pool
    if _pool is not None:
        return
    if not config.DATABASE_URL:
        raise RuntimeError(
            "Не задана DATABASE_URL в .env. Получи на https://neon.tech"
        )
    _pool = await asyncpg.create_pool(
        dsn=config.DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )
    log.info("PostgreSQL pool created")
    async with _pool.acquire() as conn:
        await _create_tables(conn)


async def _create_tables(conn: asyncpg.Connection) -> None:
    """Создать все таблицы если их нет."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     BIGINT PRIMARY KEY,
            created_at  BIGINT NOT NULL,
            morning_on  SMALLINT NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id          BIGSERIAL PRIMARY KEY,
            user_id     BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            symbol      TEXT    NOT NULL,
            direction   TEXT    NOT NULL,
            price       DOUBLE PRECISION NOT NULL,
            created_at  BIGINT  NOT NULL,
            triggered   SMALLINT NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(triggered, symbol);

        CREATE TABLE IF NOT EXISTS news_subs (
            user_id  BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            coin     TEXT    NOT NULL,
            PRIMARY KEY (user_id, coin)
        );

        CREATE TABLE IF NOT EXISTS sent_news (
            news_id  TEXT PRIMARY KEY,
            sent_at  BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS settings (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at BIGINT NOT NULL
        );
    """)


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ─────────────────────────── helpers ────────────────────────────
def _now() -> int:
    return int(time.time())


async def _fetchone(sql: str, *args: Any) -> asyncpg.Record | None:
    assert _pool is not None
    async with _pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)


async def _fetchall(sql: str, *args: Any) -> list[asyncpg.Record]:
    assert _pool is not None
    async with _pool.acquire() as conn:
        return list(await conn.fetch(sql, *args))


async def _execute(sql: str, *args: Any) -> str:
    assert _pool is not None
    async with _pool.acquire() as conn:
        return await conn.execute(sql, *args)


async def _fetchval(sql: str, *args: Any) -> Any:
    assert _pool is not None
    async with _pool.acquire() as conn:
        return await conn.fetchval(sql, *args)


# ─────────────────────────── Users ──────────────────────────────
async def upsert_user(user_id: int) -> None:
    await _execute(
        """INSERT INTO users(user_id, created_at) VALUES ($1, $2)
           ON CONFLICT (user_id) DO NOTHING""",
        user_id, _now(),
    )


async def all_morning_users() -> list[int]:
    rows = await _fetchall(
        "SELECT user_id FROM users WHERE morning_on = 1"
    )
    return [r["user_id"] for r in rows]


async def toggle_morning(user_id: int) -> bool:
    """Возвращает новое значение morning_on."""
    await upsert_user(user_id)
    row = await _fetchone(
        "SELECT morning_on FROM users WHERE user_id = $1", user_id
    )
    new_val = 0 if row and row["morning_on"] else 1
    await _execute(
        "UPDATE users SET morning_on = $1 WHERE user_id = $2",
        new_val, user_id,
    )
    return bool(new_val)


async def set_morning(user_id: int, value: bool) -> None:
    """Принудительно установить значение."""
    await upsert_user(user_id)
    await _execute(
        "UPDATE users SET morning_on = $1 WHERE user_id = $2",
        1 if value else 0, user_id,
    )


# ─────────────────────────── Alerts ─────────────────────────────
async def add_alert(user_id: int, symbol: str, direction: str, price: float) -> int:
    await upsert_user(user_id)
    new_id = await _fetchval(
        """INSERT INTO alerts(user_id, symbol, direction, price, created_at)
           VALUES ($1, $2, $3, $4, $5) RETURNING id""",
        user_id, symbol.upper(), direction, price, _now(),
    )
    return int(new_id)


async def list_user_alerts(user_id: int) -> list[asyncpg.Record]:
    return await _fetchall(
        """SELECT id, symbol, direction, price, triggered, created_at
           FROM alerts WHERE user_id = $1
           ORDER BY triggered ASC, created_at DESC""",
        user_id,
    )


async def delete_alert(user_id: int, alert_id: int) -> bool:
    status = await _execute(
        "DELETE FROM alerts WHERE id = $1 AND user_id = $2",
        alert_id, user_id,
    )
    # asyncpg возвращает "DELETE N"
    return status.endswith(" 1")


async def active_alerts() -> list[asyncpg.Record]:
    return await _fetchall(
        """SELECT id, user_id, symbol, direction, price
           FROM alerts WHERE triggered = 0"""
    )


async def mark_triggered(alert_id: int) -> None:
    await _execute(
        "UPDATE alerts SET triggered = 1 WHERE id = $1", alert_id,
    )


# ─────────────────────────── News subscriptions ─────────────────
async def add_subs(user_id: int, coins: list[str]) -> int:
    await upsert_user(user_id)
    added = 0
    assert _pool is not None
    async with _pool.acquire() as conn:
        for c in coins:
            status = await conn.execute(
                """INSERT INTO news_subs(user_id, coin)
                   VALUES ($1, $2)
                   ON CONFLICT DO NOTHING""",
                user_id, c.upper(),
            )
            # "INSERT 0 1" если добавлено, "INSERT 0 0" если нет
            if status.endswith(" 1"):
                added += 1
    return added


async def remove_subs(user_id: int, coins: list[str]) -> int:
    removed = 0
    assert _pool is not None
    async with _pool.acquire() as conn:
        for c in coins:
            status = await conn.execute(
                "DELETE FROM news_subs WHERE user_id = $1 AND coin = $2",
                user_id, c.upper(),
            )
            if status.endswith(" 1"):
                removed += 1
    return removed


async def get_user_subs(user_id: int) -> list[str]:
    rows = await _fetchall(
        "SELECT coin FROM news_subs WHERE user_id = $1", user_id
    )
    return [r["coin"] for r in rows]


async def all_subs_by_coin() -> dict[str, list[int]]:
    """Возвращает { 'BTC': [user_id, ...], ... }."""
    rows = await _fetchall("SELECT user_id, coin FROM news_subs")
    out: dict[str, list[int]] = {}
    for r in rows:
        out.setdefault(r["coin"], []).append(r["user_id"])
    return out


async def is_news_sent(news_id: str) -> bool:
    row = await _fetchone(
        "SELECT 1 FROM sent_news WHERE news_id = $1", news_id
    )
    return row is not None


async def mark_news_sent(news_id: str) -> None:
    await _execute(
        """INSERT INTO sent_news(news_id, sent_at) VALUES ($1, $2)
           ON CONFLICT DO NOTHING""",
        news_id, _now(),
    )


# ─────────────────────────── Settings ───────────────────────────
async def get_setting(key: str, default: str | None = None) -> str | None:
    row = await _fetchone(
        "SELECT value FROM settings WHERE key = $1", key
    )
    return row["value"] if row else default


async def set_setting(key: str, value: str) -> None:
    await _execute(
        """INSERT INTO settings(key, value, updated_at)
           VALUES ($1, $2, $3)
           ON CONFLICT (key) DO UPDATE SET
               value = EXCLUDED.value,
               updated_at = EXCLUDED.updated_at""",
        key, value, _now(),
    )
