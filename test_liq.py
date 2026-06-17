"""Тест ликвидаций Binance."""
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
import aiohttp

os.environ.setdefault("BOT_TOKEN", "123456:ABC")
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

import config
import services


async def main():
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 24 * 3600 * 1000

    print("Testing forceOrders per symbol...")
    async with services._shared_session() as s:
        for sym in config.LIQ_SYMBOLS:
            orders = await services.fetch_force_orders(
                s, symbol=sym, start_time_ms=start_ms, end_time_ms=now_ms, limit=1000
            )
            print(f"{sym}: {len(orders)} orders")
            if orders:
                total = sum(float(o.get("price", 0)) * float(o.get("executedQty", 0)) for o in orders)
                print(f"  total USD: {services.fmt_volume(total)}")

    print("\nTesting allForceOrders fallback...")
    async with services._shared_session() as s:
        all_orders = await services.fetch_all_force_orders(
            s, start_time_ms=start_ms, end_time_ms=now_ms, limit=1000
        )
    print(f"allForceOrders: {len(all_orders)} orders")
    if all_orders:
        totals = services.aggregate_liquidations(all_orders, config.LIQ_SYMBOLS, window_minutes=24*60)
        for sym, val in totals.items():
            print(f"  {sym}: {services.fmt_volume(val)}")

    print("\nTesting build_liquidations_text...")
    text = await services.build_liquidations_text(hours=24)
    print(text[:1500])


if __name__ == "__main__":
    asyncio.run(main())
