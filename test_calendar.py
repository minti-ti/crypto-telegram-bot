"""Тест экономического календаря."""
import asyncio
import os
from datetime import datetime, timezone, timedelta
import aiohttp

# Заглушки для config
os.environ.setdefault("BOT_TOKEN", "123456:ABC")
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

import config
import services


async def main():
    print(f"FINNHUB_API_KEY: {config.FINNHUB_API_KEY[:10]}..." if config.FINNHUB_API_KEY else "FINNHUB_API_KEY: NOT SET")
    print(f"ECON_MIN_IMPORTANCE: {config.ECON_MIN_IMPORTANCE}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    to_date = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")
    print(f"\nRequest: {today} -> {to_date}")

    async with services._shared_session() as s:
        raw = await services.fetch_economic_calendar(s, today, to_date)

    print(f"\nRaw events count: {len(raw)}")
    if raw:
        print("\nFirst 5 raw events:")
        for e in raw[:5]:
            print(e)
    else:
        print("No raw events.")
        return

    filtered = services._filter_us_macro_events(raw)
    print(f"\nFiltered US events count: {len(filtered)}")
    for e in filtered[:10]:
        print(services._fmt_event(e))


if __name__ == "__main__":
    asyncio.run(main())
