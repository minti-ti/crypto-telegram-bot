"""Тест Finnhub economic calendar."""
import asyncio
from datetime import datetime, timezone
import aiohttp
from dotenv import load_dotenv

load_dotenv()

import config
import services


async def main():
    print(f"FINNHUB_API_KEY: {config.FINNHUB_API_KEY[:10]}..." if config.FINNHUB_API_KEY else "FINNHUB_API_KEY: NOT SET")
    print(f"ECON_MIN_IMPORTANCE: {config.ECON_MIN_IMPORTANCE}")
    print(f"ECON_COUNTRIES: {config.ECON_COUNTRIES}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    to_date = (datetime.now(timezone.utc) + __import__("datetime").timedelta(days=2)).strftime("%Y-%m-%d")
    print(f"\nRequest: {today} -> {to_date}")

    async with services._shared_session() as s:
        raw = await services.fetch_economic_calendar(s, today, to_date)

    print(f"\nRaw events count: {len(raw)}")
    if raw:
        print("\nFirst 5 raw events:")
        for e in raw[:5]:
            print(e)
    else:
        print("No raw events. Check API key / region / limits.")
        return

    filtered = services._filter_us_macro_events(raw)
    print(f"\nFiltered US events count: {len(filtered)}")
    for e in filtered[:10]:
        print(services._fmt_event(e))


if __name__ == "__main__":
    asyncio.run(main())
