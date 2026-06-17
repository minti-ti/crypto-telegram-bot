"""Тест MQL5 economic calendar."""
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta


async def main():
    url = "https://www.mql5.com/en/economic-calendar"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(url, headers=headers) as r:
            print(f"Status: {r.status}")
            text = await r.text()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    to_date = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")
    print(f"\nLooking for dates between {today} and {to_date}")

    # Показываем строки, похожие на события
    import re
    lines = text.splitlines()
    found = 0
    for line in lines:
        line = line.strip()
        if re.match(r"\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2},\s+[A-Z]{3},", line):
            print(line)
            found += 1
            if found > 30:
                break

    print(f"\nTotal event-like lines found: {found}")


if __name__ == "__main__":
    asyncio.run(main())
