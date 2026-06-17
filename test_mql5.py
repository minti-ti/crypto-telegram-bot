"""Тест MQL5 economic calendar."""
import asyncio
import aiohttp
import re
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

    print(f"HTML length: {len(text)}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    to_date = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")
    print(f"\nLooking for dates between {today} and {to_date}")

    pattern = re.compile(
        r'<div class="ec-table__item ec-table__item_inline">'
        r'(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}),\s+([A-Z]{3}),\s+(.*?)</div>',
        re.S,
    )
    matches = list(pattern.finditer(text))
    print(f"Total event matches found: {len(matches)}")

    us_count = 0
    for m in matches[:30]:
        date_str, time_str, currency, rest = m.groups()
        name_match = re.search(r'<a[^>]*>(.*?)</a>', rest)
        event_name = name_match.group(1) if name_match else rest
        event_name = re.sub(r'<[^>]+>', '', event_name).strip()
        if currency == "USD" and today <= date_str.replace(".", "-") <= to_date:
            us_count += 1
            print(f"{date_str} {time_str} {currency} {event_name}")

    print(f"\nUS events in window: {us_count}")


if __name__ == "__main__":
    asyncio.run(main())
