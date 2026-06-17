import asyncio
import aiohttp


async def main():
    url = "https://www.mql5.com/en/economic-calendar"
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession() as s:
        async with s.get(url, headers=headers) as r:
            text = await r.text()
    print(f"Status: {r.status}")
    print(f"Length: {len(text)}")
    # Find interesting snippets
    for keyword in ["CPI", "Retail Sales", "FOMC", "GDP", "2026.06.17"]:
        idx = text.find(keyword)
        if idx != -1:
            print(f"\n--- {keyword} ---")
            print(text[max(0, idx-200):idx+200])


if __name__ == "__main__":
    asyncio.run(main())
