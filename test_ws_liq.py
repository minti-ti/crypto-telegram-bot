"""Тест WebSocket ликвидаций Binance."""
import asyncio
import json
import websockets


async def main():
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    print(f"Connecting to {url}...")
    try:
        async with websockets.connect(url) as ws:
            print("Connected. Waiting for liquidation events...")
            count = 0
            while count < 5:
                msg = await asyncio.wait_for(ws.recv(), timeout=30)
                data = json.loads(msg)
                print(json.dumps(data, indent=2))
                count += 1
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
