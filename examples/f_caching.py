import uxs
from uxs import fetch
import asyncio

"""
All caching works through uxs.poll
"""


async def main():
    # fetches ticker no more older than 1H. If not found in the cache
    # then fetches via ccxt, and caches the result.
    ticker = await fetch("binance", ("ticker", "BTC/USDT"), 3600)

    markets = await fetch("poloniex", "markets", 1800)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
