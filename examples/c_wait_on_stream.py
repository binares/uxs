import uxs
import asyncio


async def main():
    xs = uxs.get_socket('kucoin')
    
    xs.subscribe_to_ticker('BTC/USDT')
    
    xs.start()
    
    await xs.wait_till_subscription_active(('ticker','BTC/USDT'))
    print("'ticker BTC/USDT' activated")
    
    for _ in range(10):
        await xs.wait_on('ticker', 'BTC/USDT')
        print(xs.tickers['BTC/USDT'])
    
    xs.unsubscribe_to_ticker('BTC/USDT')
    
    await xs.stop()


asyncio.get_event_loop().run_until_complete(main())