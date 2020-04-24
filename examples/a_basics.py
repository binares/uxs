import uxs
import asyncio


async def main():
    xs = uxs.get_streamer('binance')
    
    xs.subscribe_to_all_tickers()
    xs.subscribe_to_orderbook('BTC/USDT')
    
    xs.start()
    
    await xs.wait_till_subscription_active(('all_tickers',))
    print("'all_tickers' activated")
    await xs.wait_till_subscription_active(('orderbook','BTC/USDT'))
    print("'orderbook BTC/USDT' activated")
   
    print("ETH/BTC ticker: {}".format(xs.tickers['ETH/BTC']))
    print("5 top BTC/USDT bid levels: {}".format(xs.orderbooks['BTC/USDT']['bids'][:5]))
    
    xs.unsubscribe_to_all_tickers()
    xs.unsubscribe_to_orderbook('BTC/USDT')
    
    await xs.stop()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())