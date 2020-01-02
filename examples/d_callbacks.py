import uxs
import asyncio

xs = None

def ticker_update_callback(update):
    # The update is always formatted as follows:
    #   {
    #     "_": "ticker",
    #     "symbol": symbol,
    #     "data": {... ticker values that changed},
    #   }
    print(update)


def ob_update_callback(update):
    # The update is always formatted as follows:
    #   {
    #     "_": "orderbook",
    #     "symbol": symbol,
    #     "data": {
    #         "symbol": symbol
    #         "bids": [..bid_updates..],
    #         "asks": [..ask_updates..],
    #         "nonce": (previous_nonce, current_nonce),
    #     },
    #   }
    print(update)


async def test_callback(cb, channel, symbol):
    xs.subscribe_to({'_': channel, 'symbol': symbol})
    
    await xs.wait_till_subscription_active((channel,symbol))
    print("'{} {}' activated".format(channel, symbol))
    
    xs.add_callback(cb, channel, symbol)
    
    await asyncio.sleep(5)
    
    xs.remove_callback(cb, channel, symbol)
    xs.unsubscribe_to({'_': channel, 'symbol': symbol})


async def main():
    global xs
    xs = uxs.get_socket('kucoin')#, {'connection_defaults': {'handle': {lambda x: print(x)}}})
    
    xs.start()
    
    # TICKER
    #await test_callback(ticker_update_callback, 'ticker', 'BTC/USDT')
    
    # ORDERBOOK
    await test_callback(ob_update_callback, 'orderbook', 'XRP/BTC')
    
    await xs.stop()


if __name__ == '__main__':
    from fons.log import quick_logging
    quick_logging()
    asyncio.get_event_loop().run_until_complete(main())