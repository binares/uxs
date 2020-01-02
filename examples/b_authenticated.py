import uxs
import asyncio
import sys


async def main(apiKey=None, secret=None):
    if apiKey is None:
        apiKey = sys.argv[1]
    if secret is None:
        secret = sys.argv[2]
    
    xs = uxs.get_socket('binance', {'apiKey': apiKey, 'secret': secret})
    
    query = ['balance','order','fill','position']
    has = [x for x in query if xs.has_got('account', x)]
    print("binance account stream has: {}".format(', '.join(has)))
    
    # Subscribes to balance, order, fill and position updates (if it has them)
    xs.subscribe_to_account()
    xs.start()
    
    await xs.wait_till_subscription_active(('account',))
    
    print("My USDT balance: {}".format(xs.balances['USDT']['total']))
    
    await asyncio.sleep(0.5)
    xs.unsubscribe_to_account()
    
    await xs.stop()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())