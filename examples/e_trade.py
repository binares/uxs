import uxs
import asyncio

xs = None


def do_i_wanna_place_the_order():
    # Lowest ask and its volume
    ask, askVolume = xs.orderbooks['BTC/USDT']['asks'][0] 
    return ask <= 7210


async def main():
    global xs
    xs = uxs.binance({'apiKey': '', 'secret': ''})
    xs.subscribe_to_orderbook('BTC/USDT')
    # Subscribes to balance, trade, fill and position updates
    xs.subscribe_to_account()
    xs.start()

    my_initial_btc_balance = xs.balances['BTC']['total']
    
    while True:
        await xs.wait_on('orderbook','BTC/USDT')
        # Wait till BTC/USDT price has fallen below 7210, then place the limit order
        if do_i_wanna_place_the_order():
            r = await xs.create_order('BTC/USDT', 'limit', 'buy', 0.001, 7200)
            break
    
    # Wait until the order is filled
    # (technically it could also be canceled, remaining=0 just signifies that the order is closed)
    while xs.orders[r['id']]['remaining'] > 0:
        await xs.wait_on('order',r['id'])
    
    print('Order filled') 
    
    # Wait until the BTC balance has been accounted for
    while True:
        await xs.wait_on('balance','BTC')
        new_btc_balance = xs.balances['BTC']['total']
        delta = new_btc_balance - my_initial_btc_balance
        if delta >= 0.001:
            print('YEAY! I have received {} BTC!'.format(delta))
            break
    
    await xs.stop()
    

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())