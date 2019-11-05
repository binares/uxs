from uxs.base.ccxt import get_exchange, init_exchange, ccxtWrapper
from uxs.base import poll
import ccxt
import ccxt.async_support
import sys
import asyncio
import copy
import fons.log


def test():
    try: xc = sys.argv[1]
    except IndexError: xc = 'kucoin'
    class KC(ccxtWrapper,getattr(ccxt,xc)):
        pass
    kc = KC()
    #kc = get_exchange('kucoin') 
    markets,currencies,balances = asyncio.get_event_loop().run_until_complete(
        asyncio.gather(poll.get(xc,'markets'),poll.get(xc,'currencies'),
                       poll.get(xc,'balances')))
    kc.markets = markets[0].data
    kc.currencies = currencies[0].data
    kc.balance = balances[0].data
    kc.balance['free'].update({'USDT': 120, 'ETH': 1})
    kc.balance['USDT']['free'] = 120
    kc.balance['ETH']['free'] = 1
    original = kc.FEE_FROM_TARGET
    #kc.load_markets()
    print('----xxx---')
    for balance in (kc.balance, None):
        print('balance: {}'.format(None if balance is None else 
                                   ['ETH',balance['free'].get('ETH'),'USDT',balance['free'].get('USDT')] ))
        for quotation in ['base','quote']:
            print('quotation: {}'.format(quotation))
            for side in ['buy','sell']:
                print('side: {}'.format(side))
                for amount in (1, pow(10,-10), pow(10,12)):
                    print('amount: {}'.format(amount))
                    for price in (120, 99, pow(10,16)):
                        for FFT in (True,False):
                            kc.FEE_FROM_TARGET = FFT
                            oi = kc.convert_order_input('ETH/USDT', side, amount, price,
                                                        quotation=quotation, balance=balance, full=True)
                            print(oi)
                    print()
            print('-----------------')
        print('xxxxxxxxxxxxxxxxxxxx')
    kc.FEE_FROM_TARGET = True
    print(kc.calc_order_size_from_payout('ETH/USDT','buy',100,0.1))
    kc.FEE_FROM_TARGET = False
    print(kc.calc_order_size_from_payout('ETH/USDT','buy',100,0.1))
    kc.FEE_FROM_TARGET = original
    
    
def test_update_markets(api):
    mcopy = copy.deepcopy(api.markets)
    api.update_markets(
        {'__all__': {'precision': {'price': '--'}},
         'USD': {'precision': {'price': '--USD'}},
         'DGB/USD': {'precision': {'price': '--DGB/USD'}}
         })
    for m,v in api.markets.items():
        if m == 'DGB/USD':
            assert v['precision']['price'] == '--DGB/USD'
        elif v['quote'] == 'USD':
            assert v['precision']['price'] == '--USD'
        else:
            assert v['precision']['price'] == '--'
    api.markets = mcopy
    
    
def test_exchange_init():
    fons.log.quick_logging(0)
    e1 = init_exchange('hitbtc')
    e2 = get_exchange('hitbtc')
    assert e1 is e2
    
    e3 = get_exchange({'exchange':'hitbtc','withdraw':True})
    assert e3 is not e1
    e4 = get_exchange({'exchange':'hitbtc','withdraw':True})
    assert e4 is e3
    e5 = init_exchange({'exchange':'hitbtc','withdraw':True,'add':False})
    assert e5 is not e3
    e6 = get_exchange({'exchange':'hitbtc','withdraw':True})
    assert e6 is e3
    
    e7 = get_exchange({'exchange':'hitbtc','async':False})
    assert isinstance(e7, ccxt.Exchange) and not isinstance(e7, ccxt.async_support.Exchange)
    assert e7 is not e1
    e8 = get_exchange({'exchange':'hitbtc','async':False})
    assert e8 is e7
    
    e9 = get_exchange({'exchange':'poloniex','withdraw':True})
    assert e9 is not e3
    e10 = get_exchange({'exchange':e9})
    assert e10 is e9
    

async def async_main():
    api = get_exchange({'exchange': 'bittrex', 
                        'kwargs': {'load_markets': None, 'load_currencies': None}
                        })
    test_update_markets(api)
    market = 'DGB/USD'
    cys = market.split('/')
    market_btc = '{}/BTC'.format(cys[0])
    api.tickers = (await poll.get(api,'tickers',None))[0].data
    api.markets[market]['limits']['cost'] = {}
    cur_price = api.tickers[market]['last']
    
    print(market)
    print(api.markets[market]['limits'])
    print(api.tickers[market_btc]['last'],
          api.tickers[market]['last'],
          api.tickers['BTC/USD']['last'])
    
    buy_price = api.round_price(market,cur_price/2,'buy')
    limits = api.get_amount_limits(market,buy_price)
    print('buy_price:',buy_price,limits)

    #await api.create_limit_buy_order(market,limits['min'],buy_price)
    
    
def main():
    fons.log.quick_logging(2)
    asyncio.get_event_loop().run_until_complete(async_main())

if __name__ == '__main__':
    main()