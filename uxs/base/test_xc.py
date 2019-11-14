import asyncio
import functools
import time
import sys

DEFAULT_EXCHANGE = 'bittrex'
DEFAULT_DISPLAY = ['ob']

import uxs
from fons.aio import call_via_loop_afut, lrc
from fons.argv import parse_argv
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

oid = None

async def _unsub(params, delay=0, merge=False):
    await asyncio.sleep(delay)
    print('unsubscribing {}'.format(params))
    if merge: 
        symbols = params['symbol']
        s = xs.get_subscription(dict(params, symbol=symbols[0]))
        xs.unsubscribe_to(s.merger)
    else:
        xs.unsubscribe_to(params)
    
async def _resub(params, delay=0, merge=False):
    await asyncio.sleep(delay)
    print('resubscribing {}'.format(params))
    if 'symbol' in params: params['symbol'] = xs.merge(params['symbol'])
    xs.subscribe_to(params)
    
async def _crash(params, delay=0):
    await asyncio.sleep(delay)
    s = xs.get_subscription(params)
    cnx = s.cnx
    print('crashing {} : {} socket'.format(s, cnx.name))
    if not cnx.signalr:
        await call_via_loop_afut(cnx.conn.__aexit__, sys.exc_info(), loop=cnx.loop)
    else:
        cnx.conn.close()
            
async def _print_changed(type, attr=None, clear_first=False):
    if attr is None: attr = type
    if clear_first:
        [xs.events[type][x].clear() for x in xs.events[type]]
    await xs.events[type][-1].wait()
    were_set = [x for x,y in xs.events[type].items() if y.is_set()]
    print('(d){}: {}'.format(type,{x: getattr(xs,attr,{}).get(x) for x in were_set if x!=-1}))
    [xs.events[type][x].clear() for x in were_set]
        
        
async def fetch_tickers(symbols=(), sub=True, unsub=False, resub=False, merge=False):
    await asyncio.sleep(2)
    """try: print('tickers: {}'.format(await ws.fetch_tickers()))
    except Exception as e:
        logger2.exception(e)"""
    ch = 'all_tickers' if not symbols else 'ticker'
    if not sub: pass
    elif not symbols: xs.subscribe_to_all_tickers()
    else:
        if not merge:
            for symbol in symbols:
                xs.subscribe_to_ticker(symbol)
        else:
            xs.subscribe_to_ticker(symbols)
    
    _symbols = [None] if ch == 'all_tickers' else ([symbols] if merge else symbols)
    for symbol in _symbols:
        params = {'_': ch, 'symbol': symbol} if ch == 'ticker' else {'_': ch}
        if sub and unsub: 
            asyncio.ensure_future(_unsub(params, unsub, merge))
        if sub and resub:
            asyncio.ensure_future(_resub(params, resub, merge))
            
    while True:
        await _print_changed('ticker','tickers',clear_first=True)
        
    
async def fetch_order_book(symbols, _print='changes', sub=True, unsub=False, resub=False, merge=False):
    await asyncio.sleep(2)
    """try: print('orderbook {}: {}'.format(symbols[0], await xs.fetch_order_book(symbols[0])))
    except Exception as e:
        logger2.exception(e)"""
        
    if not sub: pass
    elif not merge:
        for symbol in symbols:
            xs.subscribe_to_orderbook(symbol)
    else: xs.subscribe_to_orderbook(symbols)
        
    def _print_ob_changes(changes, symbol=symbols[0]):
        print('(d)ob  {}: {}'.format(symbol, changes))
        
    def _print_last_n(symbol=symbols[0]):
        ob = xs.orderbooks.get(symbol,{})
        print('(d)ob {} bids[:{}]: {}'.format(symbol, _print, ob.get('bids',[])[:_print]))
        print('(d)ob {} asks[:{}]: {}'.format(symbol, _print, ob.get('asks',[])[:_print]))
    
    _symbols = symbols if not merge else [symbols]
    for symbol in _symbols:
        params = {'_': 'orderbook', 'symbol': symbol}
        if sub and unsub: asyncio.ensure_future(_unsub(params, unsub, merge))
        if sub and resub: asyncio.ensure_future(_resub(params, resub, merge))
        
    if _print == 'changes':
        xs.add_callback(_print_ob_changes, 'orderbook', symbols[0])
    else:
        while True:
            e = xs.events['orderbook'][symbols[0]]
            (await e.wait()), e.clear()
            _print_last_n()
    #while True:
        #await _print_changed('orderbook','orderbooks',clear_first=True)
    
async def show_trades():
    while True:
        await xs.events['trade'][-1].wait()
        xs.events['trade'][-1].clear()
        print('trades:',xs.trades)
        #await _print_changed('trade','trades',True)
    
async def show_orders():
    while True:
        #await ws.events['order'][-1].wait()
        #ws.events['order'][-1].clear()
        #print('orders:',ws.open_orders,ws.closed_orders)
        await _print_changed('order','orders',True)
            
async def show_balances():
    while True:
        await _print_changed('balance','balances')
        
async def place_order(*args):
    global oid
    symbol, amount, price = args[:3] if len(args) else ('ETH/BTC', 0.1, 0.01)
    side = 'buy' if len(args) < 4 else args[3]
    await asyncio.sleep(4)
    try: 
        r = await xs.create_limit_order(symbol, side, amount, price)
        print('r_place: ', r)
        oid = r['id']
    except Exception as e:
        logger2.exception(e)

async def cancel_order(*args):
    symbol = args[0] if len(args) else 'ETH/BTC'
    await asyncio.sleep(6)
    r = await xs.cancel_order(oid, symbol)
    print('r_cancel ', r)

async def _stop(wait_time):
    await asyncio.sleep(wait_time)
    xs.stop()
    
async def _restart(stop_wait_time):
    await asyncio.sleep(stop_wait_time+2)
    await xs.start()   
        
        
def main():
    global xs
    fons.log.quick_logging(3)
    
    try: xc = sys.argv.pop(1)
    except IndexError:
        xc = DEFAULT_EXCHANGE
    
    def _to_float(x):
        try: return float(x)
        except ValueError:
            return None
        
    def _split(x):
        return x.split(',')
    
    activities = ['u','unsub','r','resub','s','stop','c','crash']
    apply = dict.fromkeys(activities, _to_float)
    apply.update(dict.fromkeys(['d','display','ticker','all_tickers','ob'], _split))
    
    p = parse_argv(sys.argv[1:], apply)

    unsub = p.which(['u','unsub'], False)
    resub = p.which(['r','resub'], False)
    stop = p.which(['s','stop'], False)
    crash = p.which(['c','crash'], False)
    
    if unsub:
        unsub = p.get(unsub) if p.get(unsub) is not None else 4
    if resub:
        resub = unsub + p.get(resub) if p.get(resub) is not None else unsub + 2
    if stop:
        stop = max(unsub, resub) + p.get(stop) if p.get(stop) is not None else max(resub + 2, unsub + 2, 10)
    if crash:
        crash = max(unsub, resub) + p.get(crash) if p.get(crash) is not None else max(resub + 2, unsub + 2, 10)
    
    display = p.which(['d','display'], None)
    if display:
        display = p.get(display)
    else:
        display = DEFAULT_DISPLAY
    
    config = {
        'auth': 'TRADE',
        'name':'{}Ws'.format(xc.capitalize()), 
        'channels': {
            'orderbook': {'delete_data_on_unsub': False}},
        'ob': {'assert_integrity': True},
    }

    if 'responses' in display:
        config['connection_defaults'] = {'handle': lambda x: print(x)}
    
    try:
        xs = uxs.get_socket(xc, config)
    except ValueError as e:
        config['auth'] = 'NULL'
        xs = uxs.get_socket(xc, config)
    
    #print(uxs.ExchangeSocket.obm)
    #print(xs.__class__.obm)
    print(xs.sub_to_orderbook)
    print(xs.obm)
    print(xs.__class__.apiKey)
    print(xs.apiKey)
    print('has ("all_tickers"): {}'.format(xs.has_got('all_tickers')))
    print('has ("all_tickers","last"): {}'.format(xs.has_got('all_tickers','last')))
    print('has ("all_tickers",["last"]): {}'.format(xs.has_got('all_tickers',['last'])))
    print('has ("all_tickers",["last","notExistingKey"]): {}'.format(xs.has_got('all_tickers',['last','notExistingKey'])))
    print('has ("all_tickers",["last","high"]): {}'.format(xs.has_got('all_tickers',['last','high'])))

    coros = [xs.start()]
    
    t_param = p.which(['ticker','all_tickers'], '')
    ob_param = p.which(['ob'], '')
    o_param = next((x for x in p if x.startswith('order+') or x=='order'), '')
    
    if t_param:
        t_index = p.indexes[t_param][0][0]
        t_merge = len(p.argv) > t_index + 1 and (p.argv[t_index+1] == 'm')
        t_symbols = p.get(t_param, ())
        coros += [fetch_tickers(t_symbols, sub=True, unsub=unsub, resub=resub, merge=t_merge)]
        
    if ob_param:
        ob_index = p.indexes[ob_param][0][0]
        ob_merge = len(p.argv) > ob_index + 1 and (p.argv[ob_index+1] == 'm')
        ob_symbols = p.get(ob_param, ('ETH/BTC',))
        #ob_print = 'changes'
        ob_print = 4
        coros += [fetch_order_book(ob_symbols, ob_print, sub=True, unsub=unsub, resub=resub, merge=ob_merge)]
    
    if o_param:
        xs.subscribe_to_account()
        coros += [show_trades(), show_orders(), show_balances()] #show_books()
        if crash: coros += [_crash({'_': 'account'}, crash)]
        
    if o_param.startswith('order+'):
        o_rest = o_param[len('order+'):]
        cancel = o_rest.startswith('-')
        if cancel: o_rest = o_rest[1:]
        o_args = o_rest.split(',') if o_rest else []
        logger2.debug('Scheduling order: {}'.format(o_args))
        coros += [place_order(*o_args)]
        if cancel: coros += [cancel_order(*o_args)]
        
    if stop:
        coros += [_stop(stop), _restart(stop)]
        
    g = asyncio.gather(*coros)
    #asyncio.get_event_loop().run_forever()
    lrc(g)


if __name__ == '__main__':
    main()
        