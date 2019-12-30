"""
This module allows to test an exchange stream.
python -m uxs.test_xc <exchange> <stream1> <stream2> ... <param1>
Streams:
    ticker             : all_tickers
    ticker=<symbols>
    ob=<symbols>
    trades=<symbols>
    ohlcv=<symbols>
    account            [for bitmex account=<symbols>]
    position=<symbols>
    order             : shows order updates
    order+            : -||- and attempts to place one small order
    order+-           : -||- and attempts to cancel the order
    
<symbols> are spaceless comma separated: BTC/USDT,XRP/BTC

Params:
    d / display: prints all payloads received from the server

"""
import asyncio
import functools
import itertools
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

def print_xs_info():
    #print(uxs.ExchangeSocket.obm)
    #print(xs.__class__.obm)
    print(xs.sub_to_orderbook)
    print(xs.obm)
    print(xs.apiKey)
    print('has ("all_tickers"): {}'.format(xs.has_got('all_tickers')))
    print('has ("all_tickers","last"): {}'.format(xs.has_got('all_tickers','last')))
    print('has ("all_tickers",["last"]): {}'.format(xs.has_got('all_tickers',['last'])))
    print('has ("all_tickers",["last","notExistingKey"]): {}'.format(xs.has_got('all_tickers',['last','notExistingKey'])))
    print('has ("all_tickers",["last","high"]): {}'.format(xs.has_got('all_tickers',['last','high'])))

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
            
async def _print_changed(channel, attr=None, clear_first=False, from_index=None, key=None):
    if attr is None: attr = channel
    if clear_first:
        [xs.events[channel][x].clear() for x in xs.events[channel]]
    await xs.events[channel][-1].wait()
    were_set = [x for x,y in xs.events[channel].items() if y.is_set()]
    if key is None:
        key = lambda x,y: y
    changes = {x: key(x, getattr(xs,attr,{}).get(x)) for x in were_set if x!=-1}
    if from_index is not None:
        get_index = lambda seq: max(0, from_index if from_index > 0 else len(seq) + from_index)
        changes = {x: list(itertools.islice(y, get_index(y), None)) for x,y in changes.items()}
    print('(d){}: {}'.format(channel, changes))
    [xs.events[channel][x].clear() for x in were_set]
        
        
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
        
    
async def fetch_order_book(symbols, _print='changes', sub=True, unsub=False, resub=False, merge=False, params={}):
    await asyncio.sleep(2)
    """try: print('orderbook {}: {}'.format(symbols[0], await xs.fetch_order_book(symbols[0])))
    except Exception as e:
        logger2.exception(e)"""
        
    if not sub: pass
    elif not merge:
        for symbol in symbols:
            xs.subscribe_to_orderbook(symbol, params)
    else: xs.subscribe_to_orderbook(symbols, params)
        
    def _print_ob_changes(inp, symbol=symbols[0]):
        print('(d)ob  {}: {}'.format(symbol, inp['data']))
        
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
        
async def fetch_trades(symbols=(), sub=True, unsub=False, resub=False, merge=False):
    _symbols = ([symbols] if merge else symbols)
    
    for symbol in _symbols:
        xs.subscribe_to_trades(symbol)

    for symbol in _symbols:
        params = {'_': 'trades', 'symbol': symbol}
        if sub and unsub: 
            asyncio.ensure_future(_unsub(params, unsub, merge))
        if sub and resub:
            asyncio.ensure_future(_resub(params, resub, merge))
            
    while True:
        await _print_changed('trades','trades',clear_first=True,from_index=-5)
        
async def fetch_ohlcv(symbols=(), sub=True, unsub=False, resub=False, merge=False):
    timeframes = dict.fromkeys(symbols, '1m')
    for x in symbols:
        if '_' in x:
            del timeframes[x]
            symbol,tf = x.split('_')
            timeframes[symbol] = tf
    symbols = tuple(timeframes.keys())
    _symbols = ([symbols] if merge else symbols)
    if merge:
        timeframes[symbols] = list(timeframes.values())[0]
    
    for symbol in _symbols:
        xs.subscribe_to_ohlcv(symbol, timeframes[symbol])

    for symbol in _symbols:
        params = {'_': 'ohlcv', 'symbol': symbol, 'timeframe': timeframes[symbol]}
        if sub and unsub: 
            asyncio.ensure_future(_unsub(params, unsub, merge))
        if sub and resub:
            asyncio.ensure_future(_resub(params, resub, merge))
            
    while True:
        await _print_changed('ohlcv','ohlcv',clear_first=True,
                             from_index=-5, key=lambda symbol, d: d[timeframes[symbol]])

async def fetch_position(symbols=()):
    await asyncio.sleep(2)
    while True:
        await _print_changed('position','positions',clear_first=False)
    
async def show_fills():
    while True:
        await xs.events['fill'][-1].wait()
        xs.events['fill'][-1].clear()
        print('fills:',xs.fills)
        #await _print_changed('trade','trades',True)
    
async def show_orders():
    while True:
        #await ws.events['order'][-1].wait()
        #ws.events['order'][-1].clear()
        #print('orders:',ws.open_orders,ws.closed_orders)
        await _print_changed('order','orders',clear_first=False)
            
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
    await xs.stop()
    
async def _restart(stop_wait_time):
    await asyncio.sleep(stop_wait_time)
    await xs.stop()
    await xs.start()   
        

def main():
    global xs
    
    try: xc = sys.argv.pop(1)
    except IndexError:
        xc = DEFAULT_EXCHANGE
    
    def _to_float(x):
        try: return float(x)
        except ValueError:
            return None
        
    def _to_int(x):
        try: return int(x)
        except ValueError:
            return None
        
    def _split(x):
        return x.split(',')
    
    activities = ['u','unsub','r','resub','s','stop','c','crash']
    apply = dict.fromkeys(activities, _to_float)
    apply.update(dict.fromkeys(['d','display','ticker','tickers',
                                'all_tickers','ob','trades','ohlcv',
                                'account','pos','position','positions'], _split))
    apply['log'] = apply['loggers'] = _to_int
    
    p = parse_argv(sys.argv[1:], apply)
    
    nr_test_loggers = p.get(p.which(['log','loggers']))
    if nr_test_loggers is None:
        nr_test_loggers = 2
    
    fons.log.quick_logging(nr_test_loggers)

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
        if p.contains(display, set='mapped'):
            display = p.get(display)
        else:
            display = ['responses']
    else:
        display = DEFAULT_DISPLAY
    
    config = {
        'auth': 'TRADE',
        'name':'{}Ws'.format(xc.capitalize()), 
        'channels': {
            'orderbook': {'delete_data_on_unsub': False}},
        'ob': {'assert_integrity': True},
    }

    if 'responses' in display or 'r' in display:
        print(display)
        config['connection_defaults'] = {'handle': lambda x: print(x)}
    
    try:
        xs = uxs.get_socket(xc, config)
    except ValueError as e:
        config['auth'] = 'NULL'
        xs = uxs.get_socket(xc, config)
    
    print_xs_info()

    coros = [xs.start()]
    
    t_param = p.which(['ticker','tickers','all_tickers'], '')
    ob_param = p.which(['ob'], '')
    trades_param = p.which(['trades',], '')
    ohlcv_param = p.which(['ohlcv',], '')
    o_param = next((x for x in p if x.startswith('order+') or x=='order'), '')
    o_plus = o_param.startswith('order+')
    a_param = p.which(['account'], '')
    p_param = p.which(['pos','position','positions'], '')
    account_symbols = set()
    
    def _get_items(param, default_symbols=('ETH/BTC',)):
        index = p.indexes[param][0][0]
        merge = len(p.argv) > index + 1 and (p.argv[index+1] == 'm')
        symbols = p.get(param, default_symbols)
        return symbols, merge
        
    if t_param:
        t_symbols, t_merge = _get_items(t_param, ())
        coros += [fetch_tickers(t_symbols, sub=True, unsub=unsub, resub=resub, merge=t_merge)]
        
    if ob_param:
        ob_symbols, ob_merge = _get_items(ob_param)
        extent = p.get('ob_extent')
        ob_params = {}
        if extent:
            try: extent = int(extent)
            except ValueError: pass
            ob_params['extent'] = extent
        #ob_print = 'changes'
        ob_print = 4
        coros += [fetch_order_book(ob_symbols, ob_print, sub=True, unsub=unsub, resub=resub, merge=ob_merge, params=ob_params)]
        if crash: coros += [_crash({'_': 'orderbook', 'symbol': ob_symbols[0]}, crash)]
        
    if trades_param:
        tr_symbols, tr_merge = _get_items(trades_param)
        coros += [fetch_trades(tr_symbols, sub=True, unsub=unsub, resub=resub, merge=tr_merge)]
        
    if ohlcv_param:
        oh_symbols, oh_merge = _get_items(ohlcv_param)
        coros += [fetch_ohlcv(oh_symbols, sub=True, unsub=unsub, resub=resub, merge=oh_merge)]
    
    if a_param:
        a_symbols, _ = _get_items(a_param)
        account_symbols.update(a_symbols)
    
    if o_param:
        coros += [show_fills(), show_orders(), show_balances()] #show_books()
        if crash: coros += [_crash({'_': 'account'}, crash)]
        
    if o_plus:
        o_rest = o_param[len('order+'):]
        cancel = o_rest.startswith('-')
        if cancel: o_rest = o_rest[1:]
        o_args = o_rest.split(',') if o_rest else []
        logger2.debug('Scheduling order: {}'.format(o_args))
        account_symbols.add(o_args[0] if o_args else 'ETH/BTC')
        coros += [place_order(*o_args)]
        if cancel: coros += [cancel_order(*o_args)]
        
    if p_param:
        p_symbols, _ = _get_items(p_param)
        account_symbols.update(p_symbols)
        coros += [fetch_position(p_symbols)]
    
    if any([a_param, o_param, p_param]) and not account_symbols:
        account_symbols.add('ETH/BTC')
        
    if account_symbols:
        xs.subscribe_to_account({'symbol': xs.merge(account_symbols)})  
        
    if stop:
        coros += [_stop(stop), _restart(stop)]
        
    g = asyncio.gather(*coros)
    #asyncio.get_event_loop().run_forever()
    lrc(g)


if __name__ == '__main__':
    main()
        