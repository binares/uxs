import asyncio
import functools
import time
import sys

DEFAULT_EXCHANGE = 'bittrex'
DEFAULT_SHOW = ['ob']

import uxs
from fons.aio import call_via_loop_afut, lrc
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


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
        print('(d)ob  {}: {}'.format(symbol,changes))
        
    def _print_last_n(symbol=symbols[0]):
        ob = xs.orderbooks.get(symbol,{})
        print('(d)ob {} bid[:{}]: {}'.format(symbol,_print,ob.get('bid',[])[:_print]))
        print('(d)ob {} ask[:{}]: {}'.format(symbol,_print,ob.get('ask',[])[:_print]))
    
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
            (await e.wait()),e.clear()
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

async def _stop():
    await asyncio.sleep(stop)
    xs.stop()
    
async def _restart():
    await asyncio.sleep(stop+2)
    await xs.start()   
        
        
if __name__ == '__main__':
    try: xc = sys.argv[1]
    except IndexError:
        xc = DEFAULT_EXCHANGE

    unsub = False
    for param in '-u','-unsub':
        try: i = sys.argv[2:].index(param)
        except ValueError: continue
        try: unsub = float(sys.argv[2+i+1])
        except (ValueError,IndexError): unsub = 4
    resub = False
    for param in '-r','-resub':
        try: i = sys.argv[2:].index(param)
        except ValueError: continue
        try: resub = unsub + float(sys.argv[2+i+1])
        except (ValueError,IndexError):
            resub = unsub + 2
    stop = False
    for param in '-s','-stop':
        try: i = sys.argv[2:].index(param)
        except ValueError: continue
        try: stop = float(sys.argv[2+i+1])
        except (ValueError,IndexError):
            stop = max(resub + 2, unsub + 2, 10)
    crash = False
    for param in '-c','-crash':
        try: i = sys.argv[2:].index(param)
        except ValueError: continue
        try: crash = float(sys.argv[2+i+1])
        except (ValueError,IndexError):
            crash = max(resub + 2, unsub + 2, 10)
    
    show = sys.argv[2:]
    if not show: show = DEFAULT_SHOW
    
    fons.log.quick_logging(3)
    config = {'name':'{}Ws'.format(xc.capitalize()),}
    config['channels'] = {'orderbook': {'delete_data_on_unsub': False}}
    config['ob'] = {'assert_integrity': True}
    if 'responses' in show:
        config['connection_defaults'] = {'handle': lambda x: print(x)}
        
    xs = uxs.get_socket(xc, config)
    
    oid = None
    
    print('has ("all_tickers"): {}'.format(xs.has_got('all_tickers')))
    print('has ("all_tickers","last"): {}'.format(xs.has_got('all_tickers','last')))
    print('has ("all_tickers",["last"]): {}'.format(xs.has_got('all_tickers',['last'])))
    print('has ("all_tickers",["last","notExistingKey"]): {}'.format(xs.has_got('all_tickers',['last','notExistingKey'])))
    print('has ("all_tickers",["last","high"]): {}'.format(xs.has_got('all_tickers',['last','high'])))

    coros = [xs.start()]
    t_param, t_i = next(((x,i) for i,x in enumerate(show) if x.startswith('ticker') or x=='all_tickers'),('',None))
    if t_param:
        try: t_merge = show[t_i+1] == 'm'
        except IndexError: t_merge = False
        t_symbols = () if '=' not in t_param else tuple(t_param.split('=')[1].split(','))
        coros += [fetch_tickers(t_symbols, sub=True, unsub=unsub, resub=resub, merge=t_merge)]
    ob_param, ob_i = next(((x,i) for i,x in enumerate(show) if x=='ob' or x.startswith('ob=')),('',None))
    if ob_param:
        #ob_print = 'changes'
        ob_symbols = ('ETH/BTC',) if ob_param=='ob' else tuple(ob_param.split('=')[1].split(','))
        ob_print = 4
        try: ob_merge = show[ob_i+1] == 'm'
        except IndexError: ob_merge = False
        coros += [fetch_order_book(ob_symbols, ob_print, sub=True, unsub=unsub, resub=resub, merge=ob_merge)]
    o_param, o_i = next(((x,i) for i,x in enumerate(show) if x.startswith('order+') or x=='order'),('',None))
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
        coros += [_stop(), _restart()]
        
    g = asyncio.gather(*coros)
    #asyncio.get_event_loop().run_forever()
    lrc(g)
        