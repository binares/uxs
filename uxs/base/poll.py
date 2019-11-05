import prj,os
P = prj.qs(__file__)

import datetime
dt = datetime.datetime
td = datetime.timedelta
#from dateutil.parser import parse as parsedate
from collections import (namedtuple,deque)
import itertools as it
import filelock
import json
import yaml
import asyncio
import threading
import copy as _copy
import ccxt

#from wrappers import (get_exchange,get_name)
from .ccxt import (get_exchange, get_name)

from fons.io import (DateTimeEncoder, SafeFileLock, wait_filelock)
from fons.os import make_dirpath
from fons.time import (dt_strp, dt_round_to_digit, freq_to_offset,
                       freq_to_td, pydt_from_ms, timestamp_ms)
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

PATHS = {'dir': make_dirpath(P.dpath+'\\.cache','exchanges'),
         'entries': P.dpath+'\\.cache\\exchanges\\__entries__',
         'exchange': P.dpath+'\\.cache\\exchanges\\{exchange}\\{file}',
         'lock': P.dpath+'\\.cache\\exchanges\\__entries__.lock',
         'exchange_profiles': make_dirpath(P.dpath,'user','exchange-profiles'),}

fnInf = namedtuple('fnInf','exchange type date file data')
fnInf.__new__.__defaults__ = (None,)*len(fnInf._fields)
prInf = namedtuple('prInf','profile exchange type start end file data')
prInf.__new__.__defaults__ = (None,)*len(prInf._fields)

_MP_LOCK_TIMEOUT = 2
_MIN_LOAD_AGE = 0.1
_MP_LOCK = filelock.FileLock(PATHS['lock'],timeout=_MP_LOCK_TIMEOUT)
_GL_LOCK = threading.Lock()
_MP_LOCK.release(True)

_METHODS = {
    'markets': ('load_markets',tuple(),{'reload':True},[]),
    'currencies': ('fetch_currencies',tuple(),{},[]),
    'balances': ('fetch_balance',tuple(),{},[]),
    'balances-account': ('fetch_balance',({'type':'account'},),{},[]),
    'tickers': ('fetch_tickers',tuple(),{},[]),
    'ticker': ('fetch_ticker',tuple(),{},['symbol']),
    'orderbook': ('fetch_order_book',tuple(),{},['symbol']),
}

_BLOCK = {
    'markets': 2,
    'currencies': 2,
    'balances': 2,
    'balances-account': 2,
    'tickers': 2,
    'ticker': 2,
    'orderbook': 2,
}

_MAXLENS = {
    'markets': 5,
    'currencies': 5,
    'balances': 5,
    'balances-account': 5,
    'tickers': 5,
    'ticker': 2,
    'orderbook': 2,}

_LOADS_MARKETS = ('balances','balances-account','tickers','ticker','orderbook')

storage = {}

def _assign_storage_deque(exchange, type, *args):
    t_type = _type_tuple(type)
    value_given = None
    try: value = value_given = args[0]
    except IndexError: 
        value = deque(maxlen=_MAXLENS[t_type[0]])
    full_id = list(it.chain([exchange],t_type))
    len_full_id = len(full_id)
    d = storage
    for i,k in enumerate(full_id):
        if i < len_full_id-1:
            if k not in d:
                d[k] = {}
            d = d[k]
        elif k in d and value_given is None:
            pass
        elif value is not None:
            d[k] = value
        
def _get_storage_deque(exchange, type):
    _assign_storage_deque(exchange,type)
    t_type = _type_tuple(type)
    d = storage[exchange]
    for k in t_type:
        d = d[k]
    deq = d
    return deq
        
async def set_markets(api, limit=15):
    limit = _resolve_limit(limit)
    exchange = get_name(api)
    markets = currencies = None
    try:
        m0 = (await get(exchange,'markets',limit,1))[0]
        l1 = _resolve_limit(limit)
        l2 = _resolve_limit(m0.date)
        limit = min(l1,l2) if None not in (l1,l2) else \
                next((x for x in (l1,l2) if x is not None),None)
        markets = _copy.deepcopy(m0.data)
        currencies = _copy.deepcopy(load(exchange,'currencies',limit,1)[0].data)
    except Exception as e:
        logger.exception(e)
    finally:
        if markets:
            logger.debug('{} - setting markets'.format(exchange))
            try: api.set_markets(markets,currencies)
            except Exception as e2: logger.exception(e2)


async def get(exchange, type, limit=None, max=1,*,
              file=True, globals=True, empty_update=True,
              blocked='sleep', args=None, kwargs=None, loop=None,
              attempts=2, raise_e=False):
    """Tries to retrieve the latest data"""
    type = _resolve_type(type)
    limit = _resolve_limit(limit)
    if blocked is None: blocked = 'sleep'
    elif blocked not in ('sleep','return'):
        raise ValueError(blocked)
    
    exchange0 = exchange
    if not isinstance(exchange,str):
        exchange0 = get_exchange(exchange)
        exchange = get_name(exchange0)
        
    wait_for = _is_blocked(exchange,type)
    if not wait_for: pass
    elif blocked == 'sleep':
        await asyncio.sleep(wait_for, loop=loop)
    else: return []
    
    items = []
    
    if file:
        items = load(exchange,type,limit,max,globals=globals)
    elif globals:
        items = retrieve(exchange,type,limit,max)
    
    
    if not items and empty_update:
        items = await update(exchange0,type,args,kwargs,loop=loop,
                             file=file,globals=globals,blocked=blocked,
                             attempts=attempts,raise_e=raise_e)
        
    return items
    

async def update(exchange, type, args=None, kwargs=None, *,
                 file=True, globals=True, loop=None, limit=None,
                 blocked='sleep', attempts=2, raise_e=False):
    type = _resolve_type(type)
    limit = _resolve_limit(limit)
    if blocked is None: blocked = 'sleep'
    elif blocked not in ('sleep','return'):
        raise ValueError(blocked)
    
    if args is None: args = tuple()
    if kwargs is None: kwargs = {}
    
    exchange0 = exchange
    exchange = get_name(exchange)
    
    try: api = get_exchange(exchange0)
    except ValueError as e:
        if type in ('balances','balances-account'):
            raise e
        config = {'exchange':exchange, 'info':False,'trade':False}
        logger.debug('Trying to init ccxt-exchange with lowest auth: {}'.format(config))
        api = get_exchange(config)
        
    wait_for = _is_blocked(exchange,type)
    if not wait_for: pass
    elif blocked == 'sleep':
        await asyncio.sleep(wait_for,loop=loop)
        if file:
            return load(exchange,type,limit,1,globals=globals)
        elif globals:
            return retrieve(exchange,type,limit,1)
        else: return []
    else: return []
    
    inf = []
    
    type0 = _type0(type)
    method_str,args2,kwargs2,kw_ids = _METHODS[type0]
    #for symbol, the type must be in format (name,symbol)
    # and symbol must not be included in args/kwargs
    #kwargs2 = dict(kwargs2, **{x:type[i+1] for i,x in enumerate(kw_ids)})
    method = getattr(api,method_str)
    #print(api._custom_name,'method:',method,'session:',api.session)
    args = tuple(args) + args2[len(args):]
    if kw_ids:
        args = tuple(type[i+1] for i,x in enumerate(kw_ids)) + args
    kwargs = dict(kwargs2, **kwargs)
    is_market = (type == 'markets')
    
    if type0 in _LOADS_MARKETS and not api.markets:
        await set_markets(api)
    
    exc, i = None, 0
    while i < attempts:
        _block(exchange,type,_BLOCK[type0])
        try:
            data = await method(*args,**kwargs)
            #await api.close()
            now = dt_round_to_digit(dt.utcnow(),6)
            inf.append(create_new(exchange,type,now,data=data))
            if is_market:
                inf.append(create_new(exchange,'currencies',now,data=api.currencies))
        except Exception as e:
            exc = e
            if isinstance(e,ccxt.NotSupported): 
                i = attempts-1
            elif isinstance(e,KeyError) and type=='tickers' and not i:
                logger.debug('{} - fetch_tickers caused KeyError. Re-loading markets.'.format(exchange))
                await set_markets(api)
            if i == attempts-1:
                logger2.error('{} - error fetching {}: {}'.format(exchange,type,e))
                logger.exception(e)
        else: break
        i += 1
    
    if exc is not None and i == attempts-1 and raise_e:
        raise exc
    
    globalise(inf)
    save(inf)
    
    if is_market and inf:
        inf = inf[:1]
        
    return inf


def globalise(items):
    for item in sorted(items, key=lambda x: x.date):
        seq = _get_storage_deque(item.exchange,item.type)
        pos = next((i for i,x in enumerate(seq) if item.date>=x.date), None)
        
        if pos is None:
            if len(seq): continue
            else: pos = 0
        
        try: 
            if seq[pos].date == item.date:
                seq[pos] = item
                continue
        except IndexError: pass
        
        if pos != 0:
            maxlen = _MAXLENS[_type0(item.type)]
            new_l = (list(seq[:pos]) + [item] + list(seq[pos:]))[:maxlen]
            _assign_storage_deque(item.exchange,item.type,deque(new_l, maxlen=maxlen))
        else: seq.appendleft(item)

          
def save(items):
    for item in sorted(items, key=lambda x: x.date):
        fn = item.file if item.file else encode_filename(item.exchange,item.type,item.date)
        
        _dir = os.path.join(PATHS['dir'],item.exchange)
        if not os.path.exists(_dir):
            make_dirpath(_dir)
            
        path = _dir + '\\{}'.format(fn)
        
        with SafeFileLock(path,0.01):
            with open(path,'w',encoding='utf-8') as f:
                json.dump(item.data,f,cls=DateTimeEncoder)
        
        p = probe(item.exchange,item.type,globals=False)
        maxlen = _MAXLENS[_type0(item.type)]
        with_file = (x for x in reversed(p[maxlen:]) if x.file)
        for wf in with_file:
            try: os.remove(os.path.join(_dir,wf.file))
            except OSError: pass


def retrieve(exchange, type, limit=None, max=5):
    limit = _resolve_limit(limit)
    d = storage
    for key in it.chain([exchange],_type_tuple(type)):
        try: d = d[key]
        except KeyError: return []
    items = d
    
    if limit: items = [x for x in items if x.date >= limit]
    else: items = list(items)
    if max is not None: items = items[:max]
    
    return items

def retrieve_latest(exchange, type, limit=None):
    return retrieve(exchange,type,limit,1)
    
    
def load(exchange, type, limit=None, max=5, globals=True):
    limit = _resolve_limit(limit)
    inf = probe(exchange,type,limit,max,globals=globals)
    items = []
    for tpl in inf:
        if tpl.data is not None:
            items.append(tpl)
            continue
        path = os.path.join(PATHS['dir'],exchange,tpl.file)
        wait_filelock(path)
        logger.debug('Reading: {}'.format(path))
        with open(path,encoding='utf-8') as f:
            item = fnInf(*tpl[:-1],json.load(f))
        globalise([item])
        items.append(item)
        
    return items

def load_latest(exchange, type, limit=None, globals=True):
    return load(exchange,type,limit,1,globals)
    
    
def probe(exchange, type, limit=None, max=None, globals=True):
    limit = _resolve_limit(limit)
    type_str = _type_str(type)
    begins = '[{}]_{}_'.format(exchange.lower(),type_str)
    ends = '.json'
    _len = len(begins)
    _len_ends = len(ends)
    
    _dir = os.path.join(PATHS['dir'],exchange)
    try: files = (x for x in reversed(os.listdir(_dir)) if x[:_len]==begins and x[_len:].isdigit())
    except FileNotFoundError:
        files = []
    
    _decoded = (decode_filename(x) for x in files)
    decoded = list(x for x in _decoded if x.date >= limit) if limit is not None else list(_decoded)
    
    if globals: 
        items = retrieve(exchange,type,limit,max)
        items += [x for x in decoded if not any(x.date==y.date for y in items)]
        decoded = items
        
    decoded.sort(key=lambda x: x.date, reverse=True)
    if max is not None: decoded = decoded[:max]
            
    return decoded
    
def probe_latest(exchange, type, limit=None, globals=True):
    return probe(exchange,type,limit,1,globals)


#########################################################

def encode_filename(exchange, type, date):
    type_str = _type_str(type)
    return '[{}]_{}_{}'.format(exchange.lower(),type_str,timestamp_ms(date))

def decode_filename(fn):
    e0,e1 = fn.find('['),fn.find(']')
    e = fn[e0+1:e1]
    split = fn[e1+2:].replace(';','/').split('_')
    type = tuple(split[:-1])
    datestr = split[-1]
    #date = parsedate(datestr,'%Y-%m-%dT%H-%M-%S-%f')
    date = pydt_from_ms(int(datestr))
    if len(type) < 2:
        type = type[0]
    return fnInf(e,type,date,fn)

def _get_blocks(exchange, type=None):
    _dir = make_dirpath(PATHS['dir'],exchange,'__block__')
    blocks = []
    for f in os.listdir(_dir):
        if not f.startswith('__'): continue
        try: blocks.append(_decode_block(f))
        except ValueError: continue
    if type is not None:
        type = _resolve_type(type)
        blocks = [x for x in blocks if x.type == type]
    blocks.sort(key=lambda x: x.date, reverse=True)
    return blocks

def _encode_block(exchange, type, until):
    return '__{}'.format(encode_filename(exchange,type,until))

def _decode_block(fn):
    return decode_filename(fn)

def create_new(exchange, type, date=None, file=None, data=None):
    if date is None: date = dt_round_to_digit(dt.utcnow(),6)
    if file is None: file = encode_filename(exchange,type,date)
    return fnInf(exchange,type,date,file,data)


def _is_blocked(exchange, type):
    _dir = os.path.join(PATHS['dir'],exchange)
    blocks = _get_blocks(exchange,type)
    
    if len(blocks):
        now = dt.utcnow()
        remaining = (blocks[0].date - now).total_seconds()
        return max(0,remaining)
    
    return 0

def _block(exchange, type, until):
    if not isinstance(until,dt):
        until = dt.utcnow() + freq_to_td(until)
    #round to millisecond
    until = dt_round_to_digit(until,6)
    
    _dir = os.path.join(PATHS['dir'],exchange,'__block__')
    fn = _encode_block(exchange,type,until)
    blocks = _get_blocks(exchange,type)
    #print('{} {} blocks: {}'.format(exchange,type,blocks))
    
    with open(os.path.join(_dir,fn),'w'):
        pass
    
    for b in (x for x in blocks if x.date < until):
        try: os.remove(os.path.join(_dir,b.file))
        except OSError: pass
    
def _release(exchange, types):
    if isinstance(types,str): types = (types,)
    types = [_resolve_type(t) for t in types]
    _dir = os.path.join(PATHS['dir'],exchange,'__block__')
    blocks = _get_blocks(exchange)
    blocks = [x for x in blocks if x.type in types]
    
    for b in blocks:
        try: os.remove(os.path.join(_dir,b.file))
        except OSError: pass
    

def _resolve_limit(limit):
    if limit is None: return None
    elif not isinstance(limit,dt):
        return dt.utcnow() - freq_to_offset(limit)
    else: return limit
    
def _resolve_type(type):
    if not isinstance(type,str):
        #iter to tuple
        type = tuple(type)
        if len(type) < 2:
            type = type[0]
    type0 = _type0(type)
    if (type0 not in _METHODS or isinstance(type,str) and len(_METHODS[type0][3]) 
        or not isinstance(type,str) and len(_METHODS[type0][3])!=len(type)-1):
        raise ValueError('Incorrect type: {}'.format(type))
    return type

def _type_str(type):
    return '_'.join(_type_tuple(type)).replace('/',';')

def _type0(type):
    return type if isinstance(type,str) else type[0]

def _type_tuple(type):
    return (type,) if isinstance(type,str) else tuple(type)

def load_profile(profile, exchange, type='markets'):
    now = dt.utcnow()
    dir = '{}\\{}'.format(PATHS['exchange_profiles'],profile)
    if not os.path.isdir(dir):
        return []
    files = os.listdir(dir)
    startsw = '{}_{}'.format(exchange,type)
    matching = [x for x in files if x.startswith(startsw)]
    dates = {}
    for fn in matching:
        ending = fn[len(startsw):]
        is_valid = True
        start = end = None
        exc = None
        if ending.endswith('.yaml') or ending.endswith('.yml'):
            ending = '.'.join(ending.split('.')[:-1])
        if ending in ('','_'):
            pass
        elif not ending.startswith('_'):
            is_valid = False
        else:
            split = ending[1:].split('to')
            _len = len(split)
            start = end = None
            try:
                if _len > 2:
                    is_valid = False
                else:
                    if split[0]:
                        start = dt_strp(split[0]) #parsedate(split[0])
                    if _len == 2 and split[1]:
                        end = dt_strp(split[1]) #parsedate(split[1])
            except Exception as e:
                exc = e
                is_valid = False
                
        if not is_valid:
            logger2.error('Incorrect filename: {}'.format(os.path.join(dir,fn)))
        else:
            dates[fn] = {'start': start, 'end': end}
        if exc is not None:
            logger2.exception(exc)
            
    unexpired = [x for x in matching if x in dates and (dates[x]['start'] is None or dates[x]['start'] < now)
                 and (dates[x]['end'] is None or now < dates[x]['end'])]
    logger.debug('Acquired the following profile {} unexpired files: {}'.format(
                                        (profile,exchange,type),unexpired))
    
    unexpired_items = []
    for fn in unexpired:
        path = os.path.join(dir,fn)
        try:
            with open(path) as f:
                data = yaml.load(f)
            t = prInf(profile,exchange,type,dates[fn]['start'],dates[fn]['end'],fn,data)
            unexpired_items.append(t)
        except OSError as e:
            logger2.error('Could not read file "{}"'.format(path))
            logger2.exception(e)
        
    return unexpired_items
    

