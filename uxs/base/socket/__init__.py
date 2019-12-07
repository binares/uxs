import sys
import copy as _copy
import asyncio
import itertools
import time
import ccxt
from collections import (defaultdict, deque)
import datetime
dt = datetime.datetime
td = datetime.timedelta

from ..auth import get_auth2, EXTRA_TOKEN_KEYWORDS
from ..ccxt import get_exchange
from .. import poll
from .orderbook import OrderbookMaintainer
from .errors import (ExchangeSocketError, ConnectionLimit)
from uxs.fintls.basics import (as_ob_fill_side, as_direction)
from uxs.fintls.ob import (get_stop_condition, create_orderbook, update_branch, assert_integrity,
    _resolve_times)
from wsclient import WSClient

import fons.log
from fons.aio import call_via_loop
from fons.iter import unique, sequence_insert

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)



class ExchangeSocket(WSClient):
    """Override these methods: handle, encode, auth
       Some exchanges provide socket methods: create_order, cancel_order, fetch_balance
       And attributes of your choosing."""
       
    exchange = None
    
    #Used on .subscribe_to_{x} , raises ValueError when doesn't have
    has = dict.fromkeys([
         'all_tickers', 'ticker', 'orderbook', 'trades',
         'ohlcv', 'account', 'match', 'balance',
         'fetch_balance', 'fetch_tickers', 'fetch_ticker',
         'fetch_order_book', 'fetch_ohlcv',
        ], False)
    
    channel_defaults = {
        'cnx_params_converter': 'm$convert_cnx_params',
        'delete_data_on_unsub': True,
    }
    #If 'is_private' is set to True, .sign() is called on the specific output to server
    #if 'ubsub' is set to False, .remove_subscription raises ExchangeSocketError on that channel
    channels = {
        'account': {
            'required': [],
            'is_private': True,
            #In channel_defaults this is set to True
            #If set to True then it deletes "balances" and "positions"
            #when unsubscribed (including lost connection)
            'delete_data_on_unsub': False,
        },
        'match': {
            'required': ['symbol'],
            'is_private': True,
        },
        'all_tickers': {
            'required': [],
        },
        'ticker': {
            'required': ['symbol'],
        },
        'orderbook': {
            'required': ['symbol'],
        },
        'trades': {
            'required': ['symbol'],
        },
        'ohlcv': {
            'required': ['symbol','timeframe'],
        }
    }
    
    OrderbookMaintainer_cls = OrderbookMaintainer
    
    ob = {
        #the time after which all not-already-auto-created
        # orderbooks (on 1st .send_update) will be force created
        # on (each) connection (re-)activation. Set to `None` to disable.
        'force_create': 15,
        #minimal interval between two .fetch_order_book(a_symbol)
        'reload_after': 15,
        #reload orderbook if new_nonce > prev_nonce + nonce_delta 
        #'reload_on_nonce_delta': 2,
        #the number of latest updates to be kept in cache
        'cache_size': 30,
        'uses_nonce': True,
        'receives_snapshot': False,
        'assert_integrity': False,
        #whether or not bid and ask of ticker if modified on orderbook update
        'sends_bidAsk': False,
    }
    order = {
        #these two don't matter if the websocket itself
        # receives new order/cancel order updates
        'add_automatically': True,
        'cancel_automatically': True,
        #almost all send remaining. some send filled.
        'update_remaining_on_fill': False,
        'update_filled_on_fill': False,
        #usually payouts are not sent with order updates
        #automatically calculate it from each fill
        'update_payout_on_fill': True,
    }
    
    store_trades = 1000
    store_ohlcv = 1000
    match_is_subset_of_trades = False
    reload_markets_on_keyerror = 150
    
    #since socket interpreter may clog due to high CPU usage,
    # maximum lengths for the queues are set, and if full,
    # previous items are removed one-by-one to make room for new ones
    queue_maxsizes = {
        'balance': 100, 
        'ticker': 1000, 
        'order': 100, 
        'fill': 100,
        'orderbook': 1000,
        'trades': 10000,
        'ohlcv': 100,
        'event': 1000,
        'received': 1000,
        'send': 100,
    }
    
    #The accepted age for retrieving stored markets,currencies,balances etc...
    api_attr_limits = {
        'markets': None,
        #'currencies': None,
        'balances': None,
    }
    __properties__ = \
        [['balance','balances'],
         ['obm','orderbook_maintainer'],
         ['apiKey','api.apiKey'],
         ['secret','api.secret'],
         ['auth_info','api._auth_info'],
         ['fetch_orderbook','fetch_order_book'],
         ['subscribe_to_ob','subscribe_to_orderbook'],
         ['unsubscribe_to_ob','unsubscribe_to_orderbook'],
         ['sub_to_ob','subscribe_to_orderbook'],
         ['unsub_to_ob','unsubscribe_to_orderbook'],]
    
    __extend_attrs__ = WSClient.__extend_attrs__ + ['api_attr_limits','order','ob']
    __deepcopy_on_init__ = __extend_attrs__[:]
    
    
    def __init__(self, config={}):
        
        config = config.copy()
        auth = _resolve_auth(self.exchange, config)
        
        super().__init__(config)
        
        self.api = get_exchange({
            'exchange':self.exchange,
            'async':True,
            'kwargs': {
                'load_currencies':None,
                'load_markets':None,
                'profile': getattr(self,'profile',None),},
            'auth': auth,
        })
        
        if getattr(self.api,'markets',None) is None:
            self.api.markets =  {}
        if getattr(self.api,'currencies',None) is None:
            self.api.currencies = {}
        
        names = ['ticker','orderbook','trades','ohlcv','balance',
                 'fill','order','position','empty','recv']
        #query_names = ['fetch_order_book','fetch_ticker','fetch_tickers','fetch_balance']
        self.events = defaultdict(lambda: defaultdict(lambda: asyncio.Event(loop=self.loop)))
        for n in names:
            self.events[n][-1]
        self.events['empty'][-1] = self.station.get_event('empty',0,0)
        self.events['recv'][-1] = self.station.get_event('recv',0,0)
        self._init_events()
        
        self.callbacks = {'orderbook': {}, 'ticker': {}, 'trades': {}, 'ohlcv': {},
                          'position': {}}
        self.tickers = {}
        self.balances = {'free': {}, 'used': {}, 'total': {}}
        self.orderbooks = {}
        self.trades = {}
        self.ohlcv = {}
        self.unprocessed_fills = defaultdict(list)
        self.orders = {}
        self.open_orders = {}
        self.fills = {}
        self.positions = {}
        
        self.orderbook_maintainer = self.OrderbookMaintainer_cls(self)
        self._last_markets_loaded_ts = 0
        #deep_update(self.merge_subs, {n:{} for n in ['ticker','orderbook','market']})
    
    
    async def _init_api_attrs(self):
        #if not getattr(self.api,'markets',None):
        limit = self.api_attr_limits['markets']
        await self.api.poll_load_markets(limit)
        #self.api.markets = await poll.fetch(self.api,'markets',limit)
        #self.api.currencies = await poll.fetch(self.api,'currencies',limit)
        if len(self.balances) < 5 and self.apiKey: #info,free,used,total
            self.balances = await poll.fetch(self.api,'balances',self.api_attr_limits['balances'])
        self._init_events()
        self._init_api_attrs_done = True
    
    def _init_events(self):
        markets = self.api.markets if self.api.markets is not None else {}
        currencies = self.api.markets if self.api.markets is not None else {}
        
        for market in markets:
            for x in ['ticker','orderbook','trades','ohlcv','position']:
                if market not in self.events[x]:
                    self.events[x][market] = asyncio.Event(loop=self.loop)
        
        for cy in currencies:
            for x in ['balance']:
                if cy not in self.events[x]:
                    self.events[x][cy] = asyncio.Event(loop=self.loop)
            
            
    async def on_start(self):    
        #TODO: make _init_api_attrs safe?
        if not getattr(self,'_init_api_attrs_done',None):
            await self._init_api_attrs()
        self._init_events()
        
        
    def notify_unknown(self, r, max_chars=500):
        print('{} - unknown response: {}'.format(self.name, str(r[:max_chars])))
        
    def update_tickers(self, data):
        """[{
        'symbol': 'ETH/BTC', 'timestamp': 1546177530977, 'datetime': '2018-12-30T13:45:30.977Z',
        'high': 0.037441, 'low': 0.034593, 'bid': 0.036002, 'bidVolume': 0.416, 'ask': 0.036016, 'askVolume': 0.052, 'vwap': 0.03580656,
        'open': 0.035201, 'close': 0.036007, 'last': 0.036007, 'previousClose': 0.035201, 'change': 0.000806, 'percentage': 2.29, 'average': None,
        'baseVolume': 370410.985, 'quoteVolume': 13263.14351447, }, ...]"""
        ticker_events = self.events['ticker']
        cb_data = []
        for d in data:
            #subscribe_to_all_tickers may suddenly receive new markets when they are added
            # to the exchange. Ignore them because we don't want to cause KeyError or other unexpectancies.
            symbol = d['symbol']
            if symbol not in self.api.markets:
                continue
            self.tickers[symbol] = d
            _set_event(ticker_events, symbol)
            self.broadcast_event('ticker', symbol)
            cb_input = d.copy()
            self.exec_callbacks(cb_input, 'ticker', symbol)
            cb_data.append(cb_input)
            
        _set_event(ticker_events, -1)
        self.api.tickers = self.tickers
        self.exec_callbacks(cb_data, 'ticker', -1)
    
    def create_orderbooks(self, data): 
        ob_events = self.events['orderbook']
        for ob in data:
            symbol = ob['symbol']
            self.orderbooks[symbol] = new = create_orderbook(ob)
            _set_event(ob_events, symbol)
            self.broadcast_event('orderbook',
                                 (symbol, len(new['bids']), len(new['asks'])))
            if self.ob['assert_integrity']:
                assert_integrity(self.orderbooks[symbol])
        _set_event(ob_events, -1)
            
    def update_orderbooks(self, data):
        ob_events = self.events['orderbook']
        sends = self.ob['sends_bidAsk']
        all_subbed = self.is_subscribed_to({'_': 'all_tickers'}, 1)
        only_if_not_subbed = isinstance(sends,dict) and sends.get('only_if_not_subbed_to_ticker')
        set_ticker_event = isinstance(sends,dict) and sends.get('set_ticker_event', True)
        is_subbed = lambda symbol: self.is_subscribed_to({'_':'ticker','symbol':symbol}, 1)
        cb_data = []
        
        for symbol_changes in data:
            symbol = symbol_changes['symbol']
            uniq_bid_changes,uniq_ask_changes = {},{}
            
            for side,uniq_changes in zip(('bids','asks'),(uniq_bid_changes,uniq_ask_changes)):
                branch = self.orderbooks[symbol][side]
                for item in symbol_changes[side]:
                    p,sprev,snew = update_branch(item,branch,side)
                    try: sprev = uniq_changes.pop(p)[1]
                    except KeyError: pass
                    uniq_changes[p] = [p,sprev,snew]
                    
            prev_nonce = self.orderbooks[symbol].get('nonce')
            if 'nonce' in symbol_changes:
                self.orderbooks[symbol]['nonce'] = symbol_changes['nonce']
                
            _set_event(ob_events, symbol)
            self.broadcast_event('orderbook',
                                 (symbol, len(symbol_changes['bids']), len(symbol_changes['asks'])))
            
            if sends and (not only_if_not_subbed or not all_subbed and not is_subbed(symbol)):
                self._update_ticker_from_ob(symbol, set_ticker_event)
            
            cb_input = {'symbol': symbol,
                        'bids': list(uniq_bid_changes.values()), 
                        'asks': list(uniq_ask_changes.values()),
                        'nonce': (prev_nonce, symbol_changes.get('nonce'))}
            self.exec_callbacks(cb_input, 'orderbook', symbol)
            cb_data.append(cb_input)
            
            if self.ob['assert_integrity']:
                assert_integrity(self.orderbooks[symbol])
                
        _set_event(ob_events, -1)
        self.exec_callbacks(cb_data, 'orderbook', -1)
        
    def _update_ticker_from_ob(self, symbol, set_event=True):
        try: d = self.tickers[symbol]
        except KeyError:
            d = self.tickers[symbol] = self.api.ticker_entry(symbol)
        for side in ('bid','ask'):
            try: d.update(dict(zip([side,side+'Volume'],
                                   self.orderbooks[symbol][side+'s'][0])))
            except IndexError: pass
        if set_event:
            _set_event(self.events['ticker'], symbol)
            _set_event(self.events['ticker'], -1)
            self.broadcast_event('ticker', symbol)
            
    def update_trades(self, data, key=None):
        """data: [{'symbol': symbol, 'trades': trades}, ...]
            where trades = [{'timestamp': <int>, 'datetime': <str>, 'symbol': <str>, 'id': <str>,
                             'order': <str>?, 'type': <str>, 'takerOrMaker': <str>, 'side': <str>,
                             'price': <float>, 'amount': <float>, 'cost': <float>, 'fee': <float>},
                             ... increasing timestamp]"""
        cb_data = []
        if key is None:
            key = lambda x: int(x['id'])
        
        for d in data:
            symbol = d['symbol']
            trades = d['trades']
            try: 
                add_to = self.trades[symbol]
            except KeyError: 
                add_to = self.trades[symbol] = deque(maxlen=self.store_trades)
            
            trades = sorted(trades, key=key)
            sequence_insert(trades, add_to, key=key, duplicates='drop')
            
            _set_event(self.events['trades'], symbol)
            self.broadcast_event('trades', symbol)
            
            cb_input = {'symbol': symbol,
                        'trades': [x.copy() for x in trades]}
            self.exec_callbacks(cb_input, 'trades', symbol)
            cb_data.append(cb_input)
            
        _set_event(self.events['trades'], -1)
        self.exec_callbacks(cb_data, 'trades', -1)
        
    def update_ohlcv(self, data):
        """data: [{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': ohlcv}, ...]
                where ohlcv = [[timestamp_ms, o, h, l, c, quoteVolume], ... increasing timestamp]"""
        cb_data = []
        key = lambda x: x[0]
        
        for d in data:
            symbol = d['symbol']
            timeframe = d['timeframe']
            ohlcv = d['ohlcv']
            try: 
                by_timeframes = self.ohlcv[symbol]
            except KeyError: 
                by_timeframes = self.ohlcv[symbol] = {}
            try:
                add_to = by_timeframes[timeframe]
            except KeyError:
                add_to = by_timeframes[timeframe] = deque(maxlen=self.store_ohlcv)
            
            ohlcv = sorted(ohlcv, key=key)
            sequence_insert(ohlcv, add_to, key=key, duplicates='drop')
            
            _set_event(self.events['ohlcv'], symbol)
            self.broadcast_event('ohlcv', symbol)
            
            cb_input = {'symbol': symbol,
                        'timeframe': timeframe,
                        'ohlcv': [x.copy() for x in ohlcv]}
            self.exec_callbacks(cb_input, 'ohlcv', symbol)
            cb_data.append(cb_input)
        
        if cb_data:
            _set_event(self.events['ohlcv'], -1)
            self.exec_callbacks(cb_data, 'ohlcv', -1)
    
    def update_positions(self, data):
        """data: [{'symbol': symbol, 'position': position}, ...]"""
        cb_data = []
        
        for d in data:
            symbol = d['symbol']
            position = d['position']
            prev = self.positions.get(symbol, {})
            
            changes = {k: (prev.get(k),new_value) for k,new_value in position.items()
                       if prev.get(k) != position[k]}
            
            self.positions[symbol] = dict(prev, **position)
            
            _set_event(self.events['position'], symbol)
            self.broadcast_event('position', symbol)
            
            cb_input = {'symbol': symbol,
                        'position': position.copy(),
                        'changes': changes}
            self.exec_callbacks(cb_input, 'position', symbol)
            cb_data.append(cb_input)
            
        _set_event(self.events['position'], -1)
        self.exec_callbacks(cb_data, 'position', -1)
                   
    def update_balances(self, balances):
        b_events = self.events['balance']
        for cy,free,used in balances:
            self.balances[cy] = {'free': free, 'used': used, 'total': free+used}
            self.balances['free'][cy] = free
            self.balances['used'][cy] = used
            self.balances['total'][cy] = free + used
            _set_event(b_events, cy)
            self.broadcast_event('balance', ('balance', cy))
        _set_event(b_events, -1)
    
    def update_balances_delta(self, deltas):
        balances2 = []
        for symbol,delta in deltas:
            try: d = self.balances[symbol]
            except KeyError: d = {'free': 0, 'used': 0, 'total': 0}
            free = d['free'] + delta
            balances2.append((symbol,free,d['used']))
        self.update_balances(balances2)
    
    
    def add_order(self, id, symbol, side, price, amount, timestamp, 
                  remaining=None, filled=None, payout=0, datetime=None, params={}):
        if remaining is None:
            remaining = amount
        if filled is None:
            filled = amount - remaining
            
        if id in self.orders:
            return self.update_order(id,remaining,filled,payout)
        
        datetime, timestamp = _resolve_times([datetime, timestamp])
              
        o = dict(
            {'id': id,
             'symbol': symbol,
             'side': side,
             'price': price,
             'amount': amount,
             'timestamp': timestamp,
             'datetime': datetime,
             'filled': filled,
             'remaining': remaining,
             'payout': payout
            }, **params)
        
        self.orders[id] = o
        if remaining:
            self.open_orders[id] = o
        
        _set_event(self.events['order'], id)
        _set_event(self.events['order'], -1)
        self.broadcast_event('order', (symbol, id))
        
        if id in self.unprocessed_fills:
            for args in self.unprocessed_fills[id]:
                tlogger.debug('{} - processing fill {} from unprocessed_fills'\
                              .format(self.name,args))
                self.add_fill(*args)
            self.unprocessed_fills.pop(id)


    def update_order(self, id, remaining=None, filled=None, payout=None, set_event=True, params={}):
        try: o = self.orders[id]
        except KeyError:
            logger2.error('{} - not recognized order: {}'.format(self.name, id))
            return
        
        amount_difference = 0 if 'amount' not in params else params['amount'] - o['amount']
        
        o.update(params)
        
        if remaining is not None:
            remaining = max(0, remaining)
            
            try: 
                #min_v = self.api.markets[o['symbol']]['limits']['amount']['min']
                precision = self.api.markets[o['symbol']]['precision']['amount']
                if precision is None: pass
                elif remaining < pow(10, (-1)*precision):
                    remaining = 0
            except KeyError:
                pass
        
        
        def modify_remaining(prev, new):
            #Order size was edited, thus the "remaining" can increase
            """if amount_difference >= 0:
                return min(prev+amount_difference, value)"""
            if amount_difference != 0:
                return new
            else:
                return min(prev, new)
        
        
        for name,value,op in [('remaining', remaining, modify_remaining),
                              ('filled', filled, max),
                              ('payout', payout, max)]:
            prev = o[name]
            if prev is not None and value is not None:
                value = op(prev, value)
            if value is not None:
                o[name] = value
                
        if not o['remaining']:
            try:
                del self.open_orders[id]
            except KeyError:
                pass

        if set_event:
            _set_event(self.events['order'], id)
            _set_event(self.events['order'], -1)
        self.broadcast_event('order', (o['symbol'], o['id']))
    
    
    def add_fill(self, id, symbol, side, price, amount, fee_mp, timestamp,
                 oid=None, payout=None, fee=None, datetime=None, params={}):
        """`symbol` and `side` can be left undefined (None) if oid is given"""
        try: 
            o_fills = self.fills[oid]
        except KeyError:
            o_fills = self.fills[oid] = {}
            
        try: o_fills[id]
        except KeyError: pass
        else: 
            tlogger.debug('{} - fill {} already registered.'.format(
                self.name,(id,symbol,side,price,amount,fee_mp,timestamp,oid)))
        
        def insert_fill(f):
            o_fills[id] = f
            _set_event(self.events['fill'], -1)
            self.broadcast_event('fill', (oid, id))
        
        if (symbol is None or side is None) and oid is None:
            raise ValueError('{} - fill id: {}. Both symbol and side must be '\
                            'defined if oid is None.'.format(self.name, id))
        
        #fee_mp is given as 'taker'/'maker'
        if isinstance(fee_mp,str):
            fee_mp = self.api.markets[symbol][fee_mp]

        fee_entry = None if fee is None else [fee, self.api.get_fee_quotation(side, as_str=False)]
        
        datetime, timestamp = _resolve_times([datetime, timestamp])

        f = dict(
            {'id': id,
             'symbol': symbol,
             'side': side,
             'price': price,
             'amount': amount,
             'fee_mp': fee_mp,
             'timestamp': timestamp,
             'datetime': datetime,
             'oid': oid,
             'fee': fee_entry
            }, **params)

        if oid is None:
            insert_fill(f)
            return
        
        try: o = self.orders[oid]
        except KeyError:
            tlogger.debug('{} - adding {} to unprocessed_fills'.format(
                self.name,(id,symbol,side,price,amount,fee_mp,timestamp,oid,payout)))
            self.unprocessed_fills[oid].append(
                (id,symbol,side,price,amount,fee_mp,timestamp,oid,payout,fee,datetime))
        else:
            if symbol is None: f['symbol'] = symbol = o['symbol']
            if side is None: f['side'] = side = o['side']
            insert_fill(f)
            if self.order['update_filled_on_fill']:
                o['filled'] += amount
            if self.order['update_payout_on_fill']:
                payout = self.api.calc_payout(symbol,o['side'],amount,price,fee_mp,fee=fee) \
                    if payout is None else payout
                o['payout'] += payout
            #if not self.has_got('order') and not self.has_got('account','order'):
            if self.order['update_remaining_on_fill']:
                self.update_order(oid,o['remaining']-amount,set_event=False)
            if any([self.order['update_filled_on_fill'], self.order['update_payout_on_fill'],
                    self.order['update_remaining_on_fill']]):
                _set_event(self.events['order'], oid)
                _set_event(self.events['order'], -1)
    
    
    def _fetch_callback_list(self, channel, symbol=None):
        try: d = self.callbacks[channel]
        except KeyError:
            raise ValueError(channel)
        if isinstance(d,dict) and symbol is None:
            raise TypeError('{} requires symbol to be specified'.format(channel))
        elif isinstance(d,list) and symbol is not None:
            raise TypeError('{} doesn\'t accept symbol'.format(channel))
        if symbol is None:
            return d
        if symbol not in d:
            d[symbol] = []
        return d[symbol]
    
    
    def add_callback(self, cb, channel, symbol=None):
        l = self._fetch_callback_list(channel, symbol)
        l.append(cb)
    
    def remove_callback(self, cb, channel, symbol=None):
        l = self._fetch_callback_list(channel, symbol)
        try: l.remove(cb)
        except ValueError: 
            return None
        else: return True
        
    def exec_callbacks(self, data, channel, symbol=None):
        callbacks = self._fetch_callback_list(channel, symbol)
        for cb in callbacks:
            cb(data)
        
        
    def fetch_data(self, s, prev_state):
        #If subsciption has been enabled, fetch all data
        # (except for orderbook, that is handled by .orderbook_maintainer)
        _map2 = {'all_tickers': 'fetch_tickers',
                 'ticker': 'fetch_ticker',
                 'trades': 'fetch_trades',
                 'ohlcv': 'fetch_ohlcv',
                 'account': 'fetch_balance',}
        
        attr = _map2.get(s.channel)
        if (s.state and not prev_state and
                attr is not None and self.has_got(attr)):
            corofunc = getattr(self, attr)
            tlogger.debug('Fetching data for {}'.format(s))
            call_via_loop(corofunc, s.id_tuple[1:2], loop=self.loop)
            
            
    def delete_data(self, s, prev_state):
        _map = {
            'all_tickers': ['tickers'],
            'ticker': ['tickers'],
            'account': ['balances','positions'],
            'orderbook': ['orderbooks'],
        }
        containers = [getattr(self, x) for x in _map.get(s.channel, [s.channel])]
        
        for container in containers:
            #All containers are assumed to be dicts or lists
            if isinstance(container, (list,deque)):
                while True:
                    try: del container[0]
                    except IndexError:
                        break
            elif not isinstance(container, dict):
                pass
            elif len(s.id_tuple) == 1:
                for x in list(container.keys()):
                    del container[x]
            else:
                delete = True
                for key in s.id_tuple[1:-1]:
                    try: container = container[key]
                    except KeyError:
                        delete = False
                        break
                if delete:
                    try: del container[s.id_tuple[-1]]
                    except KeyError: pass
            
            
    def get_markets_with_active_tickers(self, last=True, bidAsk=True):
        has_tAll = self.has_got('all_tickers')
        has_tAll_bidAsk = self.has_got('all_tickers','bid') and self.has_got('all_tickers','ask')
        has_t = self.has_got('ticker')
        has_t_bidAsk = self.has_got('ticker','bid') and self.has_got('ticker','ask')
        has_ob = self.has_got('orderbook')
        ob_sends_bidAsk = self.ob['sends_bidAsk']
        from_all_tickers = set()
        from_ticker = set()
        from_ob = set()
        if has_tAll and self.sh.is_subscribed_to({'_': 'all_tickers'}, 1):
            if not bidAsk or has_tAll_bidAsk:
                return [x for x in self.api.markets if x!='info']
            else:
                from_all_tickers.update(self.api.markets.keys())
        if has_t:
            from_ticker.update(s.params['symbol'] for s in self.sh.subscriptions 
                                if s.channel=='ticker' and s.is_active())
        if has_ob and ob_sends_bidAsk:
            from_ob.update(s.params['symbol'] for s in self.sh.subscriptions 
                            if s.channel=='orderbook' and s.is_active())
        from_tickers_combined = set(itertools.chain(from_all_tickers, from_ticker))
        with_last = from_tickers_combined.copy()
        with_bidAsk = from_ob.copy()
        if has_t_bidAsk: 
            with_bidAsk.update(from_ticker)
        if has_tAll_bidAsk:
            with_bidAsk.update(from_all_tickers)
        
        combined = itertools.chain(with_last, with_bidAsk)
        lists = {'last': with_last, 'bidAsk': with_bidAsk}
        checks = {'last': last, 'bidAsk': bidAsk}
        satisfied = (s for s in combined 
                     if all(not enabled or s in lists[n] for n,enabled in checks.items()))

        return unique(satisfied, astype=list)
            
                    
    def subscribe_to_ticker(self, symbol, params={}):
        return self.sh.add_subscription(self.ip.extend({
            '_': 'ticker',
            'symbol': self.merge(symbol)}, params))

    def unsubscribe_to_ticker(self, symbol):
        return self.sh.remove_subscription({
            '_': 'ticker',
            'symbol': symbol})
    
    def subscribe_to_all_tickers(self, params={}):
        #Fetch all tickers before enabling state
        return self.sh.add_subscription(self.ip.extend({
            '_': 'all_tickers'}, params))
    
    def unsubscribe_to_all_tickers(self):
        return self.sh.remove_subscription({
            '_': 'all_tickers'})
                
    def subscribe_to_orderbook(self, symbol, params={}):
        return self.sh.add_subscription(self.ip.extend({
            '_': 'orderbook',
            'symbol': self.merge(symbol)}, params))
    
    def unsubscribe_to_orderbook(self, symbol):
        return self.sh.remove_subscription({
            '_': 'orderbook',
            'symbol': symbol})
        
    def subscribe_to_trades(self, symbol, params={}):
        return self.sh.add_subscription(self.ip.extend({
            '_': 'trades',
            'symbol': self.merge(symbol)}, params))
        
    def unsubscribe_to_trades(self, symbol):
        return self.sh.remove_subscription({
            '_': 'trades',
            'symbol': symbol})
        
    def subscribe_to_ohlcv(self, symbol, timeframe='1m', params={}):
        return self.sh.add_subscription(self.ip.extend({
            '_': 'ohlcv',
            'symbol': self.merge(symbol),
            'timeframe': timeframe}, params))
        
    def unsubscribe_to_ohlcv(self, symbol):
        return self.sh.remove_subscription({
            '_': 'ohlcv',
            'symbol': symbol})
    
    def subscribe_to_account(self, params={}):
        return self.sh.add_subscription(self.ip.extend({
            '_': 'account'}, params))
                
    def unsubscribe_to_account(self):
        return self.sh.remove_subscription({
            '_': 'account'})
        
    def subscribe_to_match(self, symbol, params={}):
        if self.has_got('account','match'):
            if not self.sh.is_subscribed_to({'_': 'account'}):
                return self.subscribe_to_account()
        else:
            """if self.match_is_subset_of_trades and self.sh.is_subscribed_to(
                    {'_':'market', 'symbol':symbol}):
                return None"""
            return self.sh.add_subscription(self.ip.extend({
                '_': 'match',
                'symbol': self.merge(symbol)}, params))
        
    def unsubscribe_to_match(self, symbol):
        if not self.has_got('account','match'):
            return self.sh.remove_subscription({
                '_': 'match',
                'symbol': symbol})
            
    async def fetch_balance(self):
        balances = await poll.fetch(self.api,'balances',0)
        self.update_balances(
            [(cy,y['free'],y['used']) for cy,y in balances.items()
             if cy not in ('free','used','total','info')])
        return balances
    
    async def fetch_order_book(self, symbol, *limit): #limit=None,params={}
        """This method must be overriden if .orderbook_maintainer is .orderbook_maintainer
           is used to keep the nonce in sync AND the .api.fetch_order_book doesn't return
           orderbook in correct format (i.e. doesn't include (correct!) nonce value under "nonce").
           Some exchanges may offer websocket method for retrieving the full orderbook."""
        #ob = await self.api.fetch_order_book(symbol)
        if not limit: limit = [0]
        ob = await poll.fetch(self.api,('orderbook',symbol),*limit)
        ob['nonce'] = ob.get('nonce')
        return ob
    
    async def fetch_tickers(self):
        tickers = await poll.fetch(self.api,'tickers',0)
        self.update_tickers(
            [y for x,y in tickers.items() if x!='info'])
        return tickers
    
    async def fetch_ticker(self, symbol):
        ticker = await poll.fetch(self.api,('ticker',symbol),0)
        self.update_tickers([ticker])
        return ticker
    
    async def fetch_trades(self, symbol, since=None, limit=None, params={}):
        kwargs = {'since': since, 'limit': limit, 'params': params}
        trades = await poll.fetch(self.api,('trades',symbol),0,kwargs=kwargs)
        self.update_trades([{'symbol': symbol, 'trades': trades}])
        return trades
    
    async def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):
        kwargs = {'since': since, 'limit': limit, 'params': params}
        ohlcv = await poll.fetch(self.api,('ohlcv',symbol,timeframe),0,kwargs=kwargs)
        self.update_ohlcv([{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': ohlcv}])
        return ohlcv
    
    async def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}):
        """[{'id': '1a8eedf7-18cf-d779-0d15-3199ad677dc8', 'timestamp': 1564361280061,
             'datetime': '2019-07-29T00:48:00.610Z', 'lastTradeTimestamp': None,
             'symbol': 'ETH/BTC', 'type': 'limit', 'side': 'sell', 'price': 0.02201,
             'cost': 0.0, 'average': None, 'amount': 8.024, 'filled': 0.0,
             'remaining': 8.024, 'status': 'open',
             'fee': {'cost': 0.0, 'currency': 'BTC'}}]"""
        orders = await self.api.fetch_open_orders(symbol, since, limit, params)
        for o in orders:
            try:
                self.add_order(o['id'], o['symbol'], o['side'], o['price'], o['amount'],
                               o.get('timestamp'), remaining=o.get('remaining'), filled=o.get('filled'))
            except Exception as e:
                logger.exception(e)
        return orders
            
    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        amount = self.api.round_amount(symbol, amount)
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        if price is not None:
            price = self.api.round_price(symbol, price, side)
        r = await self.api.create_order(symbol, type, side, amount, price, params)
        #{'id': '0abc123de456f78ab9012345', 'symbol': 'XRP/USDT', 'type': 'limit', 'side': 'buy',
        # 'status': 'open'}
        if type=='limit' and self.order['add_automatically']:
            parsed = self.parse_ccxt_order(r)
            try: self.orders[r['id']]
            except KeyError:
                kw = {'id': r['id'], 'symbol': symbol,
                      'side': side, 'price': price, 
                      'amount': amount,'timestamp': int(time.time()*1000)}
                self.add_order(**dict(kw,**parsed.get('order',{})))
            for f in parsed.get('fills',[]):
                self.add_fill(**f)
        #TODO: add market and stop order support to self.orders
        return r
            
    async def create_limit_order(self, symbol, side, amount, price, *args):
        return await self.create_order(symbol, 'limit', side, amount, price, *args)
    
    async def create_limit_buy_order(self, symbol, amount, price, *args):
        return await self.create_order(symbol, 'limit', 'buy', amount, price, *args)
        
    async def create_limit_sell_order(self, symbol, amount, price, *args):
        return await self.create_order(symbol, 'limit', 'sell', amount, price, *args)
    
    async def cancel_order(self, id, symbol=None, params={}):
        error = None
        #tlogger.debug('Canceling order: {},{}'.format(id, symbol))
        try: r = await self.api.cancel_order(id, symbol, params)
        except ccxt.OrderNotFound as e: error = e
        cancel_automatically = self.is_order_auto_canceling_enabled()
        if cancel_automatically:
            #tlogger.debug('Automatically setting "remaining" to 0: {},{}'.format(id, symbol))
            self.update_order(id, 0)
            #tlogger.debug('Order {},{} now: {}'.format(id, symbol, self.orders.get(id)))
        if error is not None:
            raise error
        return r
    
    async def edit_order(self, id, symbol, *args):
        return await self.api.edit_order(id, symbol, *args)
    
    def parse_ccxt_order(self, r):
        return {'order': {}, 'fills': []}
    
    def is_order_auto_canceling_enabled(self):
        coa = self.order['cancel_automatically']
        if isinstance(coa,str):
            if coa == 'if-not-active':
                return not self.is_active()
            elif coa.startswith('if-not-subbed-to-'):
                sub_name = coa[len('if-not-subbed-to-'):]
                return not self.sh.is_subscribed_to({'_': sub_name}, True)
            else:
                return True
        else:
            return bool(coa)
        
    def is_order_conflicting(self, symbol, side, price):
        fill_side = as_ob_fill_side(side)
        sc = get_stop_condition(as_ob_fill_side(side), closed=False)
        return any(sc(price,o['price']) for o in self.open_orders.values() 
                        if o['symbol']==symbol and o['side']==fill_side)
    
    async def wait_on_order(self, id, cb=None, stream_deltas=[], defaults={}):
        """cb: a callback function accepting args: (order, changes); called on every update
           stream_deltas: list of order params which changes are monitored and sent through cb
           defaults: if given, changes are also sent before first update"""
        o = self.orders[id]
        e = self.events['order'][id]
        prev = {x: o[x] for x in stream_deltas}
        if defaults:
            #defaults are "previous values before this func was called"
            #if len(defaults) != len(deltas):
            changes = {x: o[x]-defaults[x] for x in defaults}  
            cb(o,changes)
        while o['remaining']:
            await e.wait()
            e.clear()
            if cb is not None:
                changes = {x: o[x]-prev[x] for x in stream_deltas}
                prev = {x: o[x] for x in stream_deltas}
                cb(o,changes)
    
    def convert_cy(self, currency, direction=1):
        #0: ex to ccxt 1: ccxt to ex
        try:
            if hasattr(currency, '__iter__') and not isinstance(currency, str):
                cls = list if not self.is_param_merged(currency) else self.merge
                return cls(self.convert_cy(_cy, direction) for _cy in currency)
            if not direction:
                return self.api.common_currency_code(currency)
            else:
                return self.api.currency_id(currency)
        finally:
            if isinstance(sys.exc_info()[1], KeyError):
                self._reload_markets(sys.exc_info()[1])
        
    def convert_symbol(self, symbol, direction=1):
        #0: ex to ccxt 1: ccxt to ex
        try:
            if hasattr(symbol, '__iter__') and not isinstance(symbol, str):
                cls = list if not self.is_param_merged(symbol) else self.merge
                return cls(self.convert_symbol(_s, direction) for _s in symbol)
            if not direction:
                return self.api.markets_by_id[symbol]['symbol']
            else:
                return self.api.markets[symbol]['id']
        finally:
            if isinstance(sys.exc_info()[1], KeyError):
                self._reload_markets(sys.exc_info()[1])
        
    def convert_cnx_params(self, params):
        def _lower(s):
            cls = list if not self.is_param_merged(s) else self.merge
            return s.lower() if isinstance(s, str) else cls(x.lower() for x in s)
        if 'symbol' in params:
            params['symbol'] = _lower(self.convert_symbol(params['symbol'], 1))
        for attr in ('cy','currency'):
            if attr in params:
                params[attr] = _lower(self.convert_cy(params[attr], 1))
        return params
    
    
    def _reload_markets(self, e=None):
        if self.reload_markets_on_keyerror is not None and \
                time.time() - self._last_markets_loaded_ts > self.reload_markets_on_keyerror:
            error_txt = '(due to error: {})'.format(repr(e)) if e is not None else ''
            logger.error('{} - force reloading markets {}'.format(self.name, error_txt))
            self._last_markets_loaded_ts = time.time()
            asyncio.ensure_future(self.api.poll_load_markets(limit=0), loop=self.loop)
    


def _resolve_auth(exchange, config):
    auth = config.pop('auth',{})
    if auth is None: auth = {}
    elif isinstance(auth, str):
        auth = {'id': auth}
    token_kwds = ['apiKey','secret'] + EXTRA_TOKEN_KEYWORDS.get(exchange, [])
        
    for k in token_kwds:
        value = ''
        if k in config:
            value = config.pop(k)
        if not auth.get(k):
            auth[k] = value if value is not None else ''
    
    """if not auth['apiKey']:
        relevant = {x:y for x,y in auth.items() if x not in token_kwds}
        auth = get_auth2(**relevant)"""
        
    return auth


def _set_event(events, id, via_loop=None, op='set'):
    if events is None: return
    try: event = events[id]
    except KeyError:
        events[id] = event = asyncio.Event(loop=via_loop)
    #print('Setting {}'.format(id))
    func = getattr(event,op)
    if via_loop is None:
        func()
    else:
        via_loop.call_soon_threadsafe(func)
#TODO: if cy/symbol/id not found, force self.api to reload-markets


def _new_only(new_seq, prev_seq, timestamp_key, sort=False):
    """For this to be effective new_seq (and prev_seq) must be sorted"""
    if sort:
        new_seq = sorted(new_seq, key=lambda x: x[timestamp_key])
    try:
        last_ts = prev_seq[-1][timestamp_key]
    except IndexError:
        pass
    else:
        from_i = next((i for i,x in enumerate(new_seq) if x[timestamp_key]>last_ts), len(new_seq))
        if from_i:
            new_seq = new_seq[from_i:]
            
    return new_seq
