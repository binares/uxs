import sys
import copy as _copy
import asyncio
import itertools
import time
import ccxt
from collections import defaultdict
import datetime
dt = datetime.datetime
td = datetime.timedelta

from ..auth import get_auth2, EXTRA_TOKEN_KEYWORDS
from ..ccxt import get_exchange
from .. import poll
from .orderbook import OrderbookMaintainer
from .errors import (ExchangeSocketError, ConnectionLimit)
from uxs.fintls.basics import (as_ob_fill_side, as_direction)
from uxs.fintls.ob import (get_stop_condition, create_orderbook, update_branch, assert_integrity)
from wsclient import WSClient

import fons.log
from fons.aio import call_via_loop
from fons.iter import unique

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)



class ExchangeSocket(WSClient):
    """Override these methods: handle, encode, auth
       May also be needed to override: create_connection_url
       Some exchanges provide socket methods: create_order, cancel_order, fetch_balance
       And attributes of your choosing."""
       
    exchange = None
    
    #Used on .subscribe_to_{x} , raises ValueError when doesn't have
    has = dict.fromkeys([
         'all_tickers', 'ticker', 'orderbook', 'market',
         'account', 'match', 'balance',
         'fetch_balance', 'fetch_tickers', 'fetch_ticker',
         'fetch_order_book'
        ], False)
    
    channel_defaults = {
        'cnx_params_converter': 'm$convert_cnx_params',
    }
    #If 'is_private' is set to True, .sign() is called on the specific output to server
    #if 'ubsub' is set to False, .remove_subscription raises ExchangeSocketError on that channel
    channels = {
        'account': {
            'required': [],
            'is_private': True,
            #In channel_defaults this is set to True
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
        'market': {
            'required': ['symbol'],
        },
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
        'assert_integrity': False,
        #whether or not bid and ask of ticker if modified on orderbook update
        'sends_bidAsk': False,
    }
    order = {
        #these two don't matter if the websocket itself
        # receives new order/cancel order updates
        'add_automatically': True,
        'cancel_automatically': True,
        #almost all send left. some send executed.
        'update_left_on_trade': False,
        'update_executed_on_trade': False,
        #usually payouts are not sent with order updates
        #automatically calculate it from each trade
        'update_payout_on_trade': True,
    }

    match_is_subset_of_market = False
    reload_markets_on_keyerror = 150
    
    #since socket interpreter may clog due to high CPU usage,
    # maximum lengths for the queues are set, and if full,
    # previous items are removed one-by-one to make room for new ones
    queue_lengths = {
        'balance': 100, 
        'ticker': 1000, 
        'order': 100, 
        'trade': 100, 
        'orderbook': 1000,
        'market': 10000,
        'event': 1000,
        'received': 1000,
        'to_send': 100,
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
         ['fetch_orderbook','fetch_order_book'],]
    
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
        
        names = ['ticker','orderbook','market','balance','trade','order','empty','recv']
        #query_names = ['fetch_order_book','fetch_ticker','fetch_tickers','fetch_balance']
        self.events = {x: {-1: asyncio.Event()} for x in names}
        self.events['empty'][-1] = self.station.get_event('empty',0,0)
        self.events['recv'][-1] = self.station.get_event('recv',0,0)
        self._init_events()
        
        self.callbacks = {'orderbook': {}}
        self.tickers = {}
        self.balances = {'free': {}, 'used': {}, 'total': {}}
        self.orderbooks = {}
        self.trades = []
        self.unprocessed_trades = defaultdict(list)
        self.orders = {}
        self.open_orders = []
        self.closed_orders = []
        
        self.orderbook_maintainer = self.OrderbookMaintainer_cls(self)
        self._last_markets_loaded_ts = 0
        #deep_update(self.merge_subs, {n:{} for n in ['ticker','orderbook','market']})
    
    
    async def _init_api_attrs(self):
        #if not getattr(self.api,'markets',None):
        limit = self.api_attr_limits['markets']
        await self.api._set_markets(limit)
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
            for x in ['ticker','orderbook']:
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
        for d in data:
            #subscribe_to_all_tickers may suddenly receive new markets when they are added
            # to the exchange. Ignore them because we don't want to cause KeyError or other unexpectancies.
            symbol = d['symbol']
            if symbol not in self.api.markets:
                continue
            self.tickers[symbol] = d
            _set_event(ticker_events, symbol)
            self.broadcast_event('ticker', symbol)
        _set_event(ticker_events, -1)
        self.api.tickers = self.tickers
    
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
            if 'nonce' in symbol_changes:
                self.orderbooks[symbol]['nonce'] = symbol_changes['nonce']
            _set_event(ob_events, symbol)
            self.broadcast_event('orderbook',
                                 (symbol, len(symbol_changes['bids']), len(symbol_changes['asks'])))
            if sends and (not only_if_not_subbed or not all_subbed and not is_subbed(symbol)):
                self._update_ticker_from_ob(symbol,set_ticker_event)
                
            callbacks = self.callbacks['orderbook'].get(symbol, [])
            cb_input = {'symbol': symbol,
                        'bids': list(uniq_bid_changes.values()), 
                        'asks': list(uniq_ask_changes.values()),}
            for cb in callbacks:
                cb(cb_input)
            if self.ob['assert_integrity']:
                assert_integrity(self.orderbooks[symbol])
        _set_event(ob_events, -1)
        
    def _update_ticker_from_ob(self, symbol, set_event=True):
        try: d = self.tickers[symbol]
        except KeyError: d = self.tickers[symbol] = self.api.ticker_entry(symbol)
        for side in ('bid','ask'):
            try: d.update(dict(zip([side,side+'Volume'],
                                   self.orderbooks[symbol][side+'s'][0])))
            except IndexError: pass
        if set_event:
            try: e = self.events['ticker'][symbol]
            except KeyError: e = self.events['ticker'][symbol] = asyncio.Event()
            e.set()
            self.events['ticker'][-1].set()
            self.broadcast_event('ticker', symbol)
                   
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
                
    def add_order(self, id, symbol, side, rate, qty, ts, 
                  left=None, executed=None, payout=0):
        if left is None: left = qty
        if executed is None:
            executed = qty - left
        left = max(0,left)
        if not left:
            try: self.get_order(id,'closed')
            except ValueError: pass
            else: 
                self.update_order(id,left,executed,payout)
                return
        to = self.open_orders if left else self.closed_orders
        to.insert(0,{'id':id,'symbol':symbol,'side':side,
                     'rate':rate,'qty':qty,'ts':ts,
                     'executed': executed, 'left':left,
                     'payout': payout})
        self.orders[id] = to[0]
        self.events['order'][id] = oe = asyncio.Event()
        oe.set()
        self.events['order'][-1].set()
        self.broadcast_event('order', (symbol, id))
        if id in self.unprocessed_trades:
            for args in self.unprocessed_trades[id]:
                try: self.get_trade(id)
                except ValueError:
                    tlogger.debug('{} - processing trade {} from unprocessed_trades'\
                                  .format(self.name,args))
                    self.add_trade(*args)
            self.unprocessed_trades.pop(id)

    def update_order(self, id, left, executed=None, payout=None, set_event=True):
        try: o = self.get_order(id)
        except ValueError:
            logger2.error('{} - not recognized order: {}'.format(self.name,id))
            return
            
        left = max(0,left)
        try: 
            #min_v = self.api.markets[o['symbol']]['limits']['amount']['min']
            precision = self.api.markets[o['symbol']]['precision']['amount']
            if precision is None: pass
            elif left < pow(10, (-1)*precision):
                left = 0
        except KeyError:
            pass
        prev_left = o['left']
        o['left'] = left
        if prev_left and not left:
            try: i = next(i for i,x in enumerate(self.open_orders) if x['id']==id)
            except StopIteration: pass
            else: self.open_orders.pop(i)
            self.closed_orders.insert(0,o)
        if executed is not None:
            o['executed'] = executed
        if payout is not None:
            o['payout'] = payout
        if set_event:
            self.events['order'][id].set()
            self.events['order'][-1].set()
        self.broadcast_event('order', (o['symbol'], o['id']))
    
    def add_trade(self, id, symbol, side, rate, qty, fee_mp, ts, oid=None, payout=None, fee=None):
        """`symbol` and `side` can be left undefined (None) if oid is given"""
        try: self.get_trade(id)
        except ValueError: pass
        else: 
            tlogger.debug('{} - trade {} already registered.'.format(
                self.name,(id,symbol,side,rate,qty,fee_mp,ts,oid)))

        def insert_trade(d):
            self.trades.insert(0,d)
            self.events['trade'][-1].set()
            self.broadcast_event('trade', (oid, id))
        
        if (symbol is None or side is None) and oid is None:
            raise ValueError('{} - trade id: {}. Both symbol and side must be '\
                            'defined if oid is None.'.format(self.name, id))
        
        #fee_mp is given as 'taker'/'maker'
        if isinstance(fee_mp,str):
            fee_mp = self.api.markets[symbol][fee_mp]

        fee_entry = None if fee is None else [fee, self.api.get_fee_quotation(side, as_str=False)]

        d = {'id': id, 'symbol': symbol, 'side': side,
             'rate': rate, 'qty': qty, 'fee_mp': fee_mp,
             'ts': ts, 'oid': oid, 'fee': fee_entry}

        if oid is None:
            insert_trade(d)
            return
        
        try: o = self.get_order(oid)
        except ValueError:
            tlogger.debug('{} - adding {} to unprocessed_trades'.format(
                self.name,(id,symbol,side,rate,qty,fee_mp,ts,oid,payout)))
            self.unprocessed_trades[oid].append((id,symbol,side,rate,qty,fee_mp,ts,oid,payout,fee))
        else:
            if symbol is None: d['symbol'] = symbol = o['symbol']
            if side is None: d['side'] = side = o['side']
            insert_trade(d)
            if self.order['update_executed_on_trade']:
                o['executed'] += qty
            if self.order['update_payout_on_trade']:
                payout = self.api.calc_payout(symbol,o['side'],qty,rate,fee_mp,fee=fee) \
                    if payout is None else payout
                o['payout'] += payout
            #if not self.has_got('order') and not self.has_got('account','order'):
            if self.order['update_left_on_trade']:
                self.update_order(oid,o['left']-qty,set_event=False)
            if any([self.order['update_executed_on_trade'], self.order['update_payout_on_trade'],
                    self.order['update_left_on_trade']]):
                self.events['order'][oid].set()
                self.events['order'][-1].set()
            
    def get_order(self, id, status=None):
        _ = ('open_orders','closed_orders') if status is None else (status+'_orders',)
        for attr in _:
            try: return next(x for x in getattr(self,attr) if x['id']==id)
            except StopIteration: pass    
        raise ValueError('No match for order id {} in {}'.format(id,_))
    
    def get_trade(self, id):
        try: return next(x for x in self.trades if x['id']==id)
        except StopIteration:
            raise ValueError('No match for trade id: {}'.format(id))
        
    def fetch_order_trades(self, oid):
        return [x for x in self.trades if x['oid']==oid]
    
    
    def _fetch_callback_list(self, type, symbol=None):
        try: d = self.callbacks[type]
        except KeyError:
            raise ValueError(type)
        if isinstance(d,dict) and symbol is None:
            raise TypeError('{} requires symbol to be specified'.format(type))
        elif isinstance(d,list) and symbol is not None:
            raise TypeError('{} doesn\'t accept symbol'.format(type))
        if symbol is None:
            return d
        if symbol not in d:
            d[symbol] = []
        return d[symbol]
    
    
    def add_callback(self, cb, type, symbol=None):
        l = self._fetch_callback_list(type, symbol)
        l.append(cb)
    
    def remove_callback(self, cb, type, symbol=None):
        l = self._fetch_callback_list(type, symbol)
        try: l.remove(cb)
        except ValueError: 
            return None
        else: return True
        
        
    def fetch_data(self, s, prev_state):
        #If subsciption has been enabled, fetch all data
        # (except for orderbook, that is handled by .orderbook_maintainer)
        _map2 = {'all_tickers': 'fetch_tickers',
                 'ticker': 'fetch_ticker',
                 'account': 'fetch_balance'}
        
        attr = _map2.get(s.channel)
        if (s.state and not prev_state and
                attr is not None and self.has_got(attr)):
            corofunc = getattr(self, attr)
            tlogger.debug('Fetching data for {}'.format(s))
            call_via_loop(corofunc, s.id_tuple[1:2], loop=self.loop)
            
            
    def delete_data(self, s, prev_state):
        _map = {'all_tickers': 'tickers', 'ticker': 'tickers',
                'account': 'balances', 'orderbook': 'orderbooks',}
        container = getattr(self, _map.get(s.channel, s.channel))
        #if isinstance(container,dict):
        if len(s.id_tuple) == 1:
            for x in list(container.keys()):
                del container[x]
        else:
            try: del container[s.id_tuple[1]]
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
        
    def subscribe_to_market(self, symbol, params={}):
        return self.sh.add_subscription(self.ip.extend({
            '_': 'market',
            'symbol': self.merge(symbol)}, params))
        
    def unsubscribe_to_market(self, symbol):
        return self.sh.remove_subscription({
            '_': 'market',
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
            """if self.match_is_subset_of_market and self.sh.is_subscribed_to(
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
        return self.balances
    
    async def fetch_order_book(self, symbol, *limit): #limit=None,params={}
        """This method must be overriden if .orderbook_maintainer is .orderbook_maintainer
           is used to keep the nonce in sync AND the .api.fetch_order_book doesn't return
           orderbook in correct format (i.e. doesn't include (correct!) nonce value under "nonce").
           Some exchanges may offer websocket method for retrieving the full orderbook."""
        #ob = await self.api.fetch_order_book(symbol)
        if not limit: limit = [0]
        ob = await poll.fetch(self.api,('orderbook',symbol),*limit)
        ob = _copy.deepcopy(ob)
        ob['nonce'] = ob.get('nonce')
        return ob
    
    async def fetch_tickers(self):
        tickers = await poll.fetch(self.api,'tickers',0)
        tickers = _copy.deepcopy(tickers)
        self.update_tickers(
            [y for x,y in tickers.items() if x!='info'])
        return self.tickers
    
    async def fetch_ticker(self, symbol):
        ticker = await poll.fetch(self.api,('ticker',symbol),0)
        ticker = _copy.deepcopy(ticker)
        self.update_tickers([ticker])
        return self.tickers[symbol]
    
    async def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}):
        r = await self.api.fetch_open_orders(symbol, since, limit, params)
        return r
            
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
            try: self.get_order(r['id'])
            except ValueError:
                kw = {'id': r['id'],'symbol': symbol,
                      'side': side,'rate': price, 
                      'qty': amount,'ts': int(time.time()*1000)}
                self.add_order(**dict(kw,**parsed.get('order',{})))
            for t in parsed.get('trades',[]):
                self.add_trade(**t)
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
            #tlogger.debug('Automatically setting "left" to 0: {},{}'.format(id, symbol))
            self.update_order(id, 0)
            #tlogger.debug('Order {},{} now: {}'.format(id, symbol, self.orders.get(id)))
        if error is not None:
            raise error
        return r
    
    def parse_ccxt_order(self, r):
        return {'order': {}, 'trades': []}
    
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
        return any(sc(price,o['rate']) for o in self.open_orders 
                        if o['symbol']==symbol and o['side']==fill_side)
    
    async def wait_on_order(self, id, cb=None, stream_deltas=[], defaults={}):
        """cb: a callback function accepting args: (order, changes); called on every update
           stream_deltas: list of order params which changes are monitored and sent through cb
           defaults: if given, changes are also sent before first update"""
        o = self.get_order(id)
        e = self.events['order'][id]
        prev = {x: o[x] for x in stream_deltas}
        if defaults:
            #defaults are "previous values before this func was called"
            #if len(defaults) != len(deltas):
            changes = {x: o[x]-defaults[x] for x in defaults}  
            cb(o,changes)
        while o['left']:
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
            asyncio.ensure_future(self.api._set_markets(limit=0), loop=self.loop)
    


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
