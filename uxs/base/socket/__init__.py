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
from ..ccxt import init_exchange, _ccxtWrapper
from .. import poll
from .orderbook import OrderbookMaintainer
from .errors import (ExchangeSocketError, ConnectionLimit)
from uxs.fintls.basics import (as_ob_fill_side, as_direction)
from uxs.fintls.ob import (get_stop_condition, create_orderbook, update_branch, assert_integrity)
from uxs.fintls.utils import resolve_times
from wsclient import WSClient

import fons.log
from fons.aio import call_via_loop
from fons.dict_ops import deep_update
from fons.func import get_arg_count
from fons.iter import unique, sequence_insert

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class ExchangeSocket(WSClient):
    """
    Override these methods: handle, encode, sign, setup_test_env
    May want to override: on_start, extract_message_id, extract_errors, create_error_args
    And attributes of your choosing.
    """
    
    # Class name
    exchange = None
    
    # If True, .setup_test_env(config) will be called on init
    test = False
    
    # Used on .subscribe_to_{x} , raises ValueError when doesn't have
    has = dict.fromkeys([
         'all_tickers', 'ticker', 'orderbook', 'trades',
         'ohlcv', 'account', 'match', 'balance',
         'fetch_tickers', 'fetch_ticker', 'fetch_order_book',
         'fetch_trades', 'fetch_ohlcv', 'fetch_balance',
         'fetch_orders', 'fetch_order', 'fetch_open_orders',
         'create_order', 'edit_order', 'cancel_order',
        ], False)
    
    channel_defaults = {
        'url': '<$ws>',
        'cnx_params_converter': 'm$convert_cnx_params',
        'cnx_params_converter_config': {
            'currency_aliases': [],
            'lower': {'symbol': True, 'currency': True},
            'include': {'base_cy': False, 'quote_cy': False},
            '__currency_aliases': ['currency','base_cy','quote_cy'],
        },
        'delete_data_on_unsub': True,
        # load markets before allowing adding subscriptions
        'on_creation': 'm$sn_load_markets',
    }
    # If 'is_private' is set to True, .sign() is called on the specific output to server
    # if 'ubsub' is set to False, .remove_subscription raises ExchangeSocketError on that channel
    channels = {
        # Subscriptions
        'account': {
            'required': [],
            'is_private': True,
            # In channel_defaults this is set to True
            # If set to True then it deletes "balances" and "positions"
            # when unsubscribed (including lost connection)
            'delete_data_on_unsub': False,
            'merge_option': False,
        },
        'match': {
            'required': ['symbol'],
            'is_private': True,
        },
        'all_tickers': {
            'required': [],
            'merge_option': False,
        },
        'ticker': {
            'required': ['symbol'],
        },
        'orderbook': {
            'required': ['symbol'],
            'auto_activate': False,
        },
        'trades': {
            'delete_data_on_unsub': False,
            'required': ['symbol'],
        },
        'ohlcv': {
            'delete_data_on_unsub': False,
            'required': ['symbol','timeframe'],
        },
        # Fetch
        'fetch_tickers': {
            'type': 'fetch',
            'required': ['symbols'],
            'is_private': False,
        },
        'fetch_ticker': {
            'type': 'fetch',
            'required': ['symbol'],
            'is_private': False,
        },
        'fetch_order_book': {
            'type': 'fetch',
            'required': ['symbol','limit'],
            'is_private': False,
        },
        'fetch_ohlcv': {
            'type': 'fetch',
            'required': ['symbol','timeframe'],
            'is_private': False,
        },
        'fetch_trades': {
            'type': 'fetch',
            'required': ['symbol'],
            'is_private': False,
        },
        'fetch_balance': {
            'type': 'fetch',
            'required': [],
            'is_private': True,
        },
        'fetch_order': {
            'type': 'fetch',
            'required': ['id','symbol'],
            'is_private': True,
        },
        'fetch_orders': {
            'type': 'fetch',
            'required': ['symbol', 'since', 'limit'],
            'is_private': True,
        },
        'fetch_open_orders': {
            'type': 'fetch',
            'required': ['symbol','since', 'limit'],
            'is_private': True,
        },
        'fetch_my_trades': {
            'type': 'fetch',
            'required': ['symbol', 'since', 'limit'],
            'is_private': True,
        },
        # Post
        'create_order': {
            'type': 'post',
            'required': ['symbol','type','side','amount','price'],
            'is_private': True,
        },
        'edit_order': {
            'type': 'post',
            'required': ['id','symbol','type','side','amount','price'],
            'is_private': True,
        },
        'cancel_order': {
            'type': 'post',
            'required': ['id','symbol'],
            'is_private': True,
        },
    }
    ORDER_KEYWORDS = (
        'id', 'symbol', 'side', 'price', 'amount', 'timestamp', 
        'remaining', 'filled', 'payout', 'datetime',
        'type', 'stop', 'cost', 'average',
    )
    FILL_KEYWORDS = (
        'id', 'symbol', 'side', 'price', 'amount', 'timestamp',
        'datetime', 'order', 'type', 'payout',
        'fee', 'fee_rate', 'takerOrMaker', 'cost',
    )
    TIMEFRAMES = (
        '1m','3m','5m','15m','30m',
        '1h','2h','4h','6h','8h','12h',
        '1d','3d',
        '1w',
        '1M',
    )
    symbol = {
        # List of quote currency ids
        'quote_ids': [],
        # By which a symbol id is separated
        'sep': '/',
        # Usually symbol id starts with base, but some exchanges start with quote
        'startswith': 'base',
        # Default 'force' arg value in .convert_symbol()
        # Forcing into exchange-id (direction=1) is less reliable, disable it
        'force': {0: True, 1: False},
        # If KeyError occurs in .convert_symbol(), markets are automatically reloaded
        # by this interval. Set it to `None` to not reload markets.
        'reload_markets_on_KeyError': 150,
    }
    exceptions = {
        '__default__': ccxt.ExchangeError,
    }
    # Optional, for reference in .encode
    channel_ids = {}
    
    OrderbookMaintainer_cls = OrderbookMaintainer
    
    ob = {
        # the time after which all not-already-auto-created
        # orderbooks (on 1st .send_update) will be force created
        # on (each) connection (re-)activation. Set to `None` to disable.
        'force_create': 15,
        # minimal interval between two reload (fetch) attempts (corrupted ob)
        'reload_interval': 15,
        # minimal interval between two restart (unsub, resub) attempts (corrupted ob)
        'restart_interval': 15,
        # the number of latest updates to be kept in cache
        'cache_size': 50,
        'uses_nonce': True,
        'receives_snapshot': False,
        # what to do when update's nonce is unsynced
        'on_unsync': None, # reload (re-fetch) / restart (unsub-resub)
        # what to do when the side of update's price can't be determined
        'on_unassign': None, # reload / restart
        'purge_cache_on_create': False,
        'assert_integrity': False,
        # whether or not bid and ask of ticker if modified on orderbook update
        'sends_bidAsk': False,
    }
    order = {
        # these two don't matter if the websocket itself
        # receives new order/cancel order updates
        'add_automatically': True,
        'cancel_automatically': True,
        # almost all send remaining. some send filled.
        'update_remaining_on_fill': False,
        'update_filled_on_fill': False,
        # usually payouts are not sent with order updates
        # automatically calculate it from each fill
        'update_payout_on_fill': True,
    }
    # Will be extended by .api.timeframes
    timeframes = {}
    trade = {
        'sort_by': lambda x: int(x['id']),
    }
    store = {
        'trades': 1000,
        'ohlcv': 1000,
    }
    match_is_subset_of_trades = False
    
    # since socket interpreter may clog due to high CPU usage,
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
    
    # The accepted age for retrieving cached markets,currencies,balances etc...
    fetch_limits = {
        'markets': None,
        'balances': None,
    }
    __properties__ = \
        [['balance','balances'],
         ['ob_maintainer','orderbook_maintainer'],
         ['apiKey','api.apiKey'],
         ['secret','api.secret'],
         ['auth_info','api._auth_info'],
         ['profile','api._profile_name'],
         ['markets','api.markets'],
         ['currencies','api.currencies'],
         ['convert_currency','convert_cy'],
         ['convert_tf','convert_timeframe'],
         ['fetch_orderbook','fetch_order_book'],
         ['subscribe_to_ob','subscribe_to_orderbook'],
         ['unsubscribe_to_ob','unsubscribe_to_orderbook'],
         ['sub_to_ob','subscribe_to_orderbook'],
         ['unsub_to_ob','unsubscribe_to_orderbook'],
         ['subscribe_to_L2','subscribe_to_orderbook'],
         ['unsubscribe_to_L2','unsubscribe_to_orderbook'],
         ['sub_to_L2','subscribe_to_orderbook'],
         ['unsub_to_L2','unsubscribe_to_orderbook'],
         ['fetchTicker','fetch_ticker'],
         ['fetchTickers','fetch_tickers'],
         ['fetchOrderBook','fetch_order_book'],
         ['fetchOHLCV','fetch_ohlcv'],
         ['fetchMyTrades','fetch_my_trades'],
         ['fetchBalance','fetch_balance'],
         ['fetchOrder','fetch_order'],
         ['fetchOrders','fetch_orders'],
         ['fetchMyTrades','fetch_my_trades'],
         ['createOrder','create_order'],
         ['editOrder','edit_order'],
         ['cancelOrder','cancel_order'],]
    
    __extend_attrs__ = [
        'fetch_limits',
        'channel_ids',
        'ob',
        'order',
        'store',
        'symbol',
        'trade',
        'timeframes',
    ]
    __deepcopy_on_init__ = __extend_attrs__[:]
    
    
    def __init__(self, config={}):
        """:type api: _ccxtWrapper"""
        config = config.copy()
        
        is_test = 'test' in config and config['test'] or 'test' not in config and self.test
        if is_test:
            _args = [config] if get_arg_count(self.setup_test_env) else []
            _config = self.setup_test_env(*_args)
            if _config is not None:
                deep_update(config, _config)
        
        auth = _resolve_auth(self.exchange, config, test=is_test)
        ccxt_config = config.pop('ccxt_config', {}).copy()
        load_cached_markets = config.pop('load_cached_markets', None)
        profile = config.pop('profile', None)
        ccxt_test = config.pop('ccxt_test', is_test)
        
        super().__init__(config)
        
        if load_cached_markets is None:
            load_cached_markets = self.fetch_limits['markets']
        
        # asynchronous ccxt Exchange instance
        self.api = init_exchange({
            'exchange':self.exchange,
            'async':True,
            'args': (ccxt_config,),
            'kwargs': {
                'load_cached_markets': load_cached_markets,
                'profile': profile,
                'test': ccxt_test,},
            'auth': auth,
            'add': False,
        })
        
        # synchronous ccxt Exchange instance
        self.snapi = init_exchange({
            'exchange':self.exchange,
            'async':False,
            'args': (ccxt_config,),
            'kwargs': {
                'load_cached_markets': False,
                'profile': profile,
                'test': ccxt_test,},
            'auth': auth,
            'add': False,
        })
        
        self._init_has()
        self._init_exceptions()
        
        # Some methods do not assume that a markets / currencies etc value
        # can be `None`. Replace with empty dicts / lists instead.
        self.api._ensure_no_nulls()
        
        self.api.sync_with_other(self.snapi)
        
        names = ['ticker','orderbook','trades','ohlcv','balance',
                 'fill','order','position','empty','recv']
        #query_names = ['fetch_order_book','fetch_ticker','fetch_tickers','fetch_balance']
        self.events = defaultdict(lambda: defaultdict(lambda: asyncio.Event(loop=self.loop)))
        for n in names:
            self.events[n][-1]
        self.events['empty'][-1] = self.station.get_event('empty',0,0)
        self.events['recv'][-1] = self.station.get_event('recv',0,0)
        self._init_events()
        
        self.callbacks = {x: {} for x in ['orderbook','ticker','trades',
                                          'ohlcv','position','balance',
                                          'order','fill']}
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
        
        # These are backups of the above to ensure that the id()-s of the content
        # objects are never changed. As soon as `del` op is envoked
        # (sub becomes inactive: ticker, orderbook) on one of the content items,
        # it is restored (in cleared form) from the backup on the next
        # .update_tickers / .update_orderbooks
        for attr in ('tickers','balances','orderbooks','positions'):
            setattr(self, '_'+attr, defaultdict(dict))
        self._balances.update(self.balances)
        
        self.orderbook_maintainer = self.OrderbookMaintainer_cls(self)
        self._last_markets_loaded_ts = 0
    
    
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
    
    
    def _init_has(self):
        def to_camelcase(x):
            spl = x.split('_')
            return spl[0].lower() + ''.join(w.lower().capitalize() for w in spl[1:])
        
        map = {'fetch_ohlcv': 'fetchOHLCV'}
        
        for x in ['fetch_tickers', 'fetch_ticker', 'fetch_order_book',
                  'fetch_trades', 'fetch_ohlcv', 'fetch_balance',
                  'fetch_orders', 'fetch_order', 'fetch_open_orders',
                  'create_order', 'edit_order', 'cancel_order']:
            camel = map.get(x, to_camelcase(x))
            ccxt_has = self.api.has[camel]
            self_has = self.has_got(x)
            any_has = ccxt_has or self_has
            
            if not isinstance(self.has[x], dict):
                self.has[x] = {'_': any_has}
            else:
                self.has[x]['_'] = any_has
            
            if 'ccxt' not in self.has[x]:
                self.has[x]['ccxt'] = ccxt_has
            
            self.has[camel] = _copy.deepcopy(self.has[x])
        
        for x in ['balance','order','fill','position']:
            self.has[x] = self.has_got('account', x)
        
        # OHLCV and timeframes
        deep_update(self.has, 
                    {'ohlcv': {'_': self.has_got('ohlcv'), 'timeframes': {}}})
        
        api_timeframes = getattr(self.api, 'timeframes', None)
        if api_timeframes is None:
            api_timeframes = {}
        
        if self.has_got('ohlcv'):
            self.timeframes.update(
                {x: y for x,y in api_timeframes.items() if x not in self.timeframes})
            self.timeframes = {x: y for x,y in self.timeframes.items() if y!='__del__'}
            self.has['ohlcv']['timeframes'].update(
                dict.fromkeys(list(self.timeframes), True))
        
        # '__del__' identifies timeframes that are present at ccxt Exchange but not in websocket
        self.timeframes = {x: y for x,y in self.timeframes.items() if y!='__del__'}
    
    
    def _init_exceptions(self):
        ccxt_e = self.api.exceptions
        exclude = self.exceptions.get('__exclude__')
        
        if exclude is None:
            pass
        elif exclude in ('all', True):
            ccxt_e = {}
        elif hasattr(exclude, '__iter__'):
            exclude = list(exclude) 
            ccxt_e = {k: v for k,v in ccxt_e if k not in exclude}
        
        self.exceptions = dict(ccxt_e, **self.exceptions)
    
    
    def setup_test_env(self, *args):
        """
        Modify the config, e.g. change channel urls to test urls.
        If has args, config will be passed as first argument. In that case the config MAY be modified.
        Or you can return a new dict that will deep extend the config.
        """
        if not self.url_components.get('test'):
            raise NotImplementedError('{} - test env is not implemented yet'.format(self.__class__.__name__))
        
        return {
            'url_components': {
                'ws': self.url_components['test'],
                'ws_original': self.url_components['ws'],
            },
        }
    
    
    async def load_markets(self, reload=False, limit=None):
        if not reload and self.api.markets:
            return self.api.markets
        if limit is None:
            limit = self.fetch_limits['markets']
        await self.api.poll_load_markets(limit)
        self._init_events()
        
        return self.api.markets
    
    
    def sn_load_markets(self, reload=False, limit=None):
        if not reload and self.snapi.markets:
            return self.snapi.markets
        if limit is None:
            limit = self.fetch_limits['markets']
        self.snapi.poll_load_markets(limit)
        self._init_events()
        
        return self.snapi.markets
    
    
    async def on_start(self):
        # use synchronous function to block any simultaneous loading
        # (e.g. when a subscription is created the first time),
        # which would result in double fetch
        self.sn_load_markets()
    
    
    def notify_unknown(self, r, max_chars=500):
        print('{} - unknown response: {}'.format(self.name, str(r)[:max_chars]))
    
    
    def check_errors(self, r):
        pass
    
    
    def update_tickers(self, data, *, action='update', set_event=True, enable_sub=False):
        """
        :param data:
            [
                {
                 'symbol': 'ETH/BTC', 'timestamp': 1546177530977, 'datetime': '2018-12-30T13:45:30.977Z',
                 'high': 0.037441, 'low': 0.034593, 'bid': 0.036002, 'bidVolume': 0.416,
                 'ask': 0.036016, 'askVolume': 0.052, 'vwap': 0.03580656,
                 'open': 0.035201, 'close': 0.036007, 'last': 0.036007, 'previousClose': 0.035201,
                 'change': 0.000806, 'percentage': 2.29, 'average': None,
                 'baseVolume': 370410.985, 'quoteVolume': 13263.14351447,
                },
                ...
            ]
        :param update:
            'update': add/overwrite prev dict values
            'replace': replace prev dict entirely
        """
        enable_individual = bool(enable_sub) and enable_sub!='all_tickers'
        enable_all = enable_sub=='all_tickers'
        cb_data = []
        
        for d in data:
            #subscribe_to_all_tickers may suddenly receive new markets when they are added
            # to the exchange. Ignore them because we don't want to cause KeyError or other unexpectancies.
            symbol = d['symbol']
            if symbol not in self.api.markets:
                continue
            try:
                prev = self.tickers[symbol]
                changes = self.dict_changes(d, prev)
                if action != 'update':
                    self.dict_clear(prev)
            except KeyError:
                prev = self.dict_clear(self._tickers[symbol])
                changes = d
            
            self.tickers[symbol] = self.dict_update(d, prev)
            
            if enable_individual:
                self.change_subscription_state(('ticker', symbol), 1, True)
            
            if set_event:
                self.safe_set_event('ticker', symbol, {'_': 'ticker', 'symbol': symbol})
            
            cb_input = {'_': 'ticker',
                        'symbol': symbol,
                        'data': changes}
            self.exec_callbacks(cb_input, 'ticker', symbol)
            cb_data.append(cb_input)
        
        self.api.tickers = self.tickers
        if cb_data:
            if enable_all:
                self.change_subscription_state(('all_tickers',), 1, True)
            if set_event:
                self.safe_set_event('ticker', -1)
            self.exec_callbacks(cb_data, 'ticker', -1)
    
    
    def create_orderbooks(self, data, *, set_event=True, enable_sub=False): 
        """
        :param data:
            [
                {
                 'symbol': <str>,
                 'bids': [[bprice0,bqnt0],...],
                 'asks': [[aprice0,aqnt0],...],
                 'nonce': <int> or None]}
                },
                ...
            ]
        """
        for ob in data:
            symbol = ob['symbol']
            self.orderbooks[symbol] = new = \
                self.orderbook_maintainer._deep_overwrite(create_orderbook(ob))
            if enable_sub:
                self.change_subscription_state(('orderbook', symbol), 1, True)
            if set_event:
                self.safe_set_event('orderbook', symbol, {'_': 'orderbook',
                                                          'symbol': symbol,
                                                          'bids_count': len(new['bids']),
                                                          'aks_count': len(new['asks'])})
            if self.ob['assert_integrity']:
                assert_integrity(self.orderbooks[symbol])
        if data and set_event:
            self.safe_set_event('orderbook', -1)
    
    
    def update_orderbooks(self, data, *, set_event=True, enable_sub=False):
        """
        :param data:
            [
                {
                 'symbol': <str>,
                 'bids': [[bprice0,bqnt0],...],
                 'asks': [[aprice0,aqnt0],...],
                 'nonce': <int> or None]}
                },
                ...
            ]
        """
        sends = self.ob['sends_bidAsk']
        all_subbed = self.is_subscribed_to({'_': 'all_tickers'}, 1)
        does_send = sends and (not isinstance(sends, dict) or '_' not in sends or sends['_'])
        only_if_not_subbed = isinstance(sends,dict) and sends.get('only_if_not_subbed_to_ticker')
        set_ticker_event = not isinstance(sends,dict) or sends.get('set_ticker_event', True)
        is_subbed = lambda symbol: self.is_subscribed_to({'_':'ticker','symbol':symbol}, 1)
        cb_data = []
        update_tickers = []
        
        for d in data:
            symbol = d['symbol']
            uniq_bid_changes,uniq_ask_changes = {},{}
            
            for side,uniq_changes in zip(('bids','asks'),(uniq_bid_changes,uniq_ask_changes)):
                branch = self.orderbooks[symbol][side]
                for item in d[side]:
                    p,sprev,snew = update_branch(item,branch,side)
                    try: sprev = uniq_changes.pop(p)[1]
                    except KeyError: pass
                    uniq_changes[p] = [p,sprev,snew]
            
            prev_nonce = self.orderbooks[symbol].get('nonce')
            if 'nonce' in d:
                self.orderbooks[symbol]['nonce'] = d['nonce']
            
            if enable_sub:
                self.change_subscription_state(('orderbook', symbol), 1, True)
            
            if set_event:
                self.safe_set_event('orderbook', symbol, {'_': 'orderbook',
                                                          'symbol': symbol,
                                                          'bids_count': len(d['bids']),
                                                          'aks_count': len(d['asks'])})
            
            if does_send and (not only_if_not_subbed or not all_subbed and not is_subbed(symbol)):
                update_tickers.append(symbol)
            
            cb_input = {'_': 'orderbook',
                        'symbol': symbol,
                        'data': {'symbol': symbol,
                                 'bids': list(uniq_bid_changes.values()), 
                                 'asks': list(uniq_ask_changes.values()),
                                 'nonce': (prev_nonce, d.get('nonce'))}}
            self.exec_callbacks(cb_input, 'orderbook', symbol)
            cb_data.append(cb_input)
            
            if self.ob['assert_integrity']:
                assert_integrity(self.orderbooks[symbol])
        
        if cb_data:
            if set_event:
                self.safe_set_event('orderbook', -1)
            self.exec_callbacks(cb_data, 'orderbook', -1)
        
        if update_tickers:
            self._update_tickers_from_ob(update_tickers, set_ticker_event)
    
    
    def _update_tickers_from_ob(self, symbols, set_event=True):
        data = []
        for symbol in symbols:
            d = {'symbol': symbol}
            for side in ('bid','ask'):
                try: d.update(dict(zip([side,side+'Volume'],
                                       self.orderbooks[symbol][side+'s'][0])))
                except (IndexError, KeyError): pass
            if len(d) > 1:
                data.append(d)
        self.update_tickers(data, set_event=set_event)
    
    
    def update_trades(self, data, key=None, *, set_event=True, enable_sub=False):
        """
        :param data: 
            [{'symbol': symbol, 'trades': trades}, ...]
            trades: [{'timestamp': <int>, 'datetime': <str>, 'symbol': <str>, 'id': <str>,
                      'order': <str>?, 'type': <str>, 'takerOrMaker': <str>, 'side': <str>,
                      'price': <float>, 'amount': <float>, 'cost': <float>, 'fee': <float>},
                      ... increasing timestamp]
        """
        cb_data = []
        if key is None:
            key = self.trade['sort_by']
        if not hasattr(key, '__call__') and key is not None:
            _key = key
            key = lambda x: x[_key]
        
        for d in data:
            symbol = d['symbol']
            trades = d['trades']
            try: 
                add_to = self.trades[symbol]
            except KeyError: 
                add_to = self.trades[symbol] = deque(maxlen=self.store['trades'])
            
            trades = sorted(trades, key=key)
            sequence_insert(trades, add_to, key=key, duplicates='drop')
            
            if enable_sub:
                self.change_subscription_state(('trades', symbol), 1, True)
            
            if set_event:
                self.safe_set_event('trades', symbol, {'_': 'trades', 'symbol': symbol})
            
            cb_input = {'_': 'trades',
                        'symbol': symbol,
                        'data': [x.copy() for x in trades]}
            self.exec_callbacks(cb_input, 'trades', symbol)
            cb_data.append(cb_input)
        
        if cb_data:
            if set_event:
                self.safe_set_event('trades', -1)
            self.exec_callbacks(cb_data, 'trades', -1)
    
    
    def update_ohlcv(self, data, *, set_event=True, enable_sub=False):
        """
        :param data: [{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': ohlcv}, ...]
                     ohlcv: [[timestamp_ms, o, h, l, c, quoteVolume], ... increasing timestamp]
        """
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
                add_to = by_timeframes[timeframe] = deque(maxlen=self.store['ohlcv'])
            
            ohlcv = sorted(ohlcv, key=key)
            sequence_insert(ohlcv, add_to, key=key, duplicates='drop')
            
            if enable_sub:
                self.change_subscription_state(('ohlcv', symbol, timeframe), 1, True)
            
            if set_event:
                self.safe_set_event('ohlcv', (symbol, timeframe), {'_': 'ohlcv',
                                                                   'symbol': symbol,
                                                                   'timeframe': timeframe})
                self.safe_set_event('ohlcv', symbol)
            
            cb_input = {'_': 'ohlcv',
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'data': [x.copy() for x in ohlcv]}
            self.exec_callbacks(cb_input, 'ohlcv', (symbol, timeframe))
            self.exec_callbacks(cb_input, 'ohlcv', symbol)
            cb_data.append(cb_input)
        
        if cb_data:
            if set_event:
                self.safe_set_event('ohlcv', -1)
            self.exec_callbacks(cb_data, 'ohlcv', -1)
    
    
    def update_positions(self, data, *, set_event=True, enable_sub=False):
        """
        :param data: [position0, position1, ...]
        """
        cb_data = []
        
        for d in data:
            symbol = d['symbol']
            try:
                prev = self.positions[symbol]
            except KeyError:
                prev = self.dict_clear(self._positions[symbol])
            
            changes = self.dict_changes(d, prev)
            
            self.positions[symbol] = self.dict_update(d, prev)
            
            if set_event:
                self.safe_set_event('position', symbol, {'_': 'position', 'symbol': symbol})
            
            cb_input = {'_': 'position',
                        'symbol': symbol,
                        'data': changes}
            self.exec_callbacks(cb_input, 'position', symbol)
            cb_data.append(cb_input)
        
        if cb_data:
            if enable_sub:
                self.change_subscription_state(('account',), 1, True)
            if set_event:
                self.safe_set_event('position', -1)
            self.exec_callbacks(cb_data, 'position', -1)
    
    
    def update_balances(self, data, *, set_event=True, enable_sub=False):
        """
        :param data:
            [entry_0, entry_1, ...]
            entry:
                {'cy': <str>, 'free': <float>, 'used': <float>}
                (cy, free, used)
        """
        cb_data = []
        
        def parse_dict(x):
            new = {}
            try: cy = x['cy']
            except KeyError:
                cy = x['currency']
            if 'free' in x:
                new['free'] = x['free']
            if 'used' in x:
                new['used'] = x['used']
            if 'total' in x:
                new['total'] = x['total']
                if 'free' not in new and new.get('used') is not None and new['total'] is not None:
                    new['free'] = new['total'] - new['used']
                if 'used' not in new and new.get('free') is not None and new['total'] is not None:
                    new['used'] = new['total'] - new['free']
            elif new.get('free') is not None and new.get('used') is not None:
                new['total'] = new['free'] + new['used']
            if 'info' in x:
                new['info'] = x['info']
            
            return cy, new
        
        for x in data:
            if isinstance(x, dict):
                cy, new = parse_dict(x)
            else:
                cy,free,used = x
                new = {'free': free, 'used': used, 'total': free+used}
            
            try:
                cy_prev = self.balances[cy]
            except KeyError:
                cy_prev = self.dict_clear(self._balances[cy])
            
            changes = self.dict_changes(new, cy_prev)
            
            cy_prev_updated = self.dict_update(new, cy_prev)
            
            for k in ['free','used','total']:
                if k not in cy_prev_updated:
                    cy_prev_updated[k] = None
                self.balances[k][cy] = cy_prev_updated[k]
            
            self.balances[cy] = cy_prev_updated
            
            if set_event:
                self.safe_set_event('balance', cy, {'_': 'balance', 'currency': cy})
            
            cb_input = {
                '_': 'balance',
                'currency': cy,
                'data': changes,
            }
            self.exec_callbacks(cb_input, 'balance', cy)
            cb_data.append(cb_input)
        
        if cb_data:
            if enable_sub:
                self.change_subscription_state(('account',), 1, True)
            if set_event:
                self.safe_set_event('balance', -1)
            self.exec_callbacks(cb_data, 'balance', -1)
    
    
    def update_balances_delta(self, deltas, **kw):
        data = []
        for x in deltas:
            delta_free = delta_used = 0
            if isinstance(x, dict):
                symbol = x['symbol']
                try: delta_free = x['free']
                except KeyError: pass
                try: delta_used = x['used']
                except KeyError: pass
            else:
                symbol = x[0]
                try:
                    delta_free = x[1]
                    delta_used = x[2]
                except IndexError:
                    pass
            try: d = self.balances[symbol]
            except KeyError: d = {'free': 0, 'used': 0, 'total': 0}
            free = d['free'] + delta_free
            used = d['used'] + delta_used
            data.append((symbol,free,used))
        self.update_balances(data, **kw)
    
    
    def add_order(self, id, symbol, side, amount, price=None, timestamp=None, 
                  remaining=None, filled=None, payout=0, datetime=None,
                  type='limit', stop=None, cost=None, average=None, params={}, *,
                  set_event=True, enable_sub=False):
        if remaining is None:
            remaining = amount
        if filled is None and remaining is not None:
            filled = amount - remaining
        
        datetime, timestamp = resolve_times([datetime, timestamp])
        
        if price == 0:
            price = None
        
        if id in self.orders:
            params = dict({'type': type, 'side': side, 'price': price,
                           'amount': amount, 'stop': stop, 'timestamp': timestamp,
                           'datetime': datetime, 'cost': cost, 'average': average},
                           **params)
            return self.update_order(id, remaining, filled, payout, params)
        
        o = dict(
            {'id': id,
             'symbol': symbol,
             'type': type,
             'side': side,
             'price': price,
             'stop': stop,
             'amount': amount,
             'timestamp': timestamp,
             'datetime': datetime,
             'cost': cost,
             'average': average,
             'filled': filled,
             'remaining': remaining,
             'payout': payout
            }, **params)
        
        self.orders[id] = o
        if remaining:
            self.open_orders[id] = o
        
        if enable_sub:
            self.change_subscription_state(('account',), 1, True)
                
        if set_event:
            self.safe_set_event('order', id, {'_': 'order', 'symbol': symbol, 'id': id})
            self.safe_set_event('order', symbol)
            self.safe_set_event('order', -1)
            
        cb_input = {
            '_': 'order',
            'symbol': symbol,
            'id': id,
            'data': o,
        }
        self.exec_callbacks(cb_input, 'order', id)
        self.exec_callbacks(cb_input, 'order', symbol)
        self.exec_callbacks([cb_input], 'order', -1)
        
        if id in self.unprocessed_fills:
            for kwargs in self.unprocessed_fills[id]:
                tlogger.debug('{} - processing fill {} from unprocessed_fills'\
                              .format(self.name, kwargs))
                self.add_fill(**kwargs)
            self.unprocessed_fills.pop(id)
    
    
    def add_order_from_dict(self, d, *, set_event=True, enable_sub=False, drop=None):
        if d['id'] in self.orders:
            return self.update_order_from_dict(d, set_event=set_event,
                                               enable_sub=enable_sub, drop=drop)
        if drop is not None:
            d = self.drop(d, drop)
        kw = {k: d[k] for k in self.ORDER_KEYWORDS if k in d}
        params = {k: d[k] for k in d if k not in self.ORDER_KEYWORDS}
        
        self.add_order(**kw, params=params, set_event=set_event, enable_sub=enable_sub)
    
    
    def update_order_from_dict(self, d, *, set_event=True, enable_sub=False, drop=None):
        if drop is not None:
            d = self.drop(d, drop)
        KEYWORDS = ['id','remaining','filled','payout','cost','average']
        kw = {k: d[k] for k in KEYWORDS if k in d}
        params = {k: d[k] for k in d if k not in KEYWORDS}
        
        self.update_order(**kw, params=params, set_event=set_event, enable_sub=enable_sub)
    
    
    def update_order(self, id, remaining=None, filled=None, payout=None, cost=None, average=None,
                     params={}, *, set_event=True, enable_sub=False):
        try: o = self.orders[id]
        except KeyError:
            logger2.error('{} - not recognized order: {}'.format(self.name, id))
            return
        
        if 'price' in params and params['price']==0:
            params = params.copy()
            params['price'] = None
        
        o_prev = o.copy()
        
        if params.get('amount') is not None and o['amount'] is not None:
            amount_difference = params['amount'] - o['amount']
        else:
            amount_difference = None
        
        self.dict_update(params, o)
        
        # Should it be forced that average can only decrease or increase (depending on order side),
        # assuming that the price hasn't been edited in the meanwhile?
        if average is not None:
            o['average'] = average
        
        if remaining is not None:
            remaining = max(0, remaining)
            
            try: 
                #min_v = self.api.markets[o['symbol']]['limits']['amount']['min']
                precision = self.api.markets[o['symbol']]['precision']['amount']
                smallest_allowed = pow(10, (-1)*precision) if self.api.precisionMode != ccxt.TICK_SIZE \
                                    else precision
                if precision is None: pass
                elif remaining < smallest_allowed:
                    remaining = 0
            except KeyError:
                pass
        
        
        def modify_remaining(prev, new):
            #Order size was edited, thus the "remaining" can increase
            if amount_difference != 0:
                return new
            else:
                return min(prev, new)
        
        
        for name,value,op in [('remaining', remaining, modify_remaining),
                              ('filled', filled, max),
                              ('payout', payout, max),
                              ('cost', cost, max)]:
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
        
        if enable_sub:
            self.change_subscription_state(('account',), 1, True)
        
        if set_event:
            self.safe_set_event('order', id, {'_': 'order', 'symbol': o['symbol'], 'id': id})
            self.safe_set_event('order', o['symbol'])
            self.safe_set_event('order', -1)
            
        cb_input = {
            '_': 'order',
            'symbol': o['symbol'],
            'id': id,
            'data': self.dict_changes(o, o_prev),
        }
        self.exec_callbacks(cb_input, 'order', id)
        self.exec_callbacks(cb_input, 'order', o['symbol'])
        self.exec_callbacks([cb_input], 'order', -1)
    
    
    def add_fill(self, id, symbol, side, price, amount, timestamp=None,
                 datetime=None, order=None, type=None, payout=None,
                 fee=None, fee_rate=None, takerOrMaker=None, cost=None,
                 params={}, *, set_event=True, set_order_event=True,
                 enable_sub=False):
        """
        args 'symbol' and 'side' can be left undefined (None) if order is given
        """
        try: 
            o_fills = self.fills[order]
        except KeyError:
            o_fills = self.fills[order] = []
        
        if (symbol is None or side is None) and order is None:
            raise ValueError("{} - fill id: {}. Both symbol and side must be "\
                             "defined if order is None.".format(self.name, id))
            
        loc = next((i for i,x in enumerate(o_fills) if x['id']==id), None)
        
        if loc is not None:
            tlogger.debug('{} - fill {} already registered.'.format(
                self.name,(id,symbol,side,price,amount,order)))
            del o_fills[loc]
        
        def insert_fill(f):
            o_fills.append(f)
            if set_event:
                self.safe_set_event('fill', order, {'_': 'fill',
                                                    'symbol': symbol,
                                                    'order': order,
                                                    'id': id})
                self.safe_set_event('fill', symbol)
                self.safe_set_event('fill', -1)
            cb_input = {
                '_': 'fill', 
                'symbol': symbol,
                'order': order,
                'id': id,
                'data': f,
            }
            self.exec_callbacks(cb_input, 'fill', order)
            self.exec_callbacks(cb_input, 'fill', symbol)
            self.exec_callbacks([cb_input], 'fill', -1)
        
        if fee_rate is None and takerOrMaker is not None:
            fee_rate = self.api.markets[symbol][takerOrMaker]
        
        fee_dict = None
        if isinstance(fee, (int,float)):
            fee_dict = {'cost': fee}
        elif isinstance(fee, dict):
            fee_dict = fee.copy()
        
        if fee_dict is not None and 'rate' not in fee_dict:
            fee_dict['rate'] = fee_rate
            
        if fee_dict is not None and 'cost' not in fee_dict:
            fee_dict['cost'] = cost
        
        if cost is None and fee_dict is not None and 'cost' in fee_dict:
            cost = fee_dict['cost']
        
        if fee_dict is not None and 'currency' not in fee_dict:
            fee_dict['currency'] = self.api.get_fee_quotation(side, as_str=False)
        
        datetime, timestamp = resolve_times([datetime, timestamp])
        
        f = dict(
            {'id': id,
             'symbol': symbol,
             'timestamp': timestamp,
             'datetime': datetime,
             'type': type,
             'side': side,
             'price': price,
             'amount': amount,
             'takerOrMaker': takerOrMaker,
             'fee': fee_dict,
             'cost': cost,
             'order': order,
            }, **params)
        
        if order is None:
            insert_fill(f)
        
        elif order in self.orders:
            o = self.orders[order]
            if symbol is None:
                f['symbol'] = symbol = o['symbol']
            if side is None:
                f['side'] = side = o['side']
            if type is None:
                f['type'] = type = o['type']
                
            insert_fill(f)
            o_params = {}
            
            if self.order['update_filled_on_fill']:
                o_params['filled'] = o['filled'] + amount
            
            if self.order['update_payout_on_fill']:
                payout = self.api.calc_payout(symbol,o['side'],amount,price,fee_rate,fee=fee) \
                    if payout is None else payout
                o_params['payout'] = o['payout'] + payout
            
            if self.order['update_remaining_on_fill']:
                o_params['remaining'] = o['remaining'] - amount
            
            if o_params:
                self.update_order(order, **o_params, set_event=set_order_event)
        
        else:
            tlogger.debug('{} - adding {} to unprocessed_fills'.format(
                self.name,(id,symbol,type,side,price,amount,order)))
            self.unprocessed_fills[order].append(
                {'id': id, 'symbol': symbol, 'side': side, 'price': price, 'amount': amount,
                 'fee_rate': fee_rate, 'timestamp': timestamp, 'order': order, 'payout': payout,
                 'fee': fee, 'datetime': datetime, 'type': type, 'takerOrMaker': takerOrMaker,
                 'cost': cost, 'params': params, 'set_event': set_event,
                 'set_order_event': set_order_event})
            
        if enable_sub:
            self.change_subscription_state(('account',), 1, True)
    
    
    def add_fill_from_dict(self, d, *, set_event=True, set_order_event=True,
                           enable_sub=False, drop=None):
        if drop is not None:
            d = self.drop(d, drop)
        kw = {k: d[k] for k in self.FILL_KEYWORDS if k in d}
        params = {k: d[k] for k in d if k not in self.FILL_KEYWORDS}
        
        self.add_fill(**kw, params=params, set_event=set_event,
                      set_order_event=set_order_event, enable_sub=enable_sub)
    
    
    def _fetch_callback_list(self, stream, id=-1):
        try: d = self.callbacks[stream]
        except KeyError:
            raise ValueError(stream)
        if isinstance(d,dict) and id is None:
            raise TypeError('{} requires id to be specified'.format(stream))
        elif isinstance(d,list) and id is not None:
            raise TypeError('{} doesn\'t accept id'.format(stream))
        if id is None:
            return d
        if id not in d:
            d[id] = []
        return d[id]
    
    
    def add_callback(self, cb, stream, id=-1):
        """
        :param cb: A function accepting one argument.
        :param stream: stream name
        :param id: symbol, currency, (symbol, timeframe), order_id, -1
        List of stream names:
            ticker, orderbook, ohlcv, trades, order, fill, balance, position
        Id -1 receives all updates of the stream in list form: [update1, ..., updateN]
        See method .wait_on docstring for stream + id combination examples.
        """
        l = self._fetch_callback_list(stream, id)
        l.append(cb)
    
    
    def remove_callback(self, cb, stream, id=-1):
        l = self._fetch_callback_list(stream, id)
        try: l.remove(cb)
        except ValueError: 
            return None
        else:
            return True
    
    
    def exec_callbacks(self, data, stream, id=-1, copy=True):
        callbacks = self._fetch_callback_list(stream, id)
        for cb in callbacks:
            _data = data if not copy else _copy.deepcopy(data)
            cb(_data)
    
    
    def fetch_data(self, s, prev_state):
        #If subsciption has been enabled, fetch all data
        # (except for orderbook, that is handled by .orderbook_maintainer)
        _map = {'all_tickers': 'fetch_tickers',
                'ticker': 'fetch_ticker',
                'trades': 'fetch_trades',
                'ohlcv': 'fetch_ohlcv',
                'account': 'fetch_balance',}
        
        attr = _map.get(s.channel)
        if (s.state and not prev_state and
                attr is not None and self.has_got(attr)):
            corofunc = getattr(self, attr)
            tlogger.debug('Fetching data for {}'.format(s))
            call_via_loop(corofunc, args=s.id_tuple[1:], loop=self.loop)
    
    
    def delete_data(self, s, prev_state):
        _map = {
            'all_tickers': ['tickers'],
            'ticker': ['tickers'],
            'account': ['balances','positions'],
            'orderbook': ['orderbooks'],
        }
        attrs = _map.get(s.channel, [s.channel])
        if s.channel not in self.channels:
            return
        
        for attr in attrs:
            container = getattr(self, attr, None)
            if container is None:
                continue
            #All containers are assumed to be dicts or lists
            if isinstance(container, (list,deque)):
                while True:
                    try: del container[0]
                    except IndexError:
                        break
            elif not isinstance(container, dict):
                pass
            # ('account',) / ('all_tickers',)
            elif len(s.id_tuple) == 1:
                if attr != 'balances':
                    container.clear()
                else:
                    for k in list(container.keys()):
                        if k in ('free','used','total'):
                            container[k].clear()
                        else:
                            del container[k]
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
        
        if s.channel == 'orderbook':
            symbol = s.id_tuple[1]
            self.orderbook_maintainer.is_synced[symbol] = False
    
    
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
        return self.sh.add_subscription(
            self.ip.extend({
                '_': 'ticker',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_ticker(self, symbol):
        return self.sh.remove_subscription(
            {
                '_': 'ticker',
                'symbol': symbol,
            })
    
    
    def subscribe_to_all_tickers(self, params={}):
        #Fetch all tickers before enabling state
        return self.sh.add_subscription(
            self.ip.extend({
                '_': 'all_tickers',
            }, params))
    
    
    def unsubscribe_to_all_tickers(self):
        return self.sh.remove_subscription(
            {
                '_': 'all_tickers',
            })
    
    
    def subscribe_to_orderbook(self, symbol, params={}):
        return self.sh.add_subscription(
            self.ip.extend({
                '_': 'orderbook',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_orderbook(self, symbol):
        return self.sh.remove_subscription(
            {
                '_': 'orderbook',
                'symbol': symbol,
            })
    
    
    def subscribe_to_trades(self, symbol, params={}):
        return self.sh.add_subscription(
            self.ip.extend({
                '_': 'trades',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_trades(self, symbol):
        return self.sh.remove_subscription(
            {
                '_': 'trades',
                'symbol': symbol,
            })
    
    
    def subscribe_to_ohlcv(self, symbol, timeframe='1m', params={}):
        if not self.has_got('ohlcv', 'timeframes', timeframe):
            raise ccxt.NotSupported("Timeframe '{}' is not supported." \
                                    " Choose one of the following: {}" \
                                    .format(timeframe, list(self.api.timeframes)))
        return self.sh.add_subscription(
            self.ip.extend({
                '_': 'ohlcv',
                'symbol': self.merge(symbol),
                'timeframe': timeframe,
            }, params))
    
    
    def unsubscribe_to_ohlcv(self, symbol):
        return self.sh.remove_subscription(
            {
                '_': 'ohlcv',
                'symbol': symbol,
            })
    
    
    def subscribe_to_account(self, params={}):
        return self.sh.add_subscription(
            self.ip.extend({
                '_': 'account',
            }, params))
    
    
    def unsubscribe_to_account(self):
        return self.sh.remove_subscription(
            {
                '_': 'account',
            })
    
    
    def subscribe_to_match(self, symbol, params={}):
        if self.has_got('account','match'):
            if not self.sh.is_subscribed_to({'_': 'account'}):
                return self.subscribe_to_account()
        else:
            return self.sh.add_subscription(
                self.ip.extend({
                    '_': 'match',
                    'symbol': self.merge(symbol),
                }, params))
    
    
    def unsubscribe_to_match(self, symbol):
        if not self.has_got('account','match'):
            return self.sh.remove_subscription(
                {
                    '_': 'match',
                    'symbol': symbol,
                })
    
    
    async def fetch_balance(self, params={}):
        if not self.test:
            balances = await poll.fetch(self.api, 'balances', 0, kwargs={'params': params})
        else:
            balances = await self.api.fetch_balance(params)
        self.update_balances(
            [(cy,y['free'],y['used']) for cy,y in balances.items()
             if cy not in ('free','used','total','info')])
        return balances
    
    
    async def fetch_order_book(self, symbol, limit=None, params={}):
        """This method must be overriden if .orderbook_maintainer
           is used to keep the nonce in sync AND the .api.fetch_order_book doesn't return
           orderbook in correct format (i.e. doesn't include (correct!) nonce value under "nonce").
           Some exchanges may offer websocket method for retrieving the full orderbook."""
        if not self.test:
            ob = await poll.fetch(self.api,('orderbook',symbol), 0,
                                  kwargs={'limit': limit, 'params': params})
        else:
            ob = await self.api.fetch_order_book(symbol, limit, params)
        ob['nonce'] = ob.get('nonce')
        return ob
    
    
    async def fetch_tickers(self, symbols=None, params={}):
        if not self.test:
            tickers = await poll.fetch(self.api, 'tickers', 0,
                                       kwargs={'symbols': symbols, 'params': params})
        else:
            tickers = await self.api.fetch_tickers(symbols, params)
        self.update_tickers(
            [y for x,y in tickers.items() if x!='info'])
        return tickers
    
    
    async def fetch_ticker(self, symbol, params={}):
        if not self.test:
            ticker = await poll.fetch(self.api, ('ticker',symbol), 0, kwargs={'params': params})
        else:
            ticker = await self.api.fetch_ticker(symbol, params)
        self.update_tickers([ticker])
        return ticker
    
    
    async def fetch_trades(self, symbol, since=None, limit=None, params={}):
        kwargs = {'since': since, 'limit': limit, 'params': params}
        if not self.test:
            trades = await poll.fetch(self.api, ('trades',symbol), 0, kwargs=kwargs)
        else:
            trades = await self.api.fetch_trades(symbol, since, limit, params)
        self.update_trades([{'symbol': symbol, 'trades': trades}])
        return trades
    
    
    async def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):
        kwargs = {'since': since, 'limit': limit, 'params': params}
        if not self.test:
            ohlcv = await poll.fetch(self.api, ('ohlcv',symbol,timeframe), 0, kwargs=kwargs)
        else:
            ohlcv = await self.api.fetch_ohlcv(symbol, timeframe, since, limit, params)
        self.update_ohlcv([{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': ohlcv}])
        return ohlcv
    
    
    async def fetch_order(self, id, symbol=None, params={}):
        return await self.api.fetch_order(id, symbol, params)
    
    
    async def fetch_orders(self, symbol=None, since=None, limit=None, params={}):
        return await self.api.fetch_orders(symbol, since, limit, params)
    
    
    async def fetch_my_trades(self, symbol=None, since=None, limit=None, params={}):
        return await self.api.fetch_my_trades(symbol, since, limit, params)
    
    
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
                keywords = ['id','symbol','side','price','amount','timestamp',
                            'remaining','filled','datetime','type','stop']
                params = {k:v for k,v in o.items() if k not in keywords}
                self.add_order(o['id'], o['symbol'], o['side'], o['price'], o['amount'],
                               o.get('timestamp'), remaining=o.get('remaining'), filled=o.get('filled'),
                               type=o.get('type'), datetime=o.get('datetime'), stop=o.get('stop'),
                               params=params)
            except Exception as e:
                logger.exception(e)
        return orders
    
    
    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        amount = self.api.round_amount(symbol, amount)
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        if price is not None:
            price = self.api.round_price(symbol, price, side)
        if self.has_got('create_order','ws') and self.is_active():
            pms = {
                '_': 'create_order',
                'symbol': symbol,
                'type': type,
                'side': side,
                'amount': amount,
                'price': price,
                'params': params,
            }
            r = (await self.send(pms, wait='default')).data
            self.check_errors(r)
            order_id = r['id']
            o = _copy.deepcopy(self.orders[order_id])
            return o
        else:
            r = await self.api.create_order(symbol, type, side, amount, price, params)
            #{'id': '0abc123de456f78ab9012345', 'symbol': 'XRP/USDT', 'type': 'limit', 'side': 'buy',
            # 'status': 'open'}
            if self.order['add_automatically']:
                order = {
                    'id': r['id'],
                    'symbol': symbol,
                    'type': type,
                    'side': side,
                    'amount': amount,
                    'price': price,
                    'timestamp': self.api.milliseconds(),
                }
                self.add_ccxt_order(r, order, 'create')
            
            return r
    
    
    async def create_limit_order(self, symbol, side, amount, price, *args):
        return await self.create_order(symbol, 'limit', side, amount, price, *args)
    
    
    async def create_limit_buy_order(self, symbol, amount, price, *args):
        return await self.create_order(symbol, 'limit', 'buy', amount, price, *args)
    
    
    async def create_limit_sell_order(self, symbol, amount, price, *args):
        return await self.create_order(symbol, 'limit', 'sell', amount, price, *args)
    
    
    async def cancel_order(self, id, symbol=None, params={}):
        if self.has_got('cancel_order','ws') and self.is_active():
            pms = {
                '_': 'cancel_order',
                'id': id,
                'symbol': symbol,
                'params': params,
            }
            r = (await self.send(pms, wait='default')).data
            self.check_errors(r)
            o = _copy.deepcopy(self.orders[id])
            return o
        else:
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
        # amount
        if len(args) >= 3:
            args = args[:2] + (self.api.round_amount(symbol, args[2]),) + args[3:]
        # price
        if len(args) >= 4:
            if args[3] is not None:
                args = args[:3] + (self.api.round_price(symbol, args[3], args[1]),) + args[4:]
                
        if self.has_got('edit_order','ws') and self.is_active():
            pms = {
                'id': id,
                'symbol': symbol,
                'args': args,    
            }
            r = (await self.send(pms, wait='default')).data
            self.check_errors(r)
        else:
            r = await self.api.edit_order(id, symbol, *args)
            if self.order['add_automatically']:
                order = {
                    'id': r['id'] if r.get('id') else id,
                    'symbol': symbol,
                    'type': args[0],
                    'side': args[1],
                    'timestamp': self.api.milliseconds(),
                }
                for key, value in zip(['type','side','amount','price'], args):
                    order[key] = value
                self.add_ccxt_order(r, order, 'edit')
    
    
    def add_ccxt_order(self, response, order={}, from_method='create', overwrite=None):
        if overwrite is None:
            overwrite = (from_method=='edit')
        parsed = self.parse_ccxt_order(response, from_method)
        exists = 'id' in response and response['id'] in self.orders
        
        if not exists or overwrite:
            final_order = dict(order, **parsed.get('order', {}))
            self.add_order_from_dict(final_order)
        
        for f in parsed.get('fills',[]):
            self.add_fill_from_dict(f)
    
    
    def parse_ccxt_order(self, r, *args):
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
    
    
    async def wait_on(self, stream, id=-1):
        """
        Wait on a stream event.
        :param stream: the stream name
        :param id: symbol, currency, (symbol, timeframe), order_id, -1
        List of stream names:
            ticker, orderbook, ohlcv, trades, order, fill, balance, position
        Id -1 waits till *any* update under the stream occurs.
        Examples:
            wait_on('orderbook', -1) -> waits till any orderbook is updated
            wait_on('orderbook', 'BTC/USDT') -> till orderbook BTC/USDT is updated
            wait_on('ohlcv', -1) -> till any ohlcv is updated
            wait_on('ohlcv', symbol) -> till ohlcv BTC/USDT with any timeframe is updated
            wait_on('ohlcv', (symbol, timeframe)) -> till ohlcv BTC/USDT with specific timeframe is updated
            wait_on('order', -1) -> till any order is updated
            wait_on('order', 'a8f1e2dc52e9c') -> till order with id 'a8f1e2dc52e9c' is updated
            wait_on('order', 'BTC/USDT') -> till any order with symbol 'BTC/USDT' is updated
            wait_on('balance', -1) -> till any balance is updated
            wait_on('balance', 'BTC') -> till BTC balance is updated
            wait_on('fill', -1) -> till any fill is added
            wait_on('fill', 'a8f1e2dc52e9c') -> till a fill of order 'a8f1e2dc52e9c' is added
            wait_on('fill', 'BTC/USDT') -> till any fill with symbol 'BTC/USDT' is added
        """
        event = self.events[stream][id]
        await event.wait()
        event.clear()
    
    
    async def wait_on_order(self, id, cb=None, stream_deltas=[], defaults={}):
        """
        :param cb: a callback function accepting args: (order, changes); called on every update
        :param stream_deltas: list of order params which changes are monitored and sent through cb
        :param defaults: if given, changes are also sent before first update
        """
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
    
    
    def safe_currency_code(self, currency_id, currency=None):
        return self.api.safe_currency_code(currency_id, currency)
    
    
    def currency_id(self, commonCode):
        # This can be wildly inaccurate when the currencies haven't been initated yet.
        # ccxt doesn't actually use this function in any of its exchanges
        return self.api.currency_id(commonCode)
    
    
    def symbol_by_id(self, symbol_id):
        return self.api.markets_by_id[symbol_id]['symbol']
    
    
    def id_by_symbol(self, symbol):
        return self.api.markets[symbol]['id']
    
    
    def guess_symbol(self, symbol, direction=1):
        """
        Convert by detecting and converting currencies in symbol
        :param direction:
             [0] common <-direction-> exchange-specific (id) [1]
        """
        if not isinstance(symbol, str):
            raise TypeError('Symbol must be of type <str>, got: {}'.format(type(symbol)))
        sep = self.symbol['sep']
        sw = self.symbol['startswith']
        step = 1 if sw=='base' else -1
        if not direction:
            if not sep:
                method = 'endswith' if sw=='base' else 'startswith'
                ln = next((len(q) for q in self.symbol['quote_ids']
                           if getattr(symbol, method)(q)), None)
                if ln is None:
                    raise KeyError(symbol)
                if sw == 'base':
                    cys = [symbol[:-ln], symbol[-ln:]]
                else:
                    cys = [symbol[:ln], symbol[ln:]]
            else:
                cys = symbol.split(sep)
            return '/'.join([self.convert_cy(x, 0) for x in cys[::step]])
        else:
            return sep.join([self.convert_cy(x, 1) for x in symbol.split('/')[::step]])
    
    
    def convert_cy(self, currency, direction=1):
        """
        :param direction:
             [0] common <-direction-> exchange-specific (id) [1]
        """
        self.sn_load_markets()
        try:
            if hasattr(currency, '__iter__') and not isinstance(currency, str):
                cls = list if not self.is_param_merged(currency) else self.merge
                return cls(self.convert_cy(_cy, direction) for _cy in currency)
            if not direction:
                return self.safe_currency_code(currency)
            else:
                return self.currency_id(currency)
        finally:
            if isinstance(sys.exc_info()[1], KeyError):
                self._reload_markets(sys.exc_info()[1])
    
    
    def convert_symbol(self, symbol, direction=1, force=None):
        """
        :param direction:
             [0] common <-direction-> exchange-specific (id) [1]
        """
        self.sn_load_markets()
        keyError_occurred = False
        if force is None:
            force = self.symbol['force']
        if isinstance(force, dict):
            force = force[direction]
        
        try:
            if hasattr(symbol, '__iter__') and not isinstance(symbol, str):
                cls = list if not self.is_param_merged(symbol) else self.merge
                return cls(self.convert_symbol(_s, direction, force) for _s in symbol)
            
            if not direction:
                try:
                    return self.symbol_by_id(symbol)
                except KeyError:
                    keyError_occurred = True
                    if symbol.startswith('.'):
                        # Bitmex symbol ids starting with "." map 1:1 to ccxt
                        return symbol
            else:
                try:
                    return self.id_by_symbol(symbol)
                except KeyError:
                    keyError_occurred = True
            if not force and keyError_occurred:
                raise KeyError(symbol)
            return self.guess_symbol(symbol, direction)
        finally:
            error = sys.exc_info()[1]
            if isinstance(error, KeyError) or keyError_occurred:
                error = error if isinstance(error, KeyError) else KeyError(symbol)
                self._reload_markets(error)
    
    
    def convert_timeframe(self, timeframe, direction=1):
        """
        :param direction:
             [0] common <-direction-> exchange-specific (id) [1]
        """
        if not direction:
            tf = next((common for common,id in self.timeframes.items() if id==timeframe), None)
        else:
            tf = self.timeframes.get(timeframe)
        
        if tf is None:
            raise ValueError('Not supported timeframe: {}'.format(timeframe))
        
        return tf
    
    
    def convert_cnx_params(self, params):
        params = params.copy()
        _params = params.copy()
        channel = params['_']
        
        def _get_value(key, default=None):
            return self.get_value(channel, ['cnx_params_converter_config']+key, default)
        
        include = [x for x in ('base_cy','quote_cy') if _get_value(['include', x])]
        
        cy_aliases = _get_value(['currency_aliases'], [])
        cy_aliases = list(unique(cy_aliases + _get_value(['__currency_aliases'], [])))
        if 'currency' in cy_aliases:
            cy_aliases = ['currency'] + [x for x in cy_aliases if x!='currency']
        
        cases = {'lower': {}, 'upper': {}}
        
        for case, values in cases.items():
            for x in unique(['symbol','timeframe'] + cy_aliases):
                default = None if x not in cy_aliases else values.get('currency')
                values[x] = _get_value([case, x], default)
        
        def _change_case(s, method='lower'):
            cls = list if not self.is_param_merged(s) else self.merge
            return getattr(s, method)() if isinstance(s, str) else \
                   cls(getattr(x, method)() if x is not None else x for x in s)
        
        
        def _extract_base_quote(symbol):
            cls = list if not self.is_param_merged(symbol) else self.merge
            is_str = isinstance(symbol, str)
            symbols = [symbol] if is_str else symbol
            cy_pairs = []
            for symbol in symbols:
                if symbol in self.markets:
                    m = self.markets[symbol]
                    cy_pairs.append([m['base'], m['quote']])
                elif '/' in symbol:
                    cy_pairs.append(symbol.split('/'))
                else:
                    cy_pairs.append([None, None])
            if cy_pairs:
                if 'base_cy' in include:
                    params['base_cy'] =  cy_pairs[0][0] if is_str else cls(p[0] for p in cy_pairs)
                if 'quote_cy' in include:
                    params['quote_cy'] = cy_pairs[0][1] if is_str else cls(p[1] for p in cy_pairs)
        
        
        if 'symbol' in params and params['symbol'] is not None:
            _params['symbol'] = self.convert_symbol(params['symbol'], 1, force=True)
            
            if 'base_cy' in include or 'quote_cy' in include:
                _extract_base_quote(params['symbol'])
        
        for attr in cy_aliases:
            if attr in params and params[attr] is not None:
                _params[attr] = self.convert_cy(params[attr], 1)
        
        if 'timeframe' in params and params['timeframe'] is not None:
            _params['timeframe'] = self.convert_timeframe(params['timeframe'], 1)
        
        for attr in unique(['symbol','timeframe'] + cy_aliases):
            if attr in _params and _params[attr] is not None:
                if cases['lower'][attr]:
                    _params[attr] = _change_case(_params[attr], 'lower')
                if cases['upper'][attr]:
                    _params[attr] = _change_case(_params[attr], 'upper')
        
        return _params
    
    
    def _reload_markets(self, e=None):
        reload_interval = self.symbol['reload_markets_on_KeyError']
        if reload_interval is not None and \
                time.time() - self._last_markets_loaded_ts > reload_interval:
            error_txt = '(due to error: {})'.format(repr(e)) if e is not None else ''
            logger.error('{} - force reloading markets {}'.format(self.name, error_txt))
            self._last_markets_loaded_ts = time.time()
            if self.loop.is_running():
                asyncio.ensure_future(
                    self.load_markets(reload=True, limit=0), loop=self.loop)
            else:
                self.sn_load_markets(reload=True, limit=0)
    
    
    def encode_symbols(self, symbols, topics):
        if symbols is None:
            symbols = []
        elif isinstance(symbols, str):
            symbols = [symbols]
        symbols = [self.convert_symbol(s,1) for s in symbols]
        
        with_symbol = [t for t in topics if '<symbol>' in t]
        without_symbol = [t for t in topics if '<symbol>' not in t]
        
        args = without_symbol[:]
        
        for s in symbols:
            args += [t.replace('<symbol>',s) for t in with_symbol]
        
        return args
    
    
    def safe_set_event(self, _, id, *broadcast, via_loop=None, op='set'):
        events = self.events[_]
        if events is None:
            return
        try: event = events[id]
        except KeyError:
            events[id] = event = asyncio.Event(loop=via_loop)
        #print('Setting {}'.format(id))
        func = getattr(event,op)
        if via_loop is None:
            func()
        else:
            via_loop.call_soon_threadsafe(func)
        for b in broadcast:
            self.broadcast_event(id, b)
    
    
    @staticmethod
    def dict_update(new, prev, info=True, copy=False):
        """Returns (a copy of) prev dict with added/overwritten values from new.
           If `info`==True, then does the same for new['info'] ~ prev['info']"""
        if not isinstance(prev, dict) or not isinstance(new, dict):
            return _copy.copy(new) if copy else new
        else:
            prev_info = prev.get('info')
            if copy:
                new_dict = dict(prev, **new)
            else:
                new_dict = prev
                new_dict.update(new)
            if info and isinstance(prev_info, dict) and 'info' in new and isinstance(new['info'], dict):
                new_dict['info'] = ExchangeSocket.dict_update(new['info'], prev_info, False, copy)
            return new_dict
    
    
    @staticmethod
    def dict_clear(d, info=True):
        had_info = 'info' in d
        prev_info = d.get('info')
        d.clear()
        if info and had_info and isinstance(prev_info, dict):
            prev_info.clear()
            d['info'] = prev_info
        return d
    
    
    @staticmethod
    def dict_changes(new, prev):
        """Determine the diff between two dicts"""
        if not isinstance(prev, dict) or not isinstance(new, dict):
            return _copy.copy(new)
        else:
            return {k: v for k,v in new.items() if k not in prev or prev[k]!=v}
    
    
    @staticmethod
    def list_new_only(new_seq, prev_seq, timestamp_key, sort=False):
        """Selects items from new_seq which's timestamp is newer than last timestamp in prev_seq.
           For this to be effective new_seq (and prev_seq) must be sorted"""
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
    
    
    @staticmethod
    def drop(d, keywords, copy=True):
        """
        :type keywords: dict
        :param keywords:
            {value: keys to drop for the matching value}
            if keys==None, drops all matching
        """
        if copy:
            d = d.copy()
        for value, keys in keywords.items():
            if hasattr(keys, '__iter__'):
                for k in keys:
                    if k in d and d[k]==value:
                        del d[k]
            else:
                for k in list(d.keys()):
                    if d[k]==value:
                        del d[k]
        return d


def _resolve_auth(exchange, config, test=False):
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
    
    if not auth['apiKey']:
        id = auth.get('id')
        if test and (id is None or 'test' not in id.lower().split('_')):
            auth['id'] = 'test' if not id else id+'_test'
    
    """if not auth['apiKey']:
        relevant = {x:y for x,y in auth.items() if x not in token_kwds}
        auth = get_auth2(**relevant)"""
    
    return auth