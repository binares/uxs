import sys
import copy as _copy
import asyncio
import itertools
import time
import ccxt
from collections import (defaultdict, deque, OrderedDict)
import datetime
dt = datetime.datetime
td = datetime.timedelta

from ..auth import get_auth2, EXTRA_TOKEN_KEYWORDS
from ..ccxt import init_exchange, _ccxtWrapper
from .. import poll
from .orderbook import OrderbookMaintainer
from .l3 import L3Maintainer
from .errors import (ExchangeSocketError, ConnectionLimit)
from uxs.fintls.basics import (as_ob_fill_side, as_direction)
from uxs.fintls.ob import (
    get_stop_condition, create_orderbook, update_branch, assert_integrity, get_bidask)
from uxs.fintls.l3 import (
    create_l3_orderbook, update_l3_branch, assert_l3_integrity, l3_to_l2)
from uxs.fintls.utils import resolve_times
from wsclient import WSClient
from wsclient.sub import Subscription

import fons.log
from fons.aio import call_via_loop
from fons.dict_ops import deep_update, deep_get
from fons.errors import QuitException
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
    
    api = None # ccxt.Exchange instance
    snapi = None # ccxt.async_support.Exchange instance
    pro = None # ccxtpro.Exchange instance (if ccxtpro installed and present in its library)
    use_pro = False # TODO
    
    # Used on .subscribe_to_{x} , raises ValueError when doesn't have
    has = dict.fromkeys([
         'all_tickers', 'ticker', 'orderbook', 'trades', 'ohlcv', 'l3',
         'account', 'balance', 'order', 'fill', 'position', 'own_market',
         'fetch_tickers', 'fetch_ticker', 'fetch_order_book',
         'fetch_trades', 'fetch_ohlcv', 'fetch_balance',
         'fetch_order', 'fetch_orders', 'fetch_open_orders',
         'fetch_closed_orders', 'fetch_my_trades',
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
        'own_market': { # market + account (order, fill, position)
            'required': ['symbol'],
            'is_private': True,
            'delete_data_on_unsub': False,
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
        'l3': {
            'required': ['symbol'],
            'auto_activate': False,
            'delete_data_on_unsub': False, # ccxt does not have fetchL3 method for reloading
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
        'fetch_l3_order_book': {
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
        'fetch_closed_orders': {
            'type': 'fetch',
            'required': ['symbol', 'since', 'limit'],
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
    TICKER_KEYWORDS = (
        'last', 'bid', 'bidVolume', 'ask', 'askVolume', 'high', 'low',
        'open', 'close', 'previousClose', 'change', 'percentage',
        'average', 'vwap', 'baseVolume', 'quoteVolume',
    )
    TRADE_KEYWORDS = (
        'timestamp', 'datetime', 'symbol', 'id', 'order', 'type',
        'takerOrMaker', 'side', 'price', 'amount', 'cost', 'fee',
        'orders', # orders=[makerOrder, takerOrder]
    )
    OHLCV_KEYWORDS = (
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
    )
    BALANCE_KEYWORDS = (
        'free', 'used', 'total',
    )
    ORDER_KEYWORDS = (
        'id', 'symbol', 'side', 'price', 'amount', 'timestamp', 
        'remaining', 'filled', 'payout', 'datetime',
        'type', 'stop', 'cost', 'average', 'lastTradeTimestamp',
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
    
    intervals = {
        'default': 30,
        'fetch_tickers': 15,
        'fetch_ticker': 15,
        'fetch_order_book': 15,
        'fetch_trades': 60,
        'fetch_balance': 60,
        'fetch_my_trades': 15,
        'fetch_orders': 15,
        'fetch_open_orders': 15,
        'fetch_closed_orders': 15,
        'fetch_order': 15,
    }
    max_missed_intervals = {
        'default': 2,
    }
        
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
        # exchange provided depth limits. the limit provided in the subscription
        # will be rounded up to the nearest in this list, or set to `None` if no larger exists
        'limits': [],
        # limit used for fetch_order_book. if not specified
        # then the limit provided in the subscription is used instead
        'fetch_limit': None,
        # the number of latest updates to be kept in cache
        'cache_size': 50,
        'uses_nonce': True,
        'nonce_increment': 1, # set this to `None` if increasing by any >0 step is allowed
        # set this to True if uses_nonce and fetch_order_book doesn't return nonce
        'ignore_fetch_nonce': False,
        'assume_fetch_max_age': 5,
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
    
    l3 = dict(ob)
    
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
        'enable_querying': 'if-not-immediate',
        'update_lastTradeTimestamp_on_fill': True,
        'update_balance': 'if-not-subbed-to-balance',
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
    
    # since socket interpreter may clog due to high CPU usage,
    # maximum lengths for the queues are set, and if full,
    # previous items are removed one-by-one to make room for new ones
    queue_maxsizes = {
        'balance': 100, 
        'ticker': 1000, 
        'order': 100, 
        'fill': 100,
        'orderbook': 1000,
        'l3': 1000,
        'trades': 10000,
        'ohlcv': 100,
        'event': 10000, # fetch_tickers can return hundreds/thousands of tickers at once
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
         ['fetch_l3_orderbook','fetch_l3_order_book'],
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
         ['fetchL3OrderBook','fetch_l3_order_book'],
         ['fetchOHLCV','fetch_ohlcv'],
         ['fetchMyTrades','fetch_my_trades'],
         ['fetchBalance','fetch_balance'],
         ['fetchOrder','fetch_order'],
         ['fetchOrders','fetch_orders'],
         ['fetchClosedOrders','fetch_closed_orders'],
         ['fetchMyTrades','fetch_my_trades'],
         ['createOrder','create_order'],
         ['editOrder','edit_order'],
         ['cancelOrder','cancel_order'],]
    
    __extend_attrs__ = [
        'fetch_limits',
        'channel_ids',
        'intervals',
        'l3',
        'max_missed_intervals',
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
        
        _kwargs =  {
            'load_cached_markets': False,
            'profile': profile,
            'test': ccxt_test,
        }
        _params = {
            'exchange':self.exchange,
            'async':True,
            'args': (ccxt_config,),
            'kwargs': _kwargs,
            'auth': auth,
            'add': False,
        }
        # asynchronous ccxt Exchange instance
        self.api = init_exchange(dict(
            _params,
            kwargs=dict(_kwargs, load_cached_markets=load_cached_markets)
        ))
        
        # synchronous ccxt Exchange instance
        self.snapi = init_exchange({**_params, 'async': False})
        
        if self.use_pro:
            raise NotImplementedError('uxs library does not support ccxt pro yet.')
        
        try:
            self.pro = init_exchange({**_params, 'pro': True})
        except (ImportError, AttributeError) as e:
            if self.use_pro:
                raise ccxt.NotSupported(self.exchange + ' is not supported by ccxt pro yet.')
        
        self._init_has()
        self._init_exceptions()
        
        # Some methods do not assume that a markets / currencies etc value
        # can be `None`. Replace with empty dicts / lists instead.
        self.api._ensure_no_nulls()
        
        self.api.sync_with_other(self.snapi)
        if self.pro:
            self.api.sync_with_other(self.pro)
            self.snapi.sync_with_other(self.pro)
        
        names = ['ticker','orderbook','trades','ohlcv','l3',
                 'balance','fill','order','position','empty','recv',]
        #query_names = ['fetch_order_book','fetch_ticker','fetch_tickers','fetch_balance']
        self.events = defaultdict(lambda: defaultdict(lambda: asyncio.Event(loop=self.loop)))
        for n in names:
            self.events[n][-1]
        self.events['empty'][-1] = self.station.get_event('empty',0,0)
        self.events['recv'][-1] = self.station.get_event('recv',0,0)
        self._init_events()
        
        self.callbacks = {x: {} for x in ['orderbook','ticker','trades','ohlcv','l3',
                                          'position','balance','order','fill']}
        self.tickers = {}
        self.balances = {'free': {}, 'used': {}, 'total': {}}
        self.orderbooks = {}
        self.trades = {}
        self.ohlcv = {}
        self.l3_books = {}
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
        for attr in ('tickers','balances','orderbooks','positions','l3_books'):
            setattr(self, '_'+attr, defaultdict(dict))
        self._balances.update(self.balances)
        
        self.orderbook_maintainer = self.OrderbookMaintainer_cls(self)
        self.l3_maintainer = L3Maintainer(self)
        self._last_fetches = defaultdict(float)
        self._last_markets_loaded_ts = 0
        self._cancel_scheduled = set()
    
    
    def _init_events(self):
        markets = self.api.markets if self.api.markets is not None else {}
        currencies = self.api.markets if self.api.markets is not None else {}
        
        for market in markets:
            for x in ['ticker','orderbook','trades','ohlcv','l3','position']:
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
        
        under_account = ['balance','order','fill','position']
        if not isinstance(self.has['account'], dict):
            self.has['account'] = dict.fromkeys(under_account, self.has_got('account'))
        
        camel_map = {
            'fetch_ohlcv': 'fetchOHLCV',
            'fetch_l3_order_book': 'fetchL3OrderBook',
        }
        emulation_methods = OrderedDict([
            ['l3', ['fetch_l3_order_book']],
            ['ticker', ['fetch_ticker']],
            ['all_tickers', ['fetch_tickers']],
            ['orderbook', ['fetch_order_book']],
            ['trades', ['fetch_trades']],
            ['ohlcv', ['fetch_ohlcv']],
            ['balance', ['fetch_balance']],
            ['order', ['fetch_my_trades', 'fetch_orders', 'fetch_open_orders', 'fetch_order']],
            ['fill', ['fetch_my_trades']],
            ['position', []],
            ['own_market_order', ['fetch_my_trades', 'fetch_orders', 'fetch_open_orders', 'fetch_order']],
            ['own_market_fill', ['fetch_my_trades']],
        ])
        do_not_fetch_on_emulated_sub = ['ticker','all_tickers','orderbook','trades','ohlcv','l3']
        
        for x in ['fetch_tickers', 'fetch_ticker', 'fetch_order_book',
                  'fetch_trades', 'fetch_ohlcv', 'fetch_l3_order_book',
                  'fetch_balance', 'fetch_orders', 'fetch_order',
                  'fetch_open_orders', 'fetch_closed_orders', 'fetch_my_trades',
                  'create_order', 'edit_order', 'cancel_order'] + list(emulation_methods):
            x2 = x if not x.startswith('own') else x.split('_')[-1]
            which_acc = None if x2 not in under_account else ('account' if not x.startswith('own') else 'own_market')
            get = [x] if not which_acc else [which_acc, x2]
            methods = emulation_methods.get(x, [x])
            camel = camel_map.get(x, to_camelcase(x))
            camel_methods = [camel_map.get(m, to_camelcase(m)) for m in methods]
            initial_vals = {}
            for k in ('ws', 'emulated', 'ccxt'):
                try: initial_vals[k] = (self.has[x] if x not in under_account else self.has['account'][x]).pop(k)
                except Exception: pass
            self_has = self.has_got(*get)
            ws_has = initial_vals.get('ws') 
            ccxt_has = initial_vals.get('ccxt') # applies to fetch methods only
            emulated = initial_vals.get('emulated') # applies to stream methods only
            if ws_has is None:
                ws_has =  self_has if x in emulation_methods and emulated is None else False
            if ccxt_has is None:
                ccxt_has = None if x in emulation_methods else any(self.api.has.get(cm) for cm in camel_methods)
            if emulated is None and not ws_has:
                if x=='orderbook' or (x=='trades' and self.has_got('l3','trades')):
                    if self.has_got('l3','ws'):
                        emulated = 'l3'
                elif x in ('own_market_fill', 'own_market_order'):
                    if x=='own_market_order' and self.has_got('account','order','ws') \
                            or x=='own_market_fill' and self.has_got('account','fill','ws'):
                        emulated = 'account'
                    elif self.has_got('trades','ws') and self.has_got('trades','orders') \
                            or self.has_got('l3','ws') and self.has_got('l3','trades','orders'):
                        emulated = 'trades'
                if emulated is None and x in emulation_methods:
                    emulated = 'poll' if any(self.has_got(m) for m in methods) else None
                    if x == 'balance' and not self.has_got('fetch_balance', ['free','used']):
                        emulated = None
            if hasattr(emulated, '__iter__') and not isinstance(emulated, str):
                emulated = list(emulated)
            emulated_list = emulated if isinstance(emulated, list) else [emulated]
            if any(_ in emulation_methods or _=='account' for _ in emulated_list):
                ws_has = True # emulated by ws stream
            any_has = bool(self_has or ws_has or ccxt_has or emulated)
            
            update = {x2: {'_': any_has, 'ws': ws_has, 'emulated': emulated, 'ccxt': ccxt_has}}
            if which_acc is not None:
                update = {which_acc: update}
            deep_update(self.has, update)
            
            self.has[camel] = _copy.deepcopy(deep_get([self.has], get))
            
            if x in do_not_fetch_on_emulated_sub and emulated:
                self.channels[x]['fetch_data_on_sub'] = False
            
            if emulated and x in self.channels and self.channels[x].get('url') is None:
                self.channels[x]['url'] = ''
        
        # ---LOOP END----
        
        if self.has_got('account','balance','ws'):
            self.has['account']['balance'].update(
                {k: True for k in self.BALANCE_KEYWORDS if k not in self.has['account']['balance']})
        
        for x in ('fill', 'order'):
            if self.has_got('account', x, 'emulated') and not self.has_got('account', x, 'ws') \
                    and self.has_got('own_market', x, 'ws'):
                self.has['account'][x].update({'_': False, 'ws': False, 'emulated': None})
        
        self.has.update({x: _copy.deepcopy(deep_get([self.has], ['account', x])) for x in under_account})
        deep_update(self.has, {x: _copy.deepcopy(deep_get([self.has], ['own_market', x])) for x in ['order','fill']})
        
        for acc in ('account', 'own_market'):
            emulated = unique((self.has[acc][x]['emulated'] for x in under_account
                               if self.has_got(acc, x, 'emulated')), astype=list)
            if not any(emulated):
                emulated = None
            elif len(emulated)==1:
                emulated = emulated[0]
            self.has[acc].update({
                '_': any(self.has_got(acc, x) for x in under_account),
                'ws': any(self.has_got(acc, x, 'ws') for x in under_account),
                'ccxt': None,
                'emulated': emulated,
            })
            any_independent = any(self.has_got(acc, x, 'ws') and not self.has_got(acc, x, 'emulated')
                                  for x in under_account)
            if emulated and not any_independent and self.channels[acc].get('url') is None:
                self.channels[acc]['url'] = ''
        
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
        
        for x,kw in [('all_tickers','TICKER'),('ticker','TICKER'),('trades','TRADE'),('ohlcv','OHLCV'),
                     ('balance','BALANCE')]:
            if self.has_got(x, 'emulated'):
                keywords = getattr(self, kw+'_KEYWORDS')
                source_dict = self.has[emulation_methods[x][0]] if self.has[x]['emulated']!='l3' else self.has['l3'][x]
                update = {k: source_dict.get(k, None) for k in keywords if k in source_dict or k not in self.has[x]}
                self.has[x].update(update)
                if x in under_account:
                    self.has['account'][x].update(update)
        
        # reload the url_factories
        self.reload_urls()
    
    
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
        asyncio.ensure_future(self.poll_loop())
    
    
    def notify_unknown(self, r, max_chars=500, logger=logger2):
        logger.warning('{} - unknown response: {}'.format(self.name, str(r)[:max_chars]))
    
    
    def log_error(self, msg, e=None, log_e=True, logger=logger2, e_logger=logger):
        sfx = ': {}'.format(repr(e)) if e is not None else ''
        logger.error('{} - {}{}'.format(self.name, msg, sfx))
        if log_e and e is not None:
            e_logger.exception(e)
    
    
    def check_errors(self, r):
        pass
    
    
    def get_dependencies(self, subscription):
        s = subscription
        deps = []
        try: emulated = self.has[s.channel]['emulated']
        except: emulated = None
        _is_in = lambda x, y: x==y or isinstance(y, (list,tuple)) and x in y
        
        for dependency in ('l3', 'trades'):
            if _is_in(dependency, emulated) and self.has_got(dependency):
                if 'symbol' not in self.get_value(s.channel, 'required'):
                    raise ValueError("{} - {} can't assign dependency '{}' due to it not having 'symbol' param"
                                     .format(self.name, s, dependency))
                symbol = s.params['symbol']
                is_merged = self.is_param_merged(symbol)
                if not is_merged or self.has_merge_option(dependency):
                    id_tuples = [(dependency, symbol)]
                else:
                    id_tuples = [(dependency, _symbol) for _symbol in symbol]
                deps += id_tuples
        
        if _is_in('account', emulated) and self.has_got('account'):
            deps += [('account',)]
        
        return deps
    
    
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
            
            if enable_individual and self.is_subscribed_to(('ticker', symbol)):
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
            if enable_all and self.is_subscribed_to(('all_tickers',)):
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
            if enable_sub and self.is_subscribed_to(('orderbook', symbol)):
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
        
        self._update_tickers_from_ob([ob['symbol'] for ob in data])
    
    
    def update_orderbooks(self, data, *, set_event=True, enable_sub=False, is_delta=False):
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
        cb_data = []
        
        for d in data:
            symbol = d['symbol']
            amount_pcn = self.markets.get(symbol, {}).get('precision', {}).get('amount')
            uniq_bid_changes, uniq_ask_changes = {}, {}
            
            for side, uniq_changes in zip(('bids', 'asks'), (uniq_bid_changes, uniq_ask_changes)):
                branch = self.orderbooks[symbol][side]
                for item in d[side]:
                    p, a0, a = update_branch(item, branch, side, is_delta=is_delta, round_to=amount_pcn)
                    try: a0 = uniq_changes.pop(p)[1]
                    except KeyError: pass
                    uniq_changes[p] = [p, a0, a]
            
            prev_nonce = self.orderbooks[symbol].get('nonce')
            if 'nonce' in d:
                self.orderbooks[symbol]['nonce'] = d['nonce']
            
            if enable_sub and self.is_subscribed_to(('orderbook', symbol)):
                self.change_subscription_state(('orderbook', symbol), 1, True)
            
            if set_event:
                self.safe_set_event('orderbook', symbol, {'_': 'orderbook',
                                                          'symbol': symbol,
                                                          'bids_count': len(d['bids']),
                                                          'aks_count': len(d['asks'])})
            
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
        
        self._update_tickers_from_ob([d['symbol'] for d in data])
    
    
    def _update_tickers_from_ob(self, symbols):
        sends = self.ob['sends_bidAsk']
        does_send = sends and (not isinstance(sends, dict) or '_' not in sends or sends['_'])
        all_subbed = self.is_subscribed_to({'_': 'all_tickers'}, 1)
        only_if_not_subbed = isinstance(sends,dict) and sends.get('only_if_not_subbed_to_ticker')
        if not does_send or only_if_not_subbed and all_subbed:
            return
        set_ticker_event = not isinstance(sends,dict) or sends.get('set_ticker_event', True)
        is_subbed = lambda symbol: self.is_subscribed_to({'_':'ticker','symbol':symbol}, 1)
        
        data = []
        for symbol in symbols:
            if only_if_not_subbed and is_subbed(symbol):
                continue
            d = {'symbol': symbol}
            for side in ('bid','ask'):
                try: d.update(dict(zip([side,side+'Volume'],
                                       self.orderbooks[symbol][side+'s'][0])))
                except (IndexError, KeyError): pass
            if len(d) > 1:
                data.append(d)
        
        if data:
            self.update_tickers(data, set_event=set_ticker_event)
    
    
    def create_l3_orderbooks(self, data, *, set_event=True, enable_sub=False): 
        """
        :param data:
            [
                {
                 'symbol': <str>,
                 'bids': [[bPrice0, bQnt0, bId0], ...],
                 'asks': [[aPrice0, aQnt0, aId0], ...],
                 'nonce': <int> or None]}
                },
                ...
            ]
        """
        for l3 in data:
            symbol = l3['symbol']
            amount_pcn = self.markets.get(symbol, {}).get('precision', {}).get('amount')
            self.l3_books[symbol] = new = \
                self.l3_maintainer._deep_overwrite(create_l3_orderbook(l3))
            if enable_sub and self.is_subscribed_to(('l3', symbol)):
                self.change_subscription_state(('l3', symbol), 1, True)
            if set_event:
                self.safe_set_event('l3', symbol, {'_': 'l3',
                                                   'symbol': symbol,
                                                   'bids_count': len(new['bids']),
                                                   'asks_count': len(new['asks'])})
            if self.l3['assert_integrity']:
                assert_l3_integrity(self.l3_books[symbol])
            
            if self.l3_maintainer.is_subbed_to_l2_ob(symbol):
                l2_ob = l3_to_l2(self.l3_books[symbol], amount_pcn)
                self.create_orderbooks([l2_ob], enable_sub=True)
        
        if data and set_event:
            self.safe_set_event('l3', -1)
        
        # self._update_tickers_from_ob([ob['symbol'] for ob in data])
    
    
    def update_l3_orderbooks(self, data, *, set_event=True, enable_sub=False):
        """
        :param data:
            [
                {
                 'symbol': <str>,
                 'bids': [[bPrice0, bQnt0, bId0], ...],
                 'asks': [[aPrice0, aQnt0, aId0], ...],
                 'nonce': <int> or None]}
                },
                ...
            ]
        """
        cb_data = []
        create_l2 = set()
        push_to_l2 = set()
        
        for d in data:
            symbol = d['symbol']
            amount_pcn = self.markets.get(symbol, {}).get('precision', {}).get('amount')
            bid_deltas, ask_deltas = defaultdict(float), defaultdict(float)
            
            for side, deltas in zip(('bids', 'asks'), (bid_deltas, ask_deltas)):
                branch = self.l3_books[symbol][side]
                for item in d[side]:
                    p, a, id, p0, a0 = update_l3_branch(item, branch, side)
                    if p0 == p:
                        deltas[p] += (a - a0)
                    else:
                        deltas[p0] -= a0
                        deltas[p] += a
                if amount_pcn is not None:
                    for p, a in deltas.items():
                        deltas[p] = round(a, amount_pcn)
            
            prev_nonce = self.l3_books[symbol].get('nonce')
            if 'nonce' in d:
                self.l3_books[symbol]['nonce'] = d['nonce']
            
            if enable_sub and self.is_subscribed_to(('l3', symbol)):
                self.change_subscription_state(('l3', symbol), 1, True)
            
            if set_event:
                self.safe_set_event('l3', symbol, {'_': 'l3',
                                                   'symbol': symbol,
                                                   'bids_count': len(d['bids']),
                                                   'aks_count': len(d['asks'])})
            
            cb_input = {'_': 'l3',
                        'symbol': symbol,
                        'data': {'symbol': symbol,
                                 'bids': list(bid_deltas.items()), 
                                 'asks': list(ask_deltas.items()),
                                 'nonce': (prev_nonce, d.get('nonce'))}}
            self.exec_callbacks(cb_input, 'l3', symbol)
            cb_data.append(cb_input)
            
            if self.l3['assert_integrity']:
                assert_l3_integrity(self.orderbooks[symbol])
                
            if self.l3_maintainer.is_subbed_to_l2_ob(symbol):
                if self.orderbooks.get(symbol) is None:
                    create_l2.add(symbol)
                push_to_l2.add(symbol)
        
        if cb_data:
            if set_event:
                self.safe_set_event('l3', -1)
            self.exec_callbacks(cb_data, 'l3', -1)
        
        push_data = [x['data'] for x in cb_data if x['symbol'] in push_to_l2]
        for symbol in create_l2:
            self.l3_maintainer.assign_l2(symbol)
        if push_data:
            self.update_orderbooks(push_data, is_delta=True, enable_sub=True)
        
        # self._update_tickers_from_ob([d['symbol'] for d in data])
    
    
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
            
            if enable_sub and self.is_subscribed_to(('trades', symbol)):
                self.change_subscription_state(('trades', symbol), 1, True)
            
            if set_event:
                self.safe_set_event('trades', symbol, {'_': 'trades', 'symbol': symbol})
            
            cb_input = {'_': 'trades',
                        'symbol': symbol,
                        'data': [x.copy() for x in trades]}
            self.exec_callbacks(cb_input, 'trades', symbol)
            cb_data.append(cb_input)
            
            for t in trades:
                self.add_fill_from_trade(t, enable_sub=enable_sub)
        
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
            
            if enable_sub and self.is_subscribed_to(('ohlcv', symbol, timeframe)):
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
            if enable_sub and self.is_subscribed_to(('account',)):
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
            if not isinstance(x, dict):
                x = {'cy': x[0], 'free': x[1], 'used': x[2]}
            cy, new = parse_dict(x)
            
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
            if enable_sub and self.is_subscribed_to(('account',)):
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
    
    
    def update_balances_from_order_delta(self, o_new, o_prev={}):
        diffs = {k: (o_new[k] - o_prev[k] if o_prev.get(k) is not None else o_new[k])
                 for k in ['payout','remaining','filled','amount','cost']}
        m = self.markets[o_new['symbol']]
        direction = as_direction(o_new['side'])
        cy_in = m['quote'] if direction else m['base']
        cy_out = m['base'] if direction else m['quote']
        if o_prev.get('amount') is None:
            d_in_used = o_new['cost'] if direction else o_new['amount']
            d_in_free = -d_in_used
        else:
            if direction:
                d_in_used = -self.api.calc_cost_from_payout(
                    o_new['symbol'], direction, diffs['payout'], o_new['price'], limit=False)
            else:
                d_in_used = diffs['remaining']
            d_in_free = 0
        d_payout = diffs['payout']
        data = [(cy_in, d_in_free, d_in_used),
                (cy_out, d_payout, 0)]
        self.update_balances_delta(data)
    
    
    def add_order(self, id, symbol, side, amount, price=None, timestamp=None, 
                  remaining=None, filled=None, payout=0, datetime=None, type='limit',
                  stop=None, cost=None, average=None, lastTradeTimestamp=None,
                  params={}, *, set_event=True, enable_sub=False):
        if remaining is None:
            remaining = amount
        if filled is None and remaining is not None:
            filled = amount - remaining
        if payout is None and filled==0:
            payout = 0
        
        datetime, timestamp = resolve_times([datetime, timestamp])
        
        if price == 0:
            price = None
        
        if id in self.orders:
            params = dict({'type': type, 'side': side, 'price': price,
                           'amount': amount, 'stop': stop, 'timestamp': timestamp,
                           'datetime': datetime},
                           **params)
            return self.update_order(id=id, remaining=remaining, filled=filled, payout=payout,
                                     cost=cost, average=average, lastTradeTimestamp=lastTradeTimestamp,
                                     params=params)
        
        # filled, payout, remaining calculated from fills
        cum = {'f': .0, 'r': remaining, 'p': .0}
        
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
             'payout': payout,
             'lastTradeTimestamp': lastTradeTimestamp,
             'lastUpdateTimestamp': self.api.milliseconds(),
             'previousIds': [],
             'cum': cum,
            }, **params)
        
        for _id in [id] + o['previousIds']:
            self.orders[_id] = o
            if remaining:
                self.open_orders[_id] = o
        
        if self.is_order_balance_updating_enabled(o):
            try: self.update_balances_from_order_delta(o)
            except Exception as e:
                logger.exception(e)
        
        if enable_sub:
            self._change_account_and_own_market_state(symbol, 'order')
        
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
                self.log2('processing fill {} from unprocessed_fills'.format(kwargs))
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
        KEYWORDS = ['id','remaining','filled','payout','cost','average','lastTradeTimestamp']
        kw = {k: d[k] for k in KEYWORDS if k in d}
        params = {k: d[k] for k in d if k not in KEYWORDS}
        
        self.update_order(**kw, params=params, set_event=set_event, enable_sub=enable_sub)
    
    
    def update_order(self, id, remaining=None, filled=None, payout=None, cost=None, average=None,
                     lastTradeTimestamp=None, params={}, *, from_fills=False, set_event=True,
                     enable_sub=False):
        was_dict = isinstance(id, dict)
        if was_dict:
            o = self.api.extend(dict.fromkeys(self.ORDER_KEYWORDS), _copy.deepcopy(id))
            id = o['id']
            if o.get('cum') is None:
                o['cum'] = {'f': .0, 'r': o['remaining'], 'p': .0}
            if o.get('previousIds') is None:
                o['previousIds'] = []
            if 'info' not in o:
                o['info'] = None
        elif id in self.orders:
            o = self.orders[id]
        else:
            self.log_error('not recognized order: {}'.format(id))
            return
        
        if 'price' in params and params['price']==0:
            params = dict(params, price=None)
        
        o_prev = _copy.deepcopy(o)
        amount_difference = None
        
        if params.get('amount') is not None and o['amount'] is not None:
            amount_difference = params['amount'] - o['amount']
        
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
            if amount_difference not in (None, 0):
                return new
            else:
                return min(prev, new)
        
        
        def cumulate(d, r=remaining, f=filled, p=payout, c=cost, l=lastTradeTimestamp):
            for name,al,value,op in [('remaining', 'r', r, modify_remaining),
                                     ('filled', 'f', f, max),
                                     ('payout', 'p', p, max),
                                     ('cost', 'c', c, max),
                                     ('lastTradeTimestamp', 'l', l, max)]:
                which = name if name in d else (al if al in d else None)
                if not which:
                    continue
                prev = d[which]
                if prev is not None and value is not None:
                    value = op(prev, value)
                if value is not None:
                    d[which] = value
        
        if from_fills:
            cumulate(o['cum'])
        else:
            cumulate(o)
        
        cumulate(o, *[o['cum'][x] for x in ['r','f','p']])
        
        o['lastUpdateTimestamp'] = self.api.milliseconds()
        
        if was_dict:
            return o
        
        if not o['remaining']:
            for _id in [id] + o['previousIds']:
                try:
                    del self.open_orders[_id]
                except KeyError:
                    pass
        
        if self.is_order_balance_updating_enabled(o):
            try: self.update_balances_from_order_delta(o, o_prev)
            except Exception as e:
                logger.exception(e)
        
        if enable_sub:
            self._change_account_and_own_market_state(o['symbol'], 'order')
        
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
    
    
    def close_order_assume_filled(self, order):
        o = order if isinstance(order, dict) else self.orders[order]
        average = o['average']
        payout = o['payout']
        cost = o['cost']
        if o['price'] is not None and o['remaining']:
            added_payout = self.api.calc_payout(o['symbol'], o['side'], o['remaining'], o['price'])
            payout = payout + added_payout if payout is not None else added_payout
            added_cost = o['price'] * o['remaining']
            cost = cost + added_cost if cost is not None else added_cost
            average = cost / o['amount']
        return self.update_order(order, remaining=0, filled=o['amount'], payout=payout,
                                 cost=cost, average=average, enable_sub=True)
    
    
    def add_fill(self, id, symbol, side, price, amount, timestamp=None,
                 datetime=None, order=None, type=None, payout=None,
                 fee=None, fee_rate=None, takerOrMaker=None, cost=None,
                 params={}, *, set_event=True, set_order_event=True,
                 enable_sub=False, warn=True):
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
        
        # Overwriting a fill is not preferable, as it can mess up
        # "filled", "remaining" and "payout" of the fill's order
        if loc is not None:
            if warn:
                self.log2('fill {} already registered.'
                          .format((id, symbol, side, price, amount, order)))
            if enable_sub:
                self._change_account_and_own_market_state(symbol, 'fill')
            return
        
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
            
            is_new = not o['lastTradeTimestamp'] or o['lastTradeTimestamp'] < f['timestamp']
            
            if is_new and self.order['update_lastTradeTimestamp_on_fill']:
                o_params['lastTradeTimestamp'] = f['timestamp']
            
            #if is_new:
            if self.order['update_filled_on_fill']:
                o_params['filled'] = o['cum']['f'] + amount
            
            if self.order['update_payout_on_fill']:
                payout = self.api.calc_payout(symbol,o['side'],amount,price,fee_rate,fee=fee) \
                    if payout is None else payout
                o_params['payout'] = o['cum']['p'] + payout
            
            if self.order['update_remaining_on_fill']:
                o_params['remaining'] = o['cum']['r'] - amount
            
            if o_params:
                self.update_order(order, **o_params, from_fills=True, set_event=set_order_event)
        
        elif not any(x['id']==id and x['symbol']==symbol for x in self.unprocessed_fills[order]):
            self.log2('adding {} to unprocessed_fills'
                      .format((id, symbol, type, side, price, amount, order)))
            self.unprocessed_fills[order].append(
                {'id': id, 'symbol': symbol, 'side': side, 'price': price, 'amount': amount,
                 'fee_rate': fee_rate, 'timestamp': timestamp, 'order': order, 'payout': payout,
                 'fee': fee, 'datetime': datetime, 'type': type, 'takerOrMaker': takerOrMaker,
                 'cost': cost, 'params': params, 'set_event': set_event,
                 'set_order_event': set_order_event})
        
        if enable_sub:
            self._change_account_and_own_market_state(symbol, 'fill')
    
    
    def add_fill_from_dict(self, d, *, set_event=True, set_order_event=True,
                           enable_sub=False, drop=None, warn=True):
        if drop is not None:
            d = self.drop(d, drop)
        kw = {k: d[k] for k in self.FILL_KEYWORDS if k in d}
        params = {k: d[k] for k in d if k not in self.FILL_KEYWORDS}
        
        self.add_fill(**kw, params=params, set_event=set_event,
                      set_order_event=set_order_event, enable_sub=enable_sub, warn=warn)
    
    
    def add_fill_from_trade(self, t, *, set_event=True, set_order_event=True, enable_sub=False, warn=False):
        orders = t.get('orders')
        if not orders:
            return
        for order, takerOrMaker in zip(orders, ['maker', 'taker']):
            if order not in self.orders:
                continue
            cost, fee = t.get('cost'), t.get('fee') if takerOrMaker==t.get('takerOrMaker') else None, None
            fill = dict(t, order=order, takerOrMaker=takerOrMaker, cost=cost, fee=fee)
            self.add_fill_from_dict(fill, set_event=set_event, set_order_event=set_order_event,
                                    enable_sub=enable_sub, warn=warn)
    
    
    def _change_account_and_own_market_state(self, symbol, fillOrOrder):
        if self.is_subscribed_to(('account',)) and self.has_got('account', fillOrOrder):
            self.change_subscription_state(('account',), 1, True)
        if self.is_subscribed_to(('own_market', symbol)):
            self.change_subscription_state(('own_market', symbol), 1, True)
    
    
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
        # (except for orderbook & l3, that is handled by .orderbook_maintainer / .l3_maintainer)
        _map = {'all_tickers': 'fetch_tickers',
                'ticker': 'fetch_ticker',
                'trades': 'fetch_trades',
                'ohlcv': 'fetch_ohlcv',
                'account': 'fetch_balance',}
        
        attr = _map.get(s.channel)
        if (s.state and not prev_state and
                attr is not None and self.has_got(attr)):
            corofunc = getattr(self, attr)
            self.log('fetching data for {}'.format(s))
            call_via_loop(corofunc, args=s.id_tuple[1:], loop=self.loop)
    
    
    def delete_data(self, s, prev_state):
        _map = {
            'all_tickers': ['tickers'],
            'ticker': ['tickers'],
            'account': ['balances','positions'],
            'orderbook': ['orderbooks'],
            'l3': ['l3_books'],
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
        
        if s.channel in ('orderbook','l3'):
            symbol = s.id_tuple[1]
            maintainer = getattr(self, s.channel+'_maintainer')
            maintainer.is_synced[symbol] = False
    
    
    @staticmethod
    def _is_poll(emulated):
        def is_item_poll(emul_item):
            if not isinstance(emul_item, str):
                return emul_item
            else:
                return emul_item=='poll' or emul_item.startswith('fetch')
        if isinstance(emulated, list):
            return any(is_item_poll(x) for x in emulated)
        else:
            return is_item_poll(emulated)
    
    
    def _is_time(self, method, id):
        interval = deep_get([self.intervals], method, return2=self.intervals['default'])
        return time.time() - self._last_fetches[id] > interval
    
    
    async def poll_loop(self):
        """Poll everything that can't be accessed via websocket"""
        def get_emulated_subs():
            return [s for s in self.subscriptions if self.has_got(s.channel, 'emulated') and
                    self._is_poll(self.has[s.channel]['emulated'])]
        
        def change_states():
            now = time.time()
            # disactivate subscriptions that have missed N fetch intervals
            for s in get_emulated_subs():
                if s.channel not in methods:
                    continue
                id = s.id_tuple + (methods.get(s.channel),)
                mmi = deep_get([self.max_missed_intervals], methods.get(s.channel),
                                return2=self.max_missed_intervals['default'])
                interval = deep_get([self.intervals], methods.get(s.channel),
                                     return2=self.intervals['default'])
                if now - self._last_fetches[id] > mmi*interval:
                    self.change_subscription_state(s, 0)
                    symbol = s.params.get('symbol')
                    if s.channel=='l3' and self.l3_maintainer.is_subbed_to_l2_ob(symbol):
                        self.change_subscription_state(('orderbook', symbol), 0)
        
        methods = {
            'all_tickers': 'fetch_tickers',
            'ticker': 'fetch_ticker',
            'orderbook': 'fetch_order_book',
            'trades': 'fetch_trades',
            'l3': 'fetch_l3_order_book',
        }
        pending = []
        sleep = 0.1
        while self.is_running():
            start = time.time()
            coros = {}
            change_states()
            
            while pending:
                _, pending = await asyncio.wait(pending, timeout=sleep)
                change_states()
            
            account_sub  = self.get_subscription(('account',)) if self.is_subscribed_to(('account',)) else None
            own_market_subs = [s for s in self.subscriptions if s.channel=='own_market']
            
            if account_sub:
                id_bal = ('fetch_balance',)
                if self._is_poll(self.has['account']['balance']['emulated'])\
                            and self._is_time('fetch_balance', id_bal):
                    coros['balance'] = self.poll_fetch_and_activate(account_sub, 'fetch_balance', id=id_bal)
            
            if account_sub or own_market_subs:  
                coros['order'] = self.poll_orders()
            
            for s in get_emulated_subs():
                method = methods.get(s.channel)
                id = s.id_tuple + (method,)
                if method and self._is_time(method, id):
                    coros[s.id_tuple] = self.poll_fetch_and_activate(s, method, id=id)
            
            pending = [asyncio.ensure_future(c) for c in coros.values()]
            undertime = max(0, sleep - (time.time() - start))
            if not pending:
                await asyncio.sleep(sleep)
            elif undertime:
                await asyncio.sleep(undertime)
    
    
    async def poll_orders(self):
        
        def get_order_fetch_methods():
            methods = []
            if self.has_got('fetch_my_trades'):
                methods = ['fetch_my_trades']
            elif self.has_got('fetch_orders'):
                methods = ['fetch_orders']
            elif self.has_got('fetch_open_orders'):
                if self.has_got('fetch_order'):
                    methods = ['fetch_open_orders', 'fetch_order']
                elif self.has_got('fetch_closed_orders'):
                    methods = ['fetch_open_orders', 'fetch_closed_orders']
                else:
                    methods = ['fetch_open_orders'] # this is going to be a bit tricky one
            elif self.has_got('fetch_order'):
                methods = ['fetch_order']
            return methods
        
        has_account_order_emul = self._is_poll(self.has['account']['order']['emulated'])
        has_own_market_order_emul = self._is_poll(self.has['own_market']['order']['emulated'])
        
        def get_related_subs(symbol=None):
            subs = []
            account_sub  = self.get_subscription(('account',)) if self.is_subscribed_to(('account',)) else None
            if account_sub and has_account_order_emul:
                subs.append(account_sub)
            own_market_subs = [s for s in self.subscriptions if s.channel=='own_market'
                               and (symbol is None or s.params['symbol']==symbol)]
            if has_own_market_order_emul:
                subs += own_market_subs
            return subs
        
        
        def filter_orders_by_last_update(orders, method):
            interval = deep_get([self.intervals], method, return2=self.intervals['default'])
            return [o for o in orders if time.time() - o['lastUpdateTimestamp']/1000 > interval]
        
        
        def sched_method(method, orders_to_be_probed):
            coros = {}
            _id = lambda *args: (method,) + args
            all_subs = get_related_subs()
            if method == 'fetch_order':
                for o in orders_to_be_probed:
                    subs = get_related_subs(o['symbol'])
                    _args = (o['id'], o['symbol'],)
                    if subs and self._is_time(method, _id(*_args)):
                        coros[(method, o['id'], o['symbol'])] = \
                            self.poll_fetch_and_activate(subs, method, _args, _id(*_args))
            elif self.has_got(method, 'symbolRequired'):
                symbols = unique((o['symbol'] for o in orders_to_be_probed
                                  if self._is_time(method, _id(o['symbol']))), astype=list)
                for symbol in symbols:
                    subs = get_related_subs(symbol)
                    if subs:
                        coros[(method, None, symbol)] = \
                            self.poll_fetch_and_activate(subs, method, (symbol,), _id(symbol))
            elif all_subs and self._is_time(method, _id(None)) and orders_to_be_probed:
                coros[(method, None, None)] = self.poll_fetch_and_activate(all_subs, method, id=_id(None))
            return coros
        
        async def exec_coros(coros):
            if not coros:
                return [], []
            nametuples_by_futs = {asyncio.ensure_future(coro): name for name, coro in coros.items()}
            done, _ = await asyncio.wait(nametuples_by_futs.keys())
            successful, unsuccessful = [], []
            for f in done:
                nt = nametuples_by_futs[f]
                try:
                    if f.result() is None:
                        successful.append(nt)
                    else:
                        unsuccessful.append(nt)
                except Exception as e:
                    self.log_error('error occurred', e)
                    unsuccessful.append(nt)
            return successful, unsuccessful
        
        methods = get_order_fetch_methods()
        orders_to_be_probed = list(self.open_orders.values())
        luts = {o['id']: o['lastUpdateTimestamp'] for o in orders_to_be_probed}
        
        for i, method in enumerate(methods):
            # make sure that individual orders are not be updated too frequently
            orders_to_be_probed = filter_orders_by_last_update(orders_to_be_probed, method)
            coros = sched_method(method, orders_to_be_probed)
            successful, unsuccessful = await exec_coros(coros)
            if i==0 and not successful:
                # fetch_order / fetch_closed orders is only allowed to be polled if fetch_open_orders 
                # has been polled first
                break
            # on 2nd loop probe orders on which "fetch_open_orders" didn't raise error AND which it didn't touch
            # (i.e. were closed in the meanwhile)
            orders_to_be_probed = [o for o in orders_to_be_probed if any(nt[2] is None or nt[2]==o['symbol'] for nt in successful)
                                   and o['lastUpdateTimestamp'] == luts.get(o['id'])]
            # This does some assuming (actual payout and cost could be larger)
            if methods == ['fetch_open_orders']:
                for o in orders_to_be_probed:
                    if o['id'] not in self._cancel_scheduled:
                        self.close_order_assume_filled(o['id'])
    
    
    async def poll_fetch_and_activate(self, subscription, method, args=None, id=None):
        subs = [subscription] if isinstance(subscription, Subscription) else list(subscription)
        if args is None:
            args = subs[0].id_tuple[1:]
        if id is None:
            id = subs[0].id_tuple + (method,)
        self.log('polling {}{}'.format(method, args))
        try:
            r = await getattr(self, method) (*args)
        except Exception as e:
            self.log('error occurred while polling {}{} - {}'.format(method, args, repr(e)))
            self.log(e)
            return e
        finally:
            self._last_fetches[id] = time.time()
        for s in subs:
            # In case user has unsubscribed
            s = next((_s for _s in self.subscriptions if _s.id_tuple==s.id_tuple), None)
            if s is None:
                continue
            if s.channel in ('orderbook', 'l3'):
                getattr(self, s.channel+'_maintainer').send_orderbook(r)
            else:
                self.change_subscription_state(s, 1)
    
    
    def get_current_price(self, symbol, components=['last','bid','ask'], *, active=True, as_dict=False):
        has_tAll_bidAsk = self.has_got('all_tickers', ('bid','ask')) # must have both bid and ask
        has_t_bidAsk = self.has_got('ticker', ('bid','ask'))
        has_tAll_last = self.has_got('all_tickers','last')
        has_t_last = self.has_got('ticker','last')
        is_subbed_ob = self.is_subscribed_to(('orderbook', symbol), active)
        is_subbed_t = self.is_subscribed_to(('ticker', symbol), active)
        is_subbed_tAll = self.is_subscribed_to(('all_tickers',), active)
        prices = dict.fromkeys(['last','bid','ask'])
        t = self.tickers.get(symbol, {})
        ob = self.orderbooks.get(symbol)
        if is_subbed_ob and ob:
            prices['bid'], prices['ask'] = get_bidask(ob)
        elif has_tAll_bidAsk and is_subbed_tAll or has_t_bidAsk and is_subbed_t:
            prices.update({x: t.get(x) for x in ['bid','ask']})
        if has_tAll_last and is_subbed_tAll or has_t_last and is_subbed_t:
            prices['last'] = t.get('last')
        prices = {k: v for k,v in prices.items() if k in components}
        if as_dict:
            return prices
        prices = {k: v for k,v in prices.items() if v is not None}
        if not prices:
            return None
        return sum(prices.values()) / len(prices)
    
    
    def get_markets_with_active_tickers(self, last=True, bidAsk=True):
        has_tAll = self.has_got('all_tickers')
        has_tAll_bidAsk = self.has_got('all_tickers', ('bid','ask'))
        has_t = self.has_got('ticker')
        has_t_bidAsk = self.has_got('ticker', ('bid','ask'))
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
        return self.subscribe_to(
            self.ip.extend({
                '_': 'ticker',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_ticker(self, symbol):
        return self.unsubscribe_to(
            {
                '_': 'ticker',
                'symbol': symbol,
            })
    
    
    def subscribe_to_all_tickers(self, params={}):
        #Fetch all tickers before enabling state
        return self.subscribe_to(
            self.ip.extend({
                '_': 'all_tickers',
            }, params))
    
    
    def unsubscribe_to_all_tickers(self):
        return self.unsubscribe_to(
            {
                '_': 'all_tickers',
            })
    
    
    def subscribe_to_orderbook(self, symbol, params={}):
        return self.subscribe_to(
            self.ip.extend({
                '_': 'orderbook',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_orderbook(self, symbol):
        return self.unsubscribe_to(
            {
                '_': 'orderbook',
                'symbol': symbol,
            })
    
    
    def subscribe_to_trades(self, symbol, params={}):
        return self.subscribe_to(
            self.ip.extend({
                '_': 'trades',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_trades(self, symbol):
        return self.unsubscribe_to(
            {
                '_': 'trades',
                'symbol': symbol,
            })
    
    
    def subscribe_to_ohlcv(self, symbol, timeframe='1m', params={}):
        if not self.has_got('ohlcv', 'timeframes', timeframe):
            raise ccxt.NotSupported("Timeframe '{}' is not supported." \
                                    " Choose one of the following: {}" \
                                    .format(timeframe, list(self.timeframes)))
        return self.subscribe_to(
            self.ip.extend({
                '_': 'ohlcv',
                'symbol': self.merge(symbol),
                'timeframe': timeframe,
            }, params))
    
    
    def unsubscribe_to_ohlcv(self, symbol):
        return self.unsubscribe_to(
            {
                '_': 'ohlcv',
                'symbol': symbol,
            })
    
    
    def subscribe_to_account(self, params={}):
        return self.subscribe_to(
            self.ip.extend({
                '_': 'account',
            }, params))
    
    
    def unsubscribe_to_account(self):
        return self.unsubscribe_to(
            {
                '_': 'account',
            })
    
    
    def subscribe_to_own_market(self, symbol, params={}):
        return self.subscribe_to(
            self.ip.extend({
                '_': 'own_market',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_own_market(self, symbol):
        return self.unsubscribe_to(
            {
                '_': 'own_market',
                'symbol': symbol,
            })
    
    
    def subscribe_to_l3(self, symbol, params={}):
        return self.subscribe_to(
            self.ip.extend({
                '_': 'l3',
                'symbol': self.merge(symbol),
            }, params))
    
    
    def unsubscribe_to_l3(self, symbol):
        return self.unsubscribe_to(
            {
                '_': 'l3',
                'symbol': symbol,
            })
    
    
    async def fetch_balance(self, params={}):
        if not self.test:
            balances = await poll.fetch(self.api, 'balances', 0, kwargs={'params': params})
        else:
            balances = await self.api.fetch_balance(params)
        self._last_fetches[('account', 'fetch_balance')] = time.time()
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
        return {'symbol': symbol, **ob, 'nonce': ob.get('nonce')}
    
    
    async def fetch_l3_order_book(self, symbol, limit=None):
        raise ccxt.NotSupported(self.exchange + ' does not support fetchL3OrderBook yet')
    
    
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
    
    
    async def fetch_order(self, id, symbol=None, params={}, *, skip=False):
        r = await self.api.fetch_order(id, symbol, params)
        self._last_fetches[('fetch_order', id, symbol)] = time.time()
        try:
            self.add_ccxt_order(r, from_method='fetch_order', skip=skip)
        except Exception as e:
            logger.exception(e)
        return r
    
    
    async def fetch_orders(self, symbol=None, since=None, limit=None, params={}, *, skip=False):
        orders = await self.api.fetch_orders(symbol, since, limit, params)
        self._last_fetches[('fetch_orders', symbol)] = time.time()
        for o in orders:
            try:
                self.add_ccxt_order(o, from_method='fetch_orders', skip=skip)
            except Exception as e:
                logger.exception(e)
        return orders
    
    
    async def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}, *, skip=False):
        orders = await self.api.fetch_open_orders(symbol, since, limit, params)
        self._last_fetches[('fetch_open_orders', symbol)] = time.time()
        for o in orders:
            try:
                self.add_ccxt_order(o, from_method='fetch_open_orders', skip=skip)
            except Exception as e:
                logger.exception(e)
        return orders
    
    
    async def fetch_closed_orders(self, symbol=None, since=None, limit=None, params={}, *, skip=False):
        orders = await self.api.fetch_closed_orders(symbol, since, limit, params)
        self._last_fetches[('fetch_closed_orders', symbol)] = time.time()
        for o in orders:
            try:
                self.add_ccxt_order(o, from_method='fetch_closed_orders', skip=skip)
            except Exception as e:
                logger.exception(e)
        return orders
    
    
    async def fetch_my_trades(self, symbol=None, since=None, limit=None, params={}):
        trades =  await self.api.fetch_my_trades(symbol, since, limit, params)
        self._last_fetches[('fetch_my_trades', symbol)] = time.time()
        for t in trades:
            try:
                self.add_fill_from_dict(t, warn=False)
            except Exception as e:
                logger.exception(e)
        return trades
    
    
    async def query_order(self, id, symbol):
        o = None
        if self.has_got('fetch_my_trades'):
            symbolRequired = self.has_got('fetch_my_trades', 'symbolRequired')
            _args = [symbol] if symbolRequired else []
            self.log('querying order {} {} via fetch_my_trades'.format(id, symbol))
            trades = await self.fetch_my_trades(*_args)
            o = {'id': id, 'symbol': symbol, 'trades': [t for t in trades if t['order']==id]}
        elif self.has_got('fetch_order', 'filled'):
            self.log('querying order {} {} via fetch_order'.format(id, symbol))
            o = await self.fetch_order(id, symbol, skip=True)
        elif self.has_got('fetch_orders', 'filled') or self.has_got('fetch_open_orders', 'filled'):
            method = next(x for x in ['fetch_orders', 'fetch_open_orders'] if self.has_got(x, 'filled'))
            symbolRequired = self.has_got(method, 'symbolRequired')
            _args = [symbol] if symbolRequired else []
            self.log('querying order {} {} via {}'.format(id, symbol, method))
            orders = await getattr(self, method)(*_args, skip=[id]) # update all except this order
            o = next((x for x in orders if x['id']==id), None)
            if o is None and method=='fetch_open_orders':
                return 'closed'
            if o is None:
                raise ccxt.ExchangeError('Order {} {} not found'.format(id, symbol))
        return o
    
    
    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        await self.load_markets()
        amount = self.api.round_amount(symbol, amount)
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        if price is not None:
            cp = self.get_current_price(symbol, as_dict=True)
            price = self.api.round_price(symbol, price, side, True, limit_divergence=True, current_price=(cp['bid'], cp['ask']))
            if not price and type!='market': # raise error as price `0` / `None` might accidentally create a market order
                raise ccxt.InvalidOrder('{} - {} order price {} is limited to 0'.format(self.name, symbol, price))
        if self.verbose and self.verbose >= 0.5:
            self._log('creating order {}'.format((symbol, type, side, amount, price, params)))
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
            order = {
                'id': None,
                'symbol': symbol,
                'type': type,
                'side': side,
                'amount': amount,
                'price': price,
                'timestamp': None,
            }
            immediate_fill = None
            try:
                immediate_fill = self.is_immediate_fill(order)
            except Exception as e:
                self.log_error('could not determine if order {} is immediate fill' .format(order), e)
            r = await self.api.create_order(symbol, type, side, amount, price, params)
            order.update({'id': r['id'], 'timestamp': self.api.milliseconds()})
            # In case the order is likely to get filled immediately, but has no "filled"
            # or "remaining" included in the create_order response, nor is subscribed to fills/order feed
            try:
                if self.is_order_querying_enabled(order, is_immediate_fill=immediate_fill):
                    q = await self.query_order(r['id'], symbol)
                    if q == 'closed':
                        _o = self.api.order_entry(**dict(order, **{k: v for k, v in r.items() if k not in order or v is not None}))
                        r = self.close_order_assume_filled(_o)
                    elif q is not None:
                        r = self.api.extend(r, q)
            except Exception as e:
                self.log_error('could not query order {} {}' .format(r['id'], symbol), e)
            
            if self.order['add_automatically']:
                self.add_ccxt_order(r, order, 'create_order')
            
            return r
    
    
    async def create_limit_order(self, symbol, side, amount, price, *args):
        return await self.create_order(symbol, 'limit', side, amount, price, *args)
    
    
    async def create_limit_buy_order(self, symbol, amount, price, *args):
        return await self.create_order(symbol, 'limit', 'buy', amount, price, *args)
    
    
    async def create_limit_sell_order(self, symbol, amount, price, *args):
        return await self.create_order(symbol, 'limit', 'sell', amount, price, *args)
    
    
    async def cancel_order(self, id, symbol=None, params={}):
        await self.load_markets()
        if self.verbose and self.verbose >= 0.5:
            self._log('canceling order {}'.format((id, symbol, params)))
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
            _symbol = symbol if symbol is not None else deep_get([self.orders], [id, 'symbol'])
            cancel_automatically = self.is_order_auto_canceling_enabled(_symbol)
            if cancel_automatically and not self._is_subbed_to_order_ws(_symbol) and self.has_only_fetch_open_orders():
                _args = [_symbol] if self.has_got('fetch_open_orders', 'symbolRequired') else []
                self.log('pre querying order {} {} via fetch_open_orders before canceling'.format(id, _symbol))
                oo = await self.fetch_open_orders(*_args)
                if not any(o['id']==id for o in oo):
                    self.log('order {} {} already closed'.format(id, _symbol))
                    self.close_order_assume_filled(id)
                    return None
            self._cancel_scheduled.add(id)
            #tlogger.debug('Canceling order: {},{}'.format(id, symbol))
            try: r = await self.api.cancel_order(id, symbol, params)
            except ccxt.OrderNotFound as e:
                error = e
            finally:
                self._cancel_scheduled.remove(id)
            # Canceling due to not being found is somewhat dangerous,
            # as the latest filled updates may not be received yet
            cancel_automatically = self.is_order_auto_canceling_enabled(_symbol)
            if cancel_automatically:
                await self.safely_mark_order_closed(id)
            if error is not None:
                raise error
            return r
    
    
    async def edit_order(self, id, symbol, *args):
        await self.load_markets()
        o_prev = self.orders.get(id, {}).copy()
        _symbol = symbol if symbol is not None else o_prev.get('symbol')
        new_type = args[0] if args and args[0] is not None else o_prev.get('type')
        new_side = args[1] if len(args) > 1 and args[1] is not None else o_prev.get('side')
        # amount
        if len(args) >= 3 and args[2] is not None:
            args = args[:2] + (self.api.round_amount(_symbol, args[2]),) + args[3:]
        # price
        if len(args) >= 4 and args[3] is not None:
            cp = self.get_current_price(symbol, as_dict=True)
            new_price = self.api.round_price(_symbol, args[3], new_side, True, limit_divergence=True, current_price=(cp['bid'], cp['ask']))
            if not new_price and new_type!='market': # raise error as price `0` / `None` might accidentally create a market order
                raise ccxt.InvalidOrder('{} - {} order price {} is limited to 0'.format(self.name, _symbol, new_price))
            args = args[:3] + (new_price,) + args[4:]
        
        if self.verbose and self.verbose >= 0.5:
            self._log('editing order {}'.format((id, symbol) + args))
        
        if self.has_got('edit_order','ws') and self.is_active():
            pms = {
                'id': id,
                'symbol': symbol,
                'args': args,    
            }
            r = (await self.send(pms, wait='default')).data
            self.check_errors(r)
            order_id = r['id'] if 'id' in r else id
            o = _copy.deepcopy(self.orders[order_id])
            return o
        else:
            order = {
                'id': id,
                'symbol': _symbol,
                'type': new_type,
                'side': new_side,
                'amount': args[2] if len(args) > 2 and args[2] is not None else None,
                'price': args[3] if len(args) > 3 else (o_prev.get('price') if new_type!='market' else None),
                'timestamp': None,
            }
            immediate_fill = None
            try:
                immediate_fill = self.is_immediate_fill(order)
            except Exception as e:
                self.log_error('could not determine if order {} is immediate fill' .format(order), e)
            try:
                r = await self.api.edit_order(id, symbol, *args)
            except ccxt.OrderNotFound as e:
                cancel_automatically = self.is_order_auto_canceling_enabled(_symbol)
                if cancel_automatically:
                    await self.safely_mark_order_closed(id)
                raise e
            # If the order id changed during edit (poloniex)
            if r.get('id') and r['id'] != id and id in self.orders:
                _o = self.orders[r['id']] = self.orders[id]
                _o['id'] = r['id']
                _o['previousIds'].insert(0, id)
                if id in self.open_orders:
                    self.open_orders[r['id']] = _o
                if id in self.fills:
                    self.fills[r['id']] = self.fills[id]
                else:
                    self.fills[r['id']] = self.fills[id] = []
            prev_amount = o_prev['remaining'] if r.get('id')!=id and o_prev.get('remaining') is not None \
                          else o_prev.get('amount')
            new_amount = order['amount'] if order.get('amount') is not None else prev_amount
            order.update({'id': r['id'] if r.get('id') else id,
                          'amount': new_amount,
                          'remaining': new_amount,
                          'timestamp': self.api.milliseconds()})
            # In case the order is likely to get filled immediately, but has no "filled"
            # or "remaining" included in the edit_order response, nor is subscribed to fills/order feed
            try:
                if self.api.has['editOrder']!='emulated' \
                        and self.is_order_querying_enabled(order, 'edit_order', is_immediate_fill=immediate_fill):
                    q = await self.query_order(order['id'], _symbol)
                    if q == 'closed':
                        _o = self.api.order_entry(**dict(order, **{k: v for k, v in r.items() if k not in order or v is not None}))
                        r = self.close_order_assume_filled(_o)
                    elif q is not None:
                        r = self.api.extend(r, q)
            except Exception as e:
                self.log_error('could not query order {} {}' .format(order['id'], _symbol), e)
            
            if self.order['add_automatically']:
                self.add_ccxt_order(r, order, 'edit_order')
            
            return r
    
    
    def add_ccxt_order(self, response, order={}, from_method='create_order', skip=None):
        parsed = self.parse_ccxt_order(response, from_method)
        parsed_filtered = {k: v for k,v in parsed.get('order', {}).items() if v is not None or k not in order}
        final_order = dict(order, **parsed_filtered)
        id = final_order.get('id')
        if hasattr(skip, '__iter__') and not isinstance(skip, str):
            skip = id in skip # skip these
        
        if not skip:
            o = self.orders.get(id)
            if from_method=='edit_order' and o and o['remaining'] and final_order.get('remaining'):
                o['remaining'] = max(0, final_order['remaining'])
                o['cum']['r'] = max(0, o['filled'] + o['remaining'] - o['cum']['f'])
            self.add_order_from_dict(final_order)
        
        for f in parsed.get('fills',[]):
            self.add_fill_from_dict(f)
    
    
    def parse_ccxt_order(self, r, *args):
        o = _copy.deepcopy(r)
        o['payout'] = None
        o['average'] = o.get('average') if o.get('average')!=0 else None
        
        if o.get('amount') is not None:
            if o.get('filled') is not None and o.get('remaining') is None:
                o['remaining'] = max(.0, o['amount'] - o['filled'])
            elif o.get('remaining') is not None and o.get('filled') is None:
                o['filled'] = max(.0, o['amount'] - o['remaining'])
        
        if o.get('status'):
            if o['status'] not in ('open','expired'):
                o['remaining'] = .0
        
        params = self.api.lazy_parse(o, ('symbol','side'), {'amount': 'filled', 'price': ['average','price']})
        
        if None not in params.values() and params['price']!=0:
            params.update({x: o[x] for x in ('takerOrMaker','fee') if o.get(x) is not None})
            try:
                o['payout'] = self.api.calc_payout(**params)
            except Exception as e:
                self.log_error('could not calculate payout for order: {}'.format(o), e)
        
        fills = o.pop('trades', [])
        if fills is None:
            fills = []
         
        return {'order': o, 'fills': fills}
    
    
    async def safely_mark_order_closed(self, id):
        """Used when OrderNotFound is raised"""
        o = self.orders[id]
        symbol = o['symbol']
        kwds = ['filled','payout','cost','average']
        if not o['remaining']:
            return
        if self.has_got('fetch_my_trades') and self.order['update_filled_on_fill']:
            self.log('final querying order {} {} via fetch_my_trades'.format(id, symbol))
            _args = [symbol] if self.has_got('fetch_my_trades', 'symbolRequired') else []
            try: await self.fetch_my_trades(*_args)
            except Exception as e:
                self.log_error('error fetching my trades to mark order {} {} as closed'.format(id, symbol), e)
            else:
                self.update_order(id, remaining=0)
        elif self.has_got('fetch_order'):
            self.log('final querying order {} {} via fetch_order'.format(id, symbol))
            try: o2 = await self.fetch_order(id, symbol, skip=True)
            except Exception as e:
                self.log_error('error fetching order {} {} to mark it as closed'.format(id, symbol), e)
            else:
                o3 = self.parse_ccxt_order(o2)['order']
                self.update_order(id, remaining=0, **{k: o3.get(k) for k in kwds})
        elif self.has_got('fetch_orders') or self.has_got('fetch_closed_orders'):
            method = 'fetch_orders' if self.has_got('fetch_orders') else 'fetch_closed_orders'
            self.log('final querying order {} {} via {}'.format(id, symbol, method))
            _args = [symbol] if self.has_got(method, 'symbolRequired') else []
            try: orders = await getattr(self, method)(*_args, skip=[id])
            except Exception as e:
                self.log_error('error invoking {} to mark {} {} as closed'.format(method, id, symbol), e)
            else:
                o2 = next((x for x in orders if x['id']==id), None)
                if o2 is not None:
                    o3 = self.parse_ccxt_order(o2)['order']
                    self.update_order(id, remaining=0, **{k: o3.get(k) for k in kwds})
        else:
            self.update_order(id, remaining=0)
    
    
    def has_only_fetch_open_orders(self, *args):
        methods = ['fetch_my_trades', 'fetch_orders', 'fetch_order', 'fetch_closed_orders']
        if any(self.has_got(x, *args) for x in methods):
            return False
        return self.has_got('fetch_open_orders', *args)
    
    
    def _is_subbed_to_order_ws(self, symbol=None):
        if self.has_got('account', 'order', 'ws'):
            return self.is_subscribed_to(('account',), True)
        elif self.has_got('own_market', 'order', 'ws'):
            return self.is_subscribed_to(('own_market', symbol), True)
        return False
    
    
    def is_order_auto_canceling_enabled(self, symbol=None):
        coa = self.order['cancel_automatically']
        if isinstance(coa, str):
            if coa == 'if-not-active':
                return not self.is_active()
            elif coa == 'if-not-subbed-to-account':
                return not self._is_subbed_to_order_ws(symbol)
            else:
                return True
        else:
            return bool(coa)
    
    
    def is_immediate_fill(self, order):
        """Includes partial fill"""
        symbol, type, side, price = order['symbol'], order['type'], order['side'], order.get('price')
        if type not in ('limit', 'market'):
            return False
        is_ob_subbed = self.is_subscribed_to(('orderbook', symbol), True)
        is_st_subbed = self.is_subscribed_to(('ticker', symbol), True)\
                       and self.has_got('ticker', ['ws','bid','ask'])
        is_at_subbed = self.is_subscribed_to(('all_tickers'), True)\
                       and self.has_got('all_tickers', ['ws','bid','ask'])
        if not is_ob_subbed and not is_st_subbed and not is_at_subbed:
            return None
        if is_ob_subbed:
            ticker = get_bidask(self.orderbooks[symbol], as_dict=True)
        else:
            ticker = self.tickers[symbol]
        fill_side = ('bid' if side=='sell' else 'ask')
        bidAsk = ticker[fill_side]
        if not bidAsk:
            return False
        if type=='market':
            return True
        if side=='sell' and price <= bidAsk or side=='buy' and price >= bidAsk:
            return True
        return False
    
    
    def is_order_querying_enabled(self, order, method='create_order', *, is_immediate_fill=None):
        """:type order: dict"""
        enable_querying = self.order['enable_querying']
        if not isinstance (enable_querying, str):
            return bool(enable_querying)
        elif enable_querying!='if-not-immediate':
            raise ValueError(enable_querying)
        symbol, type, side, price = order['symbol'], order['type'], order['side'], order.get('price')
        if self.has_got(method, 'filled'):
            return False
        elif self.has_got('account', 'order', 'ws') and self.is_subscribed_to(('account',), True):
            return False
        elif self.has_got('own_market', 'order', 'ws')\
                and self.is_subscribed_to(('own_market', symbol), True):
            return False
        if type == 'market':
            return True
        elif type != 'limit' or not price:
            return False
        if is_immediate_fill is None:
            is_immediate_fill = self.is_immediate_fill(order)
        if is_immediate_fill is None: # no active ob / ticker subscription
            return True
        return is_immediate_fill
    
    
    def is_order_conflicting(self, symbol, side, price):
        fill_side = as_ob_fill_side(side)
        sc = get_stop_condition(as_ob_fill_side(side), closed=False)
        return any(sc(price,o['price']) for o in self.open_orders.values() 
                        if o['symbol']==symbol and o['side']==fill_side)
    
    
    def is_order_balance_updating_enabled(self, order=None):
        # This needs some more thinking. lastTradeTimestamp is not sufficient.
        return False
        ################
        ub = self.order['update_balance']
        if order is None or not order['lastTradeTimestamp'] or not self._last_fetches['fetch_balance'] \
                or order['lastTradeTimestamp'] < self._last_fetches['fetch_balance'] * 1000:
            return False
        if isinstance(ub, str):
            return ub=='if-not-subbed-to-balance' and \
                (not self.has_got('balance','ws') or not self.is_subscribed_to(('account',),True))
        else:
            return bool(ub)
    
    
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
            self.log_error('force reloading markets {}'.format(error_txt))
            self._last_markets_loaded_ts = time.time()
            if self.loop.is_running():
                asyncio.ensure_future(
                    self.load_markets(reload=True, limit=0), loop=self.loop)
            else:
                self.sn_load_markets(reload=True, limit=0)
    
    
    def fill_in(self, strings, replace, replace_with, convert_function=None):
        if isinstance(strings, str):
            strings = [strings]
        
        if replace_with is None:
            replace_with = []
        elif isinstance(replace_with, (str, float, int)):
            replace_with = [replace_with]
        if convert_function is not None:
            replace_with = [convert_function(rw) for rw in replace_with]
        
        with_replace = [s for s in strings if replace in s]
        without_replace = [s for s in strings if replace not in s]
        
        replaced = without_replace[:]
        
        for rw in replace_with:
            replaced += [s.replace(replace, str(rw)) for s in with_replace]
        
        return replaced
    
    
    def fill_in_params(self, strings, symbols=None, timeframes=None, currencies=None):
        replaced = self.fill_in(strings, '<symbol>', symbols, self.convert_symbol)
        replaced = self.fill_in(replaced, '<timeframe>', timeframes, self.convert_timeframe)
        replaced = self.fill_in(replaced, '<currency>', currencies, self.convert_cy)
        replaced = self.fill_in(replaced, '<cy>', currencies, self.convert_cy)
        
        return replaced
    
    
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
            self.broadcast_event(_, b)
    
    
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
