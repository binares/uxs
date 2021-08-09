import ccxt
import ccxt.async_support
import ccxt_unmerged # initalize the new exchanges
import os
import json
import time
from copy import deepcopy
import ssl
import aiohttp
import aiohttp_socks
import asyncio

from .auth import (get_auth2, EXTRA_TOKEN_KEYWORDS, _interpret_exchange)
from . import wrappers as _wrappers
from .wrappers import async_support as _wrappers_async
from .wrappers import sync_support as _wrappers_sync
from uxs.fintls.basics import (as_direction, calc_price, convert_quotation, create_cy_graph)
from uxs.fintls.ob import create_orderbook
from uxs.fintls.utils import resolve_times
from uxs.fintls.margin import Position

from fons.dict_ops import deep_update
from fons.iter import flatten, unique
import fons.math
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

ccxt.async_support.poloniex.rateLimit = 100
ccxt.async_support.poloniex.enableRateLimit = True
QUOTE_PREFERENCE_ORDER = \
    ['BTC','ETH','USDT','USD','EUR','USDC','TUSD','SUSD','EURS','DAI','BNB','NEO']
DUST_DEFINITIONS = {
    'BTC': 0.001, 'ETH': 0.01, 'USDT': None,
    'USD': None, 'EUR': None, 'USDC': None,
    'TUSD': None, 'SUSD': None, 'EURS': None,
    'DAI': None, 'BNB': None, 'NEO': 0.5}
MAX_SPREAD_HARD_LIMIT = 0.15
DUST_SPREAD_LIMIT = 0.5
RETURN_ASYNC_EXCHANGE = True
FEE_FROM_TARGET = ['binance','luno','poloniex','southxchange']
COST_LIMIT_WITH_FEE = []
PUBLIC_AUTH_REQUIRED = ['bitclave', 'coinsuper'] # for public operations
_E_REPLACE = {
    'binancefu': 'binance',
    'btsefu': 'btse',
    'huobi':'huobipro',
    'coinbase-pro':'coinbasepro',
    'gdax':'coinbasepro',
}
_E_REPLACE_PRO = _E_REPLACE.copy()
#PRICE_ACCURACY = 3
AMOUNT_ACCURACY = 3
DATA_DIR = os.path.realpath(os.path.join(__file__, '..', '..', '_data'))

_ccxt_cls_wrapped = {}
_ccxt_cls_wrapped_async = {}
_ccxtpro_cls_wrapped = {}

_exchange_instances = {}
_exchange_instances_async = {}
_ccxtpro_instances = {}

ccxt_unmerged.warn_duplicated()


class ccxtWrapper:
    """Wraps any ccxt exchange"""
    Position = Position
    socks_proxy = None
    _wrapper_initiated = False
    
    def __init__(self, config={}, *, test=False,
                 load_cached_markets=None, profile=None, auth=None):
        import uxs.base.poll as poll
        if config is None: config = {}
        else: config = config.copy()
        if auth is None: auth = {}
        if 'apiKey' in auth: config['apiKey'] = auth['apiKey']
        if 'secret' in auth: config['secret'] = auth['secret']
        for param in flatten(EXTRA_TOKEN_KEYWORDS.values()):
            if param in auth: config[param] = auth[param]
        
        super().__init__(config)
        if self.socks_proxy is not None and not self.proxies:
            self.proxies = dict.fromkeys(['http','https'], self.socks_proxy)
        xc = get_name(self)
        self._custom_name = xc
        self._token_kwds = ['apiKey', 'secret'] + EXTRA_TOKEN_KEYWORDS.get(xc, [])
        self._auth_info = dict({x: getattr(self,x,'') for x in self._token_kwds},
                               **{x:y for x,y in auth.items() if x not in self._token_kwds})
        self._profile_name = profile
        self._synchronize_with = set()
        self._is_test = test
        if test:
            self.set_sandbox_mode(True)
        self.FEE_FROM_TARGET = self._custom_name in FEE_FROM_TARGET
        self.COST_LIMIT_WITH_FEE = self._custom_name in COST_LIMIT_WITH_FEE
        self._wrapper_initiated = True
        
        currencies = markets = None
        
        if load_cached_markets is not False:
            try: currencies = poll.load(xc,'currencies',load_cached_markets,1)[0].data
            except (IndexError, json.JSONDecodeError) as e:
                if self.verbose:
                    logger.debug('{} - could not (init)load cached currencies.'.format(xc))
                
        if load_cached_markets is not False:
            try: markets = poll.load(xc,'markets',load_cached_markets,1)[0].data
            except (IndexError, json.JSONDecodeError) as e:
                if self.verbose:
                    logger.debug('{} - could not (init)load cached markets.'.format(xc))
        
        if markets:
            self.set_markets(markets, currencies)
        self.cy_graph = self.load_cy_graph() if markets else None
    
    
    def sync_with_other(self, other):
        """
        :param other:
            another ccxtWrapper instance which's markets&currencies will be synced
            with this one
        """
        self._synchronize_with.add(other)
        other._synchronize_with.add(self)
        self._ensure_synchronization()
    
    
    def desync_with_other(self, other):
        if other in self._synchronize_with:
            self._syncronize_with.remove(other)
        if self in other._synchronize_with:
            other._synchronize_with.remove(self)
    
    
    def _ensure_synchronization(self):
        for api in self._synchronize_with:
            api.markets = self.markets
            api.markets_by_id = self.markets_by_id
            api.marketsById = self.markets_by_id
            api.symbols = self.symbols
            api.ids = self.ids
            api.currencies = self.currencies
            api.currencies_by_id = self.currencies_by_id
            api.base_currencies = self.base_currencies
            api.quote_currencies = self.quote_currencies
    
    
    def _ensure_no_nulls(self):
        _map = {'symbols': list, 'ids': list}
        for attr in ('markets','currencies','markets_by_id',
                     'symbols','ids','currencies_by_id'):
            if getattr(self, attr, None) is None:
                setattr(self, attr, _map.get(attr, dict)())
        
        self.marketsById = self.markets_by_id
    
    
    def _read_extendMarkets(self):
        path = os.path.join(DATA_DIR, '{}_markets.json'.format(get_name(self)))
        markets = {}
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                markets = json.load(f)
        return markets
    
    
    def describe(self):
        return self.deep_extend(
            super().describe(),
            {
                'options': {
                    'extendMarkets': self._read_extendMarkets(),
                },
            } 
        )
    
    
    def repeatedTry(self, f, args=None, kwargs=None, attempts=2, sleep=0.5):
        if isinstance(f,str): f = getattr(self,f)
        if args is None: args = tuple()
        if kwargs is None: kwargs = {}
            
        for i in range(attempts):
            try: return f(*args,**kwargs)
            except ccxt.ExchangeError as e:
                raise e
            except Exception as e:
                logger.exception(e)
                time.sleep(sleep)
                if i == attempts-1: raise e
    
    
    def _set_missing_market_keys(self):
        subkeys = {
            'precision': ['price', 'amount'],
            'limits': ['price', 'amount', 'cost'],
        }
        for market in self.markets.values():
            for key in subkeys:
                is_limits = (key == 'limits')
                if market.get(key) is None:
                    market[key] = {}
                for subkey in subkeys[key]:
                    if market[key].get(subkey) is None:
                        market[key][subkey] = {} if is_limits else None
                    if is_limits:
                        for minmax in ('min', 'max'):
                            if minmax not in market[key][subkey]:
                                market[key][subkey][minmax] = None
            for key in ('taker', 'maker', 'active', 'info'):
                if key not in market:
                    market[key] = None
    
    
    def _set_trading_fees(self, fees):
        if self.markets is None or fees is None:
            return
        for symbol, _fees in fees.items():
            for x in ('taker','maker'):
                if _fees.get(x) is not None and symbol in self.markets:
                    self.markets[symbol][x] = _fees[x]
    
    
    def _get_pnl_function(self, market):
        """linear, inverse, ..."""
        return None
    
    
    def _deduce_settle_currency(self, market):
        if market['linear']:
            return market['quote']
        elif market['inverse']:
            return market['base']
        else:
            return None
    
    
    def _get_settle_currency(self, market):
        return market.get('settle')
    
    
    def _get_lot_size(self, market):
        type = market.get('type')
        lotSize = None
        if market.get('lotSize') is not None:
            lotSize = market['lotSize']
        elif type is None or type=='spot':
            lotSize = 1
        return lotSize
    
    
    def _set_lot_sizes(self):
        if self.markets is None:
            return
        for market in self.markets.values():
            market['lotSize'] = self._get_lot_size(market)
    
    
    def _set_market_types(self):
        if self.markets is None:
            return
        for market in self.markets.values():
            type = market.get('type')
            if type == 'perpetual':
                type = 'swap'
            types = ('spot','swap','future','prediction')
            if type is None:
                type = next((t for t in types if market.get(t)), 'spot')
            market.update({
                'type': type,
                **{t: (type==t) for t in types}
            })
            linearities = ['linear','inverse']
            which = next((x for x in linearities if market.get(x)), None)
            if which is None:
                which = self._get_pnl_function(market)
            #if which is None and type=='spot':    # ccxt binance spot API will break if 'linear' is set to True
            #    which = 'linear'
            if which is not None:
                market.update({x: (which==x) for x in linearities})
            else:
                market.update({x: market.get(x) for x in linearities})
            market['settle'] = settle = self._get_settle_currency(market)
            if market.get('quanto') is None:
                market['quanto'] = settle not in (market['base'], market['quote']) if settle else None
    
    
    def set_markets(self, markets, currencies=None):
        import uxs.base.poll as poll
        super().set_markets(markets, currencies)
        if self.markets is None or not self._wrapper_initiated:
            return None
        self._set_missing_market_keys()
        self._set_lot_sizes()
        self._set_market_types()
        default = poll.load_profile('__default__', get_name(self), 'markets', verbose=self.verbose)
        custom = []
        if getattr(self,'_profile_name',None) is not None:
            custom = poll.load_profile(self._profile_name, get_name(self), 'markets', verbose=self.verbose)
        for item in default + custom:
            self.update_markets(item.data)
        self._ensure_synchronization()
        
        return self.markets
    
    
    def load_markets(self, reload=False):
        prev_markets = self.markets
        super().load_markets(reload)
        if (not prev_markets or reload) and self.has['fetchTradingFees']:
            try:
                self.load_trading_fees()
            except Exception as e:
                logger.error('{} - could not load trading fees: {}'.format(self.id, repr(e)))
                logger.exception(e)
        return self.markets
    
    
    def load_trading_fees(self, reload=False):
        if not self.markets:
            return
        no_fees = any(m.get('taker') is None or m.get('maker') is None for m in self.markets.values())
        if (no_fees or reload):
            fees = self.fetch_trading_fees()
            self._set_trading_fees(fees)
    
    
    def poll_load_markets(self, limit=None):
        import uxs.base.poll as poll
        return poll.sn_load_markets(self, limit)
    
    
    def fetch_markets(self, params={}):
        markets = super().fetch_markets(params)
        for i in range(len(markets)):
            markets[i] = self.deep_extend(markets[i], self.safe_value(self.options['extendMarkets'], markets[i]['symbol'], {}))
        return markets
    
    
    def update_markets(self, changed, deep=True, dismiss_new=True):
        """{symbol: {taker: x, maker: y}}"""
        from_markets = {x: deepcopy(y) for x,y in changed.items() if '/' in x}
        from_quote_cys = {}
        from_all = {}
        
        if '__all__' in changed:
            _dict = changed['__all__']
            from_all.update({m: deepcopy(_dict) for m in self.markets})
        
        for name,_dict in list(changed.items()):
            if '/' in name: continue
            markets = [x for x,y in self.markets.items() if y['quote']==name]
            from_quote_cys.update({m: deepcopy(_dict) for m in markets})
        #print('from_markets: {}\nfrom_quote_cys: {}\nfrom_all:{}'.format(from_markets,from_quote_cys,from_all))
        if deep:
            new = deep_update(deep_update(from_all, from_quote_cys), from_markets)
        else:
            new = dict(dict(from_all, **from_quote_cys), **from_markets)
        #print('new: {}'.format(new))
        for symbol,_dict in new.items():
            if symbol not in self.markets:
                if dismiss_new: continue
                else: self.markets[symbol] = {}
            if deep:
                deep_update(self.markets[symbol], _dict)
            else:
                self.markets[symbol].update(_dict)
    
    
    @staticmethod
    def ticker_entry(symbol=None, timestamp=None, datetime=None, high=None, low=None, bid=None, bidVolume=None,
                     ask=None, askVolume=None, vwap=None, open=None, close=None, last=None, previousClose=None,
                     change=None, percentage=None, average=None, baseVolume=None, quoteVolume=None, **kw):
        
        datetime, timestamp = resolve_times([datetime, timestamp], create=True)
            
        e = dict({
            'symbol': symbol, 
            'timestamp': timestamp, 
            'datetime': datetime,
            'high': high, 
            'low': low, 
            'bid': bid, 
            'bidVolume': bidVolume, 
            'ask': ask, 
            'askVolume': askVolume, 
            'vwap': vwap,
            'open': open, 
            'close': close, 
            'last': last, 
            'previousClose': previousClose, 
            'change': change, 
            'percentage': percentage, 
            'average': average,
            'baseVolume': baseVolume, 
            'quoteVolume': quoteVolume
        },**kw)
 
        for var in ['high', 'low', 'bid', 'bidVolume',
                    'ask', 'askVolume', 'vwap', 'open', 'close', 'last', 'previousClose',
                    'change', 'percentage', 'average', 'baseVolume', 'quoteVolume']:
            if isinstance(e[var],str):
                e[var] = float(e[var])
        
        g = {x: e[x] is not None for x in ['baseVolume', 'quoteVolume', 'vwap']}
        if not g['vwap'] and g['quoteVolume'] and g['baseVolume'] and e['baseVolume'] > 0:
            e['vwap'] = e['quoteVolume'] / e['baseVolume']
        elif not g['quoteVolume'] and g['vwap'] and g['baseVolume']:
            e['quoteVolume'] = e['vwap'] * e['baseVolume']
        elif not g['baseVolume'] and g['quoteVolume'] and g['vwap'] and e['vwap'] > 0:
            e['baseVolume'] = e['quoteVolume'] / e['vwap']
        
        if e['close'] is None and e['last'] is not None:
            e['close'] = e['last']
        
        if e['close'] is not None and e['open'] is not None:
            if e['change'] is None:
                e['change'] = e['close'] - e['open']
            if e['average'] is None:
                e['average'] = ccxt.Exchange.sum(e['close'], e['open']) / 2
            if e['open'] > 0 and e['percentage'] is None:
                e['percentage'] = e['change'] / e['open'] * 100
        
        return e
    
    
    @staticmethod
    def ob_entry(symbol=None, bids=None, asks=None, timestamp=None, datetime=None, nonce=None):
        return create_orderbook({
            'symbol': symbol,
            'datetime': datetime,
            'timestamp': timestamp,
            'bids': bids,
            'asks': asks,
            'nonce': nonce,
        })
    
    
    @staticmethod
    def trade_entry(symbol=None, timestamp=None, datetime=None, id=None, order=None, type=None,
                    takerOrMaker=None, side=None, price=None, amount=None, cost=None, fee=None,
                    orders=None, **kw):
        """{'timestamp': <int>, 'datetime': <str>, 'symbol': <str>, 'id': <str>,
             'order': <str>?, 'type': <str>, 'takerOrMaker': <str>, 'side': <str>,
             'price': <float>, 'amount': <float>, 'cost': <float>, 'fee': <float>}"""
            
        datetime, timestamp = resolve_times([datetime, timestamp])
        
        if isinstance(id, int):
            id = str(id)
        
        e = dict({
            'symbol': symbol, 
            'timestamp': timestamp, 
            'datetime': datetime,
            'id': id, 
            'order': order, 
            'type': type, 
            'takerOrMaker': takerOrMaker, 
            'side': side, 
            'price': price, 
            'amount': amount,
            'cost': cost, 
            'fee': fee,
            'orders': orders,
        },**kw)
        
        for var in ['price', 'amount', 'cost', 'fee']:
            if isinstance(e[var],str):
                e[var] = float(e[var])
                
        return e
        
    
    @staticmethod
    def ohlcv_entry(timestamp=None, open=None, high=None, low=None, close=None, volume=None):
        """volume: quoteVolume"""
        e = [timestamp, open, high, low, close, volume]
        
        for i in range(1, len(e)):
            if isinstance(e[i], str):
                e[i] = float(e[i])
                
        return e
    
    
    @staticmethod
    def balance_entry(currency=None, free=None, used=None, total=None, **kw):
        e = dict({
            'currency': currency, 
            'free': free, 
            'used': used,
            'total': total, 
        },**kw)
        
        for var in ['free', 'used', 'total']:
            if isinstance(e[var],str):
                e[var] = float(e[var])
        
        if e['total'] is not None:
            if e['free'] is None and e['used'] is not None:
                e['free'] = e['total'] - e['used']
            elif e['used'] is None and e['free'] is not None:
                e['used'] = e['total'] - e['free']
        elif e['free'] is not None and e['used'] is not None:
            e['total'] = e['free'] + e['used']
        
        return e
    
    
    @staticmethod
    def position_entry(symbol=None, timestamp=None, datetime=None, price=None, amount=None, 
                       leverage=None, liq_price=None, **kw):
        """
        :param price: current entry price
        :pram amount: current amount (negative for short)
        """
        datetime, timestamp = resolve_times([datetime, timestamp])
        
        e = dict({
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': datetime,
            'price': price,
            'amount': amount,
            'leverage': leverage,
            'liq_price': liq_price,
        },**kw)
        
        for var in ['price', 'amount', 'leverage', 'liq_price']:
            if isinstance(e[var],str):
                e[var] = float(e[var])
        
        return e
    
    
    @staticmethod
    def order_entry(id=None, symbol=None, side=None, amount=None, price=None, timestamp=None, 
                    remaining=None, filled=None, payout=None, datetime=None, type=None,
                    stop=None, cost=None, average=None, lastTradeTimestamp=None,
                    **kw):
        datetime, timestamp = resolve_times([datetime, timestamp])
        
        e = dict(
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
             'lastUpdateTimestamp': ccxt.Exchange.milliseconds(),
             'cum': None,
            }, **kw)
        
        for var in ['price', 'stop', 'amount', 'cost', 'average', 'filled',
                    'remaining', 'payout', 'lastTradeTimestamp']:
            if isinstance(e[var],str):
                e[var] = float(e[var]) if var!='lastTradeTimestamp' else int(e[var])
        
        if e['price'] == 0:
            e['price'] = None
        
        if e['remaining'] is None:
            e['remaining'] = e['amount']
        if e['filled'] is None and e['remaining'] is not None:
            e['filled'] = e['amount'] - e['remaining']
        if e['payout'] is None and e['filled']==0:
            e['payout'] = .0
        
        if e['cum'] is None:
            # filled, payout, remaining calculated from fills
            e['cum'] = {'f': .0, 'r': e['remaining'], 'p': .0}
        
        return e
    
    
    def lazy_parse(self, source, keywords=[], map={}, apply={}):
        """
        Retrieve keywords and the values from the source dict
        :type source: dict
        :param keywords: keywords to be retrieved
        :param map: {key: mapped_key(s)}
                    extra keywords that are present in source in another (mapped) form
                    each key may map to many keys (first no-None value is selected),
                    in which case pass the mapped keys as list/tuple
        :param apply: functions to call on the retrieved values,
                      (the output values will be returned instead)
        """
        final = {}
        all_keywords = list(unique(list(keywords) + list(map)))
        for k in all_keywords:
            if k in map and map[k] is None:
                continue
            source_keys = map.get(k, [k])
            if not isinstance(source_keys, (list, tuple)):
                source_keys = [source_keys]
            v = None
            for source_key in source_keys:
                v = source.get(source_key)
                if v is not None:
                    break
            if k in apply:
                v = apply[k](v)
            final[k] = v
        return final
    
    
    def convert_order_input(self, symbol, side, amount, price, takerOrMaker='taker',
                             quotation='base', as_type=None, type='limit', *,
                             balance=None, full=True, **kw):
        """Rounds amount and price.
           If `as_type` is not specified, `amount` is presumed to be:
               buy + quote -> cost
               buy + base -> size
               sell + base -> size
               sell + quote -> payout
           `quotation`: 'base'/'quote'/'target'/'destination'/'source'
           If balance provided and order size exceeds it, reduces the size.
           Set `full` to True to calculate cost and payout as well.
           `**kw`: ['method'] (`method=None` can only be given if as_type resolves to 'size')"""
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        if as_type is None:
            as_type = self._decode_as_type(direction, quotation)
        if type not in ('limit','market'):
            raise ValueError(type)
        if takerOrMaker not in ('taker','maker'):
            raise ValueError(takerOrMaker)
        if as_type not in ('cost','size','payout'):
            raise ValueError(as_type)
        if 'method' in kw and kw['method'] is None and as_type!='size':
            raise ValueError("`method` cannot be None if as_type=={}".format(as_type))
        
        _price = self.round_price(symbol, price, side, True)
        p_valid = _price if _price is not None and _price > 0 else price
        price = _price
        
        def _round_size(symbol, direction, amount, price, takerOrMaker, quotation, **kw):
            return self._prepare_order_size(symbol, direction, amount, price, quotation, True, **kw)
        
        _map = {'size': _round_size,
                'cost': self.calc_order_size_from_cost,
                'payout': self.calc_order_size_from_payout}
        
        size = _map[as_type](symbol, direction, amount, p_valid, takerOrMaker, quotation, **kw)
        
        cost = _cost = payout = None
        
        if balance is not None:
            _cost = self.calc_cost_from_order_size(symbol, direction, size, p_valid,
                                                   takerOrMaker, method=None)
            if side == 'buy':
                bal =  balance['free'].get(symbol.split('/')[1], 0)
            else:
                bal = balance['free'].get(symbol.split('/')[0], 0)

            if bal < _cost:
                _quotation = 'quote' if side=='buy' else 'base'
                _taker = self.markets[symbol]['taker']
                _maker = self.markets[symbol]['maker']
                _takerOrMaker = 'taker' if _taker >= _maker else 'maker'
                # `takerOrMaker` and `method` must be "taker" and "truncate" (to not exceed the balance limit)
                size = _map['cost'](symbol, direction, bal, p_valid, _takerOrMaker, _quotation,
                                    method='truncate', accuracy=None)
                _cost = None
                
        if full:
            cost = self.calc_cost_from_order_size(symbol, direction, size, p_valid, 
                                                  takerOrMaker, method=None) \
                                                                if _cost is None else _cost
            payout = self.calc_payout_from_order_size(symbol, direction, size, p_valid, 
                                                      takerOrMaker, method=None)
            
        return {'amount': size, 'price': price, 'cost': cost, 'payout': payout}
    
    
    @staticmethod
    def quoteToBase(amount, price):
        return amount/price
    
    @staticmethod
    def baseToQuote(amount, price):
        return amount*price
    
    @staticmethod
    def round_entity(x, precision, method='round', **kw):
        accuracy = AMOUNT_ACCURACY if 'accuracy' not in kw else kw['accuracy']
        if kw.get('precisionMode') == ccxt.TICK_SIZE:
            ccxt_method = {
                'round': ccxt.ROUND,
                'up': ccxt.ROUND,
                'ceil': ccxt.ROUND,
                'truncate': ccxt.TRUNCATE,
                'down': ccxt.TRUNCATE,
                'floor': ccxt.TRUNCATE,
                }[method]
            round_x_str = ccxt.decimal_to_precision(x, ccxt_method, precision, ccxt.TICK_SIZE)
            round_x = float(round_x_str)
            #in case it was rounded down, but "up" was requested
            if method in ('up','ceil') and round_x < x:
                round_x_str = ccxt.decimal_to_precision(round_x+precision*1.000000001, ccxt_method, precision, ccxt.TICK_SIZE)
                round_x = float(round_x_str)
            return round_x
        else:
            return fons.math.round.round(x, precision, method, accuracy)
            
        """factor = pow(10,precision)
        return f(x*factor)/factor"""
    
    
    def volume_step(self, symbol, initial_volume, sign=1, n=1):
        inf = self.markets[symbol]
        step = pow(10,-inf['precision']['amount'])
        return self.round_amount(symbol, initial_volume + sign*n*step)
    
    
    def price_step(self, symbol, initial_price, side='buy', n=1, where='inwards'):
        if where not in ('inwards','inw','outwards','outw'):
            raise ValueError(where)
        #step *inwards*
        inf = self.markets[symbol]
        if self.precisionMode == ccxt.TICK_SIZE:
            step = inf['precision']['price']
        else:
            step = pow(10,-inf['precision']['price'])
        direction = as_direction(side)
        sign = -1 if direction else 1
        if where in ('outwards','outw'):
            sign *= -1
        return self.round_price(symbol, initial_price + sign*n*step)
    
    
    def get_amount_limits(self, symbol, price):
        inf = self.markets[symbol]
        min_v = inf['limits']['amount']['min']
        max_v = inf['limits']['amount']['max']
        #Some may raise KeyError (e.g. poloniex on cost[max]. bittrex on cost)
        min_qv_items= [(inf['limits'].get('cost',{}).get('min'), price, False)]
        max_qv_items = [(inf['limits'].get('cost',{}).get('max'), price, False)]
        min_xv = inf['limits'].get('cost_in_x',{}).get('min')
        max_xv = inf['limits'].get('cost_in_x',{}).get('max')
        xcy = inf['limits'].get('cost_in_x',{}).get('cy')
        cys = symbol.split('/')
        #for min/max _ qv/xv is assumed that .FEE_FROM_TARGET == False (i.e. fee always taken from quote)
        if xcy is None or len(cys)>1 and cys[1]==xcy:
            min_qv_items.append((min_xv,price,False))
            max_qv_items.append((max_xv,price,False))
            
        elif (min_xv is not None or max_xv is not None) and getattr(self,'tickers',None):
            cys = symbol.split('/')
            prices = {x: y['last'] for x,y in self.tickers.items()}
            price_in_x = None
            for ln in range(2,4):
                try: price_in_x = calc_price((cys[0],xcy),prices,self.load_cy_graph(),ln)
                except RuntimeError: continue
            #print('{} - price_in_x: {} {}'.format(symbol, price_in_x, (min_xv, max_xv, xcy)))
            #Should these be readjusted?
            if price_in_x is not None:
                min_qv_items.append((min_xv,price_in_x,False))
                max_qv_items.append((max_xv,price_in_x,False))
        
        min_from_xv, max_from_xv = [],[]
        
        for add_to,items,method in [(min_from_xv,min_qv_items,'up'),(max_from_xv,max_qv_items,'truncate')]:
            for qv_limit,price_in_x,readjust in items:
                if qv_limit is None: continue
                limit = self.calc_order_size_from_cost(symbol, 'buy', qv_limit, price_in_x,
                                                       quotation='quote', _for_round_amount=True, method=method)
                #print(symbol, qv_limit, price_in_x, qv_limit/price_in_x, limit, limit*price_in_x)
                if readjust:
                    cur_price = self.tickers[symbol]['last']
                    limit = self.round_amount(symbol, limit * cur_price/price, method=method)
                add_to.append(limit)
        
        if min_v is None and inf['precision']['amount'] is not None:
            min_v = pow(10,-inf['precision']['amount'])
         
        try: _min = max([x for x in [min_v] + min_from_xv if x is not None])
        except ValueError: _min = None
        
        try: _max = min([x for x in [max_v] + max_from_xv if x is not None])
        except ValueError: _max = None
        
        return {'min': _min, 'max': _max}
    
    
    def round_amount(self, symbol, amount, price=None, limit=False, *, method='truncate', **kw):
        inf = self.markets[symbol]
        if 'precisionMode' not in kw:
            kw['precisionMode'] = self.precisionMode
        if limit:
            if price is None:
                raise ValueError('`price` must not be `None` if `limit` is set to `True`')
            limits = self.get_amount_limits(symbol, price)
            if limits['max'] is not None:
                amount = min(amount, limits['max'])
            if amount < limits['min']:
                amount = 0
                
        return self.round_entity(amount, inf['precision']['amount'], method, **kw)


    def round_price(self, symbol, price, direction=None, limit=False, *, method=None,
                    limit_divergence=False, current_price=None):
        if direction is not None:
            direction = as_direction(direction)
        if method is None:
            method = 'round' if direction is None else ['up','down'][direction]
        inf = self.markets[symbol]
        precision = inf['precision']['price']
        p_round = self.round_entity(price, precision, method, precisionMode=self.precisionMode)
        if limit_divergence:
            if direction is None:
                raise ValueError('`direction` must not be `None` if `limit_divergence` is set to `True`')
            if current_price is None:
                raise ValueError('`current_price` must not be `None` if `limit_divergence` is set to `True`')
            p_round = self.limit_price_by_divergence(symbol, direction, price, current_price)
        if limit:
            if direction is None:
                raise ValueError('`direction` must not be `None` if `limit` is set to `True`')
            min_p = inf['limits']['price']['min']
            max_p = inf['limits']['price']['max']
            if min_p is None:
                min_p = precision if self.precisionMode==ccxt.TICK_SIZE else pow(10,-precision)
            if min_p is not None and p_round < min_p:
                p_round = None if direction else min_p
            if max_p is not None and p_round is not None and p_round > max_p:
                p_round = max_p if direction else None
                
        return p_round
    
    
    def limit_price_by_divergence(self, symbol, direction, price, current_price=None):
        direction = as_direction(direction)
        side = ['sell','buy'][direction]
        maxDivergence = self.options.get('maxDivergence')
        if not isinstance(maxDivergence, dict) or maxDivergence.get(side) is None or current_price is None:
            return price
        divergence = maxDivergence[side]
        if isinstance(current_price, (float, int)):
            # assume the current price is `ask` in the worse case scenario, resulting in larger spread
            bid = self.price_step(symbol, current_price, 'buy', where='inwards')
            ask = self.price_step(symbol, bid, 'buy', where='outwards')
        else:
            bid, ask = current_price
            current_price = ask if direction else bid
        if current_price is None:
            return price
        minTimesSpread = maxDivergence.get('minTimesSpread')
        if bid is not None and ask is not None and minTimesSpread is not None:
            spread = ask/bid - 1
            divergence = max(minTimesSpread * spread, divergence)
        precision = self.markets[symbol]['precision']['price']
        if direction:
            div_price = self.round_entity(current_price * (1 + divergence), precision, 'down', precisionMode=self.precisionMode)
            price = min(price, div_price)
        else:
            div_price = self.round_entity(current_price * (1 - divergence), precision, 'up', precisionMode=self.precisionMode)
            price = max(price, div_price)
        return price
    
    
    @property
    def calc_cost(self):
        return self.calc_cost_from_order_size
    
    
    def calc_cost_from_order_size(self, symbol, side, amount, price, takerOrMaker='taker',
                                  quotation='base', *, method='truncate', **kw):
        direction = as_direction(side)
        fee_rate = self.markets[symbol][takerOrMaker]
        amount = self._prepare_order_size(symbol, direction, amount, price, quotation, True, method, **kw)
            
        if direction:
            cost = self.baseToQuote(amount, price)
            if not self.FEE_FROM_TARGET:
                #osq + osq*fee_rate = qcost
                #qcost = .baseToQuote(os)*(1+fee_rate)
                cost *= (1+fee_rate)
        else:
            cost = amount
        
        return cost
    
    
    def calc_order_size_from_cost(self, symbol, side, amount, price, takerOrMaker='taker',
                                  quotation='base', *, _round_w_price=True, _for_round_amount=False,
                                  method='truncate', **kw):
        """For reverse engineering (cost->size) use method='round'"""
        direction, is_quote = self._decode_params(side, quotation)
        if is_quote:
            amount = self.quoteToBase(amount, price)
        size = amount

        if direction:
            fee_rate = self.markets[symbol][takerOrMaker]
            
            if not self.FEE_FROM_TARGET and (not _for_round_amount or self.COST_LIMIT_WITH_FEE):
                #osq + osq*fee_rate = qcost | * (1/price)
                #os + os*fee_rate = qcost/price
                #os = .quoteToBase(qcost)/(1+fee_rate)
                size /= (1+fee_rate)
                
        if _for_round_amount:
            _round_w_price = False
        extra = [price,True] if _round_w_price else []

        size = self.round_amount(symbol, size, *extra, method=method, **kw)
            
        return size
    
    
    @property
    def calc_payout(self):
        return self.calc_payout_from_order_size
    
    
    def calc_payout_from_order_size(self, symbol, side, amount, price, takerOrMaker='taker',
                                    quotation='base', fee=None, *, method='truncate', **kw):
        """The amount to be received in target currency"""
        direction = as_direction(side)
        amount = self._prepare_order_size(symbol, direction, amount, price, quotation, True, method, **kw)
        
        if fee is not None:
            return self._calc_payout_with_given_fee(direction, amount, price, fee)
        
        fee_rate = self.markets[symbol][takerOrMaker] if isinstance(takerOrMaker,str) else takerOrMaker
        if direction:
            payout = amount*(1-fee_rate) if self.FEE_FROM_TARGET else amount
        else:
            payout = self.baseToQuote(amount, price) * (1-fee_rate)
            
        return payout
     
     
    def _calc_payout_with_given_fee(self, side, order_size, price, fee):
        direction = as_direction(side)
        if isinstance(fee, dict):
            fee = fee['cost']
        if direction:
            payout = order_size - fee if self.FEE_FROM_TARGET else order_size
        else:
            payout = self.baseToQuote(order_size, price) - fee
            
        return payout
    
    
    def calc_order_size_from_payout(self, symbol, side, amount, price, takerOrMaker='taker',
                                    quotation='target', *, method='ceil', **kw):
        """Calculate input amount for an order, from the final amount to be received.
           NB! Uses 'ceil' function for the resulting_payout to be >= payout.
               For reverse engineering (payout->size) use 'round' instead."""
        direction, is_quote = self._decode_params(side, quotation)
        fee_rate = self.markets[symbol][takerOrMaker]
        
        if is_quote:
            # Price must be vwap
            amount = self.quoteToBase(amount, price)
            
        if direction:
            size = amount
            if self.FEE_FROM_TARGET:
                #os - os*fee_rate = payout
                #os = payout/(1-fee_rate)
                size /= (1-fee_rate)
        else:
            size = amount/(1-fee_rate)
            
        limit = kw.pop('limit', True)
        
        return self.round_amount(symbol, size, price, limit, method=method, **kw)
                
    
    def calc_payout_from_cost(self, symbol, side, amount, price, takerOrMaker='taker',
                              quotation='source', *, method='truncate', **kw):
        """For reverse engineering (cost->payout) use method='round'"""
        size = self.calc_order_size_from_cost(symbol, side, amount, price, takerOrMaker,
                                              quotation, method=method, **kw)
        payout = self.calc_payout_from_order_size(symbol, side, size, price, takerOrMaker, 
                                                  'base', method=None)
        
        return payout 

    
    def calc_cost_from_payout(self, symbol, side, amount, price, takerOrMaker='taker',
                              quotation='target', *, method='ceil', **kw):
        """For reverse engineering (payout->cost) use method='round'"""
        size = self.calc_order_size_from_payout(symbol, side, amount, price, takerOrMaker,
                                                quotation, method=method, **kw)
        cost = self.calc_cost_from_order_size(symbol, side, size, price, takerOrMaker,
                                              'base', method=None)
        
        return cost
    
    
    @staticmethod
    def _decode_params(side, quotation):
        direction = as_direction(side)
        quotation = convert_quotation(quotation, direction)
        is_quote = (quotation=='quote')
        
        return direction, is_quote
    
    
    @staticmethod
    def _decode_as_type(side, quotation):
        direction = as_direction(side)
        quotation = convert_quotation(quotation, direction)
        _map = {
            (1,'quote'): 'cost',
            (1,'base'): 'size',
            (0,'base'): 'size',
            (0,'quote'): 'payout',
        }
        return _map[(direction, quotation)]
    
    
    def _prepare_order_size(self, symbol, side, amount, price=None, 
                            quotation='base', limit=True, method='truncate', **kw):
        direction, is_quote = self._decode_params(side, quotation)
        if is_quote:
            amount = self.quoteToBase(amount, price)
            if method is None: method = 'truncate'
            
        if method is not None:
            amount = self.round_amount(symbol, amount, price, limit, method=method, **kw)
            
        return amount
        
     
    def split_quantity(self, symbol, amount, n, evenly=True, limits=(-0.3,0.3), closed='both'):
        _min = self.markets[symbol]['limits']['amount']['min']
        _pn = pow(10,-int(self.markets[symbol]['precision']['amount']))
        n = min(n, int((amount-_min)/_pn + 1))
        if n<= 0: return []
        
        q_even = amount/n
        
        if not evenly:
            lowest_limit = (_min/q_even) - 1
            if lowest_limit < 0:
                limits = [max(x, lowest_limit) if x is not None else None for x in limits]
                split = [self.round_amount(symbol, x)
                          for x in fons.math.series.randomize_summation(amount,n,limits,closed)]
            else: evenly = True

        if evenly:
            q_even_round = self.round_amount(symbol, q_even)
            split = [q_even_round]*n
        
        remainder = amount - sum(split)
        times_even = int((remainder-_min)/_pn + 1)
        
        if times_even > 0:
            arr = sorted(range(n), key=lambda x: split[x]) if not evenly else range(n)
            for i,pos in zip(range(times_even),arr):
                split[pos] += _pn
        
        return split
    
    
    def get_fee_quotation(self, direction, as_str=True):
        direction = as_direction(direction)
        quotation = 0 if not self.FEE_FROM_TARGET else direction
        if as_str:
            quotation = ['quote','base'][quotation]
            
        return quotation
    
    
    def load_cy_graph(self, reload=False):
        graph = getattr(self,'cy_graph',None)
        if not graph or reload:
            markets = self.markets if self.markets is not None else {}
            self.cy_graph = create_cy_graph(markets)
            
        return self.cy_graph
    
    
    @staticmethod
    def convert_volumes(tickers, quote='BTC', method='last', fallback=['bid','ask'], 
                        missing='drop', sort=False, graph=None):
        if isinstance(method, str): method = [method]
        if isinstance(fallback,str): fallback = [fallback]
        elif fallback is None: fallback = []
        
        prices = {}
        for s,v in tickers.items():
            has = [x for x in method if x in v and v[x]]
            has_fb = [x for x in fallback if x in v and v[x]]
            if has: prices[s] = sum(v[x] for x in has) / len(has)
            elif has_fb: prices[s] = sum(v[x] for x in has_fb) / len(has_fb)
            
        if graph is None:
            graph = create_cy_graph(prices)

        prices_in_quote = {}
        for s in prices:
            try: prices_in_quote[s] = calc_price('{}/{}'.format(s.split('/')[0], quote), prices, graph)
            except RuntimeError as e:
                logger2.error(e)
            
        volumes_in_quote = {s:(p*tickers[s]['baseVolume']) for s,p in prices_in_quote.items()
                            if tickers[s]['baseVolume'] is not None}
        if missing != 'drop':
            if isinstance(missing,str): raise ValueError(missing)
            volumes_in_quote = {s: volumes_in_quote.get(s, missing) for s in prices}
            
        if sort:
            volumes_in_quote = dict(sorted(volumes_in_quote.items(), key=lambda x: x[1], reverse=True))
            
        return volumes_in_quote
    
    
    def create_order(self, symbol, type, side, amount, price=None, params={}):
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        # mro example of kucoin:
        # (<class '__main__.kucoin'>, <class '__main__.asyncCCXTWrapper'>, 
        #  <class '__main__.ccxtWrapper'>, <class 'ccxt.async_support.kucoin.kucoin'>, ...)
        #(super() is relative to current frame (ccxtWrapper), which is 
        # ccxt.async_support.kucoin.kucoin)
        return super().create_order(symbol, type, side, amount, price, params)
    
    
    def edit_order(self, id, symbol, *args):
        if len(args) >= 2:
            direction = as_direction(args[1])
            side = ['sell','buy'][direction]
            args = args[:1] + (side,) + args[2:]
        # mro example of kucoin:
        # (<class '__main__.kucoin'>, <class '__main__.asyncCCXTWrapper'>, 
        #  <class '__main__.ccxtWrapper'>, <class 'ccxt.async_support.kucoin.kucoin'>, ...)
        #(super() is relative to current frame (ccxtWrapper), which is 
        # ccxt.async_support.kucoin.kucoin)
        return super().edit_order(id, symbol, *args)
        
        
    async def create_limited_market_order(self, symbol, side, amount, max_spread=0.1,
                                          quotation='base', *, tickers=None, balance=None,
                                          ignore_hard_limit=False):
        """
        Similar to market order, but the price will be limited to avoid overly large slippage.
        Order price will be:
            for buy order: highest_bid * (1+max_spread)
            for sell_order: lowest_ask / (1+max_spread)
        """
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        
        if max_spread < 0:
            raise ValueError("`max_spread` must be >= 0; got: {}".format(max_spread))
        
        elif max_spread > MAX_SPREAD_HARD_LIMIT and not ignore_hard_limit:
            raise ValueError("`max_spread` exceeded its hard limit ({}); got: {}" \
                             .format(MAX_SPREAD_HARD_LIMIT, max_spread))
        
        if tickers is None:
            import uxs.base.poll as poll
            tickers = await poll.fetch(self,'tickers',limit=15)
            
        if side == 'buy':
            highest_bid = tickers[symbol]['bid']
            price = highest_bid*(1+max_spread)
        else:
            lowest_ask = tickers[symbol]['ask']
            price = lowest_ask/(1+max_spread)
            
        cy_graph = self.load_cy_graph()
        prices = {x:y['last'] for x,y in tickers.items()}
        base,quote = symbol.split('/')
        
        if quotation in (base,quote):
            quotation_cy = quotation
            quotation = ['base','quote'][quotation_cy==quote]
            
        elif quotation in self.currencies:
            quotation_cy_0 = quotation
            ref_price = calc_price((base,quotation_cy_0),prices,cy_graph)
            amount = self.quoteToBase(amount,ref_price)
            quotation_cy = base
            quotation = 'base' 
        #print(symbol,amount,price,side,quotation,'taker',balance.get(symbol) if balance else None)
        oInp = self.convert_order_input(symbol, side, amount, price, 'taker', quotation, balance=balance)
        
        if not oInp['amount'] or not oInp['price']:
            raise ccxt.ExchangeError('Invalid amount/price: {}/{}'.format(oInp['amount'], oInp['price']))
        
        return await self.create_limit_order(symbol, side, oInp['amount'], oInp['price'])
        
        
    async def create_limited_market_buy_order(self, symbol, amount, max_spread=0.1,
                                              quotation='base', *, tickers=None, 
                                              balance=None, ignore_hard_limit=False):
        return await self.create_limited_market_order(
            symbol, 'buy', amount, max_spread, quotation, tickers=tickers,
            balance=balance, ignore_hard_limit=ignore_hard_limit)
    
    
    async def create_limited_market_sell_order(self, symbol, amount, max_spread=0.1,
                                               quotation='base', *, tickers=None, 
                                               balance=None, ignore_hard_limit=False):
        return await self.create_limited_market_order(
            symbol, 'sell', amount, max_spread, quotation, tickers=tickers,
            balance=balance, ignore_hard_limit=ignore_hard_limit)
        
        
    async def sell_dust(self, quote_order=None, dust_definitions=None, spread_limit=None):
        """Sell small quantities to a quote currency"""
        import uxs.base.poll as poll
        
        if quote_order is None:
            quote_order = QUOTE_PREFERENCE_ORDER
            
        if dust_definitions is None:
            dust_definitions = DUST_DEFINITIONS
            
        if spread_limit is None:
            spread_limit = DUST_SPREAD_LIMIT
            
        balances = await poll.fetch(self,'balances',0)
        tickers = await poll.fetch(self,'tickers','2T')
        prices = {x:y['last'] for x,y in tickers.items()}
        cy_graph = self.load_cy_graph()

        sold = {}
        
        if self.verbose:
            logger.debug('{} - positive balances before selling dust: {}'.format(
                self.id, [(x,y) for x,y in balances['free'].items() if y]))
        
        for cy,free in balances['free'].items():
            #print(cy)
            if cy in quote_order:
                continue
            try:
                market_quote = next(q for q in quote_order if '/'.join([cy,q]) in self.markets)
            except StopIteration:
                continue
            
            symbol = '/'.join([cy,market_quote])
            #print(symbol)
            q_order2 = [market_quote] + [q for q in quote_order if q!=market_quote]
            is_dust = True
            
            for quote in q_order2:
                dust_definition = dust_definitions.get(quote)
                #print(dust_definition)
                if dust_definition is None: continue
                elif quote not in self.currencies: continue 
                price = calc_price((cy,quote), prices, cy_graph)
                cy_balance_in_quote = self.baseToQuote(free, price)
                is_dust = cy_balance_in_quote < dust_definition
                #print(cy_balance_in_quote,is_dust)
                break
            
            if not is_dust:
                continue
            
            #if free: print(symbol, free, spread_limit)
            try:
                await self.create_limited_market_sell_order(symbol, free, spread_limit, tickers=tickers)
            except ccxt.ExchangeError as exc:
                pass#logger.error(exc)
            else:
                sold[cy] = self.round_amount(symbol, free)
            
        return sold
    
    
    @staticmethod
    def parse_spaceless_symbol(symbol, quote_ids, startswith='base'):
        """:param quote_ids: a list of quote_ids present in exchange"""
        method = 'endswith' if startswith=='base' else 'startswith'
        quote = next((q for q in quote_ids if getattr(symbol, method)(q)), None)
        if quote is None:
            raise ValueError("Could not parse symbol: '{}'".format(symbol))
        base = symbol[:-len(quote)] if startswith=='base' else symbol[len(quote):]
        
        return (base, quote)
        

    def __del__(self):
        ccxtpro = any(x.__name__.startswith('ccxtpro.') for x in self.__class__.__mro__)
        if ccxtpro:
            d = _ccxtpro_instances
        elif isinstance(self, asyncCCXTWrapper):
            d = _exchange_instances_async
        else:
            d = _exchange_instances
        if self in d:
            del d[self]
        super(ccxtWrapper, self).__del__()


class asyncCCXTWrapper(ccxtWrapper):
    
    async def poll_load_markets(self, limit=None):
        import uxs.base.poll as poll
        return await poll.load_markets(self, limit)
    
    
    async def load_markets(self, reload=False):
        prev_markets = self.markets
        await super(ccxtWrapper, self).load_markets(reload)
        if (not prev_markets or reload) and self.has['fetchTradingFees']:
            try:
                await self.load_trading_fees()
            except Exception as e:
                logger.error('{} - could not load trading fees: {}'.format(self.id, repr(e)))
                logger.exception(e)
        return self.markets
    
    
    async def load_trading_fees(self, reload=False):
        if not self.markets:
            return
        no_fees = any(m.get('taker') is None or m.get('maker') is None for m in self.markets.values())
        if (no_fees or reload):
            fees = await self.fetch_trading_fees()
            self._set_trading_fees(fees)
    
    
    async def fetch_markets(self, params={}):
        markets = await super(ccxtWrapper, self).fetch_markets(params)
        for i in range(len(markets)):
            markets[i] = self.deep_extend(markets[i], self.safe_value(self.options['extendMarkets'], markets[i]['symbol'], {}))
        return markets
    
    
    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        # mro example of kucoin:
        # (<class '__main__.kucoin'>, <class '__main__.asyncCCXTWrapper'>, 
        #  <class '__main__.ccxtWrapper'>, <class 'ccxt.async_support.kucoin.kucoin'>, ...)
        return await super(ccxtWrapper, self).create_order(symbol, type, side, amount, price, params)
    
    
    async def edit_order(self, id, symbol, *args):
        if len(args) >= 2:
            direction = as_direction(args[1])
            side = ['sell','buy'][direction]
            args = args[:1] + (side,) + args[2:]
        
        return await super(ccxtWrapper, self).edit_order(id, symbol, *args)
    
    
    def open(self):
        if self.own_session and self.session is None:
            # Create our SSL context object with our CA cert file
            context = ssl.create_default_context(cafile=self.cafile) if self.verify else self.verify
            # Pass this SSL context to aiohttp and create a TCPConnector
            if not self.socks_proxy:
                connector = aiohttp.TCPConnector(ssl=context, loop=self.asyncio_loop)
            else:
                connector = aiohttp_socks.ProxyConnector.from_url(self.socks_proxy, ssl=context, loop=self.asyncio_loop)
            self.session = aiohttp.ClientSession(loop=self.asyncio_loop, connector=connector, trust_env=self.aiohttp_trust_env)


class _ccxtWrapper(ccxtWrapper, ccxt.Exchange):
    """A dummy class for pretty :rtype: specification"""


#_ccxtWrapper.
def get_name(xc):
    if isinstance(xc,str):
        return xc.lower().replace('-','')
    
    if isinstance(xc, ccxt.Exchange):
        try: return xc._custom_name
        except AttributeError:
            #spl = xc.name.lower().split()
            spl = xc.__class__.__name__.lower().split()
            num_loc = max(1, next((i for i,x in enumerate(spl) if any(y.isdigit() for y in x)),len(spl)))
            #return '-'.join(spl[:num_loc])
            return ''.join(spl[:num_loc]).replace('-','')
    #ExchangeSocket object
    else: 
        return xc.exchange
    
    
def init_exchange(exchange):
    """:rtype: _ccxtWrapper
    (The actual return type is type(..,[ccxtWrapper,exchange_api_cls])"""
    
    e_obj = None
    if isinstance(exchange, dict):
        e_given = exchange.get('exchange', exchange.get('xc', exchange.get('e')))
        e_given, _id = _interpret_exchange(e_given)
        e = get_name(e_given)
        D = dict({'exchange':e},**{x:y for x,y in exchange.items() if x not in ('exchange','xc','e')})
        if isinstance(e_given,ccxt.Exchange):
            e_obj = e_given
    else:
        e_given, _id = _interpret_exchange(exchange)
        e = get_name(exchange)
        D = {'exchange': e}
        if isinstance(exchange, ccxt.Exchange):
            e_obj = exchange
    
    if _id:
        D['id'] = _id
    
    pro = D.pop('pro', False)
    asyn = D.pop('async') if 'async' in D else (
        isinstance(e_obj, ccxt.async_support.Exchange) 
            if e_obj is not None else RETURN_ASYNC_EXCHANGE)
    
    if not pro:
        ccxt_module = ccxt if not asyn else ccxt.async_support
        cls_reg = _ccxt_cls_wrapped if not asyn else _ccxt_cls_wrapped_async
        e_reg = _exchange_instances if not asyn else _exchange_instances_async
    elif asyn:
        import ccxtpro
        ccxt_module = ccxtpro
        cls_reg = _ccxtpro_cls_wrapped
        e_reg = _ccxtpro_instances
        asyn = True
    else:
        raise ValueError('got async={}, while pro instance was requested'.format(asyn))
    
    if e not in cls_reg:
        # Dynamically create the class
        _name_reg = _E_REPLACE if not pro else _E_REPLACE_PRO
        _name = _name_reg.get(e, e)
        ccxt_eCls =  getattr(ccxt_module, _name)
        wrCls = ccxtWrapper if not asyn else asyncCCXTWrapper
        bases = (wrCls, ccxt_eCls)
        if e in _wrappers.__all__:
            bases = (getattr(_wrappers, e),) + bases
        if not asyn and e in _wrappers_sync.__all__:
            bases = (getattr(_wrappers_sync, e),) + bases
        if asyn and e in _wrappers_async.__all__:
            bases = (getattr(_wrappers_async, e),) + bases
        cls_reg[e] = type(e, bases, {})
       
    e_cls = cls_reg[e]
                
    get = D.pop('get', False)
    add = D.pop('add', True)
    args = D.pop('args', ())
    kwargs = D.pop('kwargs', {})
    if args is None: args = ()
    if kwargs is None: kwargs = {}

    eobj_args = args
    eobj_kw = {x:y for x,y in kwargs.items() if x!='auth'}
    
    extra_token_keywords = EXTRA_TOKEN_KEYWORDS.get(e,[])
    token_keywords = ['apiKey','secret'] + extra_token_keywords
    
    if len(D) > 1 or any(kwargs.get(x) for x in token_keywords+['auth']) or not e_obj:
        auth_dict1 = kwargs.get('auth',{})
        auth_dict2 = D.get('auth',{})
        if isinstance(auth_dict1, str): auth_dict1 = {'id': auth_dict1}
        if isinstance(auth_dict2, str): auth_dict2 = {'id': auth_dict2}
        #Make it so that auth keywords can be put literally anywhere
        auth_kw = {x:y for x,y in kwargs.items() if x in token_keywords}
        auth_kw.update(auth_dict1)
        auth_kw.update({x:y for x,y in D.items() if x!='auth'})
        auth_kw.update(auth_dict2)
        auth = get_auth2(**auth_kw)
    else:
        auth = e_obj._auth_info.copy()
    
    if e_obj is None: pass
    elif asyn and not isinstance(e_obj, ccxt.async_support.Exchange): e_obj = None 
    elif not asyn and isinstance(e_obj, ccxt.async_support.Exchange): e_obj = None
    
    if e_obj and e_obj.apiKey != auth.get('apiKey'):
        e_obj = None
        
    if not e_obj and get:
        try: e_obj = e_reg[e][auth.get('apiKey')]
        except KeyError: pass
        
    if not e_obj or e_obj.apiKey != auth.get('apiKey'):
        prev_e_obj = e_obj
        if eobj_kw.get('verbose'):
            logger.debug("Initiating ccxt-exchange '{}' with auth_id '{}'".format(e, auth.get('id')))
        e_obj = e_cls(*eobj_args, **eobj_kw, auth=auth)
        if prev_e_obj and not e_obj.markets:
            e_obj.set_markets(prev_e_obj.markets, prev_e_obj.currencies)
        #raise ValueError('Could not initiate exchange - {}'.format(e))

    if add:
        deep_update(e_reg,{e: {e_obj.apiKey: e_obj}})
        
    e_obj

    return e_obj


def _normalize(x):
    if not isinstance(x,dict):
        d = {'exchange': x}
    else:
        d = x.copy()
    if 'add' not in d:
        d['add'] = True
    if not bool(d.get('get')): 
        d['get'] = True
    return d


def get_exchange(exchange):
    """:rtype: _ccxtWrapper
    (The actual return type is type(..,[ccxtWrapper,exchange_api_cls])"""
    return init_exchange(_normalize(exchange))


def get_sn_exchange(exchange):
    """:rtype: _ccxtWrapper
    (The actual return type is type(..,[ccxtWrapper,exchange_api_cls])"""
    d = _normalize(exchange)
    d['async'] = False
    return init_exchange(d)


def list_exchanges():
    exchanges = []
    for attr,v in vars(ccxt).items():
        if isinstance(v,type) and issubclass(v,ccxt.Exchange) \
                and v is not ccxt.Exchange:
            exchanges.append(attr)
    return exchanges


async def close_all_exchanges():
    for x in [_exchange_instances, _exchange_instances_async, _ccxtpro_instances]:
        for e_name, apis in x.items():
            for apiKey, api in apis.items():
                if asyncio.iscoroutinefunction(api.close):
                    await api.close()
                else:
                    api.close()


#Notes:
#If you create_limit_buy_order(symbol, x_amount, y_price),
# do you get x_amount*(1-fee) or x_amount?
#binance: x_amount*(1-fee)
#poloniex: x_amount*(1-fee)
#bittrex: x_amount
#hitbtc: x_amount
#kucoin: x_amount
#formula1: x_amount/price/(1+fee) [bittrex,hitbtc,kucoin]
#formula2: x_amount/price*(1-fee) [binance,poloniex]

#If your balance is x_amount, can you sell x_amount?
#binance: true
#poloniex: true
#bittrex: true
#hitbtc: true
#kucoin: true
#formula: x_amount*price*(1-fee)

#binance and polo take fee from the target cy of the conversion
# ie. BTC->alt : fee from alt; you get = alt*(1-fee)
#     alt->BTC : fee from BTC; you get alt*price*(1-fee)
