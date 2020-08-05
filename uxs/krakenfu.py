import base64
import hashlib
import asyncio

from uxs.base.socket import ExchangeSocket

from fons.log import get_standard_5

logger,logger2,tlogger,tloggers,tlogger0 = get_standard_5(__name__)

# Notes
# edit_order returns correct filled amount (thanks to previous order also being returned)
# edit_order(.., amount) changes "remaining" to that new amount (regardless of previous filled)

def _extract_error(r):
    if isinstance(r, dict) and r.get('event') == 'error':
        return r['message']
    return None
    

class krakenfu(ExchangeSocket):
    exchange = 'krakenfu'
    url_components = {
        'ws': 'wss://futures.kraken.com/ws/v1',
        'test': 'wss://demo-futures.kraken.com/ws/v1',
        'version': 'v1',
    }
    auth_defaults = {
        'each_time': True,
        'takes_input': True,
    }
    channel_defaults = {
        'merge_option': True,
    }
    channels = {
        'ticker': {
            # payloads are being sent on 1 sec interval
            'fetch_data_on_sub': False,
        },
        'trades': {
            'fetch_data_on_sub': False, # receives snapshot
        },
        'account': {
            'fetch_data_on_sub': False, # receives snapshot
        },
        'ticker_lite': {
            'required': ['symbol'],
        },
        'fetch_challenge': {
            'type': 'fetch',
            'required': [],
        },
    }
    channel_ids = {
        'ticker': 'ticker',
        'orderbook': 'book',
        'trades': 'trade',
        'account': [
            'account_balances_and_margins',
            'open_orders_verbose',
            'fills',
            'open_positions',
            #'account_log',
            #'notifications_auth',
            #'open_orders',
        ],
        'ticker_lite': 'ticker_lite',
        'instruments': 'instruments',
    }
    exceptions = {
        #'Invalid product id'
    }
    has = {
        'all_tickers': False,
        'ticker': {
            'last': True, 'bid': True, 'ask': True, 'bidVolume': True, 'askVolume': True,
            'high': False, 'low': False, 'open': False, 'close': True, 'previousClose': False,
            'change': True, 'percentage': False, 'average': False, 'vwap': False,
            'baseVolume': True, 'quoteVolume': True, 'active': False},
        'orderbook': True,
        'trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'ohlcv': False,
        'account': {'balance': True, 'order': True, 'fill': True, 'position': True},
        'ticker_lite': True,
        'instruments': True,
        'fetch_tickers': {
            'ask': True, 'askVolume': True, 'average': True, 'baseVolume': False, 'bid': True, 'bidVolume': True,
            'change': True, 'close': True, 'datetime': True, 'high': False, 'info': True, 'last': True, 'low': False, 'open': True, 'percentage': True, 'previousClose': False, 'quoteVolume': True,
            'symbol': True, 'timestamp': True, 'vwap': False},
        'fetch_order_book': {'asks': True, 'bids': True, 'datetime': True, 'nonce': False, 'timestamp': True},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'fetch_balance': {'free': False, 'used': False, 'total': True},
        'fetch_my_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': True,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': True, 'timestamp': True, 'type': False},
        'create_order': {
            'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': False,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': True, 'type': True},
        'edit_order': True,
        'fetch_open_orders': {'symbolRequired': False},
    }
    has['edit_order'] = has['create_order'].copy()
    has['fetch_open_orders'].update(has['create_order']) 
    message = {
        'error': {
            'key': _extract_error,
        }
    }
    ob = {
        'receives_snapshot': True,
        'force_create': None, # the fetched one doesn't have nonce
        'on_unsync': 'restart', # -||-
    }
    order = {
        'cancel_automatically': 'if-not-subbed-to-account',
        # Fill is sent before order update, hence in case of edit order (with new larger amount)
        # it could close the order prematurely
        'update_remaining_on_fill': False,
        # Orders alone can't be relied on, as no "filled"
        # is included in the final (closed) message
        'update_filled_on_fill': True,
        'update_payout_on_fill': False,
        'update_balance': False,
    }
    trade = {
        'sort_by': lambda x: x['info']['seq'],
    }
    _cached_challenge = {
        'original': None,
        'signed': None,
        'cnx_id': None,
        'connected_ts': None,
    }
    __deepcopy_on_init__ = ['_cached_challenge']
    
    
    def handle(self, R):
        r = R.data
        if isinstance(r, dict):
            event = r.get('event')
            feed = r.get('feed')
            if event == 'challenge':
                self.tp.forward_to_waiters('fetch_challenge', R)
            elif event in ('subscribed', 'unsubscribed'):
                pass
            elif feed == 'ticker':
                self.on_ticker(r)
            elif feed in ('book', 'book_snapshot'):
                self.on_orderbook(r)
            elif feed in ('trade', 'trade_snapshot'):
                self.on_trade(r)
            elif feed == 'account_balances_and_margins':
                self.on_balance(r)
            elif feed in ('open_orders_verbose',
                          'open_orders_verbose_snapshot',
                          'open_orders',
                          'open_orders_snapshot'):
                self.on_order(r)
            elif feed in ('fills', 'fills_snapshot'):
                self.on_fill(r)
            elif feed == 'open_positions':
                self.on_position(r)
            elif feed == 'ticker_lite':
                self.on_ticker_lite(r)
            elif feed == 'notifications_auth':
                self.on_notification(r)
            elif 'funding_rate' in r:
                self.on_account_log(r)
            elif 'instruments' in r:
                self.on_instruments(r)
            else:
                self.notify_unknown(r)
        else:
            self.notify_unknown(r)
    
    
    def on_ticker(self, r):
        """{  
           "feed":"ticker",
           "product_id":"FI_XBTUSD_180511",
           "bid":9375.0,
           "ask":9380.0,
           "bid_size":101.0,
           "ask_size":101.0,
           "volume":7518205.0,
           "dtm":0,
           "leverage":"50x",
           "index":9340.19,
           "premium":0.4,
           "last":9380.0,
           "time":1525971426646,
           "change":0.2,
           "tag":"week",
           "pair":"XBT:USD",
           "openInterest":1659666.0,
           "markPrice":9353.266266,
           "maturityTime":1526050800000
        }
        {  
           'ask_size':2285.0,
           'product_id':'PI_XBTUSD',
           'volume':762208.0,
           'openInterest':230834.0,
           'relative_funding_rate':-3.429315625e-06,
           'bid':3233.0,
           'maturityTime':0,
           'dtm':-17882,
           'tag':'perpetual',
           'change':-0.5,
           'index':3233.76,
           'bid_size':10000.0,
           'funding_rate':-1.058309892e-09,
           'relative_funding_rate_prediction':-4.171106875e-05,
           'pair':'XBT:USD',
           'markPrice':3233.5,
           'leverage':'50x',
           'funding_rate_prediction':-1.2897711109e-08,
           'ask':3234.0,
           'next_funding_rate_time':1545048000000,
           'time':1545046832866,
           'premium':0.0,
           'feed':'ticker',
           'last':3231.0
        }"""
        self.update_tickers([self.parse_ticker(r)], enable_sub=True)
    
    
    def parse_ticker(self, r):
        symbol = self.convert_symbol(r['product_id'].lower(), 0)
        market = self.api.markets[symbol]
        keys = ['ask','bid','last','change']
        map = {
            'timestamp': 'time',
            'askVolume': 'ask_size',
            'bidVolume': 'bid_size',
            'baseVolume': 'volume',
        }
        if market['swap']:
            map['baseVolume'] = None
            map['quoteVolume'] = 'volume'
        
        return self.api.ticker_entry(
            symbol=symbol,
            **self.api.lazy_parse(r, keys, map),
            info=r)
    
    
    def on_orderbook(self, r):
        """
        SNAPSHOT
        {  
            "feed":"book_snapshot",
            "product_id":"FI_XBTUSD_180615",
            "seq":0,
            "bids":[  
                {  
                    "price":14851.0,
                    "qty":10000.0
                },
                {  
                    "price":14711.0,
                    "qty":5000.0
                },
                ....
            ],
            "asks":[  
                {  
                    "price":14851.0,
                    "qty":10000.0
                },
                {  
                    "price":14711.0,
                    "qty":5000.0
                },
                ....
            ]
        }
        
        UPDATE
        {  
            "feed":"book",
            "product_id":"FI_XBTUSD_180316",
            "side":"buy",
            "seq":2,
            "price":10027.0,
            "qty":10.0
        }
        """
        ob = {
            'symbol': self.convert_symbol(r['product_id'].lower(), 0),
            'bids': [],
            'asks': [],
        }
        is_snapshot = (r['feed'] == 'book_snapshot')
        if is_snapshot:
            ob.update(self.api.parse_order_book(r, price_key='price', amount_key='qty'))
            method = 'send_orderbook'
        else:
            side = 'bids' if r['side']=='buy' else 'asks'
            ob[side] += [[r['price'], r['qty']]]
            method = 'send_update'
        ob['nonce'] = r['seq']
        getattr(self.orderbook_maintainer, method)(ob)
    
    
    def on_trade(self, r):
        """
        SNAPSHOT
        {  
           "feed":"trade_snapshot",
           "product_id":"FI_XBTUSD_180615",
           "trades":[  ... ]
        }
        
        UPDATE
        {  
            "feed":"trade",
            "product_id":"FI_XBTUSD_180615",
            "seq":103,  
            "side":"buy",
            "type":"liquidation",
            "seq":103,  
            "time":1515541598000,
            "qty":5000.0,
            "price":14226.0
        }
        """
        is_snapshot = (r['feed'] == 'trade_snapshot')
        symbol = self.convert_symbol(r['product_id'].lower(), 0)
        trades = r['trades'] if is_snapshot else [r]
        parsed = [self.parse_trade(t) for t in trades]
        self.update_trades([{'symbol': symbol, 'trades': parsed}],
                           enable_sub=True)
    
    
    def parse_trade(self, t):
        """
        TRADE
        {  
            "feed":"trade",
            "product_id":"FI_XBTUSD_180615",
            "uid":"fc4c709b-edb5-402a-b92f-8cc407a004bf",
            "seq":103,
            "side":"buy",
            "type":"liquidation", // "fill"
            "seq":103,  
            "time":1515541598000,
            "qty":5000.0,
            "price":14226.0
        }
        
        FILL
        {  
            "instrument":"FI_XBTUSD_180316",
            "time":1516965089607,
            "price":10015.0,
            "seq":100,
            "buy":True,
            "qty":1.0,
            "order_id":"3696d19b-3226-46bd-993d-a9a7aacc8fbc",
            "cli_ord_id":"8b58d9da-fcaf-4f60-91bc-9973a3eba48d",
            "fill_id":"c14ee7cb-ae25-4601-853a-d0205e576099", 
            "fill_type":"takerAfterEdit"     # "taker" # "takerAfterEdit"
        }
        """
        keys = ['side','price']
        map = {
            'symbol': ['product_id', 'instrument'],
            'timestamp': 'time',
            'amount': 'qty',
            'order': 'order_id',
            'id': ['fill_id', 'uid'],
            'takerOrMaker': 'fill_type',
        }
        apply = {
            'symbol': lambda x: self.convert_symbol(x.lower(), 0),
            'takerOrMaker': lambda x: ('maker' if x.lower().startswith('maker') else 'taker')
                                      if x else None,
        }
        trade = self.api.trade_entry(
            **self.api.lazy_parse(t, keys, map, apply),
            info=t,
        )
        market = self.markets.get(trade['symbol'])
        trade['cost'] = self._calc_cost(trade['amount'], trade['price'], market)

        return trade
    
    
    def _calc_cost(self, amount, price, market):
        cost = None
        if amount is not None and price and market is not None:
            if market['linear']:
                cost = amount * price # in quote
            else:
                cost = amount / price # in base
            cost *= market['lotSize']
        return cost
    
    
    def on_balance(self, r):
        """
        {  
           'seq':4786,
           'feed':'account_balances_and_margins',
           'margin_accounts':[  
                {  
                   'name':'xbt',
                   'pv':0.0,        // The portfolio value calculated as balance plus unrealized pnl value
                   'balance':4.0,   // The current balance of the account
                   'funding':0.0,
                   'mm':0.0,        // The maintenance margin for open positions
                   'pnl':0.0,
                   'im':0.0,        // The initial margin for open positions and orders
                   'am':0.0         // The available margin for opening new positions
                },
                ...
                {  
                    'name':'f-xbt:usd',
                    'pv':96.74,
                    'balance':99.93704565,
                    'funding':-0.008069,
                    'mm':1.969,
                    'pnl':-3.18821611,
                    'im':5.048,
                    'am':91.6927
                },
            ],
        }
        """
        parsed = [self.parse_balance(b) for b in r['margin_accounts']]
        self.update_balances(parsed, enable_sub=True)
    
    
    def parse_balance(self, b):
        if b['name'].isalnum():
            return {
                'currency': self.convert_cy(b['name'], 0),
                'free': None,
                'used': None,
                'total': b['balance'],
                'info': b,
            }
        else:
            currencyIds = b['name'].split('-')[1].split(':')
            symbol = '/'.join(self.convert_cy(x, 0) for x in currencyIds)
            return {
                'currency': symbol,
                'free': b['am'],
                'used': b['pv']-b['am'],
                'total': b['pv'],
                'info': b,
            }
    
    
    def on_order(self, r):
        """
        SNAPSHOT
        {  
            'feed':'open_orders_verbose_snapshot',
            'account':'0f9c23b8-63e2-40e4-9592-6d5aa57c12ba',
            'orders':[  
                {  
                   'instrument':'PI_XBTUSD',
                   'time':1567428848005,
                   'last_update_time':1567428848005,
                   'qty':100.0,
                   'filled':0.0,
                   'limit_price':8500.0,
                   'stop_price':0.0,
                   'type':'limit',
                   'order_id':'566942c8-a3b5-4184-a451-622b09493129',
                   'direction':0,
                   'reduce_only':False
                },
            ]
        }
        
        UPDATE
        {  
           'feed':'open_orders_verbose',
           'order':{  
                'instrument':'PI_XBTUSD',
                'time':1567597581495,
                'last_update_time':1567597581495,
                'qty':102.0,
                'filled':0.0,
                'limit_price':10601.0,
                'stop_price':0.0,
                'type':'limit',
                'order_id':'fa9806c9-cba9-4661-9f31-8c5fd045a95d',
                'direction':0,
                'reduce_only':False
            },
           'is_cancel':False,
           'reason':'new_placed_order_by_user'
        }
        
        {
            'feed': 'open_orders_verbose',
            'order_id': 'a2819391-18aa-4a77-9ea1-ef4e299906fb',
            'is_cancel': True,
            'reason': 'edited_by_user'
        }
        """
        # This actually only receives OPEN orders, everything else has to be updated
        # either by fills or by .create_order / .edit_order
        # Cancel/closed messages are also received, but final filled quantity is not included.
        # Also market orders created by user (manually at the site) are not received at all!
        tlogger.debug(r)
        is_snapshot = (r['feed'] in ('open_orders_verbose_snapshot',
                                     'open_orders_snapshot'))
        if is_snapshot:
            orders = r['orders']
        elif 'order' not in r:
            if r.get('is_cancel'):
                self.update_order(id=r['order_id'], remaining=0, enable_sub=True)
            orders = []
        else:
            _order = dict(r['order'],
                          is_cancel=r['is_cancel'],
                          reason=r['reason'])
            orders = [_order]
        
        for o in orders:
            parsed = self.parse_order(o)
            #del parsed['remaining']
            self.add_order_from_dict(parsed, enable_sub=True)
    
    
    def parse_order(self, o):
        symbol = self.convert_symbol(o['instrument'].lower(), 0)
        remaining = o['qty'] if not o.get('is_cancel') else 0 # qty does indeed show remaining
        amount = o['qty'] + o['filled']
        side = 'buy' if not o['direction'] else 'sell'
        # cost can't be calculated as average is not known
        return {
            'symbol': symbol,
            'id': o['order_id'],
            'type': o['type'],
            'side': side,
            'amount': amount,
            'price': o['limit_price'] if o.get('limit_price') else None,
            'stop': o['stop_price'] if o.get('stop_price') else None,
            'filled': o['filled'],
            'remaining': remaining,
            'timestamp': o['time'],
            'info': o,
        }
    
    
    def on_fill(self, r):
        """
        SNAPSHOT
        {  
            "feed":"fills_snapshot",
            "account":"DemoUser",
            "fills":[  
                {  
                    "instrument":"FI_XBTUSD_180316",
                    "time":1516717161000,
                    "price":10016.0,
                    "seq":0,
                    "buy":True,
                    "qty":106.0,
                    "order_id":"11cc341e-8306-4ff3-b167-0f284cd45fd7",
                    "fill_id":"5d785dde-e756-480f-88a8-f4efb4d98708",
                    "fill_type":"taker"
                }
            ]
        }
        
        UPDATE
        {  
            "feed":"fills"
            "username":"DemoUser",
            "fills":[  
                {  
                    "instrument":"FI_XBTUSD_180316",
                    "time":1516965089607,
                    "price":10015.0,
                    "seq":100,
                    "buy":True,
                    "qty":1.0,
                    "order_id":"3696d19b-3226-46bd-993d-a9a7aacc8fbc",
                    "cli_ord_id":"8b58d9da-fcaf-4f60-91bc-9973a3eba48d",
                    "fill_id":"c14ee7cb-ae25-4601-853a-d0205e576099", 
                    "fill_type":"maker"
                },
                ...
            ]
        }
        """
        tlogger.debug(r)
        parsed = [self.parse_trade(f) for f in r['fills']]
        for f in parsed:
            self.add_fill_from_dict(f, enable_sub=True)
    
    
    def on_position(self, r):
        """
        {
            "feed":"open_positions",
            "account":"DemoUser",
            "positions":[  
                {  
                    "instrument":"PI_XBTUSD",                       // May be both upper/lowercase
                    "balance":15.0,                                 // The size of the position
                    "entry_price":10163.5,
                    "mark_price":10161.0,
                    "index_price":10089.19,
                    "pnl":-3.631211330413245e-07,
                    'liquidation_threshold': 149.29820640229335,    // OPTIONAL
                    'effective_leverage': 0.014762543610769273,     // OPTIONAL
                    'return_on_equity': -0.01229891277610966,       // OPTIONAL
                    'unrealized_funding': -3.665969967978667e-10    // OPTIONAL
                },
                ...
            ]
        }
        """
        parsed = [self.parse_position(p) for p in r['positions']]
        nullify = []
        for symbol, position in self.positions.items():
            # If a position was not in the payload then it means that it has been closed
            if position['amount'] and not any(p['symbol']==symbol for p in parsed):
                nullify.append(self.create_null_position(symbol))
        positions = parsed + nullify
        self.update_positions(positions, enable_sub=True)
    
    
    def parse_position(self, p):
        return self.api.position_entry(
            symbol = self.convert_symbol(p['instrument'].lower(), 0),
            price = p['entry_price'],
            amount = p['balance'],
            liq_price = p.get('liquidation_threshold'),
            leverage = p.get('effective_leverage'),
            info = p,
        )
    
    
    def create_null_position(self, symbol):
        return self.api.position_entry(
            symbol=symbol,
            info=dict.fromkeys(
                ['balance',
                 'entry_price',
                 'mark_price',
                 'index_price',
                 'pnl',
                 'liquidation_threshold',
                 'effective_leverage',
                 'return_on_equity',
                 'unrealized_funding',]
            ),
        )
    
    
    def on_account_log(self, r):
        """
        {  
            'id':1688,
            'date':'2019-07-11T00:00:00.000Z',
            'asset':'bch',
            'info':'funding rate change',
            'booking_uid':'72e3396e-8fc4-4bd9-9379-8e2b2225af85',
            'margin_account':'f-bch:usd',
            'old_balance':0.01215847263,
            'new_balance':0.01215715298,
            'old_average_entry_price':0.0,
            'new_average_entry_price':0.0,
            'trade_price':0.0,
            'mark_price':0.0,
            'realized_pnl':0.0,
            'fee':0.0,
            'execution':'',
            'collateral':'bch',
            'funding_rate':1.64955714332e-07,
            'realized_funding':-1.31965e-06
        }
        """
    
    
    def on_notification(self, r):
        """
        {  
           "feed":"notifications_auth",
           "notifications":[  
                {  
                   "id":5,
                   "type":"market",
                   "priority":"low",
                   "note":"A note describing the notification.",
                   "effective_time":1520288300000
                },
                ...
            ]
        }
        """
    
    
    def on_ticker_lite(self, r):
        """
        {  
           "feed":"ticker_lite",
           "product_id":"FI_XBTUSD_180525",
           "bid":9425.0,
           "ask":9434.0,
           "change":-0.2,
           "premium":1.2,
           "volume":6288487.0,
           "tag":"month",
           "pair":"XBT:USD",
           "dtm":14
        }
        """
    
    
    def on_instruments(self, r):
        """
        {  
          "result":"success",
          "instruments":[  
            {  
              "symbol":"fi_ethusd_180928",
              "type":"futures_inverse",
              "underlying":"rr_ethusd",
              "lastTradingTime":"2018-09-28T15:00:00.000Z",
              "tickSize":0.1,
              "contractSize":1,
              "tradeable":true,
              "marginLevels":[  
                {  
                  "contracts":0,
                  "initialMargin":0.02,
                  "maintenanceMargin":0.01
                },
                {  
                  "contracts":250000,
                  "initialMargin":0.04,
                  "maintenanceMargin":0.02
                },
                {  
                  "contracts":500000,
                  "initialMargin":0.06,
                  "maintenanceMargin":0.03
                }
              ]
            },
            
            ...
            {  
              "symbol":"in_xbtusd",
              "type":"spot index",
              "tradeable":false
            }
          ],
          "serverTime":"2018-07-19T11:32:39.433Z"
        }
        """
    
    
    def encode(self, req, sub=None):
        channel = req.channel
        p = req.params
        
        # Does the futures WS API accept 'reqid' param as the normal WS API does?
        #msg_id = self.generate_message_id(uid=None)
        
        if channel == 'fetch_challenge':
            return (
                {  
                    'event': 'challenge',
                    'api_key': self.apiKey,
                },
                'fetch_challenge'
            )
        
        out = {}
        if sub is not None:
            out['event'] = 'subscribe' if sub else 'unsubscribe'
        
        symbols = None
        if 'symbol' in self.channels[channel]['required']:
            symbol = self.convert_symbol(p['symbol'])
            symbols = [symbol] if isinstance(symbol, str) else symbol
            symbols = [x.upper() for x in symbols]
                              
        packs = []
        channel_id = self.channel_ids[channel]
        channel_ids = [channel_id] if isinstance(channel_id, str) else channel_id
        for channel_id in channel_ids:
            _out = out.copy()
            _out['feed'] = channel_id
            if symbols is not None:
                _out['product_ids'] = symbols
            packs.append(_out)
        
        return self.merge(packs)   
    
    
    async def sign(self, out):
        cache = self._cached_challenge
        cnx = self.cm.connections.get(cache['cnx_id'])
        if not cache['original'] or cnx is None or cnx.connected_ts != cache['connected_ts']:
            R = await self.tp.send({'_': 'fetch_challenge'}, wait='default', id='fetch_challenge')
            """{  
                "event":"challenge",
                "message":"226aee50-88fc-4618-a42a-34f7709570b2"
            }"""
            challenge = R.data['message']
            signed = self._sign_challenge(challenge)
            cnx_id = R.id
            connected_ts = self.cm.connections[cnx_id].connected_ts
            cache.update({'original': challenge,
                          'signed': signed,
                          'cnx_id': cnx_id,
                          'connected_ts': connected_ts})
        
        out['api_key'] = self.apiKey
        out['original_challenge'] = cache['original']
        out['signed_challenge'] = cache['signed']
        
        return out
    
    
    def _sign_challenge(self, challenge):
        """
        1. Hash the challenge with the SHA-256 algorithm
        2. Base64-decode your api_secret
        3. Use the result of step 2 to hash the result of step 1 with the HMAC-SHA-512 algorithm
        4. Base64-encode the result of step 3
        """
        challenge_h = self.api.hash(challenge.encode(), 'sha256', 'binary') # 1
        secret_b64 = base64.b64decode(self.secret) # 2
        sig = self.api.hmac(challenge_h, secret_b64, hashlib.sha512, 'base64').decode() #3-4
        
        return sig


if __name__ == '__main__':
    # Verify the test example at https://support.kraken.com/hc/en-us/articles/360022635652-Sign-Challenge-Web-Socket-API-s
    _challenge = 'c100b894-1729-464d-ace1-52dbce11db42'
    _secret = '7zxMEF5p/Z8l2p2U7Ghv6x14Af+Fx+92tPgUdVQ748FOIrEoT9bgT+bTRfXc5pz8na+hL/QdrCVG7bh9KpT0eMTm'
    _expected = '4JEpF3ix66GA2B+ooK128Ift4XQVtc137N9yeg4Kqsn9PI0Kpzbysl9M1IeCEdjg0zl00wkVqcsnG4bmnlMb3A=='
    _kr = krakenfu({'apiKey': 'randomKey',  'secret': _secret})
    _sig = _kr._sign_challenge(_challenge)
    assert _sig == _expected
    print('Challenge verified')