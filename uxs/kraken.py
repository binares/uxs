import asyncio
import aiohttp
import functools
import json
import random
from copy import deepcopy
import datetime,time
import ccxt
from fons.aio import call_via_loop
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket
from uxs.fintls.basics import as_direction

from fons.sched import AsyncTicker
from fons.time import ctime_ms
import fons.log

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class kraken(ExchangeSocket):
    exchange = 'kraken'
    url_components = {
        'ws': 'wss://ws.kraken.com',
        'private': 'wss://ws-auth.kraken.com',
        'beta': 'wss://beta-ws.kraken.com',
    }
    auth_defaults = {
        'takes_input': True,
        'each_time': True,
        #'set_authenticated': True,
    }
    channel_defaults = {
        'max_subscriptions': 100,
        'subscription_push_rate_limit': 0.04,
    }
    channels = {
        'ticker': {
            'merge_option': True,
        },
        'orderbook': {
            'merge_option': True,
        },
        'ohlcv': {
            'merge_option': True,
        },
        'trades': {
            'merge_option': True,
        },
        'spread': {
            'required': ['symbol'],
            'merge_option': True,
            'delete_data_on_unsub': False,
        },
        'account': {
            'url': '<$private>',
        },
        'create_order': {
            'url': '<$private>',
        },
        'cancel_order': {
            'url': '<$private>',
        },
        
    }
    connection_defaults = {
        'ping_interval': 30,
    }
    has = {
        'all_tickers': False,
        'ticker': {
            'last': True, 'bid': True, 'ask': True, 'bidVolume': True, 'askVolume': True,
            'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False,
            'change': False, 'percentage': False, 'average': False, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True, 'active': False},
        'orderbook': True,
        'ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        'account': {'balance': False, 'order': True, 'fill': True},
        'fetch_tickers': True,
        'fetch_ticker': {
            'ask': True, 'askVolume': False, 'average': False, 'baseVolume': True, 'bid': True, 'bidVolume': False,
            'change': False, 'close': True, 'datetime': True, 'high': True, 'last': True, 'low': True, 'open': True,
            'percentage': False, 'previousClose': False, 'quoteVolume': True, 'symbol': True, 'timestamp': True,
            'vwap': True},
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'asks': True, 'bids': True, 'datetime': False, 'nonce': False, 'timestamp': False},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        'fetch_balance': {'free': False, 'used': False, 'total': True},
        'fetch_order': {
            'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': True,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': True, 'type': True},
        'fetch_open_orders': {'symbolRequired': False},
        'fetch_closed_orders': {'symbolRequired': False},
        # Websocket versions are currently disabled
        'create_order': { # 'ws': False,
            'amount': False, 'average': False, 'clientOrderId': False, 'cost': False, 'datetime': False, 'fee': False,
            'filled': False, 'id': True, 'lastTradeTimestamp': False, 'price': False, 'remaining': False, 'side': True,
            'status': False, 'symbol': True, 'timestamp': False, 'trades': False, 'type': True},
        'cancel_order': {'ws': False},
    }
    has['fetch_tickers'] = has['fetch_ticker'].copy()
    has['fetch_open_orders'].update({**has['fetch_order'], 'trades': False})
    has['fetch_closed_orders'].update({**has['fetch_order'], 'trades': False})
    channel_ids = {
        'ticker': 'ticker',
        'orderbook': 'book',
        'ohlcv': 'ohlc',
        'trades': 'trade',
        'spread':'spread',
        'account': ['openOrders','ownTrades'],
        'create_order': 'addOrder',
        'cancel_order': 'cancelOrder',
    }
    exceptions = {
        'Public channels not available in this endpoint': ccxt.BadRequest,
        
        'Currency pair not in ISO 4217-A3 format': ccxt.BadRequest,
        'Malformed request': ccxt.BadRequest,
        'Pair field must be an array': ccxt.BadRequest,
        'Pair field unsupported for this subscription type': ccxt.BadRequest,
        'Pair(s) not found': ccxt.errors.BadSymbol,
        'Subscription book depth must be an integer': ccxt.BadRequest,
        'Subscription depth not supported': ccxt.NotSupported,
        'Subscription field must be an object': ccxt.BadRequest,
        'Subscription name invalid': ccxt.BadRequest,
        'Subscription object unsupported field': ccxt.BadRequest,
        'Subscription ohlc interval must be an integer': ccxt.BadRequest,
        'Subscription ohlc interval not supported': ccxt.NotSupported,
        'Subscription ohlc requires interval': ccxt.ArgumentsRequired,
        
        #The following error messages may be thrown for private data requests:
        
        'EAccount:Invalid permissions': ccxt.PermissionDenied,
        'EAuth:Account temporary disabled': ccxt.AccountSuspended,
        'EAuth:Account unconfirmed': ccxt.AuthenticationError,
        'EAuth:Rate limit exceeded': ccxt.DDoSProtection,
        'EAuth:Too many requests': ccxt.DDoSProtection,
        'EGeneral:Invalid arguments': ccxt.BadRequest,
        'EOrder:Cannot open opposing position': ccxt.ExchangeError,
        'EOrder:Cannot open position': ccxt.ExchangeError,
        'EOrder:Insufficient funds (insufficient user funds)': ccxt.InsufficientFunds,
        'EOrder:Insufficient margin (exchange does not have sufficient funds to allow margin trading)': ccxt.InsufficientFunds,
        'EOrder:Margin allowance exceeded': ccxt.ExchangeError,
        'EOrder:Margin level too low': ccxt.BadRequest, #InvalidOrder,
        'EOrder:Order minimum not met (volume too low)': ccxt.BadRequest, #InvalidOrder,
        'EOrder:Orders limit exceeded': ccxt.ExchangeError,
        'EOrder:Positions limit exceeded': ccxt.ExchangeError,
        'EOrder:Rate limit exceeded': ccxt.DDoSProtection,
        'EOrder:Scheduled orders limit exceeded': ccxt.ExchangeError,
        'EOrder:Unknown position': ccxt.OrderNotFound,
        'EOrder:Unknown order': ccxt.OrderNotFound,
        'EOrder:Order minimum not met': ccxt.BadRequest,
        'EService:Unavailable': ccxt.ExchangeNotAvailable,
        'ETrade:Invalid request': ccxt.BadRequest,
        
        'addOrder is currently unavailable': ccxt.NotSupported,
        'cancelOrder is currently unavailable': ccxt.NotSupported,
    }
    message = {
        'id': {'key': 'reqid'},
        'error': {'key': 'error'},
    }
    # The orderbook really isn't perfect, as we have no idea of knowing whether
    # it is in sync or not.
    ob = {
        'uses_nonce': False,
        'receives_snapshot': True,
        # This is set to 10 because its the exchange's default
        'default_limit': 10,
        'limits': [10, 25, 100, 500, 1000],
    }
    order = {
        'cancel_automatically': 'if-not-subbed-to-account',
        'update_filled_on_fill': False,
        'update_payout_on_fill': True,
        'update_remaining_on_fill': False,
    }
    symbol = {
        #'quote_ids': ['ZCAD', 'XETH', 'ZEUR', 'USDT', 'ZUSD', 'XXBT',  'DAI', 'ZGBP', 'ZJPY'],
        #'sep': '',
        # Websocket naming scheme differs from REST scheme
        'quote_ids': ['CAD', 'ETH', 'EUR', 'USDT', 'USD', 'XBT', 'DAI' 'GBP', 'JPY'],
        'sep': '/',
    }
    trade = {
        'sort_by': lambda x: (float(x['info'][2]), x['amount']),
    }
    
    _cached_wstoken = {
        'token': None,
        'expires': None,
        'ticker': None,
        'last_ticker_id': -1,
    }
    __deepcopy_on_init__ = ['_cached_wstoken']
    
    
    def setup_test_env(self):
        return {
            'private': self.url_components['beta'],
            'private_original': self.url_components['private'],
            'ccxt_test': False,
        }
    
    
    def handle(self, R):
        r = R.data
        
        if isinstance(r, list):
            channel_id = r[-1] if not isinstance(r[-2], str) else r[-2]
            if channel_id == 'ticker':
                self.on_ticker(r)
            elif channel_id.startswith('book'):
                self.on_orderbook(r)
            elif channel_id.startswith('ohlc'):
                self.on_ohlcv(r)
            elif channel_id == 'trade':
                self.on_trade(r)
            elif channel_id == 'spread':
                self.on_spread(r)
            elif channel_id == 'openOrders':
                self.on_order(r)
            elif channel_id == 'ownTrades':
                self.on_fill(r)
            else:
                self.notify_unknown(r)
        else:
            if isinstance(r, dict) and r.get('event') != 'heartbeat':
                tlogger.debug(r)
    
    
    def check_errors(self, r):
        """
        {
          'errorMessage': 'Public channels not available in this endpoint',
          'event': 'subscriptionStatus',
          'pair': 'XBT/EUR',
          'reqid': 356401587,
          'status': 'error',
          'subscription': {'name': 'ticker'},
        }
        {
          "errorMessage": "EOrder:Order minimum not met",
          "event": "addOrderStatus",
          "status": "error"
        }
        {
          "errorMessage": "EOrder:Unknown order",
          "event": "cancelOrderStatus",
          "status": "error"
        }
        """
        if not isinstance(r, dict):
            return
        
        if r.get('status') == 'ok':
            return
        
        if r.get('status') != 'error' or 'errorMessage' not in r: # or'event' not in r :
            self.notify_unknown(r)
            return
        
        msg = r['errorMessage']
        #event = r['event']
        if msg in self.exceptions:
            raise self.exceptions(msg)
    
    
    def on_ticker(self, r):
        """
        channelID     integer     ChannelID of pair-ticker subscription
        (Anonymous)     object     
            a     array     Ask
                price     float     Best ask price
                wholeLotVolume     integer     Whole lot volume
                lotVolume     float     Lot volume
            b     array     Bid
                price     float     Best bid price
                wholeLotVolume     integer     Whole lot volume
                lotVolume     float     Lot volume
            c     array     Close
                price     float     Price
                lotVolume     float     Lot volume
            v     array     Volume
                today     float     Value today
                last24Hours     float     Value 24 hours ago
            p     array     Volume weighted average price
                today     float     Value today
                last24Hours     float     Value 24 hours ago
            t     array     Number of trades
                today     float     Value today
                last24Hours     float     Value 24 hours ago
            l     array     Low price
                today     float     Value today
                last24Hours     float     Value 24 hours ago
            h     array     High price
                today     float     Value today
                last24Hours     float     Value 24 hours ago
            o     array     Open Price
                today     float     Value today
                last24Hours     float     Value 24 hours ago
        channelName     string     Channel Name of subscription
        pair     string     Asset pair
        
        [
          0,
          {
            "a": [
              "5525.40000",
              1,
              "1.000"
            ],
            "b": [
              "5525.10000",
              1,
              "1.000"
            ],
            "c": [
              "5525.10000",
              "0.00398963"
            ],
            "h": [
              "5783.00000",
              "5783.00000"
            ],
            "l": [
              "5505.00000",
              "5505.00000"
            ],
            "o": [
              "5760.70000",
              "5763.40000"
            ],
            "p": [
              "5631.44067",
              "5653.78939"
            ],
            "t": [
              11493,
              16267
            ],
            "v": [
              "2634.11501494",
              "3591.17907851"
            ]
          },
          "ticker",
          "XBT/USD"
        ]
        """
        ticker = r[1]
        symbol_id = r[3]
        self.update_tickers(
            [self.parse_ticker(ticker, symbol_id)], enable_sub=True)
    
    
    def parse_ticker(self, ticker, symbol_id):
        SELF = self
        self = self.api
        symbol = SELF.convert_symbol(symbol_id, 0)
        timestamp = self.milliseconds()
        baseVolume = float(ticker['v'][1])
        vwap = float(ticker['p'][1])
        quoteVolume = None
        if baseVolume is not None and vwap is not None:
            quoteVolume = baseVolume * vwap
        last = float(ticker['c'][0])
        # Why did ccxt leave bidVolume and askVolume to None?
        # Because in https://api.kraken.com/0/public/Ticker?pair=<symbol_id>
        # response the lotVolume == wholeLotVolume (truncated to integer)
        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'high': float(ticker['h'][1]),
            'low': float(ticker['l'][1]),
            'bid': float(ticker['b'][0]),
            'bidVolume': float(ticker['b'][2]),
            'ask': float(ticker['a'][0]),
            'askVolume': float(ticker['a'][2]),
            'vwap': vwap,
            # fetch *today's* open because so does ccxt
            'open': float(ticker['o'][0]),  
            'close': last,
            'last': last,
            'previousClose': None,
            'change': None,
            'percentage': None,
            'average': None,
            'baseVolume': baseVolume,
            'quoteVolume': quoteVolume,
            'info': ticker,
        }
    
    
    def on_orderbook(self, r):
        """
        SNAPSHOT
        ---------
        channelID     integer     ChannelID of pair-order book levels subscription
        (Anonymous)     object     
            as     array     Array of price levels, ascending from best ask
                Array     array     Anonymous array of level values
                price     float     Price level
                volume     float     Price level volume, for updates volume = 0 for level removal/deletion
                timestamp     float     Price level last updated, seconds since epoch
            bs     array     Array of price levels, descending from best bid
                Array     array     Anonymous array of level values
                price     float     Price level
                volume     float     Price level volume, for updates volume = 0 for level removal/deletion
                timestamp     float     Price level last updated, seconds since epoch
        channelName     string     Channel Name of subscription
        pair     string     Asset pair
        
        [
          0,
          {
            "as": [
              [
                "5541.30000",
                "2.50700000",
                "1534614248.123678"
              ],
              [
                "5541.80000",
                "0.33000000",
                "1534614098.345543"
              ],
              [
                "5542.70000",
                "0.64700000",
                "1534614244.654432"
              ]
            ],
            "bs": [
              [
                "5541.20000",
                "1.52900000",
                "1534614248.765567"
              ],
              [
                "5539.90000",
                "0.30000000",
                "1534614241.769870"
              ],
              [
                "5539.50000",
                "5.00000000",
                "1534613831.243486"
              ]
            ]
          },
          "book-100",
          "XBT/USD"
        ]
        
        UPDATE
        ------
        channelID     integer     ChannelID of pair-order book levels subscription
        AnyOf     anyOf     
            a     array     Ask array of level updates.
                Array     array     Anonymous array of level values
                    price     float     Price level
                    volume     float     Price level volume, for updates volume = 0 for level removal/deletion
                    timestamp     float     Price level last updated, seconds since epoch
                    updateType     string     "r" to show republish updates, optional field
            b     array     Bid array of level updates.
                Array     array     Anonymous array of level values
                    price     float     Price level
                    volume     float     Price level volume, for updates volume = 0 for level removal/deletion
                    timestamp     float     Price level last updated, seconds since epoch
                    updateType     string     "r" to show republish updates, optional field
        channelName     string     Channel Name of subscription
        pair     string     Asset pair
        [
          1234,
          {
            "a": [
              [
                "5541.30000",
                "2.50700000",
                "1534614248.456738"
              ],
              [
                "5542.50000",
                "0.40100000",
                "1534614248.456738"
              ]
            ]
          },
          "book-10",
          "XBT/USD"
        ]
        
        [
          1234,
          {
            "b": [
              [
                "5541.30000",
                "0.00000000",
                "1534614335.345903"
              ]
            ]
          },
          "book-10",
          "XBT/USD"
        ]
        
        [
          1234,
          {
            "a": [
              [
                "5541.30000",
                "2.50700000",
                "1534614248.456738"
              ],
              [
                "5542.50000",
                "0.40100000",
                "1534614248.456738"
              ]
            ]
          },
          {
            "b": [
              [
                "5541.30000",
                "0.00000000",
                "1534614335.345903"
              ]
            ]
          },
          "book-10",
          "XBT/USD"
        ]
        
        REPUBLISH
        ---------
        [
          1234,
          {
            "a": [
              [
                "5541.30000",
                "2.50700000",
                "1534614248.456738",
                "r"
              ],
              [
                "5542.50000",
                "0.40100000",
                "1534614248.456738",
                "r"
              ]
            ]
          },
          "book-25",
          "XBT/USD"
        ]
        """
        timestamp = None
        
        def parse_item(item):
            nonlocal timestamp
            price, amount, ts = item[:3]
            price = float(price)
            amount = float(amount)
            ts = int(float(ts) * 1000)
            timestamp = max(timestamp, ts) if timestamp is not None else ts
            return [price, amount]
        
        symbol = self.convert_symbol(r[-1], 0)
        
        # It may send updates for a few seconds after unsubscribing
        if not self.is_subscribed_to(('orderbook',symbol), active=None):
            return
        
        is_snapshot = 'bs' in r[1] or 'as' in r[1]
        bid_key = 'bs' if is_snapshot else 'b'
        ask_key = 'as' if is_snapshot else 'a'
        
        bids = r[1].get(bid_key, [])
        asks = r[1].get(ask_key, [])
        
        if not is_snapshot and isinstance(r[2], dict):
            if bid_key in r[2]:
                bids = r[2][bid_key]
            if ask_key in r[2]:
                asks = r[2][ask_key]
        
        ob = {
            'symbol': symbol,
            'bids': [parse_item(x) for x in bids],
            'asks': [parse_item(x) for x in asks],
            'nonce': None,
        }
        
        ob['timestamp'] = timestamp
        ob['datetime'] = self.api.iso8601(timestamp) if timestamp is not None else timestamp
        
        if is_snapshot:
            self.orderbook_maintainer.send_orderbook(ob)
        else:
            self.orderbook_maintainer.send_update(ob)
    
    
    def on_ohlcv(self, r):
        """
        channelID     integer     ChannelID of pair-ohlc subscription
            Array     array     
                time     float     Time, seconds since epoch
                etime     float     End timestamp of the interval
                open     float     Open price at midnight UTC
                high     float     Intraday high price
                low     float     Intraday low price
                close     float     Closing price at midnight UTC
                vwap     float     Volume weighted average price
                volume     integer     Accumulated volume today
                count     integer     Number of trades today
        channelName     string     Channel Name of subscription
        pair     string     Asset pair
        
        [
          42,
          [
            "1542057314.748456",
            "1542057360.435743",
            "3586.70000",
            "3586.70000",
            "3586.60000",
            "3586.60000",
            "3586.68894",
            "0.03373000",
            2
          ],
          "ohlc-5",
          "XBT/USD"
        ]
        """
        symbol = self.convert_symbol(r[3], 0)
        tf = r[2].split('-')[1]
        timeframe = self.convert_timeframe(tf, 0)
        parsed = self.parse_ohlcv(r[1], timeframe=timeframe)
        self.update_ohlcv(
            [{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': [parsed]}],
            enable_sub=True)
    
    
    def parse_ohlcv(self, ohlcv, market=None, timeframe='1m', since=None, limit=None):
        SELF = self
        self = self.api
        seconds = int(self.timeframes[timeframe]) * 60
        dot_loc = ohlcv[1].find('.') if '.' in ohlcv[1] else None
        
        return [
            (int(ohlcv[1][:dot_loc]) - seconds) * 1000,
            float(ohlcv[2]),
            float(ohlcv[3]),
            float(ohlcv[4]),
            float(ohlcv[5]),
            float(ohlcv[7]),
        ]
    
    
    def on_trade(self, r):
        """
        channelID     integer     ChannelID of pair-trade subscription
        Array     array     
            Array     array     
                price     float     Price
                volume     float     Volume
                time     float     Time, seconds since epoch
                side     string     Triggering order side, buy/sell
                orderType     string     Triggering order type market/limit
                misc     string     Miscellaneous
                channelName     string     Channel Name of subscription
        pair     string     Asset pair
        
        [
          0,
          [
            [
              "5541.20000",
              "0.15850568",
              "1534614057.321597",
              "s",
              "l",
              ""
            ],
            [
              "6060.00000",
              "0.02455000",
              "1534614057.324998",
              "b",
              "l",
              ""
            ]
          ],
          "trade",
          "XBT/USD"
        ]
        """
        marketId = r[3]
        symbol = self.convert_symbol(marketId, 0)
        trades = [self.parse_trade(x, marketId) for x in r[1]]
        # Since trade id is not includedit must be sorted by timestamp
        # This however can mess up the data when there are multiple trades on a single
        # millisecond (duplicates are dropped). To counter this, sort by ('microsecond', 'volume'),
        # which makes it highly unlikely that two trades would overlap. Although their exact
        # order might change if microseconds overlap and first trade volume > second trade volume
        key = self.trade['sort_by'] # = lambda x: (float(x['info'][2]), x['amount'])
        self.update_trades(
            [{'symbol': symbol, 'trades': trades}], key=key, enable_sub=True)
    
    
    def parse_trade(self, trade, marketId):
        SELF = self
        self = self.api
        symbol = None
        timestamp = None
        side = None
        type = None
        price = None
        amount = None
        id = None
        order = None
        fee = None
        market = None
        if marketId is None:
            marketId = self.safe_string(trade, 'pair')
        if marketId is None:
            pass
        elif '/' not in marketId:
            foundMarket = self.find_market_by_altname_or_id(marketId)
            if foundMarket is not None:
                market = foundMarket
            elif marketId is not None:
                # delisted market ids go here
                market = self.get_delisted_market_by_id(marketId)
        else:
            symbol = SELF.convert_symbol(marketId, 0)
        if symbol is None and market is not None:
            symbol = market['symbol']
        if isinstance(trade, list):
            timestamp = int(float(trade[2]) * 1000)
            side = 'sell' if (trade[3] == 's') else 'buy'
            type = 'limit' if (trade[4] == 'l') else 'market'
            price = float(trade[0])
            amount = float(trade[1])
            tradeLength = len(trade)
            if tradeLength > 6:
                id = trade[6]  # artificially added as per  #1794
        elif 'ordertxid' in trade:
            order = trade['ordertxid']
            id = self.safe_string_2(trade, 'id', 'postxid')
            if 'time' in trade:
                timestamp = int(float(trade['time']) * 1000)
            side = trade['type']
            type = trade['ordertype']
            price = self.safe_float(trade, 'price')
            amount = self.safe_float(trade, 'vol')
            if 'fee' in trade:
                currency = None
                if symbol and '/' in symbol:
                    currency = symbol.split('/')[1]
                fee = {
                    'cost': self.safe_float(trade, 'fee'),
                    'currency': currency,
                }
        return {
            'id': id,
            'order': order,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'symbol': symbol,
            'type': type,
            'side': side,
            'takerOrMaker': None,
            'price': price,
            'amount': amount,
            'cost': price * amount,
            'fee': fee,
            'info': trade,
        }
    
    
    def on_spread(self, r):
        """
        channelID     integer     ChannelID of pair-spreads subscription
        Array     array     
            bid     float     Bid price
            ask     float     Ask price
            timestamp     float     Time, seconds since epoch
            bidVolume     float     Bid Volume
            askVolume     float     Ask Volume
            channelName     string     Channel Name of subscription
            pair     string     Asset pair
            
        [
          0,
          [
            "5698.40000",
            "5700.00000",
            "1542057299.545897",
            "1.01234567",
            "0.98765432"
          ],
          "spread",
          "XBT/USD"
        ]
        """
        logger.debug(r)
    
    
    def on_order(self, r):
        """
        (Dictionary)     object     
            orderid     object     Order object
            refid     string     Referral order transaction id that created this order
            userref     integer     user reference id
            status     string     status of order:
            opentm     float     unix timestamp of when order was placed
            starttm     float     unix timestamp of order start time (if set)
            expiretm     float     unix timestamp of order end time (if set)
            descr     object     order description info
                pair     string     asset pair
                type     string     type of order (buy/sell)
                ordertype     string     order type
                price     float     primary price
                price2     float     secondary price
                leverage     float     amount of leverage
                order     string     order description
                close     string     conditional close order description (if conditional close set)
            vol     float     volume of order (base currency unless viqc set in oflags)
            vol_exec     float     volume executed (base currency unless viqc set in oflags)
            cost     float     total cost (quote currency unless unless viqc set in oflags)
            fee     float     total fee (quote currency)
            avg_price     float     average price (quote currency unless viqc set in oflags)
            stopprice     float     stop price (quote currency, for trailing stops)
            limitprice     float     triggered limit price (quote currency, when limit based order type triggered)
            misc     string     comma delimited list of miscellaneous info: stopped=triggered by stop price, touched=triggered by touch price, liquidation=liquidation, partial=partial fill
            oflags     string     Comma delimited list of order flags (optional). viqc = volume in quote currency (not available for leveraged orders), fcib = prefer fee in base currency, fciq = prefer fee in quote currency, nompp = no market price protection, post = post only order (available when ordertype = limit)
            channelName     string     Channel Name of subscription
        
        [
          [
            {
              "OGTT3Y-C6I3P-XRI6HX": {
                    "cost": "0.00000",
                    "descr": {
                      "close": "",
                      "leverage": "0:1",
                      "order": "sell 10.00345345 XBT/EUR @ limit 34.50000 with 0:1 leverage",
                      "ordertype": "limit",
                      "pair": "XBT/EUR",
                      "price": "34.50000",
                      "price2": "0.00000",
                      "type": "sell"
                    },
                    "expiretm": "0.000000",
                    "fee": "0.00000",
                    "limitprice": "34.50000",
                    "misc": "",
                    "oflags": "fcib",
                    "opentm": "0.000000",
                    "price": "34.50000",
                    "refid": "OKIVMP-5GVZN-Z2D2UA",
                    "starttm": "0.000000",
                    "status": "open",
                    "stopprice": "0.000000",
                    "userref": 0,
                    "vol": "10.00345345",
                    "vol_exec": "0.00000000"
                },
            },
            {
              "1BC4E-F7HIJ-K99NO": {
                    "status": "closed"
                },
            },
          ],
          "openOrders",
        ]
        """
        orders = r[0]
        for o_dict in orders:
            id, order = list(o_dict.items())[0]
            order['id'] = id
            o_parsed = self.parse_order(order)
            
            if id not in self.orders:
                self.add_order_from_dict(o_parsed, enable_sub=True)
            else:
                self.update_order_from_dict(o_parsed, enable_sub=True, drop={None: True})
    
    
    def parse_order(self, order, market=None):
        SELF = self
        self = self.api
        description = self.safe_value(order, 'descr', {})
        side = self.safe_string(description, 'type')
        type = self.safe_string(description, 'ordertype')
        marketId = self.safe_string(description, 'pair')
        id = self.safe_string(order, 'id')
        symbol = None
        stored_order = SELF.orders.get(id, None) if id is not None else None
        if stored_order and marketId is None:
            symbol = stored_order['symbol']
        elif marketId is None:
            pass
        elif '/' not in marketId:
            foundMarket = self.find_market_by_altname_or_id(marketId)
            if foundMarket is not None:
                market = foundMarket
            elif marketId is not None:
                # delisted market ids go here
                market = self.get_delisted_market_by_id(marketId)
        else:
            symbol = SELF.convert_symbol(marketId, 0)
        if market is None and symbol is not None:
            market = self.safe_value(self.markets, symbol, None)
        timestamp = None
        if 'opentm' in order:
            timestamp = int(float(order['opentm']) * 1000)
        amount = self.safe_float(order, 'vol')
        filled = self.safe_float(order, 'vol_exec')
        remaining = None
        if amount is None and stored_order:
            amount = stored_order['amount']
        if amount is not None and filled is not None:
            remaining = amount - filled
        fee = None
        cost = self.safe_float(order, 'cost')
        price = self.safe_float(description, 'price')
        if (price is None) or (price == 0):
            price = self.safe_float(description, 'price2')
        if (price is None) or (price == 0):
            price = self.safe_float(order, 'price', price)
        average = self.safe_float_2(order, 'avg_price', 'price')
        if average == 0:
            average = None
        if market is not None:
            symbol = market['symbol']
            if 'fee' in order:
                flags = self.safe_string(order, 'oflags', '')
                feeCost = self.safe_float(order, 'fee')
                fee = {
                    'cost': feeCost,
                    'rate': None,
                }
                if flags.find('fciq') >= 0:
                    fee['currency'] = market['quote']
                elif flags.find('fcib') >= 0:
                    fee['currency'] = market['base']
        status = self.parse_order_status(self.safe_string(order, 'status'))
        if status in ('canceled','closed'):
            remaining = 0.0
        
        return {
            'id': id,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'lastTradeTimestamp': None,
            'status': status,
            'symbol': symbol,
            'type': type,
            'side': side,
            'price': price,
            'cost': cost,
            'amount': amount,
            'filled': filled,
            'average': average,
            'remaining': remaining,
            'fee': fee,
            # 'trades': self.parse_trades(order['trades'], market),
            'info': order,
        }
    
    
    def on_fill(self, r):
        """
        (Dictionary)     object     
            tradeid     object     Trade object
                ordertxid     string     order responsible for execution of trade
                postxid     string     Position trade id
                pair     string     Asset pair
                time     float     unix timestamp of trade
                type     string     type of order (buy/sell)
                ordertype     string     order type
                price     float     average price order was executed at (quote currency)
                cost     float     total cost of order (quote currency)
                fee     float     total fee (quote currency)
                vol     float     volume (base currency)
                margin     float     initial margin (quote currency)
        channelName     string     Channel Name of subscription
        
        [
          [
            {
              "TDLH43-DVQXD-2KHVYY": {
                "cost": "1000000.00000",
                "fee": "1600.00000",
                "margin": "0.00000",
                "ordertxid": "TDLH43-DVQXD-2KHVYY",
                "ordertype": "limit",
                "pair": "XBT/EUR",
                "postxid": "OGTT3Y-C6I3P-XRI6HX",
                "price": "100000.00000",
                "time": "1560516023.070651",
                "type": "sell",
                "vol": "1000000000.00000000"
              }
            },
            {
              "TDLH43-DVQXD-2KHVYY": {
                "cost": "1000000.00000",
                "fee": "600.00000",
                "margin": "0.00000",
                "ordertxid": "TDLH43-DVQXD-2KHVYY",
                "ordertype": "limit",
                "pair": "XBT/EUR",
                "postxid": "OGTT3Y-C6I3P-XRI6HX",
                "price": "100000.00000",
                "time": "1560516023.070658",
                "type": "buy",
                "vol": "1000000000.00000000"
              }
            },
            {
              "TDLH43-DVQXD-2KHVYY": {
                "cost": "1000000.00000",
                "fee": "1600.00000",
                "margin": "0.00000",
                "ordertxid": "TDLH43-DVQXD-2KHVYY",
                "ordertype": "limit",
                "pair": "XBT/EUR",
                "postxid": "OGTT3Y-C6I3P-XRI6HX",
                "price": "100000.00000",
                "time": "1560520332.914657",
                "type": "sell",
                "vol": "1000000000.00000000"
              }
            },
            {
              "TDLH43-DVQXD-2KHVYY": {
                "cost": "1000000.00000",
                "fee": "600.00000",
                "margin": "0.00000",
                "ordertxid": "TDLH43-DVQXD-2KHVYY",
                "ordertype": "limit",
                "pair": "XBT/EUR",
                "postxid": "OGTT3Y-C6I3P-XRI6HX",
                "price": "100000.00000",
                "time": "1560520332.914664",
                "type": "buy",
                "vol": "1000000000.00000000"
              }
            }
          ],
          "ownTrades"
        ]
        """
        # Sometimes json-corrupted messages are received, causing some fills to be missed.
        fills = r[0]
        for d in fills:
            id, fill = list(d.items())[0]
            fill['id'] = [id]
            marketId = fill['pair']
            symbol = self.convert_symbol(marketId, 0)
            f_parsed = self.parse_trade(fill, marketId)
            self.add_fill_from_dict(f_parsed, enable_sub=True)
    
    
    def encode(self, req, sub=None):
        p = req.params
        channel = req.channel
        symbol = p.get('symbol')
        limit = p.get('limit')
        
        out, out2 = {}, {}
        msg_id = random.randint(0, 10**9) #None
        msg_id2 = random.randint(0, 10**9)
        name = self.channel_ids[channel]
        
        if channel == 'orderbook':
            #10, 25, 100, 500, 1000
            if limit is None:
                limit = self.ob['default_limit']
            elif limit >= 1000:
                limit = 1000
            else:
                limit = self.ob_maintainer.resolve_limit(limit)
            self.ob_maintainer.set_limit(symbol, limit)
        
        if sub is not None:
            out['event'] = out2['event'] = 'subscribe' if sub else 'unsubscribe'
            out['subscription'], out2['subscription'] = {}, {}
            if channel != 'account':
                symbol_list = [symbol] if isinstance(symbol, str) else list(symbol)
                pair = [self.convert_symbol(x,1) for x in symbol_list]
                out['pair'] = pair
                out['subscription']['name'] = name
            else:
                out['subscription']['name'] = name[0]
                out2['subscription']['name'] = name[1]
            if channel == 'ohlcv':
                out['subscription']['interval'] = int(self.api.timeframes[p['timeframe']])
            elif channel == 'orderbook' and limit is not None:
                out['subscription']['depth'] = limit
        else:
            # Do create_order and cancel_order accept 'reqid'?
            out['event'] = name
            if channel == 'create_order':
                pair = self.convert_symbol(symbol, 1)
                out.update(
                    {'ordertype': p['type'],
                     'pair': pair,
                     # the instructions say <float> but the example shows string ??
                     'price': str(p['price']), 
                     'type': p['side'],
                     'volume': str(p['amount'])})
            elif channel in ('cancel_order', 'cancel_orders'):
                txid = [p['id']] if isinstance(p['id'], str) else list(p['id'])
                out.update(
                    {'txid': txid})
            else:
                raise ValueError(channel)
        
        if msg_id is not None:
            out['reqid'] = msg_id
        if msg_id2 is not None:
            out2['reqid'] = msg_id2
        
        if channel != 'account':
            return (out, msg_id)
        else:
            return self.merge([(out, msg_id), (out2, msg_id2)])
    
    
    async def sign(self, out):
        #auth_topics = ['ownTrades']
        # Does unsubscribe need the exact same token that it was subscribed with?
        #if out.get('event') == 'subscribe' and out.get('subscription') in auth_topics:
        token = await call_via_loop(self._fetch_and_cache_auth_token, (5,),
                                    loop=self.loop, module='asyncio')
        if 'subscription' in out:
            out['subscription']['token'] = token
        else:
            out['token'] = token
        return out
    
    
    async def on_start(self):
        # This isn't perfect, as the user may not want to subscribe to account
        # (and it could be the case then the apiKey doesn't have "Access WebSockets API" right)
        #if self.apiKey is not None:
        #    self._start_wsToken_fetcher()
        
        # Instead:
        self._start_wsToken_prolonger()
        
        await super().on_start()
    
    
    async def create_connection_url(self, url_factory):
        def _is_authenticated(cnx):
            return self.cm.cnx_infs[cnx].authenticated
        auth = self.apiKey # and not self.is_subscribed_to({'_': 'account'}, active=True)
        
        which = 'base' if not auth else 'private'
        url = self.url_components[which]
        
        return url
    
    
    def _start_wsToken_prolonger(self):
        # Method 1: 
        #  Let the key be fetched via .sign, and prolong the expiry
        #  in every 30 secs if still subbed to account
        stopped = self.station.get_event('stopped', 0, loop=0)
        
        def prolong():
            self._cached_wstoken['expires'] = time.time() + 900
        
        async def start():
            while True:
                try: await asyncio.wait_for(stopped.wait(), 30)
                except asyncio.TimeoutError: pass
                else: break
                if self.is_subscribed_to({'_': 'account'}, active=True):
                    prolong()
        
        asyncio.ensure_future(start())
    
    
    def _start_wsToken_fetcher(self):
        # Method 2: 
        #  If apiKey is attached, start a ticker that fetches the key in every ~715 seconds
        # This is not as good as #1, as we don't know if the user even wants
        # to subscribe to account (despite having initated with apiKey)
        
        # Exit when ExchangeSocket is stopped
        stopped = self.station.get_event('stopped', 0, loop=0)
        lti = self._cached_wstoken['last_ticker_id']
        t = AsyncTicker(self._fetch_and_cache_auth_token,
                        60,
                        keepalive = {'attempts': True,
                                     'pause': 75,
                                     'exit_on': stopped,
                                     # apiKey doesn't have "Access WebSockets API" right
                                     'throw': ccxt.PermissionDenied,},
                        name='{}-wsToken-fetcher-{}'.format(self.name, lti+1),
                        loop=self.loop,)
        
        async def start_and_cancel_on_inactive():
            asyncio.ensure_future(t.loop())
            await stopped.wait()
            await t.close()
        
        call_via_loop(start_and_cancel_on_inactive, loop=self.loop)
        
        self._cached_wstoken['last_ticker_id'] += 1
    
    
    async def _fetch_and_cache_auth_token(self, renew_before=185):
        """
        :param renew_before: seconds before the expiry to renew the token
        """
        # This method doesn't actually add anything to the url
        # but just ensures that the token is cached and is < 15 minutes old
        if not self._cached_wstoken['token'] or \
                time.time() > self._cached_wstoken['expires'] - renew_before:
            # Each call returns new token, even if previous isn't expired yet
            #{'error': [], 'result': {'token': 'RZ67bMFDSl2g233s1bRggF2shGpCChfh1jAeefH+/dZ', 'expires': 900}}
            now = time.time()
            r = await self.api.privatePostGetWebSocketsToken()
            token = r['result']['token']
            expires = r['result']['expires']
            self._cached_wstoken.update({'token': token,
                                         'expires': now + expires})
            
        return self._cached_wstoken['token']
    
    
    async def ping(self):
        await self._socket.send(json.dumps({
            "event": "ping",
            "reqid": ctime_ms()
        }))
    
    
    def safe_currency_code(self, currency_id, currency=None):
        cy = next((x['code'] for x in self.api.currencies.values()
                   if x['info'].get('altname')==currency_id), None)
        if cy is None:
            cy = self.api.common_currency_code(currency_id)
        return cy
    
    
    def currency_id(self, commonCode):
        id = next((x['info'].get('altname') for x in self.api.currencies.values()
                   if x['code']==commonCode), None)
        if id is None:
            currencyIds = {v:k for k, v in self.api.commonCurrencies.items()}
            id = self.api.safe_string(currencyIds, commonCode, commonCode)
        return id
    
    
    def symbol_by_id(self, symbol_id):
        symbol = next((x['symbol'] for x in self.api.markets.values()
                      if x['info'].get('wsname')==symbol_id), None)
        if symbol is None:
            raise KeyError(symbol_id)
        return symbol
    
    
    def id_by_symbol(self, symbol):
        return self.api.markets[symbol]['info']['wsname']