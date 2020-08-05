import asyncio
#import aiohttp
import time
import datetime
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket, ExchangeSocketError

from fons.aio import call_via_loop
from fons.sched import AsyncTicker
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

    
class binance(ExchangeSocket):
    exchange = 'binance'
    auth_defaults = {
        'via_url': True,
    }
    url_components = {
        'ws': 'wss://stream.binance.com:9443/ws',
    }
    channel_defaults = {
        'url': '<$ws>/<m$create_url>',
        'send': False,
        'merge_limit': 45,
    }
    channels = {
        'all_tickers': {
            'url': '<$ws>/!ticker@arr',
        },
        'ticker': {
            #'url': '<$ws>/<symbol>@ticker',
            'merge_option': True,
        },
        'orderbook': {
            #'url': '<$ws>/<symbol>@depth',
            'merge_option': True,
        },
        'trades': {
            #'url': '<$ws>/<symbol>@trade',
            'merge_option': True,
        },
        'ohlcv': {
            #'url': '<$ws>/<symbol>@kline_<timeframe>',
            'merge_option': True,
        },
        'account': {
            'url': '<$ws>/<m$fetch_listen_key>',
            'auto_activate': 'on_cnx_activation',
        },
    }
    has = {
        'all_tickers': True,
        'ticker': {
            'last': True, 'bid': True, 'ask': True, 'bidVolume': True, 'askVolume': True,
            'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': True,
            'change': True, 'percentage': True, 'average': True, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True, 'active': False},
        'orderbook': True,
        'trades': {
            'timestamp': True, 'datetime': True, 'symbol': True, 'id': True,
            'order': False, 'type': False, 'takerOrMaker': False, 'side': True,
            'price': True, 'amount': True, 'cost': False, 'fee': False},
        'ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'account': {'balance': True, 'order': True, 'fill': False},
        'fetch_tickers': True,
        'fetch_ticker': {
            'ask': True, 'askVolume': True, 'average': False, 'baseVolume': True, 'bid': True, 'bidVolume': True,
            'change': True, 'close': True, 'datetime': True, 'high': True, 'last': True, 'low': True, 'open': True,
            'percentage': True, 'previousClose': True, 'quoteVolume': True, 'symbol': True, 'timestamp': True,
            'vwap': True},
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'bids': True, 'asks': True, 'nonce': True, 'datetime': False, 'timestamp': False},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'fetch_my_trades': {
            'symbolRequired': True,
            'amount': True, 'cost': True, 'datetime': True, 'fee': True, 'id': True, 'order': True,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': True, 'timestamp': True, 'type': False},
        'fetch_order': {
            'amount': True, 'average': True, 'clientOrderId': True, 'cost': True, 'datetime': True, 'fee': False,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': False, 'type': True},
        'fetch_orders': {'symbolRequired': True},
        'fetch_open_orders': {'symbolRequired': True},
        'fetch_closed_orders': {'symbolRequired': True},
        'create_order': True,
    }
    has['all_tickers'] = has['ticker'].copy()
    has['fetch_tickers'] = has['fetch_ticker'].copy()
    has['fetch_orders'].update(has['fetch_order'])
    has['fetch_open_orders'].update(has['fetch_order'])
    has['fetch_closed_orders'].update(has['fetch_order'])
    has['create_order'] = has['fetch_order'].copy()
    connection_defaults = {
        #'ping': lambda: {},
        #'ping': lambda: {'ping': nonce_ms()},
        #TODO: change back
        'ping_interval': 300,
    }
    ob = {
        #'limits': [5, 10, 20], #not supported yet
        'fetch_limit': 1000,
        'fetch_limits': [5, 10, 20, 50, 100, 500, 1000, 5000],
    }
    order = {
        'cancel_automatically': 'if-not-subbed-to-account',
    }
    symbol = {
        'quote_ids': ['BTC','ETH','BNB','XRP','PAX','TUSD','USDT','USDC','USDS','BUSD'],
        'sep': '',
    }
    
    
    def handle(self, R):
        r = R.data
        if isinstance(r, dict):
            type = r.get('e')
            if type == '24hrTicker':
                self.on_ticker(r)
            elif type == 'depthUpdate':
                self.on_orderbook_update(r)
            elif type in ('outboundAccountInfo','outboundAccountPosition'):
                self.on_balance(r)
            elif type in ('executionReport','listStatus'):
                self.on_order(r)
            elif type in ('trade','aggTrade'):
                self.on_trade(r)
            elif type == 'kline':
                self.on_ohlcv(r)
            elif type == 'ACCOUNT_UPDATE':
                self.on_futures_account(r)
            elif type == 'ORDER_TRADE_UPDATE':
                self.on_futures_order(r)
            elif 'lastUpdateId' in r:
                self.on_limited_ob_update(r)
            #elif r == {} or 'ping' in r:
            #    print('Received ping response? : {}'.format(r))
            #    cnx = self.cm.connections[R.id]
            #    call_via_loop(cnx.send, (r,), loop=cnx.loop)
            else:
                self.notify_unknown(r)
        elif isinstance(r, list) and len(r):
            if isinstance(r[0], dict) and r[0].get('e') == '24hrTicker':
                self.on_all_tickers(r)
            else:
                self.notify_unknown(r)
        else:
            self.notify_unknown(r)
    
    
    def on_all_tickers(self, r):
        """[...]"""
        reload_markets = False
        entries = []
        
        for x in r:
            try: 
                entries.append(
                    self.on_ticker(x, send=False))
            except KeyError as e:
                reload_markets = True
                
        if reload_markets and time.time() - getattr(self,'_markets_last_reloaded',0) > 60:
            logger2.error('{} - reloading markets due to KeyError'.format(self.name))
            asyncio.ensure_future(self.api.poll_load_markets(limit=60))
            self._markets_last_reloaded = time.time()
        
        enable_sub = 'all_tickers' if not reload_markets else False
        
        self.update_tickers(entries, enable_sub=enable_sub)
    
    
    def on_ticker(self, r, *, send=True):
        """Spot and futures:
        {
          "e": "24hrTicker",  // Event type
          "E": 123456789,     // Event time
          "s": "BNBBTC",      // Symbol
          "p": "0.0015",      // Price change
          "P": "250.00",      // Price change percent
          "w": "0.0018",      // Weighted average price
          "x": "0.0009",      // First trade(F)-1 price (first trade before the 24hr rolling window) [spot]
          "c": "0.0025",      // Last price
          "Q": "10",          // Last quantity
          "b": "0.0024",      // Best bid price [spot]
          "B": "10",          // Best bid quantity [spot]
          "a": "0.0026",      // Best ask price [spot]
          "A": "100",         // Best ask quantity [spot]
          "o": "0.0010",      // Open price
          "h": "0.0025",      // High price
          "l": "0.0010",      // Low price
          "v": "10000",       // Total traded base asset volume
          "q": "18",          // Total traded quote asset volume
          "O": 0,             // Statistics open time
          "C": 86400000,      // Statistics close time
          "F": 0,             // First trade ID
          "L": 18150,         // Last trade Id
          "n": 18151          // Total number of trades
        }
        #The update interval is 1000ms for spot and 3000ms for futures.
        """
        map = {
            'symbol': 's',
            'timestamp': 'E',
            'last': 'c',
            'bid': 'b',
            'bidVolume': 'B',
            'ask': 'a',
            'askVolume': 'A',
            'high': 'h',
            'low': 'l',
            'open': 'o',
            'previousClose': 'x',
            'baseVolume': 'v',
            'quoteVolume': 'q',
            'vwap': 'w',
            'change': 'p',
            'percentage': 'P',
        }
        params = {p: r[m] for p,m in map.items() if m in r}
        params['symbol'] = self.convert_symbol(params['symbol'], 0)
        params['lastVolume'] = float(r['Q']) if isinstance(r['Q'],str) else None
        
        entry = self.api.ticker_entry(**params, info=r)
        
        if send:
            self.update_tickers([entry], enable_sub=True)
        
        return entry
    
    
    def on_orderbook_update(self, r):
        """Spot and futures:
        {
          "e": "depthUpdate", // Event type
          "E": 123456789,     // Event time
          "T": 123456788,     // transaction time  [futures only]
          "s": "BNBBTC",      // Symbol
          "U": 157,           // First update ID in event
          "u": 160,           // Final update ID in event
          "pu": 149,          // last update Id in last stream(ie "u" in last stream) [futures only]
          "b": [              // Bids to be updated
            [
              "0.0024",       // Price level to be updated
              "10"            // Quantity
            ]
          ],
          "a": [              // Asks to be updated
            [
              "0.0026",       // Price level to be updated
              "100"           // Quantity
            ]
          ]
        }"""
        #While listening to the stream, each new event's pu should be equal to the previous event's u,
        #otherwise fetch new order book.
        update = {
            'symbol': self.convert_symbol(r['s'], 0),
            'timestamp': r['E'],
            'bids': [[float(y) for y in x] for x in r['b']],
            'asks': [[float(y) for y in x] for x in r['a']],
            'nonce': (r['U'],r['u']) if 'pu' not in r else (r['pu']+1,r['u']),
        }
        self.orderbook_maintainer.send_update(update)
    
    
    def on_limited_ob_update(self, R):
        """
        {
            'lastUpdateId': 2905372000,
            'bids': [['6565.79000000', '0.00300700'], ['6565.66000000', '0.06990100'], ...],
            'asks': [['6566.32000000', '0.00000900'], ['6567.43000000', '0.01500000'], ...],
        }
        """
        raise NotImplementedError('{} - limited ob not supported yet'.format(self.exchange))
        
        r = R.data
        cnx = self.cm.connections[R.id]
        ob = dict(
            symbol='?',
            **self.api.parse_order_book(r),
        )
        ob['nonce'] = r['lastUpdateId']
        self.orderbook_maintainer.send_orderbook(ob)
    
    
    def on_trade(self, r):
        """Spot:
        {
          "e": "trade",     // Event type
          "E": 123456789,   // Event time
          "s": "BNBBTC",    // Symbol
          "t": 12345,       // Trade ID
          "p": "0.001",     // Price
          "q": "100",       // Quantity
          "b": 88,          // Buyer order ID
          "a": 50,          // Seller order ID
          "T": 123456785,   // Trade time
          "m": true,        // Is the buyer the market maker?
          "M": true         // Ignore
        }
        Spot and futures:
        {
          "e": "aggTrade",  // Event type
          "E": 123456789,   // Event time
          "s": "BTCUSDT",    // Symbol
          "a": 5933014,     // Aggregate trade ID
          "p": "0.001",     // Price
          "q": "100",       // Quantity
          "f": 100,         // First trade ID
          "l": 105,         // Last trade ID
          "T": 123456785,   // Trade time
          "m": true,        // Is the buyer the market maker?
          "M": true         // Ignore [spot only]
        }"""
        
        symbol = self.convert_symbol(r['s'], 0)
        id = r['t'] if r['e']=='trade' else r['a'] 
        price = r['p']
        amount = r['q']
        ts = r['T']
        side = ['buy','sell'][r['m']]
        
        e = self.api.trade_entry(symbol=symbol, timestamp=ts, id=id,
                                 side=side, price=price, amount=amount,
                                 info=r)
        
        self.update_trades([{'symbol': symbol, 'trades': [e]}], enable_sub=True)
        
        
    def on_ohlcv(self, r):
        """Spot and futures:
        {
          "e": "kline",     // Event type
          "E": 123456789,   // Event time
          "s": "BNBBTC",    // Symbol
          "k": {
            "t": 123400000, // Kline start time
            "T": 123460000, // Kline close time
            "s": "BNBBTC",  // Symbol
            "i": "1m",      // Interval
            "f": 100,       // First trade ID
            "L": 200,       // Last trade ID
            "o": "0.0010",  // Open price
            "c": "0.0020",  // Close price
            "h": "0.0025",  // High price
            "l": "0.0015",  // Low price
            "v": "1000",    // Base asset volume
            "n": 100,       // Number of trades
            "x": false,     // Is this kline closed?
            "q": "1.0000",  // Quote asset volume
            "V": "500",     // Taker buy base asset volume
            "Q": "0.500",   // Taker buy quote asset volume
            "B": "123456"   // Ignore
          }
        }"""
        symbol = self.convert_symbol(r['s'], 0)
        rr = r['k']
        e = self.api.ohlcv_entry(timestamp=rr['t'], open=rr['o'], high=rr['h'],
                                 low=rr['l'], close=rr['c'], volume=rr['v'])
        tf = self.convert_timeframe(rr['i'], 0)
        
        self.update_ohlcv([{'symbol': symbol, 'timeframe': tf, 'ohlcv': [e]}], enable_sub=True)
        
    
    def on_balance(self, r):
        """{
          "e": "outboundAccountInfo",   // Event type
          "E": 1499405658849,           // Event time
          "m": 0,                       // Maker commission rate (bips)
          "t": 0,                       // Taker commission rate (bips)
          "b": 0,                       // Buyer commission rate (bips)
          "s": 0,                       // Seller commission rate (bips)
          "T": true,                    // Can trade?
          "W": true,                    // Can withdraw?
          "D": true,                    // Can deposit?
          "u": 1499405658848,           // Time of last account update
          "B": [                        // Balances array
            {
              "a": "LTC",               // Asset
              "f": "17366.18538083",    // Free amount
              "l": "0.00000000"         // Locked amount
            },
            {
              "a": "BTC",
              "f": "10537.85314051",
              "l": "2.19464093"
            },
            {
              "a": "ETH",
              "f": "17902.35190619",
              "l": "0.00000000"
            },
            {
              "a": "BNC",
              "f": "1114503.29769312",
              "l": "0.00000000"
            },
            {
              "a": "NEO",
              "f": "0.00000000",
              "l": "0.00000000"
            }
          ]
        }"""
        updates = [
            {
                'cy': self.convert_cy(x['a'], 0),
                'free': float(x['f']),
                'used': float(x['l']),
                'info': x,
            } for x in r['B']
        ]
        self.update_balances(updates, enable_sub=True)
        
        
    def on_order(self, r):
        tlogger.debug(r)
        if r['e'] == 'listStatus':
            return
        """{
          "e": "executionReport",        // Event type
          "E": 1499405658658,            // Event time
          "s": "ETHBTC",                 // Symbol
          "c": "mUvoqJxFIILMdfAW5iGSOW", // Client order ID
          "S": "BUY",                    // Side
          "o": "LIMIT",                  // Order type
          "f": "GTC",                    // Time in force
          "q": "1.00000000",             // Order quantity
          "p": "0.10264410",             // Order price
          "P": "0.00000000",             // Stop price
          "F": "0.00000000",             // Iceberg quantity
          "g": -1                        // OrderListId
          "C": "null",                   // Original client order ID; This is the ID of the order being canceled
          "x": "NEW",                    // Current execution type {NEW, CANCELED, REPLACED (currently unused), REJECTED, TRADE, EXPIRED}
          "X": "NEW",                    // Current order status
          "r": "NONE",                   // Order reject reason; will be an error code.
          "i": 4293153,                  // Order ID
          "l": "0.00000000",             // Last executed quantity
          "z": "0.00000000",             // Cumulative filled quantity
          "L": "0.00000000",             // Last executed price
          "n": "0",                      // Commission amount
          "N": null,                     // Commission asset
          "T": 1499405658657,            // Transaction time
          "t": -1,                       // Trade ID
          "I": 8641984,                  // Ignore
          "w": true,                     // Is the order working? Stops will have
          "m": false,                    // Is this trade the maker side?
          "M": false,                    // Ignore
          "O": 1499405658657,            // Order creation time
          "Z": "0.00000000",             // Cumulative quote asset transacted quantity
          "Y": "0.00000000"              // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
        }"""
        
        price = float(r['p'])
        stop = float(r['P'])
        
        d = {
            'id': str(r['i']), #ccxt parses to str
            'symbol': self.convert_symbol(r['s'], 0),
            'type': r['o'].lower(),
            'side': r['S'].lower(),
            'price': price if price else None,
            'amount': float(r['q']),
            'timestamp': r['E'],
            'stop': stop if stop else None,
            'filled': float(r['z']),
        }
        
        #STOP ORDER will first send
        #EXPIRED, then NEW (as it turns into MARKET/LIMIT order), then FILLED
        
        if r['X'] in ('CANCELED','REJECTED') or r['X']=='EXPIRED' and not d['stop']:
            d['remaining'] = 0
        else:
            d['remaining'] = d['amount'] - d['filled']
            
        d['payout'] = d['filled'] if d['side']=='buy' else float(r['Z'])
        d['params'] = {'info': r}
        
        o = None
        try: o = self.orders[d['id']]
        except KeyError: pass
        
        if o is None:
            self.add_order(**d)
        else:
            # include extra just in case edit_order functionality will be added
            extra = {k:v for k,v in d.items() if k in ['type','side','price','stop','amount']}
            self.update_order(d['id'], d['remaining'], d['filled'], d['payout'], params=extra)
    
    
    def create_url(self, url_factory):
        params = url_factory.params
        channel = params['_']
        symbol = params['symbol']
        timeframe = params.get('timeframe')
        limit = params.get('limit')
        if isinstance(symbol, str):
            symbol = [symbol]
        is_spot = self.exchange=='binance'
        suffixes = {'ticker': '@ticker',
                    'orderbook': '@depth',
                    'trades': '@trade' if is_spot else '@aggTrade',
                    'ohlcv': '@kline_{}'.format(timeframe)}
        if channel=='orderbook' and limit is not None:
            limit = self.ob_maintainer.resolve_limit(limit)
            _symbol = [self.convert_symbol(x.upper(), 0) for x in symbol]
            self.ob_maintainer.set_limit(_symbol, limit)
            params['limit'] = limit
            if limit is not None:
                suffixes['orderbook'] += str(limit)
        suffix = suffixes[channel]
        return '/'.join(['{}{}'.format(s, suffix) for s in symbol])
    
    
    async def fetch_listen_key(self):
        """{
            "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
        }"""
        is_spot = self.exchange=='binance'
        fapiMethod = 'fapiPrivatePostListenKey' if hasattr(self.api, 'fapiPrivatePostListenKey') \
                     else 'fapiPublicPostListenKey'
        method = 'publicPostUserDataStream' if is_spot else fapiMethod
        params = {}
        
        async def fetch_key():
            #The method isn't asynchronous itself (it just returns coro)
            # thus it can't be used directly on call_via_loop
            return await getattr(self.api, method)(params)

        #url = 'https://api.binance.com/api/v1/userDataStream'
        #api.fetch2(self, path, api='public', method='GET', params={}, headers=None, body=None):
                
        r = await call_via_loop(fetch_key,
                                loop=self.loop,
                                module='asyncio')
        
        listen_key = r['listenKey']
        
        r2 = await call_via_loop(self.prolong_listen_key_expiry,
                                 args=(listen_key,),
                                 loop=self.loop,
                                 module='asyncio')
        
        self.start_listen_key_prolonging(listen_key)
        
        return listen_key
    
    
    async def prolong_listen_key_expiry(self, listen_key):
        is_spot = self.exchange=='binance'
        fapiMethod = 'fapiPrivatePutListenKey' if hasattr(self.api, 'fapiPrivatePutListenKey') \
                     else 'fapiPublicPutListenKey'
        method = 'publicPutUserDataStream' if is_spot else fapiMethod
        params = {'listenKey': listen_key}
        
        async def prolong_key():
            return await getattr(self.api, method)(params)
        
        logger.debug('{} - prolonging listen key'.format(self.name))
        
        r = await call_via_loop(prolong_key,
                                loop=self.loop,
                                module='asyncio')
        
        #logger.debug('{} - listen key prolonging response: {}'.format(self.name))
        
        return r
    
    
    def start_listen_key_prolonging(self, listen_key):
        try:
            s = self.get_subscription({'_': 'account'})
        except ValueError:
            return
        
        inactive = s.station.get_event('inactive', 0, loop=0)
        ticker = AsyncTicker(self.prolong_listen_key_expiry,
                             50 * 60,
                             args=(listen_key,),
                             lock='next',
                             keepalive = {'attempts': True,
                                          'pause': 75,
                                          'exit_on': inactive},
                             name='{}-listen-key-prolonger:{}'.format(self.name, listen_key[:5]),
                             loop=self.loop)
        
        async def prolong_and_cancel_on_inactive():
            while True:
                try: await s.wait_till_active(15)
                except asyncio.TimeoutError:
                    if not any(_s is s for _s in self.sh.subscriptions):
                        return
                else: break
            asyncio.ensure_future(ticker.loop())
            await inactive.wait()
            await ticker.close()
            
        call_via_loop(prolong_and_cancel_on_inactive,
                      loop=self.loop,
                      module='asyncio')
