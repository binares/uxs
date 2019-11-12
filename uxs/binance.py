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
        'base': 'wss://stream.binance.com:9443/ws/',
    }
    channel_defaults = {
        'url': '<$base><m$create_url>',
        'send': False,
        'merge_limit': 45,
    }
    channels = {
        'all_tickers': {
            'url': '<$base>!ticker@arr',
        },
        'ticker': {
            #'url': '<$base><symbol>@ticker',
            'merge_option': True,
        },
        'market': {
            #'url': '<$base><symbol>@trade',
            'merge_option': True,
        },
        'orderbook': {
            #'url': '<$base><symbol>@depth',
            'merge_option': True,
            'fetch_limit': 1000,
        },
        'account': {
            'url': '<$base><m$fetch_listen_key>',
            'auto_activate': 'on_cnx_activation',
        },
    }
    has = {
        'balance': {'_': True, 'delta': False},
        'all_tickers': True,
        'ticker': True,
        'orderbook': True,
        'account': {'balance': True, 'order': True, 'match': True},
        'fetch_tickers': {
            'last': True, 'bid': True, 'ask': True, 'bidVolume': False, 'askVolume': False, 
            'high': True, 'low': True, 'open': True, 'close': True, 
            'previousClose': False, 'change': True, 'percentage': True, 
            'average': True, 'vwap': True, 'baseVolume': True, 'quoteVolume': True},
        'fetch_ticker': True,
        'fetch_order_book': True,
        'fetch_balance': True,
    }
    connection_defaults = {
        #'ping': lambda: {},
        #'ping': lambda: {'ping': nonce_ms()},
        #TODO: change back
        #'ping_interval': 300,
    }
    order = {
        'cancel_automatically': 'if-not-subbed-to-account',
    }
    
    
    def handle(self, R):
        r = R.data
        if isinstance(r, dict):
            if r.get('e') == '24hrTicker':
                self.on_ticker(r)
            elif r.get('e') == 'depthUpdate':
                self.on_orderbook_update(r)
            elif r.get('e') in ('outboundAccountInfo','outboundAccountPosition'):
                self.on_balance(r)
            elif r.get('e') in ('executionReport','listStatus'):
                self.on_order(r)
            #elif r == {} or 'ping' in r:
            #    print('Received ping response? : {}'.format(r))
            #    cnx = self.cm.connections[R.id]
            #    call_via_loop(cnx.send, (r,), loop=cnx.loop)
            else:
                self.notify_unknown_response(r)
        elif isinstance(r, list) and len(r):
            if isinstance(r[0], dict) and r[0].get('e') == '24hrTicker':
                self.on_all_tickers(r)
            else:
                self.notify_unknown_response(r)
        else:
            self.notify_unknown_response(r)
    
    
    def on_all_tickers(self, r):
        """[...]"""
        reload_markets = False
        
        for x in r:
            try: self.on_ticker(x)
            except KeyError as e:
                reload_markets = True
                
        if reload_markets and time.time() - getattr(self,'_markets_last_reloaded',0) > 60:
            logger2.error('{} - reloading markets due to KeyError'.format(self.name))
            asyncio.ensure_future(self.api._set_markets(limit=60))
            self._markets_last_reloaded = time.time()
    
    
    def on_ticker(self, r):
        """{
          "e": "24hrTicker",  // Event type
          "E": 123456789,     // Event time
          "s": "BNBBTC",      // Symbol
          "p": "0.0015",      // Price change
          "P": "250.00",      // Price change percent
          "w": "0.0018",      // Weighted average price
          "x": "0.0009",      // First trade(F)-1 price (first trade before the 24hr rolling window)
          "c": "0.0025",      // Last price
          "Q": "10",          // Last quantity
          "b": "0.0024",      // Best bid price
          "B": "10",          // Best bid quantity
          "a": "0.0026",      // Best ask price
          "A": "100",         // Best ask quantity
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
        }"""
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
        params = {p: r[m] for p,m in map.items()}
        params['symbol'] = self.convert_symbol(params['symbol'], 0)
        params['lastVolume'] = float(r['Q']) if isinstance(r['Q'],str) else None
        
        entry = self.api.ticker_entry(**params)
        self.update_tickers([entry])
        
    
    def on_orderbook_update(self, r):
        """{
          "e": "depthUpdate", // Event type
          "E": 123456789,     // Event time
          "s": "BNBBTC",      // Symbol
          "U": 157,           // First update ID in event
          "u": 160,           // Final update ID in event
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
        update = {
            'symbol': self.convert_symbol(r['s'], 0),
            'timestamp': r['E'],
            'bid': [[float(y) for y in x] for x in r['b']],
            'ask': [[float(y) for y in x] for x in r['a']],
            'nonce': (r['U'],r['u']),
        }
        self.orderbook_maintainer.send_update(update)
        
    
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
        updates = [(
                self.convert_cy(x['a'], 0),
                float(x['f']),
                float(x['l']),
            ) for x in r['B']
        ]
        self.update_balances(updates)
        
        
    def on_order(self, r):
        print(r)
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
        d = {
            'id': str(r['i']), #ccxt parses to str
            'symbol': self.convert_symbol(r['s'], 0),
            'side': r['S'].lower(),
            'rate': float(r['p']),
            'qty': float(r['q']),
            'ts': r['E'],
            'executed': float(r['z']),
        }
        d['left'] = d['qty'] - d['executed'] if r['X'] not in ('CANCELED','REJECTED','EXPIRED') else 0
        d['payout'] = d['executed'] if d['side'] == 'buy' else float(r['Z'])
        
        o = None
        try: o = self.get_order(d['id'])
        except ValueError: pass
        
        if o is None:
            #(self, id, symbol, side, rate, qty, ts, left=None, executed=None, payout=0)
            self.add_order(**d)
        else:
            d['left'] = min(d['left'],o['left'])
            self.update_order(d['id'], d['left'], d['executed'], d['payout'])
    
    
    def create_url(self, url_factory):
        params = url_factory.params
        channel = params['_']
        symbol = params['symbol']
        if isinstance(symbol, str):
            symbol = [symbol]
        map = {'ticker': '@ticker',
               'orderbook': '@depth',
               'market': '@trade'}
        suffix = map[channel]
        return '/'.join(['{}{}'.format(s,suffix) for s in symbol])
    
    
    async def fetch_listen_key(self):
        """{
            "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
        }"""
        async def fetch_key():
            #The method isn't asynchronous itself (it just returns coro)
            # thus it can't be used directly on call_via_loop
            return await self.api.publicPostUserDataStream()
        #url = 'https://api.binance.com/api/v1/userDataStream'
        #api.fetch2(self, path, api='public', method='GET', params={}, headers=None, body=None):
                
        r = await call_via_loop(fetch_key,
                                loop=self.loop,
                                module='asyncio')
        
        listen_key = r['listenKey']
        
        r2 = await call_via_loop(self.prolong_listen_key_expiry,
                                 args=(listen_key,),
                                 loop=self.loop, 
                                 cb_loop=asyncio.get_event_loop(),
                                 module='asyncio')
        
        self.start_listen_key_prolonging(listen_key)
        
        return listen_key
    
    
    async def prolong_listen_key_expiry(self, listen_key):
        async def prolong_key():
            return await self.api.publicPutUserDataStream({'listenKey': listen_key})
        
        logger.debug('{} - prolonging listen key {}'.format(self.name, listen_key))
        
        r = await call_via_loop(prolong_key,
                                loop=self.loop,
                                module='asyncio')
        
        #logger.debug('{} - listen key prolonging response: {}'.format(self.name, r))
        
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
            print('after _ | s is active: {}; s is active2: {}'.format(s.is_active(), s.is_active2()))
            while True:
                try: await s.wait_till_active(15)
                except asyncio.TimeoutError:
                    if not any(_s is s for _s in self.sh.subscriptions):
                        return
                else: break
            print('after active | s is active: {}; s is active2: {}'.format(s.is_active(), s.is_active2()))
            asyncio.ensure_future(ticker.loop())
            await inactive.wait()
            print('after inactive | s is active: {}; s is active2: {}'.format(s.is_active(), s.is_active2()))
            await ticker.close()
            
        call_via_loop(prolong_and_cancel_on_inactive,
                      loop=self.loop,
                      module='asyncio')
        
    
    def convert_symbol(self, symbol, direction=1):
        #0: ex to ccxt #1: ccxt to ex
        try: return super().convert_symbol(symbol, direction)
        except KeyError: pass
        
        if not direction:
            quotes = ['BTC','ETH','BNB','XRP','PAX',
                      'TUSD','USDT','USDC','USDS']
            ln = next(len(q) for q in quotes if symbol.endswith(q))
            return '/'.join([self.convert_cy(x,0) for x in (symbol[:-ln],symbol[-ln:])])
        else:
            return ''.join([self.convert_cy(x,1) for x in symbol.split('/')])

            
            