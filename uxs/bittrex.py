import datetime
dt = datetime.datetime
td = datetime.timedelta
from base64 import b64decode
from zlib import decompress, MAX_WBITS
import zlib
import json
import copy

from uxs.base.socket import ExchangeSocket

from fons.crypto import sign
from fons.time import ctime_ms
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

    
class bittrex(ExchangeSocket): 
    exchange = 'bittrex'
    url_components = {
        'ws': 'https://socket.bittrex.com/signalr',
    }
    auth_defaults = {
        'takes_input': True,
        'each_time': False,
        'send_separately': False,
        'set_authenticated': False,
    }
    channel_defaults = {
        'unsub_option': False,
        'merge_option': False,
    }
    channels = {
        'account': {
            'required': [],
            'is_private': True,
        },
        'Authenticate': {
            'required': ['challenge'],
            'is_private': True,
            'auth': {'set_authenticated': True},
        },
        'fetch_order_book': {
            'required': ['symbol'],
        },
        'fetch_tickers': {
            'required': [],
        },
    }
    has = {
        'all_tickers': {
            'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False, 'last': True,
            'high': True, 'low': True, 'open': False, 'close': True, 'previousClose': False, 
            'change': False, 'percentage': False, 'average': False, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True},
        'ticker': False,
        'orderbook': True,
        'account': {'balance': True, 'order': True, 'fill': False},
        'fetch_tickers': {'ws': True},
        'fetch_ticker': True,
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'ws': True},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'fetch_my_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': True, 'id': True, 'order': True,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        'fetch_order': {
            'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': True,
            'filled': True, 'id': True, 'lastTradeTimestamp': True, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': False, 'type': True},
        'fetch_open_orders': {'symbolRequired': False},
        'fetch_closed_orders': {'symbolRequired': False},
        'create_order': {'id': True, 'side': True, 'status': True, 'symbol': True, 'type': True},
    }
    has['fetch_tickers'].update(has['all_tickers'])
    has['fetch_ticker'] = has['all_tickers'].copy()
    has['fetch_open_orders'].update(has['fetch_order'])
    has['fetch_closed_orders'].update(has['fetch_order'])
    connection_defaults = {
        'signalr': True,
        'hub_name': 'c2',
        'hub_methods': ['uE','uS','uL','uB','uO'],
        'rate_limit': (1, 0.12),
        #'recv_timeout': 10,
    }
    max_subscriptions_per_connection = 95
    subscription_push_rate_limit = 0.12
    order = {
        'cancel_automatically': 'if-not-subbed-to-account',
        'update_payout_on_fill': False,
    }
    symbol = {
        'quote_ids': ['BTC','ETH','USDT','USD'],
        'sep': '-',
        'startswith': 'quote',
    }
    
    
    def handle(self, response):
        data = response.data
        """:param data: json-decoded data directly from signalr socket"""
        #print('received: {}'.format(data))
        if not len(data): pass
        
        elif 'R' in data and type(data['R']) is not bool:
            # data['I'] seems to be always '0'
            try: decoded_msg = self._decode_message(data['R'])
            except zlib.error:
                self.log('authentication procedure invoke')
                challenge = data['R']
                params = {'_': 'Authenticate', 'challenge': challenge}
                self.send(params)
            else:
                if isinstance(decoded_msg,dict) and 'M' in decoded_msg:
                    self.on_query_exchange_state(decoded_msg)
                elif isinstance(decoded_msg,dict) and 's' in decoded_msg:
                    self.on_query_summary_state(decoded_msg)
                else:
                    self.notify_unknown(dict(data, R=decoded_msg))
        
        # The first response:
        # {'C': 'd-B1762C83-w,0|q80T,0|q80U,1', 'S': 1, 'M': []}
        # Afterwards 'S' is missing and 'M' has items in it
        elif 'M' in data and isinstance(data['M'], list):
            for item in data['M']:
                method = item['M']
                msg = self._decode_message(item['A'][0])
                if method == 'uE':
                    self.on_exchange_delta(msg)
                elif method == 'uS':
                    self.on_summary_delta(msg)
                elif method == 'uO':
                    self.on_order(msg)
                elif method == 'uB':
                    self.on_balance(msg)
                elif method == 'uL':
                    pass
                else:
                    self._log('item with unknown method: {}'.format(item), 'ERROR')
                    
        # Is G the description of a subscription? Couldn't decode it with ._decode_message.
        # {'C': 'd-B1762C83-w,0|q80T,0|q80U,2|Bh,123445', 'G': 'a_long_non_decodable_str', 'M': []}
        elif 'C' in data and 'G' in data and isinstance(data['G'], str):
            pass
        
        # A subscription was activated?
        # {'R': True, 'I': '1'}
        elif 'R' in data:
            pass
        
        # {'I': '0', 'E': "There was an error invoking Hub method 'c2.GetAuthContext'."}
        elif 'E' in data:
            self.log_error(data['E'])
        
        else:
            self.notify_unknown(data)
        
        
    @staticmethod
    def _decode_message(message):
        try:
            deflated_msg = decompress(b64decode(message, validate=True), -MAX_WBITS)
        except SyntaxError:
            deflated_msg = decompress(b64decode(message, validate=True))
        return json.loads(deflated_msg.decode())
    
    
    def on_query_exchange_state(self, r):
        """{
            'M': 'BTC-NEO',
            'N': 597609,
            'Z': [{'Q': 23.54562619, 'R': 0.00155089}],
            'S': [{'Q': 227.50512533, 'R': 0.00155742}],
            'f': [{
                   'I': 25593666,
                   'T': 1560696043750,
                   'Q': 20.0,
                   'P': 0.00153292,
                   't': 0.0306584,
                   'F': 'FILL',
                   'OT': 'SELL',
                   'U': 'b0482d8d-3e98-4b72-a0dd-eef262ad053c'
                }] #100 last fills
        }"""
        #print('on_query_exchange_state r: {}'.format(r))
        ts = ctime_ms()
        d = {
            'symbol': self.convert_symbol(r['M'],0),
            'bids': [[x['R'],x['Q']] for x in r['Z']],
            'asks': [[x['R'],x['Q']] for x in r['S']],
            'timestamp': ts,
            'datetime': self.api.iso8601(ts),
            'nonce': r['N'],
        }
        #self.create_orderbooks([d])
        id = 'fetch_order_book+{}'.format(d['symbol'])
        #print('id: {}, has_waiter: {}'.format(id,self.has_waiter(id)))
        if self.tp.has_waiter(id):
            self.tp.forward_to_waiters(id, d, copy=1)
        else:
            # Send only when nonce is larger than previous?
            self.orderbook_maintainer.send_orderbook(d)
    
    
    def on_exchange_delta(self, e):
        #TY: 0 = ADD, 1 = REMOVE, 2 = UPDATE
        """{   
            'M': 'BTC-ETH',
            'N': 671830, 
            'Z': [{'TY': 2, 'R': 0.0287, 'Q': 0.92360582}],
            'S': [{'TY': 1, 'R': 0.02969634, 'Q': 0.0}, {'TY': 0, 'R': 0.02971151, 'Q': 13.47412842}],
            'f': [] #fills (see .on_query_exchange_state)
         }"""
        d = {
            'symbol': self.convert_symbol(e['M'],0),
            'bids': [],
            'asks': [],
            'nonce': e['N'],
        }    
        for name,key in zip(['bids','asks'],['Z','S']):
            try: branch_changes = e[key]
            except KeyError: continue
            d[name] = [[x['R'],x['Q']] for x in branch_changes]
        
        #self.update_orderbooks([d])
        self.orderbook_maintainer.send_update(d)
    
    
    async def fetch_order_book(self, symbol, limit=None, params={}):
        if self.is_active():
            id = 'fetch_order_book+{}'.format(symbol)
            return await self.send({'_': 'fetch_order_book', 'symbol': symbol}, True, id=id)
        else:
            return await super().fetch_order_book(symbol, limit, params)
    
    
    def on_query_summary_state(self, r):
        """{
            'N': 52259, 
            's': [{
                    'M': 'BTC-LOOM',
                    'H': 9.16e-06,
                    'L': 8.52e-06, 
                    'V': 908237.64060258, 
                    'l': 8.78e-06, 
                    'm': 8.00417016, 
                    'T': 1560708049863, 
                    'B': 8.77e-06, 
                    'A': 8.8e-06, 
                    'G': 166, 
                    'g': 584, 
                    'PD': 9.08e-06, 
                    'x': 1531853661523}]}"""
        #print('on_query_summary_state r: {}'.format(r))
        r2 = r.copy()
        r2['D'] = r2.pop('s')
        self.on_summary_delta(r2, activate=False)
        id = 'fetch_tickers'
        #print('id: {}, has_waiter: {}'.format(id,self.has_waiter(id)))
        if self.tp.has_waiter(id):
            self.tp.forward_to_waiters(id, copy.deepcopy(self.tickers))
        
        
    def on_summary_delta(self, e, activate=True):
        """{
            'N': 48394, #Nonce
            'D': [{
                    'M': 'USDT-BTT', #MarketName
                    'H': 0.00134847, #High
                    'L': 0.0012643, #Low
                    'V': 164893472.74109152, #baseVolume
                    'l': 0.00133984, #Last
                    'm': 210474.16031964, #quoteVolume
                    'T': 1560688717863, #Time
                    'B': 0.00133425, #Bid
                    'A': 0.00133977, #Ask
                    'G': 110, #OpenBuyOrders
                    'g': 181, #OpenSellOrders
                    'PD': 0.00127012, #PrevDay
                    'x': 1549664941827 #Created
                },
            ]
         }"""
        for x in e['D']:
            self.update_tickers([{
                'symbol': self.convert_symbol(x['M'],0),
                'timestamp': x['T'],
                'datetime': self.api.iso8601(x['T']),
                'high':x['H'],
                'low': x['L'],
                'bid': x['B'],
                'bidVolume': None,
                'ask': x['A'],
                'askVolume': None,
                'vwap': x['m'] / x['V'] if None not in (x['m'],x['V']) and x['V'] > 0 else None,
                'open': None,
                'close': x['l'],
                'last': x['l'], 
                'previousClose': None,
                'change': None, 
                'percentage': None, 
                'average': None,
                'baseVolume': x['V'],
                'quoteVolume': x['m'],
                'info': x,
                }]
            )
        if activate:
            self.change_subscription_state({'_': 'all_tickers'}, 1)
            
    
    async def fetch_tickers(self, symbols=None, params={}):
        if self.is_active():
            return await self.send({'_': 'fetch_tickers'}, True, id='fetch_tickers')
        else:
            return await super().fetch_tickers(symbols, params)
            
            
    def on_order(self, e):
        tlogger.debug(e)
        """{
            'w': '123456ab-7890-cd12-345e-f1234567890a', #AccountUuid
            'N': 2, #Nonce
            'TY': 0, #Type
            'o': {
                'U': '123456ab-7890-cd12-345e-f1234567890a', #Uuid (also account?)
                'I': 1234567890, #Id
                'OU': 'abcdef12-3456-7890-abcd-ef1234567890', #OrderUuid
                'E': 'BTC-TUSD', #Exchange
                'OT': 'LIMIT_SELL', #OrderType
                'Q': 30.0, #Quantity
                'q': 30.0, #QuantityRemaining
                'X': 0.000147, #Limit
                'n': 0.0, #CommissionPaid (precision: 8, in quote)
                'P': 0.0, #Price (precision: 8)
                'PU': 0.0, #PricePerUnit
                'Y': 1553847954906, #Opened (date)
                'C': None, #Closed (date)
                'i': True, #IsOpen
                'CI': False, #CancelInitiated
                'K': False, #ImmediateOrCancel
                'k': False, #IsConditional
                'J': None, #Condition (string)
                'j': None, #ConditionTarget
                'u': 1553847954906, #Updated (ms)
                'PassthroughUuid': None,
            }
        }"""
        #Note: when conditional order is placed, the balances aren't being reserved until it is triggered.
        # Nor does the socket send the order until triggered (and then they will be displayed as LIMIT_ORDER).
        o = e['o']
        symbol = self.convert_symbol(o['E'],0)
        id = o['OU']
        side = 'buy' if 'buy' in o['OT'].lower() else 'sell'
        amount = o['Q']
        remaining = o['q']
        price = o['X']
        timestamp = o['Y']
        filled = amount-remaining
        #precision = self.api.markets[symbol]['precision']['amount']
        payout = (filled if side == 'buy' else 
                  #(round down is precise operation, bittrex server doesn't round up)
                  #fons.math.round.round(self.api.baseToQuote(filled,o['PU']) - o['n'], 8, 'down'))
                  o['P'] - o['n'])
                
                  
        is_open = o['i']
        if not is_open:
            remaining = 0.0
        #print('on_order:',id,symbol,side,price,amount,timestamp,remaining,filled,payout)
        # If already exists then it will update it
        self.add_order(id=id, symbol=symbol, side=side, price=price, amount=amount,
                       timestamp=timestamp, remaining=remaining, filled=filled,
                       payout=payout, params={'info': o}, enable_sub=True)
        
        
    def on_balance(self, e):
        """{
            'N': 1, #Nonce
            'd': {
                'U': '123456ab-7890-cd12-345e-f1234567890a', #Uuid
                'W': 123456, #AccountId
                'c': 'TUSD', #Currency
                'b': 46.12345678, #Balance
                'a': 16.12345678, #Available
                'z': 0.0, #Pending
                'p': '0x1234567890abcdef1234567890abcdef12345678', #CryptoAddress
                'r': False, #Requested
                'u': 1553847954906, #Updated (ms)
                'h': None, #Autosell
            }
        }"""
        d = e['d']
        cy = self.convert_cy(d['c'],0)
        free = d['a']
        used = d['b']- free
        self.update_balances([{'cy': cy, 'free': free, 'used': used, 'info': d}], enable_sub=True)
    
    
    def encode(self, rq, sub=None):
        channel = rq.channel
        data = []
        
        if channel in ('account', 'own_market'):
            method = 'GetAuthContext'
        elif channel in ('orderbook','fetch_order_book'): #'market'?
            #retrieve full ob: 'QueryExchangeState'
            #deltas:
            method = {'orderbook': 'SubscribeToExchangeDeltas',
                      'fetch_order_book': 'queryExchangeState'}[channel]
            symbol = self.convert_symbol(rq.params['symbol'], 1)
            data.append(symbol)
        elif channel in ('all_tickers', 'fetch_tickers'):
            method = {'all_tickers': 'SubscribeToSummaryDeltas',
                      'fetch_tickers': 'querySummaryState'}[channel]
        else:
            #'Authenticate'
            method = channel
            data += [y for x,y in rq.params.items() if x!='_']
        
        return ([method] + data, None)


    def sign(self, out=[]):
        out = out[:]
        if not len(out): pass
        elif out[0] == 'GetAuthContext':
            out += [self.apiKey]
        elif out[0] == 'Authenticate':
            sig = sign(self.secret, out[1], 'sha512')
            out = ['Authenticate', self.apiKey, sig]
        return out
