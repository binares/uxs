import datetime
dt = datetime.datetime
td = datetime.timedelta
import hashlib
from base64 import b64decode
from zlib import decompress, MAX_WBITS
import zlib
import uuid
import json
import copy

from uxs.base.socket import ExchangeSocket

#from fons.crypto import sign
from fons.time import ctime_ms
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

    
class bittrex(ExchangeSocket): 
    exchange = 'bittrex'
    url_components = {
        'ws': 'https://socket-v3.bittrex.com/signalr',
    }
    auth_defaults = {
        'takes_input': False,
        'each_time': False,
        'send_separately': True,
        'set_authenticated': True,
    }
    channel_defaults = {
        'unsub_option': True,
        'merge_option': True,
    }
    has = {
        'all_tickers': {
            'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False, 'last': True,
            'high': False, 'low': False, 'open': False, 'close': True, 'previousClose': False, 
            'change': False, 'percentage': False, 'average': False, 'vwap': False,
            'baseVolume': False, 'quoteVolume': False},
        'ticker': True,
        'orderbook': True,
        'ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'account': {'balance': True, 'order': True, 'fill': True},
        'fetch_tickers': True,
        'fetch_ticker': True,
        'fetch_ohlcv': True,
        'fetch_order_book': {'bids': True, 'asks': True, 'datetime': False, 'nonce': True, 'timestamp': False},
        'fetch_trades': True,
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'fetch_my_trades': {
            'symbolRequired': False,
            'amount': True, 'cost': True, 'datetime': True, 'fee': True, 'id': True, 'order': True,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        'fetch_order': {
            'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': True,
            'filled': True, 'id': True, 'lastTradeTimestamp': True, 'postOnly': True, 'price': True, 'remaining': True,
            'side': True, 'status': True, 'stopPrice': False, 'symbol': True, 'timeInForce': True, 'timestamp': True,
            'trades': False, 'type': True,
        },
        'fetch_open_orders': {'symbolRequired': False},
        'fetch_closed_orders': {'symbolRequired': False},
        'create_order': True,
    }
    has['ticker'] = has['all_tickers'].copy()
    has['fetch_tickers'] = has['all_tickers'].copy()
    has['fetch_ticker'] = has['all_tickers'].copy()
    has['fetch_ohlcv'] = has['ohlcv'].copy()
    has['fetch_trades'] = has['trades'].copy()
    has['fetch_open_orders'].update(has['fetch_order'])
    has['fetch_closed_orders'].update(has['fetch_order'])
    has['create_order'] = has['fetch_order'].copy()
    channels = {
        'ticker': {'fetch_data_on_sub': False},
    }
    channel_ids = {
        'account': ['balance','order','conditional_order','execution'], # 'deposit'
        'all_tickers': 'tickers',
        'ticker': 'ticker_<symbol>',
        'ohlcv': 'candle_<symbol>_<timeframe>',
        'orderbook': 'orderbook_<symbol>_<limit>',
        'trades': 'trade_<symbol>',
        'market_summaries': 'market_summaries',
        'market_summary': 'market_summary_<symbol>',
        'heartbeat': 'heartbeat',
    }
    connection_defaults = {
        'signalr': True,
        'hub_name': 'c3',
        'hub_methods': ['balance','order','conditionalOrder','execution','deposit',
                        'tickers','ticker','candle','orderBook','trade',
                        'marketSummaries','marketSummary','heartbeat',
                        'authenticationExpiring'],
        'rate_limit': (1, 0.12),
    }
    max_subscriptions_per_connection = 95
    subscription_push_rate_limit = 0.12
    ob = {
        'default_limit': 25,
        'limits': [1, 25, 500],
        'fetch_limits': [1, 25, 500],
    }
    order = {
        'cancel_automatically': 'if-not-subbed-to-account',
        'update_payout_on_fill': False,
    }
    symbol = {
        'quote_ids': ['BTC','ETH','USDT','USD','EUR'],
        'sep': '-',
        'startswith': 'base',
    }
    trade = {
        'sort_by': lambda x: (x['timestamp'], x['price'], x['amount']),
    }
    
    
    async def handle(self, R):
        data = R.data
        """:param data: json-decoded data directly from signalr socket"""
        if not len(data): pass
        
        # {'R': [{'Success': True, 'ErrorCode': None}], 'I': '1'}
        # {'R': {'Success': True, 'ErrorCode': None}, 'I': '3'}
        elif 'R' in data:
            if isinstance(data['R'], list) and not all(x.get('Success') for x in data['R']) \
                    or isinstance(data['R'], dict) and not data['R'].get('Success'):
                self.log_error(data)
        
        # The first response:
        # {'C': 'd-2684AECE-B,0|EX8i,0|EX8j,1', 'S': 1, 'M': []}
        # Afterwards 'S' is missing and 'M' has items in it
        # 'M': [
        #   {
        #     'H': 'C3',
        #     'M': 'ticker',
        #     'A': ['q1YqrsxNys9RslJyCnHWDQ12CVHSUcpJLC4JKUpMSQ1KLEkFShlamhuY6hlAAVBBUmYKkpSJnpmZsYWBgZE5UCqxOBshZWSgZwmUMzI1tFSqBQA=']
        #   }
        # ]
        elif 'M' in data and isinstance(data['M'], list):
            for item in data['M']:
                method = item['M']
                msg = self._decode_message(item['A'][0]) if item['A'] else None
                #print(msg)
                if method == 'tickers':
                    self.on_all_tickers(msg)
                elif method == 'ticker':
                    self.on_ticker(msg)
                elif method == 'orderBook':
                    self.on_orderbook_delta(msg)
                elif method == 'candle':
                    self.on_ohlcv(msg)
                elif method == 'trade':
                    self.on_trade(msg)
                elif method in ['order','conditionalOrder']:
                    self.on_order(msg)
                elif method == 'execution':
                    self.on_fill(msg)
                elif method == 'balance':
                    self.on_balance(msg)
                elif method == 'authenticationExpiring':
                    self.log('authentication expiring. re-authenticating')
                    await self.reauthenticate(R.id)
                else:
                    self._log('item with unknown method: {}'.format(item), 'ERROR')
                    
        # Is G the description of a subscription? Couldn't decode it with ._decode_message.
        # {'C': 'd-2684AECE-B,0|EX8i,0|EX8j,2|RC,1658',
        #  'G': 'p+R46GFZcolE3hEunHlD8xmF8C7t39BTGkNlY61rhStzhyyaY6lo/IM1AH1CkcMQGYa91Vi8vC8l7rRVUpNInhEJj/GWJwOMVeW3ATTmygplYsD1U4HjY753rGnxpj4WByPRVw==',
        #  'M': []}
        elif 'C' in data and 'G' in data and isinstance(data['G'], str):
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
    
    
    def on_all_tickers(self, r):
        """
        {
            'sequence': 417697,
            'deltas': [{'symbol': 'DEP-USDT', 'lastTradeRate': '0.00581000', 'bidRate': '0.00577000', 'askRate': '0.00587000'}, ...]
        }"""
        tickers = [self.parse_ticker(x) for x in r['deltas']]
        self.update_tickers(tickers, enable_sub='all_tickers')
    
    
    def on_ticker(self, r):
        """
        {
            'symbol': 'BTC-USDT',
            'lastTradeRate': '20642.38980007',
            'bidRate': '20642.38980009',
            'askRate': '20657.90199999'
        }
        """
        self.update_tickers([self.parse_ticker(r)], enable_sub=True)
    
    
    def parse_ticker(self, ticker):
        symbol = self.convert_symbol(self.safe_string(ticker, 'symbol'), 0)
        map = {
            'bid': 'bidRate',
            'ask': 'askRate',
            'last': 'lastTradeRate'
        }
        return self.ticker_entry(symbol=symbol, **self.lazy_parse(ticker, [], map), info=ticker)
    
    
    def on_orderbook_delta(self, r):
        """
         {
             'marketSymbol': 'BTC-USDT',
             'depth': 25,
             'sequence': 1051714,
             'bidDeltas': [
                 {
                     'quantity': '0.00208725',
                     'rate': '20801.80935212'
                },
                {    
                    'quantity': '0',
                    'rate': '20704.39030061'
                }
            ], 
            'askDeltas': []
        }
        """
        symbol = self.convert_symbol(r['marketSymbol'], 0)
        ob = dict(self.api.parse_order_book(r, None, 'bidDeltas', 'askDeltas', 'rate', 'quantity'),
                  symbol=symbol,
                  nonce=self.safe_integer(r, 'sequence'))
        self.orderbook_maintainer.send_update(ob)
    
    
    def on_ohlcv(self, r):
        """
        {
            'sequence': 27638,
            'marketSymbol': 'BTC-USDT',
            'interval': 'MINUTE_1',
            'delta': {
                'startsAt': '2020-12-16T18:47:00Z',
                'open': '20664.57359997',
                'high': '20664.57359997',
                'low': '20655.45000016',
                'close': '20655.45000041',
                'volume': '0.22724657',
                'quoteVolume': '4693.88456846'
            },
            'candleType': 'TRADE'
        }
        """
        symbol = self.convert_symbol(r['marketSymbol'], 0)
        timeframe = self.convert_timeframe(r['interval'], 0)
        ohlcv = self.parse_ohlcv(r['delta'])
        self.update_ohlcv(
            [{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': [ohlcv]}], enable_sub=True)
    
    
    def parse_ohlcv(self, ohlcv):
        return [
            self.parse8601(self.safe_string(ohlcv, 'startsAt')),
            self.safe_float(ohlcv, 'open'),
            self.safe_float(ohlcv, 'high'),
            self.safe_float(ohlcv, 'low'),
            self.safe_float(ohlcv, 'close'),
            self.safe_float(ohlcv, 'volume'),
        ]
    
    
    def on_trade(self, r):
        """
        {
            'deltas': [
                {
                    ...
                },
            ],
            'sequence': 28869,
            'marketSymbol': 'BTC-USDT'
        }
        """
        symbol = self.convert_symbol(r['marketSymbol'], 0)
        trades = [self.parse_trade(x, symbol) for x in r['deltas']]
        self.update_trades([{'symbol': symbol, 'trades': trades}], enable_sub=True)
    
    
    def parse_trade(self, trade, symbol=None):
        """
        {
            'id': 'dd72aa91-a78a-446e-9f2a-4d0545adce50',
            'marketSymbol': 'USDT-BTC',
            'executedAt': '2020-12-13T21:01:08.1Z',
            'quantity': '0.00050000',
            'rate': '22812.13799998',
            'orderId': '3d73c265-9868-4b1e-89bd-905ad79b7c1b',
            'commission': '0.02281213',
            'isTaker': True
        }
        """
        side = self.safe_string_lower(trade, 'takerSide')
        timestamp = self.parse8601(self.safe_string(trade, 'executedAt'))
        amount = self.safe_float(trade, 'quantity')
        price = self.safe_float(trade, 'rate')
        cost = None
        if amount is not None and price is not None:
            cost = amount * price
        return self.trade_entry(
            id=self.safe_string(trade, 'id'),
            symbol=symbol,
            timestamp=timestamp,
            side=side,
            amount=amount,
            price=price,
            cost=cost,
            info=trade,
        )
    
    
    def on_order(self, r):
        """
        {
            'accountId': 'b753c7c7-021a-493d-80cf-b1311019d890',
            'sequence': 18,
            'delta': {...},
        }"""
        # Note: when conditional order is placed, the balances aren't being reserved until it is triggered.
        # Nor does the socket send the order until triggered (and then they will be displayed as LIMIT_ORDER).
        order = self.parse_order(r['delta'])
        self.add_order_from_dict(order, enable_sub=True)
    
    
    def parse_order(self, order):
        """
        CONDITIONAL (TAKE-PROFIT)
        {
            'id': '0c8560c1-51ba-4a74-986f-5ff297c4c1fc',
            'marketSymbol': 'BTC-USDT',
            'operand': 'GTE',
            'triggerPrice': '22500.00000000',
            'orderToCreate': {
                'marketSymbol': 'BTC-USDT',
                'direction': 'SELL',
                'type': 'LIMIT',
                'quantity': '0.00087310',
                'limit': '22000.00000000',
                'timeInForce': 'GOOD_TIL_CANCELLED'
                // OTHER FIELDS
                "ceiling": "number (double)",
                "clientOrderId": "string (uuid)",
                "useAwards": "boolean"
            }
            // OTHER FIELDS
            "trailingStopPercent": "number (double)",
            "createdOrderId": "string (uuid)",         // unique ID of the order that was created by this conditional, if there is one
            "orderToCancel": {                         // order or conditional order to cancel if this conditional order triggers Note that this relationship is reciprocal.
              "type": "string",
              "id": "string (uuid)"
            },
            "clientConditionalOrderId": "string (uuid)",
            "status": "string",                        //  OPEN, COMPLETED, CANCELLED, FAILED
            "orderCreationErrorCode": "string",
            "createdAt": "string (date-time)",
            "updatedAt": "string (date-time)",
            "closedAt": "string (date-time)"
        }
        LIMIT
        {
            'id': '8bbb3e5a-6b97-4e3f-bd99-3eaaa6d15edd',
            'marketSymbol': 'BTC-USDT',
            'direction': 'BUY',                        // BUY, SELL
            'type': 'LIMIT',
            'quantity': '0.00050000',
            'limit': '18500.00000000',
            'timeInForce': 'GOOD_TIL_CANCELLED',
            'fillQuantity': '0.00000000',
            'commission': '0.00000000',
            'proceeds': '0.00000000',
            'status': 'OPEN',                         // OPEN, CLOSED
            'createdAt': '2020-12-13T20:11:15.74Z',
            'updatedAt': '2020-12-13T20:11:15.74Z'
            //
            "clientOrderId": "string (uuid)",         // client-provided id or id of the conditional order that created this order
        }
        }"""
        symbol = self.convert_symbol(order['marketSymbol'], 0)
        id = self.safe_string(order, 'id')
        clientOrderId = self.safe_string_2(order, 'clientOrderId', 'clientConditionalOrderId')
        stop = self.safe_float(order, 'triggerPrice')
        info = order
        if 'orderToCreate' in order:
            order = order['orderToCreate']
        type = self.api._parse_order_type(self.safe_string(order, 'type'))
        side = self.safe_string_lower(order, 'direction')
        amount = self.safe_float(order, 'quantity')
        filled = self.safe_float(order, 'fillQuantity')
        price = self.safe_float(order, 'limit')
        timestamp = self.parse8601(self.safe_string(order, 'createdAt'))
        remaining = None
        if amount is not None and filled is not None:
            remaining = amount - filled
        quote = symbol.split('/')[1]
        fee_cost = self.safe_float(order, 'commission')
        proceeds = self.safe_float(order, 'proceeds')
        fee = None
        if fee_cost is not None:
            fee = {
                'cost': fee_cost,
                'currency': quote,
            }
        payout = None
        average = None
        if proceeds is not None:
            if fee_cost is not None:
                payout = filled if side == 'buy' else proceeds - fee_cost
            if filled is not None and filled > 0:
                average = proceeds / filled
        status = self.safe_string_lower(order, 'status')
        if status is not None and status!='open':
            remaining = 0.0
        return {
            'id': id,
            'symbol': symbol,
            'type': type,
            'side': side,
            'price': price,
            'stop': stop,
            'amount': amount,
            'timestamp': timestamp,
            'remaining': remaining,
            'filled': filled,
            'cost': proceeds,
            'payout': payout,
            'fee': fee,
            'average': average,
            'clientOrderId': clientOrderId,
            'info': info,
        }
    
    
    def on_fill(self, r):
        """
        {
            'accountId': 'b753c7c7-021a-493d-80cf-b1311019d890',
            'sequence': 0,
            'deltas': [
                {
                    ...
                }
            ]
        }
        """
        for fill in r['deltas']:
            parsed = self.parse_fill(fill)
            self.add_fill_from_dict(parsed, enable_sub=True)
    
    
    def parse_fill(self, fill):
        """
        {
            'id': 'dd72aa91-a78a-446e-9f2a-4d0545adce50',
            'marketSymbol': 'USDT-BTC',                        // This is backwards
            'executedAt': '2020-12-13T21:01:08.1Z',
            'quantity': '0.00050000',
            'rate': '20912.1548259',
            'orderId': '3d73c265-9868-4b1e-89bd-905ad79b7c1b',
            'commission': '0.02091215',
            'isTaker': True
        }
        """
        map = {
            'symbol': 'marketSymbol',
            'timestamp': 'executedAt',
            'amount': 'quantity',
            'price': 'rate',
            'order': 'orderId',
            'cost': 'commission',
            'takerOrMaker': 'isTaker',
        }
        apply = {
            'symbol': lambda x: self.convert_symbol('-'.join(x.split('-')[::-1]), 0),
            'timestamp': self.parse8601,
            'takerOrMaker': lambda x: None if x is None else ['maker','taker'][x],
        }
        return self.trade_entry(**self.lazy_parse(fill, ['id'], map, apply), info=fill)
    
    
    def on_balance(self, r):
        """
        {
            'accountId': 'b753c7c7-021a-493d-80cf-b1311019d890',
            'sequence': 25, 
            'delta': {
                'currencySymbol': 'USDT',
                'total': '67.42586295',
                'available': '22.68543127',
                'updatedAt': '2020-12-13T19:41:02.11Z'
            }
        }"""
        d = r['delta']
        cy = self.convert_cy(d['currencySymbol'], 0)
        free = float(d['available'])
        total = float(d['total'])
        self.update_balances([{'cy': cy, 'free': free, 'total': total, 'info': d}], enable_sub=True)
    
    
    def encode(self, rq, sub=None):
        channel = rq.channel
        p = rq.params
        symbol = p.get('symbol')
        timeframe = p.get('timeframe')
        limit = p.get('limit')
        
        method = 'Subscribe' if sub else 'UnSubscribe'
        args = self.fill_in_params(self.channel_ids[channel], symbol, timeframe)
        
        if channel == 'orderbook':
            if limit is None:
                limit = self.ob['default_limit']
            elif limit > max(self.ob['limits']):
                limit = max(self.ob['limits'])
            else:
                limit = self.ob_maintainer.resolve_limit(limit)
            _symbols = [symbol] if isinstance(symbol, str) else symbol
            for symbol in _symbols:
                self.ob_maintainer.set_limit(symbol, limit)
            args = self.fill_in(args, '<limit>', limit)
        
        data = [method, args]
        
        return (data, None)
    
    
    def sign(self, out=[]):
        timestamp = str(self.api.milliseconds())
        random_content = str(uuid.uuid4())
        content = timestamp + random_content
        #signed_content = sign(self.secret, content, 'sha512')
        signed_content = self.api.hmac(self.api.encode(content), self.api.encode(self.secret), hashlib.sha512)
        out = ['Authenticate', self.apiKey, timestamp, random_content, signed_content]
        return out
    
    
    async def reauthenticate(self, conn_id):
        cnx = self.cm.connections[conn_id]
        await cnx.send(self.sign())
