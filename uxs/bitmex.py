import os
from dateutil.parser import parse as parsedate
from collections import defaultdict
import functools
import urllib
import json
import asyncio
import time
import datetime
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket, ExchangeSocketError
from uxs.fintls.utils import resolve_times

from fons.time import timestamp_ms, pydt_from_ms, ctime_ms
from fons.crypto import nonce_ms, sign
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class bitmex(ExchangeSocket): 
    exchange = 'bitmex'
    url_components = {
        'ws': 'wss://www.bitmex.com/realtime',
        'test': 'wss://testnet.bitmex.com/realtime',
    }
    auth_defaults = {
        'takes_input': False,
        'each_time': False,
        'send_separately': True,
    }
    channel_defaults = {
        'unsub_option': True,
    }
    channels = {
        'account': {
            'auth': {'apply_to_packs': [0]},
        },
        'own_market': {
            'auth': {'apply_to_packs': [0]},
            'merge_option': True,
        },
        'orderbook': {
            'merge_option': True,
        }
    }
    has = {
        'ticker': False,
        'all_tickers': {
            'last': True, 'bid': True, 'ask': True, 'bidVolume': False, 'askVolume': False,
            'high': True, 'low': True, 'open': False, 'close': True, 'previousClose': False,
            'change': True, 'percentage': True, 'average': False, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True, 'active': True},
        'orderbook': True,
        'trades': False, # TODO
        'account': {'balance': True, 'position': True},
        'own_market': {'order': True, 'fill': True},
        'fetch_tickers': True,
        'fetch_ticker': {
            'ask': True, 'askVolume': False, 'average': True, 'baseVolume': True, 'bid': True, 'bidVolume': False,
            'change': True, 'close': True, 'datetime': True, 'high': True, 'last': True, 'low': True, 'open': True,
            'percentage': True, 'previousClose': False, 'quoteVolume': True, 'symbol': True, 'timestamp': True,
            'vwap': True},
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'asks': True, 'bids': True, 'datetime': False, 'nonce': False, 'timestamp': False},
        'fetch_trades': {
            'amount': True, 'cost': False, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'fetch_my_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': True, 'id': True, 'order': True,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': True, 'timestamp': True, 'type': True},
        'fetch_order': {
            'amount': True, 'average': True, 'clientOrderId': True, 'cost': True, 'datetime': True, 'fee': False,
            'filled': True, 'id': True, 'lastTradeTimestamp': True, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': False, 'type': True},
        'fetch_orders': {'symbolRequired': False},
        'fetch_open_orders': {'symbolRequired': False},
        'fetch_closed_orders': {'symbolRequired': False},
        'create_order': True,
        'edit_order': True,
    }
    has['fetch_tickers'] = has['fetch_ticker'].copy()
    has['fetch_orders'].update(has['fetch_order'])
    has['fetch_open_orders'].update(has['fetch_order'])
    has['fetch_closed_orders'].update(has['fetch_order'])
    has['create_order'] = has['fetch_order'].copy()
    has['edit_order'] = has['fetch_order'].copy()
    connection_defaults = {
        'max_subscriptions': 13, #the usual is 20, but account stream has many substreams
        #'subscription_push_rate_limit': 0.12,
        'rate_limit': (1, 2),
        #'ping': 'm$ping',
        'ping_interval': 0.5,
        #'ping_as_message': True,
        'ping_after': 5,
        'ping_timeout': 5,
        #'recv_timeout': 10,
    }
    ob = {
        'force_create': None,
        'cache_size': 1000,
        'limits': [10, 25],
        'uses_nonce': False,
        'receives_snapshot': True,
        'hold_times': {
            'orderBookL2_25': 0.05,
            #this is the lowest "exact" asyncio.sleep time on Windows
            'orderBookL2': 0.016,
        },
    }
    order = {
        'update_filled_on_fill': False,
        'update_payout_on_fill': False,
        'update_remaining_on_fill': False,
    }
    channel_ids = {
        # This can be overriden to include 'transact' and 'wallet' in the subscription
        'account': ['margin','position'], #'transact','wallet'
        'own_market': ['execution:<symbol>','order:<symbol>'],
        #'ticker': ['instrument:{symbol}'],
        'all_tickers': ['instrument'],
        'orderbook': ['orderBookL2:<symbol>'],
        'trades': ['trades:<symbol>'],
    }
    symbol = {
        'quote_ids': ['USD','BTC','ETH','ADA','BCH','XRP','TRX'],
        'sep': '',
    }
    _id_prices = defaultdict(lambda: {'start': None,
                                      'multiplier': None,
                                      'per_tick': None,
                                      'cache': []})
    
    __extend_attrs__ = []
    __deepcopy_on_init__ = ['_id_prices']
    
    
    def handle(self, response):
        message = response.data
        
        #logger.debug(json.dumps(message))
        if not isinstance(message, dict):
            print(message)
            return

        table = message['table'] if 'table' in message else None
        action = message['action'] if 'action' in message else None

        if 'subscribe' in message:
            if message['success']:
                split = message['subscribe'].split(':')
                _channel, _symbol = (split[0], None) if len(split)<2 else split
                if _channel in ('position', 'margin', 'execution', 'order'):
                    async def change_state():
                        if _channel in ('position', 'margin'):
                            s = {'_': 'account'}
                        else:
                            s = {'_': 'own_market', 'symbol': self.convert_symbol(_symbol, 0)}
                        await asyncio.sleep(1)
                        if self.is_subscribed_to(s):
                            self.change_subscription_state(s, 1, True)
                    asyncio.ensure_future(change_state())
                logger.debug("Subscribed to %s." % message['subscribe'])
            else:
                self.error("Unable to subscribe to %s. Error: \"%s\" Please check and restart." %
                           (message['request']['args'][0], message['error']))
        elif 'status' in message:
            if message['status'] == 400:
                self.error(message['error'])
            if message['status'] == 401:
                self.error("API Key incorrect, please check and restart.")
        elif action:
            if table == 'order':
                self.on_order(message)
            elif table == 'position':
                self.on_position(message)
            elif table == 'margin':
                self.on_margin(message)
            elif table == 'execution':
                self.on_fill(message)
            elif table == 'instrument':
                self.on_ticker(message)
            elif table in ('orderBookL2', 'orderBookL2_25'):
                self.on_orderbookL2(message)
            elif table == 'orderBook10':
                self.on_orderbook10(message)
            elif table == 'transact':
                self.on_transact(message)
            elif table == 'wallet':
                self.on_wallet(message)
            else:
                self.notify_unknown(message)
    
    
    def error(self, r):
        logger2.error('{} - {}'.format(self.name, r))
    
    
    def on_ticker(self, message):
        """{
        'table': 'instrument',
        'action': 'partial',
        'data': [{'symbol': 'XBTUSD', 'rootSymbol': 'XBT', 'state': 'Open', 'typ': 'FFWCSX', 'listing': '2016-05-13T12:00:00.000Z',
            'front': '2016-05-13T12:00:00.000Z', 'expiry': None, 'settle': None, 'relistInterval': None, 'inverseLeg': '',
            'sellLeg': '', 'buyLeg': '', 'optionStrikePcnt': None, 'optionStrikeRound': None, 'optionStrikePrice': None,
            'optionMultiplier': None, 'positionCurrency': 'USD', 'underlying': 'XBT', 'quoteCurrency': 'USD', 'underlyingSymbol': 'XBT=',
            'reference': 'BMEX', 'referenceSymbol': '.BXBT', 'calcInterval': None, 'publishInterval': None, 'publishTime': None,
            'maxOrderQty': 10000000, 'maxPrice': 1000000, 'lotSize': 1, 'tickSize': 0.5, 'multiplier': -100000000, 'settlCurrency': 'XBt',
            'underlyingToPositionMultiplier': None, 'underlyingToSettleMultiplier': -100000000, 'quoteToSettleMultiplier': None,
            'isQuanto': False, 'isInverse': True, 'initMargin': 0.01, 'maintMargin': 0.005, 'riskLimit': 20000000000,
            'riskStep': 10000000000, 'limit': None, 'capped': False, 'taxed': True, 'deleverage': True, 'makerFee': -0.00025,
            'takerFee': 0.00075, 'settlementFee': 0, 'insuranceFee': 0, 'fundingBaseSymbol': '.XBTBON8H', 'fundingQuoteSymbol': '.USDBON8H',
            'fundingPremiumSymbol': '.XBTUSDPI8H', 'fundingTimestamp': '2019-12-10T20:00:00.000Z', 'fundingInterval': '2000-01-01T08:00:00.000Z',
            'fundingRate': 0.0001, 'indicativeFundingRate': 0.0001, 'rebalanceTimestamp': None, 'rebalanceInterval': None,
            'openingTimestamp': '2019-12-10T12:00:00.000Z', 'closingTimestamp': '2019-12-10T13:00:00.000Z', 'sessionInterval': '2000-01-01T01:00:00.000Z',
            'prevClosePrice': 7337.66, 'limitDownPrice': None, 'limitUpPrice': None, 'bankruptLimitDownPrice': None, 'bankruptLimitUpPrice': None,
            'prevTotalVolume': 1879923977204, 'totalVolume': 1879972836587, 'volume': 48859383, 'volume24h': 2165719847,
            'prevTotalTurnover': 26472620585943624, 'totalTurnover': 26473287350302396, 'turnover': 666764358771, 'turnover24h': 29253457923532,
            'homeNotional24h': 292534.579235321, 'foreignNotional24h': 2165719847, 'prevPrice24h': 7487.5, 'vwap': 7403.5685, 'highPrice': 7689,
            'lowPrice': 7270, 'lastPrice': 7337, 'lastPriceProtected': 7337, 'lastTickDirection': 'MinusTick', 'lastChangePcnt': -0.0201,
            'bidPrice': 7337, 'midPrice': 7337.25, 'askPrice': 7337.5, 'impactBidPrice': 7336.7572, 'impactMidPrice': 7337.25, 'impactAskPrice': 7337.5,
            'hasLiquidity': True, 'openInterest': 691746804, 'openValue': 9425050204500, 'fairMethod': 'FundingRate', 'fairBasisRate': 0.1095,
            'fairBasis': 0.69, 'fairPrice': 7339.33, 'markMethod': 'FairPrice', 'markPrice': 7339.33, 'indicativeTaxRate': 0,
            'indicativeSettlePrice': 7338.64, 'optionUnderlyingPrice': None, 'settledPrice': None, 'timestamp': '2019-12-10T12:30:16.440Z'}, ...]}"""
        action = message['action']
        
        apply = {
            'symbol': functools.partial(self.convert_symbol, direction=0)}
        
        map = {
               'symbol': 'symbol',
               'vwap': 'vwap',
               'datetime': 'timestamp',
               'bid': 'bidPrice',
               'ask': 'askPrice',
               'last': 'lastPrice',
               'open': 'prevPrice24h',
               'high': 'highPrice',
               'low': 'lowPrice',
               #'close': 'lastPrice',
               'baseVolume': 'homeNotional24h',
               'quoteVolume': 'foreignNotional24h',
               'fairPrice': 'fairPrice',
               'markPrice': 'markPrice',
               'markMethod': 'markMethod',
               'referenceSymbol': 'referenceSymbol',
               'fundingTimestamp': 'fundingTimestamp',
               'fundingInterval': 'fundingInterval'}
        
        _null = lambda x: x
        
        def parse(x):
            new = {}
            for param,key in map.items():
                f = apply.get(param, _null)
                if key in x:
                    new[param] = f(x[key])
            new['info'] = x
            entry = self.api.ticker_entry(**new)
            if action != 'partial':
                #to avoid overwriting old entries with None
                entry = {p:v for p,v in entry.items() if v is not None or map.get(p) in x}
            return new
                  
        data = []  
        
        for x in message['data']:
            data.append(parse(x))
        
        _action = 'update' if action != 'partial' else 'replace'
        
        self.update_tickers(data, action=_action, enable_sub='all_tickers')
        
            
    def on_orderbookL2(self, message):
        """
        > {"op": "subscribe", "args": ["orderBookL2_25:XBTUSD"]}
        {"success":true,"subscribe":"orderBookL2_25:XBTUSD","request":{"op":"subscribe","args":["orderBookL2_25:XBTUSD"]}}
        {
            "table":"orderBookL2_25",
            "keys":["symbol","id","side"],
            "types":{"id":"long","price":"float","side":"symbol","size":"long","symbol":"symbol"}
            "foreignKeys":{"side":"side","symbol":"instrument"},
            "attributes":{"id":"sorted","symbol":"grouped"},
            "action":"partial",
            "data":[
              {"symbol":"XBTUSD","id":17999992000,"side":"Sell","size":100,"price":80},
              {"symbol":"XBTUSD","id":17999993000,"side":"Sell","size":20,"price":70},
              {"symbol":"XBTUSD","id":17999994000,"side":"Sell","size":10,"price":60},
              {"symbol":"XBTUSD","id":17999995000,"side":"Buy","size":10,"price":50},
              {"symbol":"XBTUSD","id":17999996000,"side":"Buy","size":20,"price":40},
              {"symbol":"XBTUSD","id":17999997000,"side":"Buy","size":100,"price":30}
            ]
          }
        {
            "table":"orderBookL2_25",
            "action":"update",
            "data":[
              {"symbol":"XBTUSD","id":17999995000,"side":"Buy","size":5}
            ]
          }
        {
            "table":"orderBookL2_25",
            "action":"delete",
            "data":[
              {"symbol":"XBTUSD","id":17999995000,"side":"Buy"}
            ]
          }
        {
            "table":"orderBookL2_25",
            "action":"insert",
            "data":[
              {"symbol":"XBTUSD","id":17999995500,"side":"Buy","size":10,"price":45},
            ]
        }
        """
        
        # There are four possible actions from the WS:
        # 'partial' - full table image
        # 'insert'  - new row
        # 'update'  - update row
        # 'delete'  - delete row
        
        # Since 'delete' actions are sent in separate payloads
        # there are moments of crossing (buy level <-> sell level)
        # where the crossed levels are empty (when in fact they (probably) aren't)
        # The solution is to cache the update and prevent it from being pushed for
        # a short amount of time
        
        table = message['table']
        action = message['action']
        
        hold_time = self.ob['hold_times'][table]
        _default = lambda price, size: [price, size]
        _delete = lambda price, size: [price, 0]
        op = _default if action != 'delete' else _delete
        updates = {}
        
        for x in message['data']:
            symbol = self.convert_symbol(x['symbol'], 0)
            side = 'bids' if x['side'] == 'Buy' else 'asks'
            price = self._ob_price_from_id(symbol, x['id'], x.get('price'))
            if price is not None:
                item = op(price, x.get('size'))
                if symbol not in updates:
                    updates[symbol] = self.api.ob_entry(symbol, timestamp=ctime_ms())
                updates[symbol][side].append(item)
        
        for symbol,ob in updates.items():
            if action == 'partial':
                ob['bids'].sort(key=lambda x: x[0], reverse=True)
                ob['asks'].sort(key=lambda x: x[0])
                self.ob_maintainer.send_orderbook(ob)
            else:
                force_push = False
                if action == 'delete':
                    # hold for 16/50 ms
                    # this seems to be enough for the next 'insert' action to be received
                    ob['__hold__'] = hold_time
                elif action == 'insert' and table == 'orderBookL2':
                    #orderBookL2 seems to send the associated inserts right after delete,
                    #while orderBookL2_25 may first send inserts related to fringe changes
                    #(it always shows 25 items)
                    force_push = True
                self.ob_maintainer.send_update(ob, force_push)
                
                
    def on_orderbook10(self, message):      
        """
        {
            'table': 'orderBook10',
            'action': 'partial',
            'keys': ['symbol'],
            'types': {'symbol': 'symbol', 'bids': '', 'asks': '', 'timestamp': 'timestamp'},
            'foreignKeys': {'symbol': 'instrument'},
            'attributes': {'symbol': 'sorted'},
            'filter': {'symbol': 'ETHUSD'},
            'data': [
                {'symbol': 'ETHUSD',
                 'bids': [[146.05, 52650], [146, 283623], [145.95, 125823], [145.9, 94451], [145.85, 184467], [145.8, 274912], [145.75, 102778], [145.7, 142252], [145.65, 73277], [145.6, 123599]],
                 'asks': [[146.1, 207296], [146.15, 134945], [146.2, 243843], [146.25, 167226], [146.3, 88612], [146.35, 185717], [146.4, 184628], [146.45, 54077], [146.5, 130910], [146.55, 73635]],
                 'timestamp': '2019-12-04T12:30:38.937Z'},
            ]
          }
        {
            'table': 'orderBook10',
            'action': 'update',
            'data': [
                {'symbol': 'ETHUSD',
                 'bids': [[146.05, 52650], [146, 283623], [145.95, 115823], [145.9, 94451], [145.85, 184467], [145.8, 274912], [145.75, 102778], [145.7, 142252], [145.65, 73277], [145.6, 123599]],
                 'asks': [[146.1, 207296], [146.15, 134945], [146.2, 243843], [146.25, 167226], [146.3, 88612], [146.35, 185717], [146.4, 184628], [146.45, 54077], [146.5, 130910], [146.55, 73635]],
                 'timestamp': '2019-12-04T12:30:39.239Z'},
            ]
        }
        """
        
        # Action can be either 'partial' or 'update'
        
        updates = {}
        
        for x in message['data']:
            symbol = self.convert_symbol(x['symbol'], 0)
            updates[symbol] = self.api.ob_entry(symbol, bids=x.get('bids'), asks=x.get('asks'), datetime=x.get('timestamp'))
                
        for symbol,ob in updates.items():
            self.ob_maintainer.send_orderbook(ob)
    
    
    def on_order(self, message):
        """
        #STOP ORDER:
        {'table': 'order',
         'action': 'insert',
         'data': [{'orderID': 'c4bf0e2c-e6af-5c2b-9e62-73ca8300e02e',
                   'clOrdID': '',
                   'clOrdLinkID': '',
                   'account': 123456,
                   'symbol': 'ETHUSD',
                   'side': 'Buy',
                   'simpleOrderQty': None,
                   'orderQty': 216,
                   'price': None,
                   'displayQty': None,
                   'stopPx': 153.25,
                   'pegOffsetValue': None,
                   'pegPriceType': '',
                   'currency': 'USD',
                   'settlCurrency': 'XBt',
                   'ordType': 'Stop',
                   'timeInForce': 'ImmediateOrCancel',
                   'execInst': '',
                   'contingencyType': '',
                   'exDestination': 'XBME',
                   'ordStatus': 'New',
                   'triggered': '',
                   'workingIndicator': False,
                   'ordRejReason': '',
                   'simpleLeavesQty': None,
                   'leavesQty': 216,
                   'simpleCumQty': None,
                   'cumQty': 0,
                   'avgPx': None,
                   'multiLegReportingType': 'SingleSecurity',
                   'text': 'Submitted via API.',
                   'transactTime': '2019-11-25T18:20:48.149Z',
                   'timestamp': '2019-11-25T18:20:48.149Z'}]}
                   
        {'table': 'order',
         'action': 'update',
         'data': [{'orderID': 'c4bf0e2c-e6af-5c2b-9e62-73ca8300e02e',
                   'ordStatus': 'Canceled',
                   'leavesQty': 0,
                   'text': 'Canceled: Cancel from www.bitmex.com\nSubmitted via API.',
                   'timestamp': '2019-11-25T18:20:55.304Z',
                   'clOrdID': '',
                   'account': 123456,
                   'symbol': 'ETHUSD'}]}
        
        #LIMIT ORDER:
        #1
        {'table': 'order', 'action': 'insert',
        'data': [{'orderID': 'ab4f4d1b-436c-b49a-c8f9-b1c6bcfde4c0', 'clOrdID': '', 'clOrdLinkID': '', 'account': 123456,
                  'symbol': 'ETHUSD', 'side': 'Buy', 'simpleOrderQty': None, 'orderQty': 8, 'price': 145.8, 'displayQty': None,
                  'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USD', 'settlCurrency': 'XBt',
                  'ordType': 'Limit', 'timeInForce': 'GoodTillCancel', 'execInst': '', 'contingencyType': '', 'exDestination': 'XBME',
                  'ordStatus': 'New', 'triggered': '', 'workingIndicator': False, 'ordRejReason': '', 'simpleLeavesQty': None,
                  'leavesQty': 8, 'simpleCumQty': None, 'cumQty': 0, 'avgPx': None, 'multiLegReportingType': 'SingleSecurity',
                  'text': 'Submission from www.bitmex.com', 'transactTime': '2019-11-21T15:21:53.006Z',
                  'timestamp': '2019-11-26T00:26:57.245Z'}]}
        
        #8
        {'table': 'order',
         'action': 'update',
         'data': [{'orderID': 'ab4f4d1b-436c-b49a-c8f9-b1c6bcfde4c0', 'ordStatus': 'Filled', 'workingIndicator': False,
                  'leavesQty': 0, 'cumQty': 8, 'avgPx': 145.8, 'timestamp': '2019-11-21T15:21:53.006Z', 'clOrdID': '',
                  'account': 123456, 'symbol': 'ETHUSD'}]}
        #10
        {'table': 'order', 'action': 'insert',
         'data': [{'orderID': '66ab03de-fc2a-a990-4ab7-4bd1c00353ab', 'clOrdID': '', 'clOrdLinkID': '', 'account': 123456,
                   'symbol': 'ETHUSD', 'side': '', 'simpleOrderQty': None, 'orderQty': None, 'price': None, 'displayQty': None,
                   'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USD', 'settlCurrency': 'XBt',
                   'ordType': 'Market', 'timeInForce': 'ImmediateOrCancel', 'execInst': 'Close', 'contingencyType': '',
                   'exDestination': 'XBME', 'ordStatus': 'New', 'triggered': '', 'workingIndicator': False, 'ordRejReason': '',
                   'simpleLeavesQty': None, 'leavesQty': None, 'simpleCumQty': None, 'cumQty': 0, 'avgPx': None,
                   'multiLegReportingType': 'SingleSecurity', 'text': 'Position Close from www.bitmex.com',
                   'transactTime': '2019-11-21T15:24:17.091Z', 'timestamp': '2019-11-21T15:24:17.091Z'}]}
        #11                                                         
        {'table': 'order', 'action': 'update',
         'data': [{'orderID': '66ab03de-fc2a-a990-4ab7-4bd1c00353ab', 'side': 'Sell', 'orderQty': 8, 'price': 145,
                   'workingIndicator': True, 'leavesQty': 8, 'clOrdID': '', 'account': 123456, 'symbol': 'ETHUSD',
                   'timestamp': '2019-11-21T15:24:17.091Z'}]}
        #13
        {'table': 'order', 'action': 'update',
         'data': [{'orderID': '66ab03de-fc2a-a990-4ab7-4bd1c00353ab', 'ordStatus': 'Filled', 'workingIndicator': False,
                   'leavesQty': 0, 'cumQty': 8, 'avgPx': 145, 'clOrdID': '', 'account': 123456, 'symbol': 'ETHUSD',
                   'timestamp': '2019-11-21T15:24:17.091Z'}]}
        """
                   
        action = message['action']
        for d in message['data']:
            symbol = self.convert_symbol(d['symbol'], 0)
            id = d['orderID']
            datetime = d['timestamp']
            timestamp = self.api.parse8601(datetime)
            #For stop order the price is None
            price = d.get('price')
            average = d.get('avgPx')
            #types: #Limit, Stop (market), Market, ...
            type = d['ordType'].lower() if 'ordType' in d else None 
            amount = d.get('orderQty')
            side = d['side'].lower() if 'side' in d else None
            remaining = d.get('leavesQty')
            filled = d.get('cumQty')
            stop = d.get('stopPx')
            extra = {}
            exists = id in self.orders
            is_create_action = action in ('insert','partial')
            #('partial' sends open orders upon subscription)
            if is_create_action and not exists:
                self.add_order(id=id, symbol=symbol, side=side, amount=amount,
                               price=price, timestamp=timestamp,
                               remaining=remaining, filled=filled, type=type,
                               stop=stop, datetime=datetime, average=average,
                               params=extra)
            elif is_create_action and exists or action == 'update':
                if amount is not None:
                    extra['amount'] = amount
                if 'price' in d:
                    extra['price'] = price
                if 'stopPx' in d:
                    extra['stop'] = stop
                if type is not None:
                    extra['type'] = type
                if side is not None:
                    extra['side'] = side
                self.update_order(id, remaining, filled, average=average, params=extra)
            #elif action == 'delete':
            #    self.update_order(id, 0, filled) #?
            else:
                self.error("Unknown order action: '{}'; item: {}".format(action, d))
            
            
    def on_fill(self, message):
        """
        #5
        {'table': 'execution',
         'action': 'insert',
         'data': [{'execID': '18108e22-13a1-7fd7-7115-a661254356d7',
                   'orderID': 'ab4f4d1b-436c-b49a-c8f9-b1c6bcfde4c0',
                   'clOrdID': '',
                   'clOrdLinkID': '',
                   'account': 123456,
                   'symbol': 'ETHUSD',
                   'side': 'Buy',
                   'lastQty': None,
                   'lastPx': None,
                   'underlyingLastPx': None,
                   'lastMkt': '',
                   'lastLiquidityInd': '',
                   'simpleOrderQty': None,
                   'orderQty': 8,
                   'price': 145.8,
                   'displayQty': None,
                   'stopPx': None,
                   'pegOffsetValue': None,
                   'pegPriceType': '',
                   'currency': 'USD',
                   'settlCurrency': 'XBt',
                   'execType': 'New',
                   'ordType': 'Limit',
                   'timeInForce': 'GoodTillCancel',
                   'execInst': '',
                   'contingencyType':'',
                   'exDestination': 'XBME',
                   'ordStatus': 'New',
                   'triggered': '',
                   'workingIndicator': True,
                   'ordRejReason': '',
                   'simpleLeavesQty': None,
                   'leavesQty': 8,
                   'simpleCumQty': None,
                   'cumQty': 0,
                   'avgPx': None,
                   'commission': None,
                   'tradePublishIndicator': '',
                   'multiLegReportingType': 'SingleSecurity',
                   'text': 'Submission from www.bitmex.com',
                   'trdMatchID': '00000000-0000-0000-0000-000000000000',
                   'execCost': None,
                   'execComm': None,
                   'homeNotional': None,
                   'foreignNotional': None,
                   'transactTime': '2019-11-21T15:22:09.810Z',
                   'timestamp': '2019-11-21T15:22:09.810Z'}]}
        
        #7
        {'table': 'execution',
        'action': 'insert',
        'data': [{'execID': '6fc465d9-7d94-653b-4015-0498e564907c', 'orderID': 'ab4f4d1b-436c-b49a-c8f9-b1c6bcfde4c0',
                  'clOrdID': '', 'clOrdLinkID': '', 'account': 123456, 'symbol': 'ETHUSD', 'side': 'Buy', 'lastQty': 8,
                  'lastPx': 145.8, 'underlyingLastPx': None, 'lastMkt': 'XBME', 'lastLiquidityInd': 'AddedLiquidity',
                  'simpleOrderQty': None, 'orderQty': 8, 'price': 145.8, 'displayQty': None, 'stopPx': None,
                  'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USD', 'settlCurrency': 'XBt', 'execType': 'Trade',
                  'ordType': 'Limit', 'timeInForce': 'GoodTillCancel', 'execInst': '', 'contingencyType': '',
                  'exDestination': 'XBME', 'ordStatus': 'Filled', 'triggered': '', 'workingIndicator': False,
                  'ordRejReason': '', 'simpleLeavesQty': None, 'leavesQty': 0, 'simpleCumQty': None, 'cumQty': 8,
                  'avgPx': 145.8, 'commission': -0.00025, 'tradePublishIndicator': 'PublishTrade',
                  'multiLegReportingType': 'SingleSecurity', 'text': 'Submission from www.bitmex.com',
                  'trdMatchID': 'ec3ba73b-e402-4226-63c4-d673926a2d63', 'execCost': 116640, 'execComm': -29,
                  'homeNotional': 0.05690305142613273, 'foreignNotional': -8.296464897930152,
                  'transactTime': '2019-11-21T15:22:11.351Z', 'timestamp': '2019-11-21T15:22:11.351Z'}]}
        #12
        {'table': 'execution',
         'action': 'insert',
         'data': [{'execID': '4445e25d-8c87-21da-2d09-021dc7cf6f51', 'orderID': '66ab03de-fc2a-a990-4ab7-4bd1c00353ab',
                   'clOrdID': '', 'clOrdLinkID': '', 'account': 123456, 'symbol': 'ETHUSD', 'side': 'Sell', 'lastQty': None,
                   'lastPx': None, 'underlyingLastPx': None, 'lastMkt': '', 'lastLiquidityInd': '', 'simpleOrderQty': None,
                   'orderQty': 8, 'price': 145, 'displayQty': None, 'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '',
                   'currency': 'USD', 'settlCurrency': 'XBt', 'execType': 'New', 'ordType': 'Market', 'timeInForce': 'ImmediateOrCancel',
                   'execInst': 'Close', 'contingencyType': '', 'exDestination': 'XBME', 'ordStatus': 'New', 'triggered': '',
                   'workingIndicator': True, 'ordRejReason': '', 'simpleLeavesQty': None, 'leavesQty': 8, 'simpleCumQty': None,
                   'cumQty': 0, 'avgPx': None, 'commission': None, 'tradePublishIndicator': '', 'multiLegReportingType': 'SingleSecurity',
                   'text': 'Position Close from www.bitmex.com', 'trdMatchID': '00000000-0000-0000-0000-000000000000', 'execCost': None,
                   'execComm': None, 'homeNotional': None, 'foreignNotional': None, 'transactTime': '2019-11-21T15:24:17.091Z',
                   'timestamp': '2019-11-21T15:24:17.091Z'},
                   {'execID': '30619e49-c1ac-7ef0-3406-6c0270076103', 'orderID': '66ab03de-fc2a-a990-4ab7-4bd1c00353ab',
                   'clOrdID': '', 'clOrdLinkID': '', 'account': 123456, 'symbol': 'ETHUSD', 'side': 'Sell', 'lastQty': 8,
                   'lastPx': 145, 'underlyingLastPx': None, 'lastMkt': 'XBME', 'lastLiquidityInd': 'RemovedLiquidity',
                   'simpleOrderQty': None, 'orderQty': 8, 'price': 145, 'displayQty': None, 'stopPx': None,
                   'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USD', 'settlCurrency': 'XBt', 'execType': 'Trade',
                   'ordType': 'Market', 'timeInForce': 'ImmediateOrCancel', 'execInst': 'Close', 'contingencyType': '',
                   'exDestination': 'XBME', 'ordStatus': 'Filled', 'triggered': '', 'workingIndicator': False, 'ordRejReason': '',
                   'simpleLeavesQty': None, 'leavesQty': 0, 'simpleCumQty': None, 'cumQty': 8, 'avgPx': 145, 'commission': 0.00075,
                   'tradePublishIndicator': 'PublishTrade', 'multiLegReportingType': 'SingleSecurity',
                   'text': 'Position Close from www.bitmex.com', 'trdMatchID': '3972ebc0-b24e-5683-1bd3-dae1955df36a', 
                   'execCost': -116000, 'execComm': 87, 'homeNotional': -0.05684240443370755, 'foreignNotional': 8.242148642887594,
                   'transactTime': '2019-11-21T15:24:17.091Z', 'timestamp': '2019-11-21T15:24:17.091Z'}]}
        """
        map = {
            'id': 'trdMatchID',
            'order': 'orderId',
            'symbol': 'symbol',
            'side': 'side',
            'price': 'price',
            'amount': 'lastQty',
            'datetime': 'timestamp',
        }
        
        for d in message['data']:
            if d['execType'] != 'Trade':
                continue
            
            #mapped = {x: d[map[x]] for x in map if map[x] in d}
            #mapped['symbol'] = self.convert_symbol(mapped['symbol'], 0)
            #fill = dict(**mapped, info=d)
            fill = self.api.parse_trade(d)
            
            self.add_fill_from_dict(fill)
    
    
    def on_position(self, message):
        #https://www.bitmex.com/api/explorer/#!/Position/Position_get
        """
        These are sent on 10-25 sec interval, and right after orders are created/executed
        #3
        {'table': 'position', 'action': 'update', 
         'data': [{'account': 123456, 'symbol': 'ETHUSD', 'currency': 'XBt', 'openOrderBuyQty': 8, 'openOrderBuyCost': 116640,
                   'grossOpenCost': 116640, 'markPrice': 145.88, 'riskValue': 116640, 'initMargin': 39085,
                   'timestamp': '2019-11-21T15:21:53.006Z', 'currentQty': 0, 'liquidationPrice': None}]}
        
        #6 - position updates
        ...
        #9 - position and margin updates (after order "filled" #8)
        {'table': 'position', 'action': 'update',
         'data': [{'account': 123456, 'symbol': 'ETHUSD', 'currency': 'XBt', 'openOrderBuyQty': 0, 'openOrderBuyCost': 0,
                   'execBuyQty': 8, 'execBuyCost': 116640, 'execQty': 8, 'execCost': 116640, 'execComm': -29,
                   'currentTimestamp': '2019-11-21T15:21:53.008Z', 'currentQty': 8, 'currentCost': 183120, 'currentComm': -531,
                   'unrealisedCost': 116640, 'grossOpenCost': 0, 'grossExecCost': 116640, 'isOpen': True, 'markValue': 116616,
                   'riskValue': 116616, 'homeNotional': 0.05690305142613273, 'foreignNotional': -8.294757806387368, 'posCost': 116640,
                   'posCost2': 116640, 'posInit': 38880, 'posComm': 117, 'posMargin': 38997, 'posMaint': 1349, 'initMargin': 0,
                   'maintMargin': 38973, 'realisedPnl': -65949, 'unrealisedGrossPnl': -24, 'unrealisedPnl': -24,
                   'unrealisedPnlPcnt': -0.0002, 'unrealisedRoePcnt': -0.0006, 'avgCostPrice': 145.8, 'avgEntryPrice': 145.8,
                   'breakEvenPrice': 145.8, 'marginCallPrice': 98.75, 'liquidationPrice': 98.75, 'bankruptPrice': 97.2,
                   'timestamp': '2019-11-21T15:21:53.008Z', 'lastValue': 116616, 'markPrice': 145.77}]}
        ...
        #14 (after order "filled" #13)
        {'table': 'position', 'action': 'update',
         'data': [{'account': 123456, 'symbol': 'ETHUSD', 'currency': 'XBt', 'rebalancedPnl': 66676, 'prevRealisedPnl': -698,
                   'execSellQty': 8, 'execSellCost': 116000, 'execQty': 0, 'execCost': 640, 'execComm': 58,
                   'currentTimestamp': '2019-11-21T15:24:17.092Z', 'currentQty': 0, 'currentCost': 67120, 'currentComm': -444,
                   'realisedCost': 67120, 'unrealisedCost': 0, 'grossExecCost': 0, 'isOpen': False, 'markPrice': None, 'markValue': 0,
                   'riskValue': 0, 'homeNotional': 0, 'foreignNotional': 0, 'posCost': 0, 'posCost2': 0, 'posInit': 0, 'posComm': 0,
                   'posMargin': 0, 'posMaint': 0, 'maintMargin': 0, 'realisedGrossPnl': -67120, 'realisedPnl': -66676, 'unrealisedGrossPnl': 0,
                   'unrealisedPnl': 0, 'unrealisedPnlPcnt': 0, 'unrealisedRoePcnt': 0, 'avgCostPrice': None, 'avgEntryPrice': None,
                   'breakEvenPrice': None, 'marginCallPrice': None, 'liquidationPrice': None, 'bankruptPrice': None,
                   'timestamp': '2019-11-21T15:24:17.092Z','lastPrice': None, 'lastValue': 0}]}
        """
        map = {'price': 'avgEntryPrice',
               'amount': 'currentQty',
               'leverage': 'leverage',
               'liq_price': 'liquidationPrice'}
        data = []
        
        for d in message['data']:
            symbol = self.convert_symbol(d['symbol'], 0)
            datetime, timestamp = resolve_times(d['timestamp'])
            #info = {x: d.get(x) for x in ['realisedPnl','unrealisedPnl','avgCostPrice','avgEntryPrice','currentQty',
            #                              'breakEvenPrice','marginCallPrice','liquidationPrice','bankruptPrice',
            #                              'isOpen', ] if x in d}
            mapped = {x: d[map[x]] for x in map if map[x] in d}
            
            e = dict({'symbol': symbol,
                      'timestamp': timestamp,
                      'datetime': datetime},
                     **mapped,
                     info=d)
            data.append(e)
        
        self.update_positions(data)
    
    
    def on_margin(self, r):
        """
        #4
        {'table': 'margin', 'action': 'update', 
         'data': [{'account': 123456, 'currency': 'XBt', 'grossOpenCost': 116640, 'riskValue': 116640, 'initMargin': 39085,
                   'marginUsedPcnt': 0.9858, 'excessMargin': 563, 'availableMargin': 563, 'withdrawableMargin': 563,
                   'timestamp': '2019-11-21T15:21:53.006Z'}]}   
        {'table': 'margin', 'action': 'update',
         'data': [{'account': 123456, 'currency': 'XBt', 'grossComm': -531, 'grossOpenCost': 0, 'grossExecCost': 116640,
                   'grossMarkValue': 116616, 'riskValue': 116616, 'initMargin': 0, 'maintMargin': 38973, 'realisedPnl': -65949,
                   'unrealisedPnl': -24, 'walletBalance': 39677, 'marginBalance': 39653, 'marginBalancePcnt': 0.34,
                   'marginLeverage': 2.9409124152018764, 'marginUsedPcnt': 0.9829, 'excessMargin': 680, 'excessMarginPcnt': 0.0058,
                   'availableMargin': 680, 'withdrawableMargin': 680, 'timestamp': '2019-11-21T15:21:53.009Z', 'grossLastValue': 116616}]}
        #15 [final]
        {'table': 'margin', 'action': 'update',
         'data': [{'account': 123456, 'currency': 'XBt', 'prevRealisedPnl': -588323, 'grossComm': -444, 'grossExecCost': 0, 'grossMarkValue': 0,
                   'riskValue': 0, 'maintMargin': 0, 'realisedPnl': -66676, 'unrealisedPnl': 0, 'walletBalance': 38950, 'marginBalance': 38950,
                   'marginBalancePcnt': 1, 'marginLeverage': 0, 'marginUsedPcnt': 0, 'excessMargin': 38950, 'excessMarginPcnt': 1,
                   'availableMargin': 38950, 'withdrawableMargin': 38950, 'timestamp': '2019-11-21T15:24:17.093Z', 'grossLastValue': 0}]}
        """
        to_btc = lambda x: x * 10**-8
        map = {
            'free': 'availableMargin',
            'total': 'marginBalance',
        }
        data = []
        
        for d in r['data']:
            # Are there any other currencies?
            if d['currency'] != 'XBt':
                logger2.error('{} - received unexpected margin currency: {}'.format(self.name, d['currency']))
                continue
            
            mapped = {k: to_btc(d[map[k]]) for k in map if map[k] in d}
            
            b_dict = dict(cy='BTC', **mapped, info=d)
            
            data.append(b_dict)
        
        self.update_balances(data)
    
    
    def on_transact(self, message):
        logger.debug(message)
    
    
    def on_wallet(self, message):
        logger.debug(message)
    
    
    def _ob_price_from_id(self, symbol, id, price=None):
        id_prices = self._id_prices[symbol]
        start = id_prices['start']
        per_tick = id_prices['per_tick']
        multiplier = id_prices['multiplier']
        cache = id_prices['cache']
       
        if start is None:
            if price is not None and not any(x[0]==id for x in cache):
                cache.append([id, price])
            if len(cache) >= 2:
                cache.sort(key=lambda x: x[1])
                id_0,price_0 = cache[0]
                id_1,price_1 = cache[1]
                tick_size = self.api.markets[symbol]['precision']['price']
                ticks_count = (price_1-price_0)/tick_size
                id_diff = id_0 - id_1
                per_tick = id_diff / ticks_count
                id_prices['per_tick'] = per_tick
                multiplier = 1/per_tick*tick_size
                id_prices['multiplier'] = multiplier
                start = round(id_0 + price_0 / multiplier)
                id_prices['start'] = start
                #print('start: {} per_tick: {} mp: {} cache: {}'.format(start, per_tick, multiplier, cache))
                
        if price is None and start is not None:
            price = (start - id) * multiplier
            price = self.api.round_price(symbol, price, method='round')
            #print('id: {} inferred price: {}'.format(id, price))
            
        return price
    
    
    def encode(self, rq, sub=None):
        """
            "announcement",        // Site announcements
            "chat",                // Trollbox chat
            "connected",           // Statistics of connected users/bots
            "funding",             // Updates of swap funding rates. Sent every funding interval (usually 8hrs)
            "instrument",          // Instrument updates including turnover and bid/ask
            "insurance",           // Daily Insurance Fund updates
            "liquidation",         // Liquidation orders as they're entered into the book
            "orderBookL2_25",      // Top 25 levels of level 2 order book
            "orderBookL2",         // Full level 2 order book
            "orderBook10",         // Top 10 levels using traditional full book push
            "publicNotifications", // System-wide notifications (used for short-lived messages)
            "quote",               // Top level of the book
            "quoteBin1m",          // 1-minute quote bins
            "quoteBin5m",          // 5-minute quote bins
            "quoteBin1h",          // 1-hour quote bins
            "quoteBin1d",          // 1-day quote bins
            "settlement",          // Settlements
            "trade",               // Live trades
            "tradeBin1m",          // 1-minute trade bins
            "tradeBin5m",          // 5-minute trade bins
            "tradeBin1h",          // 1-hour trade bins
            "tradeBin1d",          // 1-day trade bins
            
            "execution",   // Individual executions; can be multiple per order
            "order",       // Live updates on your orders
            "margin",      // Updates on your current account balance and margin requirements
            "position",    // Updates on your positions
            "privateNotifications", // Individual notifications - currently not used
            "transact"     // Deposit/Withdrawal updates
            "wallet"       // Bitcoin address balance data, including total deposits & withdrawals
        """
        channel = rq.channel
        params = rq.params
        
        if sub:
            op = 'subscribe'
        elif sub is False:
            op = 'unsubscribe'
        
        limit = params.get('limit')
        throttled = params.get('throttled')
        if throttled is None:
            throttled = True
        
        if limit is None or limit > 25:
            orderbook = ['orderBookL2:<symbol>']
            limit = None
        elif limit <= 10 and throttled:
            orderbook = ['orderBook10:<symbol>']
            limit = 10
        elif limit <= 25:
            orderbook = ['orderBookL2_25:<symbol>']
            limit = 25
        
        if channel == 'orderbook':
            self.ob_maintainer.set_limit(params['symbol'], limit)
        
        channel_ids = self.channel_ids[channel] if channel != 'orderbook' else orderbook
        args = self.encode_symbols(params.get('symbol'), channel_ids)
        #print(op,args)
        
        return {'op': op, 'args': args}
    
    
    def sign(self, out=None):
        #signature is hex(HMAC_SHA256(secret, 'GET/realtime' + expires))
        #expires must be a number, not a string.
        #{"op": "authKeyExpires", "args": ["<APIKey>", <expires>, "<signature>"]}
        expires = int(time.time()) + 5
        signature = sign(self.secret, 'GET/realtime' + str(expires))
        return {
            'op': 'authKeyExpires',
            'args': [self.apiKey, expires, signature],
        }
    
    
    """def ping(self):
        #This is an unrecognized request, but at least it receives response
        #that tells us that the connection is still up
        #Will regular "pinging" also make it less likely for the cnx to drop?
        #(which happened without pinging in every ~3 min)
        return 'ping'"""


# Generates an API signature.
# A signature is HMAC_SHA256(secret, verb + path + nonce + data), hex encoded.
# Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
# and the data, if present, must be JSON without whitespace between keys.
def bitmex_signature(apiSecret, verb, url, nonce, postdict=None):
    """Given an API Secret key and data, create a BitMEX-compatible signature."""
    data = ''
    if postdict:
        # separators remove spaces from json
        # BitMEX expects signatures from JSON built without spaces
        data = json.dumps(postdict, separators=(',', ':'))
    parsedURL = urllib.parse.urlparse(url)
    path = parsedURL.path
    if parsedURL.query:
        path = path + '?' + parsedURL.query
    # print("Computing HMAC: %s" % verb + path + str(nonce) + data)
    message = verb + path + str(nonce) + data
    print("Signing: %s" % str(message))
    
    signature = sign(apiSecret, message)
    print("Signature: %s" % signature)
    
    return signature


"""
{'table': 'execution', 'action': 'partial', 'keys': ['execID'], 'types': {'execID': 'guid', 'orderID': 'guid', 'clOrdID': 'symbol', 'clOrdLinkID': 'symbol', 'account': 'long', 'symbol': 'symbol', 'side': 'symbol', 'lastQty': 'long', 'lastPx': 'float', 'underlyingLastPx': 'float', 'lastMkt': 'symbol', 'lastLiquidityInd': 'symbol', 'simpleOrderQty': 'float', 'orderQty': 'long', 'price': 'float', 'displayQty': 'long', 'stopPx': 'float', 'pegOffsetValue': 'float', 'pegPriceType': 'symbol', 'currency': 'symbol', 'settlCurrency': 'symbol', 'execType': 'symbol', 'ordType': 'symbol', 'timeInForce': 'symbol', 'execInst': 'symbol', 'contingencyType': 'symbol', 'exDestination': 'symbol', 'ordStatus': 'symbol', 'triggered': 'symbol', 'workingIndicator': 'boolean', 'ordRejReason': 'symbol', 'simpleLeavesQty': 'float', 'leavesQty': 'long', 'simpleCumQty': 'float', 'cumQty': 'long', 'avgPx': 'float', 'commission': 'float', 'tradePublishIndicator': 'symbol', 'multiLegReportingType': 'symbol', 'text': 'symbol', 'trdMatchID': 'guid', 'execCost': 'long', 'execComm': 'long', 'homeNotional': 'float', 'foreignNotional': 'float', 'transactTime': 'timestamp', 'timestamp': 'timestamp'}, 'foreignKeys': {'symbol': 'instrument', 'side': 'side', 'ordStatus': 'ordStatus'}, 'attributes': {'execID': 'grouped', 'account': 'grouped', 'execType': 'grouped', 'transactTime': 'sorted'}, 'filter': {'account': 123456, 'symbol': 'ETHUSD'}, 'data': []}
{'table': 'order', 'action': 'partial', 'keys': ['orderID'], 'types': {'orderID': 'guid', 'clOrdID': 'symbol', 'clOrdLinkID': 'symbol', 'account': 'long', 'symbol': 'symbol', 'side': 'symbol', 'simpleOrderQty': 'float', 'orderQty': 'long', 'price': 'float', 'displayQty': 'long', 'stopPx': 'float', 'pegOffsetValue': 'float', 'pegPriceType': 'symbol', 'currency': 'symbol', 'settlCurrency': 'symbol', 'ordType': 'symbol', 'timeInForce': 'symbol', 'execInst': 'symbol', 'contingencyType': 'symbol', 'exDestination': 'symbol', 'ordStatus': 'symbol', 'triggered': 'symbol', 'workingIndicator': 'boolean', 'ordRejReason': 'symbol', 'simpleLeavesQty': 'float', 'leavesQty': 'long', 'simpleCumQty': 'float', 'cumQty': 'long', 'avgPx': 'float', 'multiLegReportingType': 'symbol', 'text': 'symbol', 'transactTime': 'timestamp', 'timestamp': 'timestamp'}, 'foreignKeys': {'symbol': 'instrument', 'side': 'side', 'ordStatus': 'ordStatus'}, 'attributes': {'orderID': 'grouped', 'account': 'grouped', 'ordStatus': 'grouped', 'workingIndicator': 'grouped'}, 'filter': {'account': 123456, 'symbol': 'ETHUSD'}, 'data': []}
{'table': 'execution', 'action': 'partial', 'keys': ['execID'], 'types': {'execID': 'guid', 'orderID': 'guid', 'clOrdID': 'symbol', 'clOrdLinkID': 'symbol', 'account': 'long', 'symbol': 'symbol', 'side': 'symbol', 'lastQty': 'long', 'lastPx': 'float', 'underlyingLastPx': 'float', 'lastMkt': 'symbol', 'lastLiquidityInd': 'symbol', 'simpleOrderQty': 'float', 'orderQty': 'long', 'price': 'float', 'displayQty': 'long', 'stopPx': 'float', 'pegOffsetValue': 'float', 'pegPriceType': 'symbol', 'currency': 'symbol', 'settlCurrency': 'symbol', 'execType': 'symbol', 'ordType': 'symbol', 'timeInForce': 'symbol', 'execInst': 'symbol', 'contingencyType': 'symbol', 'exDestination': 'symbol', 'ordStatus': 'symbol', 'triggered': 'symbol', 'workingIndicator': 'boolean', 'ordRejReason': 'symbol', 'simpleLeavesQty': 'float', 'leavesQty': 'long', 'simpleCumQty': 'float', 'cumQty': 'long', 'avgPx': 'float', 'commission': 'float', 'tradePublishIndicator': 'symbol', 'multiLegReportingType': 'symbol', 'text': 'symbol', 'trdMatchID': 'guid', 'execCost': 'long', 'execComm': 'long', 'homeNotional': 'float', 'foreignNotional': 'float', 'transactTime': 'timestamp', 'timestamp': 'timestamp'}, 'foreignKeys': {'symbol': 'instrument', 'side': 'side', 'ordStatus': 'ordStatus'}, 'attributes': {'execID': 'grouped', 'account': 'grouped', 'execType': 'grouped', 'transactTime': 'sorted'}, 'filter': {'account': 123456, 'symbol': 'XBTUSD'}, 'data': []}
{'table': 'order', 'action': 'partial', 'keys': ['orderID'], 'types': {'orderID': 'guid', 'clOrdID': 'symbol', 'clOrdLinkID': 'symbol', 'account': 'long', 'symbol': 'symbol', 'side': 'symbol', 'simpleOrderQty': 'float', 'orderQty': 'long', 'price': 'float', 'displayQty': 'long', 'stopPx': 'float', 'pegOffsetValue': 'float', 'pegPriceType': 'symbol', 'currency': 'symbol', 'settlCurrency': 'symbol', 'ordType': 'symbol', 'timeInForce': 'symbol', 'execInst': 'symbol', 'contingencyType': 'symbol', 'exDestination': 'symbol', 'ordStatus': 'symbol', 'triggered': 'symbol', 'workingIndicator': 'boolean', 'ordRejReason': 'symbol', 'simpleLeavesQty': 'float', 'leavesQty': 'long', 'simpleCumQty': 'float', 'cumQty': 'long', 'avgPx': 'float', 'multiLegReportingType': 'symbol', 'text': 'symbol', 'transactTime': 'timestamp', 'timestamp': 'timestamp'}, 'foreignKeys': {'symbol': 'instrument', 'side': 'side', 'ordStatus': 'ordStatus'}, 'attributes': {'orderID': 'grouped', 'account': 'grouped', 'ordStatus': 'grouped', 'workingIndicator': 'grouped'}, 'filter': {'account': 123456, 'symbol': 'XBTUSD'}, 'data': []}
{'table': 'margin', 'action': 'partial', 'keys': ['account', 'currency'], 'types': {'account': 'long', 'currency': 'symbol', 'riskLimit': 'long', 'prevState': 'symbol', 'state': 'symbol', 'action': 'symbol', 'amount': 'long', 'pendingCredit': 'long', 'pendingDebit': 'long', 'confirmedDebit': 'long', 'prevRealisedPnl': 'long', 'prevUnrealisedPnl': 'long', 'grossComm': 'long', 'grossOpenCost': 'long', 'grossOpenPremium': 'long', 'grossExecCost': 'long', 'grossMarkValue': 'long', 'riskValue': 'long', 'taxableMargin': 'long', 'initMargin': 'long', 'maintMargin': 'long', 'sessionMargin': 'long', 'targetExcessMargin': 'long', 'varMargin': 'long', 'realisedPnl': 'long', 'unrealisedPnl': 'long', 'indicativeTax': 'long', 'unrealisedProfit': 'long', 'syntheticMargin': 'long', 'walletBalance': 'long', 'marginBalance': 'long', 'marginBalancePcnt': 'float', 'marginLeverage': 'float', 'marginUsedPcnt': 'float', 'excessMargin': 'long', 'excessMarginPcnt': 'float', 'availableMargin': 'long', 'withdrawableMargin': 'long', 'timestamp': 'timestamp', 'grossLastValue': 'long', 'commission': 'float'}, 'foreignKeys': {}, 'attributes': {'account': 'sorted', 'currency': 'grouped'}, 'filter': {'account': 123456}, 'data': [{'account': 123456, 'currency': 'XBt', 'riskLimit': 1000000000000, 'prevState': '', 'state': '', 'action': '', 'amount': 105626, 'pendingCredit': 0, 'pendingDebit': 0, 'confirmedDebit': 0, 'prevRealisedPnl': -876198, 'prevUnrealisedPnl': 0, 'grossComm': -1082, 'grossOpenCost': 0, 'grossOpenPremium': 0, 'grossExecCost': 0, 'grossMarkValue': 0, 'riskValue': 0, 'taxableMargin': 0, 'initMargin': 0, 'maintMargin': 0, 'sessionMargin': 0, 'targetExcessMargin': 0, 'varMargin': 0, 'realisedPnl': -82467, 'unrealisedPnl': 0, 'indicativeTax': 0, 'unrealisedProfit': 0, 'syntheticMargin': None, 'walletBalance': 94256, 'marginBalance': 94256, 'marginBalancePcnt': 1, 'marginLeverage': 0, 'marginUsedPcnt': 0, 'excessMargin': 94256, 'excessMarginPcnt': 1, 'availableMargin': 94256, 'withdrawableMargin': 94256, 'timestamp': '2019-11-25T20:12:18.159Z', 'grossLastValue': 0, 'commission': None}]}
{'table': 'position', 'action': 'partial', 'keys': ['account', 'symbol', 'currency'], 'types': {'account': 'long', 'symbol': 'symbol', 'currency': 'symbol', 'underlying': 'symbol', 'quoteCurrency': 'symbol', 'commission': 'float', 'initMarginReq': 'float', 'maintMarginReq': 'float', 'riskLimit': 'long', 'leverage': 'float', 'crossMargin': 'boolean', 'deleveragePercentile': 'float', 'rebalancedPnl': 'long', 'prevRealisedPnl': 'long', 'prevUnrealisedPnl': 'long', 'prevClosePrice': 'float', 'openingTimestamp': 'timestamp', 'openingQty': 'long', 'openingCost': 'long', 'openingComm': 'long', 'openOrderBuyQty': 'long', 'openOrderBuyCost': 'long', 'openOrderBuyPremium': 'long', 'openOrderSellQty': 'long', 'openOrderSellCost': 'long', 'openOrderSellPremium': 'long', 'execBuyQty': 'long', 'execBuyCost': 'long', 'execSellQty': 'long', 'execSellCost': 'long', 'execQty': 'long', 'execCost': 'long', 'execComm': 'long', 'currentTimestamp': 'timestamp', 'currentQty': 'long', 'currentCost': 'long', 'currentComm': 'long', 'realisedCost': 'long', 'unrealisedCost': 'long', 'grossOpenCost': 'long', 'grossOpenPremium': 'long', 'grossExecCost': 'long', 'isOpen': 'boolean', 'markPrice': 'float', 'markValue': 'long', 'riskValue': 'long', 'homeNotional': 'float', 'foreignNotional': 'float', 'posState': 'symbol', 'posCost': 'long', 'posCost2': 'long', 'posCross': 'long', 'posInit': 'long', 'posComm': 'long', 'posLoss': 'long', 'posMargin': 'long', 'posMaint': 'long', 'posAllowance': 'long', 'taxableMargin': 'long', 'initMargin': 'long', 'maintMargin': 'long', 'sessionMargin': 'long', 'targetExcessMargin': 'long', 'varMargin': 'long', 'realisedGrossPnl': 'long', 'realisedTax': 'long', 'realisedPnl': 'long', 'unrealisedGrossPnl': 'long', 'longBankrupt': 'long', 'shortBankrupt': 'long', 'taxBase': 'long', 'indicativeTaxRate': 'float', 'indicativeTax': 'long', 'unrealisedTax': 'long', 'unrealisedPnl': 'long', 'unrealisedPnlPcnt': 'float', 'unrealisedRoePcnt': 'float', 'simpleQty': 'float', 'simpleCost': 'float', 'simpleValue': 'float', 'simplePnl': 'float', 'simplePnlPcnt': 'float', 'avgCostPrice': 'float', 'avgEntryPrice': 'float', 'breakEvenPrice': 'float', 'marginCallPrice': 'float', 'liquidationPrice': 'float', 'bankruptPrice': 'float', 'timestamp': 'timestamp', 'lastPrice': 'float', 'lastValue': 'long'}, 'foreignKeys': {'symbol': 'instrument'}, 'attributes': {'account': 'sorted', 'symbol': 'grouped', 'currency': 'grouped', 'underlying': 'grouped', 'quoteCurrency': 'grouped'}, 'filter': {'account': 123456}, 'data': [{'account': 123456, 'symbol': 'ETHUSD', 'currency': 'XBt', 'underlying': 'ETH', 'quoteCurrency': 'USD', 'commission': 0.00075, 'initMarginReq': 0.02, 'maintMarginReq': 0.01, 'riskLimit': 5000000000, 'leverage': 50, 'crossMargin': False, 'deleveragePercentile': None, 'rebalancedPnl': 65978, 'prevRealisedPnl': 12185, 'prevUnrealisedPnl': 0, 'prevClosePrice': 142.5, 'openingTimestamp': '2019-11-26T00:00:00.000Z', 'openingQty': 0, 'openingCost': 66480, 'openingComm': -502, 'openOrderBuyQty': 0, 'openOrderBuyCost': 0, 'openOrderBuyPremium': 0, 'openOrderSellQty': 0, 'openOrderSellCost': 0, 'openOrderSellPremium': 0, 'execBuyQty': 0, 'execBuyCost': 0, 'execSellQty': 0, 'execSellCost': 0, 'execQty': 0, 'execCost': 0, 'execComm': 0, 'currentTimestamp': '2019-11-26T00:00:01.236Z', 'currentQty': 0, 'currentCost': 66480, 'currentComm': -502, 'realisedCost': 66480, 'unrealisedCost': 0, 'grossOpenCost': 0, 'grossOpenPremium': 0, 'grossExecCost': 0, 'isOpen': False, 'markPrice': None, 'markValue': 0, 'riskValue': 0, 'homeNotional': 0, 'foreignNotional': 0, 'posState': '', 'posCost': 0, 'posCost2': 0, 'posCross': 0, 'posInit': 0, 'posComm': 0, 'posLoss': 0, 'posMargin': 0, 'posMaint': 0, 'posAllowance': 0, 'taxableMargin': 0, 'initMargin': 0, 'maintMargin': 0, 'sessionMargin': 0, 'targetExcessMargin': 0, 'varMargin': 0, 'realisedGrossPnl': -66480, 'realisedTax': 0, 'realisedPnl': -65978, 'unrealisedGrossPnl': 0, 'longBankrupt': 0, 'shortBankrupt': 0, 'taxBase': 0, 'indicativeTaxRate': 0, 'indicativeTax': 0, 'unrealisedTax': 0, 'unrealisedPnl': 0, 'unrealisedPnlPcnt': 0, 'unrealisedRoePcnt': 0, 'simpleQty': None, 'simpleCost': None, 'simpleValue': None, 'simplePnl': None, 'simplePnlPcnt': None, 'avgCostPrice': None, 'avgEntryPrice': None, 'breakEvenPrice': None, 'marginCallPrice': None, 'liquidationPrice': None, 'bankruptPrice': None, 'timestamp': '2019-11-26T00:00:01.236Z', 'lastPrice': None, 'lastValue': 0}, {'account': 123456, 'symbol': 'XBTUSD', 'currency': 'XBt', 'underlying': 'XBT', 'quoteCurrency': 'USD', 'commission': 0.00075, 'initMarginReq': 0.1, 'maintMarginReq': 0.005, 'riskLimit': 20000000000, 'leverage': 10, 'crossMargin': False, 'deleveragePercentile': None, 'rebalancedPnl': 0, 'prevRealisedPnl': -587625, 'prevUnrealisedPnl': 0, 'prevClosePrice': 6879.75, 'openingTimestamp': '2019-11-26T00:00:00.000Z', 'openingQty': 0, 'openingCost': 0, 'openingComm': 0, 'openOrderBuyQty': 0, 'openOrderBuyCost': 0, 'openOrderBuyPremium': 0, 'openOrderSellQty': 0, 'openOrderSellCost': 0, 'openOrderSellPremium': 0, 'execBuyQty': 0, 'execBuyCost': 0, 'execSellQty': 0, 'execSellCost': 0, 'execQty': 0, 'execCost': 0, 'execComm': 0, 'currentTimestamp': '2019-11-26T00:00:42.156Z', 'currentQty': 0, 'currentCost': 0, 'currentComm': 0, 'realisedCost': 0, 'unrealisedCost': 0, 'grossOpenCost': 0, 'grossOpenPremium': 0, 'grossExecCost': 0, 'isOpen': False, 'markPrice': None, 'markValue': 0, 'riskValue': 0, 'homeNotional': 0, 'foreignNotional': 0, 'posState': '', 'posCost': 0, 'posCost2': 0, 'posCross': 0, 'posInit': 0, 'posComm': 0, 'posLoss': 0, 'posMargin': 0, 'posMaint': 0, 'posAllowance': 0, 'taxableMargin': 0, 'initMargin': 0, 'maintMargin': 0, 'sessionMargin': 0, 'targetExcessMargin': 0, 'varMargin': 0, 'realisedGrossPnl': 0, 'realisedTax': 0, 'realisedPnl': 0, 'unrealisedGrossPnl': 0, 'longBankrupt': 0, 'shortBankrupt': 0, 'taxBase': 0, 'indicativeTaxRate': 0, 'indicativeTax': 0, 'unrealisedTax': 0, 'unrealisedPnl': 0, 'unrealisedPnlPcnt': 0, 'unrealisedRoePcnt': 0, 'simpleQty': None, 'simpleCost': None, 'simpleValue': None, 'simplePnl': None, 'simplePnlPcnt': None, 'avgCostPrice': None, 'avgEntryPrice': None, 'breakEvenPrice': None, 'marginCallPrice': None, 'liquidationPrice': None, 'bankruptPrice': None, 'timestamp': '2019-11-26T00:00:42.156Z', 'lastPrice': None, 'lastValue': 0}]}
"""