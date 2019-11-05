import asyncio
import aiohttp
import functools
import json
import datetime,time
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket, ExchangeSocketError

from fons.crypto import nonce as _nonce, sign
from fons.time import ctime_ms, pydt_from_ms
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class kucoin(ExchangeSocket):
    exchange = 'kucoin'

    auth_defaults = {
        'via_url': True,
    }
    channel_defaults = {
        'url': '<m$create_connection_url:shared>',
        'unsub_option': True,
        'merge_option': False,
    }
    channels = {
        #'account': {
        #    'url': '<m$create_connection_url>',
        #},
        'orderbook': {
            'auto_activate': False,
        },
    }
    has = {
        'balance': {'delta': False},
        'all_tickers': {'last': True, 'bid': True, 'ask': True, 'bidVolume': True, 'askVolume': True,
                        'high': False, 'low': False, 'open': False,' close': True, 'previousClose': False,
                        'change': False, 'percentage': False, 'average': False, 'vwap': False,
                        'baseVolume' :False, 'quoteVolume': False},
        'ticker': {}, #same as all_tickers
        'orderbook': True,
        'market': {'match': True},
        'account': {'balance': True, 'order': False, 'match': False},
        'match': True,
        'fetch_tickers': True,
        'fetch_ticker': True,
        'fetch_order_book': True,
        'fetch_balance': True,
    }
    has['ticker'] = has['all_tickers'].copy()
    connection_defaults = {
        'max_subscriptions': 95,
        'subscription_push_rate_limit': 0.04,
        'ping': 'm$ping',
        'ping_interval': 30,
    }
    topics = {
        'account': '/account/balance',
        'ticker': '/market/ticker:{symbol}',
        'all_tickers': '/market/ticker:all',
        'orderbook': '/market/level2:{symbol}',
        'market': '/market/match:{symbol}',
        'match': '/market/match:{symbol}',
    }
    message_id_keyword = 'id'
    match_is_subset_of_market = True
    order = {
        'update_executed_on_trade': True,
        'update_payout_on_trade': True,
        'update_left_on_trade': True,
    }

 
    def handle(self, response):
        r = response.data
        if r['type'] == 'ack':
            print(r)
            self.handle_subscription_ack(r['id'])
        elif r['type'] != 'message': 
            print(r)
        elif r['topic'].startswith('/market/ticker:'):
            self.on_ticker(r)
        elif r['topic'].startswith('/market/level2:'):
            #print(r)
            self.on_orderbook_update(r)
        elif r['topic'].startswith('/account/balance'):
            self.on_balance(r)
        elif r['topic'].startswith('/market/match:'):
            self.on_match(r)
        else:
            logger2.error('{} - unknown response: {}'.format(self.name, r))
            
            
    def on_ticker(self, r):
        """{'data': {'sequence': '1550470037508', 'bestAsk': '0.0001875', 'size': '2', 'bestBidSize': '649.35', 
                     'price': '0.0001863', 'time': 1553012443011, 'bestAskSize': '4000', 'bestBid': '0.0001862'}, 
            'subject': 'GO-ETH', 'topic': '/market/ticker:all', 'type': 'message'}"""
        if r['subject'] == 'trade.ticker':
            symbol = self.convert_symbol(r['topic'][len('/market/ticker:'):], 0)
        else:
            symbol = self.convert_symbol(r['subject'], 0)
        d = r['data']
        ts = d['time']
        ts_str = pydt_from_ms(ts).isoformat()[:23] + 'Z'
        last = float(d['price'])
        bid = float(d['bestBid'])
        bidVolume = float(d['bestBidSize'])
        ask = float(d['bestAsk'])
        askVolume = float(d['bestAskSize'])
        #last_trade_size = float(d['size'])
        self.update_tickers([{'symbol': symbol, 'timestamp': ts, 'datetime': ts_str,
            'last': last, 'high': None, 'low': None, 'open': None, 'close': None,
            'bid': bid, 'bidVolume': bidVolume,'ask': ask, 'askVolume': askVolume,
            'baseVolume': None, 'quoteVolume': None,
            'previousClose': None, 'change': None, 'percentage': None, 
            'average': None, 'vwap': None,}])
            
            
    async def fetch_order_book(self, symbol):
        request = {'symbol': self.convert_symbol(symbol,1), 'level': 2}
        response = await self.api.publicGetMarketOrderbookLevelLevel(request)
        #{'data':
        # {sequence: '1547731421688',
        #   asks: [['5c419328ef83c75456bd615c', '0.9', '0.09'], ...],
        #   bids: [['5c419328ef83c75456bd615c', '0.9', '0.09'], ...],
        #  time: 1553163056529}
        #}
        data = response['data']
        nonce = int(data['sequence'])
        timestamp = self.api.safe_integer(data, 'time')
        # level can be a string such as 2_20 or 2_100
        levelString = self.api.safe_string(request, 'level')
        levelParts = levelString.split('_')
        level = int(levelParts[0])
        ob = self.api.parse_order_book(data, timestamp, 'bids', 'asks', level - 2, level - 1)
        #with await self.locks['orderbook']:
        return {
            'symbol': symbol, 'bid': ob['bids'], 'ask': ob['asks'],
            'timestamp': ob['timestamp'], 'datetime': ob['datetime'],
            'nonce': nonce,
        }  
        
        
    def on_orderbook_update(self, r):
        """{
          "type":"message",
          "topic":"/market/level2:BTC-USDT",
          "subject":"trade.l2update",
          "data":{
            "sequenceStart":1545896669105,
            "sequenceEnd":1545896669106,
            "symbol":"BTC-USDT",
            "changes":{
              "asks":[["6","1","1545896669105"]],           //price, size, sequence
              "bids":[["4","1","1545896669106"]]
            }
          }
        }"""
        d = r['data']
        symbol = self.convert_symbol(d['symbol'], 0)
        start, end = d['sequenceStart'], d['sequenceEnd']
        updates = []
        for cur_nonce in range(start, end+1):
            str_nonce = str(cur_nonce)
            update = {'symbol': symbol, 'bid': [], 'ask': [], 'nonce': cur_nonce}
            for side in ['bid','ask']:
                try: changes = d['changes'][side+'s']
                except KeyError: continue
                update[side] = [[float(rate),float(qnt)] for rate,qnt,nonce in changes
                                if nonce==str_nonce]
            if len(update['bid']) or len(update['ask']):
                updates.append(update)
        if len(updates):
            self.orderbook_maintainer.send_update(updates)
        
        
    def on_balance(self, r):
        """{
          "type":"message",
          "topic":"/account/balance",
          "subject":"account.balance",
          "data":{
            "total": "88",
            "available": "88",
            "availableChange": "88",
            "currency": "KCS",
            "hold": "0",
            "holdChange": "0",
            "relationEvent": "main.deposit",
            "relationEventId": "5c21e80303aa677bd09d7dff",
            "time": "1545743136994",
            "accountId": "5bd6e9286d99522a52e458de"
          }
        }"""
        data = r['data']
        #print(data)
        cy = self.convert_cy(data['currency'], 0)
        balances_formatted = [(cy, float(data['available']), float(data['hold']))]
        self.update_balances(balances_formatted)
        
        
    def on_match(self, r):
        """{
          "id":"5c24c5da03aa673885cd67aa",
          "type":"message",
          "topic":"/market/match:BTC-USDT",
          "subject":"trade.l3match",
          "data":{
            "sequence":"1545896669145",
            "symbol":"BTC-USDT",
            "side":"buy",
            "size":"0.01022222000000000000",
            "price":"0.08200000000000000000",
            "takerOrderId":"5c24c5d903aa6772d55b371e",
            "time":"1545913818099033203",
            "type":"match",
            "makerOrderId":"5c2187d003aa677bd09d5c93",
            "tradeId":"5c24c5da03aa673885cd67aa"
          }
        }"""
        #print('match: {}'.format(r['data']))
        d = r['data']
        moid = d['makerOrderId']
        toid = d['takerOrderId']
        order = None
        for oid in (moid,toid):
            try: order = self.get_order(oid,'open')
            except ValueError: pass
            else: break
        if order is None:
            return
        #print('myMatch: {}'.format(r['data']))
        takerOrMaker = 'taker' if order['id'] == toid else 'maker'
        ts = int(d['time'][:-6])
        symbol = self.convert_symbol(d['symbol'],0)
        qty = float(d['size'])
        rate = float(d['price'])
        side = d['side']
        fee_mp = takerOrMaker #0.002
        self.add_trade(d['tradeId'], symbol, side, rate, qty, fee_mp, ts, order['id'])
        
        
    async def create_connection_url(self):#, auth=True):
        auth = True
        """{
            "code": "200000",
            "data": {
                "instanceServers": [
                    {
                        "pingInterval": 50000,
                        "endpoint": "wss://push1-v2.kucoin.com/endpoint",
                        "protocol": "websocket",
                        "encrypt": true,
                        "pingTimeout": 10000
                    }
                ],
                "token": "vYNlCtbz4XNJ1QncwWilJnBtmmfe4geLQDUA62kKJsDChc6I4bRDQc73JfIrlFaVYIAE0Gv2--MROnLAgjVsWkcDq_MuG7qV7EktfCEIphiqnlfpQn4Ybg==.IoORVxR2LmKV7_maOR9xOg=="
            }
        }"""
        end = '/api/v1/bullet-public' if not auth else '/api/v1/bullet-private'
        url = 'https://api.kucoin.com' + end
        headers = {}
        if auth:
            now = ctime_ms()
            #data = {}
            #data_json = json.dumps(data)
            str_to_sign = str(now) + 'POST' + '/api/v1/bullet-private' #+ data_json
            #print('str_to_sign: {}'.format(str_to_sign))
            signature = sign(self.secret, str_to_sign, 'sha256', hexdigest=False, base64=True)
            #print('signature: {}'.format(signature))
            headers = {
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": str(now),
                "KC-API-KEY": self.apiKey,
                "KC-API-PASSPHRASE": self.auth['password'],
                "Content-Type": "application/json" # specifying content type or using json=data in request
            }
        #print('headers: {}'.format(headers))
        async with aiohttp.ClientSession() as session:
            #print('connecting to: {}'.format(url))
            async with session.post(url, headers=headers) as response:
                r = (await response.read()).decode('utf-8')
        r = json.loads(r)
        #print('r: {}'.format(r))
        endpoint = r['data']['instanceServers'][0]['endpoint']
        token = r['data']['token']
        connectId = _nonce(9,'alpha')
        ws_url = endpoint + '?token=' + token + '&[connectId={}]'.format(connectId)
        return ws_url
    
    
    def encode(self, rq, sub=None):
        p = rq.params
        channel = rq.channel
        topic = self.topics[channel]
        auth = channel in ('balance', 'account', 'match')
        if channel == 'account': pass
        elif channel == 'tickers_all': pass
        elif channel in ('ticker', 'orderbook', 'market', 'match'):
            topic = topic.format(
                symbol = self.ip.comma_separate(p['symbol'], self.convert_symbol))
        
        _type = 'subscribe' if sub else 'unsubscribe'
        uid = None
        try: uid = self.get_subscription(rq).uid
        except ValueError: pass
        message_id = self.generate_message_id(uid, not sub)
        
        out = {
            "id": message_id, #The id should be an unique value, e.g. 1545910660739
            "type": _type,
            "topic": topic,  #Topic needs to be subscribed. Some topics support to divisional subscribe the informations of multiple trading pairs through ",".
            "privateChannel": auth, #Adopted the private channel or not. Set as false by default.
            "response": True, #Whether the server needs to return the receipt information of this subscription or not. Set as false by default.
        }
        return (out, message_id)
    
    
    """def subscribe_to_match(self,symbol,config={}):
        self.verify_has('market','match')
        return self.add_subscription(self.extend({
            '_': 'match',
            'symbol': symbol},config))
        
        
    def unsubscribe_to_match(self,symbol):
        self.verify_has('market','match')
        return self.remove_subscription({
            '_': 'match',
            'symbol': symbol})"""
        
        
    def convert_symbol(self, symbol, direction=1):
        #0: ex to ccxt 1: ccxt to ex
        try: return super().convert_symbol(symbol, direction)
        except KeyError: pass
        
        if not direction:
            return '/'.join(self.convert_cy(x,0) for x in symbol.split('-'))
        else:
            return '-'.join(self.convert_cy(x,1) for x in symbol.split('/'))
        
        
    def ping(self):
        return json.dumps({
            'id': ctime_ms(),
            'type': 'ping',
        })

