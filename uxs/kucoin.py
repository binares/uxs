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
        'all_tickers': {'last': True, 'bid': True, 'ask': True, 'bidVolume': True, 'askVolume': True,
                        'high': False, 'low': False, 'open': False,' close': True, 'previousClose': False,
                        'change': False, 'percentage': False, 'average': False, 'vwap': False,
                        'baseVolume' :False, 'quoteVolume': False},
        'ticker': True, #same as all_tickers
        'orderbook': True,
        'trades': {'match': True},
        'account': {'balance': True, 'order': False, 'fill': False},
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
        'ping_as_message': True,
    }
    channel_ids = {
        'account': '/account/balance',
        'ticker': '/market/ticker:{symbol}',
        'all_tickers': '/market/ticker:all',
        'orderbook': '/market/level2:{symbol}',
        'trades': '/market/match:{symbol}',
        'match': '/market/match:{symbol}',
    }
    message = {'id': {'key': 'id'}}
    match_is_subset_of_trades = True
    order = {
        'update_filled_on_fill': True,
        'update_payout_on_fill': True,
        'update_remaining_on_fill': True,
    }
    symbol = {
        'quote_ids': ['ETH', 'BTC', 'USDT', 'NEO', 'KCS', 'PAX',
                      'TUSD', 'USDC', 'NUSD', 'TRX', 'DAI'],
        'sep': '-',
    }

 
    def handle(self, response):
        r = response.data
        if r['type'] == 'ack':
            logger.debug(r)
            self.handle_subscription_ack(r['id'])
        elif r['type'] != 'message': 
            logger.debug(r)
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
            'average': None, 'vwap': None, 'info': d}])
    
    
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
            update = {'symbol': symbol, 'bids': [], 'asks': [], 'nonce': cur_nonce}
            for side in ['bids','asks']:
                try: changes = d['changes'][side]
                except KeyError: continue
                update[side] = [[float(price),float(qty)] for price,qty,nonce in changes
                                if nonce==str_nonce]
            if len(update['bids']) or len(update['asks']):
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
        balances_formatted = [{'cy': cy,
                               'free': float(data['available']),
                               'used': float(data['hold']),
                               'info': data}]
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
        symbol = self.convert_symbol(d['symbol'],0)
        moid = d['makerOrderId']
        toid = d['takerOrderId']
        order = None
        for oid in (moid,toid):
            try: order = self.orders[oid]
            except KeyError: pass
            else: break
        
        is_subbed_to_trades = self.is_subscribed_to({'_': 'trades', 'symbol': symbol})
        
        if order is None and not is_subbed_to_trades:
            return
        
        #print('myMatch: {}'.format(r['data']))
        takerOrMaker = 'taker' if order['id'] == toid else 'maker'
        ts = int(d['time'][:-6])
        amount = float(d['size'])
        price = float(d['price'])
        side = d['side']
        
        if order is not None:
            self.add_fill(id=d['tradeId'], symbol=symbol, side=side, price=price, amount=amount,
                          takerOrMaker=takerOrMaker, timestamp=ts, order=order['id'], params={'info': d})
        
        if is_subbed_to_trades:
            e = self.api.trade_entry(symbol=symbol, timestamp=ts, id=d['tradeId'],
                                     price=price, amount=amount, side=side, info=d)
            self.update_trades([{'symbol': symbol, 'trades': [e]}])
        
        
    async def create_connection_url(self):
        auth = bool(self.apiKey)
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
                "KC-API-PASSPHRASE": self.auth_info['password'],
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
        topic = self.channel_ids[channel]
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
    
    
    def ping(self):
        return {
            "id": str(ctime_ms()),
            "type": "ping",
        }

