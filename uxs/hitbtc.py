import asyncio
import datetime
dt = datetime.datetime
td = datetime.timedelta
from dateutil.parser import parse as parsedate

from uxs.base.socket import (ExchangeSocket, ExchangeSocketError)
from uxs.fintls.basics import as_direction

from fons.crypto import nonce as _nonce, sign
from fons.time import timestamp_ms
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

    
class hitbtc(ExchangeSocket): 
    exchange = 'hitbtc'
    auth_defaults = {
        'takes_input': False,
        'each_time': False,
        'send_separately': True,
    }
    channel_defaults = {
        'url': 'wss://api.hitbtc.com/api/2/ws',
    }
    channels = {
        'orderbook': {
            'auto_activate': False,
        },
        'fetch_balance': {
            'type': 'fetch',
            'required': [],
            'is_private': True,
        },
        'newOrder': {
            'type': 'post',
            'required': ['symbol','side','price','quantity'],
            'is_private': True,
        },
        'cancelOrder': {
            'type': 'post',
            'required': ['clientOrderId'],
            'is_private': True,
        },
    }
    has = {
        'balance': {'_': False, 'delta': False},
        'all_tickers': False,
        'ticker': {
            'last': True, 'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False,  
            'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False, 
            'change': True, 'percentage': True, 'average': True, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True},
        'orderbook': True,
        'account': {'balance': False, 'order': True, 'match': True},
        'fetch_tickers': {
            'last': True, 'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False,  
            'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False, 
            'change': True, 'percentage': True, 'average': True, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True},
        'fetch_ticker': True,
        'fetch_order_book': True,
        'fetch_balance': {'ws': True},
        #'feed': {'balance':True}
    }
    connection_defaults = {
        'subscription_push_rate_limit': 0.12,
        'max_subscriptions': 95,
        'rate_limit': (1, 0.12),
    }
    has['fetch_ticker'] = has['fetch_tickers'].copy()
    message_id_keyword = 'id'
    order = {
        'update_left_on_trade': False,
        'update_executed_on_trade': False,
        'update_payout_on_trade': True,
    }
    
    def handle(self, R):
        """:type r: Response"""
        r = R.data
        #print(r)
        if 'method' in r:
            method = r['method']
            if method in ('snapshotOrderbook','updateOrderbook'):
                self.on_orderbook(r)
            elif method == 'activeOrders':
                for item in r['params']: 
                    self.on_order(item)
            elif method == 'report':
                self.on_order(r['params'])
            elif method == 'ticker':
                self.on_ticker(r['params'])
        elif 'result' in r:
            res = r['result']
            is_list = isinstance(res,list)
            #reports_subbed = self.has_subscription({'_':'account'})
            if is_list and len(res):
                item_0 = res[0]
                #isinstance(item,dict) and 
                if 'available' in item_0:
                    self.on_balance(r)
                elif 'clientOrderId' in item_0:
                    for item in res:
                        self.on_order(item)
            elif is_list: pass
            elif isinstance(res,bool):
                self.sh.handle_subscription_ack(r['id'])
            elif not isinstance(res,dict):
                logger2.error('{} - unknown response {}'.format(self.name,r))
            elif 'clientOrderId' in res:
                self.on_order(res)
        elif 'error' in r:
            logger2.error(r)
        else:
            logger2.error('{} - unknown response: {}'.format(self.name,r))
            
    def check_errors(self, r):
        try: re = r['error']
        except KeyError: return
        ss = self.api.safe_string
        dict = {x: ss(re,x,'') for x in ('message','description','code')}
        msg = '{message}; {description}; code: {code}'.format(**dict)
        raise ExchangeSocketError(msg)
    
    def on_ticker(self, r):
        """{
            'ask': '10159.00', 
            'bid': '10158.00', 
            'last': '10160.88', 
            'open': '10655.13', 
            'low': '10072.45', 
            'high': '10848.16', 
            'volume': '17367.86802', 
            'volumeQuote': '182588890.8213478', 
            'timestamp': '2019-08-21T11:14:02.123Z', 
            'symbol': 'BTCUSD'
        }"""
        r = r.copy()
        r['baseVolume'] = r.pop('volume')
        r['datetime'] = r.pop('timestamp')
        r['symbol'] = self.convert_symbol(r['symbol'], 0)
        e = self.api.ticker_entry(**r)
        self.update_tickers([e])

    def on_orderbook(self, r):
        ob = self._prepare_orderbook(r)
        nonce_0 = self.orderbooks.get(ob['symbol'],{}).get('nonce')
        if not self.is_subscribed_to(('orderbook',ob['symbol']), active=None):
            "Do not update the data"
        elif r['method'] == 'snapshotOrderbook':
            #print({x:y for x,y in ob.items() if x in ('symbol','nonce')})
            self.create_orderbooks([ob])
            self.orderbook_maintainer._change_status(ob['symbol'], 1)
        elif ob['nonce'] == nonce_0+1:
            #No, this was false alarm.
            #if ob['symbol'] == 'Bitcoin Cash/USDT':
            #    ob = self._deal_with_BitcoinCashUSDT(ob)"""
            #print(ob)
            self.update_orderbooks([ob])
        #For some reason Bitcoin Cash/USDT also receives updates for BCH/USDT,
        # in that case the sequence is different, and we ignore the update
        elif ob['symbol'] in ('Bitcoin Cash/USDT','Bitcoin Cash/BTC') and (
            ob['nonce'] > nonce_0+1000 or ob['nonce']<nonce_0-1000):
            #its bid book receives updates from *ask* of BCH/USDT and BSV/USDT
            pass
        else:
            print(ob['symbol'],nonce_0,ob['nonce'])
            logger.debug('Reloading orderbook {} due to unsynced nonce'.format(ob['symbol']))
            #self.change_subscription_state({'_': 'orderbook','symbol':ob['symbol']}, 0)
            self.orderbook_maintainer._change_status(ob['symbol'], 0)
            self.unsubscribe_to_orderbook(ob['symbol'])
            self.subscribe_to_orderbook(ob['symbol'])
            
    def on_orderbook2(self, r):
        ob = self._prepare_orderbook(r)
        if r['method'] == 'snapshotOrderbook':
            self.orderbook_maintainer.send_orderbook(ob)
        else:
            self.orderbook_maintainer.send_update(ob)
            
    def _prepare_orderbook(self, r):
        decode = lambda branch: [[float(x['price']),float(x['size'])] for x in branch]
        p = r['params']
        ob = {}
        ob['symbol'] = self.convert_symbol(p['symbol'],0)
        ob['bids'] = decode(p['bid'])
        ob['asks'] = decode(p['ask'])
        ob['nonce'] = p['sequence']
        return ob
    
    def _deal_with_BitcoinCashUSDT(self, ob):
        try: bid0 = self.orderbooks['Bitcoin Cash/USDT']['bids'][0][0]
        except IndexError: return ob
        try: min_price = min(self.orderbooks['BCH/USDT']['asks'][0][0],
                             self.orderbooks['BSV/USDT']['asks'][0][0]) * 1.03
        except (KeyError,IndexError):
            min_price = bid0*0.95
        ob['bids'] = [x for x in ob['bid'] if x[0]>min_price]
        return ob
    
    def on_balance(self, r):
        #print('Updating balances')
        result = r['result']
        balances_formatted = [(
                self.convert_cy(x['currency'],0),
                float(x['available']),
                float(x['reserved'])
            ) for x in result
        ]
        self.update_balances(balances_formatted)
        
    def on_order(self, rr):
        print(rr)
        #rr = r['result']
        id = rr['clientOrderId']
        status = rr['status']
        rtype = rr['reportType']
        o = None
        try: o = self.get_order(id)
        except ValueError: pass
        symbol = self.convert_symbol(rr['symbol'],0)
        side = rr['side']
        qty = float(rr['quantity'])
        executed = float(rr['cumQuantity'])
        left = qty-executed if status not in ('canceled','filled','suspended','expired') else 0
        if o is None:
            #(status == "new")
            #"2017-10-20T12:29:43.166Z"
            dto = parsedate(rr['createdAt']).replace(tzinfo=None)
            ts = timestamp_ms(dto)
            price = float(rr['price'])
            self.add_order(id,symbol,side,price,qty,ts,left,executed)
        else:
            #Can the server send "canceled"/replaced message twice, in response
            # to both cancelOrder/cancelReplaceOrder and subscribeReports?
            #Worse yet, the "expired" may arrive sooner than update,
            # thus have to check that left is smaller than previous, and executed larger
            left = min(left,o['left'])
            executed = max(executed,o['executed'])
            if status in ('canceled','filled','suspended','expired'):
                self.update_order(id,0,executed)
                #logger.error('{} - received unregistered order {}'.format(self.name,id))
            elif status in ('partiallyFilled','new'):
                self.update_order(id,left,executed)
            else:
                #order 
                logger2.info('{} - received unknown order status. r: {}'.format(self.name,rr))
        
        if rtype == 'trade':
            tid = rr['tradeId']
            tprice = float(rr['tradePrice'])
            tqty = float(rr['tradeQuantity'])
            tdto = parsedate(rr['updatedAt']).replace(tzinfo=None)
            tts = timestamp_ms(tdto)
            #fee is negative when we are paid the rebate
            #NB! fee is always in payout currency
            fee = float(rr['tradeFee'])
            self.add_trade(tid,symbol,side,tprice,tqty,None,tts,id,fee=fee)
            
    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        #side: buy/sell
        direction = as_direction(side)
        side = ['sell','buy'][direction]
        order_id = _nonce(32,uppers=False)        
        out = dict(params,**{
            "_": "newOrder",
            "clientOrderId": order_id,
            "symbol": self.convert_symbol(symbol,1),
            "side": side,
            "type": type,
            "quantity": str(amount)
          })
        if type in ('limit','stopLimit'):
            out['price'] = str(price)
        r = (await self.send(out,True)).data
        self.check_errors(r)
        o = self.get_order(order_id)
        return o #{'id': order_id, 'price': o['rate'], 'amount': o['qty']}
                        
    async def cancel_order(self, id, symbol=None, params={}):
        out = dict(params,**{
            "_": "cancelOrder",
            "clientOrderId": id,
        })
        r = (await self.send(out,True)).data
        self.check_errors(r)
        o = self.get_order(id,'closed')
        return o #{'id': id, 'price': o['rate'], 'amount'}
    
    async def fetch_balance(self):
        r = (await self.send({'_':'fetch_balance'},True)).data
        self.check_errors(r)
        return self.balances
    
    def encode(self, request, sub=None):
        config = request.params
        channel = config['_']
        unsub = sub is False
        params = {}
        if channel == 'account':
            method = 'subscribeReports' if not unsub else 'unsubscribeReports'
        elif channel in ('orderbook','ticker','market'):
            method = {'orderbook': 'subscribeOrderbook',
                      'ticker': 'subscribeTicker',
                      'market': 'subscribeTrades'}[channel]
            if unsub:
                method = 'un'+method
            #symbol=self.comma_separate(config['symbol'],self.convert_symbol)
            symbol = self.convert_symbol(config['symbol'],1)
            params = {'symbol': symbol}
        elif channel == 'fetch_balance':
            method = 'getTradingBalance'
        else:
            method = channel
            params = {x:y for x,y in config.items() if x!='_'}
            
        uid = None
        if channel in ('account','orderbook','ticker','market'):
            #try: uid = self.sh.get_subscription_uid(config)
            #except KeyError: pass
            uid = request.uid
        message_id = self.ip.generate_message_id(uid,unsub)
        
        req = {'method': method,
               'params': params,
               'id': message_id}
        return (req, message_id)

    def sign(self, out=None):
        nonce = _nonce(15)
        sig = sign(self.secret,nonce,'sha256')
        return {
          "method": "login",
          "params": {
            "algo": "HS256",
            "pKey": self.apiKey,
            "nonce": nonce,
            "signature": sig
          }
        }
        
    def convert_symbol(self, symbol, direction=1):
        #0: ex to ccxt #1: ccxt to ex
        try: return super().convert_symbol(symbol, direction)
        except KeyError: pass
        
        if not direction:
            quotes = ['BTC','ETH','DAI','TUSD','GUSD','USDC','USDT',
                      'USD','EURS','EOS','EOSDT','PAX']
            ln = next(len(q) for q in quotes if symbol.endswith(q))
            return '/'.join([self.convert_cy(x,0) for x in (symbol[:-ln],symbol[-ln:])])
        else:
            return ''.join([self.convert_cy(x,1) for x in symbol.split('/')])
