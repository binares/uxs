from copy import deepcopy
from dateutil.parser import parse as parsedate
import datetime
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import (ExchangeSocket, ExchangeSocketError)
from uxs.fintls.basics import as_direction

from fons.crypto import nonce as _nonce, sign
from fons.time import timestamp_ms
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

    
class hitbtc(ExchangeSocket): 
    exchange = 'hitbtc'
    url_components = {
        'ws': 'wss://api.hitbtc.com/api/2/ws',
    }
    auth_defaults = {
        'takes_input': False,
        'each_time': False,
        'send_separately': True,
    }
    channels = {
        'orderbook': {
            'auto_activate': False,
        },
        # fetch_balance, create_order, edit_order and cancel_order will
        # evoke socket authentication, if not already done so
    }
    has = {
        'all_tickers': False,
        'ticker': {
            'last': True, 'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False,  
            'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False, 
            'change': True, 'percentage': True, 'average': True, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True},
        'orderbook': True,
        #'ohlcv': '?',
        'trades': False, # TODO
        'account': {'balance': False, 'order': True, 'fill': True},
        'fetch_tickers': {
            'ask': True, 'askVolume': False, 'average': True, 'baseVolume': True, 'bid': True, 'bidVolume': False,
            'change': True, 'close': True, 'datetime': True, 'high': True, 'last': True, 'low': True, 'open': True,
            'percentage': True, 'previousClose': False, 'quoteVolume': True, 'symbol': True, 'timestamp': True,
            'vwap': True},
        'fetch_ticker': True,
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'asks': True, 'bids': True, 'datetime': False, 'nonce': False, 'timestamp': False},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'fetch_balance': {'ws': True, 'free': True, 'used': True, 'total': True},
        'fetch_my_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': True, 'id': True, 'order': True,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'create_order': {'ws': True},
        'cancel_order': {'ws': True},
        'edit_order': {}, # 'ws': True  # TODO
        'fetch_order': {
            'amount': True, 'average': False, 'clientOrderId': True, 'cost': True, 'datetime': True,
            'fee': False, 'filled': True, 'id': True, 'lastTradeTimestamp': True, 'price': True,
            'remaining': True, 'side': True, 'status': True, 'symbol': True, 'timestamp': True,
            'trades': False, 'type': True},
        'fetch_open_orders': True,
        'fetch_closed_orders': True,
    }
    has['fetch_ticker'] = has['fetch_tickers'].copy()
    has['fetch_open_orders'] = has['fetch_order'].copy()
    has['fetch_closed_orders'] = has['fetch_order'].copy()
    has['create_order'].update(has['fetch_order']) # for ccxt create
    has['edit_order'].update(has['fetch_order']) # for ccxt edit
    connection_defaults = {
        'subscription_push_rate_limit': 0.12,
        'max_subscriptions': 95,
        'rate_limit': (1, 0.12),
    }
    message = {'id': {'key': 'id'}}
    ob = {
        'force_create': None,
        'receives_snapshot': True,
    }
    order = {
        'update_remaining_on_fill': False,
        'update_filled_on_fill': False,
        'update_payout_on_fill': True,
    }
    symbol = {
        'quote_ids': ['BTC','ETH','DAI','TUSD','GUSD','USDC',
                      'USDT','USD','EURS','EOS','EOSDT','PAX'],
        'sep': '',
    }
    
    
    def handle(self, R):
        """:type r: Response"""
        r = R.data
        #print(r)
        if 'method' in r:
            method = r['method']
            if method in ('snapshotOrderbook','updateOrderbook'):
                self.on_orderbook2(r)
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
        if 'error' not in r:
            return
        e = r['error']
        d = {x: self.api.safe_string(e,x,'') for x in ('message','description','code')}
        msg = '{message}; {description}; code: {code}'.format(**d)
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
        d = r.copy()
        d['info'] = r
        d['baseVolume'] = d.pop('volume')
        d['datetime'] = d.pop('timestamp')
        d['symbol'] = self.convert_symbol(d['symbol'], 0)
        e = self.api.ticker_entry(**d)
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
            logger.debug('Reloading orderbook {} due to unsynced nonce ({}!={}+1)'.format(
                ob['symbol'], ob['nonce'], nonce_0))
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
        balances_formatted = [
            {
                'cy': self.convert_cy(x['currency'],0),
                'free': float(x['available']),
                'used': float(x['reserved']),
                'info': x,
            } for x in result
        ]
        self.update_balances(balances_formatted)
    
    
    def on_order(self, rr):
        tlogger.debug(rr)
        #rr = r['result']
        id = rr['clientOrderId']
        status = rr['status']
        rtype = rr['reportType']
        o = None
        try: o = self.orders[id]
        except KeyError: pass
        symbol = self.convert_symbol(rr['symbol'],0)
        side = rr['side']
        amount = float(rr['quantity'])
        filled = float(rr['cumQuantity'])
        remaining = amount-filled if status not in ('canceled','filled','suspended','expired') else 0
        if o is None:
            #(status == "new")
            #"2017-10-20T12:29:43.166Z"
            dto = parsedate(rr['createdAt']).replace(tzinfo=None)
            ts = timestamp_ms(dto)
            price = float(rr['price'])
            self.add_order(id=id, symbol=symbol, side=side, price=price, amount=amount, timestamp=ts,
                           remaining=remaining, filled=filled, params={'info': rr})
        else:
            # Can the server send "canceled"/replaced message twice, in response
            # to both cancelOrder/cancelReplaceOrder and subscribeReports?
            # Worse yet, the "expired" may arrive sooner than update,
            # thus have to check that remaining is smaller than previous, and filled larger
            remaining = min(remaining, o['remaining'])
            filled = max(filled, o['filled'])
            if status in ('canceled','filled','suspended','expired'):
                self.update_order(id, 0, filled, params={'info': rr})
                #logger.error('{} - received unregistered order {}'.format(self.name,id))
            elif status in ('partiallyFilled','new'):
                self.update_order(id, remaining, filled, params={'info': rr})
            else:
                #order 
                logger2.info('{} - received unknown order status. r: {}'.format(self.name, rr))
        
        if rtype == 'trade':
            tid = rr['tradeId']
            tprice = float(rr['tradePrice'])
            tamount = float(rr['tradeQuantity'])
            tdto = parsedate(rr['updatedAt']).replace(tzinfo=None)
            tts = timestamp_ms(tdto)
            #fee is negative when we are paid the rebate
            #NB! fee is always in payout currency
            fee = float(rr['tradeFee'])
            self.add_fill(id=tid, symbol=symbol, side=side, price=tprice, amount=tamount,
                          timestamp=tts, order=id, fee=fee, params={'info': rr.copy()})
    
    
    # TODO: fetch_open_orders, edit_order
    # https://api.hitbtc.com/#socket-trading
    
    
    async def fetch_balance(self, params={}):
        if self.is_active():
            r = (await self.send({'_': 'fetch_balance'}, True)).data
            self.check_errors(r)
            return deepcopy(self.balances)
        else:
            return await super().fetch_balance(params)
    
    
    def encode(self, request, sub=None):
        p = request.params
        channel = request.channel
        unsub = sub is False
        symbol = None if 'symbol' not in p else self.convert_symbol(p['symbol'], 1)
        p_out = {}
        
        channel_ids = {
            'account': 'subscribeReports',
            'orderbook': 'subscribeOrderbook',
            'ticker': 'subscribeTicker',
            'trades': 'subscribeTrades',
            'fetch_balance': 'getTradingBalance',
            'create_order': 'newOrder',
            'cancel_order': 'cancelOrder',
        }
        
        method = channel_ids[channel]
        if unsub:
            method = 'un' + method
        
        if symbol is not None and channel != 'cancel_order':
            p_out['symbol'] = symbol
        
        if channel == 'create_order':
            order_id = _nonce(32, uppers=False)        
            p_out.update({
                'clientOrderId': order_id,
                'side': p['side'],
                'type': p['type'],
                'quantity': str(p['amount']),
              })
            if type in ('limit','stopLimit'):
                p_out['price'] = str(p['price'])
        
        if channel == 'cancel_order':
            p_out['clientOrderId'] = p['id']
        
        uid = None
        if channel in ('account','orderbook','ticker','trades'):
            uid = request.uid
        message_id = self.ip.generate_message_id(uid, unsub)
        
        req = {
            'method': method,
            'params': p_out,
            'id': message_id
        }
        
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