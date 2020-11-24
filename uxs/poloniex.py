import os
from dateutil.parser import parse as parsedate
import datetime
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket

from fons.time import timestamp_ms
from fons.crypto import nonce_ms, sign
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

CURRENCY_PAIR_IDS_PATH = os.path.join(os.path.dirname(__file__), '_data', 'poloniex_currency_pair_ids.txt')
CURRENCY_IDS_PATH = os.path.join(os.path.dirname(__file__), '_data', 'poloniex_currency_ids.txt')

currencies = {}
currency_pairs = {}
currency_pairs_ccxt = {}

def _decode_ids(txt):
    pairs = {}
    for line in txt.split('\n'):
        items = line.strip().split('\t')
        if len(items) < 2: continue
        id,symbol = items[0],items[-1]
        if not symbol or not id: continue
        pairs[int(id)] = symbol
    return pairs

def read_currency_pairs():
    global currency_pairs
    with open(CURRENCY_PAIR_IDS_PATH, encoding='utf-8') as f:
        currency_pairs = _decode_ids(f.read())

def read_currencies():
    global currencies
    with open(CURRENCY_IDS_PATH, encoding='utf-8') as f:
        currencies = _decode_ids(f.read())
        
read_currency_pairs()
read_currencies()
currency_pairs_ccxt = {id: '/'.join(symbol.split('_')[::-1]) for id,symbol in currency_pairs.items()}
    
    
class poloniex(ExchangeSocket): 
    exchange = 'poloniex'
    url_components = {
        'ws': 'wss://api2.poloniex.com',
    }
    auth_defaults = {
        'takes_input': True,
        'each_time': True,
        'send_separately': False,
    }
    channel_defaults = {
        'unsub_option': True,
        'merge_option': False,
    }
    has = {
        'all_tickers': {
            'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False, 'last': True,
            'high': True, 'low': True, 'open': False, 'close': True, 'previousClose': False,
            'change': True, 'percentage': True, 'average': False, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True, 'active': True},
        'ticker': False,
        'orderbook': True,
        'account': {'balance': True, 'order': True, 'fill': True},
        'fetch_tickers': True,
        'fetch_ticker': {
            'ask': True, 'askVolume': False, 'average': True, 'baseVolume': True, 'bid': True, 'bidVolume': False,
            'change': True, 'close': True, 'datetime': True, 'high': True, 'last': True, 'low': True, 'open': True,
            'percentage': True, 'previousClose': False, 'quoteVolume': True, 'symbol': True, 'timestamp': True,
            'vwap': False},
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'asks': True, 'bids': True, 'datetime': False, 'nonce': True, 'timestamp': False},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': True, 'order': True, 'price': True,
            'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'fetch_my_trades': {
            'symbolRequired': False,
            'amount': True, 'cost': True, 'datetime': True, 'fee': True, 'id': True, 'order': True,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        # fetchOrder, fetchOrders and fetchClosedOrders don't actually return closed orders, but are
        # internally calculated by ccxt (don't include them because orders could be closed with wrong final
        # `filled` value)
        'fetch_order': {'ccxt': False},
        'fetch_orders': {'ccxt': False},
        'fetch_open_orders': {
            'symbolRequired': False,
            'amount': True, 'average': False, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': False,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': False, 'type': True},
        'fetch_closed_orders': {'ccxt': False},
        'create_order': {
            'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': True,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': True, 'type': True},
        # parsing error in ccxt (KeyError: 'date'); and trades resulting from edit are left unparsed;
        # also new order has new id, which may not be handled well by uxs
        'edit_order': {'ccxt': False},
    }
    has['fetch_tickers'] = has['fetch_ticker'].copy()
    connection_defaults = {
        'rate_limit': (1, 0.12),
    }
    max_subscriptions_per_connection = 95
    subscription_push_rate_limit = 0.12
    ob = {
        'force_create': None,
        'receives_snapshot': True,
    }
    order = {
        'update_filled_on_fill': True,
        'update_payout_on_fill': True,
        'update_remaining_on_fill': True,
    }
    symbol = {
        'quote_ids': ['BTC', 'USDT', 'ETH', 'USDC', 'XMR', 'LTC'],
        'sep': '_',
        'startswith': 'quote',
    }
    
    
    def handle(self, response):
        r = response.data
        #print(r)
        if isinstance(r, dict):
            self.log_error(r)
        elif isinstance(r[0], str):
            self.log('ack: {}'.format(r))
        elif r[0] == 1010:
            self.on_idle(r)
        elif r[0] == 1000:
            self.on_account(r)
        elif r[0] == 1002:
            self.on_ticker(r)
        else:
            self.on_orderbook(r)
    
    def on_idle(self, r):
        pass
    
    def on_ticker(self, r):
        #first response: 
        #[<id>, 1]
        #following responses:
        #[<id>, null, [
        # <currency pair id>, "<last trade price>", "<lowest ask>", "<highest bid>", "<percent change in last 24 hours>",
        # "<base currency volume in last 24 hours>", "<quote currency volume in last 24 hours>", <is frozen>, 
        #"<highest trade price in last 24 hours>", "<lowest trade price in last 24 hours>" ], ... ]
        data = []
        now = dt.utcnow()
        now_str = '{}Z'.format(now.isoformat()[:23])
        now_ms = timestamp_ms(now)
        
        for pair_data in r[2:]:
            pair_id = pair_data[0]
            symbol = self.id_to_symbol(pair_id)
            last_trade_price, lowest_ask, highest_bid = [float(x) for x in pair_data[1:4]]
            percent_change_24, base_cy_vol_24, quote_cy_vol_24 = [float(x) for x in pair_data[4:7]]
            is_frozen = pair_data[7]
            H_24,L_24 = [float(x) for x in pair_data[8:]]
            change = last_trade_price*(percent_change_24/100)
            vwap = quote_cy_vol_24/base_cy_vol_24 if base_cy_vol_24 else last_trade_price
            
            data.append({'symbol': symbol, 'timestamp': now_ms, 'datetime': now_str,
                'last': last_trade_price, 'high': H_24, 'low': L_24, 'open': None, 'close': None,
                'bid': highest_bid, 'bidVolume': None, 'ask': lowest_ask, 'askVolume': None,
                'baseVolume': base_cy_vol_24, 'quoteVolume': quote_cy_vol_24,
                'previousClose': None, 'change': change, 'percentage': percent_change_24, 
                'average': None, 'vwap': vwap,
                'active': not is_frozen})
            
        self.update_tickers(data)
        self.change_subscription_state({'_': 'all_tickers'}, 1)
            
    def on_orderbook(self, r):
        #print(r)
        symbol = self.id_to_symbol(r[0])
        try: data = r[2]
        except IndexError: return
        new_obs = {'symbol': symbol, 'bids': [], 'asks': []}
        upd_obs = {'symbol': symbol, 'bids': [], 'asks': []}
        methods = dict.fromkeys(('create','update'),False)
                
        for item in data:
            if item[0] == 'i':
                ob = item[1]
                _symbol2_poloformat = ob['currencyPair']
                new_obs['bids'] = [[float(price),float(qty)] for price,qty in ob['orderBook'][1].items()]
                new_obs['asks'] = [[float(price),float(qty)] for price,qty in ob['orderBook'][0].items()]
                methods['create'] = True
            elif item[0] == 'o':
                side = 'bids' if item[1] == 1 else 'asks'
                price = float(item[2])
                qty = float(item[3])
                upd_obs[side].append([price,qty])
                methods['update'] = True
            elif item[0] == 't':
                pass
            else:
                self._log('unknown orderbook item: {}'.format(item), 'ERROR', logger2)
                
        if methods['create']:
            self.create_orderbooks([new_obs])
            self.change_subscription_state({'_': 'orderbook', 'symbol': symbol}, 1)
        if methods['update']:
            self.update_orderbooks([upd_obs])
     
    def on_account(self, r):
        #[1000, '', [..data..]]
        #[1000, '', [['o', 51973790409, '0.00000000', 'c'], ['b', 299, 'e', '13.43481824']]]
        tlogger.debug(r)
        try: data = r[2]
        except IndexError: return #[1000, 1]
        tree = {k:[] for k in ('b','p','n','o','t','m')}
        
        for item in data:
            a_type = item[0]
            try: tree[a_type].append(item)
            except KeyError:
                self._log('unknown account item: {}'.format(item), 'ERROR', logger2)
                
        if tree['b']:
            self.on_balances(tree['b'])
        for item in tree['p']:
            self.on_pending_order(item)
        for item in tree['n']:
            self.on_new_order(item)
        for item in tree['o']:
            self.on_order_update(item)
        for item in tree['t']:
            self.on_fill(item)
        for item in tree['m']:
            self.on_margin_position_update(item)
        
        self.change_subscription_state({'_':'account'}, 1)
    
    def on_balances(self, balances):
        #["b",292,"e","-0.01185766"]
        balances = [[self.id_to_cy(x[1]), float(x[3])] for x in balances if x[2] == 'e']
        self.update_balances_delta(balances)
    
    def on_pending_order(self, item):
        #['p', 750510274182, 121, '11401.22123605', '0.01941609', '0', None]
        #["p", <order number>, <currency pair id>, "<rate>", "<amount>", "<order type>", "<clientOrderId>"]
        pass
    
    def on_new_order(self, item):
        #["n", 148, 6083059, 1, "0.03000000", "2.00000000", "2018-09-08 04:54:09", "2.00000000", None]
        #["n", <currency pair id>, <order number>, <order type>, "<price>", "<amount>", "<date>", "<original amount ordered>" "<clientOrderId>"]
        _, pair_id, oid, otype, price, remaining, tstr, amount, clientOrderId = item[:9]
        #Convert to string because api.create_order returns id as string
        oid = str(oid)
        symbol = self.id_to_symbol(pair_id)
        side = 'buy' if otype == 1 else 'sell'
        price = float(price)
        amount = float(amount)
        dto = parsedate(tstr)
        if dto.tzinfo is not None:
            logger2.error('POLONIEX HAS CHANGED THEIR DATE FORMAT: {}, {}'.format(tstr, dto))
        ts = timestamp_ms(dto.replace(tzinfo=None))
        remaining = float(remaining)
        #print('on_order:',oid,symbol,side,price,amount,ts,remaining,filled,payout)
        try: self.orders[oid]
        except KeyError:
            #set filled to 0 because filled (and payout) is updated by trades
            self.add_order(id=oid, symbol=symbol, side=side, price=price, amount=amount,
                           timestamp=ts, remaining=remaining, filled=0)
        else:
            self.update_order(id=oid, remaining=remaining)
    
    def on_order_update(self, item):
        #["o", 12345, "1.50000000"]
        #["o", 12345, "0.00000000", "c"]
        #["o", <order number>, "<new amount>", "<order type>", "<clientOrderId>"]
        _, oid, remaining = item[:3]
        self.update_order(id=str(oid), remaining=float(remaining))
    
    def on_fill(self, item):
        #["t", 12345, "0.03000000", "0.50000000", "0.00250000", 0, 6083059, "0.00000375", "2018-09-08 05:54:09"]
        #['t', 9394539, '0.00057427', '0.00000476', '0.00000000', 0, 274547887461]
        #["t", <trade ID>, "<rate>", "<amount>", "<fee multiplier>", <funding type>, <order number>, <total fee>, <date>, "<clientOrderId>", "<trade total>"]
        #funding_type: 0 (exchange wallet), 1 (borrowed funds), 2 (margin funds), or 3 (lending funds).
        _, tid, price, amount, fee_rate, funding_type, oid = item[:7]
        tid, oid = str(tid), str(oid)
        price = float(price)
        amount = float(amount)
        fee_rate = float(fee_rate)
        total_fee = float(item[7]) if len(item) > 7 else None
        dto = parsedate(item[8]) if len(item) > 8 else dt.utcnow()
        if dto.tzinfo is not None:
            logger2.error('POLONIEX HAS CHANGED THEIR DATE FORMAT: {}, {}'.format(item[8], dto))
        ts = timestamp_ms(dto.replace(tzinfo=None))
        self.add_fill(id=tid, symbol=None, side=None, price=price, amount=amount,
                      fee_rate=fee_rate, timestamp=ts, order=oid)
    
    def on_margin_position_update(self, item):
        #["m", <order number>, "<currency>", "<amount>", "<clientOrderId>"]
        pass
    
    def parse_ccxt_order(self, r, *args):
        """{
        'info': {'timestamp': 1541421971000, 'status': 'open', 'type': 'limit', 
                'side': 'buy', 'price': 0.1, 'amount': 0.02, 'orderNumber': '123456789010', 
                'resultingTrades': [{'amount': '0.02000000', 'date': '2018-11-05 12:46:11', 
                'rate': '0.02341327', 'total': '0.00046826', 'tradeID': '12345678', 'type': 'buy', 
                'takerAdjustment': '0.01998000'}], 'fee': '0.00100000', 'currencyPair': 'BTC_ETH'}, 
        'id': '123456789010', 'timestamp': 1541421971000, 'datetime': '2018-11-05T12:46:11.000Z',
        'lastTradeTimestamp': None, 'status': 'open', 'symbol': 'ETH/BTC', 'type': 'limit', 'side': 'buy', 
        'price': 0.1, 'cost': 0.0, 'amount': 0.02, 'filled': 0.0, 'remaining': 0.02, 
        'trades': [{
            'id': None, 
            'info': {'amount': '0.02000000', 'date': '2018-11-05 12:46:11', 
                    'rate': '0.02341327', 'total': '0.00046826', 'tradeID': '12345678', 
                    'type': 'buy', 'takerAdjustment': '0.01998000'}, 
            'timestamp': 1541421971000, 'datetime': '2018-11-05T12:46:11.000Z', 'symbol': 'ETH/BTC', 
            'order': None, 'type': 'limit', 'side': 'buy', 'takerOrMaker': None, 'price': 0.02341327, 
            'amount': 0.02, 'cost': 0.00046826, 'fee': None}], 
        'fee': None}"""
        #Note: 'remaining' == 'amount', neither is 'filled' always accurate (check ccxt.poloniex.parse_order)
        filled = sum(t['amount'] for t in r.get('trades',[]))
        parsed = {
            #'order': {#'remaining': max(0, r['amount']-filled), 
            #          'filled': 0}, #'filled' is calculated from trades
            'fills': [
                    {'id': str(t['info']['tradeID']), # websocket only sends fill's 'tradeID', not 'globalTradeID'
                     'symbol': t['symbol'], 
                     'side': t['side'],
                     'price': t['price'],
                     'amount': t['amount'], 
                     #it may be imprecise but round should eliminate that
                     'fee_rate': round(1 - float(t['info']['takerAdjustment'])
                                / float(t['info']['amount' if t['side']=='buy' else 'total']), 5) 
                              if 'takerAdjustment' in t['info'] else round(float(r['info']['fee']),5),
                     'timestamp': t['timestamp'],
                     'datetime': t['datetime'],
                     'cost': t['cost'],
                     'order': r['id'], 
                     'payout': float(t['info']['takerAdjustment']) if 'takerAdjustment' in t['info'] else None,
                     }
                for t in r.get('trades',[])
            ]
        }
        return parsed
    
    def encode(self, rq, sub=None):
        p = rq.params
        channel = rq.channel
        
        command = 'subscribe' if sub else 'unsubscribe'
        if channel == 'account':
            _channel = 1000
        elif channel == 'all_tickers':
            _channel = 1002
        elif channel == 'orderbook':
            _channel = self.convert_symbol(p['symbol'], 1)
        else:
            raise ValueError(channel)
            
        return ({'channel': _channel, 'command': command}, None)
    
    def sign(self, out):
        out = out.copy()
        out['key'] = self.apiKey
        out['payload'] = "nonce={}".format(nonce_ms())
        out['sign'] = sign(self.secret, out['payload'], 'sha512')
        return out
    
    def id_to_cy(self, id):
        try: return next(x for x,y in self.api.currencies.items() if y['info'].get('id')==id)
        except (StopIteration,AttributeError):
            return self.convert_cy(currencies[id],0)
        
    def id_to_symbol(self, id):
        try: return next(x for x,y in self.api.markets.items() if y['info'].get('id')==id)
        except (StopIteration, AttributeError):
            return self.convert_symbol(
                '_'.join(currency_pairs_ccxt[id].split('/')[::-1]), 0)
    
    
def _to_polo_symbol(symbol):
    if isinstance(symbol,int): return symbol
    elif '/' not in symbol: return symbol
    else: return '_'.join(symbol.split('/')[::-1])
    
   
#first response: [1000,1]
#idle: [1010]
#after selling bch to btc:
"""[1000,"",[
["b",292,"e","-0.01185766"],
["b",28,"e","0.00100193"],
["t",5512721,"0.08466503","0.00595677","0.00200000",0,108083735262],
["t",5512722,"0.08466503","0.00590089","0.00200000",0,108083735262]
]]"""
#after placing bch/btc order:
"""[1000,"",[["n",189,108086089905,1,"0.08000000","1.03155300","2018-11-10 11:41:42"],["b",28,"e","-0.08252424"]]]"""
#after cancelling the order:
"""[1000,"",[["o",108086089905,"0.00000000"],["b",28,"e","0.08252424"]]]"""
#on placing order and it getting partially filled:
"""[1000, '', [
['n', 204, 1264304634, 1, '0.00000568', '149.48718908', '2018-11-10 17:07:49'], 
['b', 28, 'e', '-0.00099999'], 
['b', 300, 'e', '26.51601065'], 
['t', 16816, '0.00000568', '26.56914894', '0.00200000', 0, 1264304634]]]"""
