import os
from dateutil.parser import parse as parsedate
import datetime
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket, ExchangeSocketError

from fons.time import timestamp_ms, pydt_from_ms
from fons.crypto import nonce_ms, sign
import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

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
    with open(os.path.dirname(__file__)+'\\_data\\poloniex_currency_pair_ids.txt') as f:
        currency_pairs = _decode_ids(f.read())
        
def read_currencies():
    global currencies
    with open(os.path.dirname(__file__)+'\\_data\\poloniex_currency_ids.txt') as f:
        currencies = _decode_ids(f.read())
        
read_currency_pairs()
read_currencies()
currency_pairs_ccxt = {id: '/'.join(symbol.split('_')[::-1]) for id,symbol in currency_pairs.items()}
    
    
class poloniex(ExchangeSocket): 
    exchange = 'poloniex'

    auth_defaults = {
        'takes_input': True,
        'each_time': True,
        'send_separately': False,
    }
    channel_defaults = {
        'url': 'wss://api2.poloniex.com',
        'unsub_option': True,
        'merge_option': False,
    }
    has = {
        'balance': {'delta': True},
        'ticker': False,
        'all_tickers': {'last': True, 'bid': True, 'ask': True, 'bidVolume': False, 'askVolume': False,
                        'high': True, 'low': True, 'open': False, 'close': True, 'previousClose': False,
                        'change': True, 'percentage': True, 'average': False, 'vwap': True,
                        'baseVolume': True, 'quoteVolume': True, 'active': True},
        'orderbook': True,
        'account': {'balance': True, 'order': True, 'match': True},
        'fetch_tickers': True,
        'fetch_ticker': True,
        'fetch_order_book': True,
        'fetch_balance': True,
    }
    connection_defaults = {
        'max_subscriptions': 95, #?
        'subscription_push_rate_limit': 0.12,
        'rate_limit': (1, 0.12),
    }
    ob = {
        'force_create': None,
    }
    order = {
        'update_executed_on_trade': True,
        'update_payout_on_trade': True,
        'update_left_on_trade': False,
    }
    
    
    def handle(self, response):
        r = response.data
        #print(r)
        if isinstance(r, dict):
            self.on_error(r)
        elif isinstance(r[0], str):
            logger.debug('{} - ack: {}'.format(self.name, r))
        elif r[0] == 1010:
            self.on_idle(r)
        elif r[0] == 1000:
            self.on_account(r)
        elif r[0] == 1002:
            self.on_ticker(r)
        else:
            self.on_orderbook(r)
    
    def on_error(self, r):
        logger2.error('{} - error occurred: {}'.format(self.name, r))
        
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
                new_obs['bids'] = [[float(rate),float(qty)] for rate,qty in ob['orderBook'][1].items()]
                new_obs['asks'] = [[float(rate),float(qty)] for rate,qty in ob['orderBook'][0].items()]
                methods['create'] = True
            elif item[0] == 'o':
                side = 'bids' if item[1] == 1 else 'asks'
                rate = float(item[2])
                qty = float(item[3])
                upd_obs[side].append([rate,qty])
                methods['update'] = True
            elif item[0] == 't':
                pass
            else:
                print('Unknown orderbook item: {}'.format(item))
                
        if methods['create']:
            self.create_orderbooks([new_obs])
            self.change_subscription_state({'_': 'orderbook', 'symbol': symbol}, 1)
        if methods['update']:
            self.update_orderbooks([upd_obs])
     
    def on_account(self, r):
        #[1000, '', [..data..]]
        #[1000, '', [['o', 51973790409, '0.00000000', 'c'], ['b', 299, 'e', '13.43481824']]]
        print(r)
        try: data = r[2]
        except IndexError: return #[1000, 1]
        tree = {k:[] for k in ('b','n','o','t')}
        
        for item in data:
            a_type = item[0]
            try: tree[a_type].append(item)
            except KeyError:
                print('Unknown account item: {}'.format(item))
                
        if tree['b']:
            self.on_balances(tree['b'])
        for item in tree['n']:
            self.on_new_order(item)
        for item in tree['o']:
            self.on_order_update(item)
        for item in tree['t']:
            self.on_trade(item)
            
        self.change_subscription_state({'_':'account'}, 1)
                
    def on_balances(self, balances):
        #["b",292,"e","-0.01185766"]
        balances = [[self.id_to_cy(x[1]), float(x[3])] for x in balances if x[2] == 'e']
        self.update_balances_delta(balances)
    
    def on_new_order(self, item):
        #["n", 148, 6083059, 1, "0.03000000", "2.00000000", "2018-09-08 04:54:09", "2.00000000"]
        #["n", <currency pair id>, <order number>, <order type>, "<rate>", "<amount>", "<date>", "<amount left?>"]
        _, pair_id, oid, otype, rate, left, tstr, qty = item
        #Convert to string because api.create_order returns id as string
        oid = str(oid)
        symbol = self.id_to_symbol(pair_id)
        side = 'buy' if otype == 1 else 'sell'
        rate = float(rate)
        qty = float(qty)
        dto = parsedate(tstr)
        if dto.tzinfo is not None:
            logger2.error('POLONIEX HAS CHANGED THEIR DATE FORMAT: {}, {}'.format(tstr, dto))
        ts = timestamp_ms(dto.replace(tzinfo=None))
        left = float(left)
        #print('on_order:',oid,symbol,side,rate,qty,ts,left,executed,payout)
        try: self.get_order(oid, 'open')
        except ValueError:
            #set executed to 0 because executed (and payout) is updated by trades
            self.add_order(oid, symbol, side, rate, qty, ts, left, 0)
        else:
            self.update_order(oid, left)
    
    def on_order_update(self, item):
        #["o", 12345, "1.50000000"]
        #["o", 12345, "0.00000000", "c"]
        _,oid,remaining = item[:3]
        self.update_order(str(oid),float(remaining))
    
    def on_trade(self, item):
        #["t", 12345, "0.03000000", "0.50000000", "0.00250000", 0, 6083059, "0.00000375", "2018-09-08 05:54:09"]
        #['t', 9394539, '0.00057427', '0.00000476', '0.00000000', 0, 274547887461]
        #["t", <trade ID>, "<rate>", "<amount>", "<fee multiplier>", <funding type>, <order number>, <total fee>, <date>]
        #funding_type: 0 (exchange wallet), 1 (borrowed funds), 2 (margin funds), or 3 (lending funds).
        _, tid, rate, qty, fee_multiplier, funding_type, oid = item[:7]
        tid, oid = str(tid), str(oid)
        rate = float(rate)
        qty = float(qty)
        fee_multiplier = float(fee_multiplier)
        total_fee = float(item[7]) if len(item) > 7 else None
        dto = parsedate(item[8]) if len(item) > 8 else dt.utcnow()
        if dto.tzinfo is not None:
            logger2.error('POLONIEX HAS CHANGED THEIR DATE FORMAT: {}, {}'.format(item[8], dto))
        ts = timestamp_ms(dto.replace(tzinfo=None))
        self.add_trade(tid, None, None, rate, qty, fee_multiplier, ts, oid)
        
    def parse_ccxt_order(self, r):
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
        #If the order is filled immediately then only trade is sent
        # thus have to acquire 'left' from ccxt order
        #Adding the trades too to avoid delay between "left" and "executed"/"payout" being updated
        # (delay between .create_order and websocket order recv; in websocket itself this is not a problem
        #  as poloniex sends new_order,order_updates,trades in one batch)
        #Note: 'remaining' == 'amount', neither is 'filled' always accurate (check ccxt.poloniex.parse_order)
        filled = sum(t['amount'] for t in r.get('trades',[]))
        parsed = {
            'order': {'left': max(0, r['amount']-filled), 
                      'executed': 0}, #filled}, #'executed' is calculated from trades
            'trades': [
                    {'id': t['info']['tradeID'], 'symbol': t['symbol'], 
                     'side': t['side'], 'rate': t['price'], 'qty': t['amount'], 
                     #it may be imprecise but round should eliminate that
                     'fee_mp': round(1 - float(t['info']['takerAdjustment'])
                                / float(t['info']['amount' if t['side']=='buy' else 'total']), 5) 
                              if 'takerAdjustment' in t['info'] else round(float(r['info']['fee']),5),
                     'ts': pydt_from_ms(t['timestamp']),
                     'oid': r['id'], 
                     'payout': float(t['info']['takerAdjustment']) if 'takerAdjustment' in t['info'] else None,
                     }
                for t in r.get('trades',[])
            ]
        }
        print('r: {}'.format(r))
        print('parsed: {}'.format(parsed))
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
    
    def convert_symbol(self, symbol, direction=1):
        #0: ex to ccxt 1: ccxt to ex
        try: return super().convert_symbol(symbol, direction)
        except KeyError: pass
        
        if not direction:
            return '/'.join(self.convert_cy(x,0) for x in symbol.split('_')[::-1])
        else:
            return '_'.join(self.convert_cy(x,1) for x in symbol.split('/')[::-1])
    
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
