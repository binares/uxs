import json
import datetime
dt = datetime.datetime
td = datetime.timedelta

from uxs.base.socket import ExchangeSocket

import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)

    
class coindcx(ExchangeSocket):
    exchange = 'coindcx'
    auth_defaults = {
        'takes_input': True,
        'each_time': True,
        'send_separately': False,
    }
    url_components = {
        'ws': 'wss://stream.coindcx.com',
    }
    channel_defaults = {
        'unsub_option': True,
        'merge_option': False,
    }
    has = {
        'all_tickers': False,
        'ticker': False,
        'orderbook': True,
        'trades': {'timestamp': True, 'datetime': True, 'symbol': True, 'id': False,
                   'order': False, 'type': False, 'takerOrMaker': True, 'side': True,
                   'price': True, 'amount': True, 'cost': True, 'fee': False},
        'ohlcv': False,
        'account': {'balance': True, 'order': False, 'fill': True},
        'fetch_tickers': {
            'last': True, 'bid': True, 'ask': True, 'bidVolume': False, 'askVolume': False, 
            'high': True, 'low': True, 'open': True, 'close': True, 
            'previousClose': False, 'change': True, 'percentage': True, 
            'average': True, 'vwap': False, 'baseVolume': True, 'quoteVolume': False},
        'fetch_trades': {
            'timestamp': True, 'datetime': True, 'symbol': True, 'id': False,
            'order': False, 'type': False, 'takerOrMaker': True, 'side': True,
            'price': True, 'amount': True, 'cost': True, 'fee': False},
        'fetch_open_orders': {'symbolRequired': True},
    }
    has['fetch_ticker'] = has['fetch_tickers'].copy()
    connection_defaults = {
        'socketio': True,
        'event_names': ['depth-update','new-trade','balance-update','trade-update','bo-trade-update'],
        'connect_timeout': 5,
        'reconnect_try_interval': 5, # initial reconnect often fails
    }
    channel_ids = {
        'orderbook': 'depth-update',
        'trade': 'new-trade',
        'account': 'coindcx',
    }
    ob = {
        # 'ignore_fetch_nonce': True,
        # 'cache_size': 50, # to compensate for fetch_order_book not having a nonce
        # 'assume_fetch_max_age': 5,
        'uses_nonce': False, # only binance implements nonce
    }
    order = {
        'cancel_automatically': 'if-not-subbed-to-account',
        'update_remaining_on_fill': True,
        'update_filled_on_fill': True,
        'update_payout_on_fill': True,
    }
    trade = {
        'sort_by': lambda x: (x['timestamp'], x['price'], x['amount']),
    }
    symbol = {
        'quote_ids': ['USDT', 'BTC', 'ETH', 'TUSD', 'BNB', 'USDC', 'XRP', 'INR'],
        'sep': '',
    }
    
    def handle(self, R):
        r = R.data
        event = r['event']
        data = json.loads(r['data'])
        
        if event == 'depth-update':
            self.on_orderbook_update(data)
        elif event == 'new-trade':
            self.on_trade(data)
        elif event == 'balance-update':
            self.on_balance(data)
        elif event == 'trade-update':
            self.on_fill(data)
        elif event == 'bo-trade-update':
            self.on_fill(data)
        else:
            self.notify_unknown_response(r)
    
    
    def on_orderbook_update(self, r):
        # Delay to binance ob is ~0.5s. This really should be using nonce (frequently some updates are lost),
        # but it is difficult to implement when only 1 exchange (binance) has it
        """
        // Huobi
        {
          "replace": true,
          "e": "depthUpdate",
          "E": 1595222296780,
          "s": "TTBTC",
          "U": "",
          "b": {"6e-7":1589320.85, "5.9e-7":1440639.26, "5.8e-7":6598325.47, "5.7e-7":606957.2, "5.6e-7":217899.36, "5.5e-7":148335.73, "5.4e-7":35238.73, "5.3e-7":48964.35, "5.2e-7":28505.21,"5e-7":112284.52, "4.9e-7":56443.44, "4.8e-7":68568.31, "4.6e-7":19062.35, "4.5e-7":69698.6, "4.4e-7":43257.15, "4.3e-7":46817.05, "4.2e-7":51580.09, "4.1e-7":53364.57, "4e-7":57643.33, "3.9e-7":21709.69},
          "a": {"6.1e-7":349374.48, "6.2e-7":177838.58, "6.3e-7":355971.7, "6.4e-7":148398.84, "6.5e-7":121920.52, "6.6e-7":241475.84, "6.7e-7":57000, "6.8e-7":576189.88, "6.9e-7":130909.55, "7e-7":270578.82, "7.1e-7":315030.14, "7.2e-7":68525.44, "7.3e-7":67182.32, "7.4e-7":67217.22,"7.5e-7":286490.78, "7.6e-7":155747.1, "7.7e-7":68168.6, "7.8e-7":754335.86, "7.9e-7":888528.88, "8e-7":150920.9},
          "type": "depth-update",
          "channel":"H-TT_BTC"
        }
        // Hitbtc
        {
            "e": "depthUpdate",
            "E": 138674,
            "s":"IPLBTC",
            "b": [["0.0000001001","0"],["0.0000001001","0"]],
            "a": [],
            "type": "depth-update",
            "channel": "HB-IPL_BTC"
        }
        // Coindcx
        {
          "s": "BTCINR",
          "b": [["705358.87000000",0],["705398.04000000",0]],
          "a": [],
          "type":"depth-update",
          "channel":"I-BTC_INR"
        }
        // Binance
        {
          "e": "depthUpdate",
          "E": 1595056020816,
          "s": "BTCUSDT",
          "U": 4967226709,
          "u": 4967226753,
          "b": [["9134.14000000","0.00000000"],["9134.13000000","0.00000000"],["9132.42000000","0.00000000"],["9132.37000000","0.20000000"],["9132.36000000","7.95000000"],["9132.34000000","0.36652900"],["9132.30000000","0.50000000"],["9128.67000000","0.10947800"],["9127.54000000","0.00000000"],["9084.14000000","0.01530700"],["9080.90000000","0.60832800"],["9080.03000000","0.00000000"],["9079.99000000","0.01676800"],["9079.12000000","0.05000000"],["9063.54000000","0.02054100"],["9062.68000000","0.00000000"],["9048.89000000","0.01143300"],["9044.36000000","0.66339600"],["9043.50000000","0.00000000"],["9022.44000000","0.10624100"],["9021.58000000","0.97300000"],["8929.87000000","0.00614900"]],
          "a": [["9134.16000000","0.00001600"],["9136.08000000","0.20000000"],["9136.77000000","0.00439500"],["9137.61000000","0.10947800"],["9137.71000000","0.01904600"],["9138.30000000","0.19131000"],["9140.35000000","0.85708800"],["9144.99000000","0.00572600"],["9236.04000000","0.00000000"]],
          "type": "depth-update",
          "channel": "B-BTC_USDT"
        }
        """
        symbol = self.convert_symbol(r['s'], 0)
        if not self.is_subscribed_to(('orderbook', symbol)):
            return
        
        d = {
            'symbol': symbol,
            'timestamp': r.get('E')
        }
        if r.get('replace'):
            ob = dict(d, **{
                'bids': [[float(y) for y in x] for x in r.get('b',{}).items()],
                'asks': [[float(y) for y in x] for x in r.get('a',{}).items()],
                'nonce': None,
            })
            self.ob_maintainer.send_orderbook(ob)
        else:
            # Everything here is the replica of binance's ob stream, but does it have `pu` keyword?
            # While listening to the stream, each new event's pu should be equal to the previous event's u,
            # otherwise fetch new order book.
            update = dict(d, **{
                'bids': [[float(y) for y in x] for x in r.get('b',[])],
                'asks': [[float(y) for y in x] for x in r.get('a',[])],
                'nonce': (r.get('U'),r.get('u')) if 'pu' not in r else (r['pu']+1,r['u']),
            })
            if self.is_subscribed_to(('orderbook',update['symbol'])):
                self.ob_maintainer.send_update(update)
    
    
    def parse_trade(self, r):
        """
        Trade:                     // identical in all exchanges
        {
          "T": 1595056023091,
          "p": "9134.15000000",
          "q": "0.03664800",
          "s": "BTCUSDT",
          "m": false,              // Is the buyer the market maker
          "type": "new-trade",
          "channel": "B-BTC_USDT"
        }
        Fill:
        {
          "o": "28c58ee8-09ab-11e9-9c6b-8f2ae34ea8b0",
          "t": "17105",
          "s": "XRPBTC",
          "p": "0.00009634",
          "q": "1.0",
          "T": 1545896665076.92,
          "m": true,
          "f": "0.000000009634",   // fee
          "e": "I",                // exchange identifier
          "x": "filled"            // order status
        }
        """
        order = r.get('o')
        symbol = self.convert_symbol(r['s'], 0)
        market = self.markets[symbol]
        id = r.get('t')
        price = r['p']
        amount = r['q']
        ts = int(r['T'])
        takerSide = ['buy','sell'][r['m']]
        side = takerSide
        takerOrMaker='taker'
        if not order:
            pass # use taker side for public trades
        elif order not in self.orders:
            pass # if the order hasn't been registered yet then it was probably immediately executed (taker)
        else:
            o = self.orders[order]
            side = o['side']
            takerOrMaker = 'taker' if side==takerSide else 'maker'
        cost = float(amount) * float(price)
        fee = None
        if 'f' in r:
            fee = {
                'currency': market['quote'],
                'cost': self.api.safe_float(r, 'f'),
                'rate': market[takerOrMaker],
            }
        return self.api.trade_entry(symbol=symbol, timestamp=ts, id=id,
                                    side=side, price=price, amount=amount,
                                    cost=cost, takerOrMaker='taker', fee=fee,
                                    order=order, info=r)
    
    def on_trade(self, r):
        """
        {
          ...
        }
        """
        t = self.parse_trade(r)
        if not self.is_subscribed_to(('trades', t['symbol'])):
            return

        self.update_trades([{'symbol': t['symbol'], 'trades': [t]}], enable_sub=True)
    
    
    def on_fill(self, r):
        """
        [
          {
            ...
          }
        ]
        """
        for f in r:
            t = self.parse_trade(f)
            self.add_fill_from_dict(t, enable_sub=True)
            if f.get('x')=='filled' and t['order']:
                self.update_order(t['order'], 0)
    
    
    def on_balance(self, r):
        """
        [
          {  
            "id": "25e19bec-c7fd-11ea-a3e8-3b5fdab16aa5",
            "balance": "25.8620549",
            "locked_balance": "19.21855",
            "address": "0x2D7959077D6B34dD3E4cb922F456eCCd8039f8fF",
            "tag": null,
            "currency": {
              "short_name": "USDT",
              "name": "Tether",
              "status": "active",
              "withdrawal_charge": 2,
              "min_withdrawal": 4,
              "min_deposit": 0,
              "confirmations": 2,
              "decimal_factor": 3,
              "category": "erc20"
            }
          }
        ]
        """
        updates = [
            {
                'cy': self.convert_cy(x['currency']['short_name'], 0),
                'free': float(x['balance']),
                'used': float(x['locked_balance']),
                'info': x,
            } for x in r
        ]
        self.update_balances(updates, enable_sub=True)
        
    
    def parse_order(self, r):
        pass
    
    
    def on_order(self, r):
        # Order updates are not sent for ordinary orders. Only leveraged ones?
        logger.debug(r)
        """
        [
          {
            "id": "dbbce8e2-48e6-4aa2-a7af-15463120e241",
            "side": "sell",
            "status": "open",
            "market": "XRPBTC",
            "order_type": "market_order",
            "trailing_sl": false,
            "trail_percent": null,
            "avg_entry": 0.00003414,
            "avg_exit": 0,
            "fee": 0.02,
            "entry_fee": 4.8e-7,
            "exit_fee": 0,
            "active_pos": 70,
            "exit_pos": 0,
            "total_pos": 70,
            "quantity": 70,
            "price": 0.00003415,
            "sl_price": 0.00004866,
            "target_price": 0,
            "stop_price": 0,
            "pnl": 0,
            "initial_margin": 0.0011962062,
            "interest": 0.05,
            "interest_amount": 0,
            "leverage": 2,
            "result": null,
            "created_at": 1570620552918,
            "updated_at": 1570620553456,
            "orders": [
              {
                "id": 165059,
                "order_type": "market_order",
                "status": "filled",
                "market": "XRPBTC",
                "side": "sell",
                "avg_price": 0.00003414,
                "total_quantity": 70,
                "remaining_quantity": 0,
                "price_per_unit": 0,
                "timestamp": 1570620552975.75,
                "fee": 0.02,
                "fee_amount": 4.8e-7,
                "filled_quantity": 70,
                "bo_stage": "stage_entry",
                "cancelled_quantity": 0,
                "stop_price": 0
              }
            ]
          }
        ]
        """
    
    
    def encode(self, rq, sub=None):
        p = rq.params
        channel = rq.channel
        
        if sub is False and channel in ('orderbook','trades'):
            other = 'orderbook' if channel=='trades' else 'trades'
            # unsubbing from one would also unsub from the other
            if self.is_subscribed_to((other, p['symbol'])):
                return None
        
        event = 'join' if sub else 'leave'
        if channel == 'account':
            channelName = 'coindcx'
        else:
            channelName = self.api.markets[p['symbol']]['info']['pair']
        
        return ([event, {'channelName': channelName}], None)
    
    
    def sign(self, out):
        if out[0]=='leave':
            return out
        # if wrong apiKey / secret is used, no error is raised; instead the connection keeps crashing
        body = self.api.json({'channel': 'coindcx'}) # "channel", not "channelName" !
        signature = self.api.hmac(self.api.encode(body), self.api.encode(self.secret))
        return [out[0], dict(out[1], authSignature=signature, apiKey=self.apiKey)] # "channelName" here
