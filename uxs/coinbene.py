from uxs.base.socket import ExchangeSocket

import fons.log

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class coinbene(ExchangeSocket):
    exchange = 'coinbene'
    url_components = {
        #'ws': 'wss://ws.coinbene.vip/stream/ws',
        'ws': 'wss://ws.coinbene.com/stream/ws'
    }
    auth_defaults = {
        'takes_input': False,
        'each_time': False,
        'send_separately': True,
    }
    channel_defaults = {
        'merge_option': True,
        'fetch_data_on_sub': False, # all subscriptions have initial "insert" action
    }
    has = {
        'ticker': {
            'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False, 'last': True,
            'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False,
            'change': True, 'percentage': True, 'average': True, 'vwap': False,
            'baseVolume': False, 'quoteVolume': True, 'active': False},
        'all_tickers': True,
        'orderbook': True,
        'ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': False, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        #'account': False, #{'balance': True, 'order': True, 'fill': False},
        'fetch_tickers': {
            'ask': True, 'askVolume': False, 'average': True, 'baseVolume': False, 'bid': True, 'bidVolume': False,
            'change': True, 'close': True, 'datetime': False, 'high': True, 'last': True, 'low': True, 'open': True,
            'percentage': True, 'previousClose': False, 'quoteVolume': True, 'symbol': True, 'timestamp': False,
            'vwap': False},
        'fetch_ticker': True,
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'asks': True, 'bids': True, 'datetime': True, 'nonce': False, 'timestamp': True},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': False, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'create_order': {'id': True},
        'fetch_order': {
            'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': True,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': False, 'type': True},
        'fetch_open_orders': {'symbolRequired': False},
        'fetch_closed_orders': {'symbolRequired': False},
    }
    has['all_tickers'] = has['ticker'].copy()
    has['fetch_ticker'] = has['fetch_tickers'].copy()
    has['fetch_open_orders'].update(has['fetch_order'])
    has['fetch_closed_orders'].update(has['fetch_order'])
    channel_ids = {
        'orderbook': 'spot/orderBook.<symbol>.<limit>',
        'ohlcv': 'spot/simple/kline.<symbol>.<timeframe>',
        'trades': 'spot/tradeList.<symbol>',
        'ticker': 'spot/simple/ticker.<symbol>',
        'all_tickers': 'spot/simple/ticker.all',
        'account': ['spot/userEvent', 'margin/userEvent'], # spot/userEvent receives empty messages for some reason
        'exchangeRate': 'exchangeRate', # last price, 24h change %
    }
    connection_defaults = {
        'pong': lambda x: None if x!='ping' else 'pong',
    }
    intervals = {
        'fetch_open_orders': 60,
        'fetch_closed_orders': 60,
        'fetch_order': 60,
    }
    #max_subscriptions_per_connection = 1 # symbol per connection
    #subscription_push_rate_limit = 0.04
    message = {
        #'id': {'key': 'id'},
        #'error': {'key': 'error'},
    }
    ob = {
        'force_create': None,
        'receives_snapshot': True,
        'default_limit': 10,
        'limits': [5, 10, 50, 100],
        'fetch_limits': [5, 10, 50, 100],
    }
    timeframes = {
        '1m': '1m',
        '3m': '3m',
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '2h': '2h',
        '4h': '4h',
        '6h': '6h',
        '12h': '12h',
        '1d': 'D',
        '1w': 'W',
        '1M': 'M',
    }
    trade = {
        'sort_by': lambda x: (x['timestamp'], x['price'], x['amount']),
    }
    
    def handle(self, R):
        r = R.data
        if isinstance(r, dict):
            event = r.get('event')
            topic = r.get('topic', '')
            if event is not None:
                self.log(r)
            elif '/ticker' in topic:
                self.on_ticker(r)
            elif '/orderBook' in topic:
                self.on_orderbook(r)
            elif '/kline' in topic:
                self.on_ohlcv(r)
            elif '/tradeList' in topic:
                self.on_trade(r)
            elif '/userEvent' in topic:
                self.on_account(r)
            elif '/userMargin' in topic:
                self.notify_unknown(r)
            elif '/user.account' in topic:
                self.on_swap_balance(r)
            elif '/user.order' in topic:
                self.on_swap_order(r)
            else:
                self.notify_unknown(r)
        else:
            self.notify_unknown(r)
    
    
    def on_ticker(self, r):
        enable_sub = True if not r['topic'].endswith('ticker.all') else 'all_tickers'
        # it sometimes contains tickers that are not listed under "markets"
        tickers = [self.parse_ticker(x) for x in r['data'] if x['s'] in self.api.markets_by_id]
        self.update_tickers(tickers, enable_sub=enable_sub)
    
    
    def parse_ticker(self, r):
        """ 
        {
          's': 'QTUMUSDT',
          'lp': '3.832',
          'ap': '3.834',
          'bp': '3.827',
          'h24': '4.010',
          'l24': '3.627',
          'o24': '3.973',
          'v24': '3268367.169',
          't': 1598351007037,
          'op': '3.877'
        }
        """
        map = {
            'symbol': 's',
            'last': 'lp',
            'high': 'h24',
            'low': 'l24',
            'open': 'o24',
            'quoteVolume': 'v24',
            'bid': 'bp',
            'ask': 'ap',
            'timestamp': 't',
        }
        apply = {'symbol': lambda x: self.convert_symbol(x, 0)}
        
        return self.api.ticker_entry(
            **self.api.lazy_parse(r, [], map, apply),
            info = r)
    
    
    def on_orderbook(self, r):
        """
        // full data
        {
            "topic": "btc/orderBook.BTCUSDT", 
            "action": "insert",
            "data": [
              {
                "asks": [
                    ["5621.7", "58"], 
                    ["5621.8", "125"],
                    ["5621.9", "100"],
                    ["5622", "84"],
                    ["5623.5", "90"],
                    ["5624.2", "1540"],
                    ["5625.1", "300"],
                    ["5625.9", "350"],
                    ["5629.3", "200"],
                    ["5650", "1000"]
                ],
                "bids": [
                    ["5621.3", "287"],
                    ["5621.2", "41"],
                    ["5621.1", "2"],
                    ["5621", "26"],
                    ["5620.8", "194"],
                    ["5620", "2"],
                    ["5618.8", "204"],
                    ["5618.4", "30"],
                    ["5617.2", "2"],
                    ["5609.9", "100"]
                ],
                "version":1,
                "timestamp": 1584412740809
              }
            ]
         }
         // incremental data
        {
            "topic": "btc/orderBook.BTCUSDT", 
            "action": "update", 
            "data": [
              {
                "asks": [
                    ["5621.7", "50"],
                    ["5621.8", "0"],
                    ["5621.9", "30"]
                ],
                "bids": [
                    ["5621.3", "10"],
                    ["5621.2", "20"],
                    ["5621.1", "80"],
                    ["5621", "0"],
                    ["5620.8", "10"]
                ],
                "version":2,
                "timestamp": 1584412740809
              }
            ]
         }
        """
        symbol = self.convert_symbol(r['topic'].split('.')[-1], 0)
        is_snap = r['action'] == 'insert'
        raw_ob = r['data'][0]
        ob = {
            **self.api.parse_order_book(raw_ob, raw_ob['timestamp']),
            'symbol': symbol,
            'nonce': raw_ob['version'],
        }
        if is_snap:
            self.ob_maintainer.send_orderbook(ob)
        else:
            self.ob_maintainer.send_update(ob)
    
    
    def on_ohlcv(self, r):
        _, marketId, timeframeId = r['topic'].split('.')
        symbol = self.convert_symbol(marketId, 0)
        timeframe = self.convert_timeframe(timeframeId, 0)
        ohlcvs = [self.parse_ohlcv(x) for x in r['data']]
        self.update_ohlcv(
            [{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': ohlcvs}], enable_sub=True)
    
    
    def parse_ohlcv(self, r):
        """
        [11752.51, 11754.82, 11750.33, 11752.83, 6.9536, 1598310120]
        """
        return [r[5]*1000, r[0], r[1], r[2], r[3], r[4]]
    
    
    def on_trade(self, r):
        symbol = self.convert_symbol(r['topic'].split('.')[-1], 0)
        trades = [self.parse_trade(x, symbol) for x in r['data']]
        self.update_trades([{'symbol': symbol, 'trades': trades}], enable_sub=True)
    
    
    def parse_trade(self, r, symbol=None):
        # ["8600.0000", "s", "100", 1584412740809]
        # ['11408.88', 'b', '0.2196', 1598370243000]
        price, _side, amount, timestamp = r[:4]
        side = 'sell' if _side=='s' else 'buy'
        return self.api.trade_entry(symbol=symbol, price=price, side=side, amount=amount,
                                    timestamp=timestamp, cost=float(price)*float(amount),
                                    info=r)
    
    
    def on_account(self, r):
        pass
    
    
    def on_swap_balance(self, r):
        balances = [self.parse_swap_balance(x) for x in r['data']]
        self.update_balances(balances, enable_sub=True)
    
    
    def parse_swap_balance(self, r):
        """
        {
          "asset": "BTC",
          "availableBalance": "20.3859", 
          "frozenBalance": "0.7413",
          "balance": "21.1272", 
          "timestamp": "2019-05-22T03:11:22.0Z"
        }
        """
        return self.api.balance_entry(**{
            'currency': self.convert_cy(r['asset']),
            'free': r.get('availableBalance'),
            'used': r.get('frozenBalance'),
            'total': r.get('balance'),
            'info': r,
        })
    
    
    def on_swap_order(self, r):
        for o in r['data']:
            p = self.parse_swap_order(o)
            self.add_order_from_dict(p, enable_sub=True)
    
    
    def parse_swap_order(self, r):
        """
        { 
          "orderId": "580721369818955776",
          "baseAsset": "ETH",
          "quoteAsset": "USDT",
          "direction": "openLong",
          "leverage": "20",
          "symbol": "ETHUSDT",
          "orderType": "limit", 
          "quantity": "7", 
          "orderPrice": "146.30",
          "orderValue": "0.0010",     // (BTC cost?)
          "fee": "0.0000",
          "filledQuantity": "0",
          "averagePrice": "0.00", 
          "orderTime": "2019-05-22T03:39:24.0Z", 
          "status": "new",
          "lastFillQuantity": "0",
          "lastFillPrice": "0",
          "lastFillTime": ""
        }
        """
        symbol = self.convert_symbol(r['symbol'], 0)
        market = self.markets[symbol]
        id = r['orderId']
        direction_low = r['direction'].lower()
        side = 'buy' if ('buy' in direction_low or 'long' in direction_low) else 'sell'
        price = float(r['price'])
        if price == 0:
            price = None
        average = float(r['averagePrice'])
        if average == 0:
            average = None
        amount = float(r['quantity'])
        filled = float(r['filledQuantity'])
        remaining = amount - filled
        # new, filled, canceled, partiallyFilled
        if r['status'] not in ('new', 'partiallyFilled'):
            remaining = 0
        cost = None
        payout = None
        if average is not None:
            cost = average * filled
            payout = self.api.calc_payout(symbol, side, filled, average)
        #if average is None and filled:
        #    if cost is not None:
        #        average = cost / filled
        #    elif price is not None:
        #        average = price
        fee = None
        if r.get('fee') is not None:
            fee = {
                'cost': float(r['fee']),
                'currency': market['quote'],
                'rate': None,
            }
        def parse_time(x):
            return self.api.parse8601(x[:-1] + '00' + 'Z') if x else None
        
        return {
            'symbol': symbol,
            'id': id,
            'timestamp': parse_time(r.get('orderTime')),
            'lastTradeTimestamp': parse_time(r.get('lastFillTime')),
            'type': r['type'],
            'side': side,
            'price': price,
            'average': average,
            'filled': filled,
            'remaining': remaining,
            'cost': cost,
            'payout': payout,
            'fee': fee,
        }
    
    
    def encode(self, rq, sub=None):
        channel = rq.channel
        p = rq.params
        symbol = p.get('symbol')
        timeframe = p.get('timeframe')
        limit = p.get('limit')
        
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
        
        return {
            'op': 'subscribe' if sub else 'unsubscribe',
            'args': args,
        }
    
    
    def sign(self, out={}):
        self.api.check_required_credentials()
        # ["<apiKey>","<expires>","<signature>"]
        expires_ts = self.api.milliseconds() + 30 * 86400 * 1000 # in a month
        expires = self.api.iso8601(expires_ts).split('.')[0] + 'Z' # 2019-07-04T02:19:08Z
        signStr = expires + 'GET' + '/login'
        signature = self.api.hmac(self.api.encode(signStr), self.api.encode(self.secret))
        return {
            'op': 'login',
            'args': [self.apiKey, expires, signature],
            #'args': [self.auth_info['wsKey']],
        }
