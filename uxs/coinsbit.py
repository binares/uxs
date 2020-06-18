import random

from uxs.base.socket import ExchangeSocket

import fons.log

logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class coinsbit(ExchangeSocket):
    exchange = 'coinsbit'
    url_components = {
        'ws': 'wss://coinsbit.io/trade_ws', # crashes every 40 s
    }
    channel_defaults = {
        'max_subscriptions': 100,
        'subscription_push_rate_limit': 0.04,
        'unsub_option': False, # todo
    }
    channels = {
        'ticker': {
            'merge_option': True,
        },
        'trades': {
            'merge_option': True,
        },
        'price': {
            'required': ['symbol'],
            'merge_option': True,
            'delete_data_on_unsub': False,
        },
    }
    connection_defaults = {
        'ping': 'm$ping',
        'ping_interval': 30,
        'ping_as_message': True,
    }
    has = {
        'ticker': {'last': True, 'bid': False, 'ask': False, 'bidVolume': False, 'askVolume': False,
                   'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False,
                   'change': False, 'percentage': False, 'average': False, 'vwap': False,
                   'baseVolume': True, 'quoteVolume': True, 'active': False},     
        'all_tickers': False,
        'orderbook': True,
        'ohlcv': {'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'trades': {'timestamp': True, 'datetime': True, 'symbol': True, 'id': True,
                   'order': False, 'type': False, 'takerOrMaker': False, 'side': True,
                   'price': True, 'amount': True, 'cost': False, 'fee': False},
        'account': {'balance': False, 'order': False, 'fill': False},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'fetch_open_orders': {'symbolRequired': True},
        'fetch_my_trades': {'symbolRequired': True},
        'fetch_ticker': {'ws': True},
        'fetch_tickers': {
                'last': True, 'bid': True, 'ask': True, 'bidVolume': False, 'askVolume': False,
                'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False,
                'change': False, 'percentage': True, 'average': True, 'vwap': False,
                'baseVolume': True, 'quoteVolume': True, 'active': False},  
    }
    has['fetch_ticker'].update(has['ticker'])
    channel_ids = {
        'ticker': 'state',
        'orderbook': 'depth',
        'ohlcv': 'kline',
        'trades': 'deals',
        'price':'price',
    }
    exceptions = {
    }
    message = {
        'id': {'key': 'id'},
        'error': {'key': 'error'},
    }
    ob = {
        'force_create': None,
        'uses_nonce': False,
        'receives_snapshot': True,
        'default_limit': 10,
        'limits': [10],
    }
    order = {
        'update_filled_on_fill': True,
        'update_payout_on_fill': True,
        'update_remaining_on_fill': True,
    }
    timeframes = {
        '1m': 60,
    }
    
    def handle(self, R):
        r = R.data
        
        if isinstance(r, dict):
            ch_id = r.get('method','').split('.')[0]
            if r.get('result') == 'pong':
                tlogger0.debug(r)
            elif ch_id == 'state':
                self.on_ticker(r)
            elif ch_id == 'depth':
                self.on_orderbook(r)
            elif ch_id == 'kline':
                self.on_ohlcv(r)
            elif ch_id == 'deals':
                self.on_trade(r)
            elif ch_id == 'price':
                self.on_price(r)
            else:
                self.notify_unknown(r)
        else:
            self.notify_unknown(r)
    
    
    async def fetch_ticker(self, symbol=None, params={}):
        if self.is_active():
            inner_id = 'fetch_ticker+{}'.format(symbol)
            return await self.send({'_': 'fetch_ticker'}, True, id=inner_id)
        else:
            return await super().fetch_ticker(symbol, params)
    
    
    def on_ticker(self, r):
        """
        {
            "method": "state.update",
            "params": ["BTC_USD", 
                       {...}],
            "id": null
        }
        """
        # on subscribing to anything it erronously starts sending all ticker updates
        symbol_id, ticker = r['params']
        parsed = self.parse_ticker(ticker, symbol_id)
        if self.is_subscribed_to(('ticker', parsed['symbol'])):
            self.update_tickers([parsed], enable_sub=True)
        
        if r['id'] is not None and r['id'] > 10 ** 9:
            inner_id = 'fetch_ticker+{}'.format(parsed['symbol'])
            if self.tp.has_waiter(inner_id):
                self.tp.forward_to_waiters(inner_id, parsed, copy=1)
    
    
    def parse_ticker(self, ticker, symbol_id):
        """
        {
            "period": 86400,
            "volume": "6790.57915338",
            "high": "11000",
            "last": "10396.62814071",
            "low": "10291.07",
            "open": "10391.60804021",
            "close": "10396.62814071",
            "deal": "70615366.739616700295163"
        }
        """
        return self.api.ticker_entry(
            symbol = self.convert_symbol(symbol_id, 0),
            **self.api.lazy_parse(
                ticker,
                ['volume','high','last','low','open','close'],
                {'baseVolume': 'volume', 'quoteVolume': 'deal'}
            )
        )
    
    
    def on_orderbook(self, r):
        """
        {
            "method": "depth.update",
            "params": [
                true,
                {"asks": [["0.018519", "120.6"]], "bids": [["0.01806", "90.31637262"]]},
                "ETH_BTC"
            ],
            "id": null}
        """
        is_snapshot, ob, symbol_id = r['params']
        parsed = self.api.ob_entry(
            symbol = self.convert_symbol(symbol_id, 0),
            timestamp = self.api.milliseconds(),
            **ob
        )
        if is_snapshot:
            self.ob_maintainer.send_orderbook(parsed)
        else:
            self.ob_maintainer.send_update(parsed)
    
    
    def on_ohlcv(self, r):
        """
        {
            "method": "kline.update",
            "params": [...],
            "id": null
        }
        """
        for x in r['params']:
            symbol = self.convert_symbol(x[7], 0)
            timeframe = '1m'
            parsed = self.parse_ohlcv(x)
            self.update_ohlcv(
                [{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': [parsed]}],
                enable_sub=True)
    
    
    def parse_ohlcv(self, ohlcv, market=None, timeframe='1m', since=None, limit=None):
        """
        [
            1568548260, time
            "0.018302", open
            "0.018302", close
            "0.018302", highest
            "0.018302", lowest
            "500", volume
            "15", amount
            "ETH_BTC" market name
        ]
        """
        return [
            ohlcv[0] * 1000,
            float(ohlcv[1]),
            float(ohlcv[3]),
            float(ohlcv[4]),
            float(ohlcv[2]),
            float(ohlcv[5]),
        ]
    
    
    def on_trade(self, r):
        """
        {
            "method": "deals.update",
            "params": [
                "BTC_USD",
                [...]
            ],
            "id": null
        }
        """
        symbol_id, trades = r['params']
        symbol = self.convert_symbol(symbol_id, 0)
        parsed = [self.parse_trade(x, symbol_id) for x in trades]
        self.update_trades(
            [{'symbol': symbol, 'trades': parsed}], enable_sub=True)
    
    
    def parse_trade(self, trade, symbol_id):
        """
        {
            "type": "sell",
            "time": 1568556382.1329091,
            "id": 5478754,
            "amount": "4.9193309",
            "price": "10365.40703518"
        }
        """
        return self.api.trade_entry(
            symbol = self.convert_symbol(symbol_id, 0),
            **self.api.lazy_parse(
                trade,
                ['id','amount','price'],
                {'timestamp': 'time',
                 'side': 'type'},
                {'timestamp': lambda x: int(x*1000)}
            )
        )
    
    
    def on_price(self, r):
        logger.debug(r)
    
    
    def encode(self, req, sub=None):
        p = req.params
        channel = req.channel
        symbol = p.get('symbol')
        limit = p.get('limit')
        msg_id = random.randint(0, 10**9)
        
        out = {
            'method': None,
            'params': [],
            'id': msg_id,
        }
        name = self.channel_ids[channel]
        
        symbol_list = ([symbol] if isinstance(symbol, str) else list(symbol)) if symbol else []
        symbolIds = [self.convert_symbol(x,1) for x in symbol_list]
        out['params'] += symbolIds
        
        if channel == 'orderbook':
            if limit is None:
                limit = self.ob['default_limit']
            self.ob_maintainer.set_limit(symbol, limit)
        
        if sub is not None:
            out['method'] = '{}.{}subscribe'.format(name, '' if sub else 'un')
            if channel == 'ohlcv':
                out['params'] += [self.convert_timeframe(p['timeframe'], 1)]
            elif channel == 'orderbook':
                # limit, interval
                out['params'] += [limit, "0"]
                
        else:
            out['method'] = '{}.query'.format(name)
            if channel == 'fetch_ticker':
                out['params'] += [86400] # latest 24 h
            msg_id = 10**9 + self.api.milliseconds()
        
        return (out, msg_id)
    
    
    def ping(self):
        return {
            'method': 'server.ping',
            'params': [],
            'id': random.randint(0, 10**9),
        }
