from uxs.base.socket import ExchangeSocket

import fons.log
logger,logger2,tlogger,tloggers,tlogger0 = fons.log.get_standard_5(__name__)


class bw(ExchangeSocket):
    exchange = 'bw'
    url_components = {
        'ws': 'wss://kline.bw.com/websocket',
    }
    channels = {
        'ohlcv': {'fetch_data_on_sub': False}, # receives snapshot
        'trades': {'fetch_data_on_sub': False}, # -||-
    }
    channel_ids = {
        'orderbook': '<symbol>_ENTRUST_ADD_<name>',
        'ohlcv': '<symbol>_KLINE_<timeframe>_<name>',
        'trades': '<symbol>_TRADE_<name>',
        'all_tickers': 'ALL_TRADE_STATISTIC_24H',
        'ticker': '<symbol>_TRADE_STATISTIC_24H',
    }
    has = {
        'all_tickers': { # update frequency = 13s
            'bid': True, 'bidVolume': False, 'ask': True, 'askVolume': False, 'last': True,
            'high': True, 'low': True, 'open': True, 'close': True, 'previousClose': False,
            'change': True, 'percentage': True, 'average': True, 'vwap': True,
            'baseVolume': True, 'quoteVolume': True, 'active': False},
        'ticker': True,
        'orderbook': True,
        'trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': False, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        'ohlcv': True, # todo
        'fetch_tickers': True,
        'fetch_ticker': {
            'ask': True, 'askVolume': False, 'average': False, 'baseVolume': True, 'bid': True, 'bidVolume': False,
            'change': True, 'close': True, 'datetime': True, 'high': True, 'last': True, 'low': True, 'open': False,
            'percentage': False, 'previousClose': False, 'quoteVolume': True, 'symbol': True, 'timestamp': True,
            'vwap': False},
        'fetch_ohlcv': {'timestamp': True, 'open': True, 'high': True, 'low': True, 'close': True, 'volume': True},
        'fetch_order_book': {'asks': True, 'bids': True, 'datetime': True, 'nonce': False, 'timestamp': True},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': False, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': True},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'fetch_order': {
            'amount': True, 'average': True, 'clientOrderId': False, 'cost': True, 'datetime': True, 'fee': False,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': True, 'trades': False, 'type': True},
        'fetch_orders': {'symbolRequired': True},
        'fetch_open_orders': {'symbolRequired': True},
        'fetch_closed_orders': {'symbolRequired': True},
        'create_order': {
            'amount': True, 'average': False, 'clientOrderId': False, 'cost': False, 'datetime': False, 'fee': False,
            'filled': False, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': False, 'side': True,
            'status': True, 'symbol': True, 'timestamp': False, 'trades': False, 'type': True},
    }
    has['ticker'] = has['all_tickers'].copy()
    has['fetch_tickers'] = has['fetch_ticker'].copy()
    has['fetch_orders'].update(has['fetch_order'])
    has['fetch_open_orders'].update(has['fetch_order'])
    has['fetch_closed_orders'].update(has['fetch_order'])
    ob = {
        'uses_nonce': False,
        'receives_snapshot': True,
        'force_create': None,
        'limits': list(range(1, 501)),
    }
    timeframes = {
        '1m': '1M',
        '5m': '5M',
        '15m': '15M',
        '30m': '30M',
        '1h': '1H',
        '1d': '1D',
    }
    trade = {
        'sort_by': lambda x: (x['timestamp'], x['price'], x['amount']),
    }
    
    def handle(self, R):
        r = R.data
        
        if 'trade_statistic' in r:
            self.on_ticker(r)
        elif r[0] == 'E' or isinstance(r[0], list) and r[0][0] == 'AE':
            self.on_orderbook(r)
        elif r[0] == 'T' or isinstance(r[0], list) and r[0][0] == 'T':
            self.on_trade(r)
        elif r[0] == 'K' or isinstance(r[0], list) and r[0][0] == 'K':
            self.on_ohlcv(r)
        else:
            self.notify_unknown(r)
    
    
    def on_orderbook(self, r):
        """
        SNAPSHOT:
        [['AE', '281', 'BTC_USDT', '1596066633', {'asks': [['11154.54', '0.00023'], ['11151.41', '0.00029'], ...]},
                                                 {'bids': [['11104.46', '0.0003'], ['11101.33', '0.00018'], ...]}]]
        UPDATE:
        ['E', '281', '1596066633', 'BTC_USDT', 'BID', '11095.75', '0']
        ['E', '281', '1596066633', 'BTC_USDT', 'ASK', '11111.52', '0.00269']
        """
        if r[0] != 'E':
            ob = self.parse_ob_snapshot(r[0])
            self.ob_maintainer.send_orderbook(ob)
        else:
            u = self.parse_ob_update(r)
            self.ob_maintainer.send_update(u)
    
    
    def parse_ob_snapshot(self, ss):
        symbol = self.convert_symbol(ss[1], 0)
        timestamp = self.api.safe_timestamp(ss, 3)
        bids_asks = {**ss[4], **ss[5]}
        return self.api.parse_order_book(bids_asks, symbol, timestamp)
    
    
    def parse_ob_update(self, u):
        symbol = self.convert_symbol(u[1], 0)
        which, other = ['bids','asks'][::(1 if u[4]=='BID' else -1)]
        return {
            'symbol': symbol,
            which: [[float(u[5]), float(u[6])]], 
            other: []
        }
    
    
    def on_ticker(self, r):
        """
        For both `ticker` and `all_tickers`:
        {
            'trade_statistic': [
                [
                    '4051',      // The market id
                    '0.00006',   // Latest price
                    '0.000066',  // High
                    '0.000059',  // Low
                    '795810',    // 24h Volume
                    '-13.04',    // 24h Change
                    '[[1, 0.000059], [2, 0.00006], [3, 0.00006], [4, 0.000059], [5, 0.00006], [6, 0.00006]]', // Last 6 hours closing price list
                    '0.000058',  // take 1 price
                    '0.000061',  // make 1 price
                    '47.7035'    // 24h trading volume, i.e. sum(each transaction price * the transaction volume)
                ], 
            ]
        }
        """
        tickers = []
        enable_sub = 'all_tickers' if self.is_subscribed_to(('all_tickers',)) else True
        for t in r['trade_statistic']:
            tickers.append(self.parse_ticker(t))
        self.update_tickers(tickers, enable_sub=enable_sub)
    
    
    def parse_ticker(self, t):
        symbol = self.convert_symbol(t[0], 0)
        open = float(t[1]) / (1 + float(t[5]) / 100)
        return self.api.ticker_entry(
            symbol=symbol, last=t[1], high=t[2], low=t[3], baseVolume=t[4], percentage=t[5],
            bid=t[7], ask=t[8], quoteVolume=t[9], open=open, info=t, # the volume seems to be of this day (not 24h)
        )
    
    
    def on_ohlcv(self, r):
        """
        Snapshot:
        [ OHLCV_0, OHLCV_1, ... ]
        
        OHLCV:
        [
            "K",         //Data type, K line
            "281",       //The market id
            "btc_usdt",  //Symbol
            "1569312900",//Timestamp
            "9743.7",    //Open
            "9747.95",   //High
            "9732.75",   //Low
            "9735.95",   //Close
            "26.92",     //Volume
            "-0.0795",   //Change
            "30.57",     //Dollar currency rate
            "15M",       //KLine cycle
            "false",     //Whether to convert data
            "262139.72"  //Trading volume
        ],
        """
        if not isinstance(r[0], list):
            r = [r]
        symbol = self.convert_symbol(r[0][1], 0)
        timeframe = self.convert_timeframe(r[0][11], 0)
        ohlcvs = [self.parse_ohlcv(ro) for ro in r]
        
        self.update_ohlcv([{'symbol': symbol, 'timeframe': timeframe, 'ohlcv': ohlcvs}],
                          enable_sub=True)
    
    
    def parse_ohlcv(self, ohlcv):
        return [
            self.api.safe_timestamp(ohlcv, 3),
            self.api.safe_float(ohlcv, 4),
            self.api.safe_float(ohlcv, 5),
            self.api.safe_float(ohlcv, 6),
            self.api.safe_float(ohlcv, 7),
            self.api.safe_float(ohlcv, 8),
        ]
    
    
    def on_trade(self, r):
        """
        Snapshot:
        [Trade_0, Trade_1, ...]
        Trade:
        ['T', '281', '1596736300', 'BTC_USDT', 'ask', '11851.86', '0.00036']
        """
        if not isinstance(r[0], list):
            r = [r]
        trades = [self.parse_trade(rt) for rt in r]
        
        self.update_trades([{'symbol': trades[0]['symbol'], 'trades': trades}], enable_sub=True)
    
    
    def parse_trade(self, t):
        symbol = self.convert_symbol(t[1], 0)
        type = 'limit'  # only limit orders allowed
        side = 'sell' if t[4]=='ask' else 'buy'
        price = float(t[5])
        amount = float(t[6])
        cost = amount * price
        timestamp = self.api.safe_timestamp(t, 2)
        
        return self.api.trade_entry(symbol=symbol, type=type, side=side, amount=amount,
                                    price=price, cost=cost, timestamp=timestamp, info=t)
    
    
    def encode(self, rq, sub=None):
        channel = rq.channel
        params = rq.params
        
        action = 'ADD' if sub else 'DEL'
        dataType = self.channel_ids[channel]
        if '<symbol>' in dataType:
            dataType = dataType.replace('<symbol>', self.convert_symbol(params['symbol'], 1))
        if '<timeframe>' in dataType:
            dataType = dataType.replace('<timeframe>', self.convert_timeframe(params['timeframe'], 1))
        if '<name>' in dataType:
            market = self.markets[params['symbol']]
            dataType = dataType.replace('<name>', market['info']['name'].upper())
        dataSizes = {'orderbook': 50, 'ohlcv': 500, 'trades': 20}
        dataSize = params['limit'] if params.get('limit') is not None else dataSizes.get(channel, 1)
        if channel=='orderbook':
            dataSize = self.ob_maintainer.resolve_limit(dataSize)
        
        return {
            'dataType': dataType,
            'dataSize': dataSizes.get(channel, 1),
            'action': action,
        }

