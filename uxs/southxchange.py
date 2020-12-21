from collections import defaultdict

from uxs.base.socket import ExchangeSocket


class southxchange(ExchangeSocket):
    exchange = 'southxchange'
    url_components = {
        'ws': 'wss://www.southxchange.com/api/v2/connect'
    }
    channel_defaults = {
        'cnx_params_converter_config': {
            'lower': { 'symbol': False },
        }
    }
    connection_defaults = {
        'rate_limit': (1, 0.12),
    }
    max_subscriptions_per_connection = 45
    has = {
        'orderbook': True,
        'trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': False, 'order': False,
            'price': True, 'side': True, 'symbol': True, 'takerOrMaker': True, 'timestamp': True, 'type': False},
        'fetch_tickers': {
            'ask': True, 'askVolume': False, 'average': False, 'baseVolume': True, 'bid': True, 'bidVolume': False,
            'change': False, 'close': True, 'datetime': True, 'high': False, 'last': True, 'low': False, 'open': False,
            'percentage': True, 'previousClose': False, 'quoteVolume': False, 'symbol': True, 'timestamp': True,
            'vwap': False},
        'fetch_ticker': True,
        'fetch_order_book': {'ws': True, 'asks': True, 'bids': True, 'datetime': False, 'nonce': False, 'timestamp': False},
        'fetch_trades': {
            'amount': True, 'cost': True, 'datetime': True, 'fee': False, 'id': False, 'order': False,
            'price': True, 'side': True,'symbol': True, 'takerOrMaker': False, 'timestamp': True, 'type': False},
        'fetch_balance': {'free': True, 'used': True, 'total': True},
        'create_order': {'id': True},
        'fetch_open_orders': {
            'symbolRequired': False,
            'amount': True, 'average': False, 'clientOrderId': False, 'cost': True, 'datetime': False, 'fee': False,
            'filled': True, 'id': True, 'lastTradeTimestamp': False, 'price': True, 'remaining': True, 'side': True,
            'status': True, 'symbol': True, 'timestamp': False, 'trades': False, 'type': True},
    }
    has['fetch_ticker'] = has['fetch_tickers'].copy()
    ob = {
        #'ignore_fetch_nonce': True,
        #'cache_size': 50, # to compensate for fetch_order_book not having a nonce
        #'assume_fetch_max_age': 5,
        'uses_nonce': False,
    }
    trade = {
        'sort_by': lambda x: (x['timestamp'], x['price'], x['amount']),
    }
    
    def handle(self, R):
        r = R.data
        if isinstance(r, dict):
            k = r.get('k')
            if k == 'book':
                self.on_orderbook_snapshot(r)
            elif k == 'bookdelta':
                self.on_orderbook_update(r)
            elif k == 'trade':
                self.on_trade(r)
            else:
                self.notify_unknown(r)
        else:
            self.notify_unknown(r)
    
    
    def on_orderbook_snapshot(self, r):
        """
        {
          k: Message type = "book"
          v: Array of [MARKET_BOOK]
        }
        
        [MARKET_BOOK]
          m: Market ID
          b: Array of [BOOK_ITEM]
        
        [BOOK_ITEM]
          a: Amount of the item
          p: Price of the item
          b: True if the item is a buy
        """
        for item in r['v']:
            d = {
                'symbol': self.convert_symbol(item['m'],0),
                'bids': sorted([self.parse_ob_item(x) for x in item['b'] if x['b']], reverse=True),
                'asks': sorted([self.parse_ob_item(x) for x in item['b'] if not x['b']]),
                'timestamp': None,
                'datetime': None,
                'nonce': None,
            }
            id = 'fetch_order_book+{}'.format(d['symbol'])
            if self.tp.has_waiter(id):
                self.tp.forward_to_waiters(id, d, copy=1)
            else:
                self.orderbook_maintainer.send_orderbook(d)
    
    
    def on_orderbook_update(self, r):
        """
        {
          k: Message type = "bookdelta"
          v: Array of [BOOK_DELTA_ITEM]
        }
        
        [BOOK_DELTA_ITEM]
          m: Market ID
          a: Amount of the item (if amount = 0 item should be removed)
          p: Price of the item
          b: True if the item is a buy
        """
        by_symbol = {}
        for item in r['v']:
            symbol = self.convert_symbol(item['m'], 0)
            if not self.is_subscribed_to(('orderbook', symbol)):
                continue
            if symbol not in by_symbol:
                by_symbol[symbol] = {'symbol': symbol, 'bids': [], 'asks': [], 'nonce': None}
            side = ['asks','bids'][item['b']]
            by_symbol[symbol][side] += [self.parse_ob_item(item)]
        
        for update in by_symbol.values():
            self.ob_maintainer.send_update(update)
    
    
    def parse_ob_item(self, item):
        return [float(item['p']), float(item['a'])]
    
    
    async def fetch_order_book(self, symbol, limit=None, params={}):
        if self.is_active():
            id = 'fetch_order_book+{}'.format(symbol)
            return await self.send({'_': 'fetch_order_book', 'symbol': symbol, 'limit': None}, True, id=id)
        else:
            return await super().fetch_order_book(symbol, limit, params)
    
    
    def on_trade(self, r):
        """
        {
          k: Message type = "trade"
          v: Array of: [TRADE]
        }
        """
        by_symbol = defaultdict(list)
        for item in r['v']:
            trade = self.parse_trade(item)
            by_symbol[trade['symbol']] += [trade]
        
        for symbol, trades in by_symbol.items():
            self.update_trades([{'symbol': symbol, 'trades': trades}], enable_sub=True)
    
    
    def parse_trade(self, r):
        """
        [TRADE]
          m: Market ID
          d: Date and time of the trade (UTC)                     // 2020-09-09T11:46:38Z
          b: True if the order that triggered the trade is a buy
          a: Amount of the trade
          p: Price of the trade
        """
        side = ['sell','buy'][r['b']]
        datetime = r['d'][:-1] + '.000Z'
        return self.api.trade_entry(symbol=self.convert_symbol(r['m'], 0), side=side,
                                    price=r['p'], amount=r['a'], datetime=datetime,
                                    cost=float(r['p'])*float(r['a']), takerOrMaker='taker',
                                    info=r)
    
    
    def encode(self, rq, sub=None):
        channel = rq.channel
        p = rq.params
        symbol = p.get('symbol')
        other = 'trades' if channel=='orderbook' else 'orderbook'
        is_other_subbed = self.is_subscribed_to((other, symbol))
        is_other_active = self.is_subscribed_to((other, symbol), True)
        
        if channel.startswith('fetch'):
            k = 'request'
        elif sub and not is_other_active:
            k = 'subscribe'
        elif not sub and not is_other_subbed:
            k = 'unsubscribe'
        else:
            return None
        
        return {
            'k': k,
            'v': self.convert_symbol(symbol, 1),
        }
    
    
    def symbol_by_id(self, symbol_id):
        symbol = next((x['symbol'] for x in self.api.markets.values()
                      if x['info'][2]==symbol_id), None)
        if symbol is None:
            raise KeyError(symbol_id)
        return symbol
    
    
    def id_by_symbol(self, symbol):
        return self.api.markets[symbol]['info'][2]
