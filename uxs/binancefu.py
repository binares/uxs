from uxs.binance import binance
from fons.crypto import nonce_ms

class binancefu(binance):
    exchange = 'binancefu'
    url_components = {
        'ws': 'wss://fstream.binance.com/ws',
        'test': 'wss://stream.binancefuture.com/ws',
    }
    channel_defaults = {
        'send': False, #True
    }
    channels = {
        'all_tickers': {
            #'url': '<$ws>',
        },
        'account': {
            #'url': '<$ws>/<m$fetch_listen_key>',
            'send': False,
        }
    }
    connection_defaults = {
        'ping_interval': 55,
        #'ping_timeout': 5, #is it ponged?
    }
    has = {
        'account': {'position': True},
        'ticker': {'bid': False, 'ask': False, 'bidVolume': False,
                   'askVolume': False, 'previousClose': False},
    }
    has['all_tickers'] = has['ticker'].copy()
    
    message = {
        'id': {'key': 'id'},
        'error': {},
    }
    
    def setup_test_env(self):
        return dict(super().setup_test_env(), **{
            'ccxt_config': {
                'urls': {
                    'api': {
                        'fapiPublic': 'https://testnet.binancefuture.com/fapi/v1',
                        'fapiPrivate': 'https://testnet.binancefuture.com/fapi/v1',
                    },
                },
            },
            'ccxt_test': False,
        })
    
    def on_futures_account(self, r):
        """{
          "e": "ACCOUNT_UPDATE",        // Event Type
          "E": 1564745798939,            // Event Time
          "T": 1564745798938,               // Transaction 
          "a":                        
            {
              "B":[                     // Balances
                {
                  "a":"USDT",           // Asset
                  "wb":"122624"         // Wallet Balance
                },
                {
                  "a":"BTC",           
                  "wb":"0"         
                }
              ],
              "P":[                      // Positions
                {
                  "s":"BTCUSDT",         // Symbol
                  "pa":"1",              // Position Amount
                  "ep":"9000",           // Entry Price
                  "cr":"200",             // (Pre-fee) Accumulated Realized
                  "up":"0.2732781800"    // Unrealized PnL
                }
              ]
            }
        }"""
        data = r['a']
        
        #Balance
        if 'B' in data:
            updates = [(
                self.convert_cy(x['a'], 0),
                float(x['wb']),
                0,
                ) for x in data['B']
            ]
            self.update_balances(updates)
        
        #Position
        _null = lambda x: x
        apply = dict.fromkeys(['amount','price','rPnL','uPnL'], float)
        map = {'amount': 'pa',
               'price': 'ep',
               'rPnL': 'cr',
               'uPnL': 'up'}
        
        if 'P' in data:
            positions = []
            for x in data['P']:
                symbol = self.convert_symbol(x['s'], 0)
                mapped = {k: apply[k](x[map[k]]) for k in map.keys() if map[k] in x}
                if 'price' in mapped and not mapped['price']:
                    mapped['price'] = None
                e = self.api.position_entry(symbol=symbol, timestamp=r['E'], **mapped, info=x)
                positions.append(e)
            self.update_positions(positions)
    
    
    def on_futures_order(self, r):
        """
        Execution Type
            NEW
            PARTIAL_FILL
            FILL
            CANCELED
            REJECTED
            CALCULATED - Liquidation Execution
            EXPIRED
            TRADE
            RESTATED
        
        Order Status
            NEW
            PARTIALLY_FILLED
            FILLED
            CANCELED
            REPLACED
            STOPPED
            REJECTED
            EXPIRED
            NEW_INSURANCE - Liquidation with Insurance Fund
            NEW_ADL - Counterparty Liquidation`
        
        Time in force
            GTC
            IOC
            FOK
            GTX
        
        Working Type
            MARK_PRICE
            CONTRACT_PRICE
        
        {
          "e":"ORDER_TRADE_UPDATE",     // Event Type
          "E":1568879465651,            // Event Time
          "T": 1568879465650,           //  Transaction Time
          "o":{                             
            "s":"BTCUSDT",              // Symbol
            "c":"TEST",                 // Client Order Id
            "S":"SELL",                 // Side
            "o":"LIMIT",                // Order Type
            "f":"GTC",                  // Time in Force
            "q":"0.001",                // Original Quantity
            "p":"9910",                 // Price
            "ap":"0",                   // Average Price
            "sp":"0",                   // Stop Price
            "x":"NEW",                  // Execution Type
            "X":"NEW",                  // Order Status
            "i":8886774,                // Order Id
            "l":"0",                    // Order Last Filled Quantity
            "z":"0",                    // Order Filled Accumulated Quantity
            "L":"0",                    // Last Filled Price
            "N": "USDT",                // Commission Asset, will not push if no commission
            "n": "0",                   // Commission, will not push if no commission
            "T":1568879465651,          // Order Trade Time
            "t":0,                      // Trade Id
            "b":"0",                    // Bids Notional
            "a":"9.91",                 // Ask Notional
            "m": False,                 // Is this trade the maker side?
            "R":false,                  // Is this reduce only
            "wt": "CONTRACT_PRICE"      // stop price working type
          }
        }
        """
        rr = r['o']
        
        price = float(rr['p'])
        stop = float(rr['sp'])
        average = float(rr['ap'])
        
        d = {
            'id': str(rr['i']), #ccxt parses to str
            'symbol': self.convert_symbol(rr['s'], 0),
            'type': rr['o'].lower(),
            'side': rr['S'].lower(),
            'price': price if price else None,
            'amount': float(rr['q']),
            'timestamp': r['E'],
            'stop': stop if stop else None,
            'average': average if average else None,
            'filled': float(rr['z']),
        }
        
        if rr['X'] in ('CANCELED','REJECTED') or rr['X']=='EXPIRED' and not d['stop']:
            d['remaining'] = 0
        else:
            d['remaining'] = d['amount'] - d['filled']
            
        #d['payout'] = d['filled'] if d['side'] == 'buy' else float(rr['Z'])
        
        o = None
        try: o = self.orders[d['id']]
        except KeyError: pass
        
        if o is None:
            self.add_order(**d)
        else:
            extra = {k:v for k,v in d.items() if k in ['type','side','price','stop','amount']}
            self.update_order(d['id'], d['remaining'], d['filled'], average=d['average'], params=extra)
            
            
    def encode(self, rq, sub=None):
        method = None
        if sub:
            method = 'SUBSCRIBE'
        elif sub is False:
            method = 'UNSUBSCRIBE'
            
        channel = rq.channel
        params = rq.params
        timeframe = params.get('timeframe','')
        
        map = {
            'ticker': ['<symbol>@ticker'],
            'all_tickers': ['!ticker@arr'], #{symbol}!ticker@arr ?
            'orderbook': ['<symbol>@depth'],
            'trades': ['<symbol>@aggTrade'],
            'ohlcv': ['<symbol>@kline_{}'.format(timeframe)],
        }
        
        args = self.fill_in_params(map[channel], params.get('symbol'))
        id = nonce_ms()
        
        return ({
            'method': method,
            'params': args,
            'id': id,
        }, id)
