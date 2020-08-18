import pandas as pd


class luno:
    def describe(self):
        return self.deep_extend(
            super().describe(), 
            {
                'urls': {'fees_http': 'https://www.luno.com/en/countries'},
                'options': {
                    'markets': {
                        'BTC/AUD': {
                            'precision': {'price': 2, 'amount': 4},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/EUR': {
                            'precision': {'price': 2, 'amount': 4},
                            'limits': {
                                'amount': {'min': 0.0005, 'max': 100},
                                'price': {'min': 100, 'max': 100000},
                            },
                        },
                        'BTC/GBP': {
                            'precision': {'price': 2, 'amount': 4},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/IDR': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/MYR': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/NGN': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/SGD': {
                            'precision': {'price': 2, 'amount': 4},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/UGX': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/ZAR': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'BTC/ZMV': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'ETH/AUD': {
                            'precision': {'price': 2, 'amount': 4},
                            'limits': {'amount': {'min': 0.001}},
                        },
                        'ETH/BTC': {
                            'precision': {'price': 6, 'amount': 2},
                            'limits': {
                                'amount': {'min': 0.01, 'max': 1000},
                                'price': {'min': 0.0001, 'max': 1},
                            },
                        },
                        'ETH/EUR': {
                            'precision': {'price': 2, 'amount': 4},
                            'limits': {
                                'amount': {'min': 0.001, 'max': 1000},
                                'price': {'min': 10, 'max': 100000},
                            },
                        },
                        'ETH/GBP': {
                            'precision': {'price': 2, 'amount': 4},
                            'limits': {'amount': {'min': 0.01}},
                        },
                        'ETH/IDR': {
                            'precision': {'price': 0, 'amount': 4},
                            'limits': {'amount': {'min': 0.001}},
                        },
                        'ETH/MYR': {
                            'precision': {'price': 0, 'amount': 2},
                            'limits': {'amount': {'min': 0.01}},
                        },
                        'ETH/NGN': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.005}},
                        },
                        'ETH/ZAR': {
                            'precision': {'price': 0, 'amount': 6},
                            'limits': {'amount': {'min': 0.0005}},
                        },
                        'XRP/BTC': {
                            'precision': {'price': 8, 'amount': 0},
                            'limits': {
                                'amount': {'min': 1, 'max': 100000},
                                'price': {'min': 0.000001, 'max': 1},
                            },
                        },
                        'XRP/MYR': {
                            'precision': {'price': 4, 'amount': 0},
                            'limits': {'amount': {'min': 1}},
                        },
                        'XRP/NGN': {
                            'precision': {'price': 2, 'amount': 0},
                            'limits': {'amount': {'min': 1}},
                        },
                        'XRP/ZAR': {
                            'precision': {'price': 2, 'amount': 0},
                            'limits': {'amount': {'min': 1}},
                        },
                        'LTC/BTC': {
                            'precision': {'price': 6, 'amount': 2},
                            'limits': {
                                'amount': {'min': 0.01, 'max': 1000},
                                'price': {'min': 0.000001, 'max': 1},
                            },
                        },
                        'LTC/MYR': {
                            'precision': {'price': 0, 'amount': 4},
                            'limits': {'amount': {'min': 0.001}},
                        },
                        'LTC/NGN': {
                            'precision': {'price': 0, 'amount': 4},
                            'limits': {'amount': {'min': 0.001}},
                        },
                        'LTC/ZAR': {
                            'precision': {'price': 0, 'amount': 4},
                            'limits': {'amount': {'min': 0.001}},
                        },
                        'BCH/BTC': {
                            'precision': {'price': 6, 'amount': 2},
                            'limits': {
                                'amount': {'min': 0.01, 'max': 100},
                                'price': {'min': 0.0001, 'max': 1},
                            },
                        },
                    },
                },
            }
        )
    
    
    def parse_trading_fees_http_response(self, http_response):
        tables = pd.read_html(http_response)
        table = next(t for t in tables if 'Market' in t.columns and 'Tier 1' in t.columns)
        self._fees_df = table
        def parse_percentage(x):
            x = x.strip('%*')
            return float(x) / 100
        fees = {}
        for i, r in table.iloc[:, :3].iterrows():
            #symbol, taker, maker = r
            symbol = r['Market'].values[0]
            if self.markets_by_id and symbol in self.markets_by_id:
                symbol = self.markets_by_id[symbol]['symbol']
            maker = r[slice(None),'Maker Fee'].values[0]
            taker = r[slice(None),'Taker Fee'].values[0]
            fees[symbol] = {
                'taker': parse_percentage(taker),
                'maker': parse_percentage(maker),
                'info': None,
            }
        return fees
    
    
    def parse_trade(self, trade, market=None):
        parsed = super().parse_trade(trade, market)
        sequence = self.safe_string(parsed['info'], 'sequence')
        return dict(parsed, id=self._create_trade_id(parsed, sequence))
    
    
    def _create_trade_id(self, trade, sequence):
        return '{}+{}+{}+{}+{}'.format(sequence, trade['symbol'], trade['side'], trade['amount'], trade['price'])

