import pandas as pd


class luno:
    def describe(self):
        return self.deep_extend(
            super().describe(), 
            {
                'urls': {
                    'fees_http': 'https://www.luno.com/en/countries'
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

