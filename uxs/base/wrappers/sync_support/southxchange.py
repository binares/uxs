class southxchange:
    def fetch_markets(self, params={}):
        markets = super().fetch_markets(params)
        for i in range(len(markets)):
            markets[i] = self.deep_extend(markets[i], self.safe_value(self.options['markets'], markets[i]['symbol'], {}))
        return markets
    
    def create_order(self, symbol, type, side, amount, price=None, params={}):
        o = super().create_order(symbol, type, side, amount, price, params)
        o['id'] = o['id'].strip('"')
        return o
