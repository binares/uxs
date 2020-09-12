class southxchange:
    async def fetch_markets(self, params={}):
        markets = await super().fetch_markets(params)
        for i in range(len(markets)):
            markets[i] = self.deep_extend(markets[i], self.safe_value(self.options['markets'], markets[i]['symbol'], {}))
        return markets
    
    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        o = await super().create_order(symbol, type, side, amount, price, params)
        o['id'] = o['id'].strip('"')
        return o
