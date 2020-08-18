class luno:
    async def fetch_trading_fees(self, params={}):
        await self.load_markets()
        response = await self.fetch(self.urls['fees_http'])
        return self.parse_trading_fees_http_response(response)
    
    async def fetch_markets(self, params={}):
        markets = await super().fetch_markets(params)
        for i in range(len(markets)):
            markets[i] = self.deep_extend(markets[i], self.safe_value(self.options['markets'], markets[i]['symbol'], {}))
        return markets
