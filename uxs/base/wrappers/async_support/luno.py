class luno:
    async def fetch_trading_fees(self, params={}):
        await self.load_markets()
        response = await self.fetch(self.urls['fees_http'])
        return self.parse_trading_fees_http_response(response)
