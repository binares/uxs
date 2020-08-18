class luno:
    def fetch_trading_fees(self, params={}):
        self.load_markets()
        response = self.fetch(self.urls['fees_http'])
        return self.parse_trading_fees_http_response(response)
    
    def fetch_markets(self, params={}):
        markets = super().fetch_markets(params)
        for i in range(len(markets)):
            markets[i] = self.deep_extend(markets[i], self.safe_value(self.options['markets'], markets[i]['symbol'], {}))
        return markets
