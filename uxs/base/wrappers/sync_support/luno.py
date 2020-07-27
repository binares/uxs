class luno:
    def fetch_trading_fees(self, params={}):
        self.load_markets()
        response = self.fetch(self.urls['fees_http'])
        return self.parse_trading_fees_http_response(response)
