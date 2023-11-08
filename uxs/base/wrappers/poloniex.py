class poloniex:
    def parse_trade(self, trade, market=None):
        trade = super().parse_trade(trade, market)
        if "tradeID" in trade["info"]:
            trade["id"] = trade["info"][
                "tradeID"
            ]  # websocket sends only fill's tradeID
        return trade
