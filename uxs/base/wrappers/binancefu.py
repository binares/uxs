class binancefu:
    def describe(self):
        config = {
            'options': {
                'defaultType': 'future',
                'fetchTradesMethod': 'fapiPublicGetAggTrades',
                'fetchTickersMethod': 'fapiPublicGetTicker24hr',
            },
            'fees': {
                'trading': {
                    'taker': 0.0004,
                    'maker': 0.0002,
                },
            }
        }
        return self.deep_extend(super().describe(), config)