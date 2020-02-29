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
    
    
    def _get_lot_size(self, market):
        return 1 if market.get('type')=='future' else None
