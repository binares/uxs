from fons.dict_ops import deep_update


class binancefu:
    def __init__(self, config={}, *args, **kw):
        change = {
            'options': {
                'fetchTime': {'defaultType': 'future'},
                'fetchMarkets': {'defaultType': 'future'},
                'fetchBalance': {'defaultType': 'future'},
                'fetchOpenOrders': {'defaultType': 'future'},
            }
        }
        config = deep_update(config, change, copy=True)
        
        super().__init__(config, *args, **kw)