from fons.dict_ops import deep_update


class hitbtc:
    def __init__(self, config={}, *args, **kw):
        changed = {
            'commonCurrencies': {
                'BCC': 'Bitconnect',
            },
        }
        config = deep_update(config, changed, copy=True)
        
        super().__init__(config, *args, **kw)
