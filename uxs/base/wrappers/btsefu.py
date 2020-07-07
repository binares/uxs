class btsefu:
    def describe(self):
        config = {
            'options': {
                'defaultType': 'future',
            },
            'fees': {
                'trading': {
                    'taker': 0.0006,
                    'maker': -0.0001,
                },
            }
        }
        return self.deep_extend(super().describe(), config)
    
    
    # def _get_lot_size(self, market):
    #    return market['info'].get('contractSize')
    
    
    #def _get_pnl_function(self, market):
    #    return 'linear'
    
    
    def _get_settle_currency(self, market):
        # there seems to be various quanto options for each market,
        # but for simplication let's use quote currency
        # https://support.btse.com/en/support/solutions/articles/43000531774-contract-specifications
        return market['quote'] 
