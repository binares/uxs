class bittrex:
    def describe(self):
        config = {
            'hostname': 'global.bittrex.com',
        }
        return self.deep_extend(super().describe(), config)
    
    
    def parse_order(self, order, market=None):
        order = super().parse_order(order, market)
        order['type'] = self._parse_order_type(order.get('type'))
        return order
    
    
    def _parse_order_type(self, type):
        # LIMIT, MARKET, CEILINT_LIMIT, CEILING_MARKET
        if type is not None:
            type = type.lower()
            if 'market' in type:
                return 'market'
            elif 'limit' in type:
                return 'limit'
        return type
