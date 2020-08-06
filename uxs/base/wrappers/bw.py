
class bw:
    def parse_order(self, order, market=None):
        parsed = super().parse_order(order, market)
        parsed['cost'] = self.safe_float(parsed['info'], 'completeTotalMoney')
        
        if parsed['cost'] and parsed['filled'] and parsed['filled'] > 0:
            parsed['average'] = parsed['cost'] / parsed['filled']
        
        return parsed

