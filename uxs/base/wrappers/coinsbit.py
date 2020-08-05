
class coinsbit:
    def parse_trade(self, trade, market=None):
        parsed = super().parse_trade(trade, market)
        if parsed['id'] is None:
            parsed['id'] = self._create_trade_id(parsed)
        return parsed
    
    
    def _create_trade_id(self, trade):
        return '{}+{}+{}+{}+{}'.format(trade['symbol'], trade['side'], trade['amount'], trade['price'], trade['timestamp'])

