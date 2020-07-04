"""
Long:
    Bankrupt = avg_entry_price / (1 + initial_margin)
    Liquidation = bankrupt + (avg_entry_price * (maintenance_margin + funding_rate)
Short:
    Bankrupt = avg_entry_price / (1 - initial_margin)
    Liquidation = bankrupt - (avg_entry_price * (maintenance_margin - funding_rate)
For margin calculations:
    initial_margin = (1 / leverage) - taker_fee - taker_fee
    maintenance_margin = 0.5% * taker_fee
"""

from uxs.fintls.basics import as_direction
from uxs.fintls.margin import Position
from collections import namedtuple


class bitmexPosition(Position):
    
    def _calc_bankrupt_price(self, *, round=True):
        self.verify_is_perpetual()
        mp = 1 if self.amount > 0 else -1
        taker_fee = self.api.markets[self.symbol]['taker']
        bankrupt_price = self.price / (1 + mp*self._initial_margin(self.leverage, taker_fee))
        if round:
            method = 'up' if self.amount > 0 else 'down'
            bankrupt_price = self.api.round_price(self.symbol, bankrupt_price, method=method)
        return bankrupt_price
    
    
    def _calc_liquidation_price(self):
        self.verify_is_perpetual()
        taker_fee = self.api.markets[self.symbol]['taker']
        funding_rate = self.api.markets[self.symbol]['funding_rate']
        maintenance_margin = 0.005 - taker_fee
        
        mp = 1 if self.amount > 0 else -1
        bankrupt_price = self._calc_bankrupt_price(round=False)
        liq = bankrupt_price + mp*(self.price * (maintenance_margin + mp*funding_rate))
        
        method = 'up' if self.amount > 0 else 'down'
        
        return self.api.round_price(self.symbol, liq, method=method)
    
    
    @staticmethod
    def _initial_margin(lev, taker_fee):
        return (1 / lev) - taker_fee * 2
    
    
    def verify_is_perpetual(self):
        if self.symbol not in ('BTC/USD','ETH/USD'):
            raise ValueError('The contract must be perpetual swap. Got symbol: {}'.format(self.symbol))
        
        
class bitmex:
    
    Position = bitmexPosition
    
    def _get_lot_size(self, market):
        return market['info'].get('lotSize')
    
    
    def _get_pnl_function(self, market):
        if market.get('prediction'):
            return None
        return 'inverse' if market['info']['isInverse'] else 'linear'
    
    
    def _get_settle_currency(self, market):
        return self.safe_currency_code(market['info'].get('settlCurrency'))


if __name__ == '__main__':
    from uxs import ccxtWrapper as _ccxtWrapper
    
    class _bitmex(_ccxtWrapper):
        from ccxt import TICK_SIZE
        precisionMode = TICK_SIZE
        
        def __init__(self, taker, funding_rate):
            self.markets = {'BTC/USD': {'taker': taker,
                                        'funding_rate': funding_rate,
                                        'precision': {'price': 0.5}}}
        def __del__(self):
            pass
            
    maker = -0.00025
    taker = 0.00075
    funding = -0.000139
    data = {
        #i: [amount, price, lev, taker, funding], result | correct
        0: [1000, 5000, 25, taker, funding], #4835.5 | 4831.0 )
        1: [-1000, 5000, 25, taker, funding], #5178.0 | 5180.5
        2: [1000, 5000, 25, taker, funding, 10, 7613, 25], #4860.5
        3: [1000, 5000, 25, taker, funding, 10, 7613, 2], #4839.5 | 4847.0 (the current amount was on 2x lev)
        4: [1000, 5000, 1, taker, 0.00001], #2523.5 | 2506.5
    }
    for i,args in data.items():
        api = _bitmex(*args[3:5])
        p1 = bitmexPosition(api, 'BTC/USD', *args[:3])
        p2 = bitmexPosition(api, 'BTC/USD', *args[5:])
        print(p1.liquidation_price, p2.liquidation_price)
        p3 = p1 + p2
        print(i, round(p3.liquidation_price, 1))