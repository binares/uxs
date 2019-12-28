from copy import deepcopy


class Position:
    __slots__ = ['symbol','amount','price','leverage',
                 'liquidation_price','bankrupt_price','params']
    
    def __init__(self, api, symbol, amount=0, price=None, leverage=None,
                 liquidation_price=None, bankrupt_price=None, params=None):
        """
        :type api: ccxtWrapper
        :param amount: positive if long, negative if short
        """
        self.api = api
        self.symbol = symbol
        self.amount = amount
        self.price = price
        self.leverage = leverage
        self.params = params if params is not None else {}
            
        self.liquidation_price = liquidation_price if liquidation_price is not None else \
                                 self.calc_liquidation_price()
        self.bankrupt_price = bankrupt_price if bankrupt_price is not None else \
                              self.calc_bankrupt_price()
        
    
    def _calc_liquidation_price(self):
        return None
    
    def calc_liquidation_price(self):
        self.liquidation_price = None if self.price is None else self._calc_liquidation_price()
        return self.liquidation_price
    
    def _calc_bankrupt_price(self):
        return None
    
    def calc_bankrupt_price(self):
        self.bankrupt_price = None if self.price is None else self._calc_bankrupt_price()
        return self.bankrupt_price
    
    
    @staticmethod
    def calc_new(amount, price, leverage, current_avg_price=None, current_amount=0, current_avg_leverage=None):
        # Currently this should only be used with static leverage, as the new average leverage calculation
        # may not be accurate enough (a test placed the liquidation price farther than it actually should have been,
        # which is more dangerous than if it were nearer)
        direction = amount > 0
        new_amount = current_amount + amount
        new_direction = new_amount > 0
        
        if current_avg_price is None:
            new_avg_price = price
            new_avg_lev = leverage
        elif not new_amount:
            new_avg_price = None
            new_avg_lev = None
        elif direction != new_direction:
            new_avg_price, new_avg_lev = current_avg_price, current_avg_leverage \
                                            if abs(current_amount) > abs(amount) else \
                                         price, leverage
        else:
            ratios = [abs(amount / new_amount), abs(current_amount / new_amount)]
            new_avg_price = (price * ratios[0]) + (current_avg_price * ratios[1])
            #Is this right?
            new_avg_lev = new_amount / (amount/leverage + current_amount/current_avg_leverage)
        
        
        return new_amount, new_avg_price, new_avg_lev

    
    def __add__(self, other):
        if self.symbol != other.symbol:
            raise ValueError('Different symbols: {}, {}'.format(self.symbol, other.symbol))
        new_amount, new_avg_price, new_avg_lev = \
            self.calc_new(self.amount, self.price, self.leverage, other.price, other.amount, other.leverage)
        return self.__class__(self.api, self.symbol, new_amount, new_avg_price, new_avg_lev, params=deepcopy(self.params))
    
    
    def __iadd__(self, other):
        if self.symbol != other.symbol:
            raise ValueError('Different symbols: {}, {}'.format(self.symbol, other.symbol))
        new_amount, new_avg_price, new_avg_lev = \
            self.calc_new(self.amount, self.price, self.leverage, other.price, other.amount, other.leverage)
        self.amount = new_amount
        self.price = new_avg_price
        self.leverage = new_avg_lev
        self.liquidation_price = self.calc_liquidation_price()
        self.bankrupt_price = self.calc_bankrupt_price()
        return self
    
    @property
    def liq_price(self):
        return self.liquidation_price
    @liq_price.setter
    def liq_price(self, value):
        self.liquidation_price = value
    @liq_price.deleter
    def liq_price(self):
        del self.liquidation_price