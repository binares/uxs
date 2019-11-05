import ccxt

class ExchangeSocketError(ccxt.ExchangeError):
    pass

class ConnectionLimit(ExchangeSocketError):
    pass