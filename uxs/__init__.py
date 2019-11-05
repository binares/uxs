from uxs.base.socket import (ExchangeSocket, ExchangeSocketError)

from uxs.base.ccxt import (get_name, get_exchange, init_exchange, 
                            list_exchanges, ccxtWrapper, asyncCCXTWrapper)

from uxs.base.auth import (read_tokens, get_auth, get_auth2)

from uxs.base import poll


from uxs.binance import binance
from uxs.bittrex import bittrex
from uxs.hitbtc import hitbtc
from uxs.kraken import kraken
from uxs.kucoin import kucoin
from uxs.poloniex import poloniex



def get_socket_cls(exchange):
    socket_cls = globals().get(exchange.lower(), None)
    
    if not isinstance(socket_cls, type) or not issubclass(socket_cls, ExchangeSocket):
        raise ValueError(exchange)
    
    return socket_cls


def get_socket(exchange, args=None, kwargs=None):
    """:rtype: ExchangeSocket"""
    #instead of args can also directly pass config (dict)
    if args is None: args = tuple()
    elif isinstance(args,dict): args = (args,)
    if kwargs is None: kwargs = {}
    socket_cls = get_socket_cls(exchange)
    
    return socket_cls(*args,**kwargs)