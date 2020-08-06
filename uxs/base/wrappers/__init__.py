"""
Wrapping ccxt exchanges with 
 1. custom settings
 2. extra methods
 3. changed methods
E.g. binancefu extends a config passed to ccxt.binance with {'options': ...} which
directs the endpoints to Binance Futures (instead of the default one, Binance "spot").
"""

from .binancefu import binancefu
from .bitmex import bitmex
from .bittrex import bittrex
from .btsefu import btsefu
from .bw import bw
from .coinsbit import coinsbit
from .dragonex import dragonex
from .hitbtc import hitbtc
from .krakenfu import krakenfu
from .luno import luno

__all__ = [
    'binancefu',
    'bitmex',
    'bittrex',
    'btsefu',
    'bw',
    'coinsbit',
    'dragonex',
    'hitbtc',
    'krakenfu',
    'luno',
]
