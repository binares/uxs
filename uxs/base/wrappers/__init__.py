"""
Wrapping ccxt exchanges with 
 1. custom settings
 2. extra methods
 3. changed methods
E.g. binancefu extends a config passed to ccxt.binance with {'options': ...} which
directs the endpoints to Binance Futures (instead of the default one, Binance "spot").
"""

from .binancefutures import binancefutures
from .bitmex import bitmex
from .btsefu import btsefu
from .coinbene import coinbene
from .dragonex import dragonex
from .hitbtc import hitbtc
from .krakenfutures import krakenfutures
from .luno import luno
from .poloniex import poloniex

__all__ = [
    "binancefutures",
    "bitmex",
    "btsefu",
    "coinbene",
    "dragonex",
    "hitbtc",
    "krakenfutures",
    "luno",
    "poloniex",
]
