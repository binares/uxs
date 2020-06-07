"""
Exchanges that are not present in ccxt
"""
import ccxt

from .biki import biki
from .bitcoincom import bitcoincom
from .bitforexfu import bitforexfu
from .bitpanda import bitpanda
from .bitzfu import bitzfu
from .coinbene import coinbene
from .coindcx import coindcx
from .coinsbit import coinsbit
from .cryptocom import cryptocom
from .delta import delta
from .dragonex import dragonex
from .gateiofu import gateiofu
from .krakenfu import krakenfu
from .primexbt import primexbt
from .slicex import slicex

# Add the custom-defined exchanges to ccxt
for attr,value in list(globals().items()):
    if isinstance(value, type) and issubclass(value, ccxt.Exchange):
        setattr(ccxt, attr, value)
