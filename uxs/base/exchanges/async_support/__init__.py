"""
Async versions of exchanges that are not present in ccxt
"""
import ccxt.async_support

from .bcio import bcio
from .biki import biki
from .bitbns import bitbns
from .bitcoincom import bitcoincom
from .bitforexfu import bitforexfu
from .bitpanda import bitpanda
from .bitzfu import bitzfu
from .bkex import bkex
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
from .tokensnet import tokensnet
from .txbit import txbit

# Add the custom-defined exchanges to ccxt.async_support
for attr,value in list(globals().items()):
    if isinstance(value, type) and issubclass(value, ccxt.async_support.Exchange):
        setattr(ccxt.async_support, attr, value)
