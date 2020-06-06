"""
Async versions of exchanges that are not present in ccxt
"""
import ccxt.async_support

from .bitforexfu import bitforexfu
from .bitzfu import bitzfu
from .cryptocom import cryptocom
from .delta import delta
from .gateiofu import gateiofu
from .krakenfu import krakenfu
from .primexbt import primexbt
from .slicex import slicex

# Add the custom-defined exchanges to ccxt.async_support
for attr,value in list(globals().items()):
    if isinstance(value, type) and issubclass(value, ccxt.async_support.Exchange):
        setattr(ccxt.async_support, attr, value)
