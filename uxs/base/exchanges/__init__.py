"""
Exchanges that are not present in ccxt
"""
import ccxt

from .biki import biki
from .bitforexfu import bitforexfu
from .bitzfu import bitzfu
from .cryptocom import cryptocom
from .delta import delta
from .gateiofu import gateiofu
from .krakenfu import krakenfu
from .primexbt import primexbt
from .slicex import slicex

# Add the custom-defined exchanges to ccxt
for attr,value in list(globals().items()):
    if isinstance(value, type) and issubclass(value, ccxt.Exchange):
        setattr(ccxt, attr, value)
