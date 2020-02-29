"""
Exchanges that are not present in ccxt
"""
import ccxt

from .bitforexfu import bitforexfu
from .delta import delta

# Add the custom-defined exchanges to ccxt
for attr,value in list(globals().items()):
    if isinstance(value, type) and issubclass(value, ccxt.Exchange):
        setattr(ccxt, attr, value)
