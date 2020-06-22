"""
Exchanges that are not present in ccxt
"""
import ccxt

from ._58coin import _58coin
from .bcio import bcio
from .biki import biki
from .bitbns import bitbns
from .bitcoincom import bitcoincom
from .bitforexfu import bitforexfu
from .bitget import bitget
from .bitopro import bitopro
from .bitpanda import bitpanda
from .bitrue import bitrue
from .bitzfu import bitzfu
from .bkex import bkex
from .ceo import ceo
from .coinbene import coinbene
from .coindcx import coindcx
from .coinsbit import coinsbit
from .cryptocom import cryptocom
from .delta import delta
from .dragonex import dragonex
from .felixo import felixo
from .gateiofu import gateiofu
from .krakenfu import krakenfu
from .nominex import nominex
from .primexbt import primexbt
from .slicex import slicex
from .tokensnet import tokensnet
from .tradeogre import tradeogre
from .txbit import txbit
from .yunex import yunex

# Add the custom-defined exchanges to ccxt
for attr,value in list(globals().items()):
    if isinstance(value, type) and issubclass(value, ccxt.Exchange):
        setattr(ccxt, attr, value)
