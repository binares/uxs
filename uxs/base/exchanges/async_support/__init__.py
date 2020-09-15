"""
Async versions of exchanges that are not present in ccxt
"""
import ccxt.async_support

from ._58coin import _58coin
from .bcio import bcio
from .beaxy import beaxy
from .biki import biki
from .bitbns import bitbns
from .bitclude import bitclude
from .bitcoincom import bitcoincom
from .bitforexfu import bitforexfu
from .bitget import bitget
from .bitkub import bitkub
from .bitopro import bitopro
from .bitpanda import bitpanda
from .bitrue import bitrue
from .bitzfu import bitzfu
from .bkex import bkex
from .btse import btse
from .ceo import ceo
from .coinbene import coinbene
from .coindcx import coindcx
from .coinsbit import coinsbit
from .coinsuper import coinsuper
from .cossdex import cossdex
from .cryptocom import cryptocom
from .delta import delta
from .dragonex import dragonex
from .equos import equos
from .felixo import felixo
from .foblgate import foblgate
from .gateiofu import gateiofu
from .krakenfu import krakenfu
from .mxc import mxc
from .nominex import nominex
from .primexbt import primexbt
from .remitano import remitano
from .silgonex import silgonex
from .slicex import slicex
from .tokenomy import tokenomy
from .tokensnet import tokensnet
from .tradeogre import tradeogre
from .txbit import txbit
from .vinex import vinex
from .vitex import vitex
from .wazirx import wazirx
from .yunex import yunex

# Add the custom-defined exchanges to ccxt.async_support
for attr,value in list(globals().items()):
    if isinstance(value, type) and issubclass(value, ccxt.async_support.Exchange):
        setattr(ccxt.async_support, attr, value)
