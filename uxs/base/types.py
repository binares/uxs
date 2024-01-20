from typing import Dict
from collections import namedtuple

from .ccxt import _ccxtWrapper, _asyncCcxtWrapper

Api = _asyncCcxtWrapper
Apis = Dict[str, Api]

ApiSync = _ccxtWrapper
ApisSync = Dict[str, ApiSync]

fnInf = namedtuple("fnInf", "exchange type date file data")
fnInf.__new__.__defaults__ = (None,) * len(fnInf._fields)

prInf = namedtuple("prInf", "profile exchange type start end file data")
prInf.__new__.__defaults__ = (None,) * len(prInf._fields)
