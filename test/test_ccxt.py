import pytest

from uxs.base.ccxt import ccxtWrapper
from ccxt import TICK_SIZE

re_params = [
    [26.5, 0, 'ceil', None, 27.0],
    [26.5, 0, 'truncate', None, 26.0],
    [26.5, 0, 'round', None, 26.0],
    [27.5, 0, 'round', None, 28.0],
    [26.5, 0.5, 'ceil', TICK_SIZE, 26.5],
    [26.5, 0.5, 'truncate', TICK_SIZE, 26.5],
    [26.5, 0.5, 'round', TICK_SIZE, 26.5],
    [26.82, 0.5, 'ceil', TICK_SIZE, 27.0],
    [26.82, 0.5, 'truncate', TICK_SIZE, 26.5],
    [26.82, 0.5, 'round', TICK_SIZE, 27.0],
    [26.68, 0.5, 'ceil', TICK_SIZE, 27.0],
    [26.68, 0.5, 'truncate', TICK_SIZE, 26.5],
    [26.68, 0.5, 'round', TICK_SIZE, 26.5],
    [26.75, 0.5, 'ceil', TICK_SIZE, 27.0],
    [26.75, 0.5, 'truncate', TICK_SIZE, 26.5],
    #when x is exactly in the middle, and we use
    #"round" + TICK_SIZE, it always seems to round up
    [26.75, 0.5, 'round', TICK_SIZE, 27.0],
    [26.25, 0.5, 'round', TICK_SIZE, 26.5],
]

@pytest.mark.parametrize('x,precision,method,precisionMode,expected', re_params)
def test_round_entity(x, precision, method, precisionMode, expected):
    assert ccxtWrapper.round_entity(x, precision, method, precisionMode=precisionMode) == expected