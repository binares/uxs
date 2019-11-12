import os
import pytest

from .conftest import _init
from uxs.base.poll import (save, create_new, load, clear_cache)

test_dir, settings_test_path = _init()


EXCHANGES = ['binance','bittrex','kucoin']
TYPES = ['orderbook','tickers']

DATA = [
    [
        ('binance', ('orderbook','BTC/USDT',)),
        {
            'symbol': 'BTC/USDT',
            'datetime': '2019/09/11T12:51:21.092',
            'timestamp': 1568206281092,
            'bids': [[1,2],[3,4]],
            'asks': [[4,5],[6,7]],
            'nonce': 1,
        }
    ],
    [
        ('binance', 'tickers'),
        {
            'ETH/BTC': {'symbol': 'ETH/BTC',
                        'last': 0.013,
                        'bid': 0.01298,},
            'BTC/USD': {'symbol': 'BTC/USD',
                        'last': 8842,
                        'ask': 8844.1},
        }
    ]
]

@pytest.mark.parametrize('id, data', DATA)
def test_save_and_load(id, data, init):
    exchange, type = id
    fnInf = create_new(exchange, type, data=data)
    save([fnInf])
    
    fnInf2 = load(exchange, type, limit=-1, max=1, globals=False)[0]
    
    assert fnInf == fnInf2
    

def test_clear_cache(init):
    clear_cache(EXCHANGES, TYPES)
    
    dirs = [os.path.join(test_dir, x) for x in os.listdir(test_dir)
            if os.path.isdir(os.path.join(test_dir, x))]
    
    assert len(dirs) == 0