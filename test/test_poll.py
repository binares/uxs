import os
import pytest

from .conftest import _init
from uxs import set_enable_caching
from uxs.base.ccxt import get_sn_exchange
from uxs.base.poll import save, create_new, load, clear_cache

test_dir, settings_test_path = _init()


EXCHANGES = ["binance", "bittrex", "kucoin"]
TYPES = ["orderbook", "tickers"]

DATA = [
    [
        ("binance", "markets"),
        {
            "BTC/USDT": {
                "id": "BTCUSDT",
                "lowercaseId": "btcusdt",
                "symbol": "BTC/USDT",
                "base": "BTC",
                "quote": "USDT",
                "settle": None,
                "baseId": "BTC",
                "quoteId": "USDT",
                "settleId": None,
                "type": "spot",
                "spot": True,
                "margin": True,
                "swap": False,
                "future": False,
                "option": False,
                "index": None,
                "active": True,
                "contract": False,
                "linear": None,
                "inverse": None,
                "subType": None,
                "taker": 0.001,
                "maker": 0.001,
                "contractSize": None,
                "expiry": None,
                "expiryDatetime": None,
                "strike": None,
                "optionType": None,
                "precision": {
                    "amount": 5,
                    "price": 2,
                    "cost": None,
                    "base": 8,
                    "quote": 8,
                },
                "limits": {
                    "leverage": {"min": None, "max": None},
                    "amount": {"min": 1e-05, "max": 9000.0},
                    "price": {"min": 0.01, "max": 1000000.0},
                    "cost": {"min": 5.0, "max": 9000000.0},
                    "market": {"min": 0.0, "max": 80.30219504},
                },
                "created": None,
                "info": {},
            },
        },
    ],
    [
        (
            "binance",
            (
                "orderbook",
                "BTC/USDT",
            ),
        ),
        {
            "symbol": "BTC/USDT",
            "datetime": "2019/09/11T12:51:21.092",
            "timestamp": 1568206281092,
            "bids": [[1, 2], [3, 4]],
            "asks": [[4, 5], [6, 7]],
            "nonce": 1,
        },
    ],
    [
        ("binance", "tickers"),
        {
            "ETH/BTC": {
                "symbol": "ETH/BTC",
                "last": 0.013,
                "bid": 0.01298,
            },
            "BTC/USD": {"symbol": "BTC/USD", "last": 8842, "ask": 8844.1},
        },
    ],
]


@pytest.mark.parametrize("id, data", DATA)
def test_save_and_load(id, data, init):
    exchange, type = id
    fnInf = create_new(exchange, type, data=data)
    save([fnInf])

    fnInf2 = load(exchange, type, limit=-1, max=1, globals=False)[0]

    assert fnInf == fnInf2


def test_load_markets():
    set_enable_caching({"markets": True})
    exchange = "kucoin"
    api = get_sn_exchange({"exchange": exchange})
    markets = api.poll_load_markets(-1)

    # Now fetch them from storage
    fnInf = load(exchange, "markets", limit=-1, max=1, globals=False)[0]
    assert fnInf.data == markets


def test_clear_cache(init):
    clear_cache(EXCHANGES, TYPES)

    dirs_inside_test_dir = [
        os.path.join(test_dir, x)
        for x in os.listdir(test_dir)
        if os.path.isdir(os.path.join(test_dir, x))
    ]

    assert len(dirs_inside_test_dir) == 0
