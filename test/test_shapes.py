import uxs
from uxs.fintls import shapes_old, shapes
from fons.log import quick_logging
import itertools
import time
import logging

logging.getLogger("uxs.shapes").setLevel(logging.DEBUG)
logging.getLogger("uxs.shapes").addHandler(logging.StreamHandler())
# quick_logging(2, True)


def test_get_shapes_poc():
    pass


"""
EXCHANGES = ["okx", "binance", "kucoin"]

shapes (new):
Finding 9063 currency shapes took 4.04 seconds
Finding 20999 shapes took 0.06 seconds
Initiating 20999 shapes took 3.21 seconds

Fetching 20999 (1059, 19940) shapes for module uxs.fintls.shapes took 9.75 seconds

shapes (w/ cython):
Finding 9063 currency shapes took 1.91 seconds # Cython
Initiating 20999 shapes took 2.64 seconds  # shapes_cython._init_cys()

shapes (old):
Fetching 17448 (1059, 16389) shapes for module uxs.fintls.shapes_old took 64.93 seconds
"""


def test_get_shapes():
    EXCHANGES = ["okx", "binance", "kucoin"]
    exchanges = {
        xc: uxs.get_sn_exchange(
            {
                "exchange": xc,
                # "args": ({"verbose": True},),
                "kwargs": {"load_cached_markets": -1},
            }
        )
        for xc in EXCHANGES
    }
    print(
        "Cache dir: {}\nenable_caching: {}".format(
            uxs.get_cache_dir(), uxs.get_enable_caching()
        )
    )
    if any(not exchanges[xc].markets for xc in exchanges):
        print("Loading markets...")
        for xc in exchanges:
            if not exchanges[xc].markets:
                exchanges[xc].poll_load_markets(-1)
        print("Markets loaded\n")
    print("Loading tickers...")
    for xc in exchanges:
        exchanges[xc].tickers = uxs.poll.sn_get(exchanges[xc], "tickers", limit=-1)[
            0
        ].data
    print("Tickers loaded\n....................................................")

    # Pre-determine some base stats
    currencies = set()
    for xc in exchanges:
        for market in exchanges[xc].markets.values():
            currencies.add(market["base"])
            currencies.add(market["quote"])
    print(f"Number of currencies: {len(currencies)}")
    print(
        "Number of markets: {}".format(
            {xc: len(exchanges[xc].markets) for xc in exchanges}
        )
    )
    num_matching_markets = 0
    for xc_combination in itertools.combinations(exchanges, 2):
        num_matching_markets += sum(
            symbol in exchanges[xc_combination[1]].tickers
            or "/".join(symbol.split("/")[::-1]) in exchanges[xc_combination[1]].tickers
            for symbol in exchanges[xc_combination[0]].tickers
        )
    print(f"Number of matching markets: {num_matching_markets}")
    print("....................................................")

    # Find the shapes
    kw = {shapes_old: {}, shapes: {}}
    for module in [shapes]:  # , shapes_old]:
        started = time.time()
        # n_currencies = len(set(x[]
        retrieved_shapes = getattr(module, "get_shapes")(
            [2, 3],
            markets_coll={},  # {xc: exchanges[xc].markets for xc in exchanges},
            tickers_coll={xc: exchanges[xc].tickers for xc in exchanges},
            max_unique_exchanges=2,
            **kw[module],
        )
        """print(
            "Fetching {} ({}, {}) shapes for module {} took {:.2f} seconds".format(
                len(retrieved_shapes),
                len([_ for _ in retrieved_shapes if _.n == 2]),
                len([_ for _ in retrieved_shapes if _.n == 3]),
                getattr(module, "__name__"),
                time.time() - started,
            )
        )"""
        # print(retrieved_shapes)
