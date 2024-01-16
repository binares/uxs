import uxs
from uxs.fintls import shapes_old, shapes
from uxs.fintls.shapes import Shape
from fons.log import quick_logging
import itertools
import sys
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
Finding 20999 shapes took 0.06 seconds  [1,324,715: 13.41 seconds]
Initiating 20999 shapes took 3.21 seconds

shapes (w/ cython):
Finding 9063 currency shapes took 1.91 seconds # Cython  [31,351: 32.90 seconds]
Initiating 20999 shapes took 2.64 seconds  # shapes_cython._init_cys()

shapes (new):
Fetching 20999 (1059, 19940) shapes for module uxs.fintls.shapes took 9.75 seconds
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


def test_memalloc():
    shape = Shape(
        (("binance", "BTC/USDT", "BTC", "USDT"), ("kucoin", "BTC/USDT", "BTC", "USDT"))
    )
    print(sys.getsizeof(shape))
    print(dir(shape))
    for obj in (shape, shape.paths[0]):
        print("--------\n{}\n--------".format(obj.__class__.__name__))
        attrs = obj.__class__.__slots__
        totsize = 0
        for attr in attrs:
            size = get_total_size(getattr(obj, attr))
            print("{}: {}".format(attr, size))
            totsize += size
        print("Total size: {}".format(totsize))

    print("------------------")
    print(sys.getsizeof(Shape.get_path), sys.getsizeof(shape.get_path))
    totsize = sum(
        get_total_size(getattr(shape, attr))
        for attr in dir(shape)
        if hasattr(shape, attr)
    )
    print("Total size: {}".format(totsize))


def get_total_size(o, handlers={}, verbose=False):
    """Returns the approximate memory footprint an object and all of its contents.
    Automatically finds the contents of the following builtin containers and their subclasses: tuple, list, deque, dict, set and frozenset.
    To find other objects, add handlers to iterate over their contents:
        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}
    """
    import collections

    dict_handler = lambda d: itertools.chain.from_iterable(d.items())
    all_handlers = {
        tuple: iter,
        list: iter,
        collections.deque: iter,
        dict: dict_handler,
        set: iter,
        frozenset: iter,
    }
    all_handlers.update(handlers)  # user-defined handlers take precedence
    seen = set()  # track which object id's have already been seen
    default_size = sys.getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:  # do not double count the same object
            return 0
        seen.add(id(o))
        s = sys.getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)
