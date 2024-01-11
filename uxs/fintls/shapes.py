from __future__ import annotations  # `tuple` etc for py 3.7 and 3.8
from typing import Union, Tuple, List, Set, Dict, Any, Optional, Iterable, Callable

import itertools
from collections import defaultdict
import time
import logging
from ccxt.base.types import Market, Ticker  # , Currency

from .basics import (
    get_conversion_op,
    as_source,
    as_target,
    as_direction,
    as_ob_side,
    as_ob_fill_side,
    convert_quotation,
    get_source_cy,
    get_target_cy,
    create_cy_graph,
)
from . import shapes_cython

from fons.iter import unique, flatten

sh_logger = logging.getLogger("uxs.shapes")


class Line:
    # Linear path of symbols
    __slots__ = (
        "n",
        "exchanges",
        "symbols",
        "cys",
        "cy_sides",
        "directions",
        "xccys",
        "xcsyms",
        "cpaths",
        "cys_map",
        "paths",
    )

    def __init__(self, xc_symbol_pairs, start_direction=None, cys_map={}):
        """Symbol path must be linear
        If `start_direction` is not given, it is determined automatically,
         and may be unexpected if cys_in_symbol_0==cys_in_symbol_1 (or n==2 for circular)
        """

        xc_symbol_pairs = tuple(tuple(x[:2]) for x in xc_symbol_pairs)
        n = len(xc_symbol_pairs)
        exchanges = tuple(x[0] for x in xc_symbol_pairs)
        symbols = tuple(x[1] for x in xc_symbol_pairs)

        cys, cy_sides = Line._init_cys(n, xc_symbol_pairs, cys_map, start_direction)
        directions = tuple(int(not cy_sides[i]) for i in range(n))

        self.n = n
        self.exchanges = exchanges
        self.symbols = symbols
        self.directions = directions
        self.cys = tuple(cys)
        self.cy_sides = tuple(cy_sides)
        self.xccys = tuple(zip(exchanges + (None,), cys))
        self.xcsyms = xc_symbol_pairs
        self.cys_map = tuple(cys_map.items())

        self.create_paths()

    @staticmethod
    def _init_cys(n, xc_symbol_pairs, cys_map={}, start_direction=None):
        """
        # Speedup was like 20%
        if start_direction is None:
            start_direction = -1
        if not isinstance(cys_map, dict):
            cys_map = dict(cys_map)
        return shapes_cython._init_cys(n, xc_symbol_pairs, cys_map, start_direction)
        """
        if not isinstance(cys_map, dict):
            cys_map = dict(cys_map)
        cys = []
        cy_sides = []
        split = [
            s.split("/") if (xc, s) not in cys_map else cys_map[(xc, s)]
            for xc, s in xc_symbol_pairs
        ]
        if start_direction is not None:
            start_direction = as_direction(start_direction)

        try:
            for i in range(0, n):
                prev, cur, nxt = split[(i - 1) % n], split[i], split[(i + 1) % n]
                if not i and start_direction is not None:
                    cy, cy_side = split[i][start_direction], int(not start_direction)
                elif not i:
                    cy, cy_side = next(
                        ((cy, j) for j, cy in enumerate(cur[::-1]) if cy not in nxt),
                        (split[0][1], 0),
                    )
                else:
                    cy, cy_side = next(
                        (cy, j)
                        for j, cy in enumerate(cur[::-1])
                        if cy in prev and cy != cys[i - 1]
                    )
                cys.append(cy)
                cy_sides.append(cy_side)
        except StopIteration:
            raise ValueError(
                "Symbol path isn't linear at [{}:{}]: {}".format(
                    i - 1, i, xc_symbol_pairs
                )
            )

        final_cy_side = int(not (cy_sides[-1]))
        final_cy = split[-1][not final_cy_side]

        cys += [final_cy]
        cy_sides += [final_cy_side]

        return cys, cy_sides

    def create_paths(self):
        self.paths = []

        start_from = (0, self.n - 1)
        polarities = (1, -1)

        for i, polarity in zip(start_from, polarities):
            path = Path(self, i, polarity)
            self.paths.append(path)

    def get_path(self, start_or_indexes, polarity=None):
        """:rtype: Path"""
        if hasattr(start_or_indexes, "__iter__"):
            indexes = self.index(start_or_indexes)
            return next(x for x in self.paths if x.index_order == indexes)
        else:
            start = start_or_indexes
            return next(
                x for x in self.paths if x.start == start and x.polarity == polarity
            )

    def get_unique_paths(self):
        return self.paths[:]

    # May have non-unique (xc,cy) pairs
    # in that case raise ValueError
    def index(self, x):
        """(xc, cy), (xc, symbol) or index"""
        return Line._index(self.n, self.xccys, self.xcsyms, x)

    @staticmethod
    def _index(n, xccys, xcsyms, xc_cy_pair):  #
        if isinstance(xc_cy_pair, int):
            single = True
            pairs = [xc_cy_pair]
        else:
            x0 = xc_cy_pair[0]
            single = isinstance(x0, str)
            pairs = [xc_cy_pair] if single else xc_cy_pair
        indexes = []

        for x in pairs:
            if isinstance(x, int):
                if x < 0 or x >= n + 1:
                    raise ValueError("Out of range index: {}".format(x))
                i = x
            else:
                xccy = tuple(x)
                search = [xcsyms] if "/" in xccy[1] else [xcsyms, xccys]
                all_occurrences = [
                    [i for i, x in enumerate(_s) if x == xccy] for _s in search
                ]
                occurrences = max(all_occurrences, key=lambda l: len(l))
                if len(occurrences) > 1:
                    raise ValueError("Line contains more than one {} pair".format(xccy))
                elif len(occurrences) == 0:
                    raise ValueError("Does not contain {}".format(xccy))
                i = occurrences[0]
            indexes.append(i)

        return indexes[0] if single else tuple(indexes)

    def xccy(self, index):
        """index or (xc, cy)"""
        return Line._xccy(self.n, self.xccys, index)

    @staticmethod
    def _xccy(n, xccys, index):  # cy()
        if isinstance(index, int):
            single = True
            pairs = [index]
        else:
            x0 = index[0]
            single = isinstance(x0, str)
            pairs = [index] if single else index
        _xccys = []

        for x in pairs:
            if isinstance(x, int):
                if x < 0 or x >= n + 1:
                    raise ValueError("Out of range index: {}".format(x))
                xccy = xccys[x]
            else:
                xccy = tuple(x)
                if xccy not in xccys:
                    raise ValueError("Unknown xccy pair: {}".format(xccy))
            _xccys.append(xccy)

        return _xccys[0] if single else tuple(_xccys)

    def symbol(self, index):
        """index or symbol"""
        return Line._symbol(self.n, self.symbols, index)

    @staticmethod
    def _symbol(n, symbols, index):  # get_market() #cy1, cy2
        single = isinstance(index, str) or not hasattr(index, "__iter__")
        indexes = [index] if single else index
        _symbols = []

        for x in indexes:
            if isinstance(x, int):
                if x < 0 or x >= n:
                    raise ValueError("Out of range index: {}".format(x))
                symbol = symbols[x]
            else:
                symbol = x
                if symbol not in symbols:
                    raise ValueError("Unknown symbol: {}".format(symbol))
            _symbols.append(symbol)

        return _symbols[0] if single else tuple(_symbols)

    def has_symbol(self, symbol):  # has_market() #str or ('cy1','cy2')
        """symbol or (xc, symbol)"""
        single = isinstance(symbol, str) or isinstance(symbol[0], str)
        symbols = [symbol] if single else symbol

        def has(x):
            if isinstance(x, str):
                return x in self.symbols
            else:
                return x in self.xcsyms

        return all(has(x) for x in symbols)

    def calc_polarity(self, start, next):
        return 1 if (start + 1) % self.n == next else -1


class Shape(Line):
    # Circular path of symbols without starting point

    def __init__(self, xc_symbol_pairs, start_direction=None, cys_map={}):
        super().__init__(xc_symbol_pairs, start_direction, cys_map)

        if self.cys[0] != self.cys[-1]:
            raise ValueError("Line isn't circular: {}".format(self.symbols))

    def create_paths(self):
        self.paths = []
        _range = range(self.n)
        polarities = (1, -1)  # if self.n > 2 else (1,)

        for i in _range:
            for polarity in polarities:
                path = Path(self, i, polarity)
                self.paths.append(path)

    def get_unique_paths(self):
        if self.n > 2:
            return self.paths[:]
        elif self.n == 2:
            return [self.get_path(0, 1), self.get_path(1, 1)]
        elif self.n == 1:
            return self.get_path(0, 1)
        else:
            return []

    """def get_cpath_by_order(self, order): #
        :rtype: CPath"
        indexes = 
        return next(x for x in self.cpaths if x.index_order==indexes)"""

    """def get_order_direction(self,cy1,cy2):
        cy1,cy2 = self.index([cy1,cy2])
        #1 for buy order, 0 for sell
        return 1 if cy1 < cy2 else 0"""

    """polarities = {(0,1): 1, (1,2): 1, (2,0): 1,
                  (0,2): -1, (2,1): -1, (1,0): -1}"""  # polarities


class Path:
    # Line with starting point (xc,symbol and polarity
    __slots__ = (
        "line",
        "start",
        "polarity",
        "n",
        "indexes",
        "exchanges",
        "symbols",
        "directions",
        "xccys",
        "xcsyms",
        "entities",
        "cys",
        "cy_sides",
        "conv_pairs",
        "id",
        "id2",
    )

    index = Line.index
    xccy = Line.xccy
    symbol = Line.symbol
    has_symbol = Line.has_symbol

    def __init__(self, line, start=0, polarity=1):
        """:type line: Line
        :param start: the first (xc,symbol) index in the `line`;
                      for non circular line `0` or `n-1` must be used
                      (and polarities `1` or `-1` respectively)
        :param polarity: 1,-1 (1 moves right, -1 left)"""

        if polarity not in (1, -1):
            raise ValueError(polarity)

        self.line = line
        self.start = start
        self.polarity = polarity
        self.n = line.n
        self.indexes = tuple((start + polarity * j) % line.n for j in range(line.n))

        def _get_direction(i):
            d = line.directions[i]
            return d if polarity == 1 else int(not d)

        self.exchanges = tuple(line.exchanges[i] for i in self.indexes)
        self.symbols = tuple(line.symbol(i) for i in self.indexes)
        self.directions = tuple(_get_direction(i) for i in self.indexes)
        self.xcsyms = tuple(zip(self.exchanges, self.symbols))

        cys, cy_sides = Line._init_cys(
            line.n, self.xcsyms, line.cys_map, self.directions[0]
        )

        self.cys = tuple(cys)
        self.cy_sides = tuple(cy_sides)
        self.xccys = tuple(zip(self.exchanges + (None,), self.cys))
        self.entities = tuple(zip(self.exchanges, self.symbols, self.directions))

        self.conv_pairs = tuple((self.cys[j], self.cys[j + 1]) for j in range(self.n))

        _format_xc = lambda xc: xc[:3] if xc is not None else "*"
        self.id = "-".join(
            "[{xc}]{cy}".format(xc=_format_xc(xc), cy=cy) for xc, cy in self.xccys
        )
        self.id2 = "-".join(
            "[{xc}]{s}:{d}".format(xc=xc, s=s, d=d) for xc, s, d in self.entities
        )

    def get(self, i):
        return self.entities[i]

    def __getitem__(self, i):
        return self.entities[i]

    def __iter__(self):
        return iter(self.entities)

    def __str__(self):
        return "{}".format(self.id2)

    @property
    def l(self):
        return self.line

    @property
    def e(self):
        return self.entities


class CPath(Path):
    """def is_conflicting(self, entity): #
    Returns True if entity shares an oppositely flowing market with self"
    if isinstance(entity, CPath):
        return any(self.is_conflicting(x) for x in entity.conv_pairs)
    #(xc,symbol,direction)
    if not isinstance(entity[1],str):
        cys = entity[0].split('/')
        entity = cys if not entity[1] else cys[::-1]
    #('cy1','cy2')
    return tuple(reversed(entity)) in self.conv_pairs"""

    """@property
    def index_order(self): #
        return self.indexes"""

    @property
    def s(self):
        return self.line

    @property
    def shape(self):
        return self.line


MarketsCollection = Dict[str, Dict[str, Market]]
TickersCollection = Dict[str, Dict[str, Ticker]]
# XCSymbol = Tuple[str, str]
# XCSymbolDirection = Tuple[str, str, int]
XCSymbolBaseQuote = Tuple[str, str, str, str]
CurrencyGraph = Dict[
    str, Set[str]
]  # currenty -> all of its paired currencies (in any market of any exchange)

XCSymbols_By_SymbolID = Dict[
    Tuple[str, str], Set[XCSymbolBaseQuote]
]  # Each symbol by its sorted((base, quote)) -> [(xc, symbol, base, quote), ...] of all exchanges
Shapes_By_N = Dict[int, List[Shape]]


class Helpers:
    @staticmethod
    def _markets_from_tickers(tickers) -> dict[str, dict[str, str]]:
        return {
            s: {"base": s.split("/")[0], "quote": s.split("/")[1]}
            for s in tickers
            if len(s.split("/")) == 2
        }

    @staticmethod
    def add_markets_from_tickers(markets_coll, tickers_coll={}):
        xc_list = unique(
            list(markets_coll.keys()) + list(tickers_coll.keys()), astype=list
        )
        markets_coll_new = {}

        for xc in xc_list:
            markets_of_an_xc_from_its_tickers = (
                Helpers._markets_from_tickers(tickers_coll[xc])
                if xc in tickers_coll
                else None
            )

            if xc in markets_coll:
                markets = markets_coll[xc]
                if markets_of_an_xc_from_its_tickers is not None:
                    markets = {
                        s: y
                        for s, y in markets.items()
                        if s in markets_of_an_xc_from_its_tickers
                    }
                markets_coll_new[xc] = markets

            elif markets_of_an_xc_from_its_tickers is not None:
                markets_coll_new[xc] = markets_of_an_xc_from_its_tickers

        return markets_coll_new

    @staticmethod
    def _extract_currencies(
        symbol: str, xc: str, markets_coll: MarketsCollection
    ) -> tuple[Union[str, None], Union[str, None]]:
        base = quote = None
        both_defined = False

        if xc in markets_coll:
            if symbol in markets_coll[xc]:
                x = markets_coll[xc][symbol]
                if isinstance(x, dict):
                    both_defined = "base" in x and "quote" in x
                    base = x.get("base")
                    quote = x.get("quote")

        if not both_defined:
            try:
                base, quote = symbol.split("/")
            except ValueError:
                pass

        if None not in (base, quote):
            return base, quote
        else:
            return None, None

    @staticmethod
    def flatten_symbols(
        markets_coll: MarketsCollection,
    ) -> List[Tuple[string, string, string, string]]:
        xsymbols = []
        for xc, markets in markets_coll.items():
            for symbol in markets:
                base, quote = Helpers._extract_currencies(symbol, xc, markets_coll)
                if base is not None and quote is not None:
                    t = (xc, symbol, base, quote)
                else:
                    t = (xc, symbol, None, None)
                xsymbols.append(t)

        return xsymbols


def _create_currency_graph(
    markets_coll: MarketsCollection,
) -> Tuple[CurrencyGraph, XCSymbols_By_SymbolID]:
    """returns:
    {
        currency: [paired_currency_1 (any market of any exchange), paired_currency_2, ...],
    }
    """
    XCSymbolBaseQuote_list = Helpers.flatten_symbols(markets_coll)
    currency_graph = defaultdict(set)
    xcsymbols_by_symbol_id = defaultdict(set)
    for xc, symbol, base, quote in XCSymbolBaseQuote_list:
        if base is not None or quote is not None:
            currency_graph[base].add(quote)
            currency_graph[quote].add(base)
            xcsymbols_by_symbol_id[tuple(sorted((base, quote)))].add(
                (xc, symbol, base, quote)
            )

    return dict(currency_graph), xcsymbols_by_symbol_id


def get_shapes(
    n: Union[int, List[int]],
    markets_coll: MarketsCollection,
    tickers_coll: TickersCollection = {},
    max_unique_exchanges: int = None,
    use_cython: bool = True,
) -> Shapes_By_N:
    n_values = (n,) if isinstance(n, int) else tuple(n)
    max_n = max(n_values)

    if min(n_values) < 2:
        raise ValueError("`n` must be >= 2; got: {}".format(n))

    markets_coll = Helpers.add_markets_from_tickers(markets_coll, tickers_coll)
    currency_graph, xcsymbols_by_symbol_id = _create_currency_graph(markets_coll)

    # 1. Find the circular currency trails
    #  - starting point == any other starting point
    #  - forward == backward
    # (i.e. there'll be no duplicates in starting point/polarity sense)
    _seen_currency_shapes = set()
    n_shapes_of_currencies: Dict[Set[Tuple[str, ...]]] = {i: set() for i in n_values}

    def rec_cy_trail(trail: Tuple[str]):
        last_cy = trail[-1]

        n = len(trail)
        is_length_included = n in n_values
        is_length_unsaturated = n < max_n

        for next_cy in currency_graph[last_cy]:
            if next_cy in trail[1:]:
                continue

            is_circular = next_cy == trail[0]

            if is_length_included and is_circular:
                # We don't append the new_cy (as it's equal to the first one)
                alphabetical_loc = trail.index(sorted(trail)[0])
                trail_forwards = trail[alphabetical_loc:] + trail[:alphabetical_loc]
                trail_backwards = (trail_forwards[0],) + trail_forwards[-1:0:-1]
                if (
                    trail_forwards not in _seen_currency_shapes
                    and trail_backwards not in _seen_currency_shapes
                ):
                    _seen_currency_shapes.add(trail_forwards)
                    _seen_currency_shapes.add(trail_backwards)
                    n_shapes_of_currencies[n].add(trail_forwards)

            if is_length_unsaturated and not is_circular:
                rec_cy_trail(trail + (next_cy,))

    _started = time.time()
    if not use_cython:
        for cy in currency_graph:
            rec_cy_trail((cy,))
    else:
        n_shapes_of_currencies = shapes_cython.create_shapes_of_currencies(
            n_values, currency_graph
        )

    sh_logger.debug(
        f"Finding {sum(len(x) for x in n_shapes_of_currencies.values())} currency shapes took {time.time()-_started:.2f} seconds"
    )
    # print(n_shapes_of_currencies[2])
    """are_unique = all(
        t[::-1] not in n_shapes_of_currencies[2] for t in n_shapes_of_currencies[2]
    )
    print(f"Are unique: {are_unique}")
    print(len(n_shapes_of_currencies[2]))"""

    # 2. Match the currency paths to (exchange, symbol) paths

    def find_symbol_paths_for_cy_trail(cy_trail: Tuple[str]):
        xcsymbols_lists: List[List[XCSymbolBaseQuote]] = []
        for i in range(len(cy_trail)):
            cy = cy_trail[i]
            next_cy = cy_trail[(i + 1) % len(cy_trail)]
            symbol_id = tuple(sorted((cy, next_cy)))
            corresponding_xcsymbols = xcsymbols_by_symbol_id[symbol_id]
            xcsymbols_lists.append(corresponding_xcsymbols)

        shape_tuples: List[Tuple[Tuple[XCSymbol], Dict[XCSymbol, Tuple[str, str]]]] = []
        if len(cy_trail) == 2:
            # Both xcsymbols lists have the same (exchanges, symbol) pairs; use only one list
            xcsymbols_combinations = itertools.combinations(xcsymbols_lists[0], 2)
        else:
            # No repeating (exchange, symbol) between any of the lists
            xcsymbols_combinations = itertools.product(*xcsymbols_lists)

        # Make all combinations of xcsymbols
        for xcsymbols_combination in xcsymbols_combinations:
            num_exchanges = len(set(_[0] for _ in xcsymbols_combination))
            if max_unique_exchanges and num_exchanges > max_unique_exchanges:
                continue
            # Create the symbol path
            xc_symbol_pairs = ()
            cys_map = {}
            for i in range(len(cy_trail)):
                xc, symbol, base, quote = xcsymbols_combination[i]
                xc_symbol_pairs += ((xc, symbol),)
                cys_map[(xc, symbol)] = (base, quote)
            # Create the shape
            shape_tuples.append((xc_symbol_pairs, cys_map))

        return shape_tuples

    shape_tuples = []

    _started = time.time()
    for n in n_shapes_of_currencies:
        for cy_trail in n_shapes_of_currencies[n]:
            shape_tuples += find_symbol_paths_for_cy_trail(cy_trail)
    sh_logger.debug(
        f"Finding {len(shape_tuples)} shapes took {time.time()-_started:.2f} seconds"
    )

    # return shape_tuples

    _started = time.time()
    shapes = [Shape(xcsyms, cys_map=cys_map) for xcsyms, cys_map in shape_tuples]
    sh_logger.debug(
        f"Initiating {len(shapes)} shapes took {time.time()-_started:.2f} seconds"
    )

    return shapes
