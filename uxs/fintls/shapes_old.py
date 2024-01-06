from __future__ import annotations  # `tuple` etc for py 3.7 and 3.8
from typing import Union, Tuple, List, Dict, Any, Optional, Iterable, Callable

from ccxt.base.types import Market, Ticker  # , Currency
from collections import defaultdict
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

from fons.iter import unique, flatten

from .shapes import Line, Shape, Path, CPath


def _markets_from_tickers(tickers) -> dict[str, dict[str, str]]:
    return {
        s: {"base": s.split("/")[0], "quote": s.split("/")[1]}
        for s in tickers
        if len(s.split("/")) == 2
    }


def _filter_markets(markets_coll, tickers_coll={}):
    xc_list = unique(list(markets_coll.keys()) + list(tickers_coll.keys()), astype=list)
    markets_coll2 = {}

    for xc in xc_list:
        tmarkets = (
            _markets_from_tickers(tickers_coll[xc]) if xc in tickers_coll else None
        )

        if xc in markets_coll:
            markets = markets_coll[xc]
            if tmarkets is not None:
                markets = {s: y for s, y in markets.items() if s in tmarkets}
            markets_coll2[xc] = markets

        elif tmarkets is not None:
            markets_coll2[xc] = tmarkets

    return markets_coll2


def _flatten_symbols(markets_coll: MarketsCollection):
    xsymbols = []
    for xc, markets in markets_coll.items():
        for symbol in markets:
            base, quote = _extract_currencies(symbol, xc, markets_coll)
            if base is not None and quote is not None:
                t = (xc, symbol, base, quote)
            else:
                t = (xc, symbol, None, None)
            xsymbols.append(t)

    return xsymbols


class _symbol_rank:
    __slots__ = ("symbol", "base", "quote", "bases_by_quote")

    def __init__(self, symbol, base, quote, bases_by_quote):
        self.symbol = symbol
        self.base = base
        self.quote = quote

        self.bases_by_quote = bases_by_quote

    def is_quote(self, cy):
        return bool(self.bases_by_quote[cy])

    def __lt__(self, other):
        q, b, oq, ob, bases_by_quote = (
            self.quote,
            self.base,
            other.quote,
            other.base,
            self.bases_by_quote,
        )

        if None in (q, b):
            return True
        elif None in (oq, ob):
            return False

        isq, oisq = self.is_quote(b), self.is_quote(ob)
        if isq != oisq:
            return oisq

        A0 = b in bases_by_quote[ob]
        A1 = ob in bases_by_quote[b]
        if A0 != A1:
            return A0

        B0 = q in bases_by_quote[ob]
        B1 = oq in bases_by_quote[b]
        if B0 != B1:
            return B0

        C0 = q in bases_by_quote[oq]
        C1 = oq in bases_by_quote[q]
        if C0 != C1:
            return C0

        D0 = b in bases_by_quote[oq]
        D1 = ob in bases_by_quote[q]
        if D0 != D1:
            return D0

        """s0 = A0 + B0 + C0 + D0
        s1 = A1 + B1 + C1 + D1
        
        return s0 >= s1"""

        return True


def _create_bases_by_quote_map(xsymbols: list[XCSymbolBaseQuote]):
    bases = defaultdict(set)

    for xc, symbol, base, quote in xsymbols:
        bases[quote].add(base)

    return bases


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


def _sort_xsymbols(
    xsymbols: list[XCSymbolBaseQuote], bases_by_quote_map=None, reverse=False
):
    if bases_by_quote_map is None:
        bases_by_quote_map = _create_bases_by_quote_map(xsymbols)

    srs = [
        _symbol_rank(symbol, base, quote, bases_by_quote_map)
        for xc, symbol, base, quote in xsymbols
    ]
    srs.sort(reverse=reverse)
    # [base-only-cys -> hybrids -> quote-only-cys]

    return list(unique(x.symbol for x in srs))


MarketsCollection = Dict[str, Dict[str, Market]]
TickersCollection = Dict[str, Dict[str, Ticker]]
XCSymbol = Tuple[str, str]
XCSymbolDirection = Tuple[str, str, int]
XCSymbolBaseQuote = Tuple[str, str, str, str]
SymbolGraph = Dict[XCSymbol, List[XCSymbolDirection]]


def create_symbol_graph(
    markets_coll: MarketsCollection,
) -> SymbolGraph:
    """returns:
    {
        (exchange, symbol): [(exchange_2, symbol_2, direction), ...],
    }
    ie symbol and symbol_2 must have at least one overlapping currency
    where ?? we always move quote -> base for the keys ??
      and the direction results from BASE_OF_KEY -> THE_OTHER_CURRENCY_of_the_XCSymbolDirection_tuple
    """
    xsymbols = _flatten_symbols(markets_coll)
    symbol_list = _sort_xsymbols(xsymbols, reverse=True)
    # print(symbol_list)
    xsymbols.sort(key=lambda x: symbol_list.index(x[1]))
    graph = {}

    for i, xcsym in enumerate(xsymbols):
        xc, sym, base, quote = xcsym
        cys = (base, quote)
        graph[(xc, sym)] = add_to = []

        if base is None:
            continue

        for xcsym2 in xsymbols[i + 1 :]:
            xc2, sym2, base2, quote2 = xcsym2
            cys2 = (base2, quote2)

            if base2 is None:
                continue

            for j, _cy in enumerate(cys):
                # _cy in source_cy in sym2, and target_cy in sym1
                try:
                    pos2 = cys2.index(_cy)
                except ValueError:
                    continue
                d = int(not j)
                # (exchange, symbol, direction)
                add_to.append((xc2, sym2, d))

    return graph


def _filter_shapes(shapes):
    return unique(
        shapes, key=lambda shape: tuple(sorted(x[:2] for x in shape)), astype=list
    )


def get_shapes(
    n: Union[int, List[int]],
    markets_coll: MarketsCollection,
    tickers_coll: TickersCollection = {},
    max_unique_exchanges=None,
):
    """Returns all n-combinations of (xc,cy) pairs
    :param n: int or range
    markets_coll: {xc: {symbol: ccxt_market / None}, ...}
    tickers_coll: {xc: {symbol: ccxt_ticker}, ...}
    max_unique_exchanges: <int> - maximum number of different exchanges in a shape"""

    is_int = isinstance(n, int)
    n_values = (n,) if isinstance(n, int) else tuple(n)
    max_n = max(n_values)

    if min(n_values) < 2:
        raise ValueError("`n` must be >= 2; got: {}".format(n))

    markets_coll = _filter_markets(markets_coll, tickers_coll)
    symbol_graph = create_symbol_graph(markets_coll)

    n_shapes = {i: [] for i in n_values}
    _seen_paths = set()

    def _reverse_trail(t):
        return (t[0],) + (t[-1:0:-1])

    def _is_circular(trail):
        first_xc, first_symbol, first_d = trail[0]
        last_xc, last_symbol, last_d = trail[-1]
        f_cys = _extract_currencies(first_symbol, first_xc, markets_coll)
        l_cys = _extract_currencies(last_symbol, last_xc, markets_coll)
        return get_target_cy(l_cys, last_d) == get_source_cy(f_cys, first_d)

    _exceeds_exchanges_limit = (
        (lambda *a: False)
        if max_unique_exchanges is None
        else (
            lambda xcs, xc_count, cy_xc: xc_count >= max_unique_exchanges
            and cy_xc not in xcs
        )
    )

    def rec(trail: tuple[XCSymbolDirection, ...]):
        trail_0 = trail
        xc, symbol, direction = trail[-1]
        currencies = _extract_currencies(symbol, xc, markets_coll)
        target_cy = get_target_cy(currencies, direction)

        xcs = set(x[0] for x in trail_0)
        xc_count = len(xcs)

        n = len(trail) + 1
        is_length_included = n in n_values
        is_length_unsaturated = n < max_n

        # ~~Check if market is already in the trail?~~ Nah, in both n=2 and n=3 shouldn't be

        for xc2, symbol2, _direction in symbol_graph[(xc, symbol)]:
            if _direction != direction or _exceeds_exchanges_limit(xcs, xc_count, xc2):
                continue

            currencies2 = _extract_currencies(symbol2, xc2, markets_coll)
            direction2 = int(currencies2.index(target_cy))
            trail = trail_0 + ((xc2, symbol2, direction2),)
            is_circular = _is_circular(trail)

            if is_length_included and is_circular:
                # currently the trail is middle/(highest_)quote -> base/quote -> base/middle
                # reverse the trail for it to be middle/quote -> base/middle -> base/quote
                trail_r = _reverse_trail(trail)
                n_shapes[n].append(trail_r)

            if is_length_unsaturated and not is_circular:
                rec(trail)

    for xc, symbol in symbol_graph:
        rec(((xc, symbol, 0),))

    def _create_currencies_map(trail: tuple[XCSymbolDirection, ...]):
        cys_map = {}
        for xc, symbol, direction in trail:
            base, quote = _extract_currencies(symbol, xc, markets_coll)
            cys_map[(xc, symbol)] = (base, quote)
        return cys_map

    # n_shapes = _filter_shapes(n_shapes)
    n_shapes = {
        _n: [Shape(t, cys_map=_create_currencies_map(t)) for t in items]
        for _n, items in n_shapes.items()
    }

    return list(flatten(n_shapes.values(), exclude_types=(tuple,)))


if __name__ == "__main__":
    shapes = get_shapes(2, {"a": {"A/B": {}}, "b": {"A/B": {}}}, as_tuples=False)
    for s in shapes:
        print("{}".format("-".join(s.cys)), s.symbols, s.directions, s.exchanges)
        for c in s.paths:
            print("::{}".format("-".join(c.cys)), c.symbols, c.directions, c.exchanges)
