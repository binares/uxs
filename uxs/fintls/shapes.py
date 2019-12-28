from collections import defaultdict
from .basics import (get_conversion_op, as_source,
                     as_target, as_direction, as_ob_side,
                     as_ob_fill_side, convert_quotation, 
                     get_source_cy, get_target_cy, create_cy_graph)
from .ob import (get_to_matching_volume, get_to_matching_price,
                 get_stop_condition, exec_step, select_next_step,
                 select_prev_step, calc_vwap)

from fons.iter import (unique, flatten)


class Line:
    #Linear path of symbols
    __slots__ = ('n', 'exchanges', 'symbols', 'cys', 'cy_sides',
                 'directions', 'xccys', 'xcsyms', 'cpaths', 'cys_map')


    def __init__(self, xc_symbol_pairs, start_direction=None, cys_map={}):
        """Symbol path must be linear
           If `start_direction` is not given, it is determined automatically,
            and may be unexpected if cys_in_symbol_0==cys_in_symbol_1 (or n==2 for circular)"""
        
        xc_symbol_pairs = tuple(tuple(x[:2]) for x in xc_symbol_pairs)
        n = len(xc_symbol_pairs)
        exchanges = tuple(x[0] for x in xc_symbol_pairs)
        symbols = tuple(x[1] for x in xc_symbol_pairs)
        
        cys, cy_sides = self._init_cys(n, xc_symbol_pairs, cys_map, start_direction)
        directions = tuple(int(not cy_sides[i]) for i in range(n))
        
        self.n = n
        self.exchanges = exchanges
        self.symbols = symbols
        self.directions = directions
        self.cys = tuple(cys)
        self.cy_sides = tuple(cy_sides)
        self.xccys = tuple(zip(exchanges+(None,),cys))
        self.xcsyms = xc_symbol_pairs
        self.cys_map = tuple(cys_map.items())
        
        self.create_paths()
        
        
    @staticmethod
    def _init_cys(n, xc_symbol_pairs, cys_map={}, start_direction=None):
        if not isinstance(cys_map, dict):
            cys_map = dict(cys_map)
        cys = []
        cy_sides = []
        split = [s.split('/') if (xc,s) not in cys_map else cys_map[(xc,s)] for xc,s in xc_symbol_pairs]
        if start_direction is not None:
            start_direction = as_direction(start_direction)
            
        try: 
            for i in range(0, n):
                prev, cur, nxt = split[(i-1)%n], split[i], split[(i+1)%n]
                if not i and start_direction is not None:
                    cy,cy_side = split[i][start_direction], int(not start_direction)
                elif not i:
                    cy,cy_side = next(((cy,j) for j,cy in enumerate(cur[::-1]) 
                                       if cy not in nxt), (split[0][1], 0))
                else:
                    cy,cy_side = next((cy,j) for j,cy in enumerate(cur[::-1]) 
                                      if cy in prev and cy!=cys[i-1])
                cys.append(cy)
                cy_sides.append(cy_side)
        except StopIteration:
            raise ValueError("Symbol path isn't linear at [{}:{}]: {}".format(i-1, i, xc_symbol_pairs))
        
        final_cy_side = int(not(cy_sides[-1]))
        final_cy = split[-1][not final_cy_side]
        
        cys += [final_cy]
        cy_sides += [final_cy_side]
        
        return cys, cy_sides
    
    
    def create_paths(self):
        self.paths = []
        
        start_from = (0, self.n-1)
        polarities = (1,-1)
        
        for i,polarity in zip(start_from, polarities):
            path = Path(self, i, polarity)
            self.paths.append(path)
            

    def get_path(self, start_or_indexes, polarity=None):
        """:rtype: Path"""
        if hasattr(start_or_indexes, '__iter__'):
            indexes = self.index(start_or_indexes)
            return next(x for x in self.paths if x.index_order==indexes)
        else:
            start = start_or_indexes
            return next(x for x in self.paths if x.start==start and x.polarity==polarity)
    
    
    def get_unique_paths(self):
        return self.paths[:]


    # May have non-unique (xc,cy) pairs
    # in that case raise ValueError
    def index(self, x):
        """(xc, cy), (xc, symbol) or index"""
        return Line._index(self.n, self.xccys, self.xcsyms, x)
    
    @staticmethod
    def _index(n, xccys, xcsyms, xc_cy_pair): #
        if isinstance(xc_cy_pair, int):
            single = True
            pairs = [xc_cy_pair]
        else:
            x0 = xc_cy_pair[0]
            single = isinstance(x0,str)
            pairs = [xc_cy_pair] if single else xc_cy_pair
        indexes = []
        
        for x in pairs:
            if isinstance(x, int):
                if x < 0 or x >= n+1:
                    raise ValueError('Out of range index: {}'.format(x))
                i = x
            else:
                xccy = tuple(x)
                search = [xcsyms] if '/' in xccy[1] else [xcsyms, xccys]
                all_occurrences = [[i for i,x in enumerate(_s) if x == xccy]
                                   for _s in search]
                occurrences = max(all_occurrences, key=lambda l: len(l))
                if len(occurrences) > 1:
                    raise ValueError('Line contains more than one {} pair'.format(xccy))
                elif len(occurrences) == 0:
                    raise ValueError('Does not contain {}'.format(xccy))
                i = occurrences[0]
            indexes.append(i)
            
        return indexes[0] if single else tuple(indexes)
    
    
    def xccy(self, index):
        """index or (xc, cy)"""
        return Line._xccy(self.n, self.xccys, index)
    
    @staticmethod
    def _xccy(n, xccys, index): #cy()
        if isinstance(index, int):
            single = True
            pairs = [index]
        else:
            x0 = index[0]
            single = isinstance(x0,str)
            pairs = [index] if single else index
        _xccys = []
        
        for x in pairs:
            if isinstance(x, int):
                if x < 0 or x >= n+1:
                    raise ValueError('Out of range index: {}'.format(x))
                xccy = xccys[x]
            else:
                xccy = tuple(x)
                if xccy not in xccys:
                    raise ValueError('Unknown xccy pair: {}'.format(xccy))
            _xccys.append(xccy)
            
        return _xccys[0] if single else tuple(_xccys)
    
    
    def symbol(self, index):
        """index or symbol"""
        return Line._symbol(self.n, self.symbols, index)
    
    @staticmethod
    def _symbol(n, symbols, index): #get_market() #cy1, cy2
        single = isinstance(index, str) or not hasattr(index, '__iter__')
        indexes = [index] if single else index
        _symbols = []
        
        for x in indexes:
            if isinstance(x, int):
                if x < 0 or x >= n:
                    raise ValueError('Out of range index: {}'.format(x))
                symbol = symbols[x]
            else:
                symbol = x
                if symbol not in symbols:
                    raise ValueError('Unknown symbol: {}'.format(symbol))
            _symbols.append(symbol)
            
        return _symbols[0] if single else tuple(_symbols)
    
    
    def has_symbol(self, symbol): #has_market() #str or ('cy1','cy2')
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
        return 1 if (start+1) % self.n == next else -1


class Shape(Line):
    #Circular path of symbols without starting point
    
    def __init__(self, xc_symbol_pairs, start_direction=None, cys_map={}):
        super().__init__(xc_symbol_pairs, start_direction, cys_map)
        
        if self.cys[0] != self.cys[-1]:
            raise ValueError("Line isn't circular: {}".format(self.symbols))
    
    
    def create_paths(self):
        self.paths = []
        _range = range(self.n)
        polarities = (1,-1) #if self.n > 2 else (1,)
        
        for i in _range:
            for polarity in polarities:
                path = Path(self, i, polarity)
                self.paths.append(path)
    
    
    def get_unique_paths(self):
        if self.n > 2:
            return self.paths[:]
        elif self.n == 2:
            return [self.get_path(0,1), self.get_path(1,1)]
        elif self.n == 1:
            return self.get_path(0,1)
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
                  (0,2): -1, (2,1): -1, (1,0): -1}""" #polarities
        
    
class Path:
    #Line with starting point (xc,symbol and polarity
    __slots__ = ('line', 'start', 'polarity', 'n', 'indexes',
                 'exchanges', 'symbols', 'directions', 'xccys', 'xcsyms',
                 'entities', 'cys', 'cy_sides', 'conv_pairs', 'id', 'id2')
    
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
           
        if polarity not in (1,-1):
            raise ValueError(polarity)
        
        self.line = line
        self.start = start
        self.polarity = polarity
        self.n = line.n
        self.indexes = tuple((start + polarity*j) % line.n for j in range(line.n))
        
        def _get_direction(i):
            d = line.directions[i]
            return d if polarity == 1 else int(not d)
        
        self.exchanges = tuple(line.exchanges[i] for i in self.indexes)
        self.symbols = tuple(line.symbol(i) for i in self.indexes)
        self.directions = tuple(_get_direction(i) for i in self.indexes)
        self.xcsyms = tuple(zip(self.exchanges, self.symbols))
        
        cys, cy_sides = Line._init_cys(line.n, self.xcsyms, line.cys_map, self.directions[0])
        
        self.cys = tuple(cys)
        self.cy_sides = tuple(cy_sides)
        self.xccys = tuple(zip(self.exchanges+(None,), self.cys))
        self.entities = tuple(zip(self.exchanges, self.symbols, self.directions))
        
        self.conv_pairs = tuple((self.cys[j], self.cys[j+1]) for j in range(self.n))
        
        _format_xc = lambda xc: xc[:3] if xc is not None else '*'
        self.id = '-'.join('[{xc}]{cy}'.format(xc=_format_xc(xc), cy=cy) for xc,cy in self.xccys)
        self.id2 = '-'.join('[{xc}]{s}:{d}'.format(xc=xc, s=s, d=d) for xc,s,d in self.entities)
        
    
    def get(self, i):
        return self.entities[i]
    
    def __getitem__(self, i):
        return self.entities[i]
        
    def __iter__(self):
        return iter(self.entities)
    
    def __str__(self):
        return '{}'.format(self.id2)
    
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
    
    
    
def _markets_from_tickers(tickers):
        return {s: {'base': s.split('/')[0], 'quote': s.split('/')[1]} 
                 for s in tickers if len(s.split('/'))==2}


def _filter_markets(markets_coll, tickers_coll={}):
    xc_list = unique(list(markets_coll.keys()) + list(tickers_coll.keys()), astype=list)
    markets_coll2 = {}
    
    for xc in xc_list:
        tmarkets = _markets_from_tickers(tickers_coll[xc]) if xc in tickers_coll else None
        
        if xc in markets_coll:
            markets = markets_coll[xc]
            if tmarkets is not None:
                markets = {s:y for s,y in markets.items() if s in tmarkets}
            markets_coll2[xc] = markets  
            
        elif tmarkets is not None:
            markets_coll2[xc] = tmarkets
    
    return markets_coll2


def _flatten_symbols(markets_coll):
    xsymbols = []
    for xc,markets in markets_coll.items():
        for symbol in markets:
            base, quote = _extract_cys(symbol, xc, markets_coll)
            if base is not None and quote is not None:
                t = (xc, symbol, base, quote)
            else:
                t = (xc, symbol, None, None)
            xsymbols.append(t)
            
    return xsymbols


class _symbol_rank:
    __slots__ = ('symbol','base','quote','bases')
    
    def __init__(self, symbol, base, quote, quote_base_map):
        self.symbol = symbol
        self.base = base
        self.quote = quote

        self.bases = quote_base_map
        
    def is_quote(self, cy):
        return bool(self.bases[cy])
        
    def __lt__(self, other):
        q, b, oq, ob, bases = self.quote, self.base, other.quote, other.base, self.bases
        
        if None in (q, b):
            return True
        elif None in (oq, ob):
            return False
        
        isq, oisq = self.is_quote(b), self.is_quote(ob)
        if isq != oisq:
            return oisq
        
        A0 = b in bases[ob]
        A1 = ob in bases[b]
        if A0 != A1:
            return A0
        
        B0 = q in bases[ob]
        B1 = oq in bases[b]
        if B0 != B1:
            return B0

        C0 = q in bases[oq]
        C1 = oq in bases[q]
        if C0 != C1:
            return C0

        D0 = b in bases[oq]
        D1 = ob in bases[q]
        if D0 != D1:
            return D0
        
        """s0 = A0 + B0 + C0 + D0
        s1 = A1 + B1 + C1 + D1
        
        return s0 >= s1"""
        
        return True
            
        
def _create_quote_base_map(xsymbols, markets_coll={}):
    bases = defaultdict(set)
    
    for xc,symbol,base,quote in xsymbols:
        bases[quote].add(base)
    
    return bases


def _extract_cys(symbol, xc, markets_coll):
    base = quote = None
    both_defined = False
    
    if xc in markets_coll:
        if symbol in markets_coll[xc]:
            x = markets_coll[xc][symbol]
            if isinstance(x, dict):
                both_defined = 'base' in x and 'quote' in x
                base = x.get('base')
                quote = x.get('quote')
    
    if not both_defined:
        try: base, quote = symbol.split('/')
        except ValueError: pass
        
    if None not in (base, quote):
        return base, quote
    else:
        return None, None


def _sort_xsymbols(xsymbols, quote_base_map=None, reverse=False, markets_coll={}):
    if quote_base_map is None:
        quote_base_map = _create_quote_base_map(xsymbols, markets_coll)
        
    srs = [_symbol_rank(s, base, quote, quote_base_map) for xc,s,base,quote in xsymbols]
    srs.sort(reverse=reverse)
    #[base-only-cys -> hybrids -> quote-only-cys]
    
    return list(unique(x.symbol for x in srs))


def create_symbol_graph(markets_coll):
    
    xsymbols = _flatten_symbols(markets_coll)
    symbol_list = _sort_xsymbols(xsymbols, reverse=True)
    #print(symbol_list)
    xsymbols.sort(key=lambda x: symbol_list.index(x[1]))
    graph = {}
    
    for i,xcsym in enumerate(xsymbols):
        xc,sym,base,quote = xcsym
        cys = (base,quote)
        graph[(xc,sym)] = add_to = []
        
        if base is None:
            continue
        
        for xcsym2 in xsymbols[i+1:]:
            xc2,sym2,base2,quote2 = xcsym2
            cys2 = (base2,quote2)
            
            if base2 is None:
                continue
            
            for j,_cy in enumerate(cys):
                # _cy in source_cy in sym2, and target_cy in sym1
                try: pos2 = cys2.index(_cy)
                except ValueError: continue
                d = int(not j)
                #(exchange, symbol, direction)
                add_to.append((xc2,sym2,d))
                
    return graph


def _filter_shapes(shapes):
    return unique(shapes, key=lambda shape: tuple(sorted(x[:2] for x in shape)), astype=list)
    

def get_shapes(n, markets_coll, tickers_coll={}, limit_xcs=None, as_tuples=False, compact=None):
    """Returns all n-combinations of (xc,cy) pairs
       :param n: int or range
       markets_coll: {xc: markets, ...}
       tickers_coll: {xc: tickers, ...}
       limit_xcs: <int> - maximum number of different exchanges in a shape"""
    
    is_int = isinstance(n, int)
    n_values = (n,) if isinstance(n,int) else tuple(n)
    max_n = max(n_values)
    
    if min(n_values) < 2:
        raise ValueError('`n` must be >= 2; got: {}'.format(n))

    markets_coll = _filter_markets(markets_coll, tickers_coll)
    symbol_graph = create_symbol_graph(markets_coll)
    
    n_shapes = {i: [] for i in n_values}
    
    def _reverse_trail(t):
        return (t[0],) + (t[-1:0:-1])
    
    def _is_circular(trail):
        first_xc,first_sym, first_d = trail[0]
        last_xc,last_sym, last_d = trail[-1]
        f_cys = _extract_cys(first_sym, first_xc, markets_coll)
        l_cys = _extract_cys(last_sym, last_xc, markets_coll)
        return get_target_cy(l_cys, last_d) == get_source_cy(f_cys, first_d)
    
    _exceeds_xcs_limit = (lambda *a: False) if limit_xcs is None else \
                         (lambda xcs, xc_count, cy_xc: xc_count >= limit_xcs and cy_xc not in xcs)
                         
    def rec(trail):
        trail_0 = trail
        xc,sym,d = trail[-1]
        cys = _extract_cys(sym, xc, markets_coll)
        tcy = get_target_cy(cys, d)
        
        xcs = set(x[0] for x in trail_0)
        xc_count = len(xcs)
        
        n = len(trail) + 1
        is_included = n in n_values
        do_continue = n < max_n
        
        for xc2,sym2,_d in symbol_graph[(xc,sym)]:
            if _d != d or _exceeds_xcs_limit(xcs, xc_count, xc2):
                continue
            
            cys2 = _extract_cys(sym2, xc2, markets_coll)
            d2 = int(cys2.index(tcy))
            trail = trail_0 + ((xc2,sym2,d2),)

            if is_included and _is_circular(trail):
                # currently the trail is middle/(highest_)quote -> base/quote -> base/middle
                # reverse the trail for it to be middle/quote -> base/middle -> base/quote
                trail_r = _reverse_trail(trail)
                n_shapes[n].append(trail_r)
            
            if do_continue:
                rec(trail)
                
                
    for xc,sym in symbol_graph:
        rec(((xc,sym,0),))
    
    
    def _create_cys_map(t):
        cys_map = {}
        for xc,sym,d in t:
            mdict = markets_coll[xc][sym]
            cys_map[(xc,sym)] = (mdict['base'], mdict['quote'])
        return cys_map
    
    #n_shapes = _filter_shapes(n_shapes)
    if not as_tuples:
        n_shapes = {_n: [Shape(t, cys_map=_create_cys_map(t)) for t in items]
                    for _n,items in n_shapes.items()}
        
    if compact is None:
        compact = is_int
    
    if compact:
        n_shapes = list(flatten(n_shapes.values(), exclude_types=(tuple,)))
            
    return n_shapes


if __name__ == '__main__':
    shapes = get_shapes(2, {'a': {'A/B': {}}, 'b': {'A/B': {}}}, as_tuples=False)
    for s in shapes:
        print('{}'.format('-'.join(s.cys)), s.symbols, s.directions, s.exchanges)
        for c in s.paths:
            print('::{}'.format('-'.join(c.cys)), c.symbols, c.directions, c.exchanges)