from fons.math.graph import find_all_paths

#DIRECTION_1 = ('buy','base','to-base','quote-to-base','bid','bids','is-bid','into-ask','into-asks','from-quote','from-quote-to-base')
#DIRECTION_0 = ('sell','quote','to-quote','base-to-quote','ask',asks','is-ask','into-bid','into-bids','from-base','from-base-to-quote')

#QUOTE is 0, BASE is 1
#DIRECTION is derived from that

#QUOTATION : 'quote' (0), 'base' (1)


def as_direction(direction, inverse=False):
    """`direction` - direction of the conversion 
     - quote to base = 1 = "buy" = "base" = "into-asks"
     - base to quote = 0 = "sell" = "quote" = "into-bids"""
    if isinstance(direction,int): 
        i = bool(direction)
    elif isinstance(direction,str):
        direction = direction.lower().replace(' ','-').replace('_','-')
        if direction in ('buy','base','to-base','quote-to-base','bid','bids','is-bid',
                         'into-ask','into-asks','from-quote','from-quote-to-base'):
            i = 1
        elif direction in ('sell','quote','to-quote','base-to-quote','ask','asks','is-ask',
                           'into-bid','into-bids','from-base','from-base-to-quote'):
            i = 0
        else:
            raise ValueError(direction)
    else:
        raise TypeError(type(direction))
    if inverse:
        i = not i
    return int(i)


def as_source(direction, as_string=True):
    direction = as_direction(direction)
    source = int(not direction)
    if as_string:
        source = quotation_as_string(source)
    return source


def as_target(direction, as_string=True):
    direction = as_direction(direction)
    destination = direction
    if as_string:
        destination = quotation_as_string(destination)
    return destination

as_destination = as_target


def get_source_cy(symbol, direction):
    direction = as_direction(direction)
    split = symbol.split('/') if isinstance(symbol, str) else symbol
    return split[direction]


def get_target_cy(symbol, direction):
    direction = as_direction(direction)
    split = symbol.split('/') if isinstance(symbol, str) else symbol
    return split[not direction]


def quotation_as_string(unit):
    #unit is a member of currency pair ("base" or "quote")
    if isinstance(unit,str):
        unit = unit.lower()
        if unit not in ('quote','base'):
            raise ValueError(unit)
        return unit
    else:
        return ('quote','base')[bool(unit)]


def quotation_as_int(unit):
    if isinstance(unit,str):
        unit = unit.lower()
        if unit == 'quote':
            return 0
        elif unit == 'base':
            return 1
        else:
            raise ValueError(unit)
    else:
        return int(bool(unit))


def convert_quotation(quotation, direction, as_string=True):
    if isinstance(quotation,str):
        quotation = quotation.lower()
        if quotation == 'source':
            quotation = as_source(direction, as_string)
        elif quotation in ('target','destination'):
            quotation = as_target(direction, as_string)
    
    return quotation_as_string(quotation) if as_string else quotation_as_int(quotation)
    

def quote_to_base(amount, price):
    return amount/price


def base_to_quote(amount, price):
    return amount*price


def get_conversion_op(direction, inverse=False):
    """Returns the operation that calculates the product (base/quote) 
    if quantity x (quote/base) filled into matching order with price y"""
    _ops = [base_to_quote, quote_to_base]
    i = as_direction(direction,inverse)
    return _ops[i]


def convert(amount, price, direction='buy'):
    op = get_conversion_op(direction)
    return op(amount,price)


def as_ob_side(direction, as_string=True, inverse=False):
    """Returns (source) orderbook side for direction"""
    #1 is bid because direction-1 order is buy order (bid)
    i = as_direction(direction, inverse)
    if as_string:
        return ('asks','bids')[i]
    return i


def as_ob_side2(item, as_string=True):
    #1 is bid because direction-1 order is buy order (bid)
    if isinstance(item,int): 
        i = int(bool(item))
    elif isinstance(item,str):
        item = item.lower()
        if item in ('bid','bids'):
            i = 1
        elif item == ('ask','asks'):
            i = 0
        else:
            raise ValueError(item)
    else:
        raise TypeError(type(item))
    if as_string:
        return ('asks','bids')[i]
    return i
    

def as_ob_fill_side(direction, as_string=True, inverse=False):
    """Returns target orderbook side for direction"""
    i = as_direction(direction,inverse)
    if as_string:
        return ('bids','asks')[i]
    return i


def get_crossed_condition(side, closed=True, inverse=False):
    """For *outwards* crossing. Set inverse to True for inwards crossing."""
    if closed: _ops = [lambda x,y: x<y, lambda x,y: x>y]
    else: _ops = [lambda x,y: x<=y, lambda x,y: x>=y]
    i = as_direction(side, inverse)
    return _ops[i]


def has_crossed(price, other, side, closed=True, inverse=False):
    """
    returns True if price is more *outwards* than other
    :param side:
        'bid': price > other
        'ask': price < other
    :param closed: 
        if False, will also return True if price == other
    :param inverse:
        if True, reverses the side
    """
    sc = get_crossed_condition(side, closed, inverse)
    return sc(price, other)


def is_outwards_crossed(price, other, side, closed=True):
    return has_crossed(price, other, side, closed)

def is_inwards_crossed(price, other, side, closed=True):
    return has_crossed(price, other, side, closed, inverse=True)

#------------------------------

def create_cy_graph(markets):
    graph = {}
    
    for m in markets:
        try: base,quote = m.split('/')
        except ValueError: continue

        add= {quote: {base: 1},
              base: {quote: 0}}
        
        for cy,D in add.items():
            try: graph[cy].update(D)
            except KeyError:
                graph[cy] = D

    return graph


def find_optimal_paths(source_cy, target_cy, markets_or_graph, max_len=4):
    graph = create_cy_graph(markets_or_graph) if not \
             isinstance(markets_or_graph,dict) else markets_or_graph
    
    #this still works for a "cy_graph", because cy_graph's values (dict) still 
    # satisfy the requirement of a value being iterable over (the key's) target-nodes
    paths = find_all_paths(graph, source_cy, target_cy, max_len=max_len)
    lengths = [len(p) for p in paths]
    min_len = min(lengths) if lengths else 0
    shortest_paths = [paths[i] for i,l in enumerate(lengths) if l==min_len]
    #SNRG-BTC-USDT -> this naturally comes first if BASE_COINS = [USDT,BTC,ETH,...]
    #SNRG-ETH-USDT
    with_directions = []
    for pth in shortest_paths:
        new_pth = [(pth[0],None)]
        prev = pth[0]
        for cur in pth[1:]:
            direction = graph[prev][cur]
            new_pth.append((cur,direction))
            prev=cur
        with_directions.append(new_pth)
        
    return with_directions
    

def calc_price(market, prices, graph_or_paths=None, max_len=4):
    """`market`: 'base/quote' or (base,quote)
         Note that the market needs not exist on the graph, e.g.
         USD/EUR can be calculated from EUR/USD.
         For not calculating average price along all paths
         pass a list containing a single path (instead of graph).
       `prices`: dict of market:price
       `graph_or_paths` is either:
       `graph`: resulting from `create_cy_graph`
       `paths`: [(USD,None),(EUR,1),(JPY,0)], where 1 denotes that market 
            EUR/USD exists in the prices, where EUR is the base (==1), and
            EUR/JPY exists, where JPY is the quote (==0). The final market 
            (which's price we calculate) is first_cy/last_cy (USD/JPY)."""
    #base and quote pair
    if not isinstance(market, str):
        if len(market) != 2:
            raise ValueError(market)
        market = '/'.join(market)
    #market = market.upper()
    
    if market in prices:
        return prices[market]
    
    base,quote = market.split('/')   
    if graph_or_paths is None:
        graph_or_paths = create_cy_graph(prices.keys())
    
    if isinstance(graph_or_paths,dict):
        graph = graph_or_paths
        #print('finding optimal paths for: {}'.format(market))
        paths = find_optimal_paths(base, quote, graph, max_len)
        #print('finding optimal paths for: {} - done'.format(market))
    else:
        paths = graph_or_paths
    
    final_prices = []
    for pth in paths:
        prev = pth[0][0]
        final_price = 1
        #the base cy (in final_price) always remains pth[0][0]
        # what changes is what pth[0][0] is quoted in
        #(in the first iteration the pth[0][0] is set as base)
        for cur,direction in pth[1:]:
            #direction = graph[prev][cur]
            #(direction == side of cur cy == not(side of prev cy))
            b,q = (cur,prev) if direction else (prev,cur)
            _market = '/'.join((b,q))
            price = prices.get(_market)
            prev = cur
            if price is None:
                final_price = None
                break
            #previous quote is also new quote. we divide
            if direction:
                final_price /= price
            #previous quote is current market's base. we multiply
            else:
                final_price *= price
                
        if final_price is not None:
            final_prices.append(final_price)
            
    if not final_prices:
        raise RuntimeError("Could not resolve price for market '{}'".format(market))

    return sum(final_prices) / len(final_prices)


if __name__ == '__main__':
    prices = {'EUR/USD': 1.12, 'EUR/JPY': 124, 'EUR/AUD': 1.2, 'USD/AUD': 1.43, 'AUD/JPY': 77.55}
    graph = create_cy_graph(prices)
    print(graph)
    paths = find_optimal_paths('USD','JPY',graph)
    print(paths)
    print(calc_price('USD/JPY',prices))
    print(calc_price('USD/JPY',prices,graph))
    print(calc_price('USD/JPY',prices,paths))
    print(calc_price('JPY/USD',prices))
    print(calc_price('EUR/USD',prices))
    print(calc_price(('USD','EUR'),prices))
