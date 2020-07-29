import itertools
import pandas as pd
import ccxt
import datetime
dt = datetime.datetime

from .basics import (quotation_as_string, as_source, as_target,
                     as_ob_side, get_crossed_condition)
from .utils import resolve_times

ISOFORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def get_stop_condition(side, closed=True, inverse=False):
    """Returns the stop condition operation if book is iterated from start
       (price a is more *inwards* than stop price b; sc(a,b) == True)"""
    return get_crossed_condition(side, closed, not inverse)


def is_ob_crossed(price, other, side, closed=True, inverse=False):
    sc = get_stop_condition(side, closed, inverse)
    return sc(price, other)


def exec_step_by_base_volume(it, step, price=None, remainder=0, 
                             cumother=0, stop='raise', i=None):
    """Set `cumother` to None to exclude from calculation"""
    #To allow the remainder from `get_to_matching_price` to be used in this function
    if hasattr(remainder,'__iter__'):
        if remainder[0] is not None:
            it = itertools.chain([remainder],it)
        remainder = 0
    cumvol = remainder
    calc_co = cumother is not None
    _i = 0
    if cumvol < step:
        if remainder and calc_co:
            cumother += price*remainder
        while cumvol < step:
            try: price,vol = next(it)
            except StopIteration as e:
                if stop=='raise':
                    raise e
                #remainder will be negative
                else: break
            else: _i += 1
            cumvol += vol
            if calc_co:
                if cumvol < step:
                    cumother += price*vol
                else:
                    cumother += price*(step-(cumvol-vol))
    elif calc_co:
        cumother += price*step
    remainder = cumvol - step
    if i is not None:
        return price,remainder,cumother,i+_i
    return price,remainder,cumother


def exec_step_by_quote_volume(it, step, price=None, remainder=0,
                              cumother=0, stop='raise', i=None):
    """Set `cumother` to None to exclude from calculation"""
    #To allow the remainder from `get_to_matching_price` to be used in this function
    if hasattr(remainder,'__iter__'):
        if remainder[0] is not None:
            it = itertools.chain([remainder],it)
        remainder = 0
    cumvol = remainder
    calc_co = cumother is not None
    _i = 0
    if cumvol < step:
        if remainder and calc_co:
            cumother += remainder/price
        while cumvol < step:
            try: price,vol = next(it)
            except StopIteration as e:
                if stop=='raise':
                    raise e
                #remainder will be negative
                else: break
            else: _i += 1
            qvol = price*vol
            cumvol += qvol
            if calc_co:
                if cumvol < step:
                    cumother += vol
                else:
                    cumother += (step-(cumvol-qvol))/price
    elif calc_co:
        cumother += step/price
    remainder = cumvol - step
    if i is not None:
        return price,remainder,cumother,i+_i
    return price,remainder,cumother


def exec_step(it, step, price=None, remainder=0,
              cumother=0,stop='raise', i=None, unit='base'):
    unit = quotation_as_string(unit)
    if unit == 'base':
        return exec_step_by_base_volume(it,step,price=price,remainder=remainder,cumother=cumother,stop=stop,i=i)
    else:
        return exec_step_by_quote_volume(it,step,price=price,remainder=remainder,cumother=cumother,stop=stop,i=i)
    
    
def select_next_step(current_step, current_cumother, current_unit, current_direction):
    cur_unit = quotation_as_string(current_unit)
    cur_dest = as_target(current_direction)
    #A was converted to B. Following B->C conversion needs B as quotation step.
    # If A->B step was quoted in B, that'd be current_step
    # If A->B step was quoted in A, that'd be current_cumother
    return current_step if cur_unit == cur_dest else current_cumother


def select_prev_step(current_step, current_cumother, current_unit, current_direction):
    cur_unit = quotation_as_string(current_unit)
    cur_source = as_source(current_direction)
    #B was converted to C. A->B would need B as quotation step.
    # If B->C step was quoted in C, that'd be current_cumother
    # If B->C step was quoted in B, that'd be current_step
    return current_step if cur_source == cur_unit else current_cumother


def calc_vwap(step, cumother, unit='base'):
    unit = quotation_as_string(unit)
    if unit == 'base':
        return cumother/step
    else:
        return step/cumother
    

def get_to_matching_volume(ob_branch, volume, price=None,
                           remainder=0, cumother=0, i=0, unit='base'):
    item = exec_step(iter(ob_branch), volume, price, 
                     remainder, cumother, stop='ignore', i=i, unit=unit)
    price,remainder,cumother = item[:3]
    try: i = item[3]
    except IndexError: pass

    try: vwap = calc_vwap(volume,cumother,unit) if cumother is not None else None
    except ZeroDivisionError: vwap = None
    
    return {'price': price, 'vwap': vwap, 
            'remainder': remainder, 
            'cumother': cumother, 'i': i}


def get_to_matching_price(ob_branch, price, side, closed=True,
                          remainder=(None,0), cumother=0, i=0):
    """`remainder` is tuple containing the last iteration result."""
    op = get_stop_condition(side,closed)
    it = iter(ob_branch)
    cumvol = _i = 0
    calc_co = cumother is not None
    if remainder[0] is not None:
        it = itertools.chain([remainder],it)
        remainder = (None,0)
    while True:
        try: _p,_v = next(it)
        except StopIteration: break
        else: _i += 1
        if op(_p,price):
            remainder = (_p,_v)
            _i -= 1
            break
        cumvol += _v
        if calc_co:
            cumother += _p*_v
                    
    try: vwap = calc_vwap(cumvol,cumother) if calc_co else None
    except ZeroDivisionError: vwap = None
    
    if i is not None:
        i += _i
        
    return {'volume': cumvol, 'vwap': vwap, 
            'remainder': remainder,
            'cumother': cumother, 'i': i}


def parse_item(x, price_key=0, amount_key=1):
    return [float(x[price_key]), float(x[amount_key])]


def parse_branch(branch, price_key=0, amount_key=1):
    if branch is None:
        return []
    return [parse_item(x, price_key, amount_key) for x in branch]


def sort_branch(branch, side='bids'):
    return sorted(branch, key=lambda x:x[0], reverse=(side in ('bid','bids')))


def create_orderbook(data, add_time=False, bids_key='bids', asks_key='asks', price_key=0, amount_key=1):
    datetime, timestamp = resolve_times(data, add_time)
    return {
        'symbol': data['symbol'],
        'bids': sort_branch(parse_branch(data.get(bids_key), price_key, amount_key), 'bids'),
        'asks': sort_branch(parse_branch(data.get(asks_key), price_key, amount_key), 'asks'),
        'timestamp': timestamp,
        'datetime': datetime,
        'nonce': data.get('nonce')
    }


def update_branch(item, branch, side='bids', is_delta=False, round_to=None):
    """:param round_to: adding deltas is imprecise, new amount is rounded"""
    rate, amount = new_item = parse_item(item)
    if not rate:
        return (.0, .0, .0)
    op = rate.__le__ if side in ('ask','asks') else rate.__ge__
    empty_place = (-1, [rate, .0])
    loc, ob_item = next(((i,x) for i,x in enumerate(branch) if op(x[0])), empty_place)
    prev_rate, prev_amount = ob_item
    new_amount = amount
    if loc != -1:
        if prev_rate == rate: # prices are equal -> item is swapped out / removed
            if is_delta:
                new_amount = max(.0, prev_amount + amount)
                if round_to is not None:
                    new_amount = round(new_amount, round_to)
                new_item = [rate, new_amount]
            if new_amount:
                branch[loc] = new_item
            else:
                branch.pop(loc)
        elif new_amount > 0:
            branch.insert(loc, new_item)
    elif new_amount > 0:
        branch.append(new_item)
    
    return (rate, prev_amount, max(.0, new_amount))
    
    
def assert_integrity(ob):
    ask,bid = ob['asks'],ob['bids']
    assert all(ask[i][0] < ask[i+1][0] for i in range(max(0,len(ask)-1)))
    #except AssertionError: print('Contains unsorted ask: {}'.format(ask))
    assert all(bid[i][0] > bid[i+1][0] for i in range(max(0,len(bid)-1)))
    #except AssertionError: print('Contains unsorted bid: {}'.format(bid))


def infer_side(ob, price):
    """
    Determine in which ob side the price is located
    """
    bid, ask = get_bidask(ob)
    
    if bid and price <= bid:
        return 'bids'
    elif ask and price >= ask:
        return 'asks'
    else:
        return None


def get_bidask(ob, as_dict=False):
    bid = ask = bidVolume = askVolume = None
    if ob['bids']:
        bid, bidVolume = ob['bids'][0]
    if ob['asks']:
        ask, askVolume = ob['asks'][0]
    
    if not as_dict:
        return bid, ask
    
    return {
        'bid': bid,
        'bidVolume': bidVolume,
        'ask': ask,
        'askVolume': askVolume,
    }


def calc_ob_mid_price(ob, na=None):
    """
    :returns:
        mid price or `na` if it could not be determined
    """
    return calc_mid_price(*get_bidask(ob), na)


def calc_mid_price(bid, ask, na=None):
    """
    :returns:
        mid price or `na` if it could not be determined
    """
    bid_ = bid and not pd.isnull(bid)
    ask_ = ask and not pd.isnull(ask)
    if bid_ and ask_:
        return (bid + ask) / 2
    elif bid_:
        return bid
    elif ask_:
        return ask
    else:
        return na
