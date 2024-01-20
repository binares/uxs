from __future__ import annotations
from typing import (
    Tuple,
    List,
    Union,
    Iterator,
    Iterable,
    Literal,
    overload,
)  # , Annotated  # Python 3.9
import itertools
import pandas as pd
import ccxt
from ccxt.base.types import OrderBook as CCXTOrderBook, Num
import datetime

dt = datetime.datetime

from .basics import (
    quotation_as_string,
    as_source,
    as_target,
    as_ob_side,
    get_crossed_condition,
)
from .utils import resolve_times

ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"

IntNA = Union[int, None]
FloatNA = Union[float, None]

OrderBookSide = Literal["bids", "asks"]
OrderBookItem = Union[Tuple[float, float], Tuple[float, float, int]]
# OrderBookItem = Annotated[
#    List[float], 3
# ]  # Better use 'tuple' type and `type: ignore` comments than uniformly-typed `list`
#    # with non-enforced length
OrderBookIterator = Iterator[OrderBookItem]
OrderBookBranch = List[OrderBookItem]


class OrderBook(CCXTOrderBook):
    bids: OrderBookBranch  # type: ignore
    asks: OrderBookBranch  # type: ignore


class _OrderBook(OrderBook):
    symbol: str


Remainder = Union[float, OrderBookItem]


def get_stop_condition(side, closed=True, inverse=False):
    """Returns the stop condition operation if book is iterated from start
    (price a is more *inwards* than stop price b; sc(a,b) == True)"""
    return get_crossed_condition(side, closed, not inverse)


def is_ob_crossed(price, other, side, closed=True, inverse=False):
    sc = get_stop_condition(side, closed, inverse)
    return sc(price, other)


def exec_step_by_base_volume(
    it: OrderBookIterator,
    step: float,
    price: FloatNA = None,
    remainder: Remainder = 0,
    cumother: FloatNA = 0,
    stop="raise",
    i=None,
):
    """Set `cumother` to None to exclude from calculation"""
    # To allow the remainder from `get_to_matching_price` to be used in this function
    if isinstance(remainder, Iterable):
        if remainder[0] is not None:
            it = itertools.chain([remainder], it)
        remainder = 0
    cumvol = remainder
    calc_co = cumother is not None
    _i = 0
    if cumvol < step:
        if remainder and calc_co:
            if not price:
                raise ValueError(
                    "`price` must be provided if `cumother` is not None and `remainder` is specified"
                )
            cumother += price * remainder
        while cumvol < step:
            try:
                price, vol = next(it)[:2]
            except StopIteration as e:
                if stop == "raise":
                    raise e
                # remainder will be negative
                else:
                    break
            else:
                _i += 1
            cumvol += vol
            if calc_co:
                if cumvol < step:
                    cumother += price * vol
                else:
                    cumother += price * (step - (cumvol - vol))
    elif calc_co:
        if not price:
            raise ValueError("`price` must be provided if `cumother` is not None")
        cumother += price * step
    remainder = cumvol - step
    if i is not None:
        return price, remainder, cumother, i + _i
    return price, remainder, cumother


def exec_step_by_quote_volume(
    it: OrderBookIterator,
    step: float,
    price: FloatNA = None,
    remainder: Remainder = 0,
    cumother: FloatNA = 0,
    stop="raise",
    i=None,
):
    """
    param cumother: set to None to exclude from calculation
    """
    # To allow the remainder from `get_to_matching_price` to be used in this function
    if isinstance(remainder, Iterable):
        if remainder[0] is not None:
            it = itertools.chain([remainder], it)
        remainder = 0
    cumvol = remainder
    calc_co = cumother is not None
    _i = 0
    if cumvol < step:
        if remainder and calc_co:
            if not price:
                raise ValueError(
                    "`price` must be provided if `cumother` is not None and `remainder` is specified"
                )
            cumother += remainder / price
        while cumvol < step:
            try:
                price, vol = next(it)[:2]
            except StopIteration as e:
                if stop == "raise":
                    raise e
                # remainder will be negative
                else:
                    break
            else:
                _i += 1
            qvol = price * vol
            cumvol += qvol
            if calc_co:
                if cumvol < step:
                    cumother += vol
                else:
                    cumother += (step - (cumvol - qvol)) / price
    elif calc_co:
        if not price:
            raise ValueError("`price` must be provided if `cumother` is not None")
        cumother += step / price
    remainder = cumvol - step
    if i is not None:
        return price, remainder, cumother, i + _i
    return price, remainder, cumother


def exec_step(
    it: OrderBookIterator,
    step: float,
    price: FloatNA = None,
    remainder: Remainder = 0,
    cumother: FloatNA = 0,
    stop="raise",
    i=None,
    unit="base",
):
    unit = quotation_as_string(unit)
    if unit == "base":
        return exec_step_by_base_volume(
            it,
            step,
            price=price,
            remainder=remainder,
            cumother=cumother,
            stop=stop,
            i=i,
        )
    else:
        return exec_step_by_quote_volume(
            it,
            step,
            price=price,
            remainder=remainder,
            cumother=cumother,
            stop=stop,
            i=i,
        )


def select_next_step(current_step, current_cumother, current_unit, current_direction):
    cur_unit = quotation_as_string(current_unit)
    cur_dest = as_target(current_direction)
    # A was converted to B. Following B->C conversion needs B as quotation step.
    # If A->B step was quoted in B, that'd be current_step
    # If A->B step was quoted in A, that'd be current_cumother
    return current_step if cur_unit == cur_dest else current_cumother


def select_prev_step(current_step, current_cumother, current_unit, current_direction):
    cur_unit = quotation_as_string(current_unit)
    cur_source = as_source(current_direction)
    # B was converted to C. A->B would need B as quotation step.
    # If B->C step was quoted in C, that'd be current_cumother
    # If B->C step was quoted in B, that'd be current_step
    return current_step if cur_source == cur_unit else current_cumother


def calc_vwap(step, cumother, unit="base"):
    unit = quotation_as_string(unit)
    if unit == "base":
        return cumother / step
    else:
        return step / cumother


def get_to_matching_volume(
    ob_branch: OrderBookBranch,
    volume: float,
    price: FloatNA = None,
    remainder: Remainder = 0,
    cumother: FloatNA = 0,
    i=0,
    unit="base",
):
    item = exec_step(
        iter(ob_branch),
        volume,
        price,
        remainder,
        cumother,
        stop="ignore",
        i=i,
        unit=unit,
    )
    price, remainder, cumother = item[:3]
    try:
        i = item[3]  # type: ignore
    except IndexError:
        pass

    try:
        vwap = calc_vwap(volume, cumother, unit) if cumother is not None else None
    except ZeroDivisionError:
        vwap = None

    return {
        "price": price,
        "vwap": vwap,
        "remainder": remainder,
        "cumother": cumother,
        "i": i,
    }


def get_to_matching_price(
    ob_branch: OrderBookBranch,
    price: float,
    side: OrderBookSide,
    closed: bool = True,
    remainder: tuple[FloatNA, float] = (None, 0),
    cumother: FloatNA = 0,
    i=0,
):
    """`remainder` is tuple containing the last iteration result."""
    op = get_stop_condition(side, closed)
    it = iter(ob_branch)
    cumvol = _i = 0
    calc_co = cumother is not None
    if remainder[0] is not None:
        it: Iterator[OrderBookItem] = itertools.chain([remainder], it)  # type: ignore
        remainder = (None, 0)
    while True:
        try:
            _p, _v = next(it)[:2]
        except StopIteration:
            break
        else:
            _i += 1
        if op(_p, price):
            remainder = (_p, _v)
            _i -= 1
            break
        cumvol += _v
        if calc_co:
            cumother += _p * _v

    try:
        vwap = calc_vwap(cumvol, cumother) if calc_co else None
    except ZeroDivisionError:
        vwap = None

    if i is not None:
        i += _i

    return {
        "volume": cumvol,
        "vwap": vwap,
        "remainder": remainder,
        "cumother": cumother,
        "i": i,
    }


def parse_item(x, price_key=0, amount_key=1, count_or_id_key=None) -> OrderBookItem:
    item = [float(x[price_key]), float(x[amount_key])]
    if count_or_id_key is not None:
        item.append(int(x[count_or_id_key]))
    return item  # type: ignore


def parse_branch(branch, price_key=0, amount_key=1, count_or_id_key=None):
    if branch is None:
        return []
    return [parse_item(x, price_key, amount_key, count_or_id_key) for x in branch]


def sort_branch(branch, side="bids"):
    return sorted(branch, key=lambda x: x[0], reverse=(side in ("bid", "bids")))


def create_orderbook(
    data,
    add_time=False,
    bids_key="bids",
    asks_key="asks",
    price_key=0,
    amount_key=1,
    count_or_id_key=2,
) -> _OrderBook:
    datetime, timestamp = resolve_times(data, add_time)
    return {
        "symbol": data["symbol"],
        "bids": sort_branch(
            parse_branch(data.get(bids_key), price_key, amount_key, count_or_id_key),
            "bids",
        ),
        "asks": sort_branch(
            parse_branch(data.get(asks_key), price_key, amount_key, count_or_id_key),
            "asks",
        ),
        "timestamp": timestamp,
        "datetime": datetime,
        "nonce": data.get("nonce"),
    }


def update_branch(
    item,
    branch: OrderBookBranch,
    side: OrderBookSide = "bids",
    is_delta: bool = False,
    round_to: IntNA = None,
    **keys,
):
    """:param round_to: adding deltas is imprecise, new amount is rounded"""
    new_item = parse_item(item, **keys)
    rate, amount = new_item[:2]
    if not rate:
        return (0.0, 0.0, 0.0)
    op = rate.__le__ if side in ("ask", "asks") else rate.__ge__
    empty_place = (-1, [rate, 0.0])
    loc, ob_item = next(((i, x) for i, x in enumerate(branch) if op(x[0])), empty_place)
    prev_rate, prev_amount = ob_item[:2]
    new_amount = amount
    if loc != -1:
        if prev_rate == rate:  # prices are equal -> item is swapped out / removed
            if is_delta:
                new_amount = max(0.0, prev_amount + amount)
                if round_to is not None:
                    new_amount = round(new_amount, round_to)
                new_item: OrderBookItem = [rate, new_amount, *new_item[2:]]  # type: ignore
            if new_amount:
                branch[loc] = new_item
            else:
                branch.pop(loc)
        elif new_amount > 0:
            branch.insert(loc, new_item)
    elif new_amount > 0:
        branch.append(new_item)

    return (rate, prev_amount, max(0.0, new_amount))


def assert_integrity(ob: OrderBook):
    ask, bid = ob["asks"], ob["bids"]
    assert all(ask[i][0] < ask[i + 1][0] for i in range(max(0, len(ask) - 1)))
    # except AssertionError: print('Contains unsorted ask: {}'.format(ask))
    assert all(bid[i][0] > bid[i + 1][0] for i in range(max(0, len(bid) - 1)))
    # except AssertionError: print('Contains unsorted bid: {}'.format(bid))


def infer_side(ob: OrderBook, price: float):
    """
    Determine in which ob side the price is located
    """
    bid, ask = get_bidask(ob)

    if bid and price <= bid:
        return "bids"
    elif ask and price >= ask:
        return "asks"
    else:
        return None


@overload
def get_bidask(
    ob: OrderBook, as_dict: Literal[False] = False
) -> Tuple[FloatNA, FloatNA]:
    ...


@overload
def get_bidask(ob: OrderBook, as_dict: Literal[True] = True) -> dict[str, FloatNA]:
    ...


def get_bidask(ob: OrderBook, as_dict: bool = False):
    bid = ask = bidVolume = askVolume = None
    if ob["bids"]:
        k = ob["bids"][0]
        bid, bidVolume = ob["bids"][0][:2]
    if ob["asks"]:
        ask, askVolume = ob["asks"][0][:2]

    if not as_dict:
        return bid, ask

    return {
        "bid": bid,
        "bidVolume": bidVolume,
        "ask": ask,
        "askVolume": askVolume,
    }


def calc_ob_mid_price(ob: OrderBook, na=None):
    """
    :returns:
        mid price or `na` if it could not be determined
    """
    return calc_mid_price(*get_bidask(ob), na)


def calc_mid_price(bid: FloatNA, ask: FloatNA, na=None):
    """
    :returns:
        mid price or `na` if it could not be determined
    """
    bid_ = bid and not pd.isnull(bid)
    ask_ = ask and not pd.isnull(ask)
    if bid_ and ask_:
        return (bid + ask) / 2  # type: ignore
    elif bid_:
        return bid
    elif ask_:
        return ask
    else:
        return na
