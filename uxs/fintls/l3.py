from collections import defaultdict

from .utils import resolve_times
from .ob import sort_branch


def parse_l3_item(x, price_key=0, amount_key=1, id_key=2):
    return [float(x[price_key]), float(x[amount_key]), x[id_key]]


def parse_l3_branch(branch, price_key=0, amount_key=1, id_key=2):
    if branch is None:
        return []
    return [parse_l3_item(x, price_key, amount_key, id_key) for x in branch]


def sort_l3_branch(branch, side="bids"):
    return sorted(branch, key=lambda x: x[0], reverse=(side in ("bid", "bids")))


def create_l3_orderbook(
    data,
    add_time=False,
    bids_key="bids",
    asks_key="asks",
    price_key=0,
    amount_key=1,
    id_key=2,
):
    datetime, timestamp = resolve_times(data, add_time)
    return {
        "symbol": data["symbol"],
        "bids": sort_l3_branch(
            parse_l3_branch(data.get(bids_key), price_key, amount_key, id_key), "bids"
        ),
        "asks": sort_l3_branch(
            parse_l3_branch(data.get(asks_key), price_key, amount_key, id_key), "asks"
        ),
        "timestamp": timestamp,
        "datetime": datetime,
        "nonce": data.get("nonce"),
    }


def get_l3_loc_by_id(branch, id):
    return next((i for i, x in enumerate(branch) if x[2] == id), None)


def get_full_l3_loc_by_id(ob, id):
    side = loc = None
    for side in ("bids", "asks"):
        branch = ob[side]
        loc = get_l3_loc_by_id(branch, id)
        if loc is not None:
            break
    return side, loc


def update_l3_branch(item, branch, side="bids"):
    """:returns: (price, amount, id, previous_price, previous_amount"""
    new_item = parse_l3_item(item)
    price, amount, id = new_item[:3]
    empty_value = (-1, [0.0, 0.0, None])
    ident_loc, ident_item = next(
        ((i, x) for i, x in enumerate(branch) if x[2] == id), empty_value
    )
    ident_price, ident_amount = ident_item[:2]
    if ident_loc != -1:
        if not amount or not price:
            branch.pop(ident_loc)
        elif ident_price == price:
            branch[ident_loc] = new_item
            return (price, amount, id, ident_price, ident_amount)
        else:
            branch.pop(ident_loc)
    if not amount or not price:
        return (price, 0.0, id, ident_price, ident_amount)
    op = price.__lt__ if side in ("ask", "asks") else price.__gt__
    loc = next((i for i, x in enumerate(branch) if op(x[0])), -1)
    if loc != -1:
        branch.insert(loc, new_item)
    else:
        branch.append(new_item)
    return (price, amount, id, 0.0, 0.0)


def assert_l3_integrity(ob):
    asks, bids = ob["asks"], ob["bids"]
    assert all(asks[i][0] <= asks[i + 1][0] for i in range(max(0, len(asks) - 1)))
    # except AssertionError: print('Contains unsorted ask: {}'.format(ask))
    assert all(bids[i][0] >= bids[i + 1][0] for i in range(max(0, len(bids) - 1)))
    # except AssertionError: print('Contains unsorted bid: {}'.format(bid))


def l3_branch_to_l2(branch, side="bids", round_to=None):
    if not branch:
        return []
    by_price = defaultdict(float)
    for item in branch:
        price, amount = item[:2]
        by_price[price] += amount
    if round_to is not None:
        by_price = {p: round(a, round_to) for p, a in by_price.items()}
    return sort_branch(by_price.items(), side)


def l3_to_l2(ob, round_to=None):
    bidasks = {ba: l3_branch_to_l2(ob.get(ba), ba, round_to) for ba in ("bids", "asks")}
    return dict(ob, **bidasks)
