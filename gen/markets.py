import os, sys
import json
from copy import deepcopy
import decimal
import asyncio
import uxs
from uxs.fintls.basics import calc_price
from fons.argv import parse_argv
from fons.log import get_standard_5


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "uxs", "_data")

logger, logger2, tlogger, tloggers, tlogger0 = get_standard_5(__name__)


def get_path(exchange):
    return os.path.join(DATA_DIR, exchange + "_markets.json")


def read_info(api):
    path = get_path(api.id)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_info(api, info):
    path = get_path(api.id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(info, f)


def infer_precision(value):
    if int(value) == value:
        n = 1
        while int(value) == value:
            value /= 10
            n -= 1
        return n
    # else:
    #    n = 0
    #    while int(value * 10**n) != value * 10**n:
    #        n += 1
    #    return n
    d = decimal.Decimal(str(value))
    return -(d.as_tuple().exponent)


async def load_market_info(api, market, min_cost=None, tickers=None, cy_graph=None):
    ob = (await uxs.poll.get(api, ("orderbook", market["symbol"]), "30D"))[0].data
    trades = []
    if len(ob["bids"]) + len(ob["asks"]) < 8:
        trades = (await uxs.poll.get(api, ("trades", market["symbol"]), "30D"))[0].data
    prices = sorted(
        [x[0] for x in ob["bids"]]
        + [x[0] for x in ob["asks"]]
        + [x["price"] for x in trades]
    )
    amounts = sorted(
        [x[1] for x in ob["bids"]]
        + [x[1] for x in ob["asks"]]
        + [x["amount"] for x in trades]
    )
    _len = len(prices)
    price_precisions = [infer_precision(x) for x in prices]
    amount_precisions = [infer_precision(x) for x in amounts]
    precision = {
        "price": sorted(price_precisions)[int(_len * 7 / 8)],
        "amount": sorted(amount_precisions)[int(_len * 7 / 8)],
    }
    limits = {
        #'price': {
        #    'min': min(prices),    # this can be anomalous due to thin orderbook
        # },
        "amount": {
            "min": amounts[
                int(_len / 8)
            ],  # pick a smallish amount, but exclude the smallest which could be anomalous
        },
    }
    if hasattr(min_cost, "__iter__"):
        cost_in_currency, currency = min_cost
        symbol = market["quote"] + "/" + currency
        quote_price_in_currency = calc_price(
            symbol, {s: x["last"] for s, x in tickers.items()}, cy_graph
        )
        min_cost = cost_in_currency / quote_price_in_currency

        symbol2 = market["base"] + "/" + currency
        base_price_in_currency = calc_price(
            symbol2, {s: x["last"] for s, x in tickers.items()}, cy_graph
        )
        min_base = cost_in_currency / base_price_in_currency
        # if the min_amount is probably accurate (large enough), no need to use randomly set min_cost
        if limits["amount"]["min"] >= min_base * 1 / 4:
            min_cost = None

    if min_cost is not None:
        limits["cost"] = {"min": min_cost}
    return {
        "precision": precision,
        "limits": limits,
    }


async def load_info(api, info={}, *, min_cost=None, symbols=None, recalculate=False):
    info = deepcopy(info)
    await api.load_markets()
    tickers = await uxs.poll.fetch(api, "tickers", "1D")
    for symbol, market in sorted(api.markets.items(), key=lambda x: x[0]):
        if (
            symbol in info
            and not recalculate
            or symbols is not None
            and symbol not in symbols
        ):
            continue
        try:
            market_info = await load_market_info(
                api, market, min_cost, tickers, api.load_cy_graph()
            )
            if market_info:
                info[symbol] = market_info
        except Exception as e:
            logger2.error(
                "{} - could not load {} precisions: {}".format(api.id, symbol, repr(e))
            )
            logger.exception(e)
    return info


def _parse_cost(cost):
    cost = cost.replace("_", "").replace("-", "")
    non_digit = next(i for i, x in enumerate(cost) if not x.isdigit())
    return (float(cost[:non_digit]), cost[non_digit:])


def main():
    exchange = sys.argv[1]
    apply = {"symbols": lambda x: x.split(","), "min_cost": _parse_cost}
    p = parse_argv(sys.argv[2:], apply)
    symbols = p.get("symbols")
    min_cost = p.get("min_cost")
    recalculate = p.contains("recalculate")
    api = uxs.get_exchange({"e": exchange, "args": ({"enableRateLimit": True},)})
    previous_info = read_info(api)
    info = asyncio.get_event_loop().run_until_complete(
        load_info(
            api,
            previous_info,
            min_cost=min_cost,
            symbols=symbols,
            recalculate=recalculate,
        )
    )
    save_info(api, info)


if __name__ == "__main__":
    main()
