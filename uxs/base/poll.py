import os

# from dateutil.parser import parse as parsedate
from collections import namedtuple, deque
import itertools as it
import json
import yaml
import asyncio
from copy import deepcopy
import ccxt.async_support
import ccxt
import time
import datetime

dt = datetime.datetime
td = datetime.timedelta

from .ccxt import get_exchange, get_name
from ._settings import get_setting, get_cache_dir
from .types import fnInf, prInf

from fons.dict_ops import deep_get
from fons.io import DateTimeEncoder, SafeFileLock, wait_filelock
from fons.os import make_dirpath, delete_empty_dirs
from fons.time import (
    dt_strp,
    dt_round_to_digit,
    freq_to_offset,
    freq_to_td,
    pydt_from_ms,
    timestamp_ms,
)
import fons.log

logger, logger2, tlogger, tloggers, tlogger0 = fons.log.get_standard_5(__name__)


_PRIVATE_METHODS = (
    "balances",
    "balances-account",
)

_METHODS = {
    "markets": ("load_markets", tuple(), {"reload": True}, []),
    "currencies": ("fetch_currencies", tuple(), {}, []),
    "balances": ("fetch_balance", tuple(), {}, []),
    "balances-account": ("fetch_balance", ({"type": "account"},), {}, []),
    "tickers": ("fetch_tickers", tuple(), {}, []),
    "ticker": ("fetch_ticker", tuple(), {}, ["symbol"]),
    "orderbook": ("fetch_order_book", tuple(), {}, ["symbol"]),
    "trades": ("fetch_trades", tuple(), {}, ["symbol"]),
    "ohlcv": ("fetch_ohlcv", tuple(), {}, ["symbol", "timeframe"]),
}

_BLOCK = {
    "markets": 2,
    "currencies": 2,
    "balances": 2,
    "balances-account": 2,
    "tickers": 2,
    "ticker": 2,
    "orderbook": 2,
    "trades": 2,
    "ohlcv": 2,
}

_MAXLENS = {
    "markets": 5,
    "currencies": 5,
    "balances": 2,
    "balances-account": 2,
    "tickers": 5,
    "ticker": 2,
    "orderbook": 2,
    "trades": 2,
    "ohlcv": 2,
}

_LOADS_MARKETS = (
    "balances",
    "balances-account",
    "tickers",
    "ticker",
    "orderbook",
    "trades",
    "ohlcv",
)

ITERATION_SLEEP = 0.02
ASYNC_ITERATION_SLEEP = 0.02

storage = {}


def _assign_storage_deque(exchange, type, *args):
    t_type = _type_tuple(type)
    value_given = None
    try:
        value = value_given = args[0]
    except IndexError:
        value = deque(maxlen=_MAXLENS[t_type[0]])
    full_id = list(it.chain([exchange], t_type))
    len_full_id = len(full_id)
    d = storage
    for i, k in enumerate(full_id):
        if i < len_full_id - 1:
            if k not in d:
                d[k] = {}
            d = d[k]
        elif k in d and value_given is None:
            pass
        elif value is not None:
            d[k] = value


def _get_storage_deque(exchange, type):
    _assign_storage_deque(exchange, type)
    t_type = _type_tuple(type)
    d = storage[exchange]
    for k in t_type:
        d = d[k]
    deq = d
    return deq


def _exchange_and_type_to_str(exchange, type):
    if not isinstance(type, str):
        type = tuple(type)[0]

    exchange0 = exchange
    if not isinstance(exchange, str):
        exchange0 = get_exchange(exchange)
        exchange = get_name(exchange0)

    return exchange, type


def _get_appropriate_api(exchange, type, _async=True, verbose=False):
    if isinstance(exchange, ccxt.Exchange) and exchange._auth_info.get("info"):
        is_async = isinstance(exchange, ccxt.async_support.Exchange)
        if is_async == bool(_async):
            return exchange
    exchange0 = exchange
    exchange, _type = _exchange_and_type_to_str(exchange, type)
    if _type in _PRIVATE_METHODS:
        api = get_exchange({"xc": exchange0, "async": _async, "id": "INFO"})
    else:
        config = {"exchange": exchange, "async": _async, "info": False, "trade": False}
        if verbose:
            logger.debug(
                "Trying to init ccxt-exchange with lowest auth: {}".format(config)
            )
        api = get_exchange(config)
    return api


def _fetch_exchange_specific(exchange, type, param, param_xc, **kw):
    exchange, type = _exchange_and_type_to_str(exchange, type)

    enabled = get_setting(param)
    enabled_for_xcs = get_setting(param_xc)
    xc_specific = enabled_for_xcs.get(exchange) if enabled_for_xcs is not None else {}

    search = [xc_specific, enabled]

    return deep_get(search, type, **kw)


def is_caching_enabled(exchange, type):
    # By default (if value is not speciefied or == None), the caching is not enabled
    return _fetch_exchange_specific(
        exchange, type, "enable_caching", "enable_caching_for_exchanges", return2=False
    )


def get_cache_expiry(exchange, type):
    return _fetch_exchange_specific(
        exchange, type, "cache_expiry", "cache_expiry_for_exchanges", return2=0
    )


async def load_markets(api, limit=None):
    exchange = get_name(api)
    limit = _resolve_limit(exchange, "markets", limit)
    markets_initial = api.markets
    currencies_initial = api.currencies
    api.markets = {}
    api.currencies = {}
    markets = currencies = None
    try:
        # this will call load_markets, which will set both markets and currencies
        m0 = (await fetch(api, "markets", limit, strip=False))[0]
        # unless it loaded from cache, in which case they will have to be set manually
        if not api.markets:
            l1 = limit
            l2 = _resolve_limit(exchange, "markets", m0.date)
            limit = -1
            if -1 not in (l1, l2):
                limit = (
                    min(l1, l2)
                    if None not in (l1, l2)
                    else next((x for x in (l1, l2) if x is not None), None)
                )
            markets = m0.data

            if not api.currencies:
                currencies = load(exchange, "currencies", limit, 1)[0].data
    except Exception as e:
        logger.exception(e)
    finally:
        if markets:
            tlogger.debug("{} - setting markets".format(exchange))
            try:
                api.set_markets(markets, currencies)
            except Exception as e2:
                logger.exception(e2)
        if not api.markets:
            api.markets = markets_initial
            api.currencies = currencies_initial

    return api.markets


def sn_load_markets(api, limit=None):
    exchange = get_name(api)
    limit = _resolve_limit(exchange, "markets", limit)
    markets_initial = api.markets
    currencies_initial = api.currencies
    api.markets = {}
    api.currencies = {}
    markets = currencies = None
    try:
        # this will call load_markets, which will set both markets and currencies
        m0 = sn_fetch(api, "markets", limit, strip=False)[0]
        # print("m0", m0)
        # unless it loaded from cache, in which case they will have to be set manually
        if not api.markets:
            l1 = limit
            l2 = _resolve_limit(exchange, "markets", m0.date)
            limit = -1
            if -1 not in (l1, l2):
                limit = (
                    min(l1, l2)
                    if None not in (l1, l2)
                    else next((x for x in (l1, l2) if x is not None), None)
                )
            markets = m0.data

            if not api.currencies:
                currencies = load(exchange, "currencies", limit, 1)[0].data
    except Exception as e:
        logger.exception(e)
    finally:
        if markets:
            tlogger.debug("{} - setting markets".format(exchange))
            try:
                api.set_markets(markets, currencies)
            except Exception as e2:
                logger.exception(e2)
        if not api.markets:
            api.markets = markets_initial
            api.currencies = currencies_initial

    return api.markets


async def get(
    exchange,
    type,
    limit=None,
    max=1,
    *,
    file=True,
    globals=True,
    empty_update=True,
    blocked="sleep",
    args=None,
    kwargs=None,
    loop=None,
    cache=True,
    attempts=2,
    raise_e=False
):
    """
    Tries to retrieve the latest data, by either
      A) loading unexpired cache
      B) ccxt api fetch if no unexpired cache was found
    :param max: how many unexpired cache entries (of different time) are being loaded
    :param blocked: what to do if in this very moment ccxt api fetch
                    with the exact same (exchange, type) values is already being performed
                    'sleep','ignore' or 'return'
                    ('return' returns empty list)
    :param cache: if B was performed (update()), whether or not to cache the new data
    For other params see fetch() docstring
    """
    exchange0 = exchange
    exchange, _ = _exchange_and_type_to_str(exchange, type)
    type = _resolve_type(type)
    limit = _resolve_limit(exchange, type, limit)

    if blocked is None:
        blocked = "sleep"
    elif blocked not in ("sleep", "ignore", "return"):
        raise ValueError(blocked)

    if blocked != "ignore":
        wait_for = _is_blocked(exchange, type)
        if not wait_for:
            pass
        elif blocked == "sleep":
            await _async_wait_till_released(exchange, type, wait_for, loop=loop)
        else:
            return []

    items = []

    if file:
        items = load(exchange, type, limit, max, globals=globals)
    elif globals:
        items = retrieve(exchange, type, limit, max)

    if not items and empty_update:
        items = await update(
            exchange0,
            type,
            args,
            kwargs,
            loop=loop,
            file=file,
            globals=globals,
            blocked=blocked,
            cache=cache,
            attempts=attempts,
            raise_e=raise_e,
        )

    return items


def sn_get(
    exchange,
    type,
    limit=None,
    max=1,
    *,
    file=True,
    globals=True,
    empty_update=True,
    blocked="sleep",
    args=None,
    kwargs=None,
    loop=None,
    cache=True,
    attempts=2,
    raise_e=False
):
    """
    Tries to retrieve the latest data, by either
      A) loading unexpired cache
      B) ccxt api fetch if no unexpired cache was found
    :param max: how many unexpired cache entries (of different time) are being loaded
    :param blocked: what to do if in this very moment ccxt api fetch
                    with the exact same (exchange, type) values is already being performed
                    'sleep','ignore' or 'return'
                    ('return' returns empty list)
    :param cache: if B was performed (update()), whether or not to cache the new data
    For other params see fetch() docstring
    """
    exchange0 = exchange
    exchange, _ = _exchange_and_type_to_str(exchange, type)
    type = _resolve_type(type)
    limit = _resolve_limit(exchange, type, limit)

    if blocked is None:
        blocked = "sleep"
    elif blocked not in ("sleep", "ignore", "return"):
        raise ValueError(blocked)

    if blocked != "ignore":
        wait_for = _is_blocked(exchange, type)
        if not wait_for:
            pass
        elif blocked == "sleep":
            _wait_till_released(exchange, type, wait_for)
        else:
            return []

    items = []

    if file:
        items = load(exchange, type, limit, max, globals=globals)
    elif globals:
        items = retrieve(exchange, type, limit, max)

    if not items and empty_update:
        items = sn_update(
            exchange0,
            type,
            args,
            kwargs,
            loop=loop,
            file=file,
            globals=globals,
            blocked=blocked,
            cache=cache,
            attempts=attempts,
            raise_e=raise_e,
        )

    return items


async def fetch(
    exchange,
    type,
    limit=None,
    *,
    file=True,
    globals=True,
    empty_update=True,
    args=None,
    kwargs=None,
    loop=None,
    strip=True,
    attempts=1
):
    """
    Checks whether reading from storage if enabled for the method of the exchange,
    if true proceeds with get(...), otherwise update()
    :param exchange: str or api (ccxtWrapper instance)
    :param type: str or (str, specification)
            Examples:
                'tickers'
                ('ticker', 'ETH/BTC')
                ('orderbook', 'BTC/USD')
    :param args: args passed to ccxt api fetch
    :param kwargs: kwargs passed to ccxt api fetch
    :param attempts: retries for ccxt api fetch, should an error occur

    Only applies if caching is enabled:
    :type limit: dt or timedelta-like (timedelta, seconds, freqstr)
    :param limit: max age of the cached data. If None then default cache_expiry
                of exchange and type is used. -1 (< 0) for no age limit.
    :param file: allow reading from cache files (unexpired), if cache is not enabled, this is ignored
    :param globals: allow retrieving from global cache (unexpired)
    :param empty_update: if no unexpired cache was found, force fetch new data (update())
    """
    type = _resolve_type(type)

    # By default no caching is enabled (not for markets, tickers, ...), so no cache is retrieved nor saved
    if not is_caching_enabled(exchange, type):
        l = await update(
            exchange,
            type,
            args,
            kwargs,
            loop=loop,
            file=False,
            globals=False,
            cache=False,
            blocked="ignore",
            attempts=attempts,
            raise_e=True,
        )
    else:
        l = await get(
            exchange,
            type,
            limit=limit,
            max=1,
            file=file,
            globals=globals,
            empty_update=empty_update,
            blocked="sleep",
            args=args,
            kwargs=kwargs,
            loop=loop,
            attempts=attempts,
            raise_e=True,
        )

    if strip:
        return l[0].data
    else:
        return l


def sn_fetch(
    exchange,
    type,
    limit=None,
    *,
    file=True,
    globals=True,
    empty_update=True,
    args=None,
    kwargs=None,
    loop=None,
    strip=True,
    attempts=1
):
    """
    Checks whether reading from storage if enabled for the method of the exchange,
    if true proceeds with get(...), otherwise update()
    :param exchange: str or api (ccxtWrapper instance)
    :param type: str or (str, specification)
            Examples:
                'tickers'
                ('ticker', 'ETH/BTC')
                ('orderbook', 'BTC/USD')
    :param args: args passed to ccxt api fetch
    :param kwargs: kwargs passed to ccxt api fetch
    :param attempts: retries for ccxt api fetch, should an error occur

    Only applies if caching is enabled:
    :type limit: dt or timedelta-like (timedelta, seconds, freqstr)
    :param limit: max age of the cached data. If None then default cache_expiry
                of exchange and type is used. -1 (< 0) for no age limit.
    :param file: allow reading from cache files (unexpired); if cache is not enabled, this is ignored
    :param globals: allow retrieving from global cache (unexpired)
    :param empty_update: if no unexpired cache was found, force fetch new data (update())
    """
    type = _resolve_type(type)

    # By default no caching is enabled (not for markets, tickers, ...), so no cache is retrieved nor saved
    if not is_caching_enabled(exchange, type):
        l = sn_update(
            exchange,
            type,
            args,
            kwargs,
            loop=loop,
            file=False,
            globals=False,
            cache=False,
            blocked="ignore",
            attempts=attempts,
            raise_e=True,
        )
    else:
        l = sn_get(
            exchange,
            type,
            limit=limit,
            max=1,
            file=file,
            globals=globals,
            empty_update=empty_update,
            blocked="sleep",
            args=args,
            kwargs=kwargs,
            loop=loop,
            attempts=attempts,
            raise_e=True,
        )

    if strip:
        return l[0].data
    else:
        return l


async def update(
    exchange,
    type,
    args=None,
    kwargs=None,
    *,
    file=True,
    globals=True,
    loop=None,
    limit=None,
    cache=True,
    blocked="sleep",
    attempts=2,
    raise_e=False,
    verbose=False
):
    """
    Tries to retrieve the latest data, by either
      A) [IF (exchange, type) is being blocked by parallel update(),
          and blocked is not set to 'ignore']
         loading unexpired cache
      B) ccxt api fetch if no unexpired cache was found
    Param explanations can be found in fetch() and get() docstrings
    """
    exchange0 = exchange
    exchange, _ = _exchange_and_type_to_str(exchange, type)
    type = _resolve_type(type)
    limit = _resolve_limit(exchange, type, limit)

    if blocked is None:
        blocked = "sleep"
    elif blocked not in ("sleep", "ignore", "return"):
        raise ValueError(blocked)

    if args is None:
        args = tuple()
    if kwargs is None:
        kwargs = {}

    api = _get_appropriate_api(exchange0, type, True, verbose)

    if blocked != "ignore":
        wait_for = _is_blocked(exchange, type)
        if not wait_for:
            pass
        elif blocked == "sleep":
            await _async_wait_till_released(exchange, type, wait_for, loop=loop)
            if file:
                return load(exchange, type, limit, 1, globals=globals)
            elif globals:
                return retrieve(exchange, type, limit, 1)
            # else:
            #   return []
        else:
            return []

    inf = []

    type0 = _type0(type)
    method_str, args2, kwargs2, kw_ids = _METHODS[type0]
    # for symbol, the type must be in format (name,symbol)
    # and symbol must not be included in args/kwargs
    # kwargs2 = dict(kwargs2, **{x:type[i+1] for i,x in enumerate(kw_ids)})
    method = getattr(api, method_str)
    # print(api._custom_name,'method:',method,'session:',api.session)
    args = tuple(args) + args2[len(args) :]
    if kw_ids:
        args = tuple(type[i + 1] for i, x in enumerate(kw_ids)) + args
    kwargs = dict(kwargs2, **kwargs)
    is_market = type == "markets"

    if type0 in _LOADS_MARKETS and not api.markets:
        await load_markets(api)

    exc, i = None, 0
    while i < attempts:
        # Only block if we later cache the results
        # (we don't want parallel update() -s to wait for nothing)
        if cache:
            _block(exchange, type, _BLOCK[type0])
        try:
            data = await method(*args, **kwargs)
            # await api.close()
            now = dt_round_to_digit(dt.utcnow(), 6)
            inf.append(create_new(exchange, type, now, data=data))
            if is_market:
                inf.append(create_new(exchange, "currencies", now, data=api.currencies))
        except Exception as e:
            exc = e
            if isinstance(e, ccxt.NotSupported):
                i = attempts - 1
            elif isinstance(e, KeyError) and type == "tickers" and not i:
                logger.debug(
                    "{} - fetch_tickers caused KeyError. Re-loading markets.".format(
                        exchange
                    )
                )
                await load_markets(api)
            if i == attempts - 1:
                logger2.error("{} - error fetching {}: {}".format(exchange, type, e))
                logger.exception(e)
        else:
            break
        i += 1

    if exc is not None and i >= attempts - 1 and raise_e:
        if cache:
            _release(exchange, [type])
        raise exc

    if cache:
        globalise(inf)
        save(inf)
        _release(exchange, [type])

    if is_market and inf:
        inf = inf[:1]

    return inf


def sn_update(
    exchange,
    type,
    args=None,
    kwargs=None,
    *,
    file=True,
    globals=True,
    loop=None,
    limit=None,
    cache=True,
    blocked="sleep",
    attempts=2,
    raise_e=False,
    verbose=False
):
    """
    Tries to retrieve the latest data, by either
      A) [IF (exchange, type) is being blocked by parallel update(),
          and blocked is not set to 'ignore']
         loading unexpired cache
      B) ccxt api fetch if no unexpired cache was found
    Param explanations can be found in fetch() and get() docstrings
    """
    exchange0 = exchange
    exchange, _ = _exchange_and_type_to_str(exchange, type)
    type = _resolve_type(type)
    limit = _resolve_limit(exchange, type, limit)

    if blocked is None:
        blocked = "sleep"
    elif blocked not in ("sleep", "ignore", "return"):
        raise ValueError(blocked)

    if args is None:
        args = tuple()
    if kwargs is None:
        kwargs = {}

    api = _get_appropriate_api(exchange0, type, False, verbose)

    if blocked != "ignore":
        wait_for = _is_blocked(exchange, type)
        if not wait_for:
            pass
        elif blocked == "sleep":
            _wait_till_released(exchange, type, wait_for)
            if file:
                return load(exchange, type, limit, 1, globals=globals)
            elif globals:
                return retrieve(exchange, type, limit, 1)
            # else:
            #   return []
        else:
            return []

    inf = []

    type0 = _type0(type)
    method_str, args2, kwargs2, kw_ids = _METHODS[type0]
    # for symbol, the type must be in format (name,symbol)
    # and symbol must not be included in args/kwargs
    # kwargs2 = dict(kwargs2, **{x:type[i+1] for i,x in enumerate(kw_ids)})
    method = getattr(api, method_str)
    # print(api._custom_name,'method:',method,'session:',api.session)
    args = tuple(args) + args2[len(args) :]
    if kw_ids:
        args = tuple(type[i + 1] for i, x in enumerate(kw_ids)) + args
    kwargs = dict(kwargs2, **kwargs)
    is_market = type == "markets"

    if type0 in _LOADS_MARKETS and not api.markets:
        sn_load_markets(api)

    exc, i = None, 0
    while i < attempts:
        # Only block if we later cache the results
        # (we don't want parallel update() -s to wait for nothing)
        if cache:
            _block(exchange, type, _BLOCK[type0])
        try:
            data = method(*args, **kwargs)
            # await api.close()
            now = dt_round_to_digit(dt.utcnow(), 6)
            inf.append(create_new(exchange, type, now, data=data))
            if is_market:
                inf.append(create_new(exchange, "currencies", now, data=api.currencies))
        except Exception as e:
            exc = e
            if isinstance(e, ccxt.NotSupported):
                i = attempts - 1
            elif isinstance(e, KeyError) and type == "tickers" and not i:
                logger.debug(
                    "{} - fetch_tickers caused KeyError. Re-loading markets.".format(
                        exchange
                    )
                )
                sn_load_markets(api)
            if i == attempts - 1:
                logger2.error("{} - error fetching {}: {}".format(exchange, type, e))
                logger.exception(e)
        else:
            break
        i += 1

    if exc is not None and i >= attempts - 1 and raise_e:
        if cache:
            _release(exchange, [type])
        raise exc

    if cache:
        globalise(inf)
        save(inf)
        _release(exchange, [type])

    if is_market and inf:
        inf = inf[:1]

    return inf


def globalise(items):
    for item in sorted(items, key=lambda x: x.date):
        seq = _get_storage_deque(item.exchange, item.type)
        pos = next((i for i, x in enumerate(seq) if item.date >= x.date), None)

        if pos is None:
            if len(seq):
                continue
            else:
                pos = 0

        item = deepcopy(item)

        try:
            if seq[pos].date == item.date:
                seq[pos] = item
                continue
        except IndexError:
            pass

        if pos != 0:
            maxlen = _MAXLENS[_type0(item.type)]
            new_l = (list(seq[:pos]) + [item] + list(seq[pos:]))[:maxlen]
            _assign_storage_deque(item.exchange, item.type, deque(new_l, maxlen=maxlen))
        else:
            seq.appendleft(item)


def save(items):
    for item in sorted(items, key=lambda x: x.date):
        fn = (
            item.file
            if item.file
            else encode_filename(item.exchange, item.type, item.date)
        )

        _dir = os.path.join(get_cache_dir(), item.exchange)
        if not os.path.exists(_dir):
            make_dirpath(_dir)

        path = os.path.join(_dir, fn)

        with SafeFileLock(path, 0.01):
            with open(path, "w", encoding="utf-8") as f:
                json.dump(item.data, f, cls=DateTimeEncoder)

        p = probe(item.exchange, item.type, -1, globals=False)
        maxlen = _MAXLENS[_type0(item.type)]
        with_file = (x for x in reversed(p[maxlen:]) if x.file)
        for wf in with_file:
            try:
                os.remove(os.path.join(_dir, wf.file))
            except OSError:
                pass


def retrieve(exchange, type, limit=None, max=5):
    limit = _resolve_limit(exchange, type, limit)
    d = storage
    for key in it.chain([exchange], _type_tuple(type)):
        try:
            d = d[key]
        except KeyError:
            return []
    items = d

    if limit != -1:
        items = [x for x in items if x.date >= limit]
    else:
        items = list(items)

    if max is not None:
        items = items[:max]

    items = deepcopy(items)

    return items


def retrieve_latest(exchange, type, limit=None):
    return retrieve(exchange, type, limit, 1)


def load(exchange, type, limit=None, max=5, globals=True):
    limit = _resolve_limit(exchange, type, limit)
    inf = probe(exchange, type, limit, max, globals=globals)
    items = []
    for tpl in inf:
        if tpl.data is not None:
            items.append(tpl)
            continue
        path = os.path.join(get_cache_dir(), exchange, tpl.file)
        wait_filelock(path)
        logger.debug("Reading: {}".format(path))
        with open(path, encoding="utf-8") as f:
            item = fnInf(*tpl[:-1], json.load(f))
        globalise([item])
        items.append(item)

    return items


def load_latest(exchange, type, limit=None, globals=True):
    return load(exchange, type, limit, 1, globals)


def probe(exchange, type, limit=None, max=None, globals=True):
    limit = _resolve_limit(exchange, type, limit)
    type_str = _type_str(type)
    begins = "[{}]_{}_".format(exchange.lower(), type_str)
    ends = ".json"
    _len = len(begins)
    _len_ends = len(ends)

    _dir = os.path.join(get_cache_dir(), exchange)
    try:
        files = (
            x
            for x in reversed(os.listdir(_dir))
            if x[:_len] == begins and x[_len:].isdigit()
        )
    except FileNotFoundError:
        files = []

    _decoded = (decode_filename(x) for x in files)
    decoded = (
        list(x for x in _decoded if x.date >= limit) if limit != -1 else list(_decoded)
    )

    if globals:
        items = retrieve(exchange, type, limit, max)
        items += [x for x in decoded if not any(x.date == y.date for y in items)]
        decoded = items

    decoded.sort(key=lambda x: x.date, reverse=True)
    if max is not None:
        decoded = decoded[:max]

    return decoded


def probe_latest(exchange, type, limit=None, globals=True):
    return probe(exchange, type, limit, 1, globals)


#########################################################


def encode_filename(exchange, type, date):
    type_str = _type_str(type)
    return "[{}]_{}_{}".format(exchange.lower(), type_str, timestamp_ms(date))


def decode_filename(fn):
    e0, e1 = fn.find("["), fn.find("]")
    e = fn[e0 + 1 : e1]
    split = fn[e1 + 2 :].replace(";", "/").split("_")
    type = tuple(split[:-1])
    datestr = split[-1]
    # date = parsedate(datestr,'%Y-%m-%dT%H-%M-%S-%f')
    date = pydt_from_ms(int(datestr))
    if len(type) < 2:
        type = type[0]
    return fnInf(e, type, date, fn)


def _get_blocks(exchange, type=None):
    _dir = make_dirpath(get_cache_dir(), exchange, "__block__")
    blocks = []
    for f in os.listdir(_dir):
        if not f.startswith("__"):
            continue
        try:
            blocks.append(_decode_block(f))
        except ValueError:
            continue
    if type is not None:
        type = _resolve_type(type)
        blocks = [x for x in blocks if x.type == type]
    blocks.sort(key=lambda x: x.date, reverse=True)
    return blocks


def _encode_block(exchange, type, until):
    return "__{}".format(encode_filename(exchange, type, until))


def _decode_block(fn):
    return decode_filename(fn)


def create_new(exchange, type, date=None, file=None, data=None):
    if date is None:
        date = dt_round_to_digit(dt.utcnow(), 6)

    if file is None:
        file = encode_filename(exchange, type, date)

    return fnInf(exchange, type, date, file, data)


def _is_blocked(exchange, type):
    blocks = _get_blocks(exchange, type)

    if len(blocks):
        now = dt.utcnow()
        remaining = (blocks[0].date - now).total_seconds()
        return max(0, remaining)

    return 0


def _block(exchange, type, until):
    if not isinstance(until, dt):
        until = dt.utcnow() + freq_to_td(until)
    # round to millisecond
    until = dt_round_to_digit(until, 6)

    _dir = os.path.join(get_cache_dir(), exchange, "__block__")
    fn = _encode_block(exchange, type, until)
    blocks = _get_blocks(exchange, type)
    # print('{} {} blocks: {}'.format(exchange,type,blocks))

    with open(os.path.join(_dir, fn), "w"):
        pass

    for b in (x for x in blocks if x.date < until):
        try:
            os.remove(os.path.join(_dir, b.file))
        except OSError:
            pass


def _release(exchange, types):
    if isinstance(types, str):
        types = (types,)

    types = [_resolve_type(t) for t in types]
    _dir = os.path.join(get_cache_dir(), exchange, "__block__")
    blocks = _get_blocks(exchange)
    blocks = [x for x in blocks if x.type in types]

    for b in blocks:
        try:
            os.remove(os.path.join(_dir, b.file))
        except OSError:
            pass


def _wait_till_released(exchange, type, timeout=None):
    iterations = max(0, int(timeout / ITERATION_SLEEP)) if timeout is not None else None
    i = 0
    while (iterations is None or i < iterations) and _is_blocked(exchange, type):
        time.sleep(ITERATION_SLEEP)
        i += 1


async def _async_wait_till_released(exchange, type, timeout=None, *, loop=None):
    iterations = (
        max(0, int(timeout / ASYNC_ITERATION_SLEEP)) if timeout is not None else None
    )
    i = 0
    while (iterations is None or i < iterations) and _is_blocked(exchange, type):
        await asyncio.sleep(ASYNC_ITERATION_SLEEP, loop=loop)
        i += 1


def _resolve_limit(exchange, type, limit):
    if limit is None:
        limit = get_cache_expiry(exchange, type)
    if isinstance(limit, (float, int)) and limit < 0:
        return -1
    elif not isinstance(limit, dt):
        return dt.utcnow() - freq_to_offset(limit)
    else:
        return limit


def _resolve_type(type):
    if not isinstance(type, str):
        # iter to tuple
        type = tuple(type)
        if len(type) < 2:
            type = type[0]
    type0 = _type0(type)
    if (
        type0 not in _METHODS
        or isinstance(type, str)
        and len(_METHODS[type0][3])
        or not isinstance(type, str)
        and len(_METHODS[type0][3]) != len(type) - 1
    ):
        raise ValueError("Incorrect type: {}".format(type))
    return type


def _type_str(type):
    return "_".join(_type_tuple(type)).replace("/", ";")


def _type0(type):
    return type if isinstance(type, str) else type[0]


def _type_tuple(type):
    return (type,) if isinstance(type, str) else tuple(type)


def clear_cache(exchanges=None, types=None):
    """
    If neither exchanges nor types are specified, all cache is deleted
    """
    _dir = get_cache_dir()
    if isinstance(exchanges, str):
        exchanges = [exchanges]
    if isinstance(types, str):
        types = [types]

    xc_dirs = {
        f: os.path.join(_dir, f)
        for f in os.listdir(_dir)
        if os.path.isdir(os.path.join(_dir, f))
    }

    if exchanges is not None:
        xc_dirs = {xc: pth for xc, pth in xc_dirs.items() if xc in exchanges}

    if types is None:
        types = list(_METHODS)

    for xc, xc_dir in xc_dirs.items():
        xc_contents = os.listdir(xc_dir)
        pths = [
            os.path.join(xc_dir, f)
            for f in xc_contents
            if any(f.startswith("[{}]_{}".format(xc, method)) for method in types)
        ]

        block_dir = os.path.join(xc_dir, "__block__")

        if os.path.isdir(block_dir):
            pths += [
                os.path.join(block_dir, f)
                for f in os.listdir(block_dir)
                if any(f.startswith("__[{}]_{}".format(xc, method)) for method in types)
            ]

        for pth in pths:
            try:
                os.remove(pth)
            except OSError:
                pass

    delete_empty_dirs(_dir)


def load_profile(profile, exchange, type="markets", *, verbose=False):
    now = dt.utcnow()
    profiles_dir = get_setting("profiles_dir")

    if profiles_dir is None:
        return []

    dir = os.path.join(profiles_dir, profile)

    if not os.path.isdir(dir):
        return []

    files = os.listdir(dir)
    startsw = "{}_{}".format(exchange, type)
    matching = [x for x in files if x.startswith(startsw)]
    dates = {}

    for fn in matching:
        ending = fn[len(startsw) :]
        is_valid = True
        start = end = None
        exc = None
        if ending.endswith(".yaml") or ending.endswith(".yml"):
            ending = ".".join(ending.split(".")[:-1])
        if ending in ("", "_"):
            pass
        elif not ending.startswith("_"):
            is_valid = False
        else:
            split = ending[1:].split("to")
            _len = len(split)
            start = end = None
            try:
                if _len > 2:
                    is_valid = False
                else:
                    if split[0]:
                        start = dt_strp(split[0])  # parsedate(split[0])
                    if _len == 2 and split[1]:
                        end = dt_strp(split[1])  # parsedate(split[1])
            except Exception as e:
                exc = e
                is_valid = False

        if not is_valid:
            logger2.error("Incorrect filename: {}".format(os.path.join(dir, fn)))
        else:
            dates[fn] = {"start": start, "end": end}
        if exc is not None:
            logger2.exception(exc)

    unexpired = [
        x
        for x in matching
        if x in dates
        and (dates[x]["start"] is None or dates[x]["start"] < now)
        and (dates[x]["end"] is None or now < dates[x]["end"])
    ]
    if verbose:
        logger.debug(
            "Acquired the following profile {} unexpired files: {}".format(
                (profile, exchange, type), unexpired
            )
        )

    unexpired_items = []
    for fn in unexpired:
        path = os.path.join(dir, fn)
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            t = prInf(
                profile, exchange, type, dates[fn]["start"], dates[fn]["end"], fn, data
            )
            unexpired_items.append(t)
        except OSError as e:
            logger2.error('Could not read file "{}"'.format(path))
            logger2.exception(e)

    return unexpired_items
