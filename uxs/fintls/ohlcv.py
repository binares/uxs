from __future__ import annotations
from typing import List, Union  # Tuple, Set, Dict, Any, Optional, Iterable, Callable

import pandas as pd
import ccxt, ccxt.async_support
import asyncio
from fons.time import dt_round
from dateutil.parser import parse as parsedate
import datetime

dt = datetime.datetime
td = datetime.timedelta

FULL_NAMES = ["timestamp", "Open", "High", "Low", "Close", "Volume"]
FULL_NAMES_LOWERCASE = [x.lower() for x in FULL_NAMES]
SHORT_NAMES = ["timestamp", "O", "H", "L", "C", "V"]
SHORT_NAMES_LOWERCASE = [x.lower() for x in SHORT_NAMES]

SETS = {
    "full": FULL_NAMES,
    "short": SHORT_NAMES,
    "full_lower": FULL_NAMES_LOWERCASE,
    "short_lower": SHORT_NAMES_LOWERCASE,
}


def ohlcvs_as_df(x, full_names=False, lowercase=False):
    """
    :type x: pd.DataFrame, list
    Convert OHLCVs (list) to dataframe format"""
    name = ("full" if full_names else "short") + ("_lower" if lowercase else "")
    columns = SETS[name]

    if isinstance(x, pd.DataFrame):
        if x.columns.tolist() == columns[1:]:
            return x.copy()
        x = replace_names(x, full=full_names, lowercase=lowercase, inplace=False)
        return x.reindex(columns=columns[1:])

    df = pd.DataFrame(x, columns=columns)
    df["timestamp"] = df["timestamp"].astype("datetime64[ms]")
    df.set_index("timestamp", drop=True, inplace=True)

    return df


def ohlcvs_as_list(x):
    """
    :type x: pd.DataFrame, list
    Convert OHLCVs (DataFrame) to list format.
    """
    if isinstance(x, list):
        return x.copy()

    cols = next(
        (
            _columns
            for _columns in SETS.values()
            if any(c in x.columns for c in _columns)
        ),
        SHORT_NAMES,
    )
    x = x.reindex(columns=cols)
    x["timestamp"] = x.index.astype("int64") // 10**6
    x = x.where(pd.notnull(x), None)
    ohlcvs = x.values.tolist()

    return ohlcvs


def replace_names(df, full=True, lowercase=False, *, inplace=False):
    name = ("full" if full else "short") + ("_lower" if lowercase else "")
    map = {}
    for _name, _columns in SETS.items():
        map.update(dict(zip(_columns, SETS[name])))

    return df.rename(map, axis=1, inplace=inplace)


def sn_fetch_ohlcvs_all(
    api: ccxt.async_support.Exchange,
    symbol,
    since,
    until=None,
    timeframe="1d",
    limit=None,
):
    tf_span_seconds = ccxt.Exchange.parse_timeframe(timeframe)
    ohlcvs = []
    while True:
        failed = 0
        for i in range(2):
            try:
                new_ohlcvs = api.fetch_ohlcv(
                    symbol, timeframe=timeframe, since=since, limit=limit
                )
                break
            except Exception as e:
                failed += 1
        if failed == 2:
            print("Could not fetch OHLCV for {}".format(symbol))
            break
        len_ = len(new_ohlcvs)
        if until is not None:
            new_ohlcvs = [x for x in new_ohlcvs if x[0] <= until]
        ohlcvs += new_ohlcvs
        if not len_ or len(new_ohlcvs) < len_:
            break
        last_timestamp = new_ohlcvs[-1][0]
        since = last_timestamp + tf_span_seconds * 1000
    return ohlcvs


def yf_download(
    api: ccxt.Exchange,
    currencies: List[str],
    start: str,
    end: Union[str, None] = None,
    timeframe: str = "1d",
    limit: Union[int, None] = None,
    quote: str = "USDT",
) -> pd.DataFrame:
    "Mock the yahoo finance download function"
    if not currencies:
        raise ValueError("No currencies given")
    since = int(dt.timestamp(parsedate(start))) * 1000
    until = int(dt.timestamp(parsedate(end))) * 1000 if end is not None else None
    # Loading data
    data = {}
    # tasks = {}
    for c in currencies:
        symbol = f"{c}/{quote}" if "/" not in c else c
        data[c] = sn_fetch_ohlcvs_all(api, symbol, since, until, timeframe, limit)
    # await asyncio.gather(tasks.values())
    lengths = [len(x) for x in data.values()]
    longest_ohlcvs = list(data.values())[lengths.index(max(lengths))]
    timestamps = [x[0] for x in longest_ohlcvs]
    data_closes_only = {k: [] for k in data}
    for c in data:
        for t in timestamps:
            data_closes_only[c].append(
                next((x[4] for x in data[c] if x[0] == t), None)
            )  # pick the Close
    pd_timestamps = [pd.Timestamp(x * 1_000_000) for x in timestamps]
    df = pd.DataFrame(data_closes_only, index=pd_timestamps)
    return df
