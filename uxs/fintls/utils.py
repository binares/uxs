import ccxt
import pandas as pd
import datetime
dt = datetime.datetime

from fons.time import (
    ctime_ms, freq_to_offset, dt_round, timestamp_ms, pydt_from_ms
)


def _from_timestamp(timestamp):
    if timestamp is None:
        timestamp = ctime_ms()
    datetime = ccxt.Exchange.iso8601(timestamp)
    
    return datetime, timestamp


def _from_datetime(datetime):
    if datetime is None:
        return _from_timestamp(None)
    else:
        return datetime, ccxt.Exchange.parse8601(datetime)


def resolve_times(data, create=False):
    timestamp = None
    
    if isinstance(data, str):
        datetime = data
    elif isinstance(data, int):
        timestamp = data
    elif not isinstance(data, dict):
        it = iter(data)
        try: datetime = next(it)
        except StopIteration:
            raise ValueError(data)
        try: timestamp = next(it)
        except StopIteration: pass
    else:
        datetime = data.get('datetime')
        timestamp = data.get('timestamp')
        
    if datetime is None and timestamp is None and not create:
        pass
    elif datetime is None:
        datetime, timestamp = _from_timestamp(timestamp)
    elif timestamp is None:
        datetime, timestamp = _from_datetime(datetime)
        
    return datetime, timestamp


def parse_timeframe(timeframe, unit='T'):
    """Unit must be given as pandas frequency"""
    seconds = ccxt.Exchange.parse_timeframe(timeframe)
    return pd.offsets.Second(seconds) / freq_to_offset(unit)


def resolve_ohlcv_times(timeframe='1m', since=None, limit=None, shift=1, as_pydt=False):
    """
    :type since: int, datetime.datetime
    :param shift:
        Used if only `limit` is given
        `1` rounds to the end of tf unit ('1m'->to the end of current minute) [default]
        `0` rounds to its start
    :param as_pydt: if `True` the returned types is datetime.datetime
    
    :returns: [since, end) range in millistamps
    """
    tf_millisecs = parse_timeframe(timeframe, 'ms')
    end = None
    if since is None:
        if limit is not None:
            tf_unit_seconds = ccxt.Exchange.parse_timeframe('1'+timeframe[-1])
            end = timestamp_ms(dt_round(dt.utcnow(), tf_unit_seconds, shift=shift))
            since = int(end - limit*tf_millisecs)
    elif limit is not None:
        if not isinstance(since, (int, float)):
            since = timestamp_ms(since)
        end = int(since + limit*tf_millisecs)
    
    if as_pydt:
        if since is not None and isinstance(since, (int,float)):
            since = pydt_from_ms(since)
        if end is not None and isinstance(end, (int,float)):
            end = pydt_from_ms(end)
    
    return since, end
