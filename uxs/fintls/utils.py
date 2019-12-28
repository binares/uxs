import ccxt

from fons.time import ctime_ms


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