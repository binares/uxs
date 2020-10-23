import pandas as pd

FULL_NAMES = ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
FULL_NAMES_LOWERCASE = [x.lower() for x in FULL_NAMES]
SHORT_NAMES = ['timestamp', 'O', 'H', 'L', 'C', 'V']
SHORT_NAMES_LOWERCASE = [x.lower() for x in SHORT_NAMES]

SETS = {
    'full': FULL_NAMES,
    'short': SHORT_NAMES,
    'full_lower': FULL_NAMES_LOWERCASE,
    'short_lower': SHORT_NAMES_LOWERCASE,
}


def ohlcvs_as_df(x, full_names=False, lowercase=False):
    """
    :type x: pd.DataFrame, list
    Convert OHLCVs (list) to dataframe format"""
    name = ('full' if full_names else 'short') + ('_lower' if lowercase else '')
    columns = SETS[name]
    
    if isinstance(x, pd.DataFrame):
        if x.columns.tolist() == columns[1:]:
            return x.copy()
        x = replace_names(x, full=full_names, lowercase=lowercase, inplace=False)
        return x.reindex(columns=columns[1:])
    
    df = pd.DataFrame(x, columns=columns)
    df['timestamp'] = df['timestamp'].astype('datetime64[ms]')
    df.set_index('timestamp', drop=True, inplace=True)
    
    return df


def ohlcvs_as_list(x):
    """
    :type x: pd.DataFrame, list
    Convert OHLCVs (DataFrame) to list format.
    """
    if isinstance(x, list):
        return x.copy()
    
    cols = next((_columns for _columns in SETS.values() if any(c in x.columns for c in _columns)), SHORT_NAMES)
    x = x.reindex(columns=cols)
    x['timestamp'] = x.index.astype('int64') // 10**6
    x = x.where(pd.notnull(x), None)
    ohlcvs = x.values.tolist()
    
    return ohlcvs


def replace_names(df, full=True, lowercase=False, *, inplace=False):
    name = ('full' if full else 'short') + ('_lower' if lowercase else '')
    map = {}
    for _name, _columns in SETS.items():
        map.update(dict(zip(_columns, SETS[name])))
    
    return df.rename(map, axis=1, inplace=inplace)
