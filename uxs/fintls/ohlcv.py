import pandas as pd

FULL_NAMES = ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
SHORT_NAMES = ['timestamp', 'O', 'H', 'L', 'C', 'V']
FULL_NAMES_BY_SHORT = dict(zip(SHORT_NAMES, FULL_NAMES))
SHORT_NAMES_BY_FULL = dict(zip(FULL_NAMES, SHORT_NAMES))


def ohlcvs_as_df(x, full_names=False):
    """
    :type x: pd.DataFrame, list
    Convert OHLCVs (list) to dataframe format"""
    if isinstance(x, pd.DataFrame):
        cols = FULL_NAMES[1:] if any(c in x.columns for c in FULL_NAMES) else SHORT_NAMES[1:]
        is_indexed = x.columns.tolist() == cols
        if not is_indexed:
            x = x.reindex(cols[1:])
        else:
            x = x.copy()
        replace_names(x, full=full_names, inplace=True)
        return x
    
    columns = FULL_NAMES if full_names else SHORT_NAMES
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
    
    cols = FULL_NAMES if any(c in x.columns for c in FULL_NAMES) else SHORT_NAMES
    x = x.reindex(columns=cols)
    x['timestamp'] = x.index.astype('int64') // 10**6
    x = x.where(pd.notnull(x), None)
    ohlcvs = x.values.tolist()
    
    return ohlcvs


def replace_names(df, full=True, inplace=False):
    map = FULL_NAMES_BY_SHORT if full else SHORT_NAMES_BY_FULL
    return df.rename(map, axis=1, inplace=inplace)
