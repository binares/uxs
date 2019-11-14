TICKER_COLUMNS = ['open','close','high','low','volume','market_cap']
#def rename_columns(df,inplace=False):
RESAMPLE_FUNCS = {
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum',
    'market_cap': 'last'}

def get_column_map(df):
    lrcols = {x.lower().replace(' ','_').replace('-','_'):x for x in df.columns}
    return {x:lrcols[x] for x in TICKER_COLUMNS if x in lrcols}


def _get_resample_map(df):
    col_map = get_column_map(df)
    return {y:RESAMPLE_FUNCS[x] for x,y in col_map.items()}


def resample(df, freq):
    rs = df.resample(freq,closed='left')
    df2 = rs.apply(_get_resample_map(df))
    if df.index[-1] - df2.index.freq + df2.index.freq > df.index[-1]:
        df2 = df2.iloc[:-1].copy()
    return df2
