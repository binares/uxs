import pandas as pd
#import pandas.io.data as web
from stockstats import StockDataFrame as Sdf
import datetime
td = datetime.timedelta

from ..misc import resample

def calc_rsi(data,closes,freq='D',return_df=False):
    is_single = not isinstance(closes,(list,tuple))
    if is_single: closes = [closes]
    df = data.copy()
    if freq not in ('D','1D'):
        df = resample(df,freq)
        ofs = df.index.freq
    else:ofs = pd.offsets.Day()

    for close in closes:
        ld = df.index[-1]
        lr = df.iloc[-1:].copy()
        lr.index = [ld+ofs]
        open_new = lr.ix[0,'Close']
        lr.ix[0,'Open'] = open_new
        lr.ix[0,'Close'] = close
        rows = ['Open','Low','High','Close']
        lr.ix[0,'High'] = max(open_new,close)#lr.ix[0,rows].max()
        lr.ix[0,'Low'] = min(open_new,close)#lr.ix[0,rows].min()
        df = pd.concat([df,lr])
    stock_df = Sdf.retype(df)
    df['rsi'] = stock_df['rsi_14']
    for row in ['close_-1_s','close_-1_d', 'rs_14', 'rsi_14']:
        del df[row]
    del df['market cap']
    del df['volume']
    rsi = df.ix[-1,'rsi'] if is_single else df.ix[-len(closes):,'rsi'].tolist()
    return (rsi,df) if return_df else rsi