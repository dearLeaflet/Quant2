import pandas as pd
import tushare_data


def kdj(df, n=9):
    """
    随机指标KDJ
    N日RSV=（第N日收盘价-N日内最低价）/（N日内最高价-N日内最低价）×100%
    当日K值=2/3前1日K值+1/3×当日RSV=SMA（RSV,M1）
    当日D值=2/3前1日D值+1/3×当日K= SMA（K,M2）
    当日J值=3 ×当日K值-2×当日D值
    """
    _kdj = pd.DataFrame()
    _kdj['date'] = df['trade_date']
    rsv = (df.close - df.low.rolling(n).min()) / (df.high.rolling(n).max() - df.low.rolling(n).min()) * 100
    _kdj['k'] = sma(rsv, 3)
    _kdj['d'] = sma(_kdj.k, 3)
    _kdj['j'] = 3 * _kdj.k - 2 * _kdj.d
    return _kdj[_kdj['date'] > '20180630']


def sma(a, n, m=1):
    """
    平滑移动指标 Smooth Moving Average
    """
    a = a.fillna(0)
    b = a.ewm(min_periods=0, ignore_na=False, adjust=False, alpha=m/n).mean()
    return b


def hs300_kdj():
    """
    返回沪深300KDJ指标
    :param df:
    :return:
    """
    hs300_index = hs300_daily_info = tushare_data.get_index_daily('000300.SH')
    hs300_index = hs300_daily_info[hs300_index['trade_date'] > '20180615']
    hs300_index = hs300_index.sort_values(by=['trade_date'], ascending=True)
    return kdj(df=hs300_index)
