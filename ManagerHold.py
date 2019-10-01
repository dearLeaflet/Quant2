"""
趋势选股，近一年一直处于上涨，则可以假设还会持续上涨
"""
import time

import tushare
from sklearn import linear_model
import matplotlib.pyplot as plt
import tushare_data
import datetime
import numpy as np

import util


class CloseTrend:
    def __init__(self, ts_code):
        self.ts_code = ts_code
        self.ts_close_gradient = 0
        self.recent_days = 730
        self.__model = linear_model.LinearRegression(n_jobs=3)
        self.X = None
        self.var = 0

    def __get_stock_close(self):
        """
        获取近两年股价
        :param ts_code: 股票代码 .SZ/SH
        :return:
        """
        pro = tushare_data.get_tushare_pro()
        start_date = (datetime.datetime.now() - datetime.timedelta(days=self.recent_days)).strftime('%Y%m%d')
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        df = pro.daily(ts_code=self.ts_code, start_date=start_date, end_date=end_date)
        df = df.sort_values(by='trade_date', ascending=True)
        self.ts_close = (df['close'] - df['close'].min()) / (df['close'].max() - df['close'].min())

    def linear_model_learning(self):
        self.__get_stock_close()
        if self.ts_close.size < 400:
            return
        self.X = np.arange(1, self.ts_close.size + 1, 1).reshape(-1, 1)
        self.__model.fit(self.X, self.ts_close)
        if self.__model.coef_ > 0 and self.var_clc() < 10:
            print(self.ts_code, ':', self.var)
            """
            plt.plot(self.X, self.ts_close)
            plt.plot(self.X, self.__model.predict(self.X))
            plt.title(self.ts_code)
            plt.show()
            plt.close()
            """

    def var_clc(self):
        ts_close_predict = self.__model.predict(self.X)
        rt = ts_close_predict - self.ts_close
        self.var = np.dot(rt.T, rt)
        return self.var


if __name__ == '__main__':
    zz500 = tushare.get_zz500s()
    hs300_index = tushare.get_hs300s()
    zz800_ts_codes = hs300_index.append(zz500)
    ts_codes = zz800_ts_codes['code'].apply(util.stock_code_change)
    x = 1
    for ts_code in ts_codes:
        test = CloseTrend(ts_code=ts_code)
        try:
            test.linear_model_learning()
            time.sleep(1.0)
            print(x)
            x = x + 1
        except Exception as err:
            print(err)