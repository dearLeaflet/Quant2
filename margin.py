"""
假设股票的上涨是有惯性或者趋势的，
本类使用拟合方法，拟合近K日的融资余额，期望拟合出一个大致的直线，通过拟合直线的斜率得到上涨或者下跌的趋势
@author godIsGirl
"""

import pandas
from sklearn import linear_model
import tushare_data
import numpy as np
import matplotlib.pyplot as plt


class Margin:
    def __init__(self, ts_code, financing_type):
        """
        传入排序后的融资余额数据
        :param data_frame:
        """
        self._financing_gradient = 0
        self.var = 0
        self.recent_days = 30
        self.financing_date = pandas.DataFrame()
        self.ts_code = ts_code
        self.__model = None
        self.financing_type = financing_type

    def __data_prepare(self):
        pro = tushare_data.get_tushare_pro()
        self.financing_date = pro.margin_detail(ts_code=self.ts_code)

        if not self.__data_check():
            raise Exception("无融资信息")
        self.financing_date = self.financing_date.sort_values(by='trade_date', ascending=False)
        self.financing_date = pandas.DataFrame(self.financing_date.head(self.recent_days)[self.financing_type])
        if not self.__data_check():
            raise Exception("数据处理失败")
        print(self.financing_date.size, self.ts_code)
        self.financing_rolling()

    def financing_rolling(self):
        """
        窗口平滑
        :param financing:
        :return:
        """

        rolling = pandas.DataFrame(self.financing_date[self.financing_type]).rolling(window=1, min_periods=1, center=True)
        self.financing_date = rolling.mean()

    def __data_check(self):
        if self.financing_date is None or self.financing_date.size < self.recent_days:
            return False
        return True

    def linear_model_learning(self):
        self.__data_prepare()
        self.__model = linear_model.LinearRegression(n_jobs=3)
        X = np.arange(1, self.recent_days + 1, 1).reshape(-1, 1)
        # self.financing_date = (self.financing_date.values - self.financing_date.values.min()) * self.recent_days \
        #                       / (self.financing_date.values.max() - self.financing_date.values.min())

        self.__model.fit(X, self.financing_date)
        test.var_clc()
        plt.plot(X, self.financing_date)
        plt.plot(X, self.__model.predict(X))
        plt.title(self.ts_code)
        plt.show()
        self._financing_gradient = self.__model.coef_

    @property
    def financing_gradient(self):
        return self._financing_gradient

    def var_clc(self):
        financing_date_predict = self.__model.predict(self.financing_date)
        rt = financing_date_predict - self.financing_date
        self.var = np.dot(rt.T, rt)/np.power(self.recent_days, 2)
        print('var:', self.var)


pro = tushare_data.get_tushare_pro()
df = pro.index_weight(index_code='399300.SZ')
df = df.sort_values(by='trade_date', ascending=False)
df = df.head(300)
hs300_ts_code = df['con_code']
for ts_code in hs300_ts_code:
    test = Margin(ts_code=ts_code, financing_type='rzye')
    try:
        test.linear_model_learning()
        print(test.financing_gradient)
    except Exception as err:
        print(err)
