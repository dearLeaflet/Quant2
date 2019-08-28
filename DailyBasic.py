import tushare as ts
import datetime


class DailyBasic:
    """
    获取沪深300成分股的每日PE、PB、PS值，并计算在过去某段时间类三个值百分位值
    筛选出三个值都位于30%一下的股票，并将统计结果以邮件的方式通知到自己
    """
    def __init__(self):
        pro = ts.pro_api()
        start_date = datetime.datetime.now().strftime('%Y%m%d')
        df = pro.index_weight(index_code='399300.SZ', start_date=start_date)
        self.hs300_ts_code = df['index_code']
        self.selected_ts = []

    def __get_pe_rank(self):
        """
        :return:PE当前百分位
        """
        pass

    def __get_pb_rank(self):
        """
        :return: PB当前百分位
        """
        pass

    def __get_ps_rank(self):
        """
        :return: P当前百分位
        """
        pass

    def __check_ts(self):
        """
        判断三个指标是否都符合
        :return:
        """
        pass

    def clc_quantity(self, date):
        """
        计算百分位
        :param date:数据
        :return:
        """
        pass

    def get_today_selected_ts(self):
        """
        获取当日选择的股票
        :return: 三个指标值都位于30%以内的股票
        """
        for ts_code in self.hs300_ts_code:
            if self.__check_ts(ts_code):
                self.selected_ts.append(ts_code)
        return self.selected_ts
