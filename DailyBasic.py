import datetime
import tushare_data
import time


def get_rank(ts_data_frame, factor):
    """
    :return: 当前百分位
    """
    df = ts_data_frame[ts_data_frame['trade_date'] > '20160829']
    quantity_30 = df[factor].quantile(0.10)
    df = df.sort_values(by="trade_date", ascending=False)
    factor_rank = df[factor][0]
    return factor_rank < quantity_30


class DailyBasic:
    """
    获取沪深300成分股的每日PE、PB、PS值，并计算在过去某段时间类三个值百分位值
    筛选出三个值都位于30%一下的股票，并将统计结果以邮件的方式通知到自己
    """

    def __init__(self):
        self.pro = tushare_data.get_tushare_pro()
        trade_date = datetime.datetime.now() .strftime('%Y%m%d')
        df = self.pro.index_weight(index_code='399300.SZ')
        df = df.sort_values(by='trade_date', ascending=False)
        df = df.head(300)
        self.hs300_ts_code = df['con_code']
        self.selected_ts = []

    def check_ts(self, ts_code):
        """
        判断三个指标是否都符合
        :return:
        """
        ts_data_frame = self.pro.daily_basic(ts_code=ts_code)
        pe_perception = get_rank(ts_data_frame, 'pe_ttm')
        pb_perception = get_rank(ts_data_frame, 'pb')
        ps_perception = get_rank(ts_data_frame, 'ps_ttm')
        if pe_perception and pb_perception and ps_perception:
            return True
        else:
            return False

    def get_today_selected_ts(self):
        """
        获取当日选择的股票
        :return: 三个指标值都位于30%以内的股票
        """
        for ts_code in self.hs300_ts_code:
            if self.check_ts(ts_code):
                time.sleep(1)
                self.selected_ts.append(ts_code)
        return self.selected_ts

    """
    dailyBasic = DailyBasic()
    ts_codes = dailyBasic.get_today_selected_ts()
    print(ts_codes)
    """