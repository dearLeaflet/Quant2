"""
因子分析函数
"""
import math
import db_data
import util


def is_quantile_qth(code, date=None, n=5, q=0.2):
    """
    当前近N天交易量是否位于前q分位以内，默认近两年
    :param code 股票代码
    :param date 起始日期
    :param q 分位数
    :param n 近几天
    当前交易量是否处于上四分位以内
    :return:
    """
    hist_data = db_data.get_vol_stocks(ts_code=code, trade_date=date)
    quantitles_qth = hist_data['vol'].quantile(q)
    return code, (hist_data['vol'].tail(n).values < quantitles_qth).all()


def back_test_factors(code, date):
    """
    根据分位数进行历史收益回测，给定某一日期，如果 连续五日处于某分位以内，则计算其未来半年的收益情况
    :param code 股票代码
    :param date 回测日期 %Y-%m-%d
    :return:
    """
    daily_info = db_data.get_vol_stocks(ts_code=code)
    dates = util.data_start_end_future(date)
    daily_info_period = daily_info.loc[(dates[1] > daily_info['trade_date']) & (daily_info['trade_date'] > dates[0])]
    quantity = daily_info_period['vol'].quantile(0.2)
    if (daily_info_period['vol'].tail(5).values < quantity).all():
        return True, daily_info.loc[daily_info['trade_date'] > dates[2]].head(1)
    # TODO 待完善



