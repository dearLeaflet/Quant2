from datetime import datetime

import db_data as data
import pandas

import my_trend_line
import tushare_data
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import db_data


def money_flow_statistic_view(type):
    """
    小单成交比例统计折线图
    :return:
    """
    hs300_daily_info = tushare_data.get_index_daily('000300.SH')
    hs300_daily_info = hs300_daily_info.loc[hs300_daily_info['trade_date'] > '20180630']
    money_flow_statistic = data.get_money_flow_statistic()
    money_flow_statistic_rate = money_flow_statistic['small_total_rate']
    money_flow_statistic_rate_rolling = pandas.DataFrame(money_flow_statistic_rate).rolling(window=3, min_periods=1,
                                                                                            center=True)
    money_flow_statistic_rate_rolling = money_flow_statistic_rate_rolling.mean()
    if type == 1:
        fig, (ax0, ax1) = plt.subplots(nrows=2)
        fig.autofmt_xdate(rotation=90)
        mondays = mdates.WeekdayLocator(mdates.MONDAY)
        alldays = mdates.DayLocator()
        ax1.xaxis.set_major_locator(mondays)
        ax1.xaxis.set_minor_locator(alldays)
        ax1.xaxis.grid(True, which='major')  # x坐标轴的网格使用主刻度
        ax1.plot(money_flow_statistic['trade_date'], money_flow_statistic_rate_rolling)
        ax1.grid(True)
        ax1.set_xlabel('trade_date')
        ax1.set_ylabel('small_trade_rate')
        ax0.plot(hs300_daily_info['trade_date'], hs300_daily_info['close'])
        ax0.grid(True)
        ax0.set_ylabel('close')
        plt.show()

    if type == 2:
        plt.subplot(2, 1, 1)
        plt.plot(hs300_daily_info['trade_date'], hs300_daily_info['close'], '.-')
        plt.title('hs300 index')
        plt.ylabel('index')

        plt.subplot(2, 1, 2)
        plt.plot(money_flow_statistic['trade_date'], money_flow_statistic_rate_rolling, '.-')
        plt.title('small trade index')
        plt.xlabel('trade date')
        plt.ylabel('index')
        plt.show()


def save_trade_figure():
    """
    可视化比较
    :return:
    """
    line_sum = str(8)
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

    money_flow_statistic = data.get_money_flow_statistic()

    money_flow_statistic_rate = money_flow_statistic['small_total_rate']
    money_flow_statistic_rate_rolling = date_rolling(money_flow_statistic_rate)
    draw_single(money_flow_statistic['trade_date'], money_flow_statistic_rate_rolling, line_sum + '11',
                '中证800小单成交比例')

    # 小单买卖比例图
    small_buy_vs_sell = money_flow_statistic['small_buy_vs_sell']
    small_buy_vs_sell_rolling = pandas.DataFrame(small_buy_vs_sell).rolling(window=3, min_periods=1,
                                                                            center=True)
    small_buy_vs_sell_rolling = small_buy_vs_sell_rolling.mean()
    draw_single(money_flow_statistic['trade_date'], small_buy_vs_sell_rolling, line_sum + '12', '小单买卖比例')
    """
    money_flow_statistic['baseline_small'] = 1
    plt.plot(myIndexDate, money_flow_statistic['baseline_small'])
    """

    # 沪深300指数图
    hs300_daily_info = tushare_data.get_index_daily('000300.SH')
    hs300_daily_info = hs300_daily_info.loc[hs300_daily_info['trade_date'] > '20180630']
    hs300_close_rolling = date_rolling(hs300_daily_info['close'])
    draw_single(hs300_daily_info['trade_date'], hs300_close_rolling, line_sum + '13', '沪深300指数')

    # 沪深300成交量图
    hs300_vol_rolling = date_rolling(hs300_daily_info['vol'])
    draw_single(hs300_daily_info['trade_date'], hs300_vol_rolling, line_sum + '14', '沪深300成交量')
    """
    hs300_daily_info['vol_baseline_1'] = 100000000
    hs300_daily_info['vol_baseline_075'] = 75000000
    plt.plot(hs300_daily_info_date, hs300_daily_info['vol_baseline_1'])
    plt.plot(hs300_daily_info_date, hs300_daily_info['vol_baseline_075'], color='r')
    """

    # 融资余额
    margin_info = db_data.get_margin_info()
    rzye_rolling = date_rolling(margin_info['rzye'])
    draw_single(margin_info['trade_date'], rzye_rolling, line_sum + '15', '融资余额')

    # 沪港通现金流信息
    hsgt_info = db_data.get_hsgt_info()
    # south_money_rolling = date_rolling(hsgt_info['south_money'])
    # draw_single(hsgt_info['trade_date'], south_money_rolling, '616', '沪港通现金流')
    north_money_rolling = date_rolling(hsgt_info['north_money'])
    draw_single(hsgt_info['trade_date'], north_money_rolling, line_sum + '18', '沪港通现金流')

    # kdj指标
    KDJ_index = my_trend_line.hs300_kdj()
    draw_single(KDJ_index['date'], KDJ_index['k'], line_sum + '16', 'KDJ')
    draw_single(KDJ_index['date'], KDJ_index['d'], line_sum + '16', 'KDJ')

    # macd指标
    MACD_index = my_trend_line.hs300_macd()
    draw_single(MACD_index['date'], MACD_index['diff'], line_sum + '17', 'MACD：蓝色为DIFF')
    draw_single(MACD_index['date'], MACD_index['dea'], line_sum + '17', 'MACD：蓝色为DIFF')

    figure = plt.gcf()  # 获取当前图片
    figure.set_size_inches(21.6, 24)
    plt.savefig('./result/' + datetime.now().strftime('%Y%m%d') + '.png', dpi=100)


def draw_single(x, y, plot_msg, y_label):
    """
    画单独图片
    :param y_label：y轴坐标说明
    :param plot_msg: 子图位置
    :param x: x轴数据
    :param y: y轴数据
    :return:
    """
    plt.subplot(plot_msg)
    x = [datetime.strptime(d, '%Y%m%d').date() for d in x]
    plt.plot(x, y)
    plt.ylabel(y_label)
    plt.grid(True)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y%m%d'))
    plt.gca().xaxis.set_major_locator(mdates.WeekdayLocator())  # 日期显示粒度为天，可以换成周、月、年等
    plt.xticks(rotation=45)


def date_rolling(date):
    """
    窗口平滑
    :param date:
    :return:
    """
    if date is None or date.size < 1:
        return None
    rolling = pandas.DataFrame(date).rolling(window=3, min_periods=1, center=True)
    return rolling.mean()


def margin_info_figure():
    pro = tushare_data.get_tushare_pro()
    df = pro.margin(start_date='20140922')
    rziy = df.groupby(['trade_date'])['rzye'].sum()
    print(rziy)
    draw_single(df['trade_date'].drop_duplicates(), rziy, '211', '融资余额')

    hs300_daily_info = tushare_data.get_index_daily('000300.SH')
    hs300_daily_info = hs300_daily_info.loc[hs300_daily_info['trade_date'] > '20140922']
    hs300_close_rolling = date_rolling(hs300_daily_info['close'])
    draw_single(hs300_daily_info['trade_date'], hs300_close_rolling, '212', '沪深300指数')

    figure = plt.gcf()  # 获取当前图片
    figure.set_size_inches(21.6, 24)
    plt.savefig('./result/' + datetime.now().strftime('%Y%m%d') + 'yzye.png', dpi=100)

margin_info_figure()
