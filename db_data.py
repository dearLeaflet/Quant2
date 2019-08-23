"""
利用SQLAlchemy查询数据并转换为DataFrame
"""
import random
import time

import tushare
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime
import myDb
import tables
import tushare_data
# 初始化数据库连接:
import util
import params
import seeker

engine = create_engine(
    f'mysql+mysqlconnector://{params.DATABASE_USER}:{params.DATABASE_PASSWORD}@localhost:3306/{params.MY_INDEX_BASE}')
DBSession = sessionmaker(bind=engine)
# 创建Session:
session: object = DBSession()


def get_vol_stocks(ts_code=None, trade_date=None):
    """
    获取股票代码、交易日期、交易量，默认近两年
   :param ts_code:股票代码，默认为零
    :param trade_date:交易起始日期
    :return: DataFrame，获取股票的代码，交易日期，成交量
    """
    # 如日期为None则默认返回近三年数据
    if trade_date is None:
        trade_date = (datetime.datetime.today() - datetime.timedelta(days=365 * 2)).strftime('%Y-%m-%d')
    if ts_code:
        query_obj = session.query(tables.DailyInfo.code, tables.DailyInfo.trade_date,
                                  tables.DailyInfo.vol).filter(tables.DailyInfo.code == ts_code,
                                                               tables.DailyInfo.trade_date > trade_date) \
            .order_by(tables.DailyInfo.trade_date)
        return pd.read_sql(query_obj.statement, engine)
    else:
        query_obj = session.query(tables.DailyInfo.code, tables.DailyInfo.vol,
                                  tables.DailyInfo.trade_date > trade_date if trade_date else '')
        return pd.read_sql(query_obj.statement, engine)


def db_session_close():
    # 释放Session
    return session.close()


def hs30s_add():
    """
    导入30支沪深300股票基本权重信息
    :return:
    """
    try:
        hs30s = tushare_data.get_hs30s()
        print("导入沪深300成份股中30支股票")
        for index, row in hs30s.iterrows():
            tables.add_index_stocks(code=row['code'], date=str(row['date'])[:10], name=row['name'],
                                    weight=row['weight'])
        print("导入完成")
    except Exception as err:
        print("数据导入失败:", str(err))


def hs30s_daily_info_add(today=False):
    """
    导入30支沪深300股票日线信息
    :return:
    """
    hs_code = tables.hs30_queryy()
    for row in hs_code:
        try:
            ts_code = util.stock_code_change(row.code)
            if today:
                start_date = datetime.datetime.now().strftime('%Y%m%d')
                end_date = datetime.datetime.now().strftime('%Y%m%d')
                daily = tushare.pro_bar(ts_code=ts_code, start_date=start_date, end_date=end_date)
                print("插入日线数据：st_code =", row.code, ' date =', end_date)
                pass
            else:
                daily = tushare.pro_bar(ts_code=ts_code)
                print("插入日线数据：st_code =", row.code)
            for index, daily_info in daily.iterrows():
                id = str(daily_info['ts_code'][:6]) + str(daily_info['trade_date'][:10]).replace('-', '')
                tables.add_daily_info(id=id, code=row.code, open=daily_info['open'], close=daily_info['close'],
                                      high=daily_info['high'], low=daily_info['low'], pre_close=daily_info['pre_close'],
                                      pchange=daily_info['change'], vol=daily_info['vol'], amount=daily_info['amount'],
                                      pct_change=daily_info['pct_chg'], trade_date=daily_info['trade_date'])
        except Exception as e:
            print(e)


def tick_data_add(today=False):
    """
    历史分时数据插入
    :return:
    """
    if today:
        hs30s_daily_info = tables.hs30_queryy()
        print(hs30s_daily_info)
    else:
        hs30s_daily_info = tables.hs30_daily_queryy()
    for row in hs30s_daily_info:
        if today:
            date = datetime.datetime.now().strftime('%Y%m%d')
        else:
            date = row.trade_date[:4] + '-' + row.trade_date[4:6] + '-' + row.trade_date[6:8]
        print("插入分时数据：st_code =", row.code, ' date =', date)
        try:
            if date <= '2018-06-32':
                continue
            df = tushare.get_tick_data(code=row.code, date=date, src='tt')
            if df is None:
                continue
            for index, tick in df.iterrows():
                date = date.replace('-', '')
                id = row.code + date + tick['time'] + str(random.randint(1, 1000000)).zfill(5)
                tables.add_tick_date(code=row.code, date=date, time=tick['time'], price=tick['price'],
                                     pchange=tick['change'], volume=tick['amount'], amount=tick['amount'],
                                     type=tick['type'], id=id)
            print("插入分时数据：", "st_code =", row.code, ' date =', date, "插入完成")
        except Exception as e:
            print(e)


def hs300_insert():
    """
    沪深300成分股入库
    :return:
    """
    hs300 = tushare_data.get_hs300s()
    db = myDb.db_connect()
    cursor = db.cursor()
    for index, row in hs300.iterrows():
        sql = "INSERT INTO INDEX_STOCKS (CODE, NAME, INDEX_TYPE, DATE, WEIGHT) VALUES(" \
              + '\'' + row['code'] + '\'' + ',' \
              + '\'' + row['name'] + '\'' + ',' \
              + '\'' + '300' + '\'' + ',' + '\'' \
              + str(row['date'])[:10] + '\'' + ',' \
              + '\'' + str(row["weight"]) + '\'' \
              + ')'
        myDb.data_insert(db, cursor, sql)
    db.close()


def add_money_flow():
    """
    沪深300生成小单统计数据
    tips:成交明细列表中的买盘/卖盘：“买盘”表示以比市价高的价格进行委托买入，并已经“主动成交”，代表外盘；
        “卖盘”表示以比市价低的价格进行委托卖出，并已经“主动成交”，代表内盘
    :return:
    """
    print('插入历史现金流信息开始......')
    try:
        zz500 = tushare.get_zz500s()
        hs300_index = tushare.get_hs300s()
        zz800_ts_codes = hs300_index.append(zz500)
        ts_codes = zz800_ts_codes['code'].apply(util.stock_code_change)
        for ts_code in ts_codes:
            daily_info = tushare.pro_bar(ts_code, api=tushare_data.get_tushare_pro())
            if daily_info is None:
                continue
            for index, row in daily_info.iterrows():
                date = row['trade_date']
                date = date[:4] + '-' + date[4:6] + '-' + date[6:]
                code = row['ts_code'][:6]
                if date > '2018-06-30':
                    df = tushare.get_tick_data(code, date=date, src='tt')
                    if df is None:
                        continue
                    total_amt = df['amount'].sum()
                    total_vol = df['volume'].sum()

                    sell_trade = df.loc[(df['type'] == '卖盘')]
                    total_sell_vol = sell_trade['volume'].sum()
                    total_sell_amt = sell_trade['amount'].sum()

                    small_trade_amount = util.trade_scale(row['close'])

                    sell_sm_trade = df.loc[(df['type'] == '卖盘') & (df['amount'] < small_trade_amount)]
                    sell_sm_vol = sell_sm_trade['volume'].sum()
                    sell_sm_amt = sell_sm_trade['amount'].sum()

                    buy_trade = df.loc[(df['type'] == '买盘')]
                    total_buy_vol = buy_trade['volume'].sum()
                    total_buy_amt = buy_trade['amount'].sum()

                    buy_sm_trade = df.loc[(df['type'] == '买盘') & (df['amount'] < small_trade_amount)]
                    buy_sm_vol = buy_sm_trade['volume'].sum()
                    buy_sm_amt = buy_sm_trade['amount'].sum()

                    total_sm_trade = df.loc[df['amount'] < small_trade_amount]
                    total_sm_amt = total_sm_trade['amount'].sum()
                    total_sm_vol = total_sm_trade['volume'].sum()

                    id = code + row['trade_date']
                    tables.add_money_flow(id=id, code=code, date=row['trade_date'], sell_sm_vol=sell_sm_vol,
                                          sell_sm_amt=sell_sm_amt,
                                          buy_sm_vol=buy_sm_vol, buy_sm_amt=buy_sm_amt, total_sell_vol=total_sell_vol,
                                          total_sell_amt=total_sell_amt, total_buy_vol=total_buy_vol,
                                          total_buy_amt=total_buy_amt, total_amt=total_amt, total_vol=total_vol,
                                          total_sm_amt=total_sm_amt, total_sm_vol=total_sm_vol)
        print('插入历史现金流信息完成......')
    except Exception as err:
        print('插入历史现金流信息失败......')
        raise err


def add_money_flow_today():
    """
    沪深300当日生成小单统计数据
    tips:成交明细列表中的买盘/卖盘：“买盘”表示以比市价高的价格进行委托买入，并已经“主动成交”，代表外盘；
        “卖盘”表示以比市价低的价格进行委托卖出，并已经“主动成交”，代表内盘
    :return:
    """
    print('插入当日现金流信息开始......')
    zz500 = tushare.get_zz500s()
    hs300_index = tushare.get_hs300s()
    zz800_ts_codes = hs300_index.append(zz500)
    ts_codes = zz800_ts_codes['code'].apply(util.stock_code_change)
    for ts_code in set(ts_codes):
        time.sleep(1)
        try:
            date = datetime.datetime.now().strftime('%Y%m%d')
            daily_info = tushare_data.get_tushare_pro().daily(ts_code=ts_code, trade_date=date)
            close = daily_info['close']

            df = tushare.get_tick_data(str(ts_code[:6]), date=date, src='tt')
            if df is None:
                print("未获取到股票{0}-{1}的信息".format(ts_code, date))
                continue

            total_amt = df['amount'].sum()
            total_vol = df['volume'].sum()

            sell_trade = df.loc[(df['type'] == '卖盘')]
            total_sell_vol = sell_trade['volume'].sum()
            total_sell_amt = sell_trade['amount'].sum()

            small_trade_amount = util.trade_scale(close)

            sell_sm_trade = df.loc[(df['type'] == '卖盘') & (df['amount'] < small_trade_amount)]
            sell_sm_vol = sell_sm_trade['volume'].sum()
            sell_sm_amt = sell_sm_trade['amount'].sum()

            buy_trade = df.loc[(df['type'] == '买盘')]
            total_buy_vol = buy_trade['volume'].sum()
            total_buy_amt = buy_trade['amount'].sum()

            buy_sm_trade = df.loc[(df['type'] == '买盘') & (df['amount'] < small_trade_amount)]
            buy_sm_vol = buy_sm_trade['volume'].sum()
            buy_sm_amt = buy_sm_trade['amount'].sum()

            total_sm_trade = df.loc[df['amount'] < small_trade_amount]
            total_sm_amt = total_sm_trade['amount'].sum()
            total_sm_vol = total_sm_trade['volume'].sum()

            id = ts_code[:6] + date
            tables.add_money_flow(id=id, code=str(ts_code[:6]), date=date, sell_sm_vol=sell_sm_vol,
                                  sell_sm_amt=sell_sm_amt,
                                  buy_sm_vol=buy_sm_vol, buy_sm_amt=buy_sm_amt, total_sell_vol=total_sell_vol,
                                  total_sell_amt=total_sell_amt, total_buy_vol=total_buy_vol,
                                  total_buy_amt=total_buy_amt, total_amt=total_amt, total_vol=total_vol,
                                  total_sm_amt=total_sm_amt, total_sm_vol=total_sm_vol)
        except Exception as err:
            print('插入当日现金流信息失败......')
            raise err
    print('插入当日现金流信息完成......')


def add_money_flow_statistic(today=None):
    """
    计算资金流向统计信息并入库
    :param today:
    :return:
    """
    if today is not None:
        print('插入当日现金流统计信息开始......')
        date = datetime.datetime.now().strftime('%Y%m%d')
        df = tables.get_moneyflow_info(date)
        if df.size == 0:
            print("未获取到{0}的信息".format(date))
            return None
        small_total_rate = df['total_sm_amt'].sum() / df['total_amt'].sum() * 100
        total_sm_amt = df['total_sm_amt'].sum()
        total_sm_vol = df['total_sm_vol'].sum()
        total_amt = df['total_amt'].sum()
        total_vol = df['total_vol'].sum()
        small_buy_vs_sell = df['buy_sm_amt'].sum() / df['sell_sm_amt'].sum()
        try:
            tables.add_moneyflowstatistic(trade_date=date, small_amt=total_sm_amt, small_vol=total_sm_vol,
                                          total_amt=total_amt, total_vol=total_vol,
                                          small_total_rate=small_total_rate, small_buy_vs_sell=small_buy_vs_sell)
            print('插入当日现金流统计信息完成......')
        except Exception as err:
            print('插入当日现金流统计信息失败', err)
    else:
        print("插入历史现金流统计信息开始......")
        money_flow_info = tables.get_moneyflow_info()
        money_flow_info_date_sets = money_flow_info['date'].drop_duplicates()
        for trade_date in money_flow_info_date_sets:
            daily_money_flow_info = money_flow_info.loc[money_flow_info['date'] == trade_date]
            small_total_rate = daily_money_flow_info['total_sm_amt'].sum() / daily_money_flow_info[
                'total_amt'].sum() * 100
            total_sm_amt = daily_money_flow_info['total_sm_amt'].sum()
            total_sm_vol = daily_money_flow_info['total_sm_vol'].sum()
            total_amt = daily_money_flow_info['total_amt'].sum()
            total_vol = daily_money_flow_info['total_vol'].sum()
            small_buy_vs_sell = daily_money_flow_info['buy_sm_amt'].sum() / daily_money_flow_info['sell_sm_amt'].sum()
            try:
                tables.add_moneyflowstatistic(trade_date=trade_date, small_amt=total_sm_amt, small_vol=total_sm_vol,
                                              total_amt=total_amt, total_vol=total_vol,
                                              small_total_rate=small_total_rate, small_buy_vs_sell=small_buy_vs_sell)
            except Exception as err:
                print('插入历史现金流统计信息失败', err)
        print("插入历史现金流统计信息完成......")


def get_money_flow_statistic():
    """
    获取现金流统计信息
    :return:
    """
    return tables.getMoneyFlowStatistic()


def add_hist_etf_exchange_amt():
    """
    插入历史ETF交易数据
    :return:
    """
    etf_exchange_info = seeker.get_etf_daily_exchange_amt()
    if etf_exchange_info is not None:
        for item in etf_exchange_info:
            try:
                tables.etf_exchange_amt_add(trade_date=item[0], amt=item[1])
            except Exception as err:
                print('{0} ETF交易数据插入失败'.format(item[0]))
                print(err)


def get_etf_exchange_info():
    """
    获取ETF交易数据
    :return:
    """
    return tables.get_etf_exchange_amt()


def add_today_etf_exchange_amt():
    """
    插入历史ETF交易数据
    :return:
    """
    print("插入当日ETF交易数据")
    etf_exchange_info = seeker.get_etf_today_exchange_amt()
    if etf_exchange_info is not None:
        try:
            tables.etf_exchange_amt_add(trade_date=etf_exchange_info[0], amt=etf_exchange_info[1])
        except Exception as err:
            print('{0} ETF交易数据插入失败'.format(etf_exchange_info[0]))
            print(err)


def add_margin_info(today=None):
    """
    插入融资融券信息
    :param today:
    :return:
    """
    if today is not None:
        """
        插入今日信息
        """
        pro = tushare_data.get_tushare_pro()
        trade_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        df = pro.margin(trade_date=trade_date)
        if df is not None and df.size != 0:
            try:
                tables.margin_info_add(trade_date, df['rzye'].sum(), df['rzmre'].sum(), df['rzche'].sum(),
                                       df['rqye'].sum(), df['rqmcl'].sum(), df['rzrqye'].sum())
                print('{0}融资余额插入成功'.format(trade_date))
            except Exception as err:
                print('{0}融资余额插入失败'.format(trade_date))
                print(err)

    else:
        """
        插入历史信息
        """
        pro = tushare_data.get_tushare_pro()
        money_flow_info = tables.get_moneyflow_info()
        money_flow_info_date_sets = money_flow_info['date'].drop_duplicates()
        for trade_date in money_flow_info_date_sets:
            df = pro.margin(trade_date=trade_date)
            time.sleep(1)
            if df is not None and df.size != 0:
                try:
                    tables.margin_info_add(trade_date, df['rzye'].sum(), df['rzmre'].sum(), df['rzche'].sum(),
                                           df['rqye'].sum(), df['rqmcl'].sum(), df['rzrqye'].sum())
                    print('{0}融资余额插入成功'.format(trade_date))
                except Exception as err:
                    print('{0}融资余额插入失败'.format(trade_date))
                    print(err)


def get_margin_info():
    """
    获取融资融券信息
    :return:
    """
    return tables.get_margin_info()

