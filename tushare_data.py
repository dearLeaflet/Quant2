# coding=utf-8
import datetime

import numpy
import tushare
import pandas
import myDb
import util

'''
获取股票相关数据
'''
import params


def get_tushare_pro() -> object:
    return tushare.pro_api(params.TUSHARE_TOCKEN)


def get_hs300s() -> object:
    """
    获取沪深300指数成份股，
    return：code :股票代码
            name :股票名称
            date :日期
            weight:权重
    """
    print('获取沪深300成份股列表')
    return tushare.get_hs300s()


def get_hs30s() -> object:
    """
    获取沪深30支随机股票
    return：code :股票代码
            name :股票名称
            date :日期
            weight:权重
    :return:
    """
    df = pandas.DataFrame(tushare.get_hs300s())
    shuffle = numpy.arange(0, 299, 10)
    df = df.sort_values(by='name')
    df = df.iloc[shuffle, :]
    return df


def get_stock_code_date():
    db = myDb.db_connect()
    cursor = db.cursor()
    try:
        sql = "select DISTINCT i.CODE, i.TRADE_DATE from daily_info i order by i.CODE desc, i.TRADE_DATE"
        cursor.execute(sql)
        # 获取所有记录列表
        return cursor.fetchall()
    except Exception:
        print("Error: unable to fetch data")
    finally:
        db.close()


def tick_insert(code, date):
    if '20180630' < date < '20190707' and code <= '600570':
        stock_tick_date = tushare.get_tick_data(code=code, date=date, src='tt')
        stock_tick_date['code'] = code
        stock_tick_date['date'] = date
        return pandas.DataFrame(stock_tick_date)
    return


def tick_insert1(code, date):
    stock_tick_date = tushare.get_tick_data(code=code, date=date, src='tt')
    stock_tick_date['code'] = code
    stock_tick_date['date'] = date
    return pandas.DataFrame(stock_tick_date)


def today_ticks_insert():
    """
    插入当日历史分笔
    :return:
    """
    db = myDb.db_connect()
    cursor = db.cursor()
    hs300 = get_hs300s()
    print('获取当日分笔数据并入库......')
    for index, row in hs300.iterrows():
        date = datetime.datetime.now().strftime('%Y%m%d')
        detail = tick_insert1(code=row['code'], date=date)
        if detail is None:
            continue
        for index1, detail_row in detail.iterrows():
            sql = "INSERT INTO tick_data(ID, CODE, DATE, TIME, PRICE, PCHANGE, VOLUME, AMOUNT, TYPE) VALUES (" \
                  + '\'' + str(row['code']) + str(date) + str(detail_row['time']).replace(":", "") + '\'' + ',' \
                  + '\'' + str(row['code']) + '\'' + ',' \
                  + '\'' + str(date) + '\'' + ',' \
                  + '\'' + str(detail_row['time']) + '\'' + ',' \
                  + '\'' + str(detail_row['price']) + '\'' + ',' \
                  + '\'' + str(detail_row['change']) + '\'' + ',' \
                  + '\'' + str(detail_row['volume']) + '\'' + ',' \
                  + '\'' + str(detail_row['amount']) + '\'' + ',' \
                  + '\'' + str(detail_row['type']) + '\'' \
                  + ')'
            myDb.data_insert(db, cursor, sql)
    db.close()
    print('当日分笔数据并入库完毕')
    return


def data_convert(date_str):
    return str(datetime.datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d'))


def fetch_data_db(sql):
    """
    获取数据库数据
    :param sql:
    :return:
    """
    db = myDb.db_connect()
    cursor = db.cursor()
    try:
        print(sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        return rs
    except:
        print("获取数据")
    finally:
        db.close()
    return


def daily_today_insert(price_type):
    """
    沪深300成份股日线数据入库
    daily_info
    price_type:string 复权类型
        qfq：前复权
        hfq：后复权
        None：不复权,默认值
    :return:
    """
    print("插入复前权历史日线数据......")
    db = myDb.db_connect()
    cursor = db.cursor()
    hs300 = get_hs300s()
    table_2_insert = price_type + "_daily_info" if (price_type != "bfq") else "daily_info"
    for index, stocks in hs300.iterrows():
        ts_code = util.stock_code_change(stocks['code'])
        date = datetime.datetime.now().strftime('%Y%m%d')
        daily = tushare.pro_bar(ts_code=ts_code, adj=price_type, start_date=date)
        for index, row in daily.iterrows():
            sql = "INSERT INTO " + table_2_insert + "(ID, CODE, TRADE_DATE, OPEN, CLOSE, HIGH, " \
                  + "LOW, PRE_CLOSE,PCHANGE, PCT_CHANGE, VOL, AMOUNT) VALUES(" \
                  + '\'' + str(row['ts_code'][:6]) + str(row['trade_date'][:10]) + '\'' + ',' \
                  + '\'' + str(row['ts_code'][:6]) + '\'' + ',' \
                  + '\'' + data_convert(row['trade_date'][:8]) + '\'' + ',' \
                  + '\'' + str(row['open']) + '\'' + ',' \
                  + '\'' + str(row['close']) + '\'' + ',' \
                  + '\'' + str(row['high']) + '\'' + ',' \
                  + '\'' + str(row['low']) + '\'' + ',' \
                  + '\'' + str(row['pre_close']) + '\'' + ',' \
                  + '\'' + str(row['change']) + '\'' + ',' \
                  + '\'' + str(row['pct_chg']) + '\'' + ',' \
                  + '\'' + str(row['vol']) + '\'' + ',' \
                  + '\'' + str(row['amount']) + '\'' \
                  + ')'
            myDb.data_insert(db, cursor, sql)
    cursor.close()
    print("完成插入前复权历史日线数据")


def hist_daily_insert(price_type):
    """
    沪深300成份股历史日线数据入库
    daily_info
    :return:
    """
    print("插入历史复权日线数据......:", price_type)
    db = myDb.db_connect()
    cursor = db.cursor()
    hs300 = get_hs300s()
    table_2_insert = price_type + "_daily_info" if (price_type != "bfq") else "daily_info"
    for index, stocks in hs300.iterrows():
        ts_code = util.stock_code_change(stocks['code'])
        daily = tushare.pro_bar(ts_code=ts_code, adj=price_type)
        for index1, row in daily.iterrows():
            if row['trade_date'][:8] < '20180630':
                continue
            sql = "INSERT INTO " + table_2_insert + "(ID, CODE, TRADE_DATE, OPEN, CLOSE, HIGH, LOW, PRE_CLOSE," \
                  + "PCHANGE, PCT_CHANGE, VOL, AMOUNT) VALUES(" \
                  + '\'' + str(row['ts_code'][:6]) + str(row['trade_date'][:10]) + '\'' + ',' \
                  + '\'' + str(row['ts_code'][:6]) + '\'' + ',' \
                  + '\'' + str(row['trade_date'][:8]) + '\'' + ',' \
                  + '\'' + str(row['open']) + '\'' + ',' \
                  + '\'' + str(row['close']) + '\'' + ',' \
                  + '\'' + str(row['high']) + '\'' + ',' \
                  + '\'' + str(row['low']) + '\'' + ',' \
                  + '\'' + str(row['pre_close']) + '\'' + ',' \
                  + '\'' + str(row['change']) + '\'' + ',' \
                  + '\'' + str(row['pct_chg']) + '\'' + ',' \
                  + '\'' + str(row['vol']) + '\'' + ',' \
                  + '\'' + str(row['amount']) + '\'' \
                  + ')'
            myDb.data_insert(db, cursor, sql)
    cursor.close()
    print("完成插入历史复权日线数据:", price_type)
    return


def get_index_daily(ts_code):
    """
    获取指数信息
    :param ts_code:
    :return:
    """
    pro = get_tushare_pro()
    df = pro.index_daily(ts_code=ts_code)
    return df


