"""
描述本地数据库表属性
优化性能，可参考：
https://docs.sqlalchemy.org/en/13/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
"""
import datetime

from sqlalchemy import Column, String, Sequence, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import FLOAT, INTEGER
from sqlalchemy.orm import sessionmaker
import pandas
import myDb
import params
db = myDb.db_connect()
cursor = db.cursor()
Base = declarative_base()

# 初始化数据库连接:
engine = create_engine("mysql://root:{0}@{1}/{2}".format(params.PASSWORD, params.DBIP, params.DATABASE_NAME),
                       encoding='latin1', echo=True)
DBSession = sessionmaker(bind=engine)
# 创建Session:
session: object = DBSession()


class DailyInfo(Base):
    """
    日线数据
    """
    __tablename__ = 'daily_info'
    id = Column(String(16), Sequence('user_id_seq'), primary_key=True)
    code = Column(String(6))
    trade_date = Column(String(10))
    open = Column(FLOAT(precision=10, scale=2))
    close = Column(FLOAT(precision=10, scale=2))
    high = Column(FLOAT(precision=10, scale=2))
    low = Column(FLOAT(precision=10, scale=2))
    pre_close = Column(FLOAT(precision=10, scale=2))
    pchange = Column(FLOAT(precision=10, scale=2))
    pct_change = Column(FLOAT(precision=10, scale=2))
    vol = Column(INTEGER(20))
    amount = Column(FLOAT(precision=10, scale=2))


def add_daily_info(id, code, trade_date, open, close, high, low, pre_close, pchange, pct_change, vol, amount):
    """
    插入股票日线数据
    :param id: code+trade_date,唯一索引
    :param code 股票代码
    :param trade_date:交易日
    :param open:开盘价
    :param close:收盘价
    :param high：最高价
    :param low：最低价
    :param pre_close:昨日收盘价
    :param pchange：今日涨跌额
    :param pct_change:今日涨跌幅
    :param vol：交易量
    :param amount：交易金额
    :return:
    """
    daily_info = DailyInfo(id=id, code=code, trade_date=trade_date, open=open, close=close, high=high, low=low,
                           pre_close=pre_close, pchange=pchange, pct_change=pct_change, vol=vol, amount=amount)
    try:
        session.add(daily_info)
        session.rollback()
        session.commit()
    except Exception as err:
        print("插入失败：", err)


def hs300_daily_queryy(date):
    """
    查询沪深30股票分时数据
    :return:
    """
    return pandas.read_sql(session.query(DailyInfo).filter(DailyInfo.trade_date == date).statement, session.bind)


class IndexStocks(Base):
    """
    指数股票信息
    """
    __tablename__ = "index_stocks"
    code = Column(String(6), primary_key=True)
    date = Column(String(10))
    name = Column(String(255))
    weight = Column(FLOAT(precision=10, scale=2))


def add_index_stocks(code, date, name, weight):
    """
    股票信息插入
    :param code: 股票代码
    :param date: 指数时间
    :param name: 股票名称
    :param weight: 占指数权重
    :return:
    """
    index_stocks = IndexStocks(code=code, date=date, name=name, weight=weight)
    try:
        session.add(index_stocks)
        session.commit()
    except Exception as err:
        session.rollback()
        print("插入失败：", err)


def hs300_queryy():
    """
    查询沪深30股票代码信息
    :return:
    """
    return session.query(IndexStocks).all()


class TickDate(Base):
    """
    分时成交数据
    """
    __tablename__ = 'tick_data'
    code = Column(String(6))
    date = Column(String(10))
    time = Column(String(8))
    price = Column(FLOAT(precision=10, scale=2))
    pchange = Column(FLOAT(precision=10, scale=2))
    volume = Column(INTEGER(20))
    amount = Column(FLOAT(precision=10, scale=2))
    type = Column(String(8))
    id = Column(String(40), primary_key=True)


def add_tick_date(code, date, time, price, pchange, volume, amount, type, id):
    """
    sqlalchemy 性能较差，考虑使用原生拼接产生sql
    """
    '''
    tick_data = TickDate(code=code, date=date, time=time, price=price, pchange=pchange, volume=volume, amount=amount,
                         type=type, id=id)
    session.add(tick_data)
    '''
    '''
    # 还是很慢
    engine.execute(TickDate.__table__.insert(), {"code": code, "date": date, "time": time, "price": price,
                                                 "change": pchange, "volume": volume, "amount": amount, "type": type,
                                                 "id": id})
    '''
    '''
    手工拼sql减少了sql语句的表结构的解析与sql的生成
    '''
    sql = "INSERT INTO tick_data(ID, CODE, DATE, TIME, PRICE, PCHANGE, VOLUME, AMOUNT, TYPE) VALUES (" \
          + '\'' + str(id) + '\'' + ',' \
          + '\'' + str(code) + '\'' + ',' \
          + '\'' + str(date) + '\'' + ',' \
          + '\'' + str(time) + '\'' + ',' \
          + '\'' + str(price) + '\'' + ',' \
          + '\'' + str(pchange) + '\'' + ',' \
          + '\'' + str(volume) + '\'' + ',' \
          + '\'' + str(amount) + '\'' + ',' \
          + '\'' + str(type) + '\'' \
          + ')'
    try:
        myDb.data_insert(db, cursor, sql)
    except Exception as err:
        print("插入失败：", err)


def tick_data_query(code, date=None):
    """
    查询分时数据
    :return:
    """
    if date is None:
        date = datetime.datetime.now().strftime('%Y%m%d')
        import pandas
        pandas.read_sql()
    return session.query(TickDate).filter(TickDate.code == code, TickDate.date == date)


class MoneyFlow(Base):
    """
    个股资金流向
    """
    __tablename__ = 'moneyflow'
    id = Column(String(14), primary_key=True)
    code = Column(String(6))
    date = Column(String(8))
    sell_sm_vol = Column(INTEGER(16))
    sell_sm_amt = Column(INTEGER(16))
    buy_sm_vol = Column(INTEGER(16))
    buy_sm_amt = Column(INTEGER(16))
    total_sell_vol = Column(INTEGER(16))
    total_sell_amt =Column(INTEGER(16))
    total_buy_vol = Column(INTEGER(16))
    total_buy_amt = Column(INTEGER(16))
    total_vol = Column(INTEGER(16))
    total_amt = Column(INTEGER(16))
    total_sm_vol = Column(INTEGER(16))
    total_sm_amt = Column(INTEGER(16))


def add_money_flow(id, code, date, sell_sm_vol, sell_sm_amt, buy_sm_vol, buy_sm_amt,
                   total_sell_vol, total_sell_amt, total_buy_vol, total_buy_amt, total_amt,
                   total_vol, total_sm_amt, total_sm_vol):
    """
    插入资金流量信息
    :param total_sm_vol:
    :param total_sm_amt:
    :param total_vol:
    :param total_amt:
    :param id:
    :param code: 股票代码
    :param date: 交易日期
    :param sell_sm_vol: 小单卖出手数
    :param sell_sm_amt: 小单卖出金额
    :param buy_sm_vol: 小单买入手数
    :param buy_sm_amt: 小单买入金额
    :param total_sell_vol: 总卖出手数
    :param total_sell_amt: 总卖出金额
    :param total_buy_vol: 总买入手数
    :param total_buy_amt: 总买入金额
    :param net_inflows_amt: 净流入金额
    :return:
    """
    moneyflow = MoneyFlow(id=id, code=code, date=date, sell_sm_vol=int(sell_sm_vol), sell_sm_amt=int(sell_sm_amt),
                          buy_sm_vol=int(buy_sm_vol), buy_sm_amt=int(buy_sm_amt), total_sell_vol=int(total_sell_vol),
                          total_sell_amt=int(total_sell_amt), total_buy_vol=int(total_buy_vol), total_buy_amt=int(total_buy_amt),
                          total_amt=int(total_amt), total_vol=int(total_vol), total_sm_amt=int(total_sm_amt),
                          total_sm_vol=int(total_sm_vol))
    try:
        session.add(moneyflow)
        session.commit()
        print(code, '-', date, ":插入成功")
    except Exception as err:
        session.rollback()
        print("插入失败：", err)


def get_moneyflow_info(date=None):
    """
    查询所有个股单日资金流向信息
    :param date:
    :return:
    """
    if date is not None:
        return pandas.read_sql(session.query(MoneyFlow).filter(MoneyFlow.date == date).statement, session.bind)
    else:
        return pandas.read_sql(session.query(MoneyFlow).statement, session.bind)


class MoneyFlowStatistic(Base):
    """
    个股资金流向统计信息（沪深300）
    """
    __tablename__ = 'moneyflowstatistic'
    trade_date = Column(String(8), primary_key=True)
    small_vol = Column(INTEGER(16))
    small_amt = Column(INTEGER(16))
    total_vol = Column(INTEGER(16))
    total_amt = Column(INTEGER(16))
    small_total_rate = Column(FLOAT(precision=4, scale=4))
    small_buy_vs_sell = Column(FLOAT(precision=4, scale=4))


def add_moneyflowstatistic(trade_date, small_vol, small_amt, total_vol, total_amt, small_total_rate, small_buy_vs_sell):
    """
    插入沪深300统计信息
    :param trade_date:yyyyMMdd
    :param small_vol:
    :param small_amt:
    :param total_vol:
    :param total_amt:
    :param samll_total_rate:0.4f
    :return:
    """
    moneyflowstatistic = MoneyFlowStatistic(trade_date=int(trade_date), small_amt=int(small_amt), small_vol=int(small_vol),
                                            total_vol=int(total_vol), total_amt=int(total_amt),
                                            small_total_rate=small_total_rate, small_buy_vs_sell=small_buy_vs_sell)
    try:
        session.add(moneyflowstatistic)
        session.commit()
        print(trade_date, ":插入成功")
    except Exception as err:
        session.rollback()
        raise err


def getMoneyFlowStatistic():
    """查询现金流统计信息"""
    return pandas.read_sql(session.query(MoneyFlowStatistic).statement, session.bind)


class MyIndex(Base):
    """
    我的指数
    :param 交易日期
    :param 指数点位
    """
    __tablename__ = 'myindex'
    date = Column(String(8), primary_key=True)
    index = Column(FLOAT(precision=10, scale=2))


def my_index_add(date, index):
    """
    插入指数点位
    :param date:
    :param index:
    :return:
    """
    myindex = MyIndex(date=date, index=index)
    try:
        session.add(myindex)
        session.commit()
    except Exception as err:
        session.rollback()
        raise err


def my_index_querry():
    """
    查询指数点位
    :return:
    """
    return pandas.read_sql(session.query(MoneyFlow).statement, session.bind)


class ETFExchangeAmt(Base):
    """
    ETF基金交易金额
    """
    __tablename__ = 'etfexchangeamt'

    trade_date = Column(String(8), primary_key=True)
    amt = Column(INTEGER(32))


def etf_exchange_amt_add(trade_date, amt):
    """
    插入ETF基金交易金额
    :return:
    """
    etfExchangeAmt = ETFExchangeAmt(trade_date=trade_date, amt=amt)
    try:
        session.add(etfExchangeAmt)
        session.commit()
    except Exception as err:
        session.rollback()
        raise err


def get_etf_exchange_amt():
    """ETF交易金额查询"""
    return pandas.read_sql(session.query(ETFExchangeAmt).statement, session.bind)


class Margin(Base):
    """
    融资融券交易汇总
    @:param trade_date 交易日期
    @:param rzye 融资余额
    @:param rzmre 融资买入额
    @:param rzche 融资偿还额
    @:param rqye 融券余额
    @:param rqmcl 融券卖出额
    @:param rzrqye 融资融券余额
    """
    __tablename__ = 'margin'
    trade_date = Column(String(8), primary_key=True)
    rzye = Column(INTEGER(32))
    rzmre = Column(INTEGER(32))
    rzche = Column(INTEGER(32))
    rqye = Column(INTEGER(32))
    rqmcl = Column(INTEGER(32))
    rzrqye = Column(INTEGER(32))


def margin_info_add(trade_date, rzye, rzmre, rzche, rqye, rqmcl, rzrqye):
    """
    融资融券交易汇总信息插入

    :param date:
    :param index:
    :return:
    """
    margin = Margin(trade_date=trade_date, rzye=rzye, rzmre=rzmre, rzche=rzche, rqye=rqye, rqmcl=rqmcl, rzrqye=rzrqye)
    try:
        session.add(margin)
        session.commit()
    except Exception as err:
        session.rollback()
        raise


def get_margin_info():
    """融资融券交易汇总信息查询"""
    return pandas.read_sql(session.query(Margin).statement, session.bind)


class MoneyFlowHSGT(Base):
    """
    沪港通资金流向

    """
    __tablename__ = 'moneyflowhsgt'
    trade_date = Column(String(8), primary_key=True)
    north_money = Column(INTEGER(32))
    south_money = Column(INTEGER(32))


def money_flow_hsgt_add(trade_date, north_money, south_money):
    """
    沪港通资金信息插入

    :param date:交易日期
    :param north_money:北向资金
    :param south_money:南向资金
    :return:
    """
    hsgt = MoneyFlowHSGT(trade_date=trade_date, north_money=north_money, south_money=south_money)
    try:
        session.add(hsgt)
        session.commit()
    except Exception as err:
        session.rollback()
        raise


def get_hsgt_info():
    """沪港通资金信息查询"""
    return pandas.read_sql(session.query(MoneyFlowHSGT).statement, session.bind)