"""
数据库操作
"""

import pymysql as mysql
import traceback
import params


def db_connect():
    """
    数据库连接
    """
    db = mysql.connect("localhost", params.DATABASE_USER, params.DATABASE_PASSWORD, params.DATABASE_NAME)
    print("数据库连接成功")
    return db


def db_close():
    """
    断开连接
    :return:
    """


def data_insert(db: object, cursor: object, sql: object) -> object:
    """
    数据插入
    :param db: 数据库连接
    :param cursor: 游标
    :param sql: sql
    :return: 执行结果
    """
    try:
        # 执行sql语句
        cursor.execute(sql)
        # print(sql)
        # 提交到数据库执行
        db.commit()
        return True
    except Exception as err:
        # 如果发生错误则回滚
        db.rollback()
        print("数据插入失败: ", sql)
        traceback.print_exc()
        return False

