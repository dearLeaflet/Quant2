"""
通过抓取网页内容，获取数据
"""
import datetime
import time
from selenium import webdriver

browser = webdriver.Chrome()  # 建浏览器对象
url = 'http://www.sse.com.cn/market/funddata/overview/day/'
browser.get(url)
jsString = "document.getElementById('start_date2').removeAttribute('readonly')"
browser.execute_script(jsString)
input_datetime = browser.find_element_by_id("start_date2")

browser.find_element_by_id("start_date2").clear()
input_datetime.send_keys("2018-07-01")
input_datetime.click()
time.sleep(10)


def get_etf_exchange_amount(date):
    try:
        browser.find_element_by_id("start_date2").clear()
        input_datetime.send_keys(date)
        input_datetime.click()

        browser.find_element_by_id("btnQuery").click()
        time.sleep(10)

        etf_exchange_amount = browser.find_element_by_xpath('//*[@id="tableData_938"]/div[2]/table/tbody/tr[4]/td[4]').text
        trade_date = browser.find_element_by_xpath('//*[@id="tableData_938"]/div[1]/p').text[-10:].replace('-', '')
        return etf_exchange_amount, trade_date
    except Exception as err:
        raise err


def get_etf_daily_exchange_amt():
    """
    获取上海证券交易所2018-07-01以后的ETF基金交易金额
    :return:
    """
    print('获取上海证券交易所ETF基金交易金额')
    exchange_info = []
    trade_date = datetime.date(2018, 7, 1)
    while True:
        if trade_date.strftime('%Y-%m-%d') > datetime.datetime.now().strftime('%Y-%m-%d'):
            break
        try:
            amt, trade_date_str = get_etf_exchange_amount(trade_date.strftime('%Y-%m-%d'))
            exchange_info.append([trade_date_str, amt])
            print(trade_date.strftime('%Y-%m-%d'), '获取成功')
        except Exception as err:
            print(trade_date.strftime('%Y-%m-%d'), '获取失败')
        trade_date = trade_date + datetime.timedelta(days=1)
    browser.close()
    print('获取完成，共{0}条'.format(len(exchange_info)))
    return exchange_info if len(exchange_info) > 0 else None


def get_etf_today_exchange_amt():
    """
    获取上海证券交易所当日ETF基金交易金额
    :return:
    """
    browser.find_element_by_id("start_date2").clear()
    input_datetime.send_keys(datetime.datetime.now().strftime('%Y-%m-%d'))
    input_datetime.click()
    browser.find_element_by_id("btnQuery").click()
    time.sleep(10)

    etf_exchange_amount = browser.find_element_by_xpath('//*[@id="tableData_938"]/div[2]/table/tbody/tr[4]/td[4]').text
    trade_date = browser.find_element_by_xpath('//*[@id="tableData_938"]/div[1]/p').text[-10:].replace('-', '')
    browser.close()
    return [trade_date, etf_exchange_amount]

