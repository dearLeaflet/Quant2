#!/usr/bin/python
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
import datetime
import params


def send_mail(msg=None):
    mail_server = 'smtp.163.com'
    port = '25'
    sub = datetime.datetime.now().strftime('%Y%m%d') + ' 量化分析'
    try:
        msg = MIMEMultipart('related')
        msg['From'] = formataddr(["sender", params.SENDER])  # 发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(["receiver", params.RECEIVE])  # 收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = sub
        # 文本信息
        # txt = MIMEText('this is a test mail', 'plain', 'utf-8')
        # msg.attach(txt)

        # 附件信息
        """
        attach = MIMEApplication(open("1.csv").read())    
        attach.add_header('Content-Disposition', 'attachment', filename='1.csv')    
        msg.attach(attach)
        """
        body = """
         <b>小单成交比例、沪深300指数、ETF成交量:</b> 
           <br><img src="cid:image"><br>
           """
        text = MIMEText(body, 'html', 'utf-8')
        # 当日分析结果图
        f = open('./result/' + datetime.datetime.now().strftime('%Y%m%d') + '.png', 'rb')

        pic = MIMEImage(f.read())
        f.close()
        pic.add_header('Content-ID', '<image>')
        msg.attach(text)
        msg.attach(pic)
        server = smtplib.SMTP(mail_server, port)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(params.SENDER, params.PASSWORD)  # 发件人邮箱账号、邮箱密码
        server.sendmail(params.SENDER, params.RECEIVE, msg.as_string())  # 发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()
        print('mail send success')
    except Exception as e:
        print(e)
