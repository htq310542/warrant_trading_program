#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
python version: 2.7
"""

from futuquant import *
import talib as ta
import datetime as dt
import time
import pandas as pd
import numpy as np
import sys
import copy

reload(sys)
sys.setdefaultencoding('utf-8')


class  Moving_avg_line(object):
    """
    A simple moving average strategy
    """
    # API parameter setting
    api_svr_ip = '127.0.0.1'    # 账户登录的牛牛客户端PC的IP, 本机默认为127.0.0.1，云119.29.141.202
    api_svr_port = 11111          # 富途牛牛端口，默认为11111
    unlock_password = "123456"    # 美股和港股交易解锁密码
    trade_env = 1                 # 0真实交易  1仿真交易

    # 均线周期
    time_period5 = 5
    time_period10 = 10
    time_period20 = 20
    time_period30 = 30
    time_period50 = 50
    time_period120 = 120

    cur_price = 0            # 当前恒指当月期货价格
    warrant_qty = 10000      # 牛熊买入固定数量10k
    cur_kline_num = 1000      # get_cur_kline返回k线数量

    # 熊chase buy改单时用的变量
    br_buy_fut_price = 0     # 熊下买单时期货价格
    br_buy_orderid = 0       # 订单号
    br_buy_code = ''         # 买入熊的代码
    br_b_not_dealt_qty = 0   # 买熊未成交的数量
    br_buy_orderside = -1      # 买熊交易方向, 0买入，1卖出，-1初始化
    
    # 牛chase buy改单时用的变量
    bl_buy_fut_price = 0     # 牛下买单时期货价格
    bl_buy_orderid = 0       # 订单号
    bl_buy_code = ''         # 买入牛的代码
    bl_b_not_dealt_qty = 0   # 买牛未成交的数量
    bl_buy_orderside = -1      # 买熊交易方向, 0买入，1卖出，-1初始化
    
    # 熊止盈止损改单时用的变量
    br_sell_fut_price = 0    # 熊下卖单时期货价格
    br_sell_orderid = 0      # 订单号
    br_sell_code = ''        # 卖出熊的代码
    br_s_not_dealt_qty = 0   # 卖牛未成交的数量
    br_sell_orderside = -1      # 买熊交易方向, 0买入，1卖出，-1初始化

    # 牛止盈止损改单时用的变量
    bl_sell_fut_price = 0    # 牛下卖单时期货价格
    bl_sell_orderid = 0      # 订单号
    bl_sell_code = ''        # 卖出牛的代码
    bl_s_not_dealt_qty = 0   # 卖牛未成交的数量
    bl_sell_orderside = -1      # 买熊交易方向, 0买入，1卖出，-1初始化

    nonzero_position_num = 0 # 牛熊持仓不为零的个数
    unfinished_order_num = 0 # 部分成交、等待成交的订单数量

    k_type1 = 'K_1M'
    k_type2 = 'K_5M'

    # data_type
    da_ty_list = ["TICKER", "QUOTE", "ORDER_BOOK", "RT_DATA", "BROKER", "K_1M", "K_5M", 'K_DAY']

    def __init__(self, stock):
        """
        Constructor
        """
        self.stock = stock
        self.quote_ctx, self.trade_ctx = self.context_setting()


    def context_setting(self):
        """
        API trading and quote context setting
        """
        if self.unlock_password == "":
            raise Exception("请先配置交易解锁密码！password：{}".format(self.unlock_password))
        quote_ctx = OpenQuoteContext(host=self.api_svr_ip, port=self.api_svr_port)
        if 'HK.' or 'HK_' in self.stock:
            trade_ctx = OpenHKTradeContext(host=self.api_svr_ip, port=self.api_svr_port)
            if self.trade_env == 0:
                ret_code, ret_data = trade_ctx.unlock_trade(self.unlock_password)
                if ret_code == 0:
                    print('解锁交易成功!')
                else:
                    print("请求交易解锁失败, 请确认解锁密码! password: {}".format(self.unlock_password))
        elif 'US.' in self.stock:
            if self.trade_env != 0:
                raise Exception("美股交易接口不支持仿真环境 trade_env: {}".format(self.trade_env))
            trade_ctx = OpenUSTradeContext(host=self.api_svr_ip, port=self.api_svr_port)
        else:
            raise Exception("stock输入错误 stock: {}".format(self.stock))

        return quote_ctx, trade_ctx


    def sell_all_position(self, time_int):
        """
        每天的11:55和15:55必须平仓
        """
        if (time_int >= 115500 and time_int <= 130000) \
            or (time_int >= 155500 and time_int <= 160000):
            # 先撤掉未成交、部分成交的订单
            ret_code, order_data = self.trade_ctx.order_list_query(statusfilter="1, 2",
                                                                   envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取订单列表,{}".format(order_data))
            for ix, row in order_data.iterrows():
                ret_code, order_data = self.trade_ctx.set_order_status(0, orderid=row['orderid'],
                                                                       envtype=self.trade_env)
                while ret_code == -1:
                    time.sleep(0.2)
                    ret_code, order_data = self.trade_ctx.set_order_status(0, orderid=row['orderid'],
                                                                           envtype=self.trade_env)

                if ret_code != 0:
                    raise Exception("无法撤销{}的订单".format(row['code']))

            # 查询持仓列表
            ret_code, ret_data = self.trade_ctx.position_list_query(stocktype="WARRANT",
                                                                    envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取持仓列表")

            for ix, row in ret_data.iterrows():
                qty = row['qty']
                code = row['code']
                # 以买一价格卖掉持有数量不为零的牛熊
                if int(row['qty'].encode('utf-8')) != 0:         # 直接int()就行了
                    print("盘尾清仓中")
                    ret_code, order_data = self.quote_ctx.subscribe(code, "ORDER_BOOK")
                    if ret_code != 0:
                        raise Exception("无法订阅{}的order_book高频数据".format(code))

                    ret_code, order_data = self.quote_ctx.get_order_book(code)
                    print(order_data['Ask'])
                    print(order_data['Bid'])
                    if ret_code != 0:
                        raise Exception("无法获取摆盘的高频数据")
                    if order_data['Ask'] and order_data['Bid']:  # 或者用 len(order_data['Ask'])
                        time.sleep(0.5)   # 刚订阅返回的数据有时为空， 0.5s后重试
                        ret_code, order_data = self.quote_ctx.get_order_book(code)
                        if ret_code != 0:
                            raise Exception("无法获取摆盘的高频数据")

                    price = order_data['Bid'][0][0]  # 获得买一的价格

                    ret_code, order_data = self.trade_ctx.place_order(price, qty, code,
                                                                      orderside=1,
                                                                      envtype=self.trade_env)
                    while ret_code == -1:      # 交易接口频率限制，重试
                        time.sleep(0.2)
                        ret_code, order_data = self.trade_ctx.place_order(price, qty, code,
                                                                          orderside=1,
                                                                          envtype=self.trade_env)

                    if ret_code != 0:
                        print("下单失败{}".format(order_data))
                    order_data.to_csv("order_list.txt", index=True, sep='\t', mode='a')
                    print("下单成功,{}".format(order_data))

                    # 当未全部成交时，改单再卖，直至卖完
                    while int(order_data['status'][0]) != 3:
                        # 获得买一的价格
                        ret_code, g_order_data = self.quote_ctx.get_order_book(code)
                        if ret_code != 0:
                            print("can't get order book")
                        price = g_order_data['Bid'][0][0]
                        # 撤单
                        ret_code, s_order_data = self.trade_ctx.set_order_status(0,
                                                                                 orderid=order_data['orderid'],
                                                                                 envtype=self.trade_env)
                        while ret_code == -1:
                            time.sleep(0.2)
                            ret_code, s_order_data = self.trade_ctx.set_order_status(0,
                                                                                     orderid=order_data['orderid'],
                                                                                     envtype=self.trade_env)

                        if ret_code != 0:
                            raise Exception("无法撤销{}的订单".format(s_order_data['code']))
                        # 再下单
                        not_dealt_qty = ret_data['qty'][0]
                        ret_code, order_data = self.trade_ctx.place_order(price, not_dealt_qty,
                                                                          code, orderside=1,
                                                                          envtype=self.trade_env)
                        while ret_code == -1:  # 交易接口频率限制，重试
                            time.sleep(0.2)
                            ret_code, order_data = self.trade_ctx.place_order(price, not_dealt_qty,
                                                                              code, orderside=1,
                                                                              envtype=self.trade_env)

                        if ret_code != 0:
                            print("下单失败{}".format(order_data))
                        order_data.to_csv("order_list.txt", index=True, sep='\t', mode='a')
                        print("下单成功,{}".format(order_data))
            print("盘尾清仓完毕")


    def cal_avg_line(self, data):
        """
        计算一分钟10、20、30、50、120均线
        """
        data['avg_line5'] = data['close'].rolling(window=self.time_period5).mean()
        data['avg_line10'] = data['close'].rolling(window=self.time_period10).mean()
        data['avg_line20'] = data['close'].rolling(window=self.time_period20).mean()
        data['avg_line30'] = data['close'].rolling(window=self.time_period30).mean()
        data['avg_line50'] = data['close'].rolling(window=self.time_period50).mean()
        data['avg_line120'] = data['close'].rolling(window=self.time_period120).mean()


    def cal_avg_line_macd(self, data):
        """
        计算10、20、50均线值的macd, 用来判断短期盘整和趋势
        公式：avg_dif = (avg_line10 + avg_line20)/2 - avg_line50
        avg_dea = ema(avg_diff, 9)
        avg_macd =(avg_dif - avg_dea)*3
        ma_macd = ma(abs(avg_macd), 8)
        avg_dif 判断标准：（-15，15）10、20均线偏离50均线小，else偏离大
        ma_macd 判断标准： （0，15）偏离距离波动小， else波动大
        """
        data['avg_diff'] = (2.0 * data['avg_line10'] + data['avg_line20'])/3.0 - data['avg_line50']
        data['avg_dea'] = ta.EMA(data['close'].values, timeperiod = 9)
        data['avg_macd'] = (data['avg_diff'] -data['avg_dea'])*3.0
        data['ma_macd'] = ta.MA(np.abs(data['avg_macd'].values), timeperiod = 8)


    def cal_avg_line_macd2(self, data):
        """
        计算10、20、30、50、120均线值的macd, 用来判断中期盘整和趋势
        公式：avg_dif2 = (avg_line10 + avg_line20 + avg_line30 + avg_line50)/4 - avg_line120
        avg_dea2 = ema(avg_diff, 9)
        avg_macd2 =(avg_dif - avg_dea)*5
        ma_macd2 = ma(abs(avg_macd), 8)
        avg_dif 判断标准：（-20，20）20、50均线偏离120均线小，else偏离大
        ma_macd 判断标准： （0，20）偏离距离波动小， else波动大
        """
        data['avg_diff2'] = (data['avg_line10'] + data['avg_line20']  + data['avg_line30']
                             + data['avg_line50'])/4.0 - data['avg_line50']
        data['avg_dea2'] = ta.EMA(data['close'].values, timeperiod = 9)
        data['avg_macd2'] = (data['avg_diff2'] -data['avg_dea2'])*5.0
        data['ma_macd2'] = ta.MA(np.abs(data['avg_macd2'].values), timeperiod = 8)


    def set_green_red(self, data):
        """
        红绿k线
        """
        i = 50
        data['green_red'] = 'N'

        while i < len(data):
            if data.loc[i, 'close'] > data.loc[i, 'open']:
                data.loc[i, 'green_red'] = 'red'
            else:
                data.loc[i, 'green_red'] = 'green'
            i += 1


    def set_down_up_in(self, data):
        """
        根据当前k线与上一k线的高低点的比较，把k线分为三类：down up in
        """
        i = 51
        data['down_up_in'] = 'N'

        while i < len(data):   # len(DataFrame) 多少行
            if data.loc[i, 'high'] < data.loc[i-1, 'high'] and data.loc[i, 'low'] < data.loc[i-1, 'low']:
                data.loc[i, 'down_up_in'] = 'down'
            elif data.loc[i, 'high'] > data.loc[i-1, 'high'] and data.loc[i, 'low'] > data.loc[i-1, 'low']:
                data.loc[i, 'down_up_in'] = 'up'
            else:
                data.loc[i, 'down_up_in'] = 'in'    # 包含或者被包含, 处理k线
                # if data.loc[i-1, 'down_up_in'] == 'down':
                #     data.loc[i, 'high'] = min(data.loc[i-1, 'high'], data.loc[i, 'high'])
                #     data.loc[i, 'low'] = min(data.loc[i-1, 'low'], data.loc[i, 'low'])
                # elif data.loc[i-1, 'down_up_in'] == 'up':
                #     data.loc[i, 'high'] = max(data.loc[i-1, 'high'], data.loc[i, 'high'])
                #     data.loc[i, 'low'] = max(data.loc[i-1, 'low'], data.loc[i, 'low'])
                # else:
                #     pass
            i += 1


    def set_bottom_peak(self, data):
        """
        判断底和顶
        """
        i = 51
        data['bottom_peak'] = 'N'

        while i < len(data)-1:
            if data.loc[i, 'high'] < min(data.loc[i-1, 'high'], data.loc[i+1, 'high']) \
                    and data.loc[i, 'low'] < min(data.loc[i-1, 'low'], data.loc[i+1, 'low']):
                data.loc[i, 'bottom_peak'] = 'bottom'

            if data.loc[i, 'high'] > max(data.loc[i-1, 'high'], data.loc[i+1, 'high']) \
                    and data.loc[i, 'low'] > max(data.loc[i-1, 'low'], data.loc[i+1, 'low']):
                data.loc[i, 'bottom_peak'] = 'peak'
            i += 1


    def avg_state_rank(self, data):
        """
        根据收盘价和一分钟10,20日均线值的大小，来评价走势的强弱(data['rank']=1,2,3,4,5,6刚好一个涨跌循环)
        1   C>MA20 AND MA20>MA10    yellow 买牛
        2   C>MA10 AND MA10>=MA20   red    买牛，趋势上涨
        3   MA10>=C AND C>MA20      yellow 买牛（震荡可买熊，但趋势中陷阱多）
        4   MA10>MA20 AND MA20>=C   blue   买熊
        5   MA20>=MA10 AND MA10>=C  green  买熊，趋势下跌
        6   MA20>=C AND C>MA10      blue   买熊（震荡可买牛，但趋势中陷阱多）
        """
        i = 50
        data['rank'] = 0

        while i < len(data):
            if data.loc[i, 'close'] > data.loc[i, 'avg_line50']:
                if data.loc[i, 'avg_line50'] >= data.loc[i, 'avg_line20']:
                    data.loc[i, 'rank'] = 1
                elif data.loc[i, 'avg_line20'] >= data.loc[i, 'close']:
                    data.loc[i, 'rank'] = 3
                else:
                    data.loc[i, 'rank'] = 2
            else:
                if data.loc[i, 'avg_line20'] > data.loc[i, 'avg_line50']:
                    data.loc[i, 'rank'] = 4
                elif data.loc[i, 'close'] > data.loc[i, 'avg_line20']:
                    data.loc[i, 'rank'] = 6
                else:
                    data.loc[i, 'rank'] = 5
            i +=1

        # # 更新当前k线的rank值
        # if self.cur_price > data.loc[len(data)-1, 'avg_line50']:
        #     if data.loc[len(data)-1, 'avg_line50'] >= data.loc[len(data)-1, 'avg_line20']:
        #         data.loc[len(data)-1, 'rank'] = 1
        #     elif data.loc[len(data)-1, 'avg_line20'] >= self.cur_price:
        #         data.loc[len(data)-1, 'rank'] = 3
        #     else:
        #         data.loc[len(data)-1, 'rank'] = 2
        # else:
        #     if data.loc[len(data)-1, 'avg_line20'] > data.loc[len(data)-1, 'avg_line50']:
        #         data.loc[len(data)-1, 'rank'] = 4
        #     elif self.cur_price > data.loc[len(data)-1, 'avg_line20']:
        #         data.loc[len(data)-1, 'rank'] = 6
        #     else:
        #         data.loc[len(data)-1, 'rank'] = 5


    def set_buy_trigger_range(self, data):
        """
        设置买牛熊触发值的范围(入场)
        """
        # delta_mean = np.round(data['delta'][-100:].mean())
        delta_mean = np.round(13.0)
        range = 3.0
        data['delta_buy'] = np.where(data['delta'] <= delta_mean - range, delta_mean - range, data['delta'])
        data['delta_buy'] = np.where(data['delta'] >= delta_mean + range, delta_mean + range, data['delta_buy'])


    def buy_trigger_signal(self, data):
        """
        设置触发买牛熊的信号（入场）
        """
        i = 51
        data['bull_b_tri'] = 'N'
        data['bull_b_price'] = 0
        data['bear_b_tri'] = 'N'
        data['bear_b_price'] = 0

        while i < len(data)-1:
            if data.loc[i, 'high'] > data.loc[i - 1, 'low'] + data.loc[i - 1, 'delta_buy'] \
                    and (data.iloc[i]['rank'] == 2 or data.iloc[i]['rank'] == 1 or data.iloc[i]['rank'] == 3)\
                    and data.loc[i - 1, 'close'] - data.loc[i - 1, 'open'] > -15 \
                    and data.loc[i - 1, 'close'] - data.loc[i - 1, 'low'] > -20:
                data.loc[i, 'bull_b_tri'] = 'bl_b'
                data.loc[i, 'bull_b_price'] = data.loc[i - 1, 'low'] + data.loc[i - 1, 'delta_buy']
            if data.loc[i, 'low'] < data.loc[i - 1, 'high'] - data.loc[i - 1, 'delta_buy'] \
                    and (data.iloc[i]['rank'] == 5 or data.iloc[i]['rank'] == 4 or data.iloc[i]['rank'] == 6)\
                    and data.loc[i - 1, 'close'] - data.loc[i - 1, 'open'] < 15 \
                    and data.loc[i - 1, 'close'] - data.loc[i - 1, 'low'] < 20:
                data.loc[i, 'bear_b_tri'] = 'br_b'
                data.loc[i, 'bear_b_price'] = data.loc[i - 1, 'high'] - data.loc[i - 1, 'delta_buy']
            i += 1

        # 更新当前k线信号
        if self.cur_price > data.loc[len(data) - 2, 'low'] + data.loc[len(data) - 2, 'delta_buy'] \
                and (data.loc[len(data) - 1, 'rank'] == 2 or data.loc[len(data) - 1, 'rank'] == 1
                      or data.loc[len(data) - 1, 'rank'] == 3) \
                and data.loc[len(data) - 2, 'close'] - data.loc[len(data) - 2, 'open'] > -15 \
                and data.loc[len(data) - 2, 'close'] - data.loc[len(data) - 2, 'low'] > -20:
            data.loc[len(data) - 1, 'bull_b_tri'] = 'bl_b'
            data.loc[len(data) - 1, 'bull_b_price'] = data.loc[len(data) - 2, 'low'] \
                                                      + data.loc[len(data) - 2, 'delta_buy']

        if self.cur_price < data.loc[len(data) - 2, 'high'] - data.loc[len(data) - 2, 'delta_buy'] \
                and (data.loc[len(data) - 1, 'rank'] == 5 or data.loc[len(data) - 1, 'rank'] == 4
                     or data.loc[len(data) - 1, 'rank'] == 6)\
                and data.loc[len(data) - 2, 'close'] - data.loc[len(data) - 2, 'open'] < 15 \
                and data.loc[len(data) - 2, 'close'] - data.loc[len(data) - 2, 'low'] < 20:
            data.loc[len(data) - 1, 'bear_b_tri'] = 'br_b'
            data.loc[len(data) - 1, 'bear_b_price'] = data.loc[len(data) - 2, 'high'] \
                                                      - data.loc[len(data) - 2]['delta_buy']


    def set_sell_trigger_range(self, data):
        """
        设置卖牛熊触发值的范围(止损)
        """
        # delta_mean = np.round(data['delta'][-100:].mean())
        delta_mean = np.round(20.0)
        range = 5.0
        data['delta_sell'] = np.where(data['delta'] <= delta_mean - range, delta_mean - range, data['delta'])
        data['delta_sell'] = np.where(data['delta'] >= delta_mean + range, delta_mean + range, data['delta_sell'])


    def sell_trigger_signal(self, data):
        """
        设置触发卖牛熊的信号（止损）
        """
        i = 51
        data['bull_s_tri'] = 'N'
        data['bull_s_price'] = 0
        data['bear_s_tri'] = 'N'
        data['bear_s_price'] = 0

        while i < len(data) - 1:
            if data.loc[i, 'high'] > data.loc[i - 1, 'low'] + data.loc[i - 1, 'delta_sell']:
                data.loc[i, 'bear_s_tri'] = 'br_s'
                data.loc[i, 'bear_s_price'] = data.loc[i - 1, 'low'] + data.loc[i - 1, 'delta_sell']
            if data.loc[i, 'low'] < data.loc[i - 1, 'high'] - data.loc[i - 1, 'delta_sell']:
                data.loc[i, 'bull_s_tri'] = 'bl_s'
                data.loc[i, 'bull_s_price'] = data.loc[i - 1, 'high'] - data.loc[i - 1, 'delta_sell']
            i += 1

        # 更新当前k线信号
        if self.cur_price > data.loc[len(data) - 2, 'low'] + data.loc[len(data) - 2, 'delta_sell']:
            data.loc[len(data) - 1, 'bear_s_tri'] = 'br_s'
            data.loc[len(data) - 1, 'bear_s_price'] = data.loc[len(data) - 2, 'low'] \
                                                      + data.loc[len(data) - 2, 'delta_sell']

        if self.cur_price < data.loc[len(data) - 2, 'high'] - data.loc[len(data) - 2, 'delta_sell']:
            data.loc[len(data) - 1, 'bull_s_tri'] = 'bl_s'
            data.loc[len(data) - 1, 'bull_s_price'] = data.loc[len(data) - 2, 'high'] \
                                                      - data.loc[len(data) - 2, 'delta_sell']


    def compare_with_avg_line5(self, data):
        """
        每一根一分钟k线的最低价、最高价和中间价与一分钟k线的5日均线比较
        用来评估近几分钟走势的强弱（止盈，排队卖）
        data['high'] 跟15比较，上穿以卖一价下卖单（牛）
        data['middle']
        data['close']
        data['low']  跟-15比较，上穿以卖一价下卖单（熊）
        """
        data['middle'] = (data['high'] + data['low'])/2.0

        data['delta_high5'] = data['high'] - data['avg_line5']
        data['delta_close5'] = data['close'] - data['avg_line5']
        data['delta_middle5'] = data['middle'] - data['avg_line5']
        data['delta_low5'] = data['low'] - data['avg_line5']

        data['delta_high10'] = data['high'] - data['avg_line10']
        data['delta_close10'] = data['close'] - data['avg_line10']
        data['delta_middle10'] = data['middle'] - data['avg_line10']
        data['delta_low10'] = data['low'] - data['avg_line10']


    def warrant_pool1(self):
        """
        这个函数负责初选牛熊,减少筛选牛熊需要的时间，每天09:30:30、13:00:30和运行程序各跑一次
        """
        # 选出恒指的牛熊，发行商为法巴、摩通、法兴
        t0 = time.clock()

        bear_list = []
        bull_list = []
        ret_code, ret_data = self.quote_ctx.get_stock_basicinfo("HK", stock_type='WARRANT')
        if ret_code == 0:
            for ix, row in ret_data.iterrows():
                # if row['name'].find(u"恒指法巴") >= 0 or row['name'].find(u"恒指摩通") >= 0 \
                #         or row['name'].find(u"恒指法兴") >= 0:
                if row['name'].find(u"恒指法巴") >= 0 or row['name'].find(u"恒指摩通") >= 0:
                    if row['stock_child_type'] == "BULL":
                        bull_list.append(row['code'])
                    if row['stock_child_type'] == "BEAR":
                        bear_list.append(row['code'])

        warrant_list = bear_list + bull_list
        if len(warrant_list) == 0:
            print("Error :can not get warrant info... warrants_list:{}".format(warrant_list))

        bear_pool1 = []
        bull_pool1 = []
        for i in range(0, len(bear_list), 200):
            j = min(i + 200, len(bear_list))
            print("bear_list[{}:{}]".format(i, j))
            ret_code, ret_data = self.quote_ctx.get_market_snapshot(bear_list[i : j])
            if ret_code == 0:
                for ix, row in ret_data.iterrows():
                    if row['wrt_conversion_ratio'] == 10000 and row['last_price'] >= 0.001 \
                            and row['suspension'] == False:
                        bear_pool1.append(row['code'])
            else:
                print('市场快照数据获取异常, 正在重试中... {}'.format(ret_data))
            time.sleep(5)

        for i in range(0, len(bull_list), 200):
            j = min(i + 200, len(bull_list))
            print("bull_list[{}:{}]".format(i, j))
            ret_code, ret_data = self.quote_ctx.get_market_snapshot(bull_list[i : j])
            if ret_code == 0:
                for ix, row in ret_data.iterrows():
                    if row['wrt_conversion_ratio'] == 10000 and row['last_price'] >= 0.001 \
                            and row['suspension'] == False:
                        bull_pool1.append(row['code'])
            else:
                print('市场快照数据获取异常, 正在重试中... {}'.format(ret_data))
            time.sleep(5)

        print("warrant pool1: %d, bear %d, bull %d" % (len(bear_pool1 + bull_pool1),len(bear_pool1),len(bull_pool1)))
        print("用时：%5f \n" % (time.clock()-t0))

        return bear_pool1, bull_pool1


    def warrant_pool2(self, bear_pool1, bull_pool1):
        """
        筛选换股比率为10000、街货比小于60%的恒指牛熊、最新价格小于0.180, 记录下它们的收回价
        """
        t0 = time.clock()
        bear_pool2 = []
        bear_rec_price = []
        bull_pool2 = []
        bull_rec_price = []
        for i in range(0, len(bear_pool1), 200):
            j = min(i + 200, len(bear_pool1))
            print("bear_pool1[{}:{}]".format(i, j))
            ret_code, ret_data = self.quote_ctx.get_market_snapshot(bear_pool1[i : j])
            if ret_code == 0:
                for ix, row in ret_data.iterrows():
                    if row['wrt_street_ratio'] <= 50 and row['last_price'] <= 0.180 and \
                                    row['last_price'] >= 0.001 and row['suspension'] == False:
                        bear_pool2.append(row['code'])
                        bear_rec_price.append(row['wrt_recovery_price'])
            else:
                print('市场快照数据获取异常, 正在重试中... {}'.format(ret_data))
            time.sleep(5)

        for i in range(0, len(bull_pool1), 200):
            j = min(i + 200, len(bull_pool1))
            print("bull_pool1[{}:{}]".format(i, j))
            ret_code, ret_data = self.quote_ctx.get_market_snapshot(bull_pool1[i : j])  # i+200 对的
            if ret_code == 0:
                for ix, row in ret_data.iterrows():
                    if row['wrt_street_ratio'] <= 50 and row['last_price'] <= 0.180 and \
                                    row['last_price'] >= 0.001 and row['suspension'] == False:
                        bull_pool2.append(row['code'])
                        bull_rec_price.append(row['wrt_recovery_price'])
            else:
                print('市场快照数据获取异常, 正在重试中... {}'.format(ret_data))
            time.sleep(5)

        # 订阅牛熊的order_book、quote高频数据（一次订阅就够了）
        warrant_pool2 = bear_pool2 + bull_pool2
        i = 0
        for code in warrant_pool2:
            for data_type in ["ORDER_BOOK", "QUOTE"]:
                ret_code, ret_data = self.quote_ctx.subscribe(code, data_type)
                if ret_code != 0:
                    raise Exception("无法订阅{}的order_book、quote高频数据; ret_code={}"
                                    .format(code, ret_code))
                else:
                    i += 1
        if i == 2 * len(warrant_pool2):
            print("成功订阅 warrant_pool2 的stock_quote高频数据\n")

        print("warrant pool2: %d, bear %d, bull %d" % (len(bear_pool2 + bull_pool2),len(bear_pool2),len(bull_pool2)))
        print("用时：%5f \n" % (time.clock()-t0))

        return bear_pool2, bear_rec_price, bull_pool2, bull_rec_price


    def update_warrant_pool(self, bear_pool2, bear_rec_price, bull_pool2, bull_rec_price):
        """
        实时更新牛熊池, 恒指当月期货价格距离熊收回价小于-0.75%，距离牛收回价大于0.75%
        最新价格0.040---0.120, 0.040---[0.060 + i/100.0 for i in range(0,10)]
        """
        # 根据收回价筛选牛熊
        bear_temp = copy.deepcopy(bear_pool2 )  # 使用‘=’赋值，是引用赋值，更改一个，另一个也会改变
        bull_temp = copy.deepcopy(bull_pool2)
        for i in range(0, len(bear_rec_price)):
            if (self.cur_price - bear_rec_price[i])/self.cur_price*100.0 >= -0.75:
                bear_temp.remove(bear_pool2[i])
        for i in range(0, len(bull_rec_price)):
            if (self.cur_price - bull_rec_price[i])/self.cur_price*100.0 <= 0.75:
                bull_temp.remove(bull_pool2[i])

        # 根据价格筛选熊
        data = pd.DataFrame(columns = [u'code', u'data_date', u'data_time', u'last_price', u'open_price',
                                       u'high_price', u'low_price', u'prev_close_price', u'volume',
                                       u'turnover', u'turnover_rate', u'amplitude', u'suspension',
                                       u'listing_date', u'price_spread'])
        for i in range(0, len(bear_temp), 50):
            j = min(i + 50, len(bear_temp))
            ret_code, ret_data = self.quote_ctx.get_stock_quote(bear_temp[i : j])
            if ret_code != 0:
                raise Exception("无法获取 bear_pool2[{}:{}] 的stock_quote高频数据".format(i, j))
            data = data.append(ret_data, ignore_index=True)
        ret_data = data

        price_limit = [0.06 + i/100.0 for i in range(0,10)]
        bear_update = pd.DataFrame()
        for price_l in price_limit:
            bear_update_temp = []
            for ix, row in ret_data.iterrows():
                if row['last_price'] >= 0.040 and row['last_price'] <= price_l:
                    row = dict(row)
                    bear_update_temp.append(row)
            bear_update = pd.DataFrame(bear_update_temp)
            if len(bear_update) < 5:
                continue
            else:
                break
        print("bear_update['turnover'][0]:{}".format(bear_update['turnover'][0]))
        bear_update = bear_update.sort_values(by = 'turnover', ascending = False)\
                           .reset_index(drop=True)
        print("bear_update['turnover'][0]:{}".format(bear_update['turnover'][0]))

        bear_candidate_list = bear_update['code'].tolist()
        # bear_candidate = bear_update['code'][bear_update['turnover'].idxmax()]

        # 根据价格筛选牛
        data = pd.DataFrame(columns = [u'code', u'data_date', u'data_time', u'last_price', u'open_price',
                                       u'high_price', u'low_price', u'prev_close_price', u'volume',
                                       u'turnover', u'turnover_rate', u'amplitude', u'suspension',
                                       u'listing_date', u'price_spread'])
        for i in range(0, len(bull_temp), 50):
            j = min(i + 50, len(bull_temp))
            ret_code, ret_data = self.quote_ctx.get_stock_quote(bull_temp[i:j])
            if ret_code != 0:
                raise Exception("无法获取 bull_pool2[{}:{}] 的stock_quote高频数据".format(i, j))
            # else:
            #     print("获取 bull_pool2[{}:{}] 的stock_quote高频数据".format(i, j))
            data = data.append(ret_data, ignore_index=True)
        ret_data = data

        price_limit = [0.06 + i/100.0 for i in range(0,10)]
        bull_update = pd.DataFrame()
        for price_l in price_limit:
            bull_update_temp = []
            for ix, row in ret_data.iterrows():
                if row['last_price'] >= 0.040 and row['last_price'] <= price_l:
                    row = dict(row)
                    bull_update_temp.append(row)
            bull_update = pd.DataFrame(bull_update_temp)
            if len(bull_update) < 5:
                continue
            else:
                break

        bull_update = bull_update.sort_values(by = 'turnover', ascending = False)\
                           .reset_index(drop=True)
        bull_candidate_list = bull_update['code'].tolist()
        # bull_candidate = bull_update['code'][bull_update['turnover'].idxmax()]

        # print("pool_update:%d, bear %d, bull %d" % (len(bear_update) + len(bull_update),
        #                                             len(bear_update),len(bull_update)))

        return bear_candidate_list, bull_candidate_list


    def update_position_order_num(self, data):
        # 查看牛熊持仓情况
        ret_code, ret_data = self.trade_ctx.position_list_query(stocktype="WARRANT",
                                                                envtype=self.trade_env)
        if ret_code != 0:
            raise Exception("无法获取持仓列表")

        # 牛熊持仓数量不为零的个数
        num = 0
        for ix, row in ret_data.iterrows():
            if int(row['qty'].encode('utf-8')) != 0:
                num += 1
        self.nonzero_position_num = num

        # 查看未成交、部分成交的订单数
        ret_code, order_data = self.trade_ctx.order_list_query(statusfilter="1, 2",
                                                               envtype=self.trade_env)
        if ret_code != 0:
            raise Exception("无法获取订单列表,{}".format(order_data))
        self.unfinished_order_num = len(order_data)


    def market_in(self, data, bear_candidate_list, bull_candidate_list):
        """
        当信号出现时，以买一价或者中间价排队买入
        """
        # f = open("market_in.txt", "a+")

        # 买熊条件, (当出现同时buy和sell时不买data.iloc[-1]['bear_s_tri']!='br_s')
        br_buy_con1 = data.iloc[-1]['bear_b_tri']=='br_b' and data.iloc[-1]['bear_s_tri']!='br_s' \
                      and ((data.iloc[-1]['rank'] == 5 or data.iloc[-1]['rank'] == 4
                            or data.iloc[-1]['rank'] == 6)
                      and data.iloc[-2]['bear_b_tri'] != 'br_b')
        br_buy_con2 = data.iloc[-1]['bear_b_tri']=='br_b' and data.iloc[-1]['bear_s_tri']!='br_s' \
                      and (data.iloc[-1]['rank'] == 5 and data.iloc[-2]['bear_s_tri'] != 'br_s'
                           and data.iloc[-1]['delta_high5'] < 2)
        if br_buy_con1 or br_buy_con2:
        # if (data.iloc[-1]['rank'] == 5 or data.iloc[-1]['rank'] == 4 or data.iloc[-1]['rank'] == 6) \
        #         and data.iloc[-1]['bear_b_tri']=='br_b':

            print("买熊中")
            # f.write("买熊中\n\n")

            # 选择买一卖一差价小于三格的熊下单，先以买一价或者中间价排队买入
            for i in range(0, len(bear_candidate_list)):
                # 获得买一卖一的价格
                bear_candidate = bear_candidate_list[i]
                ret_code, g_order_data = self.quote_ctx.get_order_book(bear_candidate)
                if ret_code != 0:
                    raise Exception("无法获取摆盘数据:{}".format(g_order_data))
                print(g_order_data)
                ask_price1 = g_order_data['Ask'][0][0]  # 卖一的价格
                bid_price1 = g_order_data['Bid'][0][0]  # 买一的价格

                # 获得price_spread
                ret_code, quote_data = self.quote_ctx.get_stock_quote(bear_candidate)
                if ret_code != 0:
                    raise Exception("无法获取{}的stock_quote高频数据".format(bear_candidate))
                times = int(round((ask_price1 - bid_price1),4)/quote_data['price_spread'][0])
                # 确定下单价格
                if times == 1 or times == 2:
                    if times == 1:
                        order_price = bid_price1                   # 买一的价格
                    else:     # times == 2
                        order_price = bid_price1 + quote_data['price_spread'][0]  # 中间价
                    print("bid_price1:{}, times:{}, order_price:{}, ask_price1:{}".format(bid_price1,
                                                                                          times,
                                                                                          order_price,
                                                                                          ask_price1))
                    ret_code, order_data = self.trade_ctx.place_order(order_price, self.warrant_qty,
                                                                      bear_candidate, orderside=0,
                                                                      envtype=self.trade_env)
                    while ret_code == -1:      # 交易接口频率限制，重试
                        time.sleep(0.2)
                        ret_code, order_data = self.trade_ctx.place_order(order_price, self.warrant_qty,
                                                                          bear_candidate, orderside=0,
                                                                          envtype=self.trade_env)

                    if ret_code != 0:
                        print("下单失败{}".format(order_data))

                    self.br_buy_fut_price = self.cur_price
                    self.br_buy_orderid = int(order_data['orderid'][0])
                    self.br_buy_code = bear_candidate
                    self.br_buy_orderside = int(order_data['order_side'][0])
                    self.br_b_not_dealt_qty = int(order_data['qty'][0]) \
                                              - int(order_data['dealt_qty'][0])

                    print(order_data)
                    order_data.to_csv("market_in.txt", index=True, sep='\t', mode='a')
                    order_data.to_csv("order_list.txt", index=True, sep='\t', mode='a')

                    if order_data['status'][0] == 3:
                        print("买熊成功,{}".format(order_data))
                        self.br_buy_fut_price = 0
                        self.br_buy_orderid = 0
                        self.br_buy_code = ''
                        self.br_buy_orderside = -1
                        self.br_b_not_dealt_qty = 0
                    break

        # 买牛条件
        bl_buy_con1 = data.iloc[-1]['bull_b_tri']=='bl_b' and data.iloc[-1]['bull_s_tri']!='bl_s' \
                      and ((data.iloc[-1]['rank'] == 2 or data.iloc[-1]['rank'] == 1
                            or data.iloc[-1]['rank'] == 3)
                      and data.iloc[-2]['bull_b_tri'] != 'bl_b')
        bl_buy_con2 = data.iloc[-1]['bull_b_tri']=='bl_b' and data.iloc[-1]['bull_s_tri']!='bl_s' \
                      and (data.iloc[-1]['rank'] == 2 and data.iloc[-2]['bull_s_tri'] != 'bl_s'
                           and data.iloc[-1]['delta_low5'] > -2)
        if bl_buy_con1 or bl_buy_con2:
        # if (data.iloc[-1]['rank'] == 2 or data.iloc[-1]['rank'] == 1 or data.iloc[-1]['rank'] == 3) \
        #     and data.iloc[-1]['bull_b_tri']=='bl_b':

            print("买牛中")
            # f.write("买牛中\n\n")

            # 选择买一卖一差价小于三格的牛下单，先以买一价或者中间价排队买入
            for i in range(0, len(bull_candidate_list)):
                # 获得买一卖一的价格
                bull_candidate = bull_candidate_list[i]
                ret_code, g_order_data = self.quote_ctx.get_order_book(bull_candidate)
                if ret_code != 0:
                    raise Exception("无法获取摆盘数据:{}; ret_code={}".format(g_order_data, ret_code))
                print(g_order_data)
                ask_price1 = g_order_data['Ask'][0][0]  # 卖一的价格
                bid_price1 = g_order_data['Bid'][0][0]  # 买一的价格

                # 获得price_spread
                ret_code, quote_data = self.quote_ctx.get_stock_quote(bull_candidate)
                if ret_code != 0:
                    raise Exception("无法获取{}的stock_quote高频数据".format(bull_candidate))
                times = int(round((ask_price1 - bid_price1),4)/quote_data['price_spread'][0])
                # 确定下单价格
                if times == 1 or times == 2:
                    if times == 1:
                        order_price = bid_price1             # 买一的价格
                    else:                        # times == 2
                        order_price = bid_price1 + quote_data['price_spread'][0]  # 中间价
                    print("bid_price1:{}, times:{}, order_price:{}, ask_price1:{}".format(bid_price1,
                                                                                          times,
                                                                                          order_price,
                                                                                          ask_price1))
                    ret_code, order_data = self.trade_ctx.place_order(order_price, self.warrant_qty,
                                                                      bull_candidate, orderside=0,
                                                                      envtype=self.trade_env)
                    while ret_code == -1:      # 交易接口频率限制，重试
                        time.sleep(0.2)
                        ret_code, order_data = self.trade_ctx.place_order(order_price, self.warrant_qty,
                                                                          bull_candidate, orderside=0,
                                                                          envtype=self.trade_env)

                    if ret_code != 0:
                        print("下单失败{}".format(order_data))

                    self.bl_buy_fut_price = self.cur_price
                    self.bl_buy_orderid = int(order_data['orderid'][0])
                    self.bl_buy_code = bull_candidate
                    self.bl_buy_orderside = int(order_data['order_side'][0])
                    self.bl_b_not_dealt_qty = int(order_data['qty'][0]) \
                                              - int(order_data['dealt_qty'][0])

                    print(order_data)
                    order_data.to_csv("market_in.txt", index=True, sep='\t', mode='a')
                    order_data.to_csv("order_list.txt", index=True, sep='\t', mode='a')

                    if order_data['status'][0] == 3:
                        print("买牛成功,{}".format(order_data))
                        self.bl_buy_fut_price = 0
                        self.bl_buy_orderid = 0
                        self.bl_buy_code = ''
                        self.bl_buy_orderside = -1
                        self.bl_b_not_dealt_qty = 0
                    break
        # f.close()


    def chase_buy_change_order(self, data):
        """
        当排队买入未全部成交且往盈利方向变化超4格且小于10格时，立马以卖一价或者中间价买入
        """
        # f = open("chase_buy.txt", "a+")

        # 熊chase buy改单
        if self.br_buy_fut_price > 0 and self.cur_price - self.br_buy_fut_price < -3.0 \
                and self.cur_price - self.br_buy_fut_price > -8.0 :

            print("熊追买改单中")
            # f.write("熊追买改单中\n\n")

            # 获取卖一的价格
            ret_code, g_order_data = self.quote_ctx.get_order_book(self.br_buy_code)
            if ret_code != 0:
                raise Exception("无法获取摆盘数据:{}; ret_code={}".format(g_order_data, ret_code))
            ask_price1 = g_order_data['Ask'][0][0]
            bid_price1 = g_order_data['Bid'][0][0]

            # 获得price_spread
            ret_code, quote_data = self.quote_ctx.get_stock_quote(self.br_buy_code)
            if ret_code != 0:
                raise Exception("无法获取{}的stock_quote高频数据".format(self.br_buy_code))
            times = int(round((ask_price1 - bid_price1), 4) / quote_data['price_spread'][0])
            # 确定改单价格
            if times == 1:
                order_price = ask_price1  # 卖一的价格
            else:
                order_price = bid_price1 + quote_data['price_spread'][0]  # 中间价
            print("bid_price1:{}, times:{}, order_price:{}, ask_price1:{}".format(bid_price1,
                                                                                  times,
                                                                                  order_price,
                                                                                  ask_price1))
            # 获取未成交数量
            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_buy_orderid,
                                                                   statusfilter="1, 2",
                                                                   envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取订单列表,{}".format(order_data))

            print(order_data)
            # order_data.to_csv("chase_buy.txt", index=True, sep='\t', mode='a')

            if order_data.empty:
                print("熊排队买入{}成功".format(self.br_buy_code))
                self.br_buy_fut_price = 0
                self.br_buy_orderid = 0
                self.br_buy_code = ''
                self.br_buy_orderside = -1
                self.br_b_not_dealt_qty = 0

                # f.write("熊排队买入成功\n\n")

            elif int(1000.0 * order_price) != int(1000.0 * order_data['price'][0]):
                self.br_b_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                # 改单
                ret_code, change_data = self.trade_ctx.change_order(price=order_price,
                                                                    qty=self.br_b_not_dealt_qty,
                                                                    orderid=self.br_buy_orderid,
                                                                    envtype=1)
                if ret_code != 0:
                    print("{}熊追单买入, 改单失败:{}".format(self.br_buy_code, change_data))
                time.sleep(0.5)
                # 获取未成交数量
                ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_buy_orderid,
                                                                       statusfilter="1, 2",
                                                                       envtype=self.trade_env)

                # order_data.to_csv("chase_buy.txt", index=True, sep='\t', mode='a')

                if ret_code != 0:
                    raise Exception("无法获取订单列表,{}".format(order_data))
                else:
                    if order_data.empty:
                        print("熊追单买入{}成功".format(self.br_buy_code))
                        self.br_buy_fut_price = 0
                        self.br_buy_orderid = 0
                        self.br_buy_code = ''
                        self.br_buy_orderside = -1
                        self.br_b_not_dealt_qty = 0

                        # f.write("熊追单买入成功\n\n")

                    else:
                        self.br_b_not_dealt_qty = int(order_data['qty'][0]) \
                                                  - int(order_data['dealt_qty'][0])
            else:
                pass

        print('br_b_not_dealt_qty:%s \t br_buy_orderside:%s' % (self.br_b_not_dealt_qty,
                self.br_buy_orderside))
        # 熊买不上就撤单不买了
        if self.br_buy_fut_price > 0 and (self.cur_price - self.br_buy_fut_price <= -8.0
                                          or self.cur_price - self.br_buy_fut_price >= 8.0):
            # 获取未成交数量
            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_buy_orderid,
                                                                   statusfilter="1, 2",
                                                                   envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取订单列表,{}".format(order_data))
            if order_data.empty:
                print("熊追单买入{}成功".format(self.br_buy_code))
                self.br_buy_fut_price = 0
                self.br_buy_orderid = 0
                self.br_buy_code = ''
                self.br_buy_orderside = -1
                self.br_b_not_dealt_qty = 0
            else:
                self.br_b_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                if self.br_b_not_dealt_qty > 0:
                    ret_code, order_data = self.trade_ctx.set_order_status(status=0,
                                                                           orderid=self.br_buy_orderid,
                                                                           envtype=1)
                    while ret_code == -1:
                        time.sleep(0.2)
                        ret_code, order_data = self.trade_ctx.set_order_status(status=0,
                                                                               orderid=self.br_buy_orderid,
                                                                               envtype=1)

                    print("熊买不上，撤单")
                    # f.write("熊买不上，撤单\n\n")
                    print(order_data)
                    # order_data.to_csv("chase_buy.txt", index=True, sep='\t', mode='a')

                    if ret_code != 0:
                        raise Exception("无法撤销{}的订单".format(self.br_buy_code))
                    self.br_buy_fut_price = 0
                    self.br_buy_orderid = 0
                    self.br_buy_code = ''
                    self.br_buy_orderside = -1
                    self.br_b_not_dealt_qty = 0

        # 牛chase buy改单
        if self.bl_buy_fut_price > 0 and self.cur_price - self.bl_buy_fut_price > 3.0 \
                and self.cur_price - self.bl_buy_fut_price < 8.0 :

            print("牛追买改单中")
            # f.write("牛追买改单中\n\n")

            # 获取卖一的价格
            ret_code, g_order_data = self.quote_ctx.get_order_book(self.bl_buy_code)
            if ret_code != 0:
                raise Exception("无法获取摆盘数据:{}; ret_code={}".format(g_order_data, ret_code))
            ask_price1 = g_order_data['Ask'][0][0]
            bid_price1 = g_order_data['Bid'][0][0]

            # 获得price_spread
            ret_code, quote_data = self.quote_ctx.get_stock_quote(self.bl_buy_code)
            if ret_code != 0:
                raise Exception("无法获取{}的stock_quote高频数据".format(self.bl_buy_code))
            times = int(round((ask_price1 - bid_price1), 4) / quote_data['price_spread'][0])
            # 确定改单价格
            if times == 1:
                order_price = ask_price1  # 卖一的价格
            else:
                order_price = bid_price1 + quote_data['price_spread'][0]  # 中间价
            print("bid_price1:{}, times:{}, order_price:{}, ask_price1:{}".format(bid_price1,
                                                                                  times,
                                                                                  order_price,
                                                                                  ask_price1))
            # 获取未成交数量
            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_buy_orderid,
                                                                   statusfilter="1, 2",
                                                                   envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取订单列表,{}".format(order_data))

            print(order_data)
            # order_data.to_csv("chase_buy.txt", index=True, sep='\t', mode='a')

            if order_data.empty:
                print("牛排队买入{}成功".format(self.bl_buy_code))
                self.bl_buy_fut_price = 0
                self.bl_buy_orderid = 0
                self.bl_buy_code = ''
                self.bl_buy_orderside = -1
                self.bl_b_not_dealt_qty = 0

                # f.write("牛排队买入成功\n\n")

            elif int(1000.0 * order_price) != int(1000.0 * order_data['price'][0]):
                self.bl_b_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                # 改单
                ret_code, change_data = self.trade_ctx.change_order(price=order_price,
                                                                    qty=self.bl_b_not_dealt_qty,
                                                                    orderid=self.bl_buy_orderid,
                                                                    envtype=1)
                if ret_code != 0:
                    print("{}熊追单买入, 改单失败:{}".format(self.bl_buy_code, change_data))
                time.sleep(0.5)
                # 获取未成交数量
                ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_buy_orderid,
                                                                       statusfilter="1, 2",
                                                                       envtype=self.trade_env)

                # order_data.to_csv("chase_buy.txt", index=True, sep='\t', mode='a')

                if ret_code != 0:
                    raise Exception("无法获取订单列表,{}".format(order_data))
                else:
                    if order_data.empty:
                        print("牛追单买入{}成功".format(self.bl_buy_code))
                        self.bl_buy_fut_price = 0
                        self.bl_buy_orderid = 0
                        self.bl_buy_code = ''
                        self.bl_buy_orderside = -1
                        self.bl_b_not_dealt_qty = 0

                        # f.write("牛追单买入成功\n\n")
                    else:
                        self.bl_b_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
            else:
                pass

        print('bl_b_not_dealt_qty:%s \t bl_buy_orderside:%s' % (self.bl_b_not_dealt_qty,
                self.bl_buy_orderside))
        # 牛买不上就撤单不买了
        if self.bl_buy_fut_price > 0 and (self.cur_price - self.bl_buy_fut_price >= 8.0
                                          or self.cur_price - self.bl_buy_fut_price <= -8.0):
            # 获取未成交数量
            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_buy_orderid,
                                                                   statusfilter="1, 2",
                                                                   envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取订单列表,{}".format(order_data))
            if order_data.empty:
                print("牛追单买入{}成功".format(self.bl_buy_code))
                self.bl_buy_fut_price = 0
                self.bl_buy_orderid = 0
                self.bl_buy_code = ''
                self.bl_buy_orderside = -1
                self.bl_b_not_dealt_qty = 0
            else:
                self.bl_b_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])

                if self.bl_b_not_dealt_qty > 0:
                    ret_code, order_data = self.trade_ctx.set_order_status(status=0,
                                                                           orderid=self.bl_buy_orderid,
                                                                           envtype=1)
                    while ret_code == -1:
                        time.sleep(0.2)
                        ret_code, order_data = self.trade_ctx.set_order_status(status=0,
                                                                               orderid=self.bl_buy_orderid,
                                                                               envtype=1)

                    print("牛买不上，撤单")
                    # f.write("牛买不上，撤单\n\n")
                    print(order_data)
                    # order_data.to_csv("chase_buy.txt", index=True, sep='\t', mode='a')

                    if ret_code != 0:
                        raise Exception("无法撤销{}的订单".format(self.bl_buy_code))
                    self.bl_buy_fut_price = 0
                    self.bl_buy_orderid = 0
                    self.bl_buy_code = ''
                    self.bl_buy_orderside = -1
                    self.bl_b_not_dealt_qty = 0
        # f.close()


    def market_out(self, data):
        """
        包括止盈和止损两部分
        """
        # 查看牛熊持仓情况
        ret_code, ret_data = self.trade_ctx.position_list_query(stocktype="WARRANT",
                                                                envtype=self.trade_env)
        if ret_code != 0:
            raise Exception("无法获取持仓列表")
        for ix, row in ret_data.iterrows():
            code = row['code']
            qty = row['qty']

            if int(row['qty'].encode('utf-8')) != 0:
                if int(row['can_sell_qty'].encode('utf-8')) != 0:
                    # 以高于买入价两格的价格排队等待卖出（还是卖二的价格，有待商榷？？）
                    ret_code, quote_data = self.quote_ctx.get_stock_quote(code)
                    if ret_code != 0:
                        raise Exception("无法获取{}的stock_quote高频数据".format(code))
                    price_delta = 2.0 * quote_data['price_spread'][0]
                    print(row['cost_price']+price_delta)
                    ret_code, order_data = self.trade_ctx.place_order(row['cost_price']+price_delta,
                                                                      qty, code,
                                                                      orderside=1,
                                                                      envtype=self.trade_env,
                                                                      price_mode=2)
                    while ret_code == -1:      # 交易接口频率限制，重试
                        time.sleep(0.2)
                        ret_code, order_data = self.trade_ctx.place_order(row['cost_price'] + price_delta,
                                                                          qty, code,
                                                                          orderside=1,
                                                                          envtype=self.trade_env,
                                                                          price_mode=2)
                    if ret_code != 0:
                        print("下单失败{}".format(order_data))

                    if row['stock_name'].find(u"熊") >= 0:
                        self.br_sell_orderid = int(order_data['orderid'][0])
                        self.br_sell_code = code
                        self.br_sell_orderside = int(order_data['order_side'][0])
                        self.br_s_not_dealt_qty = int(order_data['qty'][0]) \
                                                  - int(order_data['dealt_qty'][0])
                    if row['stock_name'].find(u"牛") >= 0:
                        self.bl_sell_orderid = int(order_data['orderid'][0])
                        self.bl_sell_code = code
                        self.bl_sell_orderside = int(order_data['order_side'][0])
                        self.bl_s_not_dealt_qty = int(order_data['qty'][0]) \
                                                  - int(order_data['dealt_qty'][0])

                    order_data.to_csv("order_list.txt", index=True, sep='\t', mode='a')

                # 卖熊
                if row['stock_name'].find(u"熊") >= 0:
                    print("卖熊中")
                    self.br_sell_code = code
                    # 熊止盈（2大类：卖二价卖，卖一价卖；5小类）
                    # 卖二价卖：1 data['delta_low5'] 跌破 -15
                    # 卖一价卖: 2 data['delta_low5']的底形成，
                    #          3 data['delta_close5']上穿data['delta_middle5']
                    #          4 data['delta_low5']上穿-15
                    #          5 data['delta_high5']上穿0轴，
                    br_stop_profit_con1 = data.iloc[-1]['delta_low5'] < -15 \
                                          and data.iloc[-2]['delta_low5'] > -15
                    # br_stop_profit_con2 = data.iloc[-1]['delta_low5'] > data.iloc[-2]['delta_low5'] \
                    #                       and data.iloc[-3]['delta_low5'] > data.iloc[-2]['delta_low5']
                    # br_stop_profit_con3 = data.iloc[-1]['delta_close5'] > data.iloc[-1]['delta_middle5'] \
                    #                       and data.iloc[-2]['delta_close5'] < data.iloc[-2]['delta_middle5']
                    # br_stop_profit_con4 = data.iloc[-1]['delta_low5'] > -15 \
                    #                       and data.iloc[-2]['delta_low5'] < -15
                    br_stop_profit_con5 = data.iloc[-1]['delta_high5'] > 0 \
                                          and data.iloc[-2]['delta_high5'] < 0

                    # print(br_stop_profit_con1, br_stop_profit_con2, br_stop_profit_con3,
                    #       br_stop_profit_con4, br_stop_profit_con5)

                    # if br_stop_profit_con1:
                    #     # 以卖二的价格排队卖出
                    #     print("熊止盈，卖二排队卖出中")
                    #     # 获取卖二的价格
                    #     ret_code, g_order_data = self.quote_ctx.get_order_book(self.br_sell_code)
                    #     if ret_code != 0:
                    #         raise Exception("无法获取摆盘数据:{}; ret_code={}"
                    #                         .format(g_order_data, ret_code))
                    #     ask_price2 = g_order_data['Ask'][1][0]
                    #     # 获取未成交数量
                    #     ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                    #                                                            statusfilter="1, 2",
                    #                                                            envtype=self.trade_env)
                    #     if ret_code != 0:
                    #         raise Exception("无法获取订单列表,{}".format(order_data))
                    #     self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                    #
                    #     # 改单
                    #     if int(1000.0 * ask_price2) != int(1000.0 * order_data['price'][0]):
                    #         ret_code, change_data = self.trade_ctx.change_order(price=ask_price2,
                    #                                                             qty=self.br_s_not_dealt_qty,
                    #                                                             orderid=self.br_sell_orderid,
                    #                                                             envtype=1)
                    #         if ret_code != 0:
                    #             print("{}熊止盈，卖二排队改单失败:{}".format(self.br_sell_code, change_data))
                    #         time.sleep(0.5)
                    #         # 获取未成交数量
                    #         ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                    #                                                                statusfilter="1, 2",
                    #                                                                envtype=self.trade_env)
                    #         print(order_data)
                    #
                    #         if ret_code != 0:
                    #             raise Exception("无法获取订单列表,{}".format(order_data))
                    #         else:
                    #             if order_data.empty:
                    #                 print("熊卖二排队卖出成功:{}".format(self.br_sell_code, order_data))
                    #                 self.br_sell_fut_price = 0
                    #                 self.br_sell_orderid = 0
                    #                 self.br_sell_code = ''
                    #                 self.br_sell_orderside = -1
                    #                 self.br_s_not_dealt_qty = 0
                    #             else:
                    #                 self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])

                    if br_stop_profit_con5:
                        # 以卖一的价格排队卖出
                        print("熊止盈，卖一排队卖出中")
                        # 获取卖一的价格
                        ret_code, g_order_data = self.quote_ctx.get_order_book(self.br_sell_code)
                        if ret_code != 0:
                            raise Exception("无法获取摆盘数据:{}; ret_code={}"
                                            .format(g_order_data, ret_code))
                        ask_price1 = g_order_data['Ask'][0][0]
                        # 获取未成交数量
                        ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                                                                               statusfilter="1, 2",
                                                                               envtype=self.trade_env)
                        if ret_code != 0:
                            raise Exception("无法获取订单列表,{}".format(order_data))
                        self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                        # 改单
                        if int(1000.0 * ask_price1) != int(1000.0 * order_data['price'][0]):
                            ret_code, change_data = self.trade_ctx.change_order(price=ask_price1,
                                                                                qty=self.br_s_not_dealt_qty,
                                                                                orderid=self.br_sell_orderid,
                                                                                envtype=1)
                            if ret_code != 0:
                                print("{}熊止盈，卖一排队改单失败:{}".format(self.br_sell_code, change_data))
                            time.sleep(0.5)
                            # 获取未成交数量
                            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                                                                                   statusfilter="1, 2",
                                                                                   envtype=self.trade_env)
                            print(order_data)

                            if ret_code != 0:
                                raise Exception("无法获取订单列表,{}".format(order_data))
                            else:
                                if order_data.empty:
                                    print("熊卖一排队卖出成功:{}".format(self.br_sell_code, order_data))
                                    self.br_sell_fut_price = 0
                                    self.br_sell_orderid = 0
                                    self.br_sell_code = ''
                                    self.br_sell_orderside = -1
                                    self.br_s_not_dealt_qty = 0
                                else:
                                    self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])


                    # 熊止损（两类:data['bear_s_tri'] == 'br_s', data['delta_middle5']上穿0轴
                    #             都是以中间价卖或者买一价卖，如果未卖掉且期货价格比触发信号时的价格
                    #              往止损方向变化5格，立马以买一价格出掉）
                    if data.iloc[-1]['bear_s_tri'] == 'br_s' or (data.iloc[-1]['delta_middle5'] > 0
                            and data.iloc[-2]['delta_middle5'] < 0):
                        print("熊止损中")

                        # 获得买一卖一的价格
                        ret_code, g_order_data = self.quote_ctx.get_order_book(self.br_sell_code)
                        if ret_code != 0:
                            raise Exception("无法获取摆盘数据:{}; ret_code={}"
                                            .format(g_order_data, ret_code))
                        ask_price1 = g_order_data['Ask'][0][0]  # 卖一的价格
                        bid_price1 = g_order_data['Bid'][0][0]  # 买一的价格

                        # 获得price_spread
                        ret_code, quote_data = self.quote_ctx.get_stock_quote(self.br_sell_code)
                        if ret_code != 0:
                            raise Exception("无法获取{}的stock_quote高频数据".format(self.br_sell_code))
                        times = int(round((ask_price1 - bid_price1),4)/quote_data['price_spread'][0])
                        if times == 1:
                            order_price = bid_price1    # 买一价
                        elif times == 2:
                            order_price = ask_price1 - quote_data['price_spread'][0]  # 中间价
                        else:
                            order_price = ask_price1 - quote_data['price_spread'][0]  # 中间价
                            print("{}买一卖一差价过大".format(self.br_sell_code))

                        print("bid_price1:{}, times:{}, order_price:{}, ask_price1:{}".format(bid_price1,
                                                                                              times,
                                                                                              order_price,
                                                                                              ask_price1))
                        # 获取未成交数量
                        ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                                                                               statusfilter="1, 2",
                                                                               envtype=self.trade_env)
                        if ret_code != 0:
                            raise Exception("无法获取订单列表,{}".format(order_data))
                        self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                        # 改单
                        if int(1000.0 * order_price) != int(1000.0 * order_data['price'][0]):
                            ret_code, change_data = self.trade_ctx.change_order(price=order_price,
                                                                                qty=self.br_s_not_dealt_qty,
                                                                                orderid=self.br_sell_orderid,
                                                                                envtype=1)
                            if ret_code != 0:
                                print("{}熊止损改单失败:{}".format(self.br_sell_code, change_data))
                            self.br_sell_fut_price = self.cur_price
                            time.sleep(0.5)
                            # 获取未成交数量
                            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                                                                                   statusfilter="1, 2",
                                                                                   envtype=self.trade_env)

                            if ret_code != 0:
                                raise Exception("无法获取订单列表,{}".format(order_data))
                            else:
                                if order_data.empty:
                                    print("熊止损卖出成功:{}".format(self.br_sell_code, order_data))
                                    self.br_sell_fut_price = 0
                                    self.br_sell_orderid = 0
                                    self.br_sell_code = ''
                                    self.br_sell_orderside = -1
                                    self.br_s_not_dealt_qty = 0
                                else:
                                    self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])

                # 卖牛
                if row['stock_name'].find(u"牛") >= 0:
                    print("卖牛中")
                    self.bl_sell_code = code
                    # 牛止盈（2大类：卖二价卖，卖一价卖；5小类）
                    # 卖二价卖：1 data['delta_high5'] 上穿 15
                    # 卖一价卖: 2 data['delta_high5']的顶形成
                    #          3 data['delta_close5']跌破data['delta_middle5']
                    #          4 data['delta_high5']跌破15
                    #          5 data['delta_low5']跌破0轴
                    bl_stop_profit_con1 = data.iloc[-1]['delta_high5'] > 15 \
                                          and data.iloc[-2]['delta_high5'] < 15
                    # bl_stop_profit_con2 = data.iloc[-1]['delta_high5'] < data.iloc[-2]['delta_high5'] \
                    #                       and data.iloc[-3]['delta_high5'] < data.iloc[-2]['delta_high5']
                    # bl_stop_profit_con3 = data.iloc[-1]['delta_close5'] < data.iloc[-1]['delta_middle5'] \
                    #                       and data.iloc[-2]['delta_close5'] > data.iloc[-2]['delta_middle5']
                    # bl_stop_profit_con4 = data.iloc[-1]['delta_high5'] < 15 \
                    #                       and data.iloc[-2]['delta_high5'] > 15
                    bl_stop_profit_con5 = data.iloc[-1]['delta_low5'] < 0 \
                                          and data.iloc[-2]['delta_low5'] > 0

                    # print(bl_stop_profit_con1, bl_stop_profit_con2, bl_stop_profit_con3,
                    #       bl_stop_profit_con4, bl_stop_profit_con5)

                    # if bl_stop_profit_con1:
                    #     # 以卖二的价格排队卖出
                    #     print("牛止盈，卖二排队卖出中")
                    #     # 获取卖二的价格
                    #     ret_code, g_order_data = self.quote_ctx.get_order_book(self.bl_sell_code)
                    #     if ret_code != 0:
                    #         raise Exception("无法获取摆盘数据:{}; ret_code={}"
                    #                         .format(g_order_data, ret_code))
                    #     ask_price2 = g_order_data['Ask'][1][0]
                    #     # 获取未成交数量
                    #     ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                    #                                                            statusfilter="1, 2",
                    #                                                            envtype=self.trade_env)
                    #     if ret_code != 0:
                    #         raise Exception("无法获取订单列表,{}".format(order_data))
                    #     self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                    #
                    #     # 改单
                    #     if int(1000.0 * ask_price2) != int(1000.0 * order_data['price'][0]):
                    #         ret_code, change_data = self.trade_ctx.change_order(price=ask_price2,
                    #                                                             qty=self.bl_s_not_dealt_qty,
                    #                                                             orderid=self.bl_sell_orderid,
                    #                                                             envtype=1)
                    #         if ret_code != 0:
                    #             print("{}熊止盈，卖二排队改单失败:{}".format(self.bl_sell_code, change_data))
                    #         time.sleep(0.5)
                    #         # 获取未成交数量
                    #         ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                    #                                                                statusfilter="1, 2",
                    #                                                                envtype=self.trade_env)
                    #         print(order_data)
                    #
                    #         if ret_code != 0:
                    #             raise Exception("无法获取订单列表,{}".format(order_data))
                    #         else:
                    #             if order_data.empty:
                    #                 print("熊卖二排队卖出成功:{}".format(self.bl_sell_code, order_data))
                    #                 self.bl_sell_fut_price = 0
                    #                 self.bl_sell_orderid = 0
                    #                 self.bl_sell_code = ''
                    #                 self.bl_sell_orderside = -1
                    #                 self.bl_s_not_dealt_qty = 0
                    #             else:
                    #                 self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])

                    if bl_stop_profit_con5:
                        # 以卖一的价格排队卖出
                        print("牛止盈，卖一排队卖出中")
                        # 获取卖一的价格
                        ret_code, g_order_data = self.quote_ctx.get_order_book(self.bl_sell_code)
                        if ret_code != 0:
                            raise Exception("无法获取摆盘数据:{}; ret_code={}"
                                            .format(g_order_data, ret_code))
                        ask_price1 = g_order_data['Ask'][0][0]
                        # 获取未成交数量
                        ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                                                                               statusfilter="1, 2",
                                                                               envtype=self.trade_env)
                        if ret_code != 0:
                            raise Exception("无法获取订单列表,{}".format(order_data))
                        self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                        # 改单
                        if int(1000.0 * ask_price1) != int(1000.0 * order_data['price'][0]):
                            ret_code, change_data = self.trade_ctx.change_order(price=ask_price1,
                                                                                qty=self.bl_s_not_dealt_qty,
                                                                                orderid=self.bl_sell_orderid,
                                                                                envtype=1)
                            if ret_code != 0:
                                print("{}牛止盈，卖一排队改单失败:{}".format(self.bl_sell_code, change_data))
                            time.sleep(0.5)
                            # 获取未成交数量
                            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                                                                                   statusfilter="1, 2",
                                                                                   envtype=self.trade_env)

                            if ret_code != 0:
                                raise Exception("无法获取订单列表,{}".format(order_data))
                            else:
                                if order_data.empty:
                                    print("熊卖一排队卖出成功:{}".format(self.bl_sell_code, order_data))
                                    self.bl_sell_fut_price = 0
                                    self.bl_sell_orderid = 0
                                    self.bl_sell_code = ''
                                    self.bl_sell_orderside = -1
                                    self.bl_s_not_dealt_qty = 0
                                else:
                                    self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])


                    # 牛止损（两类:data['bull_s_tri'] == 'bl_s', data['delta_middle5']跌破0轴
                    #             都是以中间价卖或者买一价卖，如果未卖掉且期货价格比触发信号时的价格
                    #              往止损方向变化5格，立马以买一价格出掉）
                    if data.iloc[-1]['bull_s_tri'] == 'bl_s' or (data.iloc[-1]['delta_middle5'] < 0
                            and data.iloc[-2]['delta_middle5'] >= 0):
                        print("牛止损中")

                        # 获得买一卖一的价格
                        ret_code, g_order_data = self.quote_ctx.get_order_book(self.bl_sell_code)
                        if ret_code != 0:
                            raise Exception("无法获取摆盘数据:{}; ret_code={}"
                                            .format(g_order_data, ret_code))
                        ask_price1 = g_order_data['Ask'][0][0]  # 卖一的价格
                        bid_price1 = g_order_data['Bid'][0][0]  # 买一的价格

                        # 获得price_spread
                        ret_code, quote_data = self.quote_ctx.get_stock_quote(self.bl_sell_code)
                        if ret_code != 0:
                            raise Exception("无法获取{}的stock_quote高频数据".format(self.bl_sell_code))
                        times = int(round((ask_price1 - bid_price1), 4) / quote_data['price_spread'][0])
                        if times == 1:
                            order_price = bid_price1  # 买一价
                        elif times == 2:
                            order_price = ask_price1 - quote_data['price_spread'][0]  # 中间价
                        else:
                            order_price = ask_price1 - quote_data['price_spread'][0]  # 中间价
                            print("{}买一卖一差价过大".format(self.bl_sell_code))

                        print("bid_price1:{}, times:{}, order_price:{}, ask_price1:{}".format(bid_price1,
                                                                                              times,
                                                                                              order_price,
                                                                                              ask_price1))
                        # 获取未成交数量
                        ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                                                                               statusfilter="1, 2",
                                                                               envtype=self.trade_env)
                        if ret_code != 0:
                            raise Exception("无法获取订单列表,{}".format(order_data))
                        self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                        # 改单
                        if int(1000.0 * order_price) != int(1000.0 * order_data['price'][0]):
                            ret_code, change_data = self.trade_ctx.change_order(price=order_price,
                                                                                qty=self.bl_s_not_dealt_qty,
                                                                                orderid=self.bl_sell_orderid,
                                                                                envtype=1)
                            if ret_code != 0:
                                print("{}熊止损改单失败:{}".format(self.bl_sell_code, change_data))
                            self.bl_sell_fut_price = self.cur_price
                            time.sleep(0.5)
                            # 获取未成交数量
                            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                                                                                   statusfilter="1, 2",
                                                                                   envtype=self.trade_env)

                            if ret_code != 0:
                                raise Exception("无法获取订单列表,{}".format(order_data))
                            else:
                                if order_data.empty:
                                    print("熊止损卖出成功:{}".format(self.bl_sell_code, order_data))
                                    self.bl_sell_fut_price = 0
                                    self.bl_sell_orderid = 0
                                    self.bl_sell_code = ''
                                    self.bl_sell_orderside = -1
                                    self.bl_s_not_dealt_qty = 0
                                else:
                                    self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])


    def chase_sell_change_order(self, data):
        """
        当止损未全部成交且往亏损方向变化超5格时，立马以买一价格卖出
        """
        # 熊chase sell改单
        if self.br_sell_fut_price > 0 and self.cur_price - self.br_sell_fut_price >= 5.0 \
                and self.br_s_not_dealt_qty > 0:
            print("熊追卖改单中")
            # 获取买一的价格
            ret_code, g_order_data = self.quote_ctx.get_order_book(self.br_sell_code)
            if ret_code != 0:
                raise Exception("无法获取摆盘数据:{}; ret_code={}".format(g_order_data, ret_code))
            bid_price1 = g_order_data['Bid'][0][0]

            # 确定改单价格
            order_price = bid_price1  # 买一的价格
            # 获取未成交数量
            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                                                                   statusfilter="1, 2",
                                                                   envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取订单列表,{}".format(order_data))

            if order_data.empty:
                print("止损卖熊{}成功".format(self.br_sell_code))
                self.br_sell_fut_price = 0
                self.br_sell_orderid = 0
                self.br_sell_code = ''
                self.br_sell_orderside = -1
                self.br_s_not_dealt_qty = 0
            elif int(1000.0 * order_price) != int(1000.0 * order_data['price'][0]):
                self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                # 改单
                ret_code, change_data = self.trade_ctx.change_order(price=order_price,
                                                                    qty=self.br_s_not_dealt_qty,
                                                                    orderid=self.br_sell_orderid,
                                                                    envtype=1)
                if ret_code != 0:
                    print("{}熊追单卖出, 改单失败:{}".format(self.br_sell_code, change_data))
                time.sleep(0.5)
                # 获取未成交数量
                ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.br_sell_orderid,
                                                                       statusfilter="1, 2",
                                                                       envtype=self.trade_env)
                if ret_code != 0:
                    raise Exception("无法获取订单列表,{}".format(order_data))
                if order_data.empty:
                    print("追单卖出{}成功".format(self.br_sell_code))
                    self.br_sell_fut_price = 0
                    self.br_sell_orderid = 0
                    self.br_sell_code = ''
                    self.br_sell_orderside = -1
                    self.br_s_not_dealt_qty = 0
                else:
                    self.br_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
            else:
                pass

        # 牛chase sell改单
        if self.bl_sell_fut_price > 0 and self.cur_price - self.bl_sell_fut_price <= -5.0 \
                and self.br_s_not_dealt_qty > 0:
            print("牛追卖改单中")
            # 获取买一的价格
            ret_code, g_order_data = self.quote_ctx.get_order_book(self.bl_sell_code)
            if ret_code != 0:
                raise Exception("无法获取摆盘数据:{}; ret_code={}".format(g_order_data, ret_code))
            bid_price1 = g_order_data['Bid'][0][0]

            # 确定改单价格
            order_price = bid_price1  # 买一的价格
            # 获取未成交数量
            ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                                                                   statusfilter="1, 2",
                                                                   envtype=self.trade_env)
            if ret_code != 0:
                raise Exception("无法获取订单列表,{}".format(order_data))

            if order_data.empty:
                print("止损卖牛{}成功".format(self.bl_sell_code))
                self.bl_sell_fut_price = 0
                self.bl_sell_orderid = 0
                self.bl_sell_code = ''
                self.bl_sell_orderside = -1
                self.bl_s_not_dealt_qty = 0
            elif int(1000.0 * order_price) != int(1000.0 * order_data['price'][0]):
                self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
                # 改单
                ret_code, change_data = self.trade_ctx.change_order(price=order_price,
                                                                    qty=self.bl_s_not_dealt_qty,
                                                                    orderid=self.bl_sell_orderid,
                                                                    envtype=1)
                if ret_code != 0:
                    print("{}熊追单卖出, 改单失败:{}".format(self.bl_sell_code, change_data))
                time.sleep(0.5)
                # 获取未成交数量
                ret_code, order_data = self.trade_ctx.order_list_query(orderid=self.bl_sell_orderid,
                                                                       statusfilter="1, 2",
                                                                       envtype=self.trade_env)
                if ret_code != 0:
                    raise Exception("无法获取订单列表,{}".format(order_data))
                if order_data.empty:
                    print("追单卖出{}成功:{}".format(self.bl_sell_code, change_data))
                    self.bl_sell_fut_price = 0
                    self.bl_sell_orderid = 0
                    self.bl_sell_code = ''
                    self.bl_sell_orderside = -1
                    self.bl_s_not_dealt_qty = 0
                else:
                    self.bl_s_not_dealt_qty = int(order_data['qty'][0]) - int(order_data['dealt_qty'][0])
            else:
                pass


    def write_signal_to_txt(self, data):
        """
        输出并研究买卖信号数据用来改进策略
        """
        data_c = data.copy()
        data_c['time_key'] = pd.Series([dt.datetime.strptime(x.encode('GBK'),"%Y-%m-%d %H:%M:%S")
                                          .strftime("%H:%M:%S") for x in data_c['time_key']])
        # 输出bull的买卖信号数据
        data_bl_bs = data_c.copy()
        name_bl_bs = dt.datetime.now().date().strftime('%Y-%m-%d') + '_bl_bs_' + '.txt'
        data_bl_bs = data_bl_bs[(data_bl_bs['time_key'] >= '09:15:00')
                              & (data_bl_bs['time_key'] <= '16:00:00')].reset_index(drop=True)
        data_bl_bs = data_bl_bs[(data_bl_bs.bull_b_tri == 'bl_b') | (data_bl_bs.bull_s_tri == 'bl_s')]\
                               .reset_index(drop=True)
        data_bl_bs.to_csv(name_bl_bs, index=True, sep='\t',
                    columns=['down_up_in', 'delta', 'delta_buy', 'delta_sell', 'time_key', 'rank',
                             'green_red', 'bull_b_tri', 'bull_b_price', 'bull_s_tri', 'bull_s_price'])
        print(data_bl_bs['green_red'].value_counts())

        # 输出bear的买卖信号数据
        data_br_bs = data_c.copy()
        name_br_bs = dt.datetime.now().date().strftime('%Y-%m-%d') + '_br_bs_' + '.txt'
        data_br_bs = data_br_bs[(data_br_bs['time_key'] >= '09:15:00')
                                & (data_br_bs['time_key'] <= '16:00:00')].reset_index(drop=True)
        data_br_bs = data_br_bs[(data_br_bs['bear_b_tri'] == 'br_b') | (data_br_bs['bear_s_tri'] == 'br_s')]\
                               .reset_index(drop=True)
        data_br_bs.to_csv(name_br_bs, index=True, sep='\t',
                    columns=['down_up_in', 'delta', 'delta_buy', 'delta_sell', 'time_key', 'rank',
                             'green_red', 'bear_b_tri', 'bear_b_price', 'bear_s_tri', 'bear_s_price'])
        print(data_br_bs['green_red'].value_counts())

        # 输出bull和bear的买卖信号数据
        data_save = data_c.copy()
        name_save = dt.datetime.now().date().strftime('%Y-%m-%d') + '_bl_br_bs' + '.txt'
        data_save = data_save[(data_save['time_key'] >= '09:15:00')
                              & (data_save['time_key'] <= '16:00:00')].reset_index(drop=True)
        data_save = data_save[(data_save['bull_b_tri'] == 'bl_b') | (data_save['bull_s_tri'] == 'bl_s')
                            | (data_save['bear_b_tri'] == 'br_b') | (data_save['bear_s_tri'] == 'br_s')]\
                             .reset_index(drop=True)
        data_save.to_csv(name_save, index=True, sep='\t',
                    columns=['down_up_in', 'delta', 'delta_buy', 'delta_sell', 'time_key', 'rank',
                             'green_red', 'bull_b_tri', 'bull_b_price', 'bull_s_tri', 'bull_s_price',
                             'bear_b_tri', 'bear_b_price', 'bear_s_tri', 'bear_s_price'])
        print(data_save['green_red'].value_counts())


    def text_save(self, content, filename, mode='a'):
        # Try to save a list variable in txt file.
        file = open(filename, mode)
        for i in range(len(content)):
            file.write(str(content[i]) + '\n')
        file.close()


    def text_read(self, filename):
        # Try to read a txt file and return a list.Return [] if there was a mistake.
        try:
            file = open(filename, 'r')
        except IOError:
            error = []
            return error
        content = file.readlines()

        for i in range(len(content)):
            content[i] = content[i][:len(content[i]) - 1]

        file.close()
        return content


    def handle_data(self, bear_pool2, bear_rec_price, bull_pool2, bull_rec_price):
        # 每天的11:55和15:55必须平仓
        cur_time = dt.datetime.now()   # 当前时间
        time_int =  int(cur_time.strftime("%H%M%S"))
        self.sell_all_position(time_int)

        # 每个月倒数第二个交易日按照'HK_FUTURE.999011'
        date = "%d-%02d-01" % (cur_time.year, cur_time.month + 1)
        ret_code, ret_data = self.quote_ctx.get_trading_days("HK_FUTURE", end_date=date)
        if ret_data[1] == cur_time.strftime("%Y-%m-%d"):
            self.stock = 'HK_FUTURE.999011'

        # 订阅恒指当月期货高频数据
        i = 0
        for data_type in self.da_ty_list:
            ret_code, ret_data = self.quote_ctx.subscribe(self.stock, data_type)
            if ret_code != 0:
                raise Exception("无法订阅{}的{}高频数据".format(self.stock, data_type))
            else:
                i += 1
        if i == len(self.da_ty_list):
            print("成功订阅{}的高频数据".format(self.stock))

        # 获取期货当前价格self.cur_price
        ret_code, ret_data = self.quote_ctx.get_stock_quote(self.stock)
        if ret_code != 0:
            raise Exception("无法获取{}的stock_quote高频数据".format(self.stock))
        self.cur_price = ret_data['last_price'][0]

        # 获取150个一分钟k线数据(最大1000)
        ret_code, ret_data = self.quote_ctx.get_cur_kline(self.stock, num=self.cur_kline_num,
                                                          ktype='K_1M')
        if ret_code != 0:
            raise Exception("无法获取{}的cur_kline高频数据".format(self.stock))

        # 处理k线
        data = ret_data.copy()
        data['price_change'] = data['close'] - data['close'].shift(1)
        data['delta'] = data['high'] - data['low']

        self.cal_avg_line(data)
        # self.cal_avg_line_macd(data)
        # self.cal_avg_line_macd2(data)
        self.set_green_red(data)
        self.set_down_up_in(data)
        self.set_bottom_peak(data)
        self.avg_state_rank(data)
        self.set_buy_trigger_range(data)
        self.buy_trigger_signal(data)
        self.set_sell_trigger_range(data)
        self.sell_trigger_signal(data)
        self.compare_with_avg_line5(data)

       # 获取牛熊买入标的
        # 变量bear_pool2, bear_rec_price, bull_pool2, bull_rec_price不能变
        bear_candidate_list, bull_candidate_list = self.update_warrant_pool(bear_pool2, bear_rec_price,
                                                                            bull_pool2, bull_rec_price)

        print("bear_candidate: {}; bull_candidate: {}"
              .format(bear_candidate_list[0], bull_candidate_list[0]))

        print("\ncurrent time:{}".format(cur_time.strftime("%H:%M:%S")))
        print("rank:{}".format(data.iloc[-1]['rank']))

        print("\nbull_b_tri:({}, {}, {}, {})".format(data.iloc[-2]['bull_b_tri'],
                                                     data.iloc[-2]['bull_b_price'],
                                                     data.iloc[-1]['bull_b_tri'],
                                                     data.iloc[-1]['bull_b_price']))
        print("bull_s_tri:({}, {}, {}, {})\n".format(data.iloc[-2]['bull_s_tri'],
                                                     data.iloc[-2]['bull_s_price'],
                                                     data.iloc[-1]['bull_s_tri'],
                                                     data.iloc[-1]['bull_s_price']))

        print("bear_b_tri:({}, {}, {}, {})".format(data.iloc[-2]['bear_b_tri'],
                                                   data.iloc[-2]['bear_b_price'],
                                                   data.iloc[-1]['bear_b_tri'],
                                                   data.iloc[-1]['bear_b_price']))
        print("bear_s_tri:({}, {}, {}, {})\n".format(data.iloc[-2]['bear_s_tri'],
                                                     data.iloc[-2]['bear_s_price'],
                                                     data.iloc[-1]['bear_s_tri'],
                                                     data.iloc[-1]['bear_s_price']))

        if (time_int >= 93100 and time_int <= 115400) or \
                (time_int >= 130100 and time_int <= 155400):

            # print("hello 11")
            self.update_position_order_num(data)
            if self.nonzero_position_num == 0 and self.unfinished_order_num == 0:
                # 先以买一价或者中间价排队买入
                self.market_in(data, bear_candidate_list, bull_candidate_list)

            # print("hello 22")
            self.update_position_order_num(data)
            if self.unfinished_order_num > 0 \
                    and (self.br_buy_orderside == 0 or self.bl_buy_orderside == 0):
                # 当排队买入未全部成交且往盈利方向变化大于4格且小于10格时，立马以卖一价或者中间价买入
                # 当往盈利方向变化大于10格或者往亏损方向变化大于8格时，撤单
                self.chase_buy_change_order(data)

            # print("hello 33")
            self.update_position_order_num(data)
            if self.nonzero_position_num > 0 \
                    and (self.br_buy_orderside == -1 or self.bl_buy_orderside == -1):
                # 止盈卖二卖一排队卖出，止损中间价和买一卖出
                self.market_out(data)

            # print("hello 44")
            self.update_position_order_num(data)
            if self.nonzero_position_num > 0 \
                    and (self.br_sell_orderside == 1 or self.bl_sell_orderside == 1):
                # 当止损未全部成交且往亏损方向变化超5格时，立马以买一价格卖出
                self.chase_sell_change_order(data)

            print("cur_price:{}; br_buy_fut_price:{}; bl_buy_fut_price:{};"
                  .format(self.cur_price, self.br_buy_fut_price, self.bl_buy_fut_price))
            print("\ncur_price:{}; br_sell_fut_price:{}; bl_sell_fut_price:{};"
                  .format(self.cur_price, self.br_sell_fut_price, self.bl_sell_fut_price))


        if cur_time_int > 160100:
            self.write_signal_to_txt(data)

        # print(data.loc[:,['time_key', 'open', 'close', 'green_red', 'down_up_in', 'bottom_peak']].tail(10))
        # print(data['delta'].mean())

        data.tail(30).to_csv("HS_FUTURE.txt", index=True, sep='\t',
                    columns=['time_key', 'down_up_in', 'delta', 'delta_buy', 'delta_sell', 'rank',
                             'green_red', 'bull_b_tri', 'bull_b_price', 'bull_s_tri', 'bull_s_price',
                             'bear_b_tri', 'bear_b_price', 'bear_s_tri', 'bear_s_price'])


        br_stop_profit_con1 = data.iloc[-1]['delta_low5'] < -15 \
                              and data.iloc[-2]['delta_low5'] > -15
        br_stop_profit_con2 = data.iloc[-1]['delta_low5'] > data.iloc[-2]['delta_low5'] \
                              and data.iloc[-3]['delta_low5'] > data.iloc[-2]['delta_low5']
        br_stop_profit_con3 = data.iloc[-1]['delta_close5'] > data.iloc[-1]['delta_middle5'] \
                              and data.iloc[-2]['delta_close5'] < data.iloc[-2]['delta_middle5']
        br_stop_profit_con4 = data.iloc[-1]['delta_low5'] > -15 \
                              and data.iloc[-2]['delta_low5'] < -15
        br_stop_profit_con5 = data.iloc[-1]['delta_high5'] > 0 \
                              and data.iloc[-2]['delta_high5'] < 0
        br_stop_profit_con6 = data.iloc[-1]['delta_middle5'] > 0 \
                              and data.iloc[-2]['delta_middle5'] < 0
        print("\n熊止盈信号")
        print(br_stop_profit_con5, br_stop_profit_con6)


        bl_stop_profit_con1 = data.iloc[-1]['delta_high5'] > 15 \
                              and data.iloc[-2]['delta_high5'] < 15
        bl_stop_profit_con2 = data.iloc[-1]['delta_high5'] < data.iloc[-2]['delta_high5'] \
                              and data.iloc[-3]['delta_high5'] < data.iloc[-2]['delta_high5']
        bl_stop_profit_con3 = data.iloc[-1]['delta_close5'] < data.iloc[-1]['delta_middle5'] \
                              and data.iloc[-2]['delta_close5'] > data.iloc[-2]['delta_middle5']
        bl_stop_profit_con4 = data.iloc[-1]['delta_high5'] < 15 \
                              and data.iloc[-2]['delta_high5'] > 15
        bl_stop_profit_con5 = data.iloc[-1]['delta_low5'] < 0 \
                              and data.iloc[-2]['delta_low5'] > 0
        bl_stop_profit_con6 = data.iloc[-1]['delta_middle5'] < 0 \
                              and data.iloc[-2]['delta_middle5'] > 0

        print("\n牛止盈信号")
        print(bl_stop_profit_con5, bl_stop_profit_con6)


if __name__ == "__main__":
    stock = 'HK_FUTURE.999010'   # 每个月倒数第二个交易日按照'HK_FUTURE.999011'

    avg_line = Moving_avg_line(stock)
    print("策略启动成功！\n")
    bear_pool1, bull_pool1 = avg_line.warrant_pool1()
    bear_pool2, bear_rec_price, bull_pool2, bull_rec_price = avg_line.warrant_pool2(bear_pool1,
                                                                                    bull_pool1)
    # while True:
    #     cur_time = dt.datetime.now()
    #     cur_time_int = int(cur_time.strftime("%H%M%S"))
    #     if cur_time_int > 915000 and cur_time_int < 160000:
    while True:
        cur_time = dt.datetime.now()
        cur_time_int = int(cur_time.strftime("%H%M%S"))
        if cur_time_int == 93020 or cur_time_int == 130020:
            bear_pool2, bear_rec_price, bull_pool2, bull_rec_price = avg_line.warrant_pool2(bear_pool1,
                                                                                            bull_pool1)
        t0 = time.clock()
        avg_line.handle_data(bear_pool2, bear_rec_price, bull_pool2, bull_rec_price)    # 调用handle_data函数
        # time.sleep(0.01)
        print("handle data用时：%5f \n" % (time.clock()-t0))
