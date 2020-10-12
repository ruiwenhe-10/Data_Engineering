import pandas as pd
import cx_Oracle
# from WindPy import *
from datetime import *
import numpy as np
import matplotlib.pyplot as plt
import os

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
# plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

# def get_index(): #获取指数信息
#     conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
#     cursor = conn.cursor()
#     # date = pd.read_csv('hq/day_hq.csv').iloc[-1]['date'].replace('-', '')
#     cursor.execute("select F1_1289 as index_name, F2_1289 as ID, F4_1289 as index_intro, F5_1289 as style, F9_1289 as quanzhong, F10_1289 as weighted_method FROM TB_OBJECT_1289")
#     hq = pd.DataFrame(cursor.fetchall(), columns=['指数名称', '证券ID', '指数简介', '[内部]指数风格', '权重类型', '[内部]加权方式'])
#     hq.to_csv('hq/指数.csv',header=True,index=False,encoding='utf_8_sig')


def get_index_price(): #get ZZ500 close price
    now = datetime.now().strftime("%Y-%m-%d")
    w.start()
    price = w.wsd("000905.SH", "close", "2009-01-01", now, "PriceAdj=B")
    df = pd.DataFrame({'日期': price.Times,'收盘价': price.Data[0]})
    df.to_csv('hq/zz500收盘价/zz500日收盘价2009到现在.csv', mode='a', header=True,index=False,encoding='utf_8_sig')
    w.stop()

def update_day_hq():#获取后复权行情数据
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    # date = pd.read_csv('hq/day_hq.csv').iloc[-1]['date'].replace('-', '')
    date1 = '20090101'
    date2 = '20191231'
    cursor.execute("select F2_1425 as tdate, F16_1090 as code, F3_1425 as last_close, F4_1425 as open, F5_1425 as high, F6_1425 as low, F7_1425 as close, (case when F8_1425=0 then F7_1425 else ROUND(F9_1425/F8_1425*10*F10_1425,2) end) as vwap, F9_1425 as amount FROM TB_OBJECT_1090, TB_OBJECT_1425 where F4_1090='A' and F2_1090=F1_1425 and F2_1425>{0} and F2_1425<{1}".format(date1,date2))
    hq= pd.DataFrame(cursor.fetchall(),columns=['date', 'code', 'last_close', 'open', 'high', 'low', 'close', 'vwap', 'amount'])
    hq['date']=pd.to_datetime(hq['date'])
    hq['last_close'] = hq['last_close'].astype(float).round(2)
    hq['open']=hq['open'].astype(float).round(2)
    hq['high'] = hq['high'].astype(float).round(2)
    hq['low'] = hq['low'].astype(float).round(2)
    hq['close']=hq['close'].astype(float).round(2)
    hq['vwap'] = hq['vwap'].astype(float).round(2)
    hq['amount'] = hq['amount'].astype(float).round(2)
    #hq.sort_values(['date', 'code']).to_csv('hq/day_hq.csv', index=False)
    hq.sort_values(['code','date']).to_csv('hq/day_hq.csv',mode='a',header=False, index=False)

def get_estimate_info(year): #获取分析师预测资料
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    date = '{0}1231'.format(year)
    startdate = '{0}1031'.format(year)
    enddate = '{0}1231'.format(year)
    cursor.execute("select F5_1571 as es_date, F4_1571 as report_period, F16_1090 as tradecode, F8_1571 as estimate_profit, F2_1571 as org_name, F3_1571 as person from TB_OBJECT_1571, TB_OBJECT_1090 where F1_1571 = OB_REVISIONS_1090 and F4_1090='A' and F4_1571 = {0} and F5_1571 > {1} and F5_1571 < {2}".format(date, startdate, enddate))
    df = pd.DataFrame(cursor.fetchall(), columns=['预测日期','报告期','交易代码','预测净利润', '预测机构','分析师姓名'])
    df['预测净利润'] = df['预测净利润'] * 10000
    df.dropna().sort_values(['交易代码','预测日期']).to_csv('业绩超预期/预测净利润/预测净利润(未处理){0}.csv'.format(year), header=True, index=False, encoding="utf_8_sig")

def get_profit(year): #获取净利润资料
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    date = '{0}1231'.format(year)
    cursor.execute("select F3_1854 as release_date, F2_1854 as report_period, F16_1090 as tradecode, F4_1854 as category, F61_1854 as actual_profit from TB_OBJECT_1854, TB_OBJECT_1090 where F1_1854 = OB_REVISIONS_1090 and F4_1090='A' and F2_1854 = {0}".format(date))
    df = pd.DataFrame(cursor.fetchall(), columns=['实际净利润公告日期','实际净利润报告期','交易代码','报表类型','净利润'])
    df['实际净利润公告日期']=pd.to_datetime(df['实际净利润公告日期'])
    df = df.sort_values(['交易代码'])
    df.to_csv('业绩超预期/实际净利润/实际净利润(未处理){0}.csv'.format(year), header=True, index=False, encoding="utf_8_sig")

def get_unique(dataframe):
    unique_code = []
    for code in dataframe['交易代码']:  # 找到唯一股票代码成为列表
        if code not in unique_code:
            unique_code.append(code)
    return unique_code

def process_estimate_average(year): #找到平均预测净利润
    average = []
    data = pd.read_csv('业绩超预期/预测净利润/预测净利润(未处理){0}.csv'.format(year), header=0)
    unique = get_unique(data)
    for i in range(len(unique)):
        sub_data = data.loc[data["交易代码"] == unique[i]] #以交易代码选取股票
        data_after_dup = sub_data.drop_duplicates(subset = ['分析师姓名'], keep='last', inplace=False) #去掉重复分析师预测
        # data_after_dup.to_csv('业绩超预期/test.csv', mode='a', header = True, encoding='utf_8_sig')
        average.append(data_after_dup['预测净利润'].mean())
    new_data = [unique,average]
    labels = ['交易代码', '平均预测净利润']
    df = pd.DataFrame.from_records(new_data,labels).T
    df.to_csv('业绩超预期/预测净利润/预测净利润(已处理){0}.csv'.format(year), header = True, index=False, encoding='utf_8_sig')
    return df


def process_profit_file(year): #选择合并报表作为净利润
    df = pd.read_csv('业绩超预期/实际净利润/实际净利润(未处理){0}.csv'.format(year), header=0)
    df = df.loc[df['报表类型'] == '合并报表']
    df.to_csv('业绩超预期/实际净利润/实际净利润(已处理){0}.csv'.format(year), header=True, index=False, encoding='utf_8_sig')
    return df

def get_excess(year): #找到业绩超预期公司股
    df1=process_estimate_average(year)
    df2=process_profit_file(year)
    df3 = pd.merge(df1,df2,on='交易代码',how='inner') #合并预测与实际数据
    a = df3.pop('平均预测净利润')
    df3.insert(5,'平均预测净利润',a)
    # df3.to_csv('业绩超预期/合并表/合并表{0}.csv'.format(year), header=True, index=False, encoding='utf_8_sig')
    df4 = df3.loc[df3["净利润"] > df3["平均预测净利润"]]
    a = df4.pop('交易代码')
    a = a.astype(int)
    df4.insert(0,'交易代码', a)
    df4.to_csv('业绩超预期/业绩超预期/业绩超预期{0}.csv'.format(year),header=True,index=False, encoding='utf_8_sig')

def get_graph_info(year,option): #获取每股对应后80天股价信息
    df = pd.read_csv('hq/day_hq.csv',  #股价
                         names=['date', 'code', 'last_close', 'open', 'high', 'low', 'close', 'vwap', 'amount'])
    df = df.loc[:,('date','code','last_close','close')]
    df2 = pd.read_csv('业绩超预期/业绩超预期/业绩超预期{0}.csv'.format(year),header='infer',encoding='utf_8_sig')
    df2 = df2.loc[:,('交易代码','实际净利润公告日期','实际净利润报告期','净利润','平均预测净利润')]
    start_time = "{0}0101".format(year)
    end_time = "{0}1231".format(year+1)
    start_time = pd.Timestamp(start_time).strftime("%Y-%m-%d")
    end_time = pd.Timestamp(end_time).strftime("%Y-%m-%d")
    df = df.loc[(df['date']>=start_time)&(df['date']<=end_time)]
    unique_code=[]
    unique_date=[]
    delete = []
    data_value = []
    size = []
    for i in range(len(df2)):
        code = df2.loc[i, '交易代码']
        date = df2.loc[i, '实际净利润公告日期']
        if code in df['code'].values:
            original_date = pd.Timestamp(df2.loc[i, '实际净利润公告日期'])
            after_130_date = (original_date + pd.Timedelta(days=130)).strftime('%Y-%m-%d')
            original_date = original_date.strftime('%Y-%m-%d')
            # print('origindate: {0} enddate: {1}'.format(original_date, after_130_date))
            sample = df.loc[(df['code']==code)&(df['date']<after_130_date)&(df['date']>=original_date)]
            sample = sample.iloc[:80,:]
            if (len(sample) != 80):
                delete.append((code,date))
                continue
            if (option == '胜率'):
                sample.insert(len(df.iloc[0,:]), 'win_ratio', sample['close'] / sample['last_close'].values)
                num = sample['win_ratio'].values
            elif (option == '盈亏比'):
                sample.insert(len(df.iloc[0,:]), 'revenue', (sample['close'] - sample['last_close']).values)
                num = sample['revenue'].values
            # print(data_value)
            elif (option == '累计收益'):
                # print(sub_df)
                sample.insert(len(df.iloc[0,:]), 'cum_return', (sample['close'] / sample.iloc[0, 3] - 1).values)
                num = sample['cum_return'].values
            else:
                print('无正确选择')
            unique_code.append(code)
            unique_date.append(date)
            size.append(len(num))
            data_value.append(num)
            # sample.to_csv('高管增持/每年增持股票股价情况/股价{0}.csv'.format(year),mode='a',index=False,header=False,encoding='utf_8_sig')
    max_size = max(size)
    column_1 = ['+' + str(i) for i in range(max_size)]
    multi_index = pd.MultiIndex.from_arrays([unique_date, unique_code], names=['实际净利润公告日期', '交易代码'])
    dataframe = pd.DataFrame(data_value, index=multi_index, columns=column_1)
    # print(dataframe)
    # return
    average = []
    if (option == '胜率'):
        for i in range(max_size):
            num = len(dataframe.loc[dataframe['+{0}'.format(i)] > 1, '+{0}'.format(i)])
            total_num = len(dataframe['+{0}'.format(i)].dropna())
            average.append(num / total_num)
        dataframe.loc['平均'] = average
        dataframe.to_csv('业绩超预期/胜率情况/胜率{0}.csv'.format(year), encoding='utf_8_sig')
    elif (option == '盈亏比'):
        for i in range(max_size):
            positive = dataframe.loc[dataframe['+{0}'.format(i)] > 0, '+{0}'.format(i)].values
            negative = dataframe.loc[dataframe['+{0}'.format(i)] < 0, '+{0}'.format(i)].values
            num = len(positive) / len(negative)
            average.append(num)
        dataframe.loc['盈亏比'] = average
        dataframe.to_csv('业绩超预期/盈亏比/盈亏比{0}.csv'.format(year), encoding='utf_8_sig')
    elif (option == '累计收益'):
        for i in range(max_size):
            num = dataframe['+{0}'.format(i)].dropna().values
            average.append(np.mean(num))
        dataframe.loc['平均'] = average
        dataframe.to_csv('业绩超预期/累计收益/平均累计收益{0}.csv'.format(year), encoding='utf_8_sig')



def get_cum_return(year): #获取超额累积
    df = pd.read_csv('hq/zz500收盘价/zz500日收盘价2009到现在.csv', header='infer')
    df['日期']=pd.to_datetime(df['日期'])
    df2 = pd.read_csv('业绩超预期/累计收益/平均累计收益{0}.csv'.format(year))
    unique_date = [df2.iloc[i, 0][2:12] for i in range(len(df2.iloc[:, 0]))]
    unique_code = [df2.iloc[i, 0][15:-1] for i in range(len(df2.iloc[:, 0]))]
    unique_date.pop()
    unique_code.pop()
    multi_index = pd.MultiIndex.from_arrays([unique_date, unique_code], names=['实际净利润公告日期', '交易代码'])
    size = []
    data_value = []
    for i in range(len(df2) - 1):
        xlength = len(df2.iloc[i, 1:].dropna())
        startdate = pd.Timestamp(unique_date[i]).strftime("%Y-%m-%d")
        sub_df = df.loc[df['日期'] >= startdate]
        sub_df = sub_df.iloc[:xlength, :]
        index_c_return = [sub_df.iloc[j, 1] / sub_df.iloc[0, 1] - 1 for j in range(len(sub_df))]
        stock_c_return = df2.iloc[i, 1:].dropna().values
        x_data = stock_c_return - index_c_return
        size.append(len(x_data))
        data_value.append(x_data)
    dataframe = pd.DataFrame(data_value, index=multi_index,
                                 columns=['+{0}'.format(i) for i in range(max(size))])
    average = []
    for i in range(max(size)):
        num = np.mean(dataframe['+{0}'.format(i)].dropna().values)
        average.append(num)
    dataframe.loc['平均'] = average
    dataframe.to_csv('业绩超预期/超额累计收益/超额累计收益{0}.csv'.format(year), header=True, index=True, encoding='utf_8_sig')

def process_info(year,option): #获取超额胜率和超额盈亏比
    df = pd.read_csv('业绩超预期/超额累计收益/超额累计收益{0}.csv'.format(year), index_col=0, encoding='utf_8_sig')
    sub_df = df.iloc[:-1,:]
    if (option == '超额盈亏比'):
        win_loss = []
        list = [  (len(sub_df.loc[sub_df['+{0}'.format(i)]>0])
                    /len(sub_df.loc[sub_df['+{0}'.format(i)]<0])) for i in range(1,len(df.iloc[0,:]),1)  ]
        win_loss.append(list)
        dataframe = pd.DataFrame(win_loss)
        dataframe.to_csv('业绩超预期/超额盈亏比/超额盈亏比{0}.csv'.format(year))
    elif (option == '超额胜率'):
        win_rate=[]
        list = [(len(sub_df.loc[sub_df['+{0}'.format(i)] > 0])
                 / len(sub_df['+{0}'.format(i)].dropna())) for i in range(1, len(df.iloc[0, :]), 1)]
        win_rate.append(list)
        dataframe=pd.DataFrame(win_rate)
        dataframe.to_csv('业绩超预期/超额胜率/超额胜率{0}.csv'.format(year))

def ten_year_graph(option): #画十年图
    data=[]
    for i in range(2009, 2019, 1):
        if (option=='胜率'):
            dataframe = pd.read_csv('业绩超预期/胜率情况/胜率{0}.csv'.format(i),index_col=0)
        elif (option=='累计收益'):
            dataframe = pd.read_csv('业绩超预期/累计收益/平均累计收益{0}.csv'.format(i),index_col=0)
        elif (option=='盈亏比'):
            dataframe = pd.read_csv('业绩超预期/盈亏比/盈亏比{0}.csv'.format(i), index_col=0)
        elif (option=='超额累计收益'):
            dataframe = pd.read_csv('业绩超预期/超额累计收益/超额累计收益{0}.csv'.format(i), index_col=0)
        elif (option=='超额胜率'):
            dataframe = pd.read_csv('业绩超预期/超额胜率/超额胜率{0}.csv'.format(i), index_col=0)
        elif (option=='超额盈亏比'):
            dataframe = pd.read_csv('业绩超预期/超额盈亏比/超额盈亏比{0}.csv'.format(i), index_col=0)
        subdata=dataframe.iloc[-1, :]
        # subdata.plot(grid=True,figsize=(25,15), label=i, linewidth=2)
        data.append(subdata.values)
    # plt.legend()
    # plt.title(option)
    # plt.show()
    max_size=max([len(data[i]) for i in range(len(data))])
    column_1 = ['+' + str(i) for i in range(max_size)]
    dataframe = pd.DataFrame(data, columns=column_1)
    average=[]
    for i in range(max_size):
        num = np.mean(dataframe['+{0}'.format(i)].dropna().values)
        average.append(num)
    dataframe.loc['平均'] = average
    # print(dataframe)
    dataframe.iloc[-1, :].plot(grid=True, figsize=(25, 15), label='{0}'.format(option),linewidth=2)
    plt.legend()
    plt.title('{0}(10年平均)'.format(option))
    # plt.show()

def read_file():
    path = os.path.expanduser(r"~/Desktop/classes/管理层持股增变化情况.csv")
    df = pd.read_csv(path)
    print(df)


#main:
# get_index_price()
# update_day_业绩超预期()

# for i in range(2009,2019,1):
    # get_graph_info(i,'累计收益')
    # get_graph_info(i, '胜率')
    # get_graph_info(i, '盈亏比')

# for i in range(2009,2019,1):
#     get2D_csv(2009,'胜率')
# for i in range(2009,2019,1):
#     get_cum_return(i)
#     process_info(i,'超额胜率')
#     process_info(i, '超额盈亏比')

# plt.figure(figsize=(20,20), dpi=80)
# plt.figure(1)
# plt.subplot(231)
# ten_year_graph('累计收益')
# plt.subplot(232)
# ten_year_graph('胜率')
# plt.subplot(233)
# ten_year_graph('盈亏比')
# plt.subplot(234)
# ten_year_graph('超额累计收益')
# plt.subplot(235)
# ten_year_graph('超额胜率')
# plt.subplot(236)
# ten_year_graph('超额盈亏比')
# plt.show()

# read_file()