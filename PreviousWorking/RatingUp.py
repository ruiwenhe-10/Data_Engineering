import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import cx_Oracle
# from WindPy import *
from datetime import *

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS'] #MacOS
# plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

def get_grade(): #获取评级
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    date = '20090101'
    date2 = '20181231'
    # cursor.execute("select F1_1569 as code, F2_1569 as org_name, F3_1569 as analyst, F4_1569 as date, F5_1569 as deadline, F10_1569 as standard_grade, F15_1569 as grade, F19_1569 as type, F21_1569 as pre_grade, F22_1569 as pre_standard_grade, F23_1569 as direction, F29_1569 as view FROM TB_OBJECT_1569 where F4_1569 > {0}".format(date))
    cursor.execute("select F16_1090, F2_1569, F3_1569, F4_1569, F5_1569, F15_1569, F21_1569, F2_0003, F1_0003 FROM TB_OBJECT_0003, TB_OBJECT_1569 ,TB_OBJECT_1090 where F23_1569=F3_0003 and F1_1569 = F2_1090 and F4_1090 = 'A'  and F4_1569 > {0} and F4_1569 < {1}".format(date, date2))
    df = pd.DataFrame(cursor.fetchall(), columns = ['交易代码','机构名称','分析师名称','评级日期','评级有效期截止日','本次评级','前次评级','类型名称','分类'])
    df['评级日期'] = pd.to_datetime(df['评级日期'])
    # df = pd.DataFrame(cursor.fetchall())
    # df.sort_values(['code', 'date']).to_csv('分析师评级/评级信息.csv', mode='a', header=True, index=False)
    df.sort_values(['交易代码','评级日期']).to_csv('分析师评级上调/评级信息.csv', header=True, index=False,encoding='utf_8_sig')


def update_day_hq():#获取后复权行情数据
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    # date = pd.read_csv('hq/day_hq.csv').iloc[-1]['date'].replace('-', '')
    date1 = '20190701'
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
    hq.sort_values(['code','date']).to_csv('股票.csv',mode='a',header=False, index=False)

def get_upgrade():
    df = pd.read_csv('分析师评级上调/评级信息.csv',header='infer')
    improved = df.loc[(df['类型名称']=='调高')&(df['本次评级']=='买入')]
    for i in range(2009,2019,1):
        set=[]
        sub = improved.loc[(improved['评级日期']>="{0}0101".format(i-1))&(improved['评级日期']<"{0}1231".format(i))]
        names = sub['交易代码'].drop_duplicates(keep='first',inplace=False).values
        for key in names:
            sub_df = sub.loc[sub['交易代码']==key]
            list = []
            for item in sub_df.values:
                if item[3][:7] not in list:
                    list.append((item[3][:7]))
                    set.append(item)
        a = pd.DataFrame(set,columns = ['交易代码','机构名称','分析师名称','评级日期','评级有效期截止日','本次评级','前次评级','类型名称','分类'])
        a.sort_values(['交易代码','评级日期']).to_csv('分析师评级上调/每年评级提升公司/{0}.csv'.format(i),index=False,encoding='utf_8_sig')

def get_graph_info(year,option): #获取每股对应后80天股价信息
    df = pd.read_csv('hq/day_hq.csv',  #股价
                         names=['date', 'code', 'last_close', 'open', 'high', 'low', 'close', 'vwap', 'amount'])
    df2 = pd.read_csv('分析师评级上调/每年评级提升公司/{0}.csv'.format(year),header='infer',encoding='utf_8_sig')
    unique_code=[]
    unique_date=[]
    delete = []
    data_value = []
    size = []
    for i in range(len(df2)):
        code = df2.loc[i, '交易代码']
        date = df2.loc[i, '评级日期']
        if code in df['code'].values:
            original_date = pd.Timestamp(df2.loc[i, '评级日期'])
            after_130_date = (original_date + pd.Timedelta(days=130)).strftime('%Y-%m-%d')
            original_date = original_date.strftime('%Y-%m-%d')
            # print('origindate: {0} enddate: {1}'.format(original_date, after_130_date))
            sample = df.loc[(df['code']==code)&(df['date']<after_130_date)&(df['date']>=original_date)]
            sample = sample.iloc[:80,:]
            if (len(sample) != 80):
                delete.append((code,date))
                continue
            if (option == '胜率'):
                sample.insert(9, 'win_ratio', sample['close'] / sample['last_close'].values)
                num = sample['win_ratio'].values
            elif (option == '盈亏比'):
                sample.insert(9, 'revenue', (sample['close'] - sample['last_close']).values)
                num = sample['revenue'].values
            # print(data_value)
            elif (option == '累计收益'):
                # print(sub_df)
                sample.insert(9, 'cum_return', (sample['close'] / sample.iloc[0, 6] - 1).values)
                num = sample['cum_return'].values
            else:
                print('无正确选择')
            unique_code.append(code)
            unique_date.append(date)
            size.append(len(num))
            data_value.append(num)
            # sample.to_csv('分析师评级上调/每年增持股票股价情况/股价{0}.csv'.format(year),mode='a',index=False,header=False,encoding='utf_8_sig')
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
        dataframe.to_csv('分析师评级上调/胜率/胜率{0}.csv'.format(year), encoding='utf_8_sig')
    elif (option == '盈亏比'):
        for i in range(max_size):
            positive = dataframe.loc[dataframe['+{0}'.format(i)] > 0, '+{0}'.format(i)].values
            negative = dataframe.loc[dataframe['+{0}'.format(i)] < 0, '+{0}'.format(i)].values
            num = len(positive) / len(negative)
            average.append(num)
        dataframe.loc['盈亏比'] = average
        dataframe.to_csv('分析师评级上调/盈亏比/盈亏比{0}.csv'.format(year), encoding='utf_8_sig')
    elif (option == '累计收益'):
        for i in range(max_size):
            num = dataframe['+{0}'.format(i)].dropna().values
            average.append(np.mean(num))
        dataframe.loc['平均'] = average
        dataframe.to_csv('分析师评级上调/累计收益/平均累计收益{0}.csv'.format(year), encoding='utf_8_sig')

def get_cum_return(year): #获取超额累积
    df = pd.read_csv('hq/中证500收盘价/中证500日收盘价2009到现在.csv', header='infer')
    df2 = pd.read_csv('分析师评级上调/累计收益/平均累计收益{0}.csv'.format(year))
    df['日期'] = pd.to_datetime(df['日期'])
    unique_date = [df2.iloc[i, 0][2:12] for i in range(len(df2.iloc[:, 0]))]
    unique_code = [df2.iloc[i, 0][15:-1] for i in range(len(df2.iloc[:, 0]))]
    unique_date.pop()
    unique_code.pop()
    multi_index = pd.MultiIndex.from_arrays([unique_date, unique_code], names=['实际净利润公告日期', '交易代码'])
    size = []
    data_value = []
    for i in range(len(df2) - 1):
        xlength = len(df2.iloc[i, 1:].dropna())
        startdate = unique_date[i]
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
    dataframe.to_csv('分析师评级上调/超额累计收益/超额累计收益{0}.csv'.format(year), header=True, index=True, encoding='utf_8_sig')

def process_info(year,option): #获取超额胜率和超额盈亏比
    df = pd.read_csv('分析师评级上调/超额累计收益/超额累计收益{0}.csv'.format(year), index_col=0, encoding='utf_8_sig')
    sub_df = df.iloc[:-1,:]
    if (option == '超额盈亏比'):
        win_loss = []
        list = [  (len(sub_df.loc[sub_df['+{0}'.format(i)]>0])
                    /len(sub_df.loc[sub_df['+{0}'.format(i)]<0])) for i in range(1,len(df.iloc[0,:]),1)  ]
        win_loss.append(list)
        dataframe = pd.DataFrame(win_loss)
        dataframe.to_csv('分析师评级上调/超额盈亏比/超额盈亏比{0}.csv'.format(year))
    elif (option == '超额胜率'):
        win_rate=[]
        list = [(len(sub_df.loc[sub_df['+{0}'.format(i)] > 0])
                 / len(sub_df['+{0}'.format(i)].dropna())) for i in range(1, len(df.iloc[0, :]), 1)]
        win_rate.append(list)
        dataframe=pd.DataFrame(win_rate)
        dataframe.to_csv('分析师评级上调/超额胜率/超额胜率{0}.csv'.format(year))


def ten_year_graph(option): #画十年图
    data=[]
    for i in range(2009, 2019, 1):
        if (option=='胜率'):
            dataframe = pd.read_csv('分析师评级上调/胜率/胜率{0}.csv'.format(i),index_col=0)
        elif (option=='累计收益'):
            dataframe = pd.read_csv('分析师评级上调/累计收益/平均累计收益{0}.csv'.format(i),index_col=0)
        elif (option=='盈亏比'):
            dataframe = pd.read_csv('分析师评级上调/盈亏比/盈亏比{0}.csv'.format(i), index_col=0)
        elif (option=='超额累积收益'):
            dataframe = pd.read_csv('分析师评级上调/超额累计收益/超额累计收益{0}.csv'.format(i), index_col=0)
        elif (option=='超额胜率'):
            dataframe = pd.read_csv('分析师评级上调/超额胜率/超额胜率{0}.csv'.format(i), index_col=0)
        elif (option=='超额盈亏比'):
            dataframe = pd.read_csv('分析师评级上调/超额盈亏比/超额盈亏比{0}.csv'.format(i), index_col=0)
        subdata=dataframe.iloc[-1, :]
        # subdata.plot(grid=True,figsize=(30,20), label=i, )
        data.append(subdata.values)
    # plt.legend()
    # plt.title(option)
    max_size=max([len(data[i]) for i in range(len(data))])
    column_1 = ['+' + str(i) for i in range(max_size)]
    dataframe = pd.DataFrame(data, columns=column_1)
    average=[]
    for i in range(max_size):
        num = np.mean(dataframe['+{0}'.format(i)].dropna().values)
        average.append(num)
    dataframe.loc['平均'] = average
    # print(dataframe)
    dataframe.iloc[-1, :].plot(grid=True, figsize=(30, 20), label='{0}'.format(option))
    plt.legend()
    plt.title('{0}(10年平均)'.format(option))
    # plt.show()

# get_grade()
# get_upgrade()
# modify()
# for i in range(2009,2019,1):
#     get_graph_info(i, '累计收益')
#     get_graph_info(i,'胜率')
#     get_graph_info(i,'盈亏比')

# for i in range(2009,2019,1):
#     get_cum_return(i)
#     process_info(i,'超额胜率')
#     process_info(i,'超额盈亏比')
#
plt.figure(figsize=(20,20), dpi=80)
plt.figure(1)
plt.subplot(231)
ten_year_graph('累计收益')
plt.subplot(232)
ten_year_graph('胜率')
plt.subplot(233)
ten_year_graph('盈亏比')
plt.subplot(234)
ten_year_graph('超额累积收益')
plt.subplot(235)
ten_year_graph('超额胜率')
plt.subplot(236)
ten_year_graph('超额盈亏比')
plt.suptitle('分析师评级上调回测情况',fontsize=20)
plt.show()