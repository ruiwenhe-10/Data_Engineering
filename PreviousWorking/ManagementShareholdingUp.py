# from WindPy import *
import numpy as np
import matplotlib.pyplot as plt
import cx_Oracle
import os
import pandas as pd

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS'] #MacOS
# plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号


def get_data(start,end): #丛wind获取员工持股计划
    w.start()
    info = w.wset("esop","startdate={0}-01-01;enddate={1}-12-31".format(start,end))
    col = ['wind代码','证券名称','董事会预案日','股东大会公告日','初始资金规模(万元)','预计持股数量(万股)','占总股本比例','资金来源','股票来源','持股人数','员工认购比例','高管认购比例']
    df = pd.DataFrame(info.Data,index=col,columns=info.Codes)
    df = df.T
    df.sort_values(['证券名称','股东大会公告日','董事会预案日']).to_csv('员工持股计划.csv',index=False,header=True,encoding='utf_8_sig')
    w.stop()

def get_data2(): #丛底层获取高管持股计划
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    date = "20090101"
    cursor.execute("select F16_1090, F2_1842, F3_1842, F4_1842, F5_1842, F6_1842, F7_1842, F8_1842, F9_1842, F10_1842, F12_1842 FROM TB_OBJECT_1842, TB_OBJECT_1090 where F1_1842 = OB_REVISIONS_1090 and F4_1090 = 'A' and F8_1842 > {0}".format(date))
    df = pd.DataFrame(cursor.fetchall(),columns=['交易代码','公司名称','董监高姓名','职务','变动数','变动后持股数','变动原因','变动日期','填报日期','股份变动人姓名','变动人与董监高的关系'])
    df['变动日期'] = pd.to_datetime(df['变动日期'])
    df['填报日期'] = pd.to_datetime(df['填报日期'])
    df.sort_values(['变动日期','交易代码']).to_csv('管理层持股增变化情况.csv',index=False,header=True,encoding='utf_8_sig')

def get_improve(year):
    set=[]
    df = pd.read_csv('管理层持股增变化情况.csv', header='infer')
    modified_df=df.loc[(df['变动数']>0)&(df['变动日期'] >= '{0}0101'.format(year-1))&(df['变动日期']<='{0}1231'.format(year))].sort_values(['交易代码','变动日期'])
    name = modified_df['公司名称'].drop_duplicates(keep='first',inplace=False)
    for key in name:
        sub_df=modified_df.loc[modified_df['公司名称']==key].drop_duplicates(subset=['变动日期'],keep='first',inplace=False)
        list=[]
        for item in sub_df.values:
            if item[7][:7] not in list:
                list.append((item[7][:7]))
                set.append(item)
    df2=pd.DataFrame(set,columns=['交易代码','公司名称','董监高姓名','职务','变动数','变动后持股数','变动原因','变动日期','填报日期','股份变动人姓名','变动人与董监高的关系'])
    df2.to_csv('高管增持/每年增持公司/{0}.csv'.format(year),index=False,header=True,encoding='utf_8_sig')

def get_graph_info(year,option): #获取每股对应后80天股价信息
    df = pd.read_csv('hq/day_hq.csv',  #股价
                         names=['date', 'code', 'last_close', 'open', 'high', 'low', 'close', 'vwap', 'amount'])
    df = df.loc[:,('date','code','last_close','close')]
    df2 = pd.read_csv('高管增持/每年增持公司/{0}.csv'.format(year),header='infer',encoding='utf_8_sig')
    df2 = df2.loc[:,('交易代码','公司名称','董监高姓名','变动数','变动日期','填报日期')]
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
        date = df2.loc[i, '变动日期']
        if code in df['code'].values:
            original_date = pd.Timestamp(df2.loc[i, '变动日期'])
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
        dataframe.to_csv('高管增持/胜率情况/胜率{0}.csv'.format(year), encoding='utf_8_sig')
    elif (option == '盈亏比'):
        for i in range(max_size):
            positive = dataframe.loc[dataframe['+{0}'.format(i)] > 0, '+{0}'.format(i)].values
            negative = dataframe.loc[dataframe['+{0}'.format(i)] < 0, '+{0}'.format(i)].values
            num = len(positive) / len(negative)
            average.append(num)
        dataframe.loc['盈亏比'] = average
        dataframe.to_csv('高管增持/盈亏比/盈亏比{0}.csv'.format(year), encoding='utf_8_sig')
    elif (option == '累计收益'):
        for i in range(max_size):
            num = dataframe['+{0}'.format(i)].dropna().values
            average.append(np.mean(num))
        dataframe.loc['平均'] = average
        dataframe.to_csv('高管增持/累计收益/平均累计收益{0}.csv'.format(year), encoding='utf_8_sig')


def get_cum_return(year): #获取超额累积
    df = pd.read_csv('hq/中证500收盘价/中证500日收盘价2009到现在.csv', header='infer')
    df2 = pd.read_csv('高管增持/累计收益/平均累计收益{0}.csv'.format(year))
    df['日期']=pd.to_datetime(df['日期'])
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
    dataframe.to_csv('高管增持/超额累计收益/超额累计收益{0}.csv'.format(year), header=True, index=True, encoding='utf_8_sig')


def process_info(year,option): #获取超额胜率和超额盈亏比
    df = pd.read_csv('高管增持/超额累计收益/超额累计收益{0}.csv'.format(year), index_col=0, encoding='utf_8_sig')
    sub_df = df.iloc[:-1,:]
    if (option == '超额盈亏比'):
        win_loss = []
        list = [  (len(sub_df.loc[sub_df['+{0}'.format(i)]>0])
                    /len(sub_df.loc[sub_df['+{0}'.format(i)]<0])) for i in range(1,len(df.iloc[0,:]),1)  ]
        win_loss.append(list)
        dataframe = pd.DataFrame(win_loss)
        dataframe.to_csv('高管增持/超额盈亏比/超额盈亏比{0}.csv'.format(year))
    elif (option == '超额胜率'):
        win_rate=[]
        list = [(len(sub_df.loc[sub_df['+{0}'.format(i)] > 0])
                 / len(sub_df['+{0}'.format(i)].dropna())) for i in range(1, len(df.iloc[0, :]), 1)]
        win_rate.append(list)
        dataframe=pd.DataFrame(win_rate)
        dataframe.to_csv('高管增持/超额胜率/超额胜率{0}.csv'.format(year))


def ten_year_graph(option): #画十年图
    data=[]
    for i in range(2009, 2019, 1):
        if (option=='胜率'):
            dataframe = pd.read_csv('高管增持/胜率情况/胜率{0}.csv'.format(i),index_col=0)
        elif (option=='累计收益'):
            # print("高管增持/累计收益/平均累计收益{0}.csv".format(i))
            dataframe = pd.read_csv("高管增持/累计收益/平均累计收益{0}.csv".format(i),index_col=0)
        elif (option=='盈亏比'):
            dataframe = pd.read_csv('高管增持/盈亏比/盈亏比{0}.csv'.format(i), index_col=0)
        elif (option=='超额累计收益'):
            dataframe = pd.read_csv('高管增持/超额累计收益/超额累计收益{0}.csv'.format(i), index_col=0)
        elif (option=='超额胜率'):
            dataframe = pd.read_csv('高管增持/超额胜率/超额胜率{0}.csv'.format(i), index_col=0)
        elif (option=='超额盈亏比'):
            dataframe = pd.read_csv('高管增持/超额盈亏比/超额盈亏比{0}.csv'.format(i), index_col=0)
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

def find_stock(year,option):
    dataframe = pd.read_csv('cons/{0}.csv'.format(option), names=['date', 'code', 'name', 'weight'], encoding='utf_8_sig')
    found=[]
    df = pd.read_csv('hq/业绩超预期/业绩超预期{0}.csv'.format(year),header='infer',encoding='utf_8_sig')
    date = df['实际净利润公告日期'].values
    tradecode= df['交易代码'].values
    unique = dataframe.drop_duplicates(subset=['date'], keep='first', inplace=False)
    unique_date = unique['date'].values
    for i in range(len(df)):
        if date[i] in unique_date:
            if tradecode[i] in dataframe.loc[dataframe['date']==date[i],'code'].values:
                list = dataframe.loc[(dataframe['date'] == date[i])&(dataframe['code'] == tradecode[i]),('date','code','name')].values[0]
                sub_list=[date[i]]
                [sub_list.append(list[i]) for i in range(1,len(list))]
                found.append(sub_list)
        elif tradecode[i] in dataframe['code'].values:
            list = dataframe.loc[(dataframe['code'] == tradecode[i])].values
            name = list[0][2]
            month = [list[i][0][:7] for i in range(len(list))]
            if date[i][:7] in month:
                sub_list=[date[i],tradecode[i],name]
                found.append(sub_list)
    found_df=pd.DataFrame(found,index=['{0}'.format(year) for i in range(len(found))],columns=['财报公告日','交易代码','公司名称'])
    result = [[len(df),len(found_df), len(found_df)/len(df)]]
    result_pd = pd.DataFrame(result,index=['{0}'.format(year)],columns=['总共超预期数量','{0}成分股数量'.format(option),'比重'])
    found_df.to_csv('业绩超预期{0}成分股（十年）.csv'.format(option),mode='a',index=True,header=False,encoding='utf_8_sig')
    result_pd.to_csv('业绩超预期{0}成分股比重（十年）.csv'.format(option),mode='a',index=True,header=False,encoding='utf_8_sig')

# get_data2()
# for i in range(2009,2019,1):
#     get_improve(i)
# for i in range(2009,2019,1):
#     get_graph_info(2018,'累计收益')
#     get_graph_info(i, '胜率')
#     get_graph_info(i, '盈亏比')
#     get_cum_return(i)
#     process_info(i,'超额盈亏比')
#     process_info(i,'超额胜率')

plt.figure(figsize=(20,20), dpi=80)
plt.figure(1)
plt.subplot(231)
ten_year_graph('累计收益')
plt.subplot(232)
ten_year_graph('胜率')
plt.subplot(233)
ten_year_graph('盈亏比')
plt.subplot(234)
ten_year_graph('超额累计收益')
plt.subplot(235)
ten_year_graph('超额胜率')
plt.subplot(236)
ten_year_graph('超额盈亏比')
plt.suptitle('高管增持回测情况',fontsize=20)
plt.show()