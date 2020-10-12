import pandas as pd
import cx_Oracle


def update_day_hq():#获取后复权行情数据
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    date = pd.read_csv('hq/day_hq.csv').iloc[-1]['date'].replace('-', '')
    #date = '20040101'
    cursor.execute("select F2_1425 as tdate, F16_1090 as code, F3_1425 as last_close, F4_1425 as open, F5_1425 as high, F6_1425 as low, F7_1425 as close, (case when F8_1425=0 then F7_1425 else ROUND(F9_1425/F8_1425*10*F10_1425,2) end) as vwap, F9_1425 as amount FROM TB_OBJECT_1090, TB_OBJECT_1425 where F4_1090='A' and F2_1090=F1_1425 and F2_1425>{0}".format(date))
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
    hq.sort_values(['date', 'code']).to_csv('hq/day_hq.csv', mode='a', header=False, index=False)

def update_lt_value():#获取流通市值
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    date = pd.read_csv('hq/lt_value.csv').iloc[-1]['date'].replace('-', '')
    #date = '20040101'
    cursor.execute("select F2_5004, F4_0001, F10_5004 from TB_OBJECT_5004,TB_OBJECT_0001 where F1_5004=F16_0001 and F12_0001='A' and F2_5004>{0}".format(date))
    df = pd.DataFrame(cursor.fetchall(), columns=['date', 'code', 'lt_value'])
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['date', 'code']).to_csv('hq/lt_value.csv', mode='a', header=False, index=False)

def update_zz800_cons():#获取中证800历史成分股
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    # date = pd.read_csv(open('cons/中证800.csv')).iloc[-1]['date'].replace('-', '')
    date = '20100101'
    cursor.execute("select F3_1807, F16_1090, OB_OBJECT_NAME_1090, F4_1807 from TB_OBJECT_1807,TB_OBJECT_1090 where F1_1807='S24133' and F2_1807=F2_1090 and F3_1807>{0}".format(date))
    df = pd.DataFrame(cursor.fetchall(), columns=['date', 'code', 'name', 'weight'])
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['date', 'code']).to_csv('cons/中证800.csv', mode='a', header=False, index=False, encoding='utf_8_sig')

def update_zz500_cons():
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    # date = pd.read_csv(open('cons/中证500.csv')).iloc[-1]['date'].replace('-', '')
    date = '20100101'
    cursor.execute("select F3_1807, F16_1090, OB_OBJECT_NAME_1090, F4_1807 from TB_OBJECT_1807,TB_OBJECT_1090 where F1_1807='S24125' and F2_1807=F2_1090 and F3_1807>{0}".format(date))
    df = pd.DataFrame(cursor.fetchall(), columns=['date', 'code', 'name', 'weight'])
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['date', 'code']).to_csv('cons/中证500.csv', mode='a', header=False, index=False,encoding='utf_8_sig')

def update_zz1000_cons():
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    # date = pd.read_csv(open('cons/中证1000.csv')).iloc[-1]['date'].replace('-', '')
    date = '20100101'
    cursor.execute("select F3_1807, F16_1090, OB_OBJECT_NAME_1090, F4_1807 from TB_OBJECT_1807,TB_OBJECT_1090 where F1_1807='S5096626' and F2_1807=F2_1090 and F3_1807>{0}".format(date))
    df = pd.DataFrame(cursor.fetchall(), columns=['date', 'code', 'name', 'weight'])
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['date', 'code']).to_csv('cons/中证1000.csv', mode='a', header=False, index=False,encoding='utf_8_sig')

def update_hs300_cons():
    conn = cx_Oracle.connect('wind/wind@172.16.50.232/dfcf')
    cursor = conn.cursor()
    # date = pd.read_csv(open('cons/沪深300.csv')).iloc[-1]['date'].replace('-', '')
    date = '20100101'
    cursor.execute("select F3_1807, F16_1090, OB_OBJECT_NAME_1090, F4_1807 from TB_OBJECT_1807,TB_OBJECT_1090 where F1_1807='S12426' and F2_1807=F2_1090 and F3_1807>{0}".format(date))
    df = pd.DataFrame(cursor.fetchall(), columns=['date', 'code', 'name', 'weight'])
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['date', 'code']).to_csv('cons/沪深300.csv', mode='a', header=False, index=False,encoding='utf_8_sig')

#main:
# update_day_hq()
# update_lt_value()
# update_zz800_cons()
update_zz500_cons()
update_zz1000_cons()
update_hs300_cons()
