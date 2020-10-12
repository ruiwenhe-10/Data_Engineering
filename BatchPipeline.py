from alpha_vantage.fundamentaldata import FundamentalData
import pandas as pd
import json
import requests
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, IntegerType
from pyspark.sql import SparkSession
from pyspark import SparkContext

fd = FundamentalData(key='apikey')
def getAllOverview():
    def getOverview(result_dic, symbol):
        try:
            data =  fd.get_company_overview(symbol=symbol)[0]
            result_dic.update({symbol : data})
        except ValueError:
            print('Stock {0} has error'.format(symbol))
    stockInfo  = pd.read_table('NASDAQ.txt')
    result = dict()
    for a in stockInfo['Symbol'].values[:5]:
        getOverview(result, a)

def getAllList():
    response = requests.get("https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=apikey")
    file1 = open("stocklist.txt", "w")
    file1.write(response.text)

def getStock(symbols):
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={0}&outputsize=full&apikey=apikey"
    result = []
    # result = ''
    for symbol in symbols:
        try:
            response = requests.get(url.format(symbol))
            if not response.ok:
                print(symbol)
                continue
            content = response.json()
            for s in content['Time Series (Daily)'].items():
                s[1].update({'code' : content['Meta Data']['2. Symbol']})
                s[1].update({'date' : s[0]})
                # result+=json.dumps(s[1])+'\n'
                result.append(json.dumps(s[1]))
        except KeyError:
            print('Stock {0} has error'.format(symbol))
            print(json.dumps(content,indent=2))
        except ValueError:
            print('Stock {0} has error'.format(symbol))
        # break
    # result += '}'
    return result

def testSpark():
    sc = SparkContext(master="local")
    sqlContext = SparkSession.builder.master("local").appName("getStock").getOrCreate()
    symbols = pd.read_table('stocklist.txt', sep=',')['symbol'].values
    while (len(symbols) > 0):
        data = getStock(symbols[:30])
        symbols = symbols[30:]
        print("phase 1 success")
        RDD = sc.parallelize(data).map(lambda x : json.loads(x))
        print("phase 2 success")
        df = sqlContext.createDataFrame(RDD).toPandas()
        df.columns = ['open', 'high', 'low', 'close', 'adjusted close','volume','dividend amount', 'split coefficient', 'code', 'date']
        df['date'] = pd.to_datetime(df['date'])
        df['adjusted close'] = df['adjusted close'].astype(float).round(2)
        df['open'] = df['open'].astype(float).round(2)
        df['high'] = df['high'].astype(float).round(2)
        df['low'] = df['low'].astype(float).round(2)
        df['close'] = df['close'].astype(float).round(2)
        df['volume'] = df['volume'].astype(int)
        df['dividend amount'] = df['dividend amount'].astype(float).round(2)
        df['split coefficient'] = df['split coefficient'].astype(float).round(2)
        df = df.set_index("date").sort_values(['code','date'])
        # print(df.head())
        print('phase 3 success')
        df.to_csv('stockinfo/result.csv',mode='a',header=False)

# def upload_blob(bucket_name, source_file_name, destination_blob_name):
def upload_blob(file):
    """Uploads a file to the bucket."""
    from google.cloud import storage
    bucket_name = "ruiwen_test_storage"
    source_file_name = "stockinfo/{0}.csv".format(file)
    destination_blob_name = "{0}.csv".format(file)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

#testing authtication
def implicit():
    from google.cloud import storage
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()
    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)

#main
# getAllList()
# print(getStock())
# print(json.loads(getStock()))
testSpark()
upload_blob("result")
# implicit()
