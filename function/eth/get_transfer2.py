from .. import columns
from .. import transform_balance

import datetime
import pytz
import pandas


def get_transfer_eth(ethObj,addr,start=datetime.datetime(2010,1,1),
                     end=datetime.datetime.now(),
                     transType='ERC20',batchnum=10000,limit=100000,
                     endblock=999999999,startblock=0,sort='desc',
                     debugMode=False):
    addr = addr.lower()

    collects = []

    pageNo = 0
    pageLimit = 10000/batchnum
    while True:
        if pageNo >= pageLimit:
            if sort == 'desc':
                endblock = eval(collects[-1]['blockNumber'])
            elif sort == 'asc':
                startblock = eval(collects[-1]['blockNumber'])
            pageNo = 0
        pageNo += 1
        if debugMode:
            print('page',pageNo,sort,startblock,endblock)
        if transType in ['Normal','Internal','ERC20']:
            res = ethObj.get_transfer_once(addr,batchnum=batchnum,
                                        endblock=endblock,
                                        startblock=startblock,
                                        sort=sort,page=pageNo,
                                          transType=transType)
        ##目前尚未支援ERC721
        elif transType in ['ERC721']:
            res = ethObj.get_transfer_once(addr,batchnum=batchnum,
                                        sort=sort,page=pageNo,
                                          transType=transType)
        
        collects += res

        if debugMode:
            if len(res) == 0:
                print(len(res),batchnum)
            else:
                #print(len(res),batchnum,datetime.datetime.fromtimestamp(eval(res[-1]['timeStamp']))) ###datetime.datetime.fromtimestamp會直接以當地時間進行轉換
                print(len(res),batchnum,pandas.to_datetime(eval(res[-1]['timeStamp']),unit='s'))
        if len(res) < batchnum:
            break
        if sort == 'desc' and eval(res[-1]['timeStamp']) < start.timestamp():
            break
        if sort == 'asc' and eval(res[-1]['timeStamp']) > end.timestamp():
            break
        if len(collects) >= limit:
            break

    dfCollect = json2df_eth(collects,transType)
    dfCollect = dfCollect.drop_duplicates()
    dfCollect = dfCollect[(dfCollect['Date']>=start)&(dfCollect['Date']<=end)]
    return dfCollect

def json2df_eth(entries,transType):
    if len(entries) == 0:
        return pandas.DataFrame(columns=columns)
    
    df = pandas.json_normalize(entries)
    df['Date'] = pandas.to_datetime(df['timeStamp'].astype(int),unit='s')
    
    # Normal交易不會特別寫symbol和tokendecimal
    if not 'tokenSymbol' in df.columns:
        df['tokenSymbol'] = 'ETH'
    if not 'tokenDecimal' in df.columns:
        df['tokenDecimal'] = 18
    # Internal交易不會寫gasPrice
    if not 'gasPrice' in df.columns:
        df['gasPrice'] = 0
    
    df['tokenDecimal'] = pandas.to_numeric(df['tokenDecimal'])
    df['Value'] = df[['value','tokenDecimal']].apply(lambda x: transform_balance(x['value'], x['tokenDecimal']), axis=1)
    df['TxFee'] = df[['gasUsed','gasPrice']].apply(lambda x: eval(x['gasUsed'])*transform_balance(x['gasPrice']), axis=1)
    
    dfTrim = df[['blockNumber','hash','Date','from','to','Value','TxFee','tokenSymbol','contractAddress']]
    dfTrim.columns = ['BlockNo','TxID','Date','From','To','Value','TxFee','Token','Contract']
    #dfTrim['Date(UTC+8)'] = dfTrim['Date'].dt.tz_localize('Asia/Taipei')
    dfTrim.loc[:,['TXType']] = [transType for _ in range(len(dfTrim))]
    return dfTrim