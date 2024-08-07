from .. import columns
from .. import transform_balance

import datetime
import pytz
import pandas

def get_transfer_tron_desc(tronObj,addr,start=datetime.datetime(2010,1,1,tzinfo=pytz.timezone('Asia/Taipei')),
                           end=datetime.datetime.now(pytz.timezone('Asia/Taipei')),
                           transType='TRC20',limit=100000,debugMode=False):
    collects = []

    pageNo = 0
    pageLimit = 200 #10000/50
    starttime = start
    endtime = end
    time_colname = 'timestamp' if transType != 'TRC20' else 'block_ts'
    while True:
        if debugMode:
            print('page',pageNo,starttime,endtime)
        res = tronObj.get_transfer_once(addr,startnum=pageNo*50,
                                        start=starttime,end=endtime,
                                          transType=transType)
        if transType == 'TRC10':
            txs = res['data']
        elif transType == 'Internal':
            txs = res['data']
        else:
            txs = res['token_transfers']
        
        collects += txs

        if debugMode:
            if len(txs) == 0:
                print(len(txs),pageNo*50,res['rangeTotal'])
            else:
                print(len(txs),pageNo*50,res['rangeTotal'],pandas.to_datetime(txs[-1][time_colname],unit='ms').tz_localize('Asia/Taipei'))
        if len(txs) < 50 or pageNo*50 > res['rangeTotal']:
            break
        if pandas.to_datetime(txs[-1][time_colname],unit='ms').tz_localize('Asia/Taipei') < start:
            break
        if len(collects) >= limit:
            break
        pageNo += 1
        
        if pageNo >= pageLimit:
            endtime = pandas.to_datetime(txs[-1][time_colname],unit='ms').tz_localize('Asia/Taipei')
            pageNo = 0

    dfCollect = json2df_tron(collects,transType=transType)
    dfCollect = dfCollect.drop_duplicates()
    dfCollect = dfCollect[(dfCollect['Date(UTC+8)']>=start)&(dfCollect['Date(UTC+8)']<=end)]
    return dfCollect


def get_transfer_tron(tronObj,addr,start=datetime.datetime(2010,1,1,tzinfo=pytz.timezone('Asia/Taipei')),
                          end=datetime.datetime.now(pytz.timezone('Asia/Taipei')),
                          transType='TRC20',limit=500,sort='desc',
                          totalLimit=100000,debugMode=False):
    def set_starttime():
        starttime = start-datetime.timedelta(hours=8)
        res = tronObj.get_transfer_once(addr,startnum=0,
                                        start=starttime,end=end,
                                            transType=transType)
        if res['rangeTotal'] > totalLimit:
            if debugMode:
                print(f'***查詢期間交易總量超過容許值={res["rangeTotal"]}，可能為交易所')
            return False,"疑似交易所"
        if res['rangeTotal'] == 0:
            if debugMode:
                print(f'***查詢期間無交易，請確認設定值')
            return False,"查無交易"
        return True,starttime
    def set_endtime():
        starttime = start
        endtime = end+datetime.timedelta(hours=8)
        count = 0
        time_colname = 'timestamp' if transType != 'TRC20' else 'block_ts'
        res = tronObj.get_transfer_once(addr,startnum=0,
                                        start=starttime,end=endtime,
                                            transType=transType)
        if res['rangeTotal'] > totalLimit:
            if debugMode:
                print(f'***查詢期間交易總量超過容許值={res["rangeTotal"]}，可能為交易所')
            return False,"疑似交易所"
        if debugMode:
            print('count',0,res['rangeTotal'],0,starttime,endtime)
        count = 0
        while True:
            #先改startnum查一次
            lastTotal = res['rangeTotal']
            if lastTotal <= 10000:
                startnum = max(lastTotal-limit,0)
            else:
                startnum = 10000-limit
            count += 1
            res = tronObj.get_transfer_once(addr,startnum=startnum,
                                                 start=starttime,end=endtime,
                                                 transType=transType)
            if transType == 'TRC10':
                txs = res['data']
            elif transType == 'Internal':
                txs = res['data']
            else:
                txs = res['token_transfers']
            if debugMode:
                print('count1',count,lastTotal,startnum,starttime,endtime,res['rangeTotal'],len(txs))
            
            if len(txs) == 0:
                if res["rangeTotal"] > 0:
                    print(f'***查詢期間交易總量={res["rangeTotal"]}，但API未回傳任何交易')
                    return False,"limit可以調整看看"
                else:
                    return False,"查無交易"

            if lastTotal <= 10000:
                endtime = pandas.to_datetime(txs[0][time_colname],unit='ms').tz_localize('Asia/Taipei')
                break
            
            tmptime = pandas.to_datetime(txs[-1][time_colname],unit='ms').tz_localize('Asia/Taipei')
            if tmptime > endtime:
                #交易數量過大會導致API有問題，反正不追
                if debugMode:
                    print('***疑似查詢期間交易總量過大，導致API回傳的交易時間有問題',tmptime,endtime)
                    print(res.url)
                    print('-----------------------------------------')
                return False,"API異常"
            elif tmptime == endtime:
                endtime = tmptime-datetime.timedelta(seconds=1)
            else:
                endtime = tmptime
            count += 1
            res = tronObj.get_transfer_once(addr,startnum=startnum,
                                                 start=starttime,end=endtime,
                                                 transType=transType)
            if debugMode:
                print('count2',count,lastTotal,startnum,starttime,endtime,res['rangeTotal'])
            
        return True,endtime

    if sort == 'asc':
        setTrue,setInfo = set_endtime()
        if setTrue:
            dfCollect = get_transfer_tron_desc(tronObj,addr,start=start,
                     end=setInfo,transType=transType,limit=limit,
                     debugMode=debugMode)
            dfCollect = dfCollect.sort_values(by='Date(UTC+8)')
        else:
            dfCollect = json2df_tron([],transType=transType)
    if sort == 'desc':
        setTrue,setInfo = set_starttime()
        if setTrue:
            dfCollect = get_transfer_tron_desc(tronObj,addr,start=start,
                         end=end,transType=transType,limit=limit,
                         debugMode=debugMode)
        else:
            dfCollect = json2df_tron([],transType=transType)
    return setTrue,setInfo,dfCollect

def json2df_tron(entries,transType):
    if len(entries) == 0:
        return pandas.DataFrame(columns=columns)

    df = pandas.json_normalize(entries)
    if transType == 'TRC10':
        #有些交易類型會沒有token(ex:vote...)
        df.dropna(subset=['tokenInfo.tokenId'],inplace=True)
        #展開toAddressList欄位
        df = df.explode('toAddressList')
        #展開後如果為空值需要去除(ex:update permission...)
        df.dropna(subset=['toAddressList'],inplace=True)
    
        #token名稱統一大寫
        df['tokenAbbr'] = df['tokenInfo.tokenAbbr'].str.upper()
        df['amount'] = df['amount'].apply(transform_balance,decimalLen=6)
        df['txfee'] = df['cost.fee'].apply(transform_balance,decimalLen=6)
        df['time'] = pandas.to_datetime(df['timestamp'],unit='ms').dt.tz_localize('Asia/Taipei')
    
        dfTrim = df[['block','hash','time','ownerAddress','toAddressList','amount','txfee','tokenAbbr','tokenInfo.tokenType']]
        
    elif transType == 'Internal':
        #token名稱統一大寫
        df['tokenAbbr'] = df['token_list.tokenInfo.tokenAbbr'].str.upper()
        df['amount'] = df[['call_value','token_list.tokenInfo.tokenDecimal']].apply(lambda x: transform_balance(x['call_value'], x['token_list.tokenInfo.tokenDecimal']), axis=1)
        #此API沒有回覆手續費資訊
        df['txfee'] = 0
        df['time'] = pandas.to_datetime(df['timestamp'],unit='ms').dt.tz_localize('Asia/Taipei')
        
        dfTrim = df[['block','hash','time','from','to','amount','txfee','tokenAbbr','token_list.tokenInfo.tokenType']]
    
    elif transType == 'TRC20':
        #不確定會不會有 沒有這兩個欄位的交易
        df.dropna(subset=['to_address','tokenInfo.tokenId'],inplace=True)
    
        #token名稱統一大寫
        df['tokenAbbr'] = df['tokenInfo.tokenAbbr'].str.upper()
        df['amount'] = df[['quant','tokenInfo.tokenDecimal']].apply(lambda x: transform_balance(x['quant'], x['tokenInfo.tokenDecimal']), axis=1)
        #此API沒有回覆手續費資訊
        df['txfee'] = 0
        df['time'] = pandas.to_datetime(df['block_ts'],unit='ms').dt.tz_localize('Asia/Taipei')
        
        dfTrim = df[['block','transaction_id','time','from_address','to_address','amount','txfee','tokenAbbr','contract_address']]
    
    dfTrim.columns = ['BlockNo','TxID','Date(UTC+8)','From','To','Value','TxFee','Token','Contract']
    dfTrim.loc[:,['TXType']] = [transType for _ in range(len(dfTrim))]
    return dfTrim