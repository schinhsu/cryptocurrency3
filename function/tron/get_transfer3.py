from .. import columns
from .. import transform_balance

import datetime
import pandas

## columns = ['BlockNo','TxID','Date','From','To','Value','TxFee','Token','Contract','TXType']
def get_transfer_tron_desc(tronObj,addr,start=datetime.datetime(2010,1,1),
                           end=datetime.datetime.now(),
                           transType='TRC20',limit=100000,debugMode=False):
    if transType == 'TRC10' or transType == 'Internal':
        collects = {'data':[],'contractMap':{}}
    else:
        collects = {'token_transfers':[],'contractInfo':{}}

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
        if transType == 'TRC10' or transType == 'Internal':
            collects['data'] += res['data']
            if 'contractMap' in res.keys():
                collects['contractMap'].update(res['contractMap'])
            txs = res['data']
        else:
            collects['token_transfers'] += res['token_transfers']
            if 'contractInfo' in res.keys():
                collects['contractInfo'].update(res['contractInfo'])
            txs = res['token_transfers']
        

        if debugMode:
            if len(txs) == 0:
                print(len(txs),pageNo*50,res['rangeTotal'])
            else:
                print(len(txs),pageNo*50,res['rangeTotal'],pandas.to_datetime(txs[-1][time_colname],unit='ms'))
        if len(txs) < 50 or pageNo*50 > res['rangeTotal']:
            break
        if pandas.to_datetime(txs[-1][time_colname],unit='ms') < start:
            break
        if len(collects) >= limit:
            break
        pageNo += 1
        
        if pageNo >= pageLimit:
            endtime = pandas.to_datetime(txs[-1][time_colname],unit='ms')
            pageNo = 0

    dfCollect = json2df_tron(collects,transType=transType)
    dfCollect = dfCollect.drop_duplicates()
    #dfCollect = dfCollect[(dfCollect['Date']>=start)&(dfCollect['Date']<=end)]
    return dfCollect

def get_transfer_tron(tronObj,addr,start=datetime.datetime(2010,1,1),
                          end=datetime.datetime.now(),
                          transType='TRC20',limit=500,sort='desc',
                          totalLimit=100000,debugMode=False):
    def set_starttime():
        res = tronObj.get_transfer_once(addr,startnum=0,
                                        start=start,end=end,
                                            transType=transType)
        if res['rangeTotal'] > totalLimit:
            if debugMode:
                print(f'***查詢期間交易總量超過容許值={res["rangeTotal"]}，可能為交易所')
            return False,"疑似交易所"
        if res['rangeTotal'] == 0:
            if debugMode:
                print(f'***查詢期間無交易，請確認設定值')
            return False,"查無交易"
        return True,start
    def set_endtime():
        starttime = start
        endtime = end
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
                endtime = pandas.to_datetime(txs[0][time_colname],unit='ms')
                break
            
            tmptime = pandas.to_datetime(txs[-1][time_colname],unit='ms')
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
                     end=setInfo,transType=transType,limit=totalLimit,
                     debugMode=debugMode)
            dfCollect = dfCollect.sort_values(by='Date')
        else:
            dfCollect = json2df_tron({},transType=transType)
    if sort == 'desc':
        setTrue,setInfo = set_starttime()
        if setTrue:
            dfCollect = get_transfer_tron_desc(tronObj,addr,start=setInfo,
                         end=end,transType=transType,limit=totalLimit,
                         debugMode=debugMode)
        else:
            dfCollect = json2df_tron({},transType=transType)
    return setTrue,setInfo,dfCollect

def json2df_tron(response,transType):
    if len(response) == 0:
        return pandas.DataFrame(columns=columns)
    txinfos = []

    if transType == 'TRC10':
        for tx in response['data']:
            block = tx['block']
            txid = tx['hash']
            txtime = pandas.to_datetime(tx['timestamp'],unit='ms')
            from_ = tx['ownerAddress']
            
            amount = transform_balance(tx['amount'],decimalLen=tx['tokenInfo']['tokenDecimal'])
            txfee = transform_balance(tx['cost']['fee'],decimalLen=6)
            token = tx['tokenInfo']['tokenAbbr']
            contract = ''
            if not tx['tokenInfo'].get('tokenId') is None:
                contract = tx['tokenInfo']['tokenId']
            txType = tx['tokenInfo']['tokenType']
            
            for to_ in tx['toAddressList']:
                txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                                token,contract,txType])
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns)
        dfTxs['FromContract'] = dfTxs.apply(lambda tx:response['contractMap'][tx['From']],axis=1)
        dfTxs['ToContract'] = dfTxs.apply(lambda tx:response['contractMap'][tx['To']],axis=1)
        
    elif transType == 'Internal':
        for tx in response['data']:
            block = tx['block']
            txid = tx['hash']
            txtime = pandas.to_datetime(tx['timestamp'],unit='ms')
            from_ = tx['from']
            to_ = tx['to']
            amount = transform_balance(tx['call_value'],decimalLen=tx['token_list']['tokenInfo']['tokenDecimal'])
            txfee = 0 ##未提供手續費資訊
            token = tx['token_list']['tokenInfo']['tokenAbbr']
            contract = tx['token_list']['tokenInfo']['tokenId']
            txType = tx['token_list']['tokenInfo']['tokenType']
            txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                            token,contract,txType])
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns)
        dfTxs['FromContract'] = dfTxs.apply(lambda tx:response['contractMap'][tx['From']],axis=1)
        dfTxs['ToContract'] = dfTxs.apply(lambda tx:response['contractMap'][tx['To']],axis=1)
        
    elif transType == 'TRC20':
        for tx in response['token_transfers']:
            block = tx['block']
            txid = tx['transaction_id']
            txtime = pandas.to_datetime(tx['block_ts'],unit='ms')
            from_ = tx['from_address']
            to_ = tx['to_address']
            amount = transform_balance(tx['quant'],decimalLen=tx['tokenInfo']['tokenDecimal'])
            txfee = 0 ##未提供手續費資訊
            token = tx['tokenInfo']['tokenAbbr']
            contract = tx['tokenInfo']['tokenId']
            txType = tx['tokenInfo']['tokenType']
    
            from_label = tx['from_address_tag']['from_address_tag']
            to_label = tx['to_address_tag']['to_address_tag']
            from_contract = not response['contractInfo'].get(from_) is None
            to_contract = not response['contractInfo'].get(to_) is None
            txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                            token,contract,txType,from_contract,to_contract,
                            from_label,to_label,
                           ])
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns+['FromContract','ToContract','FromLabel','ToLabel'])
    
    
    return dfTxs