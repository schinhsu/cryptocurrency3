from .. import columns
from .. import transform_balance

import datetime
import pandas

## columns = ['BlockNo','TxID','Date','From','To','Value','TxFee','Token','Contract','TXType',]
## 可再多新增 += ['FromContract','ToContract','FromLabel','ToLabel']
def get_transfer_tron_desc(tronObj,addr,start=datetime.datetime(2010,1,1),
                           end=datetime.datetime.now(),
                           transType='TRC20',limit=100000,debugMode=False):
    ##依transType不同，回傳的key值不同
    if transType == 'TRC10' or transType == 'Internal':
        collects = {'data':[],'contractMap':{}}
        time_colname = 'timestamp'
    else:
        collects = {'token_transfers':[],'contractInfo':{}}
        time_colname = 'block_ts'
    
    ##起迄時間設定相同，最多就是回傳10000筆、每次只回傳50次(使用startnum控制回傳資料的序號)
    pageNo = 0
    pageLimit = 200 #10000/50
    starttime = start
    endtime = end
    while True:
        if debugMode:
            print(f'下載錢包位址{addr} 時間 {starttime} 至 {endtime} (第{pageNo+1}頁)')
        res = tronObj.get_transfer_once(addr,startnum=pageNo*50,
                                            start=starttime,end=endtime,
                                              transType=transType)
        if transType == 'TRC10' or transType == 'Internal':
            collects['data'] += res['data']
            if 'contractMap' in res.keys():
                collects['contractMap'].update(res['contractMap'])
            txs = res['data']
            downloadNum = len(collects['data'])
        else:
            collects['token_transfers'] += res['token_transfers']
            if 'contractInfo' in res.keys():
                collects['contractInfo'].update(res['contractInfo'])
            txs = res['token_transfers']
            downloadNum = len(collects['token_transfers'])
            
        if debugMode:
            ## 下載情形
            if len(txs) == 0:
                print(f'> 查詢期間共 {res["rangeTotal"]} 筆、此次下載 {len(txs)} 筆')
            else:
                print(f'> 查詢期間共 {res["rangeTotal"]} 筆、此次下載 {len(txs)} 筆、資料最早時間 {pandas.to_datetime(txs[-1][time_colname],unit="ms")}')
    
        # 不用再繼續下載的情況:
        ##1.如果下載筆數未超過50筆、startnum已經超過查詢期間的筆數
        if len(txs) < 50 or pageNo*50 > res['rangeTotal']:
            break
        ##2.如果資料最早時間已經比當初設定的還要早
        if pandas.to_datetime(txs[-1][time_colname],unit='ms') < start:
            break
        ##3.如果已經下載的資料筆數超過當初設定的上限值
        if downloadNum >= limit:
            break
        
        pageNo += 1 #翻頁
        # 超過頁數上限就要調整查詢時間的迄值，並將頁數重新歸零
        if pageNo >= pageLimit:
            endtime = pandas.to_datetime(txs[-1][time_colname],unit='ms')
            pageNo = 0
    
    dfCollect = json2df_tron(tronObj,collects,transType=transType)
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

def json2df_tron(tronObj,response,transType):
    if len(response) == 0:
        return pandas.DataFrame(columns=columns)
    txinfos = []

    if transType == 'TRC10':
        for tx in response['data']:
            block = tx['block']
            txid = tx['hash']
            txtime = pandas.to_datetime(tx['timestamp'],unit='ms')+datetime.timedelta(hours=8)
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
            txtime = pandas.to_datetime(tx['timestamp'],unit='ms')+datetime.timedelta(hours=8)
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
            txtime = pandas.to_datetime(tx['block_ts'],unit='ms')+datetime.timedelta(hours=8)
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
    
    if not 'FromLabel' in dfTxs.columns:
        for addr in dfTxs['From'].drop_duplicates():
            #print(addr)
            addr_info = tronObj.get_account_detailed_info(addr)
            label = ''
            if 'addressTag' in addr_info.keys():
                label = addr_info['addressTag']
            dfTxs.loc[dfTxs['From']==addr,'FromLabel'] = label
    if not 'ToLabel' in dfTxs.columns:
        for addr in dfTxs['To'].drop_duplicates():
            #print(addr)
            addr_info = tronObj.get_account_detailed_info(addr)
            label = ''
            if 'addressTag' in addr_info.keys():
                label = addr_info['addressTag']
            dfTxs.loc[dfTxs['To']==addr,'ToLabel'] = label
    return dfTxs