from .. import transform_balance
from .. import columns
from .. import query_label_tron as query_label
from .. import labels
### 新增錢包位址標記資料儲存

import datetime
import pandas


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
            if debugMode:
                print(f'>> 此次下載筆數未超過50筆或startnum已超過查詢期間總數')
            break
        ##2.如果資料最早時間已經比當初設定的還要早
        if pandas.to_datetime(txs[-1][time_colname],unit='ms') < start:
            if debugMode:
                print(f'>> 下載的資料最早交易時間已經比起始查詢時間早')
            break
        ##3.如果已經下載的資料筆數超過當初設定的上限值
        if downloadNum >= limit:
            if debugMode:
                print(f'>> 下載的資料筆數已超過當初設定的上限值')
            break
        
        pageNo += 1 #翻頁
        # 超過頁數上限就要調整查詢時間的迄值，並將頁數重新歸零
        if pageNo >= pageLimit:
            endtime = pandas.to_datetime(txs[-1][time_colname],unit='ms')
            pageNo = 0

    if error:
        return pandas.DataFrame(columns=columns)
    
    dfCollect = json2df_tron(tronObj,collects,transType=transType)
    dfCollect = dfCollect.drop_duplicates()
    return dfCollect


def get_transfer_tron(tronObj,addr,start=datetime.datetime(2010,1,1),
                          end=datetime.datetime.now(),
                          transType='TRC20',limit=3000,sort='desc',
                          debugMode=False):
    msg = ''
    endtime = end
    error = False
    if sort == 'asc':
        while True:
            res = tronObj.get_transfer_once(addr,startnum=0,
                                         start=start,end=endtime,
                                         transType=transType)
            thisTotal = res['rangeTotal']
            if debugMode:
                print(f'>> 設定查詢時間(迄)= {endtime.strftime("%Y-%m-%d %H:%M:%S")} 共 {thisTotal} 筆；預計調整startnum = min({thisTotal}-{limit},9950) = {max(min(thisTotal-limit,9950),0)}')
            if thisTotal > 1000000:
                msg = f'交易總數超過1000000次，應為交易所或服務商'
                if debugMode:
                    print(msg)
                    error = True
                    break
            if thisTotal > limit:
                offset = min(thisTotal-limit,9950)
                offset = max(offset,0)
                res = tronObj.get_transfer_once(addr,startnum=offset,
                                         start=start,end=endtime,
                                         transType=transType)
                if transType != 'TRC20':
                    time_colname = 'timestamp'
                    txs = res['data']
                else:
                    time_colname = 'block_ts'
                    txs = res['token_transfers']
                if len(txs) == 0:
                    msg = f'異常1:無回傳交易資料(查詢筆數total={thisTotal}、startnum={offset})'
                    if debugMode:
                        print(msg)
                    error = True
                    break
                
                tmptime = pandas.to_datetime(txs[0][time_colname],unit='ms')
                if tmptime > endtime:
                    msg = f'異常2:無法正常下載資料，請檢查是否為交易所或服務商'
                    if debugMode:
                        print(msg)
                    error = True
                    break
                elif tmptime == endtime:
                    endtime = tmptime-datetime.timedelta(seconds=1)
                else:
                    endtime = tmptime
            else:
                break
    
    dfCollect = get_transfer_tron_desc(tronObj,addr,start=start,
                         end=endtime,transType=transType,limit=limit,
                         debugMode=debugMode)
    dfCollect = dfCollect.sort_values(by='Date',ascending=sort=='asc')
    return error,msg,dfCollect

def json2df_tron(tronObj,response,transType):
    txinfos = []

    if transType == 'TRC10':
        for tx in response['data']:
            block = tx['block']
            txid = tx['hash']
            txtime = pandas.to_datetime(tx['timestamp'],unit='ms')+datetime.timedelta(hours=8)
            from_ = tx['ownerAddress']
            
            try:
                txfee = transform_balance(tx['cost']['fee'],decimalLen=6)
            except KeyError:
                txfee = 0
            #有些交易沒有tokenInfo?
            try:
                amount = transform_balance(tx['amount'],decimalLen=tx['tokenInfo']['tokenDecimal'])
                token = tx['tokenInfo']['tokenAbbr']
                contract = ''
                if not tx['tokenInfo'].get('tokenId') is None:
                    contract = tx['tokenInfo']['tokenId']
                txType = tx['tokenInfo']['tokenType']
            except KeyError:
                print('>確認交易',txid)
                amount = transform_balance(tx['amount'],decimalLen=0)
                token = 'TRX'
                contract = ''
                txType = 'trc10'
                
            for to_ in tx['toAddressList']:
                txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                                token,contract,txType])
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns)
        if len(response['contractMap']) > 0:
            dfTxs['FromContract'] = dfTxs.apply(lambda tx:response['contractMap'].get(tx['From'],False),axis=1)
            dfTxs['ToContract'] = dfTxs.apply(lambda tx:response['contractMap'].get(tx['To'],False),axis=1)
        
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
        if len(response['contractMap']) > 0:
            dfTxs['FromContract'] = dfTxs.apply(lambda tx:response['contractMap'][tx['From']],axis=1)
            dfTxs['ToContract'] = dfTxs.apply(lambda tx:response['contractMap'][tx['To']],axis=1)
        
    elif transType == 'TRC20':
        for tx in response['token_transfers']:
            block = tx['block']
            txid = tx['transaction_id']
            txtime = pandas.to_datetime(tx['block_ts'],unit='ms')+datetime.timedelta(hours=8)
            from_ = tx['from_address']
            to_ = tx['to_address']
            try:
                amount = transform_balance(tx['quant'],decimalLen=tx['tokenInfo']['tokenDecimal'])
            except KeyError:
                amount = transform_balance(tx['quant'],decimalLen=0)
            txfee = 0 ##未提供手續費資訊
            try:
                token = tx['tokenInfo']['tokenAbbr']
                contract = tx['tokenInfo']['tokenId']
                txType = tx['tokenInfo']['tokenType']
            except KeyError:
                token = 'TRX'
                contract = 'unknown'
                txType = '待檢查'
                print('>>確認tx是否為假交易',txid)
            
            #以物件記錄錢包標記
            from_label = tx['from_address_tag']['from_address_tag']
            to_label = tx['to_address_tag']['to_address_tag']
            labels.set(from_,blockchain="TRX", data_source="tronscan",tag_name=from_label)
            labels.set(to_,blockchain="TRX", data_source="tronscan",tag_name=to_label)
            
            from_contract = not response['contractInfo'].get(from_) is None
            to_contract = not response['contractInfo'].get(to_) is None
            txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                            token,contract,txType,from_contract,to_contract,
                            from_label,to_label,
                           ])
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns+['FromContract','ToContract','FromLabel','ToLabel'])

    
    if not 'FromLabel' in dfTxs.columns:
        if dfTxs.empty:
            dfTxs['FromLabel'] = []
        else:
            dfTxs['FromLabel'] = dfTxs.apply(lambda tx:query_label(tronObj,tx['From'],tx['FromContract']),axis=1)
    if not 'ToLabel' in dfTxs.columns:
        if dfTxs.empty:
            dfTxs['ToLabel'] = []
        else:
            dfTxs['ToLabel'] = dfTxs.apply(lambda tx:query_label(tronObj,tx['To'],tx['ToContract']),axis=1)
    return dfTxs