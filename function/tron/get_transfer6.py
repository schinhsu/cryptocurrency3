from .. import transform_balance
from .. import columns
from . import query_addr
### 新增錢包位址標記資料儲存內容(是否為contract)

import datetime
import pandas

def get_transfer_tron_desc(tronObj,addr,start=datetime.datetime(2010,1,1),
                           end=datetime.datetime.now(),
                           transType='TRC20',limit=100000,
                           totalLimit=1000000,
                           debugMode=False):
    msg = ''
    error = False
    dfCollect = pandas.DataFrame(columns=columns)
    
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
        if res['rangeTotal'] > totalLimit:
            msg = f'交易總數超過{totalLimit}次，應為交易所或服務商'
            if debugMode:
                print(msg)
                error = True
                break
        
        df = json2df_tron(tronObj,res,transType=transType)
        dfCollect = pandas.concat([dfCollect,df])
        if debugMode:
            ## 下載情形
            if len(df) == 0:
                print(f'> 查詢期間共 {res["rangeTotal"]} 筆、此次下載 {len(df)} 筆')
            else:
                print(f'> 查詢期間共 {res["rangeTotal"]} 筆、此次下載 {len(df)} 筆、資料最早時間 {df["Date"].min()}')
    
        # 不用再繼續下載的情況:
        ##1.如果下載筆數未超過50筆、startnum已經超過查詢期間的筆數
        if len(df) < 50 or pageNo*50 > res['rangeTotal']:
            if debugMode:
                print(f'>> 此次下載筆數未超過50筆或startnum已超過查詢期間總數')
            break
        ##2.如果資料最早時間已經比當初設定的還要早
        if df['Date'].min() < start:
            if debugMode:
                print(f'>> 下載的資料最早交易時間已經比起始查詢時間早')
            break
        ##3.如果已經下載的資料筆數超過當初設定的上限值
        if len(dfCollect) >= limit:
            if debugMode:
                print(f'>> 下載的資料筆數已超過當初設定的上限值')
            break
        
        pageNo += 1 #翻頁
        # 超過頁數上限就要調整查詢時間的迄值，並將頁數重新歸零
        if pageNo >= pageLimit:
            endtime = df['Date'].min()
            pageNo = 0

    
    dfCollect = dfCollect.drop_duplicates()
    return error,msg,dfCollect

def get_transfer_tron(tronObj,addr,start=datetime.datetime(2010,1,1),
                          end=datetime.datetime.now(),
                          transType='TRC20',limit=3000,
                          totalLimit=1000000,
                          sort='desc',
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
            if thisTotal > totalLimit:
                msg = f'交易總數超過{totalLimit}次，應為交易所或服務商'
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
    if error:
        return error,msg,pandas.DataFrame(columns=columns)
        
    error,msg,dfCollect = get_transfer_tron_desc(tronObj,addr,start=start,
                         end=endtime,transType=transType,limit=limit,
                         totalLimit=totalLimit,debugMode=debugMode)
    dfCollect = dfCollect.sort_values(by='Date',ascending=sort=='asc')
    return error,msg,dfCollect


def json2df_tron(tronObj,response,transType):
    txinfos = []
    trx_info = {'addressTag':{},'contractInfo':{}}
    contract_maps = {}

    if transType == 'TRC10':
        for tx in response['data']:
            block = tx['block']
            txid = tx['hash']
            #交易時間(UTC+8)轉換方式
            txtime = pandas.to_datetime(tx['timestamp'],unit='ms')+datetime.timedelta(hours=8)
            from_ = tx['ownerAddress']
            
            try:
                txfee = transform_balance(tx['cost']['fee'],decimalLen=6)
            except KeyError:
                txfee = None ##手續費統一寫None
            #有些交易沒有tokenInfo
            try:
                amount = transform_balance(tx['amount'],decimalLen=tx['tokenInfo']['tokenDecimal'])
                token = tx['tokenInfo']['tokenAbbr']
                contract = ''
                if not tx['tokenInfo'].get('tokenId') is None:
                    contract = tx['tokenInfo']['tokenId']
                txType = tx['tokenInfo']['tokenType']
            except KeyError:
                print(f'In function <json2df>:\n{transType}交易 {txid} 無tokenInfo內容')
                amount = transform_balance(tx['amount'],decimalLen=0)
                token = 'TRX'
                contract = ''
                txType = 'trc10'
            
            for to_ in tx['toAddressList']:
                txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                                    token,contract,txType])
        
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns)
        contract_maps = response['contractMap']
        
        
    elif transType == 'Internal':
        for tx in response['data']:
            block = tx['block']
            txid = tx['hash']
            #交易時間(UTC+8)轉換方式
            txtime = pandas.to_datetime(tx['timestamp'],unit='ms')+datetime.timedelta(hours=8)
            from_ = tx['from']
            to_ = tx['to']
            try:
                amount = transform_balance(tx['call_value'],decimalLen=tx['token_list']['tokenInfo']['tokenDecimal'])
                txfee = None ##未提供手續費資訊
                token = tx['token_list']['tokenInfo']['tokenAbbr']
                contract = tx['token_list']['tokenInfo']['tokenId']
                txType = tx['token_list']['tokenInfo']['tokenType']
            except Keyrror:
                print(f'In function <json2df>:\n{transType}交易 {txid} 無tokenInfo內容')
                amount = transform_balance(tx['call_value'],decimalLen=0)
                txfee = None
                token = 'unknown'
                contract = 'unknown'
                txType = 'trc20'
            txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                            token,contract,txType])
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns)
        contract_maps = response['contractMap']
        
        
    elif transType == 'TRC20':
        for tx in response['token_transfers']:
            block = tx['block']
            txid = tx['transaction_id']
            #交易時間(UTC+8)轉換方式
            txtime = pandas.to_datetime(tx['block_ts'],unit='ms')+datetime.timedelta(hours=8)
            from_ = tx['from_address']
            to_ = tx['to_address']
            try:
                amount = transform_balance(tx['quant'],decimalLen=tx['tokenInfo']['tokenDecimal'])
                txfee = None ##未提供手續費資訊
                token = tx['tokenInfo']['tokenAbbr']
                contract = tx['tokenInfo']['tokenId']
                txType = tx['tokenInfo']['tokenType']
            except KeyError:
                print(f'In function <json2df>:\n{transType}交易 {txid} 無tokenInfo內容')
                amount = transform_balance(tx['quant'],decimalLen=0)
                txfee = None
                token = 'unknown'
                contract = 'unknown'
                txType = 'trc20'
                
    
            ##改成最後一起apply給值
            trx_info['addressTag'][from_] = tx['from_address_tag']['from_address_tag']
            trx_info['addressTag'][to_] = tx['to_address_tag']['to_address_tag']
            contract_maps[from_] = tx['fromAddressIsContract']
            contract_maps[to_] = tx['toAddressIsContract']
            txinfos.append([block,txid,txtime,from_,to_,amount,txfee,
                            token,contract,txType,
                            # from_contract,to_contract,
                            # from_label,to_label,
                           ])
        dfTxs = pandas.DataFrame(data=txinfos,columns=columns)

    try:
        trx_info['contractInfo'].update(response['contractInfo'])
    except KeyError:
        pass
    if not dfTxs.empty:
        dfTxs[['FromContract','FromLabel']] = dfTxs.apply(lambda tx:pandas.Series(query_addr(tronObj,tx['From'],trx_info,contract_maps=contract_maps)),axis=1)
        dfTxs[['ToContract','ToLabel']] = dfTxs.apply(lambda tx:pandas.Series(query_addr(tronObj,tx['To'],trx_info,contract_maps=contract_maps)),axis=1)
    return dfTxs