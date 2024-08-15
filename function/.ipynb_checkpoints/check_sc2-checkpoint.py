from . import transform_balance
from . import columns

import pandas

# 如果追查到錢包位址屬於智能合約
def lookup_details_eth(ethObj,txinfo):
    res = ethObj.get_txinfo_by_hash(txinfo['TxID'])
    df = pandas.json_normalize(res['result']['logs'])

    df['block'] = df['blockNumber'].apply(lambda x:int(x,16))
    tx_logs = df['topics'].tolist()
    df[['topic','from','to']] = [[tx_log[0],tx_log[-2][:2]+tx_log[-2][-40:],tx_log[-1][:2]+tx_log[-1][-40:]] if len(tx_log) >=3 else [tx_log[0],'',''] for tx_log in tx_logs]
    dfTrim = df[((df['topic']=='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')|(df['topic']=='0x7055e3d08e2c20429c6b162f3e3bee3f426d59896e66084c3580dc353e54129d'))&(df['from']!=df['address'])&(df['to']!=df['address'])]
    if dfTrim.empty:
        print('>lookup_details_eth_check','非swap',txinfo['TxID'])
        blockNo = int(res['result']['blockNumber'],16)
        addrFrom = res['result']['from']
        return pandas.DataFrame(data=[[blockNo,txinfo['TxID'],txinfo['Date'],addrFrom,txinfo['To'],txinfo['Value'],txinfo['TxFee'],txinfo['Token'],txinfo['Contract']]],columns=columns)
    
    tokenValues = []
    for i,row in dfTrim.iterrows():
        # 統一用ERC20去查
        tmp = ethObj.get_token_transfer_with_contract(row['from'],row['address'],page=1,batchnum=10,sort='desc',transType='ERC20')
        tokenSymbol = tmp[0]['tokenSymbol']
        tokenDecimal = eval(tmp[0]['tokenDecimal'])
        value = transform_balance(str(int(row['data'],16)),decimalLen=tokenDecimal)
        tokenValues.append([tokenSymbol,value])
    dfTrim.loc[:,['symbol','amount']] = tokenValues
    dfTrim.loc[:,['time']] = txinfo['Date']
    dfTrim.loc[:,['txfee']] = txinfo['TxFee']
    dfFiltered = dfTrim[['block','transactionHash','time','from','to','amount','txfee','symbol','address']]
    dfFiltered.columns = ['BlockNo','TxID','Date','From','To','Value','TxFee','Token','Contract']

    dfFiltered = dfFiltered[~((dfFiltered['From']==txinfo['From']) & (dfFiltered['To']==txinfo['To']) & (dfFiltered['Value']==txinfo['Value']) & (dfFiltered['Token']==txinfo['Token']))]
    
    txinfo_columns = txinfo.index.tolist()
    for i in range(len(columns),len(txinfo_columns)):
        dfFiltered[txinfo_columns[i]] = txinfo[txinfo_columns[i]]
    return dfFiltered

def lookup_details_tron(tronObj,txinfo):
    res = tronObj.get_txinfo_by_hash(txinfo['TxID'])
    df = pandas.json_normalize(res['transfersAllList'])

    df['amount'] = df[['amount_str','decimals']].apply(lambda x: transform_balance(x['amount_str'], x['decimals']), axis=1)
    df['block'] = res['block']
    df['txid'] = res['hash']
    df['time'] = pandas.to_datetime(res['timestamp'],unit='ms').tz_localize(pytz.utc).tz_convert('Asia/Taipei')
    df['txfee'] = transform_balance(res['cost']['energy_fee'],decimalLen=6)
    df['symbol'] = df['symbol'].str.upper()
    dfTrim = df[['block','txid','time','from_address','to_address','amount','txfee','symbol','contract_address']]
    dfTrim.columns = columns

    dfFiltered = dfTrim[~((dfTrim['From']==txinfo['From']) & (dfTrim['To']==txinfo['To']) & (dfTrim['Value']==txinfo['Value']) & (dfTrim['Token']==txinfo['Token']))]
    
    txinfo_columns = txinfo.index.tolist()
    for i in range(len(columns),len(txinfo_columns)):
        dfFiltered.loc[:,[txinfo_columns[i]]] = [txinfo[txinfo_columns[i]] for _ in range(len(dfFiltered))]

    ## tron的智能合約排列順序好像都是固定從trx開始，多一個欄位確認To錢包位址是否為智能合約
    dfFiltered.loc[:,['From_Contract']] = dfFiltered['From'].apply(lambda addr:res['contract_map'][addr])
    dfFiltered.loc[:,['To_Contract']] = dfFiltered['To'].apply(lambda addr:res['contract_map'][addr])
    return dfFiltered