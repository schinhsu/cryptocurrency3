from .. import transform_balance
from .. import columns

import pandas
import datetime

def get_tx_by_hash(ethObj,txid):
    res = ethObj.get_txinfo_by_hash(txid)
    res3 = ethObj.get_time_by_blockno(res['result']['blockNumber'])
    timestamp = int(res3['result']['timestamp'], 16)
    tx_time = pandas.to_datetime(timestamp,unit='s').tz_localize('Asia/Taipei')
    
    res2 = ethObj.get_txinfo_by_hash2(txid)
    value = transform_balance(str(int(res2['result']['value'],16)),decimalLen=18)
    txfee = transform_balance(str(int(res2['result']['gasPrice'],16)*int(res['result']['gasUsed'],16)),decimalLen=18)
    
    if len(res['result']['logs']) == 0:
        block = int(res['result']['blockNumber'],16)
        from_ = res['result']['from']
        to_ = res['result']['to']
        
        token = 'ETH'
        contract = 'eth'
        txtype = 'Normal'
        dfFiltered = pandas.DataFrame(data=[[block,txid,tx_time,from_,to_,value,txfee,token,contract,txtype]],
                                      columns=columns)
    else:
        df = pandas.json_normalize(res['result']['logs'])
    
        df['block'] = df['blockNumber'].apply(lambda x:int(x,16))
        tx_logs = df['topics'].tolist()
        df[['topic','from','to']] = [[tx_log[0],tx_log[-2][:2]+tx_log[-2][-40:],tx_log[-1][:2]+tx_log[-1][-40:]] if len(tx_log) >=3 else [tx_log[0],'',''] for tx_log in tx_logs]
        dfTrim = df[((df['topic']=='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')|(df['topic']=='0x7055e3d08e2c20429c6b162f3e3bee3f426d59896e66084c3580dc353e54129d'))&(df['from']!=df['address'])&(df['to']!=df['address'])]
    
        tokenValues = []
        for i,row in dfTrim.iterrows():
            # 統一用ERC20去查
            tmp = ethObj.get_token_transfer_with_contract(row['from'],row['address'],page=1,batchnum=10,sort='desc',transType='ERC20')
            tokenSymbol = tmp[0]['tokenSymbol']
            tokenDecimal = eval(tmp[0]['tokenDecimal'])
            value = transform_balance(str(int(row['data'],16)),decimalLen=tokenDecimal)
            tokenValues.append([tokenSymbol,value])
        dfTrim.loc[:,['symbol','amount']] = tokenValues
        dfTrim.loc[:,['time']] = tx_time
        dfTrim.loc[:,['txfee']] = txfee
        dfTrim.loc[:,['txtype']] = 'ERC20'
        dfFiltered = dfTrim[['block','transactionHash','time','from','to','amount','txfee','symbol','address','txtype']]
        dfFiltered.columns = ['BlockNo','TxID','Date(UTC+8)','From','To','Value','TxFee','Token','Contract','TXType']
    return dfFiltered