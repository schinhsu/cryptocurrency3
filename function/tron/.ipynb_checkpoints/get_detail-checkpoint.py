from .. import columns
from .. import transform_balance

import datetime
import pandas
import pytz




def get_tx_by_hash(tronObj,txid):
    res = tronObj.get_txinfo_by_hash(txid)
    if res.get('contract_map') is None:
        print(res)
    for key,value in res['contract_map'].items():
        if value:
            if res['addressTag'].get(key) is None:
                res['addressTag'][key] = res['contractInfo'][key]['tag1']
        else:
             if res['addressTag'].get(key) is None:
                res['addressTag'][key] = ''
    
    if 'transfersAllList' in res.keys():
        txlist = pandas.json_normalize(res['transfersAllList'])
        txlist['Value'] = txlist.apply(lambda tx:eval(tx['amount_str'][:-tx['decimals']]+'.'+tx['amount_str'][-tx['decimals']:]),axis=1)
        txlist['FromContract'] = txlist.apply(lambda tx:res['contract_map'][tx['from_address']],axis=1)
        txlist['ToContract'] = txlist.apply(lambda tx:res['contract_map'][tx['to_address']],axis=1)
        txlist[['BlockNo','TxID','Date(UTC+8)','TxFee']] = [[res['block'],res['hash'],
                                                             pandas.to_datetime(res['timestamp'],unit='ms').tz_localize('Asia/Taipei'),
                                                             res['cost']['energy_fee']/(10**6)] for _ in range(len(txlist))]
        txlist['FromLabel'] = txlist.apply(lambda tx:'' if res['addressTag'].get(tx['from_address']) is None else res['addressTag'][tx['from_address']],axis=1)
        txlist['ToLabel'] = txlist.apply(lambda tx:'' if res['addressTag'].get(tx['to_address']) is None else res['addressTag'][tx['to_address']],axis=1)
        txlist = txlist.rename(columns={'symbol':'Token','from_address':'From',
                                        'to_address':'To','contract_address':'Contract',
                                        'tokenType':'TXType'})
        txlist = txlist[columns]
    else:
        blockNo,txid,date,fee = res['block'],res['hash'],pandas.to_datetime(res['timestamp'],unit='ms').tz_localize('Asia/Taipei'),res['cost']['energy_fee']/(10**6)
        from_,to_ = res['contractData']['owner_address'],res['contractData']['to_address']
        from_contract = res['contract_map'][from_]
        from_label = '' if res['addressTag'].get(from_) is None else res['addressTag'][from_]
        to_contract = res['contract_map'][to_]
        to_label = '' if res['addressTag'].get(to_) is None else res['addressTag'][to_]
        if len(res['contractInfo']) == 0:
            value = res['contractData']['amount']/(10**6)
            data = [[blockNo,txid,date,from_,to_,value,fee,
                    'TRX','trc10','trc10',from_contract,
                     to_contract,from_label,to_label]]
            txlist = pandas.DataFrame(data=data,columns=columns)
    return txlist