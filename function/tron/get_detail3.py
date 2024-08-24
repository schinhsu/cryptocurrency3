from .. import columns
from .. import transform_balance

import datetime
import pandas

## columns = ['BlockNo','TxID','Date','From','To','Value','TxFee','Token','Contract','TXType']

def get_addr_label(string,trx_info={'contract_map':{}}):
    label = ''
    if trx_info['contract_map'][string]:
        if string in trx_info['contractInfo'].keys():
            label = trx_info['contractInfo'][string]['tag1']
    else:
        if string in trx_info['addressTag'].keys():
            label = trx_info['addressTag'][string]
    return label

def get_tx_by_hash(tronObj,txid):
    trx_info = tronObj.get_txinfo_by_hash(txid)
    block = trx_info['block']
    hash = trx_info['hash']
    ## 交易時間(UTC+8)轉換方式
    txtime = pandas.to_datetime(trx_info['timestamp'],unit='ms')+datetime.timedelta(hours=8)
    ## 交易手續費計算方式
    txfee = trx_info['cost']['energy_fee']/(10**6)+trx_info['cost']['net_fee']/(10**6)
    
    # print(f'block 區塊編號',block)
    # print(f'txid 交易序號',hash)
    # print(f'timestamp 交易時間(UTC+8)',txtime)
    # print(f'cost 交易手續費',txfee)
    
    txcomment = ''
    if 'signature_addresses' in trx_info.keys() and len(trx_info['signature_addresses']) > 0:
        txcomment = '此筆交易由 '+' '.join(trx_info['signature_addresses'])+ ' 簽署'
    if 'project' in trx_info.keys() and len(trx_info['project']) > 0:
        txcomment = '此筆交易使用 '+trx_info['project']
    
    txinfos = []
    ##除了trx以外的交易都沒有['contractData']['to_address']
    if 'to_address' in trx_info['contractData'].keys():
        from_ = trx_info['contractData']['owner_address']
        to_ = trx_info['contractData']['to_address']
        amount = trx_info['contractData']['amount']/(10**6)
        token = 'TRX'
        contract = ''
        txtype = 'trc10'
        if 'tokenInfo' in trx_info['contractData'].keys():
            token = trx_info['contractData']['tokenInfo']['tokenAbbr']
            contract = trx_info['contractData']['tokenInfo']['tokenId']
            txtype = trx_info['contractData']['tokenInfo']['tokenType']
        # print(f'{from_} 轉帳 {amount} {token}({contract}/{txtype}) 給 {to_}')
        txinfos.append([block,hash,txtime,from_,to_,amount,txfee,token,contract,txtype,txcomment])
    ##但除了trx以外的交易都有['transfersAllList']
    if 'transfersAllList' in trx_info.keys():
        for transfer in trx_info['transfersAllList']:
            from_ = transfer['from_address']
            to_ = transfer['to_address']
            amount = transform_balance(transfer['amount_str'],transfer['decimals'])
            token = transfer['symbol']
            contract = transfer['contract_address']
            txtype = transfer['tokenType']
            # print(f'{from_} 轉帳 {amount} {token}({contract}/{txtype}) 給 {to_}')
            txinfos.append([block,hash,txtime,from_,to_,amount,txfee,token,contract,txtype,txcomment])
    
    
    dfTxInfo = pandas.DataFrame(txinfos,columns=columns+['交易資訊'])
    dfTxInfo['FromContract'] = dfTxInfo.apply(lambda tx:trx_info['contract_map'][tx['From']],axis=1)
    dfTxInfo['ToContract'] = dfTxInfo.apply(lambda tx:trx_info['contract_map'][tx['To']],axis=1)
    dfTxInfo['FromLabel'] = dfTxInfo['From'].apply(get_addr_label,trx_info=trx_info)
    dfTxInfo['ToLabel'] = dfTxInfo['To'].apply(get_addr_label,trx_info=trx_info)
    return dfTxInfo