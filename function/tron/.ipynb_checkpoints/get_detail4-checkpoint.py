from .. import columns
from .. import transform_balance
from .. import query_label_tron as query_label
### 新增錢包位址標記資料儲存

import datetime
import pandas

## columns = ['BlockNo', 'TxID', 'Date', 'From', 'To', 'Value', 'TxFee', 'Token',  'Contract', 'TXType', '交易資訊', 'FromContract', 'ToContract', 'FromLabel'   'ToLabel]
### '交易資訊'可傳遞換幣、盜用情形


def get_txinfo_by_hash(tronObj,txid):
    trx_info = tronObj.get_txinfo_by_hash(txid)
    block = trx_info['block']
    hash = trx_info['hash']
    ## 交易時間(UTC+8)轉換方式
    txtime = pandas.to_datetime(trx_info['timestamp'],unit='ms')+datetime.timedelta(hours=8)
    ## 交易手續費計算方式
    ## 交易手續費計算方式
    try:
        txfee = trx_info['cost']['energy_fee']/(10**6)+trx_info['cost']['net_fee']/(10**6)
    except TypeError:
        #部分API回傳值是str非int
        txfee = eval(trx_info['cost']['energy_fee'])/(10**6)+eval(trx_info['cost']['net_fee'])/(10**6)
    
    # print(f'block 區塊編號',block)
    # print(f'txid 交易序號',hash)
    # print(f'timestamp 交易時間(UTC+8)',txtime)
    # print(f'cost 交易手續費',txfee)
    
    txcomment = ''
    if 'trigger_info' in trx_info.keys() and len(trx_info['trigger_info']) > 0:
        try:
            if not trx_info["trigger_info"].get('methodId') == 'a9059cbb':
                txcomment = f'method: {trx_info["trigger_info"]["method"]}\nparameter: {trx_info["trigger_info"]["parameter"]}'
        except KeyError:
            print(f'In function <get_txinfo_by_hash>:\ntrigger_info無method或parameter: {trx_info["trigger_info"]}')
    if 'project' in trx_info.keys() and len(trx_info['project']) > 0:
        txcomment = '此筆交易使用 '+trx_info['project']
    ## 交易行為transactionBehavior
    if 'transactionBehavior' in trx_info.keys() and len(trx_info['transactionBehavior']) > 0:
        txBehavior = trx_info.get('transactionBehavior')
        if not txBehavior.get('event') is None:
            if len(txcomment) > 0:
                txcomment += '，'
            txcomment += f'服務類型：{txBehavior["event"]}'
        
        token_out_info = None
        token_out_keys = ['token_out_','token_sold_']
        for token_out_key in token_out_keys:
            if not txBehavior.get(token_out_key+'info') is None:
                token_out_info = txBehavior[token_out_key+'info']
                token_out_amount = txBehavior[token_out_key+'amount']
                token_out_name = token_out_info.get('tokenAbbr','')
                token_out_value = int(token_out_amount) / 10**token_out_info.get('tokenDecimal', 6)
                break
        token_in_info = None
        token_in_keys = ['token_in_','token_bought_']
        for token_in_key in token_in_keys:
            if not txBehavior.get(token_in_key+'info') is None:
                token_in_info = txBehavior[token_in_key+'info']
                token_in_amount = txBehavior[token_in_key+'amount']
                token_in_name = token_in_info.get('tokenAbbr','')
                token_in_value = int(token_in_amount) / 10**token_in_info.get('tokenDecimal', 6)
                break
        if token_out_info and token_in_info:
            if len(txcomment) > 0:
                txcomment += '; '
            txcomment += f'使用 {token_out_value} {token_out_name} 兌換 {token_in_value} {token_in_name}'
    ## 有多簽權限signature_address會有其他錢包
    if 'signature_addresses' in trx_info.keys() and len(trx_info['signature_addresses']) > 0:
        if len(txcomment) > 0:
            txcomment += '; '
        txcomment += '此筆交易由 '+' '.join(trx_info['signature_addresses'])+ ' 簽署'
    
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
    ##呼叫智能合約的時候有轉帳TRX的情況，不會在上述兩種情況
    if 'call_value' in trx_info['contractData'].keys():
        from_ = trx_info['contractData']['owner_address']
        to_ = trx_info['contractData']['contract_address']
        amount = trx_info['contractData']['call_value']/(10**6)
        token = 'TRX'
        contract = ''
        txtype = 'trc10'
        txinfos.append([block,hash,txtime,from_,to_,amount,txfee,token,contract,txtype,txcomment])
    ##如果真的完全沒抓到交易先標成0
    if len(txinfos) == 0:
        from_ = trx_info['contractData']['owner_address']
        to_ = trx_info['contractData']['contract_address']
        amount = 0
        token = 'TRX'
        contract = ''
        txtype = 'trc10'
        txinfos.append([block,hash,txtime,from_,to_,amount,txfee,token,contract,txtype,txcomment])
        print(f'In function <get_txinfo_by_hash>:\n特殊類型交易: {txid}')
    
    
    dfTxInfo = pandas.DataFrame(txinfos,columns=columns+['交易資訊'])
    dfTxInfo['FromContract'] = dfTxInfo.apply(lambda tx:trx_info['contract_map'][tx['From']],axis=1)
    dfTxInfo['ToContract'] = dfTxInfo.apply(lambda tx:trx_info['contract_map'][tx['To']],axis=1)
    
    dfTxInfo['FromLabel'] = dfTxInfo.apply(lambda tx:query_label(tronObj,tx['From'],tx['FromContract'],trx_info),axis=1)
    dfTxInfo['ToLabel'] = dfTxInfo.apply(lambda tx:query_label(tronObj,tx['To'],tx['ToContract'],trx_info),axis=1)
    return dfTxInfo