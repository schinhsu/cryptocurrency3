from .. import columns
from .. import transform_balance

import datetime
import pandas

## columns = ['BlockNo', 'TxID', 'Date', 'From', 'To', 'Value', 'TxFee', 'Token',  'Contract', 'TXType', '交易資訊', 'FromContract', 'ToContract', 'FromLabel'   'ToLabel]
### '交易資訊'可傳遞換幣、盜用情形

def get_addr_label(string,trx_info={'contract_map':{}}):
    label = ''
    if trx_info['contract_map'][string]:
        if string in trx_info['contractInfo'].keys():
            label = trx_info['contractInfo'][string]['tag1']
    else:
        if string in trx_info['addressTag'].keys():
            label = trx_info['addressTag'][string]
    return label

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
        txfee = eval(trx_info['cost']['energy_fee'])/(10**6)+eval(trx_info['cost']['net_fee'])/(10**6)
    
    # print(f'block 區塊編號',block)
    # print(f'txid 交易序號',hash)
    # print(f'timestamp 交易時間(UTC+8)',txtime)
    # print(f'cost 交易手續費',txfee)
    
    txcomment = ''
    if 'signature_addresses' in trx_info.keys() and len(trx_info['signature_addresses']) > 0:
        txcomment = '此筆交易由 '+' '.join(trx_info['signature_addresses'])+ ' 簽署'
    if 'trigger_info' in trx_info.keys():
        method = trx_info["trigger_info"].get('method')
        parameter = trx_info["trigger_info"].get('parameter')
        if method and parameter:
            txcomment = f'method: {method}\nparameter: {parameter}'
    if 'project' in trx_info.keys() and len(trx_info['project']) > 0:
        txcomment = '此筆交易使用 '+trx_info['project']
    ## 交易行為transactionBehavior
    if 'transactionBehavior' in trx_info.keys() and len(trx_info['transactionBehavior']) > 0:
        #print(f'transactionBehavior 交易行為',trx_info['transactionBehavior'])
        project = trx_info.get('project')
        txBehavior = trx_info.get('transactionBehavior')
        token_out_amount = txBehavior.get('token_out_amount')
        if not txBehavior.get('event') is None:
            txcomment = f'此筆交易透過 {project} 平台，服務類型：{txBehavior["event"]}'
        else:
            try:
                token_out_amount = txBehavior.get('token_out_amount')
                if token_out_amount is None:
                    token_out_amount = txBehavior.get('token_sold_amount')
                token_out_info = txBehavior.get('token_out_info')
                #不知道為什麼有兩種Key值
                if token_out_info is None:
                    token_out_info = txBehavior.get('token_sold_info')
                token_out_name = token_out_info.get('tokenAbbr')
                token_in_amount = txBehavior.get('token_in_amount')
                if token_in_amount is None:
                    token_in_amount = txBehavior.get('token_bought_amount')
                token_in_info = txBehavior.get('token_in_info')
                #不知道為什麼有兩種Key值
                if token_in_info is None:
                    token_in_info = txBehavior.get('token_bought_info')
                token_in_name = token_in_info.get('tokenAbbr')
                
                if project and token_out_amount and token_out_name and token_in_amount and token_in_name:
                    token_out_value = int(token_out_amount) / 10**token_out_info.get('tokenDecimal', 6)
                    token_in_value = int(token_in_amount) / 10**token_in_info.get('tokenDecimal', 6)
                    txcomment = f'此筆交易透過 {project} 平台，使用 {token_out_value} {token_out_name} 兌換 {token_in_value} {token_in_name}'
            except AttributeError:
                txcomment = f'此筆交易透過 {project} 平台，待更新類型'
                print('新型態交易',txBehavior)
    
    
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
    ##JustLend能源租借
    if len(txinfos) == 0:
        from_ = trx_info['contractData']['owner_address']
        to_ = trx_info['contractData']['contract_address']
        amount = 0
        token = 'TRX'
        contract = to_
        txtype = 'trc10'
        txinfos.append([block,hash,txtime,from_,to_,amount,txfee,token,contract,txtype,txcomment])
    
    
    dfTxInfo = pandas.DataFrame(txinfos,columns=columns+['交易資訊'])
    dfTxInfo['FromContract'] = dfTxInfo.apply(lambda tx:trx_info['contract_map'][tx['From']],axis=1)
    dfTxInfo['ToContract'] = dfTxInfo.apply(lambda tx:trx_info['contract_map'][tx['To']],axis=1)
    dfTxInfo['FromLabel'] = dfTxInfo['From'].apply(get_addr_label,trx_info=trx_info)
    dfTxInfo['ToLabel'] = dfTxInfo['To'].apply(get_addr_label,trx_info=trx_info)
    return dfTxInfo