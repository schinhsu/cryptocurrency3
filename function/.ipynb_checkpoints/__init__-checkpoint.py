# 統一輸出欄位
#columns = ['BlockNo','TxID','Date(UTC+8)','From','To','Value','TxFee','Token','Contract','TXType']
columns = ['BlockNo','TxID','Date','From','To','Value','TxFee','Token','Contract','TXType']


def transform_balance(balance,decimalLen=18):
    decimalLen = int(decimalLen)
    balance = str(balance)
    balance = balance.replace('nan','')
    if balance.find('.') >= 0:
        return eval(balance)
    if len(balance) == 0:
        return 0
    try:
        integer = '0' if len(balance) <= decimalLen else balance[:len(balance)-decimalLen]
    except TypeError:
        print(type(balance),balance)
        print(len(balance),decimalLen)
        
    decimal = balance[len(integer):] if len(balance) > decimalLen else balance.zfill(decimalLen)

    balance = integer+'.'+decimal
    
    return eval(balance)

    
from .trace_tx2 import get_target_txs
from .check_sc2 import lookup_details_eth
from .check_sc2 import lookup_details_tron

from .labels import WalletLabels
labels = WalletLabels()

from .query_label import query_label_tron

from .prices import PriceHistoryManager