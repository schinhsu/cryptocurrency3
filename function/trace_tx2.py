from . import columns
import pandas

#確認追查結果
def get_target_txs(txinfo,traceType,toTrace,traceTolerance=10,ignoreAmount=1):
    toTrace = toTrace.sort_values(by=['Date'],ascending=(traceType=='To'))

    start = False #用相同的交易序號確認追查起始點
    amount = 0
    count = 0
    result = pandas.DataFrame(columns=columns)
    for _,row in toTrace.iterrows():
        #用相同的交易序號確認追查起始點
        if row['TxID'] == txinfo['TxID']:
            start = True
            continue
        if start:
            if row[traceType] != txinfo[traceType] and row['Contract'] == txinfo['Contract'] and row['Token'] == txinfo['Token']:
                amount += row['Value']
                count += 1
                result = pandas.concat([result,pandas.DataFrame([row.values],columns=row.index)])
                if amount >= txinfo['Value']:
                    break
    if toTrace.empty:
        errMsg = f'下載交易資料(總數={len(toTrace)}'
    else:
        errMsg = f'下載交易資料(總數={len(toTrace)}、時間={toTrace.iloc[-1]["Date"].strftime("%Y-%m-%d %H:%M:%S")})'
    if not start:
        errMsg += '未包含原始追蹤交易'
    elif count == 0:
        if traceType == 'From':
            errMsg += f'未有從其他錢包轉入'
        else:
            errMsg += f'未有轉出至其他錢包'
    elif amount+traceTolerance < txinfo['Value']:
        errMsg += f'追查結果總額={amount}，未達原始追蹤交易額={txinfo["Value"]}'
    else:
        result = result[result['Value']>=ignoreAmount]
        resultAmount = result['Value'].sum()
        if resultAmount+traceTolerance < txinfo['Value']:
            errMsg = f'篩選交易金額{ignoreAmount}後追查結果總額={resultAmount}，未達原始追蹤交易額={txinfo["Value"]}'
        else:
            errMsg = f'無異常'
    return result,errMsg