from . import labels

def query_label_tron(tronObj,addr,isContract=False,trx_info={'addressTag':{},'contractInfo':{}}):
    record = labels.get(addr)
    if record is None:
        label = ''
        if isContract:
            if addr in trx_info['contractInfo'].keys():
                if len(trx_info['contractInfo'][addr]['tag1']) > 0:
                    label = trx_info['contractInfo'][addr]['tag1']
                elif len(trx_info['contractInfo'][addr]['name']) > 0:
                    label = trx_info['contractInfo'][addr]['name']
            else:
                addr_info = tronObj.get_contract_info(addr)
                if 'data' in addr_info.keys():
                    if len(addr_info['data']) > 0:
                        if len(addr_info['data'][0]['tag1']) > 0:
                            label = addr_info['data'][0]['tag1']
                        elif len(addr_info['data'][0]['name']) > 0:
                            label = addr_info['data'][0]['name']
        else:
            if addr in trx_info['addressTag'].keys():
                label = trx_info['addressTag'][addr]
            else:
                addr_info = tronObj.get_account_detailed_info(addr)
                if 'addressTag' in addr_info.keys():
                    label = addr_info['addressTag']
                if 'publicTag' in addr_info.keys():
                    label = addr_info['publicTag']
        labels.set(addr,blockchain="TRX", data_source="tronscan",tag_name=label)
        return label
    else:
        return record['tag_name']