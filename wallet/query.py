from . import labels


#修改成From(To)Contract、From(To)Label同時給值
#tron.get_txinfo_by_hash()
##1.addressTag有
##2.contractInfo有
##3.contract_map有
#tron.get_transfer_once()
##1.addressTag沒有，但TRC20有from_address_tag、to_address_tag
##2.contractInfo有
##3.contractMap只有TRC20沒有，但TRC20有fromAddressIsContract、toAddressIsContract
def query_addr_tron(tronObj,addr,trx_info={'addressTag':{},'contractInfo':{}},contract_maps={}):
    record = labels.get(addr)
    if record is None:
        addr_info = None
        if not contract_maps.get(addr) is None:
            isContract = contract_maps[addr]
        else:
            addr_info = tronObj.get_account_detailed_info(addr)
            isContract = addr_info['accountType'] != 0

        label = ''
        if isContract:
            if addr in trx_info['contractInfo'].keys():
                if len(trx_info['contractInfo'][addr]['tag1']) > 0:
                    label = trx_info['contractInfo'][addr]['tag1']
                elif len(trx_info['contractInfo'][addr]['name']) > 0:
                    label = trx_info['contractInfo'][addr]['name']
            else:
                contract_info = tronObj.get_contract_info(addr)
                if 'data' in contract_info.keys():
                    if len(contract_info['data']) > 0:
                        if len(contract_info['data'][0]['tag1']) > 0:
                            label = contract_info['data'][0]['tag1']
                        elif len(contract_info['data'][0]['name']) > 0:
                            label = contract_info['data'][0]['name']
        else:
            if addr in trx_info['addressTag'].keys():
                label = trx_info['addressTag'][addr]
            else:
                if addr_info is None:
                    addr_info = tronObj.get_account_detailed_info(addr)
                if 'addressTag' in addr_info.keys():
                    label = addr_info['addressTag']
                elif 'publicTag' in addr_info.keys():
                    label = addr_info['publicTag']
        labels.set(addr,blockchain="TRX", data_source="tronscan",is_contract=isContract,tag_name=label)
        # print(addr,isContract,label)
        return isContract,label
    else:
        # print(addr,record['is_contract'],record['tag_name'])
        return record['is_contract'],record['tag_name']