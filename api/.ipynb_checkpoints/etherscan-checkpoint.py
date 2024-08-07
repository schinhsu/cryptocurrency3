from .req_management import ReqManagement

def transform_balance(balance,decimalLen=18):
    balance = str(balance)
    if balance.find('.') >= 0:
        return eval(balance)
    integer = '0' if len(balance) <= decimalLen else balance[:len(balance)-decimalLen]
    decimal = balance[len(integer):] if len(balance) > decimalLen else balance.zfill(decimalLen)

    balance = integer+'.'+decimal
    return eval(balance)


#需至網站註冊取得apikey
class Etherscan:
    def __init__(self,apkikey,debugMode=False,sessionVerify=False):
        self.req_manage = ReqManagement(includes=['result'],debugMode=debugMode,sessionVerify=sessionVerify)
        self.url = 'https://api.etherscan.io/api'
        
        #NOTE!!! API limit: 5 requests at a second
        self.key = apkikey

    def get_txinfo_by_hash(self,txid:str):
        params = {'module':'proxy','action':'eth_getTransactionReceipt',
                  'txhash':txid,'apikey':self.key}
        res = self.req_manage.send_requests(method='get',url=self.url,params=params)
        return res.json()
    
    def get_balance(self,addr:str):
        params = {'module':'account','action':'balance','address':addr,'apikey':self.key}
        res = self.req_manage.send_requests('get',self.url,params=params)
        balance = transform_balance(res.json()['result'])
        return balance #float

    def get_token_balance(self,addr:str,tokenAddr:str):
        params = {'module':'account','action':'tokenbalance','contractaddress':tokenAddr,
                  'address':addr,'tag':'latest','apikey':self.key}
        res = self.req_manage.send_requests('get',self.url,params=params)
        balance = transform_balance(res.json()['result'],decimalLen=6)
        return balance #float

    
    def get_multi_balance(self,addrs:list):
        addr = ','.join(addrs)
        params = {'module':'account','action':'balancemulti','address':addr,'tag':'latest','apikey':self.key}
        res = self.req_manage.send_requests('get',self.url,params=params)
        
        results = {}
        for info in res.json()['result']:
            account = info['account']
            balance = transform_balance(info['balance'])
            results[account] = balance
        return results #dict

    def get_blockno_by_timestamp(self,timestamp,closest='before'):
        params = {'module':'block','action':'getblocknobytime',
                  'timestamp':timestamp,'closest':closest,'apikey':self.key}
        res = self.req_manage.send_requests('get',self.url,params=params)
        return eval(res.json()['result']) #float

    #API每次回覆batchnum數量(最大10000筆)的資料量 (batchnum*page <= 10000)
    #transType=['Normal','Internal','ERC20','ERC721']
    def get_transfer_once(self,addr,page=1,batchnum=10000,sort='desc',startblock=0,endblock=999999999,transType='Normal'):
        addr = addr.lower()
        params = {'module':'account','action':'txlist','address':addr,
                  'page':page,'offset':batchnum,'sort':sort,
                  'apikey':self.key}
        if transType == 'Internal':
            params['action'] = 'txlistinternal'
        elif transType == 'ERC20':
            params['action'] = 'tokentx'
        elif transType == 'ERC721':
            params['action'] = 'tokennfttx'
        elif transType != 'Normal':
            print(f'!!Undefined TransType!! transType should be one of "Normal", "Internal", "ERC20", "ERC721"')
            return None
        
        if transType != 'ERC721':
            params['startblock'] = startblock
            params['endblock'] = endblock

        res = self.req_manage.send_requests('get',self.url,params=params)
        txs = res.json()['result']
        return txs #list(dict)

    #API每次回覆batchnum數量(最大10000筆)的資料量 (batchnum*page <= 10000)
    #transType=['Normal','Internal','ERC20','ERC721']
    def get_token_transfer_with_contract(self,addr,contractAddr,page=1,batchnum=10000,sort='desc',startblock=0,endblock=999999999,transType='ERC20'):
        params = {'module':'account','action':'tokentx','address':addr,
                  'contractaddress':contractAddr,
                  'page':1,'offset':1,'sort':'desc','apikey':self.key}
        if transType == 'ERC20':
            params['action'] = 'tokentx'
        elif transType == 'ERC721':
            params['action'] = 'tokennfttx'
        else:
            print(f'!!Undefined TransType!! transType should be one of "Normal", "Internal", "ERC20", "ERC721"')
            return None

        res = self.req_manage.send_requests('get',self.url,params=params)
        txs = res.json()['result']
        return txs #list(dict)