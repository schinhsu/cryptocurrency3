import json
import os
import threading
import datetime


class WalletInfos:
    _instance = None
    _lock = threading.Lock()
    dirpath = 'data\\'
    cache_file = os.path.join(os.path.dirname(dirpath), "wallet_infos.json")

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:  #確保資料寫入安全
                if cls._instance is None:
                    cls._instance = super(WalletInfos, cls).__new__(cls)
                    cls._instance._load_cache()
        return cls._instance

    def _load_cache(self):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    self.wallet_cache = json.load(f)
            else:
                self.wallet_cache = {}
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading cache: {e}")
            self.wallet_cache = {}

    def get(self, wallet_address):
        """获取钱包地址的详细信息"""
        return self.wallet_cache.get(wallet_address)

    #新增is_contract
    def set(self, wallet_address, blockchain, data_source, is_contract, tag_name):
        """设置钱包地址的详细信息，包括区块链、数据来源、标记名称和记录时间"""
        record = {
            "blockchain": blockchain,
            "data_source": data_source,
            "is_contract": is_contract,
            "tag_name": tag_name,
            "timestamp": datetime.datetime.now().isoformat()  # 记录当前时间
        }
        self.wallet_cache[wallet_address] = record
        self._save_cache()

    def _save_cache(self):
        """将缓存数据保存到文件"""
        os.makedirs(self.dirpath,exist_ok=True)
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.wallet_cache, f, indent=4)
        except IOError as e:
            print(f"Error saving cache: {e}")