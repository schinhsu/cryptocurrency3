# function/prices.py
## gpt寫的
import pandas
import glob
import os
import time
import datetime
import requests

class PriceHistoryManager:
    def __init__(self, df_tokens_path, history_prices_dir='data\\token\\'):
        # 加载代币数据（根据具体区块链的路径传入）
        self.df_tokens = pandas.read_excel(df_tokens_path)
        self.history_prices = {}
        self.history_prices_dir = history_prices_dir
        
        # 加载历史价格数据
        for filepath in glob.glob(os.path.join(history_prices_dir, '*.xlsx')):
            token = os.path.basename(filepath).replace('.xlsx', '')
            self.history_prices[token] = pandas.read_excel(filepath)
    
    def get_history_price_by_date(self, symbol, day_str, vs_currency='USD', api_key='ddb61a83c035a5032205c2e87b48b6fdf70090de061db902c43dbb73ba7a7c0e'):
        """从API获取代币在特定日期的历史价格，并自动保存到缓存"""
        timestamp = int(time.mktime(datetime.datetime.strptime(day_str, "%Y-%m-%d").timetuple()))
        url = "https://min-api.cryptocompare.com/data/pricehistorical"
        params = {
            'fsym': symbol,
            'tsyms': vs_currency,
            'ts': timestamp,
            'api-key': api_key
        }
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if symbol in data:
                usd_value = data[symbol].get(vs_currency)
                return usd_value
            else:
                print("Error:", symbol, data.get("Message", "Unknown error"))
                return None
        except Exception as e:
            print("!!An error occurred while requesting", symbol, ":", e)
            time.sleep(0.5)
            return None

    def _save_price(self, symbol, day_str, usd_value):
        """保存查询到的历史价格到 DataFrame 和文件"""
        # 如果没有价格记录表，初始化一个空表
        if symbol not in self.history_prices:
            self.history_prices[symbol] = pandas.DataFrame(columns=['Date', 'USDValue'])
        
        # 将新数据添加到 DataFrame
        self.history_prices[symbol].loc[len(self.history_prices[symbol])] = [day_str, usd_value]
        
        # 保存到文件
        self._save_cache(symbol)

    def _save_cache(self, symbol):
        """将特定代币的历史价格保存到文件"""
        filepath = os.path.join(self.history_prices_dir, f"{symbol}.xlsx")
        self.history_prices[symbol].to_excel(filepath, index=False)
    
    def get_usd_value(self, contract, value, token, date):
        """查找或更新给定日期和合约的历史价格"""
        symbol = self.get_symbol(contract, token)
        if not symbol:
            return None
        
        if symbol not in self.history_prices:
            # 初始化空的历史价格表
            self.history_prices[symbol] = pandas.DataFrame(columns=['Date', 'USDValue'])
        
        day_str = date.strftime('%Y-%m-%d')
        lookup = self.history_prices[symbol][self.history_prices[symbol]['Date'] == day_str]
        
        if lookup.empty:
            # 没有记录则从API获取价格并存储
            usd_value = self.get_history_price_by_date(symbol, day_str)
            self._save_price(symbol, day_str, usd_value)
        else:
            # 有记录则直接使用
            usd_value = lookup.iloc[0]['USDValue']
        
        
        return usd_value * value if usd_value is not None else None

    def get_symbol(self, contract, token):
        """获取代币的符号，子类可以覆盖此方法"""
        check = self.df_tokens[self.df_tokens['id'] == contract]
        if check.empty:
            return None
        return check.iloc[0]['abbr'].upper()