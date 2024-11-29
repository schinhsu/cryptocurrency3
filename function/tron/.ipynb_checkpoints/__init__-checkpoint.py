# from .get_detail import get_tx_by_hash
# from .get_transfer import json2df_tron
# from .get_transfer import get_transfer_tron_desc
# from .get_transfer import get_transfer_tron

# from .get_detail2 import get_tx_by_hash
# from .get_detail3 import get_txinfo_by_hash
from .get_detail4 import get_txinfo_by_hash
# from .get_transfer2 import json2df_tron
# from .get_transfer2 import get_transfer_tron_desc
# from .get_transfer2 import get_transfer_tron
# from .get_transfer4 import json2df_tron
# from .get_transfer4 import get_transfer_tron_desc
# from .get_transfer4 import get_transfer_tron
from .get_transfer5 import json2df_tron
from .get_transfer5 import get_transfer_tron_desc
from .get_transfer5 import get_transfer_tron

import pandas
import os


from .. import PriceHistoryManager
class TRONPriceHistoryManager(PriceHistoryManager):
    def __init__(self, df_tokens_path='data\\priced_asset.xlsx', history_prices_dir='data\\token\\'):
        super().__init__(df_tokens_path=df_tokens_path, history_prices_dir=history_prices_dir)

    def get_symbol(self, contract, token):
        """TRON特定的符号逻辑"""
        if token.upper() == 'TRX':
            contract = '_'
        check = self.df_tokens[self.df_tokens['id'] == contract]
        if check.empty:
            return None
        return check.iloc[0]['abbr'].upper()

from wallet.query import query_addr_tron as query_addr