from collections import defaultdict
from datetime import datetime, timezone, timedelta
from models import save_holding, clear_all_holdings
from token_info import TokenUtils
from decimal import Decimal
from loguru_logger import *

# async def calculate_remaining_tokens(transactions, wallet_address, session):
#     try:
#         # 初始化字典来存储每个代币的交易数据
#         token_summary = defaultdict(lambda: {
#             'buy_amount': 0,
#             'sell_amount': 0,
#             'cost': 0,
#             'profit': 0,
#             'marketcap': 0,
#             'buy_count': 0
#         })

#         # 遍历所有交易
#         for wallet_address, transactions in transactions.items():
#             for transaction in transactions:
#                 print(transaction)
#                 if not isinstance(transaction, dict):
#                     print(f"Unexpected transaction type: {type(transaction)} for wallet {wallet_address}")
#                     continue
#                 for token, data in transaction.items():
#                     if isinstance(data, dict):  # 确保 token_data 是一个字典
#                         # 累加相同代币的买入量、卖出量
#                         token_summary[token]['buy_amount'] += data['buy_amount']
#                         token_summary[token]['sell_amount'] += data['sell_amount']
#                         token_summary[token]['cost'] += data['cost']
#                         token_summary[token]['profit'] += data['profit']

#                         # 只有在 buy_amount > 0 时才累加 marketcap 和买入次数
#                         if data['buy_amount'] > 0:
#                             token_summary[token]['marketcap'] += data['marketcap']
#                             token_summary[token]['buy_count'] += 1  # 累加买入笔数
#         # 筛选出仍然持有代币的交易（buy_amount - sell_amount > 0）
#         remaining_tokens = {
#             token_address: data for token_address, data in token_summary.items()
#             if data['buy_amount'] - data['sell_amount'] > 0
#         }

#         # 保存持仓到数据库
#         for token_address, token_data in remaining_tokens.items():
#             # 计算平均买入市值（只计算买入的 marketcap）
#             if token_data['buy_count'] > 0:
#                 average_marketcap = token_data['marketcap'] / token_data['buy_count']
#             else:
#                 average_marketcap = 0  # 如果没有买入笔数，则市值为 0

#             # 计算买入均价（单个代币成本）
#             if token_data['buy_amount'] > 0:
#                 buy_price = token_data['cost'] / token_data['buy_amount']
#             else:
#                 buy_price = 0  # 如果没有买入量，则买入均价为 0

#             # 获取代币的实时价格信息
#             token_info = TokenUtils.get_token_info(token_address)
#             url = token_info.get('url', "")
#             symbol = token_info.get('symbol', "")
#             token_price = token_info.get('priceNative', 0)
#             token_price_USDT = token_info.get('priceUsd', 0)

#             # 当前持仓量
#             current_amount = token_data['buy_amount'] - token_data['sell_amount']

#             # 计算未实现利润
#             unrealized_profit = current_amount * (token_price_USDT - buy_price)

#             # 计算总 PnL
#             total_pnl = token_data['profit'] + unrealized_profit

#             # 计算 PnL 百分比
#             if buy_price > 0:
#                 pnl_percentage = ((token_price_USDT - buy_price) / buy_price) * 100
#             else:
#                 pnl_percentage = 0  # 如果没有买入价格，则涨跌幅为 0

#             # 准备写入数据库的数据
#             tx_data = {
#                 "token_address": token_address,
#                 "token_icon": url,  # 可以通过其他方式查询获取代币图标
#                 "token_name": symbol,  # 可以通过其他方式查询获取代币名称
#                 "chain": "SOLANA",  # 根据实际需要设置区块链名称
#                 "buy_amount": token_data['buy_amount'],
#                 "sell_amount": token_data['sell_amount'],
#                 "amount": current_amount,  # 当前持仓量
#                 "value": current_amount * token_price,  # 当前持仓价值（本地货币）
#                 "value_USDT": round(current_amount * token_price_USDT, 2),  # 当前持仓价值（USDT）
#                 "cost": token_data['cost'],  # 总买入成本
#                 "profit": token_data['profit'],  # 已实现利润
#                 "unrealized_profit": unrealized_profit,  # 未实现利润
#                 "pnl": total_pnl,  # 总 PnL（已实现 + 未实现）
#                 "pnl_percentage": pnl_percentage,  # PnL 百分比
#                 "marketcap": average_marketcap,  # 平均买入市值
#                 "time": datetime.now(timezone(timedelta(hours=8))),  # 设置为当前时间
#             }
#             # 调用 save_holding 函数写入到数据库
#             await save_holding(tx_data, wallet_address, session)

#     except Exception as e:
#         print(f"Error while processing and saving holdings: {e}")
def make_naive_time(dt):
    """将带时区的时间转换为无时区时间"""
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

@async_log_execution_time
async def calculate_remaining_tokens(transactions, wallet_address, session, chain):
    try:
        # 初始化字典来存储每个代币的交易数据
        token_summary = defaultdict(lambda: {
            'buy_amount': 0,
            'sell_amount': 0,
            'cost': 0,
            'profit': 0,
            'marketcap': 0,
            'buy_count': 0,
            'last_transaction_time': 0  # 用于存储最新交易时间
        })

        for wallet_address, transactions in transactions.items():
            for transaction in transactions:
                if not isinstance(transaction, dict):
                    print(f"Unexpected transaction type: {type(transaction)} for wallet {wallet_address}")
                    continue
                for token, data in transaction.items():
                    if isinstance(data, dict):  # 确保 token_data 是一个字典
                        # 累加相同代币的买入量、卖出量
                        token_summary[token]['buy_amount'] += data['buy_amount']
                        token_summary[token]['sell_amount'] += data['sell_amount']
                        token_summary[token]['cost'] += data['cost']
                        token_summary[token]['profit'] += data['profit']

                        # 只有在 buy_amount > 0 时才累加 marketcap 和买入次数
                        if data['buy_amount'] > 0:
                            token_summary[token]['marketcap'] += data['marketcap']
                            token_summary[token]['buy_count'] += 1  # 累加买入笔数

                        # 计算该代币的最后交易时间（最新的 timestamp）
                        if transaction['timestamp'] > token_summary[token]['last_transaction_time']:
                            token_summary[token]['last_transaction_time'] = transaction['timestamp']

        # 筛选出仍然持有代币的交易（buy_amount - sell_amount > 0）
        remaining_tokens = {
            token_address: data for token_address, data in token_summary.items()
            if data['buy_amount'] - data['sell_amount'] > 0
        }

        # 如果没有任何持仓，删除所有持仓记录
        if not remaining_tokens:
            await clear_all_holdings(wallet_address, session, chain)
            return  # 直接返回，避免繼續執行
        tx_data_list = []
        # 保存持仓到数据库
        for token_address, token_data in remaining_tokens.items():
            # 计算平均买入市值（只计算买入的 marketcap）
            if token_data['buy_count'] > 0:
                average_marketcap = token_data['marketcap'] / token_data['buy_count']
            else:
                average_marketcap = 0  # 如果没有买入笔数，则市值为 0

            # 计算买入均价（单个代币成本）
            if token_data['buy_amount'] > 0:
                buy_price = token_data['cost'] / token_data['buy_amount']
            else:
                buy_price = 0  # 如果没有买入量，则买入均价为 0
            formatted_buy_price = Decimal(buy_price or 0).quantize(Decimal('0.0000000000'))
            
            # 获取代币的实时价格信息
            token_info = TokenUtils.get_token_info(token_address)
            url = token_info.get('url', "")
            symbol = token_info.get('symbol', "")
            token_price = token_info.get('priceNative', 0)
            token_price_USDT = token_info.get('priceUsd', 0)

            # 当前持仓量
            current_amount = token_data['buy_amount'] - token_data['sell_amount']

            # 计算未实现利润
            unrealized_profit = current_amount * token_price_USDT

            # 计算总 PnL
            total_pnl = token_data['profit'] + unrealized_profit

            # 计算 PnL 百分比
            if buy_price > 0:
                pnl_percentage = ((token_price_USDT - buy_price) / buy_price) * 100
            else:
                pnl_percentage = 0  # 如果没有买入价格，则涨跌幅为 0

            # 准备写入数据库的数据
            tx_data = {
                "token_address": token_address,
                "token_icon": url,  # 可以通过其他方式查询获取代币图标
                "token_name": symbol,  # 可以通过其他方式查询获取代币名称
                "chain": "SOLANA",  # 根据实际需要设置区块链名称
                "buy_amount": token_data['buy_amount'],
                "sell_amount": token_data['sell_amount'],
                "amount": current_amount,  # 当前持仓量
                "value": current_amount * token_price,  # 当前持仓价值（本地货币）
                "value_USDT": round(current_amount * token_price_USDT, 2),  # 当前持仓价值（USDT）
                "cost": token_data['cost'],  # 总买入成本
                "profit": token_data['profit'],  # 已实现利润
                "unrealized_profit": unrealized_profit,  # 未实现利润
                "pnl": total_pnl,  # 总 PnL（已实现 + 未实现）
                "pnl_percentage": pnl_percentage,  # PnL 百分比
                "avg_price": formatted_buy_price,
                "marketcap": average_marketcap,  # 平均买入市值
                "last_transaction_time": make_naive_time(token_data['last_transaction_time']),  # 设置为最后交易时间
                "time": make_naive_time(datetime.now()),  # 当前时间，无时区
            }
            tx_data_list.append(tx_data)

            # 调用 save_holding 函数写入到数据库
        await save_holding(tx_data_list, wallet_address, session, chain)

    except Exception as e:
        print(f"Error while processing and saving holdings: {e}")
