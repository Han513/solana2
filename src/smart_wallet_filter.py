# import asyncio
# from collections import defaultdict
# from datetime import datetime, timedelta, timezone
# from models import *
# from loguru_logger import *
# from token_info import TokenUtils

# async def calculate_distribution(aggregated_tokens, days):
#     """
#     计算7天或30天内的收益分布
#     """
#     distribution = {
#         'distribution_gt500': 0,
#         'distribution_200to500': 0,
#         'distribution_0to200': 0,
#         'distribution_0to50': 0,
#         'distribution_lt50': 0
#     }

#     # 计算分布
#     for stats in aggregated_tokens.values():
#         if stats['cost'] > 0:  # 防止除零错误
#             pnl_percentage = ((stats['profit'] - stats['cost']) / stats['cost']) * 100
#             if pnl_percentage > 500:
#                 distribution['distribution_gt500'] += 1
#             elif 200 <= pnl_percentage <= 500:
#                 distribution['distribution_200to500'] += 1
#             elif 0 <= pnl_percentage < 200:
#                 distribution['distribution_0to200'] += 1
#             elif 0 <= pnl_percentage < 50:
#                 distribution['distribution_0to50'] += 1
#             elif pnl_percentage < 0:
#                 distribution['distribution_lt50'] += 1

#     # 计算分布百分比
#     total_distribution = sum(distribution.values())    
#     distribution_percentage = {
#         'distribution_gt500_percentage': round((distribution['distribution_gt500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
#         'distribution_200to500_percentage': round((distribution['distribution_200to500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
#         'distribution_0to200_percentage': round((distribution['distribution_0to200'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
#         'distribution_0to50_percentage': round((distribution['distribution_0to50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
#         'distribution_lt50_percentage': round((distribution['distribution_lt50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
#     }

#     return distribution, distribution_percentage

# async def calculate_statistics(transactions, days, sol_usdt_price):
#     """
#     计算统计数据，包括总买卖次数、总成本、平均成本、PNL、每日PNL图等。
#     """
#     # 定义稳定币地址
#     STABLECOINS = [
#         "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
#         "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"   # USDC
#     ]

#     start_time = datetime.now(timezone.utc) - timedelta(days=days)

#     # 如果传入的 transactions 已经是 30 天内的数据，直接使用；否则进行过滤
#     filtered_transactions = transactions if days == 30 else [
#         tx for tx in transactions if tx['timestamp'] >= start_time
#     ]

#     aggregated_tokens = defaultdict(lambda: {
#         'buy_amount': 0,
#         'sell_amount': 0,
#         'cost': 0,
#         'profit': 0,
#         'remaining_amount': 0,
#         'unrealized_value': 0
#     })

#     total_buy = total_sell = total_cost = total_profit = total_unrealized_profit = 0
#     daily_pnl = {}
#     profitable_tokens = total_tokens = 0

#     for tx in filtered_transactions:
#         for token, stats in tx.items():
#             if token in ["timestamp", "signature"]:
#                 continue

#             # 跳过稳定币交易
#             if token in STABLECOINS:
#                 continue

#             total_buy += 1 if stats['buy_amount'] > 0 else 0
#             total_sell += 1 if stats['sell_amount'] > 0 else 0

#             aggregated_tokens[token]['buy_amount'] += stats['buy_amount']
#             aggregated_tokens[token]['sell_amount'] += stats['sell_amount']
#             aggregated_tokens[token]['cost'] += stats['cost']
#             aggregated_tokens[token]['profit'] += stats['profit']

#             date_str = tx['timestamp'].astimezone(timezone.utc).strftime("%Y-%m-%d")
#             daily_pnl[date_str] = daily_pnl.get(date_str, 0) + (stats['profit'] - stats['cost'])

#     for token, stats in aggregated_tokens.items():
#         remaining_amount = stats['buy_amount'] - stats['sell_amount']
#         stats['remaining_amount'] = remaining_amount

#         if remaining_amount > 0:
#             token_info = TokenUtils.get_token_info(token)
#             current_price = token_info.get("priceUsd", 0)

#             # 计算买入均价
#             if stats['buy_amount'] > 0:
#                 buy_price = stats['cost'] / stats['buy_amount']
#             else:
#                 buy_price = 0

#             # 计算未实现利润
#             stats['unrealized_value'] = remaining_amount * (current_price - buy_price)
#             total_unrealized_profit += stats['unrealized_value']
#         else:
#             stats['unrealized_value'] = 0

#     total_cost = sum(stats['cost'] for stats in aggregated_tokens.values())
#     total_profit = sum(stats['profit'] for stats in aggregated_tokens.values())

#     # 计算总 PNL
#     pnl = total_profit + total_unrealized_profit

#     profitable_tokens = sum(1 for stats in aggregated_tokens.values() if stats['profit'] > stats['cost'])
#     total_tokens = len(aggregated_tokens)

#     daily_pnl_chart = [
#         f"{daily_pnl.get((datetime.now(timezone.utc) - timedelta(days=i)).strftime('%Y-%m-%d'), 0):.2f}"
#         for i in range(days)
#     ]

#     average_cost = total_cost / total_buy if total_buy > 0 else 0
#     pnl_percentage = (pnl / total_cost) * 100 if total_cost > 0 else 0
#     win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0

#     realized_profits = [stats['profit'] - stats['cost'] for stats in aggregated_tokens.values() if stats['profit'] > stats['cost']]
#     avg_realized_profit = sum(realized_profits) / len(realized_profits) if realized_profits else 0

#     # 调用计算收益分布
#     distribution, distribution_percentage = await calculate_distribution(aggregated_tokens, days)

#     # 计算资产杠杆
#     asset_multiple = pnl_percentage / 100 if total_cost > 0 else 0
#     # print(aggregated_tokens)
#     return {
#         "asset_multiple": round(asset_multiple, 1),
#         "total_buy": total_buy,
#         "total_sell": total_sell,
#         "total_transaction_num": total_buy + total_sell,
#         "total_cost": total_cost,
#         "average_cost": average_cost,
#         "total_profit": total_profit,
#         "pnl": pnl,
#         "pnl_percentage": round(pnl_percentage, 2),
#         "win_rate": round(win_rate, 2),
#         "avg_realized_profit": round(avg_realized_profit, 2),
#         "daily_pnl_chart": ",".join(daily_pnl_chart),
#         "total_unrealized_profit": total_unrealized_profit,
#         **distribution,
#         **distribution_percentage
#     }

# @async_log_execution_time
# async def filter_smart_wallets(wallet_transactions, rules, sol_usdt_price, session, client):
#     # print(wallet_transactions)
#     """
#     根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
#     """
#     # smart_wallets = []
#     for wallet_address, transactions in wallet_transactions.items():
#         token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
#         token_last_trade_time = {}
#         for transaction in transactions:
#             for token_mint, data in transaction.items():
#                 if not isinstance(data, dict):
#                     continue
#                 token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
#                 token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
#                 token_summary[token_mint]['cost'] += data.get('cost', 0)
#                 token_summary[token_mint]['profit'] += data.get('profit', 0)
#                 token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), data.get('timestamp', 0))

#         sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
#         recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  # 获取最近的三个代币

#         token_list=[]
#         for token in recent_tokens:
#             token_info = TokenUtils.get_token_info(token)
            
#             # Check if token_info is a dictionary and contains the 'symbol' field
#             if isinstance(token_info, dict):
#                 symbol = token_info.get('symbol', {})
#                 if isinstance(symbol, str) and symbol:  # If symbol is valid
#                     token_list.append(symbol)
#                 else:
#                     token_list.append('')  # If symbol is invalid, append an empty string
#             else:
#                 token_list.append('')  # If token_info is not a dict, append empty string

#         # Join token symbols into a comma-separated string
#         token_list_str = ','.join(token_list)

#         # 计算 1 日、7 日、30 日统计数据
#         stats_1d = await calculate_statistics(transactions, 1, sol_usdt_price)
#         stats_7d = await calculate_statistics(transactions, 7, sol_usdt_price)
#         stats_30d = await calculate_statistics(transactions, 30, sol_usdt_price)

#         unique_token_count = len(token_summary)

#         if (
#             stats_30d["win_rate"] >= rules.get("win_rate", 50) and
#             unique_token_count > rules.get("total_transaction", 10) and
#             stats_30d["pnl"] > rules.get("PNL", 0)
#         ):
#             sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
#             wallet_data = {
#                 "wallet_address": wallet_address,
#                 "balance": round(sol_balance_data["balance"]["float"], 3),
#                 "balance_USD": round(sol_balance_data["balance_usd"], 2),
#                 "chain": "SOLANA",
#                 "is_pump_wallet": True,
#                 "asset_multiple": stats_30d["asset_multiple"],  # 传入 asset_multiple
#                 "token_list": token_list_str,  # 传入最近的三个代币符号
#                 "stats_1d": stats_1d,
#                 "stats_7d": stats_7d,
#                 "stats_30d": stats_30d,
#                 "token_summary": token_summary,
#                 "last_transaction_time": max(tx["timestamp"] for tx in transactions),
#             }
#             await write_wallet_data_to_db(session, wallet_data)
#             return True
#         else:
#             print(stats_30d["win_rate"])
#             print(unique_token_count)
#             print(stats_30d["pnl"])
#         return False
#     # return smart_wallets

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from models import *
from loguru_logger import *
from token_info import TokenUtils

async def calculate_distribution(aggregated_tokens, days):
    """
    计算7天或30天内的收益分布
    """
    distribution = {
        'distribution_gt500': 0,
        'distribution_200to500': 0,
        'distribution_0to200': 0,
        'distribution_0to50': 0,
        'distribution_lt50': 0
    }

    # 计算分布
    for stats in aggregated_tokens.values():
        if stats['cost'] > 0:  # 防止除零错误
            pnl_percentage = ((stats['profit'] - stats['cost']) / stats['cost']) * 100
            if pnl_percentage > 500:
                distribution['distribution_gt500'] += 1
            elif 200 <= pnl_percentage <= 500:
                distribution['distribution_200to500'] += 1
            elif 0 <= pnl_percentage < 200:
                distribution['distribution_0to200'] += 1
            elif 0 <= pnl_percentage < -50:
                distribution['distribution_0to50'] += 1
            elif pnl_percentage < -50:
                distribution['distribution_lt50'] += 1

    # 计算分布百分比
    total_distribution = sum(distribution.values())    
    distribution_percentage = {
        'distribution_gt500_percentage': round((distribution['distribution_gt500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_200to500_percentage': round((distribution['distribution_200to500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_0to200_percentage': round((distribution['distribution_0to200'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_0to50_percentage': round((distribution['distribution_0to50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_lt50_percentage': round((distribution['distribution_lt50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
    }

    return distribution, distribution_percentage

async def calculate_statistics(transactions, days, sol_usdt_price):
    """
    计算统计数据，包括总买卖次数、总成本、平均成本、PNL、每日PNL图等。
    """
    # 定义稳定币地址
    STABLECOINS = [
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"   # USDC
    ]

    current_timestamp = int(time.time())  # 当前时间的时间戳（单位：秒）

    # 计算 `days` 天前的时间戳
    start_timestamp = current_timestamp - (days * 24 * 60 * 60)  # 减去指定天数的秒数

    # 如果传入的 transactions 已经是 30 天内的数据，直接使用；否则进行过滤
    filtered_transactions = transactions if days == 30 else [
        tx for tx in transactions if tx['timestamp'] >= start_timestamp
    ]

    aggregated_tokens = defaultdict(lambda: {
        'buy_amount': 0,
        'sell_amount': 0,
        'cost': 0,
        'profit': 0,
        'remaining_amount': 0,
        'unrealized_value': 0
    })

    total_buy = total_sell = total_cost = total_profit = total_unrealized_profit = 0
    daily_pnl = {}
    profitable_tokens = total_tokens = 0

    for tx in filtered_transactions:
        for token, stats in tx.items():
            if token in ["timestamp", "signature"]:
                continue

            # 跳过稳定币交易
            if token in STABLECOINS:
                continue
            
            total_buy += 1 if stats['buy_amount'] > 0 else 0
            total_sell += 1 if stats['sell_amount'] > 0 else 0

            aggregated_tokens[token]['buy_amount'] += stats['buy_amount']
            aggregated_tokens[token]['sell_amount'] += stats['sell_amount']
            aggregated_tokens[token]['cost'] += stats['cost']
            aggregated_tokens[token]['profit'] += stats['profit']

            date_str = time.strftime("%Y-%m-%d", time.gmtime(tx['timestamp']))
            daily_pnl[date_str] = daily_pnl.get(date_str, 0) + (stats['profit'] - stats['cost'])

    for token, stats in aggregated_tokens.items():
        remaining_amount = stats['buy_amount'] - stats['sell_amount']
        stats['remaining_amount'] = remaining_amount

        if remaining_amount > 0:
            token_info = TokenUtils.get_token_info(token)
            current_price = token_info.get("priceUsd", 0)

            # 计算买入均价
            if stats['buy_amount'] > 0:
                buy_price = stats['cost'] / stats['buy_amount']
            else:
                buy_price = 0

            # 计算未实现利润
            if current_price == 0:
                stats['unrealized_value'] = 0
                total_unrealized_profit += stats['unrealized_value']
            else:
                stats['unrealized_value'] = remaining_amount * (current_price - buy_price)
                total_unrealized_profit += stats['unrealized_value']
        else:
            stats['unrealized_value'] = 0

    total_cost = sum(stats['cost'] for stats in aggregated_tokens.values())
    total_profit = sum(stats['profit'] for stats in aggregated_tokens.values())

    # 计算总 PNL
    pnl = total_profit - total_cost

    profitable_tokens = sum(1 for stats in aggregated_tokens.values() if stats['profit'] > stats['cost'])
    total_tokens = len(aggregated_tokens)

    daily_pnl_chart = [
        f"{daily_pnl.get((datetime.now(timezone.utc) - timedelta(days=i)).strftime('%Y-%m-%d'), 0):.2f}"
        for i in range(days)
    ]

    average_cost = total_cost / total_buy if total_buy > 0 else 0
    pnl_percentage = max((pnl / total_cost) * 100, -100) if total_cost > 0 else 0
    win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0

    realized_profits = [stats['profit'] - stats['cost'] for stats in aggregated_tokens.values() if stats['profit'] > stats['cost']]
    avg_realized_profit = sum(realized_profits) / len(realized_profits) if realized_profits else 0

    # 调用计算收益分布
    distribution, distribution_percentage = await calculate_distribution(aggregated_tokens, days)

    # 计算资产杠杆
    asset_multiple = float(pnl_percentage / 100) if total_cost > 0 else 0.0

    return {
        "asset_multiple": round(asset_multiple, 2),
        "total_buy": total_buy,
        "total_sell": total_sell,
        "total_transaction_num": total_buy + total_sell,
        "total_cost": total_cost,
        "average_cost": average_cost,
        "total_profit": total_profit,
        "pnl": pnl,
        "pnl_percentage": round(pnl_percentage, 2),
        "win_rate": round(win_rate, 2),
        "avg_realized_profit": round(avg_realized_profit, 2),
        "daily_pnl_chart": ",".join(daily_pnl_chart),
        "total_unrealized_profit": total_unrealized_profit,
        **distribution,
        **distribution_percentage
    }

@async_log_execution_time
async def filter_smart_wallets(wallet_transactions, sol_usdt_price, session, client, chain, wallet_type):
    """
    根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
    """
    # smart_wallets = []
    for wallet_address, transactions in wallet_transactions.items():
        token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
        token_last_trade_time = {}
        for transaction in transactions:
            for token_mint, data in transaction.items():
                if not isinstance(data, dict):
                    continue
                token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
                token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
                token_summary[token_mint]['cost'] += data.get('cost', 0)
                token_summary[token_mint]['profit'] += data.get('profit', 0)
                token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), data.get('timestamp', 0))
        sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
        recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  # 获取最近的三个代币

        token_list=[]
        for token in recent_tokens:
            token_info = TokenUtils.get_token_info(token)
            
            # Check if token_info is a dictionary and contains the 'symbol' field
            if isinstance(token_info, dict):
                symbol = token_info.get('symbol', {})
                if isinstance(symbol, str) and symbol:  # If symbol is valid
                    token_list.append(symbol)

        # Join token symbols into a comma-separated string
        token_list_str = ','.join(filter(None, token_list))

        # 计算 1 日、7 日、30 日统计数据
        stats_1d = await calculate_statistics(transactions, 1, sol_usdt_price)
        stats_7d = await calculate_statistics(transactions, 7, sol_usdt_price)
        stats_30d = await calculate_statistics(transactions, 30, sol_usdt_price)

        sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(sol_balance_data["balance"]["float"], 3),
            "balance_USD": round(sol_balance_data["balance_usd"], 2),
            "chain": "SOLANA",
            "tag": "",
            "is_smart_wallet": True,
            "wallet_type": wallet_type,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": str(token_list_str),
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": max(tx["timestamp"] for tx in transactions),
        }

        if (
            stats_7d.get("total_cost", 0) > 0 and
            stats_30d.get("pnl", 0) > 0 and
            stats_30d.get("win_rate", 0) > 30 and
            float(stats_30d.get("asset_multiple", 0)) > 0.3 and
            stats_30d.get("total_transaction_num", 0) < 3000
        ):
            await write_wallet_data_to_db(session, wallet_data, chain)  # 写入数据库
            return True  # 返回 True 表示满足条件
        else:
            return False
        
@async_log_execution_time
async def update_smart_wallets_filter(wallet_transactions, sol_usdt_price, session, client, chain):
    """
    根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
    """
    # smart_wallets = []
    for wallet_address, transactions in wallet_transactions.items():
        token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
        token_last_trade_time = {}
        for transaction in transactions:
            for token_mint, data in transaction.items():
                if not isinstance(data, dict):
                    continue
                token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
                token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
                token_summary[token_mint]['cost'] += data.get('cost', 0)
                token_summary[token_mint]['profit'] += data.get('profit', 0)
                token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), data.get('timestamp', 0))
        sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
        recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  # 获取最近的三个代币

        token_list=[]
        for token in recent_tokens:
            token_info = TokenUtils.get_token_info(token)
            
            # Check if token_info is a dictionary and contains the 'symbol' field
            if isinstance(token_info, dict):
                symbol = token_info.get('symbol', {})
                if isinstance(symbol, str) and symbol:  # If symbol is valid
                    token_list.append(symbol)

        # Join token symbols into a comma-separated string
        token_list_str = ','.join(filter(None, token_list))

        # 计算 1 日、7 日、30 日统计数据
        stats_1d = await calculate_statistics(transactions, 1, sol_usdt_price)
        stats_7d = await calculate_statistics(transactions, 7, sol_usdt_price)
        stats_30d = await calculate_statistics(transactions, 30, sol_usdt_price)

        sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(sol_balance_data["balance"]["float"], 3),
            "balance_USD": round(sol_balance_data["balance_usd"], 2),
            "chain": "SOLANA",
            "tag": "",
            "is_smart_wallet": True,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": str(token_list_str),
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": max(tx["timestamp"] for tx in transactions),
        }

        await write_wallet_data_to_db(session, wallet_data, chain)
        if (
            stats_7d.get("total_cost", 0) > 0 and
            stats_30d.get("pnl", 0) > 0 and
            stats_30d.get("win_rate", 0) > 30 and
            float(stats_30d.get("asset_multiple", 0)) > 0.3 and
            stats_30d.get("total_transaction_num", 0) < 3000
        ):            
            return True  # 返回 True 表示满足条件
        else:
            return False

@async_log_execution_time
async def filter_smart_wallets_true(
        wallet_transactions, 
        sol_usdt_price, 
        session, 
        client, 
        chain, 
        is_smart_wallet=None,  # 可选参数
        wallet_type=None       # 可选参数
    ):
    """
    根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
    """
    # smart_wallets = []
    for wallet_address, transactions in wallet_transactions.items():
        token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
        token_last_trade_time = {}
        for transaction in transactions:
            for token_mint, data in transaction.items():
                if not isinstance(data, dict):
                    continue
                token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
                token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
                token_summary[token_mint]['cost'] += data.get('cost', 0)
                token_summary[token_mint]['profit'] += data.get('profit', 0)
                token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), data.get('timestamp', 0))
        sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
        recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  # 获取最近的三个代币

        token_list=[]
        for token in recent_tokens:
            token_info = TokenUtils.get_token_info(token)
            
            # Check if token_info is a dictionary and contains the 'symbol' field
            if isinstance(token_info, dict):
                symbol = token_info.get('symbol', {})
                if isinstance(symbol, str) and symbol:  # If symbol is valid
                    token_list.append(symbol)

        # Join token symbols into a comma-separated string
        token_list_str = ','.join(filter(None, token_list))

        # 计算 1 日、7 日、30 日统计数据
        stats_1d = await calculate_statistics(transactions, 1, sol_usdt_price)
        stats_7d = await calculate_statistics(transactions, 7, sol_usdt_price)
        stats_30d = await calculate_statistics(transactions, 30, sol_usdt_price)

        sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)

        # if is_smart_wallet is None:
        #     is_smart_wallet = False
        # if wallet_type is None:
        #     wallet_type = 0
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(sol_balance_data["balance"]["float"], 3),
            "balance_USD": round(sol_balance_data["balance_usd"], 2),
            "chain": "SOLANA",
            "tag": "",
            "is_smart_wallet": is_smart_wallet,
            "wallet_type": wallet_type,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": str(token_list_str),
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": max(tx["timestamp"] for tx in transactions),
        }

        await write_wallet_data_to_db(session, wallet_data, chain)
        return True