import asyncio
import requests
import random
import time
import sys
import os
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from datetime import datetime, timezone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from models import *

min_delay = 5
max_delay = 10

def get_USD_price():
    try:
        # url = f"https://api.dexscreener.com/latest/dex/tokens/0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        url = f"https://min-api.cryptocompare.com/data/price?fsym=ETH&tsyms=USD"
        
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            return float(data['USD'])
        else:
            data = response.json()
    except Exception as e:
        print(f"獲取代幣信息時出錯: {e}")

def get_wallet_details_30d(wallet_address):    
    payload_1 = { 'api_key': 'acb979481dfe43dd088f1bba026bd4a3',
                'url': f'https://gmgn.ai/defi/quotation/v1/smartmoney/eth/walletNew/{wallet_address}?period=30d',
                'autoparse': 'true' }
    payload_2 = { 'api_key': 'acb979481dfe43dd088f1bba026bd4a3',
                'url': f'https://gmgn.ai/defi/quotation/v1/smartmoney/eth/daily_profit/{wallet_address}?period=30d',
                'autoparse': 'true' }
    try:
    # 發送GET請求
        response_1 = requests.get('https://api.scraperapi.com/', params=payload_1)
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
        response_2 = requests.get('https://api.scraperapi.com/', params=payload_2)

        # 確保請求成功
        if response_1.status_code == 200 and response_2.status_code == 200:
            # 解析JSON數據
            return response_1.json(), response_2.json()
        else:
            print(f"请求详细数据失败，状态码: {response_1.status_code}")
            return None, None
    except Exception as e:
        print(f"获取详细数据时出错: {e}")
        return None, None
    
def get_wallet_transaction_history(wallet_address, limit=20):
    """
    獲取指定錢包的交易歷史記錄
    """
    payload = { 'api_key': 'acb979481dfe43dd088f1bba026bd4a3',
                'url': f"https://gmgn.ai/api/v1/wallet_activity/eth?type=buy&type=sell&wallet={wallet_address}&limit={limit}&cost=10",
                'autoparse': 'true' }
    try:
        response = requests.get('https://api.scraperapi.com/', params=payload)
        if response.status_code == 200:
            data = response.json()
            if data["code"] == 0:
                return data["data"]["activities"]
            else:
                print(f"Error: {data['message']}")
                return None
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return None
    except Exception as e:
        print(f"获取详细数据时出错: {e}")
        return None, None
    
def get_wallet_holdings(wallet_address, limit=20):
    """
    獲取指定錢包的持倉數據
    """
    payload = { 'api_key': 'acb979481dfe43dd088f1bba026bd4a3',
                'url': f"https://gmgn.ai/api/v1/wallet_holdings/eth/{wallet_address}?limit={limit}&orderby=last_active_timestamp&direction=desc&showsmall=false&sellout=false&hide_abnormal=false",
                'autoparse': 'true' }
    try:
        # 發送GET請求
        response = requests.get('https://api.scraperapi.com/', params=payload)
        
        # 確保請求成功
        if response.status_code == 200:
            data = response.json()
            if data["code"] == 0:
                return data["data"]["holdings"]
            else:
                print(f"Error: {data['message']}")
                return None
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching holdings: {e}")
        return None
    
# ------------------------------------------------------------------------------------------------------------------------

def transform_wallet_data(wallet):
    # 获取最近买入的三种token的symbol
    recent_buy_tokens = wallet.get("recent_buy_tokens", [])
    token_list = ",".join([token["symbol"] for token in recent_buy_tokens[:3]])
    token_price_USD = get_USD_price()

    # 获取钱包详细信息
    wallet_details_30d, daily_profit = get_wallet_details_30d(wallet.get("wallet_address"))
    detailed_data_30d = wallet_details_30d.get("data", {})

    distributions_30d = [
        detailed_data_30d.get("pnl_gt_5x_num", 0),
        detailed_data_30d.get("pnl_2x_5x_num", 0),
        detailed_data_30d.get("pnl_lt_2x_num", 0),
        detailed_data_30d.get("pnl_minus_dot5_0x_num", 0),
        detailed_data_30d.get("pnl_lt_minus_dot5_num", 0),
    ]

    total_distribution_30d = sum(distributions_30d)

    # 如果总和大于0，则计算百分比
    percentages_30d = [d / total_distribution_30d * 100 if total_distribution_30d > 0 else 0 for d in distributions_30d]

    # 获取 daily_profit_7d 和 daily_profit_30d 数据
    daily_profit_7d = wallet.get("daily_profit_7d", [])
    # daily_profit_30d = wallet.get("daily_profit_30d", [])

    # 获取当前时间戳
    current_time = int(time.time())

    # 创建一个字典来保存过去30天的盈亏数据，初始化为0
    daily_profit_dict_30d = {i: 0 for i in range(30)}

    # 创建一个字典来保存过去7天的盈亏数据，初始化为0
    daily_profit_dict_7d = {i: 0 for i in range(7)}

    # 填充 30 天的盈亏数据
    for record in daily_profit['data']:
        timestamp = record.get('timestamp')
        profit = record.get('profit', 0)
        
        # 计算该 timestamp 对应的天数（当前时间减去该 timestamp）
        day_diff = (current_time - timestamp) // (24 * 3600)  # 计算相差天数
        
        if 0 <= day_diff < 30:  # 只考虑过去30天的数据
            daily_profit_dict_30d[day_diff] = round(profit, 2)

    # 填充 7 天的盈亏数据
    for record in daily_profit_7d:
        timestamp = record.get('timestamp')
        profit = record.get('profit', 0)
        
        # 计算该 timestamp 对应的天数（当前时间减去该 timestamp）
        day_diff = (current_time - timestamp) // (24 * 3600)  # 计算相差天数
        
        if 0 <= day_diff < 7:  # 只考虑过去7天的数据
            daily_profit_dict_7d[day_diff] = round(profit, 2)

    # 将 daily_profit_dict_30d 和 daily_profit_dict_7d 转换为逗号分隔的字符串
    daily_pnl_chart_30d = ",".join([str(daily_profit_dict_30d[i]) for i in range(30)])
    daily_pnl_chart_7d = ",".join([str(daily_profit_dict_7d[i]) for i in range(7)])

    test_wallet_data = {
        "wallet_address": wallet.get("wallet_address", None),
        "balance": wallet.get("eth_balance", None),
        "balance_USD": wallet.get("eth_balance", None) * token_price_USD,
        "chain": "ETH",
        "twitter_name": wallet.get("twitter_name", None),
        "twitter_username": wallet.get("twitter_username", None),
        "is_smart_wallet": True,
        "is_pump_wallet": False,
        "asset_multiple": None,
        "token_list": token_list,  # 将获取到的token_list加入
        "tag": "GMGN smart money",
        "stats_30d": {
            "average_cost": detailed_data_30d.get("token_avg_cost", None),
            "total_transaction_num": wallet.get("txs_30d", None),
            "total_buy": wallet.get("buy_30d", None),
            "total_sell": wallet.get("sell_30d", None),
            "win_rate": detailed_data_30d.get("winrate", None) * 100,
            "pnl": wallet.get("realized_profit_30d", None),
            "pnl_percentage": wallet.get("pnl_30d", None),
            "daily_pnl_chart": daily_pnl_chart_30d,  # 更新为30天的盈亏数据
            "total_unrealized_profit": detailed_data_30d.get("unrealized_profit", None),
            "total_cost": detailed_data_30d.get("history_bought_cost", None),
            "avg_realized_profit": detailed_data_30d.get("realized_profit_30d", None),
            "distribution_gt500": distributions_30d[0],
            "distribution_200to500": distributions_30d[1],
            "distribution_0to200": distributions_30d[2],
            "distribution_0to50": distributions_30d[3],
            "distribution_lt50": distributions_30d[4],
            "distribution_gt500_percentage": round(percentages_30d[0], 2),
            "distribution_200to500_percentage": round(percentages_30d[1], 2),
            "distribution_0to200_percentage": round(percentages_30d[2], 2),
            "distribution_0to50_percentage": round(percentages_30d[3], 2),
            "distribution_lt50_percentage": round(percentages_30d[4], 2),
        },
        "stats_7d": {
            "average_cost": wallet.get("avg_cost_7d", None),
            "total_transaction_num": wallet.get("txs", None),
            "total_buy": detailed_data_30d.get("buy_7d", None),
            "total_sell": detailed_data_30d.get("sell_7d", None),
            "win_rate": wallet.get("winrate_7d", None) * 100,
            "pnl": wallet.get("realized_profit_7d", None),
            "pnl_percentage": wallet.get("pnl_7d", None),
            "daily_pnl_chart": daily_pnl_chart_7d,  # 更新为7天的盈亏数据
            "total_unrealized_profit": 100.0,
            "total_cost": 500.0,
            "avg_realized_profit": 50.0,
            "distribution_gt500": wallet.get("pnl_gt_5x_num_7d", None),
            "distribution_200to500": wallet.get("pnl_2x_5x_num_7d", None),
            "distribution_0to200": wallet.get("pnl_lt_2x_num_7d", None),
            "distribution_0to50": wallet.get("pnl_minus_dot5_0x_num_7d", None),
            "distribution_lt50": wallet.get("pnl_lt_minus_dot5_num_7d", None),
            "distribution_gt500_percentage": wallet.get("pnl_gt_5x_num_7d_ratio", None),
            "distribution_200to500_percentage": wallet.get("pnl_2x_5x_num_7d_ratio", None),
            "distribution_0to200_percentage": wallet.get("pnl_lt_2x_num_7d_ratio", None),
            "distribution_0to50_percentage": wallet.get("pnl_minus_dot5_0x_num_7d_ratio", None),
            "distribution_lt50_percentage": wallet.get("pnl_lt_minus_dot5_num_7d_ratio", None),
        },
        "stats_1d": {
            "average_cost": wallet.get("avg_cost_1d", None),
            "total_transaction_num": wallet.get("txs_1d", None),
            "total_buy": wallet.get("buy_1d", None),
            "total_sell": wallet.get("sell_1d", None),
            "win_rate": wallet.get("winrate_1d", None),
            "pnl": wallet.get("realized_profit_1d", None),
            "pnl_percentage": wallet.get("pnl_1d", None),
            "daily_pnl_chart": None,
            "total_unrealized_profit": None,
            "total_cost": None,
            "avg_realized_profit": None,
        },
        "last_transaction_time": int(wallet.get("last_active", None)),
    }
    return test_wallet_data

async def process_and_write_wallets(session, wallets):

    for wallet in wallets[:50]:
        transformed_data = transform_wallet_data(wallet)
        result = await write_wallet_data_to_db(session, transformed_data, "ETH")
        if result:
            print(f"Wallet {transformed_data['wallet_address']} written successfully.")
        else:
            print(f"Failed to write wallet {transformed_data['wallet_address']}.")

async def process_and_save_transactions(session, wallet_addresses):
    try:
        # 查詢資料庫中所有錢包的地址
        # wallet_addresses = await session.execute(select(WalletSummary.address))
        # wallet_addresses = [wallet_address[0] for wallet_address in wallet_addresses.fetchall()]

        if not wallet_addresses:
            print("No wallet addresses found.")
            return

        # 遍歷每個錢包地址
        for wallet_address in wallet_addresses:
            delay = random.uniform(min_delay, max_delay)
            transactions = get_wallet_transaction_history(wallet_address)
            await asyncio.sleep(delay)
            if transactions:
                for transaction in transactions:
                    # 提取交易信息
                    tx_data = {
                        'wallet_address': wallet_address,
                        'token_address': transaction.get("token", {}).get("address"),
                        'token_icon': transaction.get("token", {}).get("logo"),
                        'token_name': transaction.get("token", {}).get("symbol"),
                        'price': float(transaction.get("price_usd", 0)),
                        'amount': float(transaction.get("token_amount", 0)),
                        'marketcap': float(transaction.get("marketcap", 0)),
                        'value': float(transaction.get("cost_usd", 0)),
                        'holding_percentage': float(transaction.get("holding_percentage", 0)),
                        'chain': 'ETH',
                        'realized_profit': float(transaction.get("realized_profit", 0)),
                        'transaction_type': transaction.get("event_type"),
                        'transaction_time': transaction.get("timestamp"),
                    }

                    # Example of signature (could be a placeholder or actual value)
                    signature = transaction.get("tx_hash")

                    # Save transaction to DB
                    await save_past_transaction(session, tx_data, wallet_address, signature, "ETH")
                    print(f"Transaction {transaction['tx_hash']} saved successfully.")
            else:
                print(f"No transaction history found for {wallet_address}")
    except Exception as e:
        print(f"Error processing transactions: {str(e)}")

async def process_and_save_holdings(session, wallet_addresses):
    try:
        if not wallet_addresses:
            print("No wallet addresses found.")
            return

        for wallet_address in wallet_addresses:
            delay = random.uniform(min_delay, max_delay)
            holdings = get_wallet_holdings(wallet_address)
            await asyncio.sleep(delay)
            
            if holdings:
                tx_data_list = []  # 創建一個列表來收集所有的 tx_data
                
                for holding in holdings:
                    if isinstance(holding, str):
                        try:
                            holding = json.loads(holding)
                        except json.JSONDecodeError:
                            print(f"Could not parse holding data: {holding}")
                            continue
                    
                    if isinstance(holding, dict):
                        try:
                            token_data = holding.get("token", {})
                            tx_data = {
                                'wallet_address': wallet_address,
                                'token_address': token_data.get('address'),
                                'token_icon': token_data.get('logo'),
                                'token_name': token_data.get('symbol'),
                                'chain': 'ETH',
                                'amount': float(holding.get('balance', 0)),
                                'value': float(holding.get('usd_value', 0)),
                                'value_USDT': float(holding.get('usd_value', 0)),
                                'unrealized_profit': float(holding.get('unrealized_profit', 0)),
                                'pnl': float(holding.get('realized_pnl', 0)),
                                'pnl_percentage': float(holding.get('realized_pnl', 0)),
                                'marketcap': float(holding.get('marketcap', 0)),
                                'sell_amount': holding.get('sell_30d', 0),
                                'buy_amount': holding.get('buy_30d', 0),
                                'cost': float(holding.get('cost', 0)),
                                'profit': float(holding.get('realized_profit_30d', 0)),
                                'last_transaction_time': holding.get('last_active_timestamp'),
                                'time': holding.get('timestamp', datetime.now())
                            }
                            tx_data_list.append(tx_data)  # 將處理好的數據添加到列表中
                            
                        except (ValueError, TypeError) as e:
                            print(f"Error processing holding data for {wallet_address}: {str(e)}")
                            continue
                
                # 一次性保存該錢包的所有持倉數據
                if tx_data_list:
                    await save_holding(tx_data_list, wallet_address, session, "ETH")
                    print(f"Holdings for wallet {wallet_address} saved successfully.")
            else:
                print(f"No holdings found for {wallet_address}")
                
    except Exception as e:
        print(f"Error processing holdings: {str(e)}")

async def process_wallet_data(session, wallets):
    """
    将钱包数据处理并保存到数据库中
    """
    # 处理并保存钱包数据
    await process_and_write_wallets(session, wallets)
    
    # 并行执行保存交易和持仓数据
    wallet_addresses = await get_active_wallets(session, "ETH")
    await asyncio.gather(
        process_and_save_transactions(session, wallet_addresses),
        process_and_save_holdings(session, wallet_addresses)
    )

async def main():
    # 定义目标URL
    payload = {
        'api_key': 'acb979481dfe43dd088f1bba026bd4a3',
        'url': 'https://gmgn.ai/defi/quotation/v1/rank/eth/wallets/7d?tag=smart_degen&tag=pump_smart&orderby=pnl_7d&direction=desc',
        'autoparse': 'true'
    }

    # 发送GET请求
    response = requests.get('https://api.scraperapi.com/', params=payload)

    # 确保请求成功
    if response.status_code == 200:
        # 解析JSON数据
        wallets = response.json().get("data", {}).get("rank", [])
        session_factory = sessions.get("ETH".upper())

        async with session_factory() as session:
            # 处理钱包数据并保存
            await process_wallet_data(session, wallets)
    else:
        print(f"请求失败，状态码: {response.status_code}")

def start_scheduler():
    scheduler = AsyncIOScheduler()
    scheduler.start()
    print("Scheduler started.")  # 确认调度器启动

    asyncio.create_task(main())
    scheduler.add_job(main, 'interval', weeks=1)  # 为测试设置为每分钟运行一次

    return scheduler

async def run():
    start_scheduler()
    print("Scheduler initialized.")
    while True:
        await asyncio.sleep(3600)  # 每小时检查调度器状态

if __name__ == "__main__":
    try:
        # 使用事件循环来运行调度器
        asyncio.run(run())
    except RuntimeError as e:
        print(f"Runtime error: {e}")

# --------------------------------------------------------------------------------------------------------------------

# import asyncio
# import requests
# import random
# import time
# import sys
# import os
# from datetime import datetime, timezone, timedelta
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
# from models import *  # 根據你的資料庫模型進行相應的導入

# # 配置最小和最大延遲
# min_delay = 10
# max_delay = 20

# # 最大重試次數
# MAX_RETRIES = 5
# # 初始重試延遲時間（秒）
# INITIAL_DELAY = 1
# # 重試延遲增長係數（指數回退）
# BACKOFF_FACTOR = 2

# API_KEYS = [
#     '8c5d68c2d97cb1f29f53e3e94f28f340',
#     'ef9432596aaf573f74bac4b222ecd486'
# ]

# def get_USD_price():
#     try:
#         # 根據區塊鏈種類，選擇不同的token (ETH, TRON等)
#         url = f"https://min-api.cryptocompare.com/data/price?fsym=TRON&tsyms=USD"  # 可以根據不同鏈更改
#         response = requests.get(url)
#         if response.status_code == 200:
#             data = response.json()
#             return float(data['USD'])
#         else:
#             print(f"Failed to get USD price: {response.status_code}")
#     except Exception as e:
#         print(f"Error fetching USD price: {e}")

# async def fetch_wallet_data(chain, wallet_address, payload):
#     """
#     根據區塊鏈和錢包地址獲取數據，並加上重試機制
#     """
#     retries = 0
#     while retries < MAX_RETRIES:
#         try:
#             # 設置不同的 API URL 和參數
#             if chain == "ETH":
#                 base_url = "https://gmgn.ai/defi/quotation/v1/smartmoney/eth"
#             elif chain == "BASE":
#                 base_url = "https://gmgn.ai/defi/quotation/v1/smartmoney/base"
#             elif chain == "TRON":
#                 base_url = "https://gmgn.ai/defi/quotation/v1/smartmoney/tron"
#             else:
#                 raise ValueError("Unsupported chain")

#             url_1 = f"{base_url}/walletNew/{wallet_address}?period=30d"
#             url_2 = f"{base_url}/daily_profit/{wallet_address}?period=30d"
#             payload_1 = { **payload, 'url': url_1 }
#             payload_2 = { **payload, 'url': url_2 }

#             response_1 = requests.get('https://api.scraperapi.com/', params=payload_1)
#             time.sleep(random.uniform(min_delay, max_delay))  # 延遲
#             response_2 = requests.get('https://api.scraperapi.com/', params=payload_2)

#             if response_1.status_code == 200 and response_2.status_code == 200:
#                 return response_1.json(), response_2.json()
#             elif response_1.status_code == 499 or response_2.status_code == 499:
#                 print(f"Received 499 error, retrying...")
#                 retries += 1
#                 delay = INITIAL_DELAY * (BACKOFF_FACTOR ** retries)
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 print(f"Request failed with status code: {response_1.status_code} or {response_2.status_code}")
#                 return None, None
#         except Exception as e:
#             print(f"Error fetching data: {e}")
#             retries += 1
#             delay = INITIAL_DELAY * (BACKOFF_FACTOR ** retries)
#             print(f"Retrying in {delay} seconds...")
#             time.sleep(delay)

#     print("Max retries reached, giving up.")
#     return None, None

# def get_wallet_transaction_history(wallet_address, chain, limit=20):
#     """
#     獲取指定錢包的交易歷史記錄
#     """
#     try:
#         # 隨機選擇API key
#         api_key = random.choice(API_KEYS)

#         payload = {
#             'api_key': api_key,
#             'url': f"https://gmgn.ai/api/v1/wallet_activity/{chain}?type=buy&type=sell&wallet={wallet_address}&limit={limit}&cost=10",
#             'autoparse': 'true'
#         }
        
#         response = requests.get('https://api.scraperapi.com/', params=payload)
#         if response.status_code == 200:
#             data = response.json()
#             if data["code"] == 0:
#                 return data["data"]["activities"]
#             else:
#                 print(f"Error: {data['message']}")
#                 return None
#         else:
#             print(f"Failed to fetch data: {response.status_code}")
#             return None
#     except Exception as e:
#         print(f"Error fetching transaction data: {e}")
#         return None

# def transform_wallet_data(wallet, chain, payload):
#     """
#     Transform wallet data for saving to the database.
#     """
#     recent_buy_tokens = wallet.get("recent_buy_tokens", [])
#     token_list = ",".join([token["symbol"] for token in recent_buy_tokens[:3]])
#     token_price_USD = get_USD_price()

#     wallet_details_30d, daily_profit = asyncio.run(fetch_wallet_data(chain, wallet.get("wallet_address"), payload))
    
#     # 這裡處理None的情況
#     if wallet_details_30d is None or daily_profit is None:
#         return None

#     detailed_data_30d = wallet_details_30d.get("data", {})

#     distributions_30d = [
#         detailed_data_30d.get("pnl_gt_5x_num", 0),
#         detailed_data_30d.get("pnl_2x_5x_num", 0),
#         detailed_data_30d.get("pnl_lt_2x_num", 0),
#         detailed_data_30d.get("pnl_minus_dot5_0x_num", 0),
#         detailed_data_30d.get("pnl_lt_minus_dot5_num", 0),
#     ]
#     total_distribution_30d = sum(distributions_30d)
#     percentages_30d = [d / total_distribution_30d * 100 if total_distribution_30d > 0 else 0 for d in distributions_30d]

#     daily_profit_7d = wallet.get("daily_profit_7d", [])
#     current_time = int(time.time())

#     daily_profit_dict_30d = {i: 0 for i in range(30)}
#     daily_profit_dict_7d = {i: 0 for i in range(7)}

#     # 填充盈虧數據
#     for record in daily_profit['data']:
#         timestamp = record.get('timestamp')
#         profit = record.get('profit', 0)
#         day_diff = (current_time - timestamp) // (24 * 3600)
#         if 0 <= day_diff < 30:
#             daily_profit_dict_30d[day_diff] = round(profit, 2)

#     for record in daily_profit_7d:
#         timestamp = record.get('timestamp')
#         profit = record.get('profit', 0)
#         day_diff = (current_time - timestamp) // (24 * 3600)
#         if 0 <= day_diff < 7:
#             daily_profit_dict_7d[day_diff] = round(profit, 2)

#     daily_pnl_chart_30d = ",".join([str(daily_profit_dict_30d[i]) for i in range(30)])
#     daily_pnl_chart_7d = ",".join([str(daily_profit_dict_7d[i]) for i in range(7)])

#     return {
#         "wallet_address": wallet.get("wallet_address", None),
#         "balance": wallet.get("eth_balance", None),
#         "balance_USD": wallet.get("eth_balance", None) * token_price_USD,
#         "chain": "BASE",
#         "twitter_name": wallet.get("twitter_name", None) or None,
#         "twitter_username": wallet.get("twitter_username", None) or None,
#         "is_smart_wallet": True,
#         "is_pump_wallet": False,
#         "asset_multiple": None,
#         "token_list": token_list,  # 将获取到的token_list加入
#         "tag": "GMGN smart money",
#         "stats_30d": {
#             "average_cost": detailed_data_30d.get("token_avg_cost", None),
#             "total_transaction_num": wallet.get("txs_30d", None),
#             "total_buy": wallet.get("buy_30d", None),
#             "total_sell": wallet.get("sell_30d", None),
#             "win_rate": detailed_data_30d.get("winrate", None) or 0 * 100,
#             "pnl": wallet.get("realized_profit_30d", None),
#             "pnl_percentage": wallet.get("pnl_30d", None),
#             "daily_pnl_chart": daily_pnl_chart_30d,  # 更新为30天的盈亏数据
#             "total_unrealized_profit": detailed_data_30d.get("unrealized_profit", None),
#             "total_cost": detailed_data_30d.get("history_bought_cost", None),
#             "avg_realized_profit": detailed_data_30d.get("realized_profit_30d", None),
#             "distribution_gt500": distributions_30d[0],
#             "distribution_200to500": distributions_30d[1],
#             "distribution_0to200": distributions_30d[2],
#             "distribution_0to50": distributions_30d[3],
#             "distribution_lt50": distributions_30d[4],
#             "distribution_gt500_percentage": round(percentages_30d[0], 2),
#             "distribution_200to500_percentage": round(percentages_30d[1], 2),
#             "distribution_0to200_percentage": round(percentages_30d[2], 2),
#             "distribution_0to50_percentage": round(percentages_30d[3], 2),
#             "distribution_lt50_percentage": round(percentages_30d[4], 2),
#         },
#         "stats_7d": {
#             "average_cost": wallet.get("avg_cost_7d", None),
#             "total_transaction_num": wallet.get("txs", None),
#             "total_buy": detailed_data_30d.get("buy_7d", None),
#             "total_sell": detailed_data_30d.get("sell_7d", None),
#             "win_rate": wallet.get("winrate_7d", 0) * 100,
#             "pnl": wallet.get("realized_profit_7d", None),
#             "pnl_percentage": wallet.get("pnl_7d", None),
#             "daily_pnl_chart": daily_pnl_chart_7d,  # 更新为7天的盈亏数据
#             "total_unrealized_profit": 100.0,
#             "total_cost": 500.0,
#             "avg_realized_profit": 50.0,
#             "distribution_gt500": wallet.get("pnl_gt_5x_num_7d", None),
#             "distribution_200to500": wallet.get("pnl_2x_5x_num_7d", None),
#             "distribution_0to200": wallet.get("pnl_lt_2x_num_7d", None),
#             "distribution_0to50": wallet.get("pnl_minus_dot5_0x_num_7d", None),
#             "distribution_lt50": wallet.get("pnl_lt_minus_dot5_num_7d", None),
#             "distribution_gt500_percentage": wallet.get("pnl_gt_5x_num_7d_ratio", None),
#             "distribution_200to500_percentage": wallet.get("pnl_2x_5x_num_7d_ratio", None),
#             "distribution_0to200_percentage": wallet.get("pnl_lt_2x_num_7d_ratio", None),
#             "distribution_0to50_percentage": wallet.get("pnl_minus_dot5_0x_num_7d_ratio", None),
#             "distribution_lt50_percentage": wallet.get("pnl_lt_minus_dot5_num_7d_ratio", None),
#         },
#         "stats_1d": {
#             "average_cost": wallet.get("avg_cost_1d", None),
#             "total_transaction_num": wallet.get("txs_1d", None),
#             "total_buy": wallet.get("buy_1d", None),
#             "total_sell": wallet.get("sell_1d", None),
#             "win_rate": wallet.get("winrate_1d", None),
#             "pnl": wallet.get("realized_profit_1d", None),
#             "pnl_percentage": wallet.get("pnl_1d", None),
#             "daily_pnl_chart": None,
#             "total_unrealized_profit": None,
#             "total_cost": None,
#             "avg_realized_profit": None,
#         },
#         "last_transaction_time": int(wallet.get("last_active", None)),
#     }

# async def process_and_save_transactions(session, chain):
#     try:
#         wallet_addresses = await session.execute(select(WalletSummary.address))
#         wallet_addresses = [wallet_address[0] for wallet_address in wallet_addresses.fetchall()]

#         if not wallet_addresses:
#             print("No wallet addresses found.")
#             return

#         for wallet_address in wallet_addresses:
#             delay = random.uniform(min_delay, max_delay)
#             transactions = await get_wallet_transaction_history(wallet_address, chain)
#             await asyncio.sleep(delay)
#             if transactions:
#                 for transaction in transactions:
#                     tx_data = {
#                         'wallet_address': wallet_address,
#                         'chain': chain,
#                         'token_name': transaction.get("token", {}).get("symbol"),
#                         'transaction_time': transaction.get("timestamp"),
#                         'realized_profit': transaction.get("realized_profit", None),
#                     }
#                     signature = transaction.get("tx_hash")
#                     await save_past_transaction(session, tx_data, wallet_address, signature, chain)
#             else:
#                 print(f"No transaction history found for {wallet_address}")
#     except Exception as e:
#         print(f"Error processing transactions: {str(e)}")

# async def process_wallet_data(session, wallets, chain):
#     await asyncio.gather(
#         process_and_save_transactions(session, chain),
#     )

# async def main():
#     # 同時處理三個區塊鏈
#     chains = ["ETH", "BASE", "TRON"]
#     payload = {
#         'api_key': '8c5d68c2d97cb1f29f53e3e94f28f340',
#         'url': 'https://gmgn.ai/defi/quotation/v1/rank/{}/wallets/7d?orderby=pnl_7d&direction=desc'
#     }

#     for chain in chains:
#         payload['url'] = payload['url'].format(chain.lower())
#         response = requests.get('https://api.scraperapi.com/', params=payload)

#         if response.status_code == 200:
#             wallets = response.json().get("data", {}).get("rank", [])
#             session_factory = sessions.get(chain.upper())

#             async with session_factory() as session:
#                 await process_wallet_data(session, wallets, chain)
#         else:
#             print(f"Request failed with status code: {response.status_code}")

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except RuntimeError as e:
#         print(f"Runtime error: {e}")