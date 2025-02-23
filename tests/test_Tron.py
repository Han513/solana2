import asyncio
import requests
import random
import time
import sys
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from datetime import datetime, timezone
from models import *

min_delay = 5
max_delay = 10

def get_USD_price():
    try:
        return float(0.2558)
        # url = f"https://api.dexscreener.com/latest/dex/tokens/0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        # url = f"https://min-api.cryptocompare.com/data/price?fsym=TRON&tsyms=USD"
        
        # response = requests.get(url)
        
        # if response.status_code == 200:
        #     data = response.json()
        #     if data:
        #         return float(0.221)
        #     else:
        #         return float(0.221)
        # else:
        #     data = response.json()
    except Exception as e:
        print(f"獲取代幣信息時出錯: {e}")

def get_wallet_details_30d(wallet_address):    
    payload_1 = { 'api_key': 'b4d21e175c03905dafbca1dc561bcc44',
                'url': f'https://gmgn.ai/defi/quotation/v1/smartmoney/tron/walletNew/{wallet_address}?period=30d',
                'autoparse': 'true' }
    payload_2 = { 'api_key': 'b4d21e175c03905dafbca1dc561bcc44',
                'url': f'https://gmgn.ai/defi/quotation/v1/smartmoney/tron/daily_profit/{wallet_address}?period=30d',
                'autoparse': 'true' }
    try:
    # 發送GET請求
        delay = random.uniform(min_delay, max_delay)
        response_1 = requests.get('https://api.scraperapi.com/', params=payload_1)
        time.sleep(delay)
        time.sleep(delay)
        response_2 = requests.get('https://api.scraperapi.com/', params=payload_2)

        # 確保請求成功
        if response_1.status_code == 200 and response_2.status_code == 200:
            # 解析JSON數據
            return response_1.json(), response_2.json() or None
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
    payload = { 'api_key': 'b4d21e175c03905dafbca1dc561bcc44',
                'url': f"https://gmgn.ai/api/v1/wallet_activity/tron?type=buy&type=sell&wallet={wallet_address}&limit={limit}&cost=10",
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
    payload = { 'api_key': 'b4d21e175c03905dafbca1dc561bcc44',
                'url': f"https://gmgn.ai/api/v1/wallet_holdings/tron/{wallet_address}?limit={limit}&orderby=last_active_timestamp&direction=desc&showsmall=false&sellout=false&hide_abnormal=false",
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
        return None
    
# ------------------------------------------------------------------------------------------------------------------------

def transform_wallet_data(wallet):
    # 获取最近买入的三种token的symbol
    recent_buy_tokens = wallet.get("recent_buy_tokens", [])
    token_list = ",".join([token["symbol"] for token in recent_buy_tokens[:3]])
    token_price_USD = get_USD_price()

    # 获取钱包详细信息
    wallet_details_30d, daily_profit = get_wallet_details_30d(wallet.get("wallet_address"))
    detailed_data_30d = wallet_details_30d.get("data", {}) or {}

    distributions_30d = [
        detailed_data_30d.get("pnl_gt_5x_num", 0) or 0,
        detailed_data_30d.get("pnl_2x_5x_num", 0) or 0,
        detailed_data_30d.get("pnl_lt_2x_num", 0)or 0,
        detailed_data_30d.get("pnl_minus_dot5_0x_num", 0) or 0,
        detailed_data_30d.get("pnl_lt_minus_dot5_num", 0) or 0,
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
        "balance_USD": wallet.get("eth_balance", None) or 0 * token_price_USD,
        "chain": "TRON",
        "twitter_name": wallet.get("twitter_name", None) or None,
        "twitter_username": wallet.get("twitter_username", None) or None,
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
            "win_rate": detailed_data_30d.get("winrate", None) or 0 * 100,
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
            "win_rate": wallet.get("winrate_7d", 0) or 0 * 100,
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

    for wallet in wallets[:]:
        transformed_data = transform_wallet_data(wallet)
        result = await write_wallet_data_to_db(session, transformed_data, "TRON")
        if result:
            print(f"Wallet {transformed_data['wallet_address']} written successfully.")
        else:
            print(f"Failed to write wallet {transformed_data['wallet_address']}.")

async def process_and_save_transactions(session):
    try:
        # 查詢資料庫中所有錢包的地址
        wallet_addresses = await session.execute(select(WalletSummary.address))
        wallet_addresses = [wallet_address[0] for wallet_address in wallet_addresses.fetchall()]

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
                        'price': transaction.get("price_usd", None),
                        'amount': transaction.get("token_amount", None),
                        'marketcap': transaction.get("marketcap", None),
                        'value': transaction.get("cost_usd", None),
                        'holding_percentage': transaction.get("holding_percentage", None),
                        'chain': 'TRON',
                        'realized_profit': transaction.get("realized_profit", None),
                        'transaction_type': transaction.get("event_type"),
                        'transaction_time': transaction.get("timestamp"),
                    }

                    # Example of signature (could be a placeholder or actual value)
                    signature = transaction.get("tx_hash")

                    # Save transaction to DB
                    await save_past_transaction(session, tx_data, wallet_address, signature, "TRON")
                    print(f"Transaction {transaction['tx_hash']} saved successfully.")
            else:
                print(f"No transaction history found for {wallet_address}")
    except Exception as e:
        print(f"Error processing transactions: {str(e)}")

async def process_and_save_holdings(session):
    try:
        # 查詢資料庫中所有錢包的地址
        wallet_addresses = await session.execute(select(WalletSummary.address))
        wallet_addresses = [wallet_address[0] for wallet_address in wallet_addresses.fetchall()]

        if not wallet_addresses:
            print("No wallet addresses found.")
            return

        # 遍歷每個錢包地址
        for wallet_address in wallet_addresses:
            try:
                delay = random.uniform(min_delay, max_delay)
                holdings = get_wallet_holdings(wallet_address)
                await asyncio.sleep(delay)

                if not holdings:
                    print(f"No holdings found for {wallet_address}")
                    continue

                tx_data_list = []
                
                for holding in holdings:
                    try:
                        if not isinstance(holding, dict):
                            print(f"Invalid holding data type for {wallet_address}: {type(holding)}")
                            continue

                        token_data = holding.get("token", {})
                        
                        # 處理數值型數據，確保為 float 或 None
                        amount = holding.get('balance')
                        amount = float(amount) if amount is not None else None
                        
                        value = holding.get('usd_value')
                        value = float(value) if value is not None else None
                        
                        marketcap = holding.get('marketcap')
                        marketcap = float(marketcap) if marketcap is not None else None
                        
                        unrealized_profit = holding.get('unrealized_profit')
                        unrealized_profit = float(unrealized_profit) if unrealized_profit is not None else None
                        
                        # 其他數值欄位統一轉換為 float
                        tx_data = {
                            'wallet_address': wallet_address,
                            'token_address': token_data.get('address'),
                            'token_icon': token_data.get('logo'),
                            'token_name': token_data.get('symbol'),
                            'chain': holding.get('chain', 'TRON'),
                            'amount': amount,
                            'value': value,
                            'value_USDT': value,  # 與 value 相同
                            'unrealized_profit': unrealized_profit,
                            'pnl': float(holding.get('realized_pnl', 0)),
                            'pnl_percentage': float(holding.get('realized_pnl', 0)),
                            'marketcap': marketcap,
                            'sell_amount': float(holding.get('sell_30d', 50)),
                            'buy_amount': float(holding.get('buy_30d', 100)),
                            'cost': float(holding.get('history_bought_cost', 0)),
                            'profit': float(holding.get('realized_profit', 0)),
                            'last_transaction_time': holding.get('last_active_timestamp'),
                            'time': datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)  # 去除時區信息
                        }

                        # 驗證必要的數據
                        if not tx_data['token_address']:
                            print(f"Missing token address for holding in wallet {wallet_address}")
                            continue

                        tx_data_list.append(tx_data)

                    except (ValueError, TypeError) as e:
                        print(f"Error processing holding in wallet {wallet_address}: {str(e)}")
                        continue

                # 批量保存
                if tx_data_list:
                    await save_holding(tx_data_list, wallet_address, session, "TRON")
                    print(f"Successfully saved {len(tx_data_list)} holdings for wallet {wallet_address}")
                else:
                    print(f"No valid holdings to save for wallet {wallet_address}")

            except Exception as e:
                print(f"Error processing wallet {wallet_address}: {str(e)}")
                continue

    except Exception as e:
        print(f"Fatal error in process_and_save_holdings: {str(e)}")
        raise

async def process_wallet_data(session, wallets):
    """
    将钱包数据处理并保存到数据库中
    """
    # 处理并保存钱包数据
    await process_and_write_wallets(session, wallets)
    
    # 并行执行保存交易和持仓数据
    await asyncio.gather(
        process_and_save_transactions(session),
        process_and_save_holdings(session)
    )

async def main():
    # 定义目标URL
    payload = {
        'api_key': 'b4d21e175c03905dafbca1dc561bcc44',
        'url': 'https://gmgn.ai/defi/quotation/v1/rank/tron/wallets/7d?tag=smart_degen&tag=pump_smart&orderby=txs&direction=desc',
        'autoparse': 'true'
    }

    # 发送GET请求
    response = requests.get('https://api.scraperapi.com/', params=payload)

    # 确保请求成功
    if response.status_code == 200:
        # 解析JSON数据
        wallets = response.json().get("data", {}).get("rank", [])

        # 获取数据库连接
        session_factory = sessions.get("TRON".upper())

        async with session_factory() as session:
            # 处理钱包数据并保存
            await process_wallet_data(session, wallets)
    else:
        print(f"请求失败，状态码: {response.status_code}")

def start_scheduler():
    scheduler = AsyncIOScheduler()
    scheduler.start()
    print("Scheduler started.")  # 确认调度器启动

    # 立即執行一次 main()
    asyncio.create_task(main())

    # 每週執行一次
    scheduler.add_job(main, 'interval', weeks=1)

    # 監聽任務執行狀態
    def job_listener(event):
        if event.exception:
            print(f"Job {event.job_id} failed: {event.exception}")
        else:
            print(f"Job {event.job_id} executed successfully.")

    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

async def run():
    start_scheduler()
    print("Scheduler initialized.")
    while True:
        await asyncio.sleep(3600)  # 每小时检查调度器状态

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except RuntimeError as e:
        print(f"Runtime error: {e}")