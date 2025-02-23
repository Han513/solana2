import aiohttp
import asyncio
from datetime import datetime, timedelta
import time

API_URL = "https://api.helius.xyz/v0/addresses/{}/transactions"
LIMIT = 100

async def fetch_transactions(address, api_key, before=None, session=None):
    """
    發送單次請求以獲取交易數據。
    直接返回 API 響應，讓上層函數處理錯誤情況。
    """
    params = {"api-key": api_key, "type": "SWAP", "limit": LIMIT}
    if before:
        params["before"] = before
        print(f"使用 before 參數: {before}")  # 添加日誌

    try:
        async with session.get(API_URL.format(address), params=params) as response:
            return await response.json()
    except Exception as e:
        print(f"請求過程中發生錯誤: {str(e)}")
        return None

async def fetch_all_transactions(address, api_key, max_records=3000):
    """
    使用異步批次請求獲取所有相關交易數據。
    改進了錯誤處理和分頁邏輯。
    """
    transactions = []
    total_fetched = 0
    before_signature = None
    thirty_days_ago = datetime.now() - timedelta(days=30)
    
    async with aiohttp.ClientSession() as session:
        while total_fetched < max_records:
            try:
                print(f"\n開始新的請求，before_signature: {before_signature}")  # 添加日誌
                data = await fetch_transactions(address, api_key, before_signature, session)
                
                if data is None:  # 請求出錯
                    print("請求失敗，退出循環")
                    break
                
                # 處理 API 返回的錯誤信息
                if isinstance(data, dict) and 'error' in data:
                    error_msg = data['error']
                    print(f"收到 API 錯誤信息: {error_msg}")
                    
                    if "before` parameter set to" in error_msg:
                        # 提取新的 signature
                        import re
                        match = re.search(r'before` parameter set to ([^.]+)', error_msg)
                        if match:
                            before_signature = match.group(1)
                            print(f"提取到新的 before signature: {before_signature}")
                            continue
                    print("無法處理的錯誤信息，退出循環")
                    break
                
                # 處理正常的數據響應
                if isinstance(data, list):
                    print(f"成功獲取數據，筆數: {len(data)}")
                    
                    if not data:  # 空列表
                        print("收到空數據，退出循環")
                        break
                        
                    valid_transactions = 0
                    for txn in data:
                        timestamp = txn.get('timestamp')
                        if timestamp is None:
                            continue
                            
                        tx_time = datetime.fromtimestamp(timestamp)
                        if tx_time < thirty_days_ago:
                            print("超出時間範圍，結束查詢")
                            return transactions
                        
                        transactions.append(txn)
                        valid_transactions += 1
                    
                    print(f"本批次有效交易數: {valid_transactions}")
                    total_fetched += len(data)
                    
                    if data:
                        before_signature = data[-1].get("signature")
                        print(f"更新 before_signature: {before_signature}")
                else:
                    print(f"未預期的數據格式: {type(data)}")
                    break
                
            except Exception as e:
                print(f"處理數據時發生錯誤: {str(e)}")
                print(f"錯誤詳情：", e.__class__.__name__)
                break
    
    print(f"\n查詢完成，總共獲取交易數量: {len(transactions)}")
    return transactions

def analyze_transactions(transactions):
    """
    分析交易數據，計算勝率、盈虧、剩餘幣種數量與平均花費成本。
    """
    token_stats = {}
    transactions.reverse()  # 反轉數據，從最早交易開始分析

    for txn in transactions:
        events = txn.get("events", {})
        swap_event = events.get("swap", {})
        
        # 安全地獲取所有需要的值
        token_inputs = swap_event.get("tokenInputs", [])
        token_outputs = swap_event.get("tokenOutputs", [])
        
        # 處理 nativeInput 和 nativeOutput 可能為 None 的情況
        nativeOutput = swap_event.get("nativeOutput") or {}
        nativeInput = swap_event.get("nativeInput") or {}

        # 處理買入
        for token in token_inputs:
            raw_amount = token.get("rawTokenAmount")
            if raw_amount:
                mint = token["mint"]
                amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                if mint not in token_stats:
                    token_stats[mint] = {
                        "cost": 0, "revenue": 0, "wins": 0, "total": 0,
                        "remaining_tokens": 0, "total_cost": 0
                    }
                # 安全地獲取 amount，默認值為 0
                native_output_amount = float(nativeOutput.get("amount", 0)) / (10 ** 9)
                token_stats[mint]["cost"] += native_output_amount
                token_stats[mint]["remaining_tokens"] += amount
                token_stats[mint]["total_cost"] += amount * (token_stats[mint]["cost"] / max(token_stats[mint]["remaining_tokens"], 1))

        # 處理賣出
        for token in token_outputs:
            raw_amount = token.get("rawTokenAmount")
            if raw_amount:
                mint = token["mint"]
                amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                if mint not in token_stats:
                    token_stats[mint] = {
                        "cost": 0, "revenue": 0, "wins": 0, "total": 0,
                        "remaining_tokens": 0, "total_cost": 0
                    }
                # 安全地獲取 amount，默認值為 0
                native_input_amount = float(nativeInput.get("amount", 0)) / (10 ** 9)
                token_stats[mint]["revenue"] += native_input_amount
                token_stats[mint]["remaining_tokens"] -= amount
                token_stats[mint]["total_cost"] -= amount * (token_stats[mint]["cost"] / max(token_stats[mint]["remaining_tokens"], 1))

        # 判斷勝負
        for mint, stats in token_stats.items():
            stats["total"] += 1
            if stats["revenue"] > stats["cost"]:
                stats["wins"] += 1

    return token_stats

def calculate_win_rate_and_summary(token_stats):
    """
    計算每個幣種的勝率、總盈虧、剩餘數量與平均花費成本。
    """
    win_rate_summary = {}
    for token, stats in token_stats.items():
        win_rate = (stats["wins"] / stats["total"]) * 100 if stats["total"] > 0 else 0
        average_cost = stats["total_cost"] / max(stats["remaining_tokens"], 1) if stats["remaining_tokens"] > 0 else 0
        win_rate_summary[token] = {
            "win_rate": win_rate,
            "total_trades": stats["total"],
            "profitability": stats["revenue"] - stats["cost"],
            "remaining_tokens": stats["remaining_tokens"],
            "average_cost": average_cost
        }
    return win_rate_summary

async def main():
    address = "J9HqxT4U39B45YcXxL4B1GZzdhSrXFr2N2Lam4bXrQeB"
    api_key = "a8aa1dc1-feed-4bfa-91b5-daf2c0eedc08"

    start_time = time.time()  # 開始計時

    # 獲取所有交易數據
    transactions = await fetch_all_transactions(address, api_key)
    total_transactions = len(transactions)

    # 分析數據
    token_stats = analyze_transactions(transactions)

    # 計算勝率與概要
    win_rate_summary = calculate_win_rate_and_summary(token_stats)

    end_time = time.time()  # 結束計時
    elapsed_time = end_time - start_time

    # 輸出結果
    print(f"總分析交易筆數: {total_transactions}")
    print(f"耗時: {elapsed_time:.2f} 秒")
    for token, stats in win_rate_summary.items():
        print(f"Token: {token}, Win Rate: {stats['win_rate']}%, "
              f"Profitability: {stats['profitability']:.6f}, "
              f"Remaining: {stats['remaining_tokens']:.6f}, "
              f"Average Cost: {stats['average_cost']:.6f}")

# 啟動異步程序
asyncio.run(main())

# -------------------------------------------------------------------------------------------------------------------
# import base58
# from solana.rpc.api import Client
# from solana.rpc.core import RPCException
# from solders.pubkey import Pubkey

# def check_solana_balance(rpc_url, wallet_address):
#     try:
#         # 初始化 Solana 客戶端
#         client = Client(rpc_url)
        
#         # 獲取指定錢包地址的餘額
#         balance_response = client.get_balance(Pubkey(base58.b58decode(wallet_address)))
        
#         # 驗證返回的結果是否成功
#         if balance_response:
#             balance = {
#                 'decimals': 9,
#                 'balance': {
#                     'int': balance_response.value,
#                     'float': float(balance_response.value / 10**9)
#                 }
#             }
#             print(balance)
#         else:
#             print(f"Error fetching balance: {balance_response}")
#     except RPCException as e:
#         print(f"RPCException occurred: {e}")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     # 輸入 RPC 節點 URL
#     rpc_url = "http://18.181.110.247:64321"
    
#     # 輸入要查詢的錢包地址
#     wallet_address = "GLMwSLoqyy6XnP7AfWbEX7vet266NHjR97NTQawyWbpn"
    
#     # 執行測試
#     check_solana_balance(rpc_url, wallet_address)
# -------------------------------------------------------------------------------------------------------------------

# from web3 import Web3

# # 设置连接到 Binance Smart Chain 的节点
# w3 = Web3(Web3.HTTPProvider('https://bsc-dataseed.binance.org/'))

# def is_valid_bsc_address(address: str) -> bool:
#     try:
#         # 将地址转换为标准的 EIP-55 校验和格式
#         address = w3.to_checksum_address(address)
#     except ValueError as e:
#         print(f"地址格式不合法: {address}")
#         return False

#     # 检查地址格式是否合法
#     if w3.is_address(address):
#         print("地址格式合法")

#         # 检查地址余额
#         balance = w3.eth.get_balance(address)
#         if balance > 0:
#             print("该地址余额大于 0")
#         else:
#             print("该地址余额为 0")

#         # 检查是否为合约地址
#         code = w3.eth.get_code(address)
#         if code == b'':  # 如果没有合约代码，说明是普通账户地址
#             print("该地址是普通账户地址")
#         else:
#             print("该地址是合约地址")

#         return True
#     else:
#         print("地址格式不合法")
#         return False

# # 示例地址
# address = "0x0ce0386c91370d4d441B48a7B86d90EA814d9FB9"  # 修改为合适格式的地址
# is_valid_bsc_address(address)