import base58
import re
import httpx
import logging
import json
from loguru_logger import logger
from datetime import datetime, timedelta, timezone
from solders.pubkey import Pubkey
from sqlalchemy.ext.asyncio import AsyncSession
from token_info import TokenUtils
from models import *
from solana.rpc.async_api import AsyncClient
from config import RPC_URL, RPC_URL_backup, HELIUS_API_KEY
from smart_wallet_filter import filter_smart_wallets, filter_smart_wallets_true, update_smart_wallets_filter
from WalletHolding import calculate_remaining_tokens
from collections import defaultdict
from loguru_logger import *

# 配置參數
LIMIT = 100                      # 每次請求的交易數量
DAYS_TO_ANALYZE = 30             # 查詢過去多少天的交易
token_supply_cache = {}

async def get_client():
    """
    轮流尝试RPC_URL和RPC_URL_backup，返回一个有效的客户端
    """
    # 尝试使用两个 URL，轮替
    for url in [RPC_URL, RPC_URL_backup]:
        try:
            client = AsyncClient(url)
            # 简单请求RPC服务进行健康检查
            resp = await client.is_connected()
            if resp:
                return client
            else:
                logging.warning(f"RPC连接失败，尝试下一个 URL: {url}")
        except Exception as e:
            logging.warning(f"请求RPC URL {url}时发生错误: {e}")
            continue

    logging.error("所有RPC URL都不可用")
    raise Exception("无法连接到任何 RPC URL")

async def get_token_supply(client, mint: str) -> float:
    if mint in token_supply_cache:
        return token_supply_cache[mint]

    try:
        supply_data = await client.get_token_supply(Pubkey(base58.b58decode(mint)))
        supply = int(supply_data.value.amount) / (10 ** supply_data.value.decimals)
        token_supply_cache[mint] = supply
        return supply
    except Exception:
        return 0
    
async def process_transactions_concurrently(all_transactions, address, async_session, wallet_balance_usdt, sol_usdt_price, client, token_buy_data, chain):
    # 創建異步任務列表
    wallet_transactions = defaultdict(list)
    
    # 順序處理交易，保持原子性
    for tx in all_transactions:
        if tx["type"] != "SWAP":
            continue
        
        try:
            # 在一個事務中處理每筆交易
            async with async_session.begin():
                tx_data = await analyze_swap_transaction(
                    tx, address, async_session, 
                    wallet_balance_usdt, sol_usdt_price, 
                    client, token_buy_data, chain
                )
                
                # 安全地檢查和處理交易數據
                if not tx_data:
                    continue
                
                token_address = tx_data.get("token_address", "")

                if not token_address or token_address == "So11111111111111111111111111111111111111112":
                    continue
                
                transaction_record = {
                    "buy_amount": tx_data.get("amount", 0) if tx_data.get("transaction_type") == "buy" else 0,
                    "sell_amount": tx_data.get("amount", 0) if tx_data.get("transaction_type") == "sell" else 0,
                    "cost": tx_data.get("price", 0) * tx_data.get("amount", 0) if tx_data.get("transaction_type") == "buy" else 0,
                    "profit": tx_data.get("realized_profit", 0),
                    "marketcap": tx_data.get("marketcap", 0)
                }
                
                wallet_transactions[address].append({
                    token_address: transaction_record,
                    "timestamp": tx_data["transaction_time"]
                })
        
        except Exception as e:
            print(f"處理交易失敗: {e}")
            # 如果一筆交易失敗，繼續處理下一筆
            continue
    
    return wallet_transactions
    
@async_log_execution_time
async def fetch_transactions_within_30_days(
        async_session: AsyncSession,
        address: str,
        chain: str,
        is_smart_wallet: bool = None,  # 设置为可选参数
        wallet_type: int = None,       # 设置为可选参数
        days: int = 30,
        limit: int = 100
    ):
    """
    查詢指定錢包過去30天內的所有交易數據（分頁處理），
    最後按照最舊到最新排序後逐一進行分析。
    """
    now = datetime.utcnow()
    client = await get_client()
    cutoff_timestamp = int((now - timedelta(days=days)).timestamp())
    print(f"正在查詢 {address} 錢包 {days} 天內的交易數據...")

    last_signature = None
    fetch_count = 0  # 用於計算拉取次數

    sol_token_info = TokenUtils.get_sol_info("So11111111111111111111111111111111111111112")
    sol_usdt_price = sol_token_info.get("priceUsd", 230.41)
    sol_balance = await TokenUtils.get_usd_balance(client, address)
    wallet_balance_usdt = sol_balance.get("balance_usd")

    token_buy_data = defaultdict(lambda: {"total_amount": 0, "total_cost": 0})

    # 用來存儲所有符合條件的交易數據
    all_transactions = []

    # 分頁抓取交易數據
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as async_client:
        # 分頁抓取交易數據
        while True:
            if fetch_count >= 30:  # 如果拉取次數達到30次，停止循環
                print(f"拉取次數已達到 {fetch_count} 次，停止拉取交易數據")
                break

            url = f"https://api.helius.xyz/v0/addresses/{address}/transactions?api-key={HELIUS_API_KEY}&type=SWAP&limit={limit}"
            if last_signature:
                url += f"&before={last_signature}"

            try:
                response = await async_client.get(url)
                response.raise_for_status()
            except httpx.RequestError as exc:
                print(f"HTTP 請求失敗: {exc}")
                break
            except httpx.HTTPStatusError as exc:
                print(f"HTTP 錯誤: {exc.response.status_code}, {exc.response.text}")
                break

            transactions = response.json()
            if not transactions:  # 如果没有更多数据，结束循环
                break

            # 过滤符合时间范围的交易并加入全局列表
            filtered_transactions = [tx for tx in transactions if tx["timestamp"] >= cutoff_timestamp]
            all_transactions.extend(filtered_transactions)  # 将分页数据加入总列表

            # 更新 last_signature
            last_signature = transactions[-1]["signature"] if transactions else None

            # 判断分页结束条件
            last_tx_timestamp = transactions[-1]["timestamp"]
            if last_tx_timestamp < cutoff_timestamp:
                break

            fetch_count += 1  # 拉取次数加1

    # 排序所有抓取到的交易數據（按照時間戳從最舊到最新）
    all_transactions = sorted(all_transactions, key=lambda tx: tx["timestamp"])
    print(f"已抓取並排序所有交易數據，共 {len(all_transactions)} 筆")
    await reset_wallet_buy_data(address, async_session, chain)
    # 逐一分析排序後的交易數據
    wallet_transactions = await process_transactions_concurrently(all_transactions, address, async_session, wallet_balance_usdt, sol_usdt_price, client, token_buy_data, chain)

    # with NamedTemporaryFile(delete=False, suffix=".json", mode="w", encoding="utf-8") as temp_file:
    #     json.dump(wallet_transactions, temp_file, indent=4, default=str)
    #     print(f"wallet_transactions 已保存到臨時文件: {temp_file.name}")
    is_smart_wallet = await filter_smart_wallets(wallet_transactions, sol_usdt_price, async_session, client, chain, wallet_type)
    if is_smart_wallet:
        await calculate_remaining_tokens(wallet_transactions, address, async_session, chain)
        print(f"{address} is smart wallet")
        return

async def fetch_transactions_within_30_days_for_smartmoney(
        async_session: AsyncSession,
        address: str,
        chain: str,
        is_smart_wallet: bool = None,  # 设置为可选参数
        wallet_type: int = None,       # 设置为可选参数
        days: int = 30,
        limit: int = 100
    ):
    """
    查詢指定錢包過去30天內的所有交易數據（分頁處理）
    最後按照最舊到最新排序後逐一進行分析。
    """
    now = datetime.utcnow()
    client = await get_client()
    cutoff_timestamp = int((now - timedelta(days=days)).timestamp())
    print(f"正在查詢 {address} 錢包 {days} 天內的交易數據...")

    wallet_transactions = defaultdict(list)  # 初始化全局容器，存储交易数据
    last_signature = None
    fetch_count = 0  # 用於計算拉取次數

    sol_token_info = TokenUtils.get_sol_info("So11111111111111111111111111111111111111112")
    sol_usdt_price = sol_token_info.get("priceUsd", 230.41)
    sol_balance = await TokenUtils.get_usd_balance(client, address)
    wallet_balance_usdt = sol_balance.get("balance_usd")

    token_buy_data = defaultdict(lambda: {"total_amount": 0, "total_cost": 0})

    # 用來存儲所有符合條件的交易數據
    all_transactions = []

    # 分頁抓取交易數據
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as async_client:
        # 分頁抓取交易數據
        while True:
            if fetch_count >= 30:  # 如果拉取次數達到30次，停止循環
                print(f"拉取次數已達到 {fetch_count} 次，停止拉取交易數據")
                break

            url = f"https://api.helius.xyz/v0/addresses/{address}/transactions?api-key={HELIUS_API_KEY}&type=SWAP&limit={limit}"
            if last_signature:
                url += f"&before={last_signature}"

            try:
                response = await async_client.get(url)
                response.raise_for_status()
            except httpx.RequestError as exc:
                print(f"HTTP 請求失敗: {exc}")
                break
            except httpx.HTTPStatusError as exc:
                print(f"HTTP 錯誤: {exc.response.status_code}, {exc.response.text}")
                break

            transactions = response.json()
            if not transactions:  # 如果没有更多数据，结束循环
                break

            # 过滤符合时间范围的交易并加入全局列表
            filtered_transactions = [tx for tx in transactions if tx["timestamp"] >= cutoff_timestamp]
            all_transactions.extend(filtered_transactions)  # 将分页数据加入总列表

            # 更新 last_signature
            last_signature = transactions[-1]["signature"] if transactions else None

            # 判断分页结束条件
            last_tx_timestamp = transactions[-1]["timestamp"]
            if last_tx_timestamp < cutoff_timestamp:
                break

            fetch_count += 1  # 拉取次数加1

    # 排序所有抓取到的交易數據（按照時間戳從最舊到最新）
    all_transactions = sorted(all_transactions, key=lambda tx: tx["timestamp"])
    print(f"已抓取並排序所有交易數據，共 {len(all_transactions)} 筆")
    await reset_wallet_buy_data(address, async_session, chain)
    # 逐一分析排序後的交易數據
    for tx in all_transactions:
        if tx["type"] == "SWAP":
            tx_data = await analyze_swap_transaction(
                tx, address, async_session, wallet_balance_usdt, sol_usdt_price, client, token_buy_data, chain
            )
            if tx_data:
                token_address = tx_data["token_address"]
                if not token_address or token_address == "So11111111111111111111111111111111111111112":  # 如果 token_address 是空字符串，跳过这笔交易
                    continue
                transaction_record = {
                    "buy_amount": tx_data["amount"] if tx_data["transaction_type"] == "buy" else 0,
                    "sell_amount": tx_data["amount"] if tx_data["transaction_type"] == "sell" else 0,
                    "cost": tx_data["price"] * tx_data["amount"] if tx_data["transaction_type"] == "buy" else 0,
                    "profit": tx_data["realized_profit"],
                    "marketcap": tx_data["marketcap"]
                }
                wallet_transactions[address].append({
                    token_address: transaction_record,
                    "timestamp": tx_data["transaction_time"]
                })
    # with NamedTemporaryFile(delete=False, suffix=".json", mode="w", encoding="utf-8") as temp_file:
    #     json.dump(wallet_transactions, temp_file, indent=4, default=str)
    #     print(f"wallet_transactions 已保存到臨時文件: {temp_file.name}")
    await filter_smart_wallets_true(wallet_transactions, sol_usdt_price, async_session, client, chain, is_smart_wallet, wallet_type)
    await calculate_remaining_tokens(wallet_transactions, address, async_session, chain)  
    print("Done")

async def update_smart_money_data(
    session: AsyncSession,  # 修改參數，直接接受 session
    address: str,
    chain: str,
    is_smart_wallet: bool = None,
    wallet_type: int = None,
    days: int = 30,
    limit: int = 100
):
    """
    查詢指定錢包過去30天內的所有交易數據（分頁處理），
    最後按照最舊到最新排序後逐一進行分析。
    """
    client = await get_client()
    print(f"正在查詢 {address} 錢包 {days} 天內的交易數據...")

    wallet_transactions = defaultdict(list)  # 初始化全局容器，存储交易数据
    sol_token_info = TokenUtils.get_sol_info("So11111111111111111111111111111111111111112")
    sol_usdt_price = sol_token_info.get("priceUsd", 230.41)

    try:
        # 使用 get_transactions_for_wallet 獲取交易紀錄
        all_transactions = await get_transactions_for_wallet(
            session=session, chain=chain, wallet_address=address, days=days
        )
    except Exception as e:
        print(f"獲取交易紀錄失敗: {e}")
        return

    await reset_wallet_buy_data(address, session, chain)

    # 逐一分析排序後的交易數據
    for tx_data in all_transactions:
        token_address = tx_data["token_address"]
        if not token_address or token_address == "So11111111111111111111111111111111111111112":  # 如果 token_address 是空字符串，跳過這筆交易
            continue
        transaction_record = {
            "buy_amount": tx_data["amount"] if tx_data["transaction_type"] == "buy" else 0,
            "sell_amount": tx_data["amount"] if tx_data["transaction_type"] == "sell" else 0,
            "cost": tx_data["price"] * tx_data["amount"] if tx_data["transaction_type"] == "buy" else 0,
            "profit": tx_data["realized_profit"],
            "marketcap": tx_data["marketcap"]
        }
        wallet_transactions[address].append({
            token_address: transaction_record,
            "timestamp": tx_data["transaction_time"]
        })

    is_smart_wallet = await update_smart_wallets_filter(wallet_transactions, sol_usdt_price, session, client, chain)
    await calculate_remaining_tokens(wallet_transactions, address, session, chain) 
    if is_smart_wallet:
        await activate_wallets(session, [address])
    else:
        await deactivate_wallets(session, [address])

async def analyze_swap_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client, token_buy_data, chain):
    """
    優化後的 SWAP 交易分析函數
    """
    # print(transaction)
    try:
        # 將常用的靜態數據移到函數外部作為常量
        STABLECOINS = frozenset([
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
        ])
        SOL_ADDRESS = "So11111111111111111111111111111111111111112"
        
        # 預先提取常用數據，減少字典查詢
        signature = transaction.get("signature")
        timestamp = transaction.get("timestamp")
        fee = transaction.get("fee", 0)
        
        # 使用 dict 而不是多個變量來存儲中間結果
        swap_data = {
            "sold_amount": None,
            "bought_amount": None,
            "sold_token": None,
            "bought_token": None,
            "swap_type": None,
            "value": None
        }
        
        # 快速檢查必要條件
        events = transaction.get("events")
        if not events or "swap" not in events:
            return await analyze_special_transaction(transaction, address, async_session, 
                                                  wallet_balance_usdt, sol_usdt_price, client, chain)
        
        swap_event = events["swap"]
        
        # 優化 SWAP 類型判斷邏輯
        if "tokenInputs" in swap_event and swap_event["tokenInputs"]:
            swap_data.update(_process_sell_swap(swap_event, sol_usdt_price))
        elif "tokenOutputs" in swap_event and swap_event["tokenOutputs"]:
            swap_data.update(_process_buy_swap(swap_event, sol_usdt_price))
        else:
            return None
            
        # 快速驗證交易有效性
        if _should_skip_transaction(swap_data, STABLECOINS, SOL_ADDRESS):
            return None

        # 批量處理 token 數據
        token_address = swap_data.get("sold_token") if swap_data["swap_type"] == "SELL" else swap_data.get("bought_token")
        amount = swap_data["sold_amount"] if swap_data["swap_type"] == "SELL" else swap_data["bought_amount"]
        
        if not token_address or not amount:
            return None
            
        # 優化 token 數據獲取和計算
        token_data = await _process_token_data(
            address, token_address, amount, swap_data, 
            async_session, chain, wallet_balance_usdt
        )
        
        if not token_data:
            return None

        # 構建最終結果
        result =  await _build_transaction_result(
            token_address, token_data, swap_data, address, signature, timestamp,
            chain, wallet_balance_usdt, client
        )
        await save_past_transaction(async_session, result, address, signature, chain)
        return result
        
    except Exception as e:
        print(f"分析或保存交易失敗: {e}")
        return None

def _process_sell_swap(swap_event, sol_usdt_price):
    """處理賣出類型的 SWAP"""

    result = {"swap_type": "SELL"}
    
    token_input = swap_event["tokenInputs"][0]
    result["sold_token"] = token_input["mint"]
    result["sold_amount"] = float(token_input["rawTokenAmount"]["tokenAmount"]) / 10**token_input["rawTokenAmount"]["decimals"]
    
    if "nativeOutput" in swap_event and swap_event["nativeOutput"]:
        result["bought_token"] = "So11111111111111111111111111111111111111112"
        result["bought_amount"] = float(swap_event["nativeOutput"]["amount"]) / 1e9
        result["value"] = result["bought_amount"] * sol_usdt_price
    
    return result

def _process_buy_swap(swap_event, sol_usdt_price):
    """處理買入類型的 SWAP"""

    result = {"swap_type": "BUY"}
    
    token_output = swap_event["tokenOutputs"][0]
    result["bought_token"] = token_output["mint"]
    result["bought_amount"] = float(token_output["rawTokenAmount"]["tokenAmount"]) / 10**token_output["rawTokenAmount"]["decimals"]
    
    if "nativeInput" in swap_event and swap_event["nativeInput"]:
        result["sold_token"] = "So11111111111111111111111111111111111111112"
        result["sold_amount"] = float(swap_event["nativeInput"]["amount"]) / 1e9
        result["value"] = result["sold_amount"] * sol_usdt_price
    
    return result

def _should_skip_transaction(swap_data, STABLECOINS, SOL_ADDRESS):
    """快速判斷是否需要跳過交易"""
    sold_token = swap_data["sold_token"]
    bought_token = swap_data["bought_token"]
    
    return (
        (sold_token in STABLECOINS and bought_token == SOL_ADDRESS) or
        (bought_token in STABLECOINS and sold_token == SOL_ADDRESS) or
        (bought_token != SOL_ADDRESS and sold_token != SOL_ADDRESS)
    )

class TokenBuyDataCache:
    def __init__(self, max_size=1000):
        self._cache = {}  # 緩存字典
        self._max_size = max_size  # 最大緩存大小

    async def get_token_data(self, wallet_address, token_address, session, chain):
        # 生成唯一緩存鍵
        cache_key = f"{wallet_address}_{token_address}_{chain}"
        
        # 如果已經在緩存中，直接返回
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # 從數據庫獲取數據
        token_data = await get_token_buy_data(wallet_address, token_address, session, chain)
        
        # 緩存已滿時，移除最早的條目
        if len(self._cache) >= self._max_size:
            self._cache.popitem()  
        
        # 將數據存入緩存
        self._cache[cache_key] = token_data
        return token_data

token_buy_data_cache = TokenBuyDataCache()

async def _process_token_data(address, token_address, amount, swap_data, async_session, chain, wallet_balance_usdt):
    """處理代幣相關數據"""
    token_data = await token_buy_data_cache.get_token_data(
        address, token_address, async_session, chain
    )
    
    if swap_data["swap_type"] == "SELL":
        token_data = _process_sell_token_data(token_data, amount, swap_data)
    else:
        token_data = _process_buy_token_data(token_data, amount, swap_data)
        
    # 構建要保存的交易數據
    tx_data = {
        "avg_buy_price": token_data.get("avg_buy_price", 0),
        "token_address": token_address,
        "total_amount": token_data["total_amount"],
        "total_cost": token_data["total_cost"]
    }

    # 保存到數據庫
    await save_wallet_buy_data(tx_data, address, async_session, chain)
    
    return token_data

def _process_sell_token_data(token_data, amount, swap_data):
    """處理賣出時的代幣數據"""
    if token_data["total_amount"] <= 0:
        token_data.update({
            "total_amount": 0,
            "total_cost": 0,
            "pnl": 0,
            "pnl_percentage": 0,
            "sell_percentage": 0
        })
        return token_data
        
    sell_percentage = min((amount / token_data["total_amount"]) * 100, 100)
    avg_buy_price = token_data["total_cost"] / token_data["total_amount"]
    sell_price = swap_data["value"] / amount
    total_amount = max(0, token_data["total_amount"] - amount)
    
    token_data.update({
        "pnl": (sell_price - avg_buy_price) * amount,
        "pnl_percentage": max(((sell_price / avg_buy_price) - 1) * 100, -100) if avg_buy_price > 0 else 0,
        "sell_percentage": sell_percentage,
        "total_amount": total_amount,
        "total_cost": 0 if total_amount <= amount else token_data["total_cost"]
    })
    
    return token_data

def _process_buy_token_data(token_data, amount, swap_data):
    """處理買入時的代幣數據"""
    token_data["total_amount"] += amount
    token_data["total_cost"] += swap_data["value"]
    token_data["avg_buy_price"] = token_data["total_cost"] / token_data["total_amount"]
    return token_data

async def _build_transaction_result(token_address, token_data, swap_data, address, signature, timestamp, chain, wallet_balance_usdt, client):
    """構建最終的交易結果"""
    amount = swap_data["sold_amount"] if swap_data["swap_type"] == "SELL" else swap_data["bought_amount"]
    
    # 使用緩存獲取 token 信息和供應量
    token_info = TokenUtils.get_token_info(token_address)
    supply = await get_token_supply(client, token_address) or 0
    
    price = swap_data["value"] / amount if swap_data["value"] else 0
    marketcap = round(price * supply, 2)

    return {
        "wallet_address": address,
        "token_address": token_address,
        "token_icon": token_info.get('url', ''),
        "token_name": token_info.get('symbol', ''),
        "price": price,
        "amount": amount,
        "marketcap": marketcap,
        "value": swap_data["value"],
        "holding_percentage": (min((swap_data["value"] / (swap_data["value"] + wallet_balance_usdt)) * 100, 100)
                             if swap_data["swap_type"] == "BUY" 
                             else token_data["sell_percentage"]),
        "chain": "SOLANA",
        "realized_profit": token_data.get("pnl", 0)  if swap_data["swap_type"] == "SELL" else 0,
        "realized_profit_percentage": (token_data.get("pnl_percentage", 0) 
                                     if swap_data["swap_type"] == "SELL" else 0),
        "transaction_type": swap_data["swap_type"].lower(),
        "transaction_time": timestamp,
        "time": datetime.now(timezone(timedelta(hours=8)))
    }

async def analyze_special_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client, chain):
    """
    分析一笔 SWAP 类型的交易，依据 tokenTransfers 判断是买入还是卖出，并保存到数据库。
    """
    try:
        # 提前獲取常用值，避免重複 dict lookup
        signature = transaction["signature"]
        timestamp = transaction["timestamp"]
        fee = transaction.get("fee", 0)
        token_transfers = transaction.get("tokenTransfers", [])
        
        if not token_transfers:
            return None
        
        # 初始化基礎結果
        result = {
            "signature": signature,
            "timestamp": timestamp,
            "swap_type": None,
            "sold_token": None,
            "sold_amount": None,
            "bought_token": None,
            "bought_amount": None,
            "fee": fee,
            "value": None,
            "pnl": 0,
            "pnl_percentage": 0
        }
        # 提前獲取 transfer 相關數據
        first_transfer = token_transfers[0]
        from_user = first_transfer["fromUserAccount"]
        to_user = first_transfer["toUserAccount"]
        token_amount = first_transfer["tokenAmount"]
        token_mint = first_transfer["mint"]

        # 提前判斷交易類型
        is_sell = from_user == address
        is_buy = to_user == address

        if not (is_sell or is_buy):
            return None
        # 提前獲取賬戶數據
        account_data = transaction.get("accountData", [])
        target_account = to_user if is_sell else from_user

        # 使用 dict comprehension 優化賬戶數據查找
        account_dict = {acc["account"]: acc for acc in account_data}
        target_account_data = account_dict.get(target_account)

        if target_account_data:
            native_balance_change = abs(target_account_data.get("nativeBalanceChange", 0))
            value = (native_balance_change / 1e9) * sol_usdt_price
        else:
            value = 0

        # 根據交易類型處理數據
        if is_sell:
            result.update({
                "swap_type": "SELL",
                "sold_token": token_mint,
                "sold_amount": token_amount,
                "value": value
            })
            
            token_data = await token_buy_data_cache.get_token_data(
                address, token_mint, async_session, chain
            )
            avg_buy_price = token_data.get("avg_buy_price", 0)
            sell_price = value / token_amount
            total_amount = max(0, token_data.get('total_amount', 0) - token_amount)
            
            if token_amount and avg_buy_price:
                result["pnl_percentage"] = max(((sell_price / avg_buy_price) - 1) * 100, -100) if avg_buy_price > 0 else 0
                result["pnl"] = (sell_price - avg_buy_price) * token_amount
            
            tx_data = {
                "avg_buy_price": avg_buy_price,
                "token_address": token_mint,
                "total_amount": total_amount,
                "total_cost": 0 if total_amount <= token_amount else token_data["total_cost"]
            }
            
        else:  # Buy case
            result.update({
                "swap_type": "BUY",
                "bought_token": token_mint,
                "bought_amount": token_amount,
                "value": value
            })
            
            token_data = await token_buy_data_cache.get_token_data(
                address, token_mint, async_session, chain
            )
            total_amount = token_data.get('total_amount', 0) + token_amount
            total_cost = token_data.get('total_cost', 0) + value
            
            tx_data = {
                "avg_buy_price": total_cost / total_amount if total_amount else 0,
                "token_address": token_mint,
                "total_amount": total_amount,
                "total_cost": total_cost
            }

        await save_wallet_buy_data(tx_data, address, async_session, chain)

        # 檢查是否為穩定幣交易
        STABLECOINS = frozenset([
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
        ])
        
        sol_address = "So11111111111111111111111111111111111111112"
        if ((result["sold_token"] in STABLECOINS and result["bought_token"] == sol_address) or
            (result["bought_token"] in STABLECOINS and result["sold_token"] == sol_address)):
            return None

        # 獲取 token 信息
        token_address = token_mint
        token_info = TokenUtils.get_token_info(token_address) or {"url": "", "symbol": "", "priceUsd": 0}
        
        # 計算相關數據
        amount = token_amount
        price = value / amount if amount else 0
        supply = await get_token_supply(client, token_address)
        marketcap = round(price * sol_usdt_price * (supply or 0), 2)

        # 計算持倉百分比
        holding_percentage = (
            min((value / wallet_balance_usdt) * 100, 100) if is_buy and value
            else (amount / token_data["total_amount"]) * 100 if not is_buy and token_data.get("total_amount")
            else 0
        )

        # 構建最終交易數據
        tx_data = {
            "wallet_address": address,
            "token_address": token_address,
            "token_icon": token_info.get('url', ''),
            "token_name": token_info.get('symbol', ''),
            "price": price,
            "amount": amount,
            "marketcap": marketcap,
            "value": value,
            "holding_percentage": holding_percentage,
            "chain": "SOLANA",
            "realized_profit": result.get("pnl", 0)  if is_sell else value,
            "realized_profit_percentage": result.get("pnl_percentage", 0) if is_sell else 0,
            "transaction_type": result["swap_type"].lower(),
            "transaction_time": timestamp,
            "time": datetime.now(timezone(timedelta(hours=8)))
        }

        await save_past_transaction(async_session, tx_data, address, signature, chain)
        return tx_data

    except Exception as e:
        logger.error(f"處理交易失敗: {e}", exc_info=True)
        return None
    
async def analyze_event_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client, chain):
    """
    分析一筆 SWAP 類型的交易，依據 description 判斷是否為買入或賣出，並保存到資料表。
    排除 USDC 或 USDT 與 SOL 之間的交易。
    """
    try:
        description = transaction.get("description")
        if not description:
            
            # 如果 description 为空，进入特殊分析逻辑
            tx_data = await analyze_special_transaction(transaction, address, async_session, wallet_balance_usdt, sol_usdt_price, client, chain)
            if tx_data:  # 如果成功解析并保存，则返回结果
                return tx_data
            else:
                return None

        # 穩定幣的地址
        STABLECOINS = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
        ]
        sold_amount = None
        bought_amount = None
        sold_token = None
        bought_token = None

        result = {
            "signature": transaction.get("signature"),
            "timestamp": transaction.get("timestamp"),
            "description": transaction.get("description", ""),
            "swap_type": None,
            "sold_token": None,
            "sold_amount": None,
            "bought_token": None,
            "bought_amount": None,
            "fee": transaction.get("fee", 0),
            "value": None  # 交易的價值，統一以 SOL 為單位
        }
        # 使用 description 解析交易
        if result["description"]:
            # 匹配 swapped 和 for 後的代幣與數量
            match = re.search(r"swapped ([\d\.]+) ([\w\d\$\-]+) for ([\d\.]+) ([\w\d\$\-]+)", result["description"])

            if match:
                sold_amount = float(match.group(1))
                sold_token = match.group(2)
                bought_amount = float(match.group(3))
                bought_token = match.group(4)

                # 判斷是否需要排除該交易
                if (
                    sold_token in STABLECOINS and bought_token == "SOL" and bought_token == "So11111111111111111111111111111111111111112"
                ) or (
                    bought_token in STABLECOINS and sold_token == "SOL" and sold_token == "So11111111111111111111111111111111111111112"
                ) or (
                    bought_token != "SOL" and sold_token != "SOL" and bought_token != "So11111111111111111111111111111111111111112" and sold_token != "So11111111111111111111111111111111111111112"
                ):
                    return  # 直接跳過該交易

                token_transfers = transaction.get("tokenTransfers", [])

                if token_transfers:
                    # Sold token 地址修正
                    if sold_token != token_transfers[0]["mint"]:
                        sold_token = token_transfers[0]["mint"]
                    # Bought token 地址修正
                    if len(token_transfers) > 1 and bought_token != token_transfers[1]["mint"]:
                        bought_token = token_transfers[1]["mint"]

                # 判斷是買入還是賣出
                if sold_token == "SOL" or sold_token == "So11111111111111111111111111111111111111112" or sold_token in STABLECOINS:  # SOL 或穩定幣的地址
                    result["swap_type"] = "BUY"
                    result["bought_token"] = bought_token
                    result["bought_amount"] = bought_amount
                    result["sold_token"] = sold_token
                    result["sold_amount"] = sold_amount
                    result["value"] = sold_amount * sol_usdt_price  # 使用 SOL 的數量作為價值
                    

                    token_data = await token_buy_data_cache.get_token_data(
                        address, bought_token, async_session, chain
                    )
                    total_amount = token_data.get('total_amount', 0) + bought_amount
                    total_cost = token_data.get('total_cost', 0) + result["value"]
                    avg_buy_price = total_cost / total_amount if total_amount else 0

                    tx_data = {
                        "avg_buy_price": avg_buy_price,
                        "token_address": bought_token,
                        "total_amount": total_amount,
                        "total_cost": total_cost
                    }
                    await save_wallet_buy_data(tx_data, address, async_session, chain)

                else:
                    result["swap_type"] = "SELL"
                    result["sold_token"] = sold_token
                    result["sold_amount"] = sold_amount
                    result["bought_token"] = bought_token
                    result["bought_amount"] = bought_amount
                    result["value"] = bought_amount * sol_usdt_price  # 使用 SOL 的數量作為價值

                    token_data = await token_buy_data_cache.get_token_data(
                        address, sold_token, async_session, chain
                    )

                    if token_data and token_data["total_amount"] > 0:
                        avg_buy_price = token_data["avg_buy_price"]
                        total_amount = token_data["total_amount"]
                        total_cost = token_data["total_cost"]  # 使用新的變數名稱存儲 total_cost

                        # 计算卖出占比和 PNL
                        if total_amount > 0:
                            sell_percentage = min((sold_amount / total_amount) * 100, 100)
                            sell_price = result["value"] / sold_amount
                            pnl_percentage = max(((sell_price / avg_buy_price) - 1) * 100, -100)

                            result["sell_percentage"] = sell_percentage
                            result["pnl_percentage"] = pnl_percentage

                            new_total_amount = total_amount - sold_amount
                            new_total_cost = 0 if new_total_amount <= sold_amount else total_cost
                            

                            if new_total_amount <= 0:
                                new_total_amount = 0
                                new_total_cost = 0

                            tx_data = {
                                "avg_buy_price": avg_buy_price,
                                "token_address": sold_token,
                                "total_amount": new_total_amount,
                                "total_cost": new_total_cost
                            }
                            await save_wallet_buy_data(tx_data, address, async_session, chain)
                    else:
                        result["sell_percentage"] = 0
                        result["pnl_percentage"] = 0

        # 構造數據，準備保存到資料庫
        token_info = TokenUtils.get_token_info(result["sold_token"] if result["swap_type"] == "SELL" else result["bought_token"])
        token_address = result["sold_token"] if result["swap_type"] == "SELL" else result["bought_token"]
        amount = result["sold_amount"] if result["swap_type"] == "SELL" else result["bought_amount"]
        price = result["value"] / amount if amount else 0
        supply = await get_token_supply(client, token_address)
        marketcap = round(price * sol_usdt_price * supply, 2)

        tx_data = {
            "wallet_address": address,
            "token_address": token_address,
            "token_icon": token_info.get('url', ''),
            "token_name": token_info.get('symbol', ''),
            "price": price,
            "amount": amount,
            "marketcap": marketcap,
            "value": result["value"],
            "holding_percentage": result["value"] / wallet_balance_usdt if result["swap_type"] == "BUY" else result["sell_percentage"],
            "chain": "SOLANA",  # 固定為 Solana
            "realized_profit": result["value"] if result["swap_type"] == "SELL" else 0,
            "realized_profit_percentage": result["pnl_percentage"] if result["swap_type"] == "SELL" else 0,
            "transaction_type": result["swap_type"].lower(),
            "transaction_time": result["timestamp"],
            "time": datetime.now(timezone.utc)
        }

        await save_past_transaction(async_session, tx_data, address, result["signature"], chain)
        return tx_data  # 返回交易数据
    
    except Exception as e:
        print(f"分析或保存交易失败: {e}")
        return None

# import base58
# import time
# import re
# import httpx
# import json
# from datetime import datetime, timedelta, timezone
# from functools import lru_cache
# from solders.pubkey import Pubkey
# from sqlalchemy.ext.asyncio import AsyncSession
# from token_info import TokenUtils
# from models import save_past_transaction, save_wallet_buy_data, get_token_buy_data
# from solana.rpc.async_api import AsyncClient
# from config import RPC_URL, HELIUS_API_KEY
# from smart_wallet_filter import filter_smart_wallets
# from WalletHolding import calculate_remaining_tokens
# from loguru import logger
# from collections import defaultdict

# # 常數定義
# LIMIT = 100  # 每次請求的交易數量
# DAYS_TO_ANALYZE = 30  # 查詢過去多少天的交易
# STABLECOINS = {
#     "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
#     "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
# }

# @lru_cache(maxsize=100)
# async def get_token_supply(client, mint: str) -> float:
#     """
#     查詢代幣的供應量，並進行快取。
#     """
#     try:
#         supply_data = await client.get_token_supply(Pubkey(base58.b58decode(mint)))
#         supply = int(supply_data.value.amount) / (10 ** supply_data.value.decimals)
#         return supply
#     except Exception as e:
#         logger.error(f"獲取代幣供應失敗: {e}")
#         return 0

# async def fetch_transactions_within_30_days(async_session: AsyncSession, address, days=30, limit=100):
#     """
#     查詢指定錢包過去 30 天內的所有交易數據，分頁處理並排序後逐一分析。
#     """
#     now = datetime.utcnow()
#     cutoff_timestamp = int((now - timedelta(days=days)).timestamp())
#     logger.info(f"正在查詢 {address} 錢包 {days} 天內的交易數據...")

#     async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as http_client, AsyncClient(RPC_URL) as sol_client:
#         wallet_transactions = []
#         last_signature = None
#         fetch_count = 0

#         # 獲取 SOL 價格和餘額
#         sol_token_info = TokenUtils.get_token_info("So11111111111111111111111111111111111111112")
#         sol_usdt_price = sol_token_info.get("priceUsd", 0)
#         sol_balance = await TokenUtils.get_usd_balance(sol_client, address)
#         wallet_balance_usdt = sol_balance.get("balance_usd")

#         while True:
#             if fetch_count >= 30:
#                 logger.info(f"達到最大拉取次數 {fetch_count}，停止拉取交易數據")
#                 break

#             url = f"https://api.helius.xyz/v0/addresses/{address}/transactions?api-key={HELIUS_API_KEY}&type=SWAP&limit={limit}"
#             if last_signature:
#                 url += f"&before={last_signature}"  
#             response = await http_client.get(url)
#             if response.status_code != 200:
#                 logger.error(f"API 請求失敗，錯誤碼: {response.status_code}")
#                 break

#             transactions = response.json()
#             if not transactions:
#                 break

#             filtered_transactions = [tx for tx in transactions if tx["timestamp"] >= cutoff_timestamp]
#             wallet_transactions.extend(filtered_transactions)

#             if transactions:
#                 last_signature = transactions[-1]["signature"]
#             else:
#                 break

#             last_tx_timestamp = transactions[-1]["timestamp"]
#             if last_tx_timestamp < cutoff_timestamp:
#                 break

#             fetch_count += 1

#         wallet_transactions = sorted(wallet_transactions, key=lambda tx: tx["timestamp"])
#         logger.info(f"已抓取並排序交易數據，共 {len(wallet_transactions)} 筆")

#         token_buy_data = defaultdict(lambda: {"total_amount": 0, "total_cost": 0})

#         for tx in wallet_transactions:
#             if tx["type"] == "SWAP":
#                 tx_data = await analyze_swap_transaction(tx, address, async_session, wallet_balance_usdt, sol_usdt_price, sol_client, token_buy_data)
#                 if tx_data:
#                     wallet_transactions.append(tx_data)

#         await filter_smart_wallets(wallet_transactions, sol_usdt_price, async_session, sol_client)
#         await calculate_remaining_tokens(wallet_transactions, address, async_session)
#         logger.info("處理完成")

# async def analyze_swap_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client, token_buy_data):
#     """
#     分析交易類型並保存交易數據。
#     """
#     try:
#         result = parse_transaction_description(transaction)
#         if not result:
#             return None

#         token_info = TokenUtils.get_token_info(result["sold_token"] if result["swap_type"] == "SELL" else result["bought_token"])
#         token_address = result["sold_token"] if result["swap_type"] == "SELL" else result["bought_token"]
#         amount = result["sold_amount"] if result["swap_type"] == "SELL" else result["bought_amount"]
#         price = result["value"] / amount if amount else 0
#         supply = await get_token_supply(client, token_address)
#         marketcap = round(price * sol_usdt_price * supply, 2)

#         tx_data = {
#             "wallet_address": address,
#             "token_address": token_address,
#             "token_icon": token_info.get("url", ""),
#             "token_name": token_info.get("symbol", ""),
#             "price": price,
#             "amount": amount,
#             "marketcap": marketcap,
#             "value": result["value"],
#             "holding_percentage": result["value"] / wallet_balance_usdt if result["swap_type"] == "BUY" else result["sell_percentage"],
#             "chain": "SOLANA",
#             "realized_profit": result["value"] if result["swap_type"] == "SELL" else 0,
#             "realized_profit_percentage": result["pnl_percentage"] if result["swap_type"] == "SELL" else 0,
#             "transaction_type": result["swap_type"].lower(),
#             "transaction_time": result["timestamp"],
#             "time": datetime.now(timezone(timedelta(hours=8)))
#         }

#         await save_past_transaction(async_session, tx_data, address, result["signature"])
#         return tx_data
#     except Exception as e:
#         logger.error(f"交易分析失敗: {e}")
#         return None

# def parse_transaction_description(transaction):
#     """
#     根據交易的描述解析交易數據。
#     """
#     try:
#         description = transaction.get("description", "")
#         if not description:
#             return None

#         match = re.search(r"swapped ([\d\.]+) ([\w\d\$-]+) for ([\d\.]+) ([\w\d\$-]+)", description)
#         if not match:
#             return None

#         sold_amount, sold_token, bought_amount, bought_token = map(match.group, [1, 2, 3, 4])
#         return {
#             "swap_type": "BUY" if sold_token in STABLECOINS or sold_token == "SOL" else "SELL",
#             "sold_amount": float(sold_amount),
#             "sold_token": sold_token,
#             "bought_amount": float(bought_amount),
#             "bought_token": bought_token,
#             "value": float(sold_amount) if sold_token in STABLECOINS else float(bought_amount),
#             "timestamp": transaction.get("timestamp"),
#             "signature": transaction.get("signature"),
#         }
#     except Exception as e:
#         logger.error(f"解析交易描述失敗: {e}")
#         return None

# async def analyze_event_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client):
#     """
#     分析一筆 SWAP 類型的交易，依據 description 判斷是否為買入或賣出，並保存到資料表。
#     排除 USDC 或 USDT 與 SOL 之間的交易。
#     """
#     try:
#         if not transaction.get("description"):
#             # 如果 description 为空，进入特殊分析逻辑
#             tx_data = await analyze_special_transaction(transaction, address, async_session, wallet_balance_usdt, sol_usdt_price, client)
#             if tx_data:  # 如果成功解析并保存，则返回结果
#                 return tx_data
#             else:
#                 print(f"未能解析特殊交易: {transaction.get('signature')}")
#                 return None
#         # 穩定幣的地址
#         STABLECOINS = [
#             "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
#             "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
#         ]
#         sold_amount = None
#         bought_amount = None
#         sold_token = None
#         bought_token = None

#         result = {
#             "signature": transaction.get("signature"),
#             "timestamp": transaction.get("timestamp"),
#             "description": transaction.get("description", ""),
#             "swap_type": None,
#             "sold_token": None,
#             "sold_amount": None,
#             "bought_token": None,
#             "bought_amount": None,
#             "fee": transaction.get("fee", 0),
#             "value": None  # 交易的價值，統一以 SOL 為單位
#         }

#         # 使用 description 解析交易
#         if result["description"]:
#             # 匹配 swapped 和 for 後的代幣與數量
#             match = re.search(r"swapped ([\d\.]+) ([\w\d\$\-]+) for ([\d\.]+) ([\w\d\$\-]+)", result["description"])

#             if match:
#                 sold_amount = float(match.group(1))
#                 sold_token = match.group(2)
#                 bought_amount = float(match.group(3))
#                 bought_token = match.group(4)

#                 # 判斷是否需要排除該交易
#                 if (
#                     sold_token in STABLECOINS and bought_token == "SOL" and bought_token == "So11111111111111111111111111111111111111112"
#                 ) or (
#                     bought_token in STABLECOINS and sold_token == "SOL" and sold_token == "So11111111111111111111111111111111111111112"
#                 ) or (
#                     bought_token != "SOL" and sold_token != "SOL" and bought_token != "So11111111111111111111111111111111111111112" and sold_token != "So11111111111111111111111111111111111111112"
#                 ):
#                     return  # 直接跳過該交易

#                 token_transfers = transaction.get("tokenTransfers", [])
#                 if token_transfers:
#                     # Sold token 地址修正
#                     if sold_token != token_transfers[0]["mint"]:
#                         sold_token = token_transfers[0]["mint"]
#                     # Bought token 地址修正
#                     if len(token_transfers) > 1 and bought_token != token_transfers[1]["mint"]:
#                         bought_token = token_transfers[1]["mint"]

#                 # 判斷是買入還是賣出
#                 if sold_token == "SOL" or sold_token == "So11111111111111111111111111111111111111112" or sold_token in STABLECOINS:  # SOL 或穩定幣的地址
#                     result["swap_type"] = "BUY"
#                     result["bought_token"] = bought_token
#                     result["bought_amount"] = bought_amount
#                     result["sold_token"] = sold_token
#                     result["sold_amount"] = sold_amount
#                     result["value"] = sold_amount * sol_usdt_price  # 使用 SOL 的數量作為價值

#                     token_data = await get_token_buy_data(address, bought_token, async_session)
#                     if token_data:
#                         tx_data = {
#                         "token_address": bought_token,
#                         "total_amount": token_data["total_amount"] + bought_amount,
#                         "total_cost": token_data["total_cost"] + result["value"]
#                         }
#                         await save_wallet_buy_data(tx_data, address, async_session)
#                     else:
#                         # 插入新數據
#                         tx_data = {
#                         "token_address": bought_token,
#                         "total_amount": bought_amount,
#                         "total_cost": result["value"]
#                         }
#                         await save_wallet_buy_data(tx_data, address, async_session)

#                 else:
#                     result["swap_type"] = "SELL"
#                     result["sold_token"] = sold_token
#                     result["sold_amount"] = sold_amount
#                     result["bought_token"] = bought_token
#                     result["bought_amount"] = bought_amount
#                     result["value"] = bought_amount * sol_usdt_price  # 使用 SOL 的數量作為價值

#                     token_data = await get_token_buy_data(address, sold_token, async_session)

#                     if token_data and token_data["total_amount"] > 0:
#                         avg_buy_price = token_data["avg_buy_price"]
#                         total_amount = token_data["total_amount"]

#                         # 计算卖出占比和 PNL
#                         if total_amount > 0:
#                             sell_percentage = sold_amount / total_amount
#                             sell_price = result["value"] / sold_amount
#                             pnl_percentage = ((sell_price / avg_buy_price) - 1) * 100

#                             result["sell_percentage"] = sell_percentage
#                             result["pnl_percentage"] = pnl_percentage

#                             token_data["total_amount"] -= sold_amount
#                             if token_data["total_amount"] == 0:
#                                 # 清仓时重置 total_cost
#                                 token_data["total_cost"] = 0

#                             tx_data = {
#                                 "token_address": sold_token,
#                                 "total_amount": token_data["total_amount"],
#                                 "total_cost": token_data["total_cost"]
#                             }
#                             await save_wallet_buy_data(tx_data, address, async_session)
#                     else:
#                         result["sell_percentage"] = 0
#                         result["pnl_percentage"] = 0

#         # 構造數據，準備保存到資料庫
#         token_info = TokenUtils.get_token_info(result["sold_token"] if result["swap_type"] == "SELL" else result["bought_token"])
#         token_address = result["sold_token"] if result["swap_type"] == "SELL" else result["bought_token"]
#         amount = result["sold_amount"] if result["swap_type"] == "SELL" else result["bought_amount"]
#         price = result["value"] / amount if amount else 0
#         supply = await get_token_supply(client, token_address)
#         marketcap = round(price * sol_usdt_price * supply, 2)

#         tx_data = {
#             "wallet_address": address,
#             "token_address": token_address,
#             "token_icon": token_info.get('url', ''),
#             "token_name": token_info.get('symbol', ''),
#             "price": price,
#             "amount": amount,
#             "marketcap": marketcap,
#             "value": result["value"],
#             "holding_percentage": result["value"] / wallet_balance_usdt if result["swap_type"] == "BUY" else result["sell_percentage"],
#             "chain": "SOLANA",  # 固定為 Solana
#             "realized_profit": result["value"] if result["swap_type"] == "SELL" else 0,
#             "realized_profit_percentage": result["pnl_percentage"] if result["swap_type"] == "SELL" else 0,
#             "transaction_type": result["swap_type"].lower(),
#             "transaction_time": result["timestamp"],
#             "time": datetime.now(timezone.utc)
#         }

#         await save_past_transaction(async_session, tx_data, address, result["signature"])
#         return tx_data  # 返回交易数据
    
#     except Exception as e:
#         print(f"分析或保存交易失败: {e}")
#         return None