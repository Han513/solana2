# import os
# import json
# import logging
# import requests
# import traceback
# from dotenv import load_dotenv
# from decimal import Decimal
# from quart import Quart, jsonify, request
# from asyncio import create_task
# from solana.rpc.async_api import AsyncClient
# from models import *
# from WalletAnalysis import *
# from token_info import TokenUtils
# from is_valid_address import *
# from functools import lru_cache
# from cachetools import TTLCache
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
# from cache import RedisCache, generate_cache_key

import os
import json
import logging
import requests
import traceback
from dotenv import load_dotenv
from decimal import Decimal
from quart import Quart, jsonify, request
from asyncio import create_task
from solana.rpc.async_api import AsyncClient
import asyncio
from database import (
    Transaction, WalletSummary, ErrorLog,
    sessions, get_utc8_time
)
from models import (
    query_all_wallets,
    get_transactions_by_params,
    query_wallet_holdings,
    get_token_trend_data,
    get_token_trend_data_allchain
)
from WalletAnalysis import *
from token_info import TokenUtils
from is_valid_address import *
from functools import lru_cache
from cachetools import TTLCache
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from cache import RedisCache, generate_cache_key

app = Quart(__name__)
# cache_warmer = CacheWarmer(sessions)
load_dotenv()
Helius_API = os.getenv("Helius_API")
HELIUS_SMARTMONEY_WEBHOOK_ID = os.getenv("HELIUS_SMARTMONEY_WEBHOOK_ID")
memory_cache = TTLCache(maxsize=1000, ttl=300)

cache_service = RedisCache()

@app.before_serving
async def startup():
    # 啟動緩存服務
    cache_service.start()
    
    # 首次更新過濾列表
    await cache_service.get_filtered_token_list()
    
    # 啟動過濾列表更新任務
    asyncio.create_task(cache_service.update_filtered_token_list())
    
@app.after_serving
async def shutdown():
    cache_service.stop()

@app.route('/robots/smartmoney/allwallet', methods=['GET'])
async def get_wallets():
    """
    優化後的跨資料庫錢包查詢 API
    直接返回已轉換為字典格式的數據
    """
    try:
        wallets = await query_all_wallets(sessions)
        return jsonify({"code": 200, "data": wallets}), 200

    except Exception as e:
        logging.error(f"API 服務器錯誤: {e}")
        return jsonify({"code": 500, "message": f"服務器錯誤: {str(e)}"}), 500

# @app.route('/robots/smartmoney/event', methods=['POST'])
# async def get_transactions():
#     """
#     改進的交易查詢 API，根據提供的參數動態篩選資料。
#     """
#     try:
#         # 解析請求資料
#         request_data = await request.get_json()
#         wallet_addresses = request_data.get('wallet_address', [])
#         token_address = request_data.get('token_address')
#         chain = request_data.get('chain')
#         name = request_data.get('token_name')
#         query = request_data.get('query')
#         fetch_all = request_data.get('fetch_all', False)
#         transaction_type = request_data.get('transaction_type')
#         filter_token_address = request_data.get('filter_token_address')
        
#         # 必須包含 chain
#         if not chain:
#             return jsonify({"code": 400, "message": "缺少必需的參數 'chain'"}), 400

#         session_factory = sessions.get(chain.upper())
#         if not session_factory:
#             return jsonify({"code": 400, "message": f"無效的鏈類型: {chain}"}), 400

#         async with session_factory() as session:
#             # 調用查詢函數，傳入動態參數
#             transactions = await get_transactions_by_params(
#                 session=session,
#                 chain=chain,
#                 wallet_addresses=wallet_addresses,
#                 token_address=token_address,
#                 name=name,
#                 query_string=query,
#                 fetch_all=fetch_all,
#                 transaction_type=transaction_type,
#                 filter_token_address=filter_token_address,
#             )

#         # 格式化查詢結果
#         data = [
#             {
#                 "wallet_address": tx["transaction"].wallet_address,
#                 "signature": tx["transaction"].signature,
#                 "token_address": tx["transaction"].token_address,
#                 "token_icon": tx["transaction"].token_icon,
#                 "token_name": tx["transaction"].token_name,
#                 "price": float(Decimal(tx["transaction"].price or 0).quantize(Decimal('0.000000'))),
#                 "amount": float(Decimal(tx["transaction"].amount or 0).quantize(Decimal('0.000000'))),
#                 "marketcap": float(Decimal(tx["transaction"].marketcap or 0).quantize(Decimal('0.000000'))),
#                 "value": float(Decimal(tx["transaction"].value or 0).quantize(Decimal('0.000000'))),
#                 "holding_percentage": float(Decimal(tx["transaction"].holding_percentage or 0).quantize(Decimal('0.000000'))),
#                 "chain": tx["transaction"].chain,
#                 "realized_profit": float(Decimal(tx["transaction"].realized_profit or 0).quantize(Decimal('0.000000'))),
#                 "realized_profit_percentage": float(Decimal(tx["transaction"].realized_profit_percentage or 0).quantize(Decimal('0.000000'))),
#                 "transaction_type": tx["transaction"].transaction_type,
#                 "wallet_count_last_hour": tx["wallet_count_last_hour"],
#                 "buy_count_last_hour": tx["buy_count_last_hour"],
#                 "sell_count_last_hour": tx["sell_count_last_hour"],
#                 "transaction_time": tx["transaction"].transaction_time,
#                 "time": tx["transaction"].time,
#             }
#             for tx in transactions
#         ]

#         return jsonify({"code": 200, "data": data}), 200

#     except Exception as e:
#         logging.error(f"API 伺服器錯誤: {e}")
#         return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500

# @app.route('/robots/smartmoney/event', methods=['POST'])
# async def get_transactions():
#     """
#     改進的交易查詢 API，根據提供的參數動態篩選資料。
#     預設只返回最近一小時的數據，除非指定 fetch_all=True。
#     """
#     try:
#         # 解析請求資料
#         request_data = await request.get_json()
#         wallet_addresses = request_data.get('wallet_address', [])
#         token_address = request_data.get('token_address')
#         chain = request_data.get('chain')
#         name = request_data.get('token_name')
#         query = request_data.get('query')
#         fetch_all = request_data.get('fetch_all', False)
#         transaction_type = request_data.get('transaction_type')
#         filter_token_address = request_data.get('filter_token_address')
        
#         # 驗證必要參數
#         if not chain:
#             return jsonify({"code": 400, "message": "缺少必需的參數 'chain'"}), 400
        
#         if not isinstance(wallet_addresses, list):
#             return jsonify({"code": 400, "message": "缺少必需的參數 'wallet_addresses' 或其格式錯誤"}), 400


#         session_factory = sessions.get(chain.upper())
#         if not session_factory:
#             return jsonify({"code": 400, "message": f"無效的鏈類型: {chain}"}), 400

#         use_cache = (
#             not fetch_all and
#             not wallet_addresses and
#             not token_address and
#             not name and
#             not query and
#             not transaction_type and
#             not filter_token_address
#         )

#         if use_cache:
#             cached_data = await cache_service.get_cached_data(chain)
#             if cached_data:
#                 print(cached_data)
#                 return jsonify({"code": 200, "data": cached_data}), 200

#         # 執行查詢
#         async with session_factory() as session:
#             transactions = await get_transactions_by_params(
#                 session=session,
#                 chain=chain,
#                 wallet_addresses=wallet_addresses,
#                 token_address=token_address,
#                 name=name,
#                 query_string=query,
#                 fetch_all=fetch_all,
#                 transaction_type=transaction_type,
#                 filter_token_address=filter_token_address,
#             )

#         # 格式化查詢結果
#         data = [
#             {
#                 "wallet_address": tx["transaction"].wallet_address,
#                 "signature": tx["transaction"].signature,
#                 "token_address": tx["transaction"].token_address,
#                 "token_icon": tx["transaction"].token_icon,
#                 "token_name": tx["transaction"].token_name,
#                 "price": float(Decimal(tx["transaction"].price or 0).quantize(Decimal('0.000000'))),
#                 "amount": float(Decimal(tx["transaction"].amount or 0).quantize(Decimal('0.000000'))),
#                 "marketcap": float(Decimal(tx["transaction"].marketcap or 0).quantize(Decimal('0.000000'))),
#                 "value": float(Decimal(tx["transaction"].value or 0).quantize(Decimal('0.000000'))),
#                 "holding_percentage": float(Decimal(tx["transaction"].holding_percentage or 0).quantize(Decimal('0.000000'))),
#                 "chain": tx["transaction"].chain,
#                 "realized_profit": float(Decimal(tx["transaction"].realized_profit or 0).quantize(Decimal('0.000000'))),
#                 "realized_profit_percentage": float(Decimal(tx["transaction"].realized_profit_percentage or 0).quantize(Decimal('0.000000'))),
#                 "transaction_type": tx["transaction"].transaction_type,
#                 "wallet_count_last_hour": tx["wallet_count_last_hour"],
#                 "buy_count_last_hour": tx["buy_count_last_hour"],
#                 "sell_count_last_hour": tx["sell_count_last_hour"],
#                 "transaction_time": tx["transaction"].transaction_time,
#                 "time": tx["transaction"].time,
#             }
#             for tx in transactions
#         ]

#         return jsonify({"code": 200, "data": data}), 200

#     except Exception as e:
#         logging.error(f"API 伺服器錯誤: {e}")
#         return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500

@app.route('/robots/smartmoney/event', methods=['POST'])
async def get_transactions():
    """
    改進的交易查詢 API，自動處理代幣過濾
    """
    try:
        # 解析請求資料
        request_data = await request.get_json()
        wallet_addresses = request_data.get('wallet_address', [])
        token_address = request_data.get('token_address')
        chain = request_data.get('chain')
        name = request_data.get('token_name')
        query = request_data.get('query')
        fetch_all = request_data.get('fetch_all', False)
        transaction_type = request_data.get('transaction_type')
        page = request_data.get('page', 1)
        page_size = request_data.get('page_size', 30)
        
        # 驗證必要參數
        if not chain:
            return jsonify({"code": 400, "message": "缺少必需的參數 'chain'"}), 400

        session_factory = sessions.get(chain.upper())
        if not session_factory:
            return jsonify({"code": 400, "message": f"無效的鏈類型: {chain}"}), 400

        # 構建緩存 key
        cache_key = generate_cache_key(
            "transactions",
            chain=chain,
            wallet_addresses=",".join(sorted(wallet_addresses)) if wallet_addresses else "all",
            token_address=token_address or "all",
            name=name or "all",
            query=query or "all",
            transaction_type=transaction_type or "all",
            page=page,
            page_size=page_size
        )

        # 嘗試從緩存獲取數據
        if not fetch_all:
            cached_data = await cache_service.get_json(cache_key)

            
            if cached_data and cached_data.get("data"):  # 檢查 cached_data 是否為 None 且有 "data" 欄位
                # 過濾緩存的數據
                filtered_data = await cache_service.filter_transactions(cached_data['data'])
                cached_data['data'] = filtered_data
                return jsonify({"code": 200, **cached_data}), 200

        # 執行查詢
        async with session_factory() as session:
            result = await get_transactions_by_params(
                session=session,
                chain=chain,
                wallet_addresses=wallet_addresses,
                token_address=token_address,
                name=name,
                query_string=query,
                fetch_all=fetch_all,
                transaction_type=transaction_type,
                page=page,
                page_size=page_size
            )

            # 格式化查詢結果
            formatted_data = []
            for tx in result["transactions"]:
                formatted_tx = {
                    "wallet_address": tx["transaction"].wallet_address,
                    "signature": tx["transaction"].signature,
                    "token_address": tx["transaction"].token_address,
                    "token_icon": tx["transaction"].token_icon,
                    "token_name": tx["transaction"].token_name,
                    "price": float(Decimal(tx["transaction"].price or 0).quantize(Decimal('0.000000'))),
                    "amount": float(Decimal(tx["transaction"].amount or 0).quantize(Decimal('0.000000'))),
                    "marketcap": float(Decimal(tx["transaction"].marketcap or 0).quantize(Decimal('0.000000'))),
                    "value": float(Decimal(tx["transaction"].value or 0).quantize(Decimal('0.000000'))),
                    "holding_percentage": float(Decimal(tx["transaction"].holding_percentage or 0).quantize(Decimal('0.000000'))),
                    "chain": tx["transaction"].chain,
                    "realized_profit": float(Decimal(tx["transaction"].realized_profit or 0).quantize(Decimal('0.000000'))),
                    "realized_profit_percentage": float(Decimal(tx["transaction"].realized_profit_percentage or 0).quantize(Decimal('0.000000'))),
                    "transaction_type": tx["transaction"].transaction_type,
                    "wallet_count_last_hour": tx["wallet_count_last_hour"],
                    "buy_count_last_hour": tx["buy_count_last_hour"],
                    "sell_count_last_hour": tx["sell_count_last_hour"],
                    "transaction_time": tx["transaction"].transaction_time,
                    "time": tx["transaction"].time,
                }
                formatted_data.append(formatted_tx)

            # 過濾數據
            filtered_data = await cache_service.filter_transactions(formatted_data)

            response_data = {
                "data": filtered_data,
                "pagination": {
                    "total": result["total"],
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (result["total"] + page_size - 1) // page_size
                }
            }

            # 如果不是 fetch_all，則緩存結果
            if not fetch_all:
                await cache_service.set_json(cache_key, response_data, expire=300)  # 5分鐘緩存

            return jsonify({"code": 200, **response_data}), 200

    except Exception as e:
        logging.error(f"API 伺服器錯誤: {e}")
        return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500

@app.route('/robots/smartmoney/position', methods=['GET'])
async def get_wallet_holding():
    """
    API 端點：根據錢包地址查詢錢包持倉數據
    """
    try:
        # 獲取查詢參數
        wallet_address = request.args.get('wallet_address')
        chain = request.args.get('chain')
        pnl_sort = request.args.get('pnl', type=str)

        if not wallet_address or not chain:
            return jsonify({"code": 400, "message": "缺少必需的參數 'wallet_address' 或 'chain'"}), 400
        
        if pnl_sort is None:
            return jsonify({"code": 400, "message": "缺少必需的參數 'pnl'"}), 400

        pnl_sort = pnl_sort.lower() == 'true'

        # 查詢資料庫
        session_factory = sessions.get(chain.upper())
        if not session_factory:
            return jsonify({"code": 400, "message": f"無效的鏈類型: {chain}"}), 400
        
        holdings = await query_wallet_holdings(session_factory, wallet_address, chain)

        if not holdings:
            return jsonify({"code": 200, "data": [], "message": "未找到符合數據"}), 200

        # 整理返回數據
        data = []
        for holding in holdings:
            current_holding_value = holding.amount * holding.value  # 当前持仓价值
            pnl_percentage = (
                (holding.pnl / holding.cumulative_cost) * 100
                if holding.cumulative_cost > 0
                else 0
            )  # PnL 百分比
            
            # 整理单个代币的数据
            holding_data = {
                "token_address": holding.token_address,
                "token_icon": holding.token_icon,
                "token_name": holding.token_name,
                "chain": holding.chain,
                "amount": float(Decimal(holding.amount or 0).quantize(Decimal('0.0000000000'))),  # 当前持仓量 
                "current_holding_value": float(Decimal(current_holding_value or 0).quantize(Decimal('0.0000000000'))),  # 当前持仓价值
                "total_cost": float(Decimal(holding.cumulative_cost or 0).quantize(Decimal('0.0000000000'))),  # 总买入成本
                "sell_value": float(Decimal(holding.cumulative_profit or 0).quantize(Decimal('0.0000000000'))),  # 卖出总价值（已实现利润）
                "pnl": float(Decimal(holding.pnl or 0).quantize(Decimal('0.0000000000'))),  # 总 PnL
                "pnl_percentage": float(Decimal(holding.pnl_percentage or 0).quantize(Decimal('0.0000000000'))),  # PnL 百分比
                "avgPrice": Decimal(holding.avg_price or 0).quantize(Decimal('0.0000000000')),
                "average_marketcap": float(Decimal(holding.marketcap or 0).quantize(Decimal('0.0000000000'))),  # 平均买入市值
                "last_transaction_time": holding.last_transaction_time,  # 平均买入市值
                # "time": holding.time.isoformat(),  # 转换为 ISO 时间格式
            }
            data.append(holding_data)

        # 根据 pnl_sort 参数进行排序
        if pnl_sort:
            # 如果 pnl_sort 为 True, 按 PnL 从高到低排序
            data.sort(key=lambda x: x['pnl'], reverse=True)
        else:
            # 如果 pnl_sort 为 False, 按 PnL 从低到高排序
            data.sort(key=lambda x: x['pnl'])

        # 返回數據
        return jsonify({"code": 200, "data": data}), 200

    except Exception as e:
        logging.error(f"API 錯誤: {e}")
        return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500

@app.route('/robots/smartmoney/tokentrend', methods=['POST'])
async def get_token_trend():
    """
    API 端點：根據代幣地址列表和區塊鏈類型查詢代幣趨勢數據
    """
    try:
        # 獲取 JSON 請求體
        request_data = await request.get_json()

        # 提取參數
        token_addresses = request_data.get('token_addresses')
        chain = request_data.get('chain')
        time_range = request_data.get('time', 60)  # 默認為60分鐘（即一小時）

        # 檢查必需參數
        if not token_addresses or not chain:
            return jsonify({"code": 400, "message": "缺少必需的參數 'token_addresses' 或 'chain'"}), 400

        # 確保 token_addresses 是一個列表
        if not isinstance(token_addresses, list):
            return jsonify({"code": 400, "message": "'token_addresses' 必須是有效的列表"}), 400

        # 查詢資料庫
        session_factory = sessions.get(chain.upper())
        async with session_factory() as session:
            trend_data = await get_token_trend_data(session, token_addresses, chain, time_range)

        # 如果未找到數據
        if not trend_data:
            return jsonify({"code": 200, "data": [], "message": "未找到符合數據"}), 200

        # 返回數據
        return jsonify({"code": 200, "data": trend_data}), 200

    except Exception as e:
        # 錯誤處理
        logging.error(f"API 錯誤: {e}")
        return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500
    
@app.route('/robots/smartmoney/tokentrend_allchain', methods=['POST'])
async def get_token_trend_allchain():
    """
    API 端點：根據代幣地址列表和多個區塊鏈類型查詢代幣趨勢數據
    """
    try:
        # 獲取 JSON 請求體
        request_data = await request.get_json()

        # 提取參數
        token_addresses = request_data.get('token_addresses')
        chains = request_data.get('chain')  # 改為接收多個chain
        time_range = request_data.get('time', 60)  # 默認為60分鐘（即一小時）

        # 檢查必需參數
        if not token_addresses or not chains:
            return jsonify({"code": 400, "message": "缺少必需的參數 'token_addresses' 或 'chains'"}), 400

        # 確保 token_addresses 是一個列表
        if not isinstance(token_addresses, list):
            return jsonify({"code": 400, "message": "'token_addresses' 必須是有效的列表"}), 400

        # 確保 chains 是一個列表
        if not isinstance(chains, list):
            return jsonify({"code": 400, "message": "'chains' 必須是有效的列表"}), 400

        all_trend_data = []
        
        # 對每個chain進行查詢
        for chain in chains:
            # 檢查該chain是否有對應的session
            session_factory = sessions.get(chain.upper())
            if not session_factory:
                logging.warning(f"找不到chain {chain}的session配置")
                continue

            # 查詢該chain的資料
            async with session_factory() as session:
                chain_trend_data = await get_token_trend_data_allchain(session, token_addresses, chain, time_range)
                if chain_trend_data:
                    all_trend_data.extend(chain_trend_data)

        # 如果所有chain都未找到數據
        if not all_trend_data:
            return jsonify({"code": 200, "data": [], "message": "未找到符合數據"}), 200

        # 返回所有chain的數據
        return jsonify({"code": 200, "data": all_trend_data}), 200

    except Exception as e:
        # 錯誤處理
        logging.error(f"API 錯誤: {e}")
        return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500

@app.route('/robots/smartmoney/analyzewallet', methods=['POST'])
async def analyze_wallet():
    """
    API 端點：接收錢包地址和鏈類型，開始分析交易數據
    """
    try:
        # 從請求體中解析 JSON，獲取 `wallet_address` 和 `chain`
        data = await request.get_json()
        wallet_address = data.get('wallet_address')
        chain = data.get('chain')
        wallet_type = data.get('wallet_type') or 0

        # 檢查必需參數
        if not wallet_address:
            return jsonify({"code": 400, "message": "缺少必需的參數 'wallet_address'"}), 400
        if not chain:
            return jsonify({"code": 400, "message": "缺少必需的參數 'chain'"}), 400

        # 檢查鏈類型是否支持
        supported_chains = ["SOLANA", "ETH", "BSC", "BASE", "TRON"]
        if chain.upper() not in supported_chains:
            return jsonify({"code": 400, "message": f"鏈類型 '{chain}' 不受支持。支持的鏈為: {supported_chains}"}), 400

        # 啟動異步背景任務
        if chain.upper() == "SOLANA":
            valid_address = is_existing_solana_address(wallet_address)
            if valid_address:
                create_task(background_analyze_GMGN_and_save(wallet_address, chain.upper(), True, wallet_type))
            else:
                return jsonify({"code": 400, "message": f"{wallet_address} is not a valid address"}), 400
        else:
            return jsonify({"code": 200, "message": "Analysis functions have not yet been developed"}), 200
        return jsonify({"code": 200, "message": f"{wallet_address} analysis on {chain} in progress..."}), 200
        
    except Exception as e:
        logging.error(f"API 錯誤: {e}")
        return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500
    
@app.route('/robots/smartmoney/verifyaddress', methods=['POST'])
async def verify_address():
    """
    API 端點：驗證 Solana 地址是否合法
    請求必須包含 chain 和 wallet_address
    """
    try:
        # 獲取請求中的 JSON 數據
        request_data = await request.get_json()

        # 獲取 chain 和 wallet_address 參數
        chain = request_data.get('chain')
        wallet_address = request_data.get('wallet_address')

        # 驗證參數是否存在
        if not chain or not wallet_address:
            return jsonify({"code": 400, "message": "缺少必需的參數 'chain' 或 'wallet_address'"}), 400

        # 僅支持 Solana 鏈的地址驗證
        if chain != "SOLANA":
            return jsonify({"code": 400, "message": "目前僅支持驗證 Solana 鏈地址"}), 400

        # 驗證地址是否合法
        is_valid = is_existing_solana_address(wallet_address)

        if is_valid:
            return jsonify({"code": 200, "wallet_address": wallet_address, "message": "valid"}), 200
        else:
            return jsonify({"code": 400, "wallet_address": wallet_address, "message": "invalid"}), 400

    except Exception as e:
        logging.error(f"驗證地址時發生錯誤: {e}")
        return jsonify({"code": 500, "message": f"伺服器錯誤: {str(e)}"}), 500

async def background_analyze_GMGN_and_save(wallet_address, chain, is_smart_wallet, wallet_type):
    """
    背景執行錢包交易分析與保存
    """
    try:
        # 根據鏈類型選擇正確的資料庫 session
        session = sessions[chain]
        async with session() as db_session:
            # 分析交易數據並保存到資料庫
            await fetch_transactions_within_30_days(db_session, wallet_address, chain, is_smart_wallet, wallet_type)
        logging.info(f"分析完成並保存交易記錄：{wallet_address} on {chain}")
    except Exception as e:
        # 打印完整的異常訊息和堆疊
        error_message = f"分析或保存交易數據時出錯：{e}"
        error_traceback = traceback.format_exc()
        logging.error(f"{error_message}\n詳細錯誤堆疊：\n{error_traceback}")
        logging.error(f"錢包地址：{wallet_address}，鏈類型：{chain}")

async def background_analyze_and_save(wallet_address, chain, is_smart_wallet, wallet_type):
    """
    背景執行錢包交易分析與保存
    """
    try:
        # 根據鏈類型選擇正確的資料庫 session
        session = sessions[chain]
        async with session() as db_session:
            # 分析交易數據並保存到資料庫
            await fetch_transactions_within_30_days_for_smartmoney(db_session, wallet_address, chain, is_smart_wallet, wallet_type)
        logging.info(f"分析完成並保存交易記錄：{wallet_address} on {chain}")
    except Exception as e:
        # 打印完整的異常訊息和堆疊
        error_message = f"分析或保存交易數據時出錯：{e}"
        error_traceback = traceback.format_exc()
        logging.error(f"{error_message}\n詳細錯誤堆疊：\n{error_traceback}")
        logging.error(f"錢包地址：{wallet_address}，鏈類型：{chain}")

# ---------------------------------------------------Helius Webhook----------------------------------------------------------------
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

def register_webhook():
    """
    註冊 Webhook 到 Helius 服務，監聽指定的地址和事件類型
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {Helius_API}"
    }
    # 僅監聽 SWAP（買入/賣出代幣）事件
    data = {
        # "webhookURL": "https://web3-webhook.bydtms.com/webhook-endpoint",
        "webhookURL": "https://moonx.bydfi.com/webhook-endpoint",
        "transactionTypes": ["SWAP"],  # 替換為 Helius 文檔中 SWAP 對應的類型名稱
        "accountAddresses": ["GLMwSLoqyy6XnP7AfWbEX7vet266NHjR97NTQawyWbpn"],
        "webhookType": "enhanced"
    }
    HELIUS_WEBHOOK_URL = f"https://api.helius.xyz/v0/webhooks?api-key={Helius_API}"
    # HELIUS_WEBHOOK_URL = f"https://api.helius.xyz/v0/webhooks"

    response = requests.post(HELIUS_WEBHOOK_URL, json=data)
    if response.status_code == 200:
        print("Webhook 註冊成功！")
    else:
        print(f"Webhook 註冊失敗: {response.status_code}, {response.text}")

# Webhook 事件處理
@app.route('/webhook-endpoint', methods=['POST'])
async def webhook():
    """
    接收並處理 Helius 推送的 Webhook 事件
    """
    try:
        # 获取请求数据
        data = await request.json
        if not isinstance(data, list):
            logging.error("Webhook 數據格式錯誤，應為列表類型")
            return jsonify({"status": "error", "message": "Invalid data format, expected a list"}), 400

        logging.info(f"收到的交易事件: {json.dumps(data, indent=2)}")

        client = await get_client()

        async with sessions["SOLANA"]() as session:
            for transaction in data:
                if transaction.get("type") == "SWAP":
                    logging.info(f"處理 SWAP 交易: {transaction.get('signature')}")

                    address = transaction.get("feePayer")
                    if not address:
                        logging.error(f"交易 {transaction.get('signature')} 缺少 feePayer 地址")
                        continue

                    # 获取 SOL/USDT 价格
                    sol_token_info = TokenUtils.get_sol_info("So11111111111111111111111111111111111111112")
                    sol_usdt_price = sol_token_info.get("priceUsd", 0)

                    # 获取钱包地址的 USD 余额
                    sol_balance = await TokenUtils.get_usd_balance(client, address)
                    wallet_balance_usdt = sol_balance.get("balance_usd", 0)

                    logging.info(f"地址 {address} 的餘額為: {wallet_balance_usdt} USD")
                    
                    result = await analyze_event_transaction(
                        transaction=transaction,
                        address=address,
                        async_session=session,
                        wallet_balance_usdt=wallet_balance_usdt,
                        sol_usdt_price=sol_usdt_price,
                        client=client,
                        chain="solana"
                    )

                    # 檢查 result 是否為 None
                    if result is None:
                        logging.error(f"交易分析失敗: {transaction.get('signature')}")
                        continue

                    # 構建 payload 前檢查必要欄位
                    payload = {
                        "network": "SOLANA",
                        "tokenAddress": result.get("token_address"),
                        "transactionType": result.get("transaction_type"),
                        "transactionTime": result.get("transaction_time"),
                        "brand": "BYD"
                    }

                    # 確保所有必要欄位都有值
                    if all(payload.values()):
                        try:
                            response = requests.post(
                                "http://172.25.183.151:4200/internal/smart_token_event", 
                                json=payload
                            )
                            
                            if response.status_code == 200:
                                logging.info(f"交易紀錄成功發送: {response.json()}")
                            else:
                                logging.error(f"交易紀錄發送失敗: {response.status_code}, {response.text}")
                        except Exception as e:
                            logging.error(f"發送請求時發生錯誤: {str(e)}")
                    else:
                        logging.error(f"payload 缺少必要欄位: {payload}")

                    logging.info(f"交易分析結果: {result}")
                    
            await session.commit()

        return jsonify({"status": "success"}), 200
    except Exception as e:
        logging.error(f"Webhook 處理錯誤：{str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/robots/smartmoney/webhook/update-addresses', methods=['POST'])
async def update_addresses():
    """
    根據前端傳遞的參數 type 來動態更新監聽的地址列表。 
    如果 type 是 "add"，則新增地址；如果 type 是 "remove"，則刪除對應地址並更新 Webhook 配置。
    """
    try:
        # 獲取請求中的 chain 參數
        data = await request.json
        chain = data.get("chain")
        address_type = data.get("type")
        addresses = data.get("address", [])

        # 檢查 chain 參數是否提供
        if not chain:
            return jsonify({"code": 400, "message": "請提供有效的 chain 參數"}), 400

        # 檢查是否支持指定的鏈
        if chain.upper() == "SOLANA":            
            async with sessions["SOLANA"]() as session:
                if not address_type or not addresses:
                    return jsonify({"code": 400, "message": "請提供有效的 type 和 address 列表"}), 400

                # 確認每個 address 是否是合法的 Solana 地址
                invalid_addresses = [addr for addr in addresses if not is_existing_solana_address(addr)]
                if invalid_addresses:
                    return jsonify({"code": 400, "message": f"無效的地址: {', '.join(invalid_addresses)}"}), 400

                # 處理 "add" 類型，新增地址
                if address_type == "add":
                    for address in addresses:
                        create_task(background_analyze_and_save(address, "SOLANA", False, 0))

                # 處理 "remove" 類型，刪除地址
                elif address_type == "remove":
                    # 如果是刪除地址，則從列表中移除該地址
                    current_addresses = await get_wallets_address_by_chain("SOLANA", session)

                    # 篩選出要刪除的地址
                    addresses_to_remove = [address for address in addresses if address in current_addresses]

                    # 檢查每個地址是否在現有地址列表中
                    if len(addresses_to_remove) != len(addresses):
                        missing_addresses = set(addresses) - set(addresses_to_remove)
                        return jsonify({"code": 400, "message": f"地址 {', '.join(missing_addresses)} 不在監聽列表中"}), 400

                    # 一次性將所有要刪除的地址的 is_active 設為 False
                    await deactivate_wallets(session, addresses_to_remove)

                else:
                    return jsonify({"code": 400, "message": "無效的 type, 應該為 'add' 或 'remove'"}), 400

                # 4. 更新 Webhook 配置的數據
                active_wallets = await get_active_or_smart_wallets(session, "SOLANA") + addresses
                webhook_url = f"https://api.helius.xyz/v0/webhooks/{HELIUS_SMARTMONEY_WEBHOOK_ID}?api-key={Helius_API}"
                headers = {"Content-Type": "application/json"}

                # 使用更新後的有效地址列表
                data = {
                    "webhookURL": "https://web3-webhook.bydtms.com/webhook-endpoint",
                    # "webhookURL": "https://moonx.bydfi.com/webhook-endpoint",
                    "transactionTypes": ["SWAP"],  # 只監聽 SWAP 交易
                    "accountAddresses": active_wallets,  # 更新後的地址列表
                    "webhookType": "enhanced"  # 設置 webhook 類型為 enhanced
                }

                # 5. 更新 Webhook 配置
                update_response = requests.put(webhook_url, json=data, headers=headers)
                if update_response.status_code == 200:
                    return jsonify({"code": 200, "message": "地址更新成功！"}), 200
                else:
                    logging.error(f"Webhook 更新失敗: {update_response.status_code}, {update_response.text}")
                    return jsonify({"code": update_response.status_code, "message": f"Webhook 更新失敗: {update_response.text}"}), update_response.status_code
        elif chain.upper() == "BSC":
            if not address_type or not addresses:
                return jsonify({"code": 400, "message": "請提供有效的 type 和 address 列表"}), 400
            
            if address_type != "add" and address_type != "remove":
                return jsonify({"code": 400, "message": "無效的 type, 應該為 'add' 或 'remove'"}), 400

            invalid_addresses = [addr for addr in addresses if not is_existing_bsc_address(addr)]
            if invalid_addresses:
                return jsonify({"code": 400, "message": f"無效的地址: {', '.join(invalid_addresses)}"}), 400
            
            url = "http://127.0.0.1:5000/robots/smartmoney/webhook/update-addresses/BSC"

            # 使用 requests 發送 POST 請求
            try:
                # 構建發送的數據
                data = {
                    "chain": "BSC",  # chain 參數為 BSC
                    "type": address_type,  # "add" 或 "remove"
                    "address": addresses  # 地址列表
                }

                # 發送請求
                response = requests.post(url, json=data)

                # 檢查請求結果
                if response.status_code == 200:
                    return jsonify({"code": 200, "message": "BSC 地址更新成功！"}), 200
                else:
                    logging.error(f"BSC 地址更新失敗: {response.status_code}, {response.text}")
                    return jsonify({"code": response.status_code, "message": f"BSC 地址更新失敗: {response.text}"}), response.status_code

            except requests.exceptions.RequestException as e:
                logging.error(f"發送請求到本地 Flask API 失敗: {e}")
                return jsonify({"code": 500, "message": f"發送請求到本地 Flask API 失敗: {str(e)}"}), 500
        else:
            return jsonify({"code": 400, "message": "Analysis functions have not yet been developed"}), 400
    except Exception as e:
        logging.error(f"更新地址失敗: {e}")
        return jsonify({"code": 500, "message": f"更新地址失敗: {str(e)}"}), 500

if __name__ == '__main__':
    # 注册 Webhook（如果需要）
    # register_webhook()

    # 创建并运行事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 启动 Quart 应用
    app.run(debug=False, host='0.0.0.0', port=5031, loop=loop)
