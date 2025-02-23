from datetime import datetime, timezone
import json
import asyncio
import logging
import aiohttp
from redis import asyncio as aioredis
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any
from sqlalchemy import select, and_, or_, func, case
from sqlalchemy.ext.asyncio import AsyncSession
from database import (
    Transaction, WalletSummary, sessions, 
    get_utc8_time, make_naive_time
)

def generate_cache_key(prefix: str, **kwargs) -> str:
    """生成緩存鍵"""
    sorted_items = sorted(kwargs.items())
    key_parts = [str(k) + ":" + str(v) for k, v in sorted_items]
    return f"{prefix}:{':'.join(key_parts)}"

def safe_decimal_to_float(value, precision: int = 6) -> float:
    """安全地將值轉換為指定精度的浮點數，支持科學記數法"""
    try:
        if value is None:
            return 0.0
            
        if isinstance(value, str) and ('e' in value.lower() or 'E' in value):
            value = float(value)
        elif isinstance(value, (int, float)):
            value = str(value)
            
        if isinstance(value, float) and abs(value) > 1e10:
            value = f"{value:.2f}"

        decimal_value = Decimal(str(value))
        return float(decimal_value.quantize(Decimal('0.' + '0' * precision)))
    except (InvalidOperation, ValueError, TypeError, OverflowError) as e:
        logging.warning(f"轉換數值時出錯: {value}, 類型: {type(value)}, 錯誤: {repr(e)}")
        if isinstance(value, (int, float)):
            try:
                return round(float(value), precision)
            except Exception as e2:
                logging.warning(f"備用轉換也失敗: {e2}")
                return 0.0
        return 0.0

class RedisCache:
    def __init__(self, redis_url: str = "redis://localhost"):
        self.redis = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self.update_lock = asyncio.Lock()
        self._stop_flag = False
        self._update_task = None

    def start(self):
        """啟動緩存更新任務"""
        if self._update_task is None:
            self._stop_flag = False
            self._update_task = asyncio.create_task(self._update_loop())
            logging.info("緩存更新任務已啟動")

    def stop(self):
        """停止緩存更新任務"""
        self._stop_flag = True
        if self._update_task:
            self._update_task.cancel()
            self._update_task = None
            logging.info("緩存更新任務已停止")

    async def _update_loop(self):
        """定期更新緩存的循環"""
        while not self._stop_flag:
            try:
                await self._update_cache()
                await asyncio.sleep(120)  # 每2分鐘更新一次
            except Exception as e:
                logging.error(f"緩存更新錯誤: {e}")
                await asyncio.sleep(10)  # 發生錯誤時等待10秒後重試

    async def get_cached_transactions(self, cache_key: str) -> Optional[Dict]:
        """
        獲取緩存的交易數據，支持分頁
        """
        try:
            data = await self.get_json(cache_key)
            if not data:
                return None
            
            # 檢查緩存是否過期（3分鐘）
            last_update_key = f"{cache_key}:last_update"
            last_update = await self.get(last_update_key)
            
            if last_update:
                now = int(datetime.now(timezone.utc).timestamp())
                if now - int(last_update) > 180:  # 3分鐘
                    return None
            
            return data
                
        except Exception as e:
            logging.error(f"清除緩存時發生錯誤: {e}")

    async def batch_update_cache(self, chain: str, transactions: List[Dict]):
        """
        批量更新緩存數據
        """
        try:
            # 按照不同的分頁大小預先緩存熱門數據
            page_sizes = [10, 20, 30, 50]
            for page_size in page_sizes:
                for page in range(1, 4):  # 預緩存前3頁
                    cache_key = generate_cache_key(
                        "transactions",
                        chain=chain,
                        wallet_addresses="all",
                        page=page,
                        page_size=page_size
                    )
                    
                    start_idx = (page - 1) * page_size
                    end_idx = start_idx + page_size
                    page_data = {
                        "data": transactions[start_idx:end_idx],
                        "pagination": {
                            "page": page,
                            "page_size": page_size,
                            "total": len(transactions),
                            "total_pages": (len(transactions) + page_size - 1) // page_size
                        }
                    }
                    await self.cache_transactions(cache_key, page_data)

        except Exception as e:
            logging.error(f"批量更新緩存時發生錯誤: {e}")

    async def _update_cache(self):
        """更新緩存數據的改進版本"""
        # from models import Transaction, WalletSummary, sessions  # 避免循環導入
        
        async with self.update_lock:
            available_chains = list(sessions.keys())
            logging.info(f"開始更新緩存，可用的鏈: {available_chains}")
            
            for chain in available_chains:
                try:
                    session_factory = sessions.get(chain.upper())
                    if not session_factory:
                        continue

                    async with session_factory() as session:
                        schema = chain.lower()
                        Transaction.with_schema(schema)
                        WalletSummary.with_schema(schema)

                        # 優化智能錢包查詢
                        smart_wallets_query = (
                            select(WalletSummary.address)
                            .where(
                                and_(
                                    WalletSummary.chain == chain,
                                    WalletSummary.is_smart_wallet == True
                                )
                            )
                        )
                        smart_wallets_result = await session.execute(smart_wallets_query)
                        smart_wallet_addresses = [row[0] for row in smart_wallets_result]

                        if not smart_wallet_addresses:
                            continue

                        # 緩存最近時間段的交易
                        time_ranges = [
                            ("1h", 3600),  # 1小時
                            ("24h", 86400),  # 24小時
                            ("7d", 604800),  # 7天
                        ]

                        for time_label, time_range in time_ranges:
                            now_timestamp = int(datetime.now(timezone.utc).timestamp())
                            start_timestamp = now_timestamp - time_range

                            query = (
                                select(Transaction)
                                .where(
                                    and_(
                                        Transaction.chain == chain,
                                        Transaction.wallet_address.in_(smart_wallet_addresses),
                                        Transaction.transaction_time >= start_timestamp
                                    )
                                )
                                .order_by(Transaction.transaction_time.desc())
                            )
                            result = await session.execute(query)
                            transactions = result.scalars().all()

                            if not transactions:
                                continue

                            # 獲取統計數據
                            stats_data = await self._get_transaction_stats(
                                session, 
                                chain, 
                                transactions, 
                                start_timestamp
                            )

                            # 準備緩存數據
                            cache_data = []
                            for tx in sorted(transactions, key=lambda x: x.transaction_time, reverse=True):
                                try:
                                    stats = stats_data.get(tx.token_address, {
                                        'wallet_count': 0,
                                        'buy_count': 0,
                                        'sell_count': 0
                                    })
                                    
                                    cache_data.append({
                                        "wallet_address": tx.wallet_address,
                                        "signature": tx.signature,
                                        "token_address": tx.token_address,
                                        "token_icon": tx.token_icon,
                                        "token_name": tx.token_name,
                                        "price": safe_decimal_to_float(tx.price),
                                        "amount": safe_decimal_to_float(tx.amount),
                                        "marketcap": safe_decimal_to_float(tx.marketcap),
                                        "value": safe_decimal_to_float(tx.value),
                                        "holding_percentage": safe_decimal_to_float(tx.holding_percentage),
                                        "chain": tx.chain,
                                        "realized_profit": safe_decimal_to_float(tx.realized_profit),
                                        "realized_profit_percentage": safe_decimal_to_float(tx.realized_profit_percentage),
                                        "transaction_type": tx.transaction_type,
                                        "wallet_count_last_hour": stats['wallet_count'],
                                        "buy_count_last_hour": stats['buy_count'],
                                        "sell_count_last_hour": stats['sell_count'],
                                        "transaction_time": tx.transaction_time,
                                        "time": tx.time.isoformat() if tx.time else None,
                                    })
                                except Exception as e:
                                    logging.error(f"處理交易記錄時出錯: {e}")
                                    continue

                            # 更新緩存
                            cache_key = f"transactions:{chain.lower()}:{time_label}"
                            await self.set_json(cache_key, cache_data)
                            await self.set(f"{cache_key}:last_update", str(now_timestamp))

                            # 對熱門數據進行分頁預緩存
                            await self.batch_update_cache(chain, cache_data)

                            logging.info(f"已更新 {chain} 鏈的 {time_label} 緩存數據，共 {len(cache_data)} 條記錄")

                except Exception as e:
                    logging.error(f"更新 {chain} 鏈的緩存時發生錯誤: {e}")
    
    async def _get_transaction_stats(
        self, 
        session: AsyncSession,
        chain: str,
        transactions: List[Transaction],
        start_timestamp: int
    ) -> Dict:
        """獲取交易統計數據"""
        try:
            token_addresses = {tx.token_address for tx in transactions}
            stats_query = (
                select(
                    Transaction.token_address,
                    func.count(func.distinct(Transaction.wallet_address)).label('wallet_count'),
                    func.sum(case((Transaction.transaction_type == 'buy', 1), else_=0)).label('buy_count'),
                    func.sum(case((Transaction.transaction_type == 'sell', 1), else_=0)).label('sell_count')
                )
                .where(
                    and_(
                        Transaction.chain == chain,
                        Transaction.transaction_time >= start_timestamp,
                        Transaction.token_address.in_(token_addresses)
                    )
                )
                .group_by(Transaction.token_address)
            )

            stats_result = await session.execute(stats_query)
            return {
                row.token_address: {
                    'wallet_count': row.wallet_count,
                    'buy_count': row.buy_count,
                    'sell_count': row.sell_count
                }
                for row in stats_result
            }
        except Exception as e:
            logging.error(f"獲取交易統計數據時發生錯誤: {e}")
            return {}.error(f"獲取緩存數據時發生錯誤: {e}")
            return None
        
    async def cache_transactions(self, cache_key: str, data: Dict, expire: int = 300):
        """
        緩存交易數據
        """
        try:
            await self.set_json(cache_key, data, expire)
            await self.set(f"{cache_key}:last_update", 
                          str(int(datetime.now(timezone.utc).timestamp())), 
                          expire)
        except Exception as e:
            logging.error(f"緩存數據時發生錯誤: {e}")

    async def invalidate_transaction_cache(self, chain: str):
        """
        當有新的交易數據時，使相關的緩存失效
        """
        try:
            pattern = f"transactions:{chain.lower()}:*"
            keys = await self.redis.keys(pattern)
            if keys:
                await self.redis.delete(*keys)
        except Exception as e:
            logging

    async def get_filtered_token_list(self) -> List[str]:
        """
        獲取需要過濾的代幣地址列表
        先從緩存獲取,如果緩存不存在或過期則從 API 獲取
        """
        try:
            cache_key = "filtered_tokens:BYD"
            cached_data = await self.get_json(cache_key)
            
            if cached_data:
                return cached_data
            # 如果緩存不存在,從 API 獲取
            async with aiohttp.ClientSession() as session:
                # async with session.get('http://172.25.183.151:4200/internal/smart_token_filter_list?brand=BYD') as response:
                async with session.get('http://127.0.0.1:5005/internal/smart_token_filter_list?brand=BYD') as response:
                    if response.status == 200:
                        data = await response.json()
                        token_list = data.get('data', [])
                        
                        # 緩存24小時
                        await self.set_json(cache_key, token_list, 86400)
                        return token_list
                    else:
                        logging.error(f"獲取過濾代幣列表失敗: {response.status}")
                        return []
                        
        except Exception as e:
            logging.error(f"獲取過濾代幣列表時發生錯誤: {e}")
            return []

    async def filter_transactions(self, transactions: List[Dict]) -> List[Dict]:
        """根據過濾列表過濾交易數據"""
        try:
            filtered_tokens = await self.get_filtered_token_list()
            if not filtered_tokens:
                return transactions
                
            return [
                tx for tx in transactions 
                if tx.get('token_address') not in filtered_tokens
            ]
            
        except Exception as e:
            logging.error(f"過濾交易數據時發生錯誤: {e}")
            return transactions

    async def update_filtered_token_list(self):
        """
        定期更新過濾代幣列表
        每24小時執行一次
        """
        while not self._stop_flag:
            try:
                await self.get_filtered_token_list()  # 這會自動更新緩存
                await asyncio.sleep(86400)  # 休眠24小時
            except Exception as e:
                logging.error(f"更新過濾代幣列表時發生錯誤: {e}")
                await asyncio.sleep(3600)  # 發生錯誤時等待1小時後重試
    
    async def get_cached_data(self, chain: str) -> Optional[List[Dict]]:
        """
        獲取緩存的交易數據，只返回最新的30條
        """
        try:
            cache_key = f"transactions:{chain.lower()}"
            data = await self.redis.get(cache_key)
            if not data:
                return None
            
            # 檢查緩存是否過期（超過3分鐘）
            last_update = await self.redis.get(f"{cache_key}:last_update")
            if last_update:
                now = int(datetime.now(timezone.utc).timestamp())
                if now - int(last_update) > 180:  # 3分鐘
                    logging.warning(f"{chain} 鏈的緩存數據已過期")
                    return None
            
            # 解析並排序數據
            cached_data = json.loads(data)
            sorted_data = sorted(
                cached_data,
                key=lambda x: x.get('transaction_time', 0),
                reverse=True
            )
            
            # 只返回最新的30條數據
            return sorted_data[:30]
                
        except Exception as e:
            logging.error(f"獲取 {chain} 鏈的緩存數據時發生錯誤: {e}")
            return None

    async def set(self, key: str, value: str, expire: int = 3600):
        """設置緩存"""
        await self.redis.set(key, value, ex=expire)

    async def get(self, key: str) -> Optional[str]:
        """獲取緩存"""
        return await self.redis.get(key)

    async def get_json(self, key: str) -> Optional[Dict]:
        """獲取 JSON 格式的緩存"""
        data = await self.get(key)
        return json.loads(data) if data else None

    async def set_json(self, key: str, value: Dict, expire: int = 3600):
        """設置 JSON 格式的緩存"""
        await self.set(key, json.dumps(value), expire)

    async def _update_filtered_token_list(self):
        """更新過濾代幣列表緩存"""
        await self.get_filtered_token_list()  # 這會自動更新緩存