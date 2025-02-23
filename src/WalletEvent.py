import asyncio
import logging
from solders.pubkey import Pubkey
from solana.rpc.api import Client
import base58
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from solana.rpc.async_api import AsyncClient
from models import async_session, save_transaction, get_active_wallets
from config import DATABASE_URI, RPC_URL
from token_info import TokenUtils

# logging.basicConfig(
#     level=logging.WARNING,  # 只顯示警告和錯誤級別的訊息
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('smart_wallet_monitor.log'),
#         logging.StreamHandler()
#     ]
# )

@asynccontextmanager
async def session_scope():
    engine = create_async_engine(DATABASE_URI, echo=True, future=True)
    Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    
    # 創建 session
    async with Session() as session:
        yield session  # 在異步上下文中返回 session

class SmartWalletMonitor:
    def __init__(self, rpc_url: str, async_session):
        self.client = AsyncClient(rpc_url)
        self.last_signatures = {}
        self.async_session = async_session

    async def analyze_transaction(self, tx_result, wallet_address: str):
        """分析交易詳情"""
        try:
            account_keys = tx_result.transaction.transaction.message.account_keys
            is_signer = any(
                str(acc.pubkey) == wallet_address and acc.signer 
                for acc in account_keys
            )
            if not is_signer:
                return None

            pre_balances = tx_result.transaction.meta.pre_token_balances
            post_balances = tx_result.transaction.meta.post_token_balances

            for pre, post in zip(pre_balances, post_balances):
                if str(pre.owner) == wallet_address:
                    token_address = str(pre.mint)
                    token_info = TokenUtils.get_token_info(token_address)
                    if token_address == "So11111111111111111111111111111111111111112":
                        continue  # 跳過 SOL 代幣

                    pre_amount = float(pre.ui_token_amount.ui_amount or 0)
                    post_amount = float(post.ui_token_amount.ui_amount or 0)
                    
                    if pre_amount != post_amount:
                        sol_balance = await TokenUtils.get_usd_balance(self.client, wallet_address)
                        sol_balance_usdt = sol_balance.get("balance_usd")
                        transaction_type = 'buy' if post_amount > pre_amount else 'sell'
                        amount = abs(post_amount - pre_amount)
                        price = token_info.get('priceUsd', '')
                        value = amount * price
                        holding_percentage = (value / sol_balance_usdt) if sol_balance_usdt > 0 else 0
                        realized_profit = value - (pre_amount * price) if transaction_type == 'sell' else None
                        current_time = datetime.now(timezone(timedelta(hours=8)))
                        block_time = tx_result.block_time
                        if block_time:
                            transaction_time = datetime.fromtimestamp(block_time, timezone.utc) + timedelta(hours=8)
                        else:
                            transaction_time = datetime.now(timezone(timedelta(hours=8)))

                        return {
                            'wallet_address': wallet_address,
                            'token_address': token_address,
                            'token_icon': token_info.get('url', ''),
                            'token_name': token_info.get('symbol', ''),
                            'price': price,
                            'amount': amount,
                            'marketcap': token_info.get('marketcap', 0),
                            'value': value,
                            'holding_percentage': holding_percentage,
                            'chain': "SOLANA", 
                            'realized_profit': 0 if transaction_type == 'buy' else value,
                            'transaction_type': transaction_type,
                            'transaction_time': transaction_time,
                            'time': current_time
                        }

            return None

        except Exception as e:
            logging.error(f"分析交易時發生錯誤: {e}")
            return None

    async def monitor_wallet(self, wallet_address: str):
        """監控單個錢包的交易"""
        try:
            address = Pubkey(base58.b58decode(wallet_address))
            
            # 第一次獲取該錢包的最後一筆簽名
            response = await self.client.get_signatures_for_address(address, limit=1)
            if not response.value:
                return
            
            latest_signatures = [sig.signature for sig in response.value]
            last_known_signature = self.last_signatures.get(wallet_address)

            if not last_known_signature:
                # 第一次啟動監控，記錄該錢包的最後一筆簽名
                self.last_signatures[wallet_address] = latest_signatures[0]
                return

            # 每過1分鐘抓取10筆最新的交易簽名
            response = await self.client.get_signatures_for_address(address, limit=10)
            if not response.value:
                return
            
            latest_signatures = [sig.signature for sig in response.value]

            # 如果第一次抓取的交易簽名不在最新的10筆交易中，則代表有新交易
            if last_known_signature != latest_signatures[0]:
                # 分析從上次簽名開始的所有新交易
                for sig in latest_signatures:
                    if sig == last_known_signature:
                        break

                    tx_details = await self.client.get_transaction(
                        sig,
                        "jsonParsed",
                        max_supported_transaction_version=0
                    )
                    
                    if tx_details.value and not tx_details.value.transaction.meta.err:
                        tx_data = await self.analyze_transaction(tx_details.value, wallet_address)
                        if tx_data:
                            # 正確地傳遞所有參數，包括 signature
                            await save_transaction(self, tx_data, wallet_address, sig)

                # 更新最新簽名
                self.last_signatures[wallet_address] = latest_signatures[0]

        except Exception as e:
            self.last_signatures[wallet_address] = latest_signatures[0]
            logging.error(f"監控錢包 {wallet_address} 時發生錯誤: {e}")

    async def start_monitoring(self, session):
        """開始監控所有智能錢包"""
        while True:
            try:
                active_wallets = await get_active_wallets(self, session)  # 來自 models.py 的函數
                
                # 並行監控所有錢包
                await asyncio.gather(
                    *[self.monitor_wallet(address) for address in active_wallets]
                )
                
                await asyncio.sleep(60)
            
            except asyncio.CancelledError:
                logging.info("監控過程被取消，將退出監控。")
                break  # Gracefully break the loop when the task is cancelled
                
            except Exception as e:
                logging.error(f"監控過程中發生錯誤: {e}")
                await asyncio.sleep(60)  # Retry after some delay

async def main():
    monitor = SmartWalletMonitor(RPC_URL, async_session)

    try:
        client = AsyncClient(RPC_URL)
        async with session_scope() as session:
            await monitor.start_monitoring(session)

    except KeyboardInterrupt:
        logging.info("\n程序被手動中斷，安全退出。")
    
    except Exception as e:
        logging.error(f"執行分析時發生錯誤: {e}")
    
    finally:
        # Ensure the session and resources are properly closed after execution
        logging.info("程序結束，清理資源...")

if __name__ == "__main__":
    try:
        asyncio.run(main())  # Start the monitoring process
    except KeyboardInterrupt:
        logging.info("監控過程被手動中斷，程序正在安全退出...")
    except Exception as e:
        logging.error(f"發生錯誤: {e}")
    finally:
        # Cleanup if necessary
        logging.info("程序結束")