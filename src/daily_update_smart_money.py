import logging
import traceback
import asyncio
from asyncio import create_task
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.asyncio import AsyncIOExecutor
from models import *
from WalletAnalysis import *

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def schedule_daily_updates():
    """
    Schedule the Solana smart money data update to run daily at 12:00 AM.
    """
    scheduler = AsyncIOScheduler()
    scheduler.add_executor(AsyncIOExecutor())
    scheduler.add_job(
        func=update_solana_smart_money_data,
        trigger='cron',
        hour=0,
        minute=0,
        id="daily_solana_update"
    )
    scheduler.start()
    logging.info("Daily update task for Solana smart money data scheduled at 12:00 AM.")

async def update_solana_smart_money_data():
    """
    每日更新 Solana 链的活跃钱包数据，按批次并行处理，每次处理 5 个钱包
    """
    try:
        logging.info("Starting daily update for Solana smart money data...")
        
        chain = "SOLANA"
        session_factory = sessions.get(chain.upper())
        if not session_factory:
            logging.error("Session factory for Solana chain is not configured.")
            return

        async with session_factory() as session:
            active_wallets = await get_smart_wallets(session, chain)
            if not active_wallets:
                logging.info("No active wallets found for Solana.")
                return

        # 分批处理钱包地址
        batch_size = 5
        for i in range(0, len(active_wallets), batch_size):
            batch_wallets = active_wallets[i:i + batch_size]
            logging.info(f"Processing batch {i // batch_size + 1} with {len(batch_wallets)} wallets.")

            # 为每个钱包创建独立的任务
            tasks = [
                process_wallet_with_new_session(session_factory, wallet_address, chain)
                for wallet_address in batch_wallets
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

            logging.info(f"Completed processing batch {i // batch_size + 1}.")

        logging.info("Daily update for Solana smart money data completed successfully.")
    except Exception as e:
        logging.error(f"Error during daily update for Solana smart money data: {e}")
        logging.error(traceback.format_exc())

async def process_wallet_with_new_session(session_factory, wallet_address, chain):
    """
    为每个钱包创建独立的 AsyncSession 并处理数据
    """
    try:
        async with session_factory() as session:
            logging.info(f"Processing wallet: {wallet_address}")
            await update_smart_money_data(session, wallet_address, chain)
            logging.info(f"Completed processing wallet: {wallet_address}")
    except Exception as e:
        logging.error(f"Error processing wallet {wallet_address}: {e}")
        logging.error(traceback.format_exc())

async def main():
    """
    主入口，啟動事件循環並調用定時任務。
    """
    schedule_daily_updates()  # 啟動定時任務
    logging.info("Scheduler started. Running the event loop...")
    while True:
        await asyncio.sleep(3600)  # 避免主協程結束，讓事件循環保持運行

if __name__ == "__main__":
    try:
        asyncio.run(main())  # 使用 asyncio.run 啟動事件循環
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down scheduler...")
