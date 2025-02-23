import asyncio
import logging
from hypercorn.config import Config
from hypercorn.asyncio import serve
from main import app

if __name__ == "__main__":
    # 配置日誌
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # 配置 Hypercorn
    config = Config()
    config.bind = ["0.0.0.0:5031"]
    config.worker_class = "asyncio"
    
    # 創建並運行事件循環
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # 運行應用
    loop.run_until_complete(serve(app, config))