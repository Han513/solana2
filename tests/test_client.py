# import sys
# import os
# import requests
# from flask import Flask, request, jsonify
# import logging
# import json
# from solana.rpc.async_api import AsyncClient
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
# from config import RPC_URL, RPC_URL_backup  # 从配置中导入主RPC_URL和备份URL

# # 轮替RPC URL的异步函数
# async def get_client():
#     """
#     轮流尝试RPC_URL和RPC_URL_backup，返回一个有效的客户端
#     """
#     # 尝试使用两个 URL，轮替
#     for url in [RPC_URL, RPC_URL_backup]:
#         try:
#             client = AsyncClient(url)
#             # 异步请求RPC服务进行健康检查
#             resp = await client.is_connected()
#             if resp:
#                 return client
#             else:
#                 logging.warning(f"RPC连接失败，尝试下一个 URL: {url}")
#         except Exception as e:
#             logging.warning(f"请求RPC URL {url}时发生错误: {e}")
#             continue

#     logging.error("所有RPC URL都不可用")
#     raise Exception("无法连接到任何 RPC URL")


# # 异步调用 get_client() 并获取客户端的示例
# import asyncio
# async def test_client():
#     client = await get_client()
#     print(client)

# # 测试代码
# if __name__ == "__main__":
#     asyncio.run(test_client())

import requests
import json

# 設定 RPC URL 和區塊編號
rpc_url = "https://methodical-capable-firefly.solana-mainnet.quiknode.pro/f660ad44a1d7512bb5f81c93144712e8ddc5c2dc"
block_number = 322069587  # 將區塊編號設為整數

# 定義請求的 payload，添加 maxSupportedTransactionVersion 參數
payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getBlock",
    "params": [block_number, {"maxSupportedTransactionVersion": 0}]
}

# 發送請求
response = requests.post(rpc_url, json=payload)

# 確認是否成功獲取數據
if response.status_code == 200:
    # 獲取並格式化返回的 JSON 數據
    block_data = response.json()
    formatted_data = json.dumps(block_data, indent=4)
    print(formatted_data)
else:
    print(f"Error fetching block data: {response.status_code}")
