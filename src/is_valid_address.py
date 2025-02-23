import requests
from config import RPC_URL, RPC_URL_backup, BSC_RPC_URL
from web3 import Web3

# 设置连接到 Binance Smart Chain 的节点
w3 = Web3(Web3.HTTPProvider('https://bsc-dataseed.binance.org/'))

def is_existing_solana_address(address: str) -> bool:
    headers = {"Content-Type": "application/json"}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [address, {"encoding": "jsonParsed"}]
    }
    
    # 尝试使用两个 URL，轮替
    for url in [RPC_URL, RPC_URL_backup]:
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)  # 添加超时设置
            response.raise_for_status()  # 如果响应代码不是 2xx，将抛出异常
            
            data = response.json()
            # 检查地址是否存在
            if "result" in data and "error" not in data:
                return True
            else:
                print(f"地址不存在或未初始化（URL: {url}）")
                return False
        except requests.exceptions.RequestException as e:
            print(f"请求失败（URL: {url}）: {e}")
            continue  # 如果一个请求失败，继续尝试下一个 URL
            
    # 如果两个 URL 都失败，返回 False
    print("两个 RPC URL 都不可用")
    return False

def is_existing_bsc_address(address: str) -> bool:
    try:
        # 将地址转换为标准的 EIP-55 校验和格式
        address = w3.to_checksum_address(address)
    except ValueError as e:
        print(f"地址格式不合法: {address}")
        return False

    # 检查地址格式是否合法
    if w3.is_address(address):

        # 检查是否为合约地址
        code = w3.eth.get_code(address)
        if code != b'':  # 如果没有合约代码，说明是普通账户地址
            return False

        return True
    else:
        print("地址格式不合法")
        return False

# 测试示例
# address = "C3DSibDg8GHRrw1obDQnFT8aaYiGaBfZevsciZBQqf3K"
# print(is_existing_solana_address(address))  # True 或 False
