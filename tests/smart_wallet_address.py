# import requests
# import json
# import pandas as pd
# import time
# import random

# def fetch_data(token_address, wallet_type, file_name="top_traders.xlsx"):
#     payload_1 = { 'api_key': '10c49deefe86d00a0f6905eb0830a98d',
#                 'url': f'https://gmgn.ai/defi/quotation/v1/tokens/top_traders/sol/{token_address}?orderby=realized_profit&direction=desc',
#                 'autoparse': 'true' }
#     response = requests.get('https://api.scraperapi.com/', params=payload_1)
#     # response = requests.get(
#     #     url='https://app.scrapingbee.com/api/v1',
#     #     params={
#     #         'api_key': '7AZP5D48TNTPJPYUHSUF2XUWCPWNEBCARXFK5NDP0R955K76D0J661TA9RD1LXI0QWLLCWZAWXIIHJIH',
#     #         'url': f'https://gmgn.ai/defi/quotation/v1/tokens/top_traders/sol/{token_address}?orderby=realized_profit&direction=desc'
#     #     },
#     # )
    
#     # Check if the response is successful
#     if response.status_code == 200:
#         data = response.json()
#         # Extract the necessary fields: address and token_address
#         if 'data' in data:
#             new_data = []
#             for item in data["data"]:
#                 # 檢查 tags 是否包含 "sandwich_bot"
#                 if "tags" in item and "sandwich_bot" in item["tags"]:
#                     continue  # 跳過該筆資料

#                 # 正常處理資料
#                 new_data.append({
#                     "token_address": token_address,
#                     "wallet_address": item["address"],
#                     'twitter_username': item['twitter_username'] if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
#                     'twitter_name': item['twitter_name'] if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
#                     'tag': 'kol' if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
#                     "total_cost": item['total_cost'],
#                     "profit": item['realized_profit'],
#                     "unrealized_profit": item['unrealized_profit'],
#                     "last_active_timestamp": item['last_active_timestamp'],
#                     "wallet_type": wallet_type,
#                     "is_analyzed": 0
#                 })
            
#             # 呼叫 save_to_excel 函數將數據存入 Excel
#             save_to_excel(new_data, file_name)
#         else:
#             print("Error: No data found.")
#     else:
#         print(f"Error: Failed to fetch data (Status code {response.status_code})")

# def fetch_data(token_address, wallet_type, file_name="top_traders.xlsx", json_file_path="data.json"):
#     try:
#         # 明確指定編碼為 UTF-8
#         with open(json_file_path, 'r', encoding='utf-8') as file:
#             data = json.load(file)
        
#         # 確保 JSON 中有 data 欄位
#         if 'data' in data and isinstance(data['data'], list):
#             new_data = []
#             for item in data["data"]:
#                 # 檢查 tags 是否包含 "sandwich_bot"
#                 if "tags" in item and "sandwich_bot" in item["tags"]:
#                     continue  # 跳過該筆資料

#                 # 正常處理資料
#                 new_data.append({
#                     "token_address": token_address,
#                     "wallet_address": item["address"],
#                     'twitter_username': item['twitter_username'] if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
#                     'twitter_name': item['twitter_name'] if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
#                     'tag': 'kol' if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
#                     "total_cost": item['total_cost'],
#                     "profit": item['realized_profit'],
#                     "unrealized_profit": item['unrealized_profit'],
#                     "last_active_timestamp": item['last_active_timestamp'],
#                     "wallet_type": wallet_type,
#                     "is_analyzed": 0
#                 })
            
#             # 呼叫 save_to_excel 函數將數據存入 Excel
#             save_to_excel(new_data, file_name)
#         else:
#             print("Error: No valid data found in the JSON file.")
#     except FileNotFoundError:
#         print(f"Error: JSON file '{json_file_path}' not found.")
#     except json.JSONDecodeError:
#         print(f"Error: Failed to decode JSON from '{json_file_path}'.")
#     except UnicodeDecodeError as e:
#         print(f"Error: Unicode decode error - {e}")

# def save_to_excel(data, file_name="top_traders.xlsx"):
#     if data:
#         try:
#             # 如果Excel文件已經存在，讀取它並添加新數據
#             existing_df = pd.read_excel(file_name)
#             existing_wallets = set(existing_df['wallet_address'].values)

#             # 過濾掉已存在的 wallet_address
#             filtered_data = [item for item in data if item['wallet_address'] not in existing_wallets]

#             if filtered_data:
#                 # 合併新數據和現有數據
#                 new_df = pd.DataFrame(filtered_data)
#                 combined_df = pd.concat([existing_df, new_df], ignore_index=True)
#                 combined_df.to_excel(file_name, index=False)
#                 print(f"Data saved to {file_name}, added {len(filtered_data)} new records.")
#         except FileNotFoundError:
#             # 如果文件不存在，则创建一个新文件
#             df = pd.DataFrame(data)
#             df.to_excel(file_name, index=False)
#             print(f"Data saved to {file_name}")
#     else:
#         print("No data to save.")

# def load_and_filter_excel(file_name="top_traders.xlsx"):
#     # 读取 Excel 文件，获取需要分析的wallet地址（is_analyzed == 0）
#     try:
#         df = pd.read_excel(file_name)
#         unprocessed_data = df[df["is_analyzed"] == 0]
#         return unprocessed_data
#     except FileNotFoundError:
#         print(f"文件 {file_name} 不存在.")
#         return pd.DataFrame()  # 返回一个空的 DataFrame

# def update_is_analyzed(file_name="top_traders.xlsx", wallet_address=None):
#     # 讀取Excel文件，將已分析的錢包地址標記為1
#     df = pd.read_excel(file_name)
#     df.loc[df["wallet_address"] == wallet_address, "is_analyzed"] = 1
#     df.to_excel(file_name, index=False)
#     print(f"Wallet address {wallet_address} is marked as analyzed.")

# def post_request(wallet_address, wallet_type):
#     # url = "http://127.0.0.1:5001/robots/smartmoney/analyzewallet"         #本地
#     # url = "http://172.25.183.177:5031/robots/smartmoney/analyzewallet"    #測試
#     url = "http://192.168.26.10:5031/robots/smartmoney/analyzewallet"      #生產
    
#     data = {
#         "chain": "SOLANA",
#         "wallet_address": wallet_address,
#         "wallet_type": wallet_type
#     }
#     headers = {
#         'Content-Type': 'application/json'
#     }
    
#     response = requests.post(url, json=data, headers=headers)
    
#     if response.status_code == 200:
#         print(f"分析成功: {wallet_address}")
#     else:
#         print(f"分析失败: {wallet_address}, 状态码: {response.status_code}")

# def analyze_wallets():
#     # 读取Excel并筛选未分析的数据
#     unprocessed_data = load_and_filter_excel()
    
#     # 逐一处理每个未分析的钱包地址
#     for _, row in unprocessed_data.iterrows():
#         wallet_address = row["wallet_address"]
#         wallet_type = row["wallet_type"]
#         print(f"正在分析钱包: {wallet_address}")
        
#         post_request(wallet_address, wallet_type)
        
#         sleep_time = random.randint(60, 120)
#         print(f"等待 {sleep_time} 秒后再分析下一个钱包...")
#         time.sleep(sleep_time)
#         update_is_analyzed(wallet_address=wallet_address)

# if __name__ == "__main__":
#     token_address = "99kBqp1dCstDZk16EdvZ9KijDSYeGakNVdkh3qHjpump"
#     data = fetch_data(token_address, 2)
#     save_to_excel(data)
    
#     # 分析并更新未分析的钱包地址
#     # analyze_wallets()

# -----------------------------------------------------------------------------------------
import requests
import threading
import time
import random
import logging
import pandas as pd
import json
from queue import Queue

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')

class DataProcessor:
    def __init__(self):
        self.stop_flag = False
        self.excel_lock = threading.Lock()  # 用於同步Excel文件訪問

    def fetch_token_data(self):
        """獲取並篩選符合條件的代幣地址"""
        filtered_tokens = set()  # 使用集合來避免重複
        
        # 定義兩個時間週期的URL
        urls = [
            {
                'period': '6h',
                'url': 'https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/6h?' +
                      'device_id=fc03f488-f52f-4552-9a12-bcd0d1fa04e1&' +
                      'client_id=gmgn_web_2025.0218.115546&from_app=gmgn&' +
                      'app_ver=2025.0218.115546&tz_name=Asia%2FTaipei&' +
                      'tz_offset=28800&app_lang=en&orderby=marketcap&' +
                      'direction=desc&filters[]=renounced&filters[]=frozen&' +
                      'filters[]=not_wash_trading'
            },
            {
                'period': '24h',
                'url': 'https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/24h?' +
                      'device_id=fc03f488-f52f-4552-9a12-bcd0d1fa04e1&' +
                      'client_id=gmgn_web_2025.0218.115546&from_app=gmgn&' +
                      'app_ver=2025.0218.115546&tz_name=Asia%2FTaipei&' +
                      'tz_offset=28800&app_lang=en&orderby=marketcap&' +
                      'direction=desc&filters[]=renounced&filters[]=frozen&' +
                      'filters[]=not_wash_trading'
            }
        ]
        
        for url_info in urls:
            try:
                logging.info(f"開始獲取 {url_info['period']} 時間週期的代幣數據...")
                
                payload = {
                    'api_key': '10c49deefe86d00a0f6905eb0830a98d',
                    'url': url_info['url'],
                    'autoparse': 'true'
                }
                
                response = requests.get('https://api.scraperapi.com/', params=payload, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'data' in data and 'rank' in data['data']:
                    period_tokens = 0  # 計算此時間週期找到的代幣數
                    for token in data['data']['rank']:
                        # 篩選條件
                        if (token.get('top_10_holder_rate', 1) < 0.2 and
                            token.get('market_cap', 0) >= 500000 and
                            token.get('rat_trader_amount_rate', 1) == 0 and
                            token.get('burn_status') == 'burn'):
                            
                            filtered_tokens.add(token['address'])
                            period_tokens += 1
                            logging.info(f"在 {url_info['period']} 週期中找到符合條件的代幣: {token['symbol']} ({token['address']})")
                    
                    logging.info(f"{url_info['period']} 週期找到 {period_tokens} 個符合條件的代幣")
                
                # 在處理完一個URL後添加短暫延遲
                time.sleep(random.randint(5, 10))
                
            except Exception as e:
                logging.error(f"獲取 {url_info['period']} 週期代幣數據時發生錯誤: {str(e)}")
                continue
        
        logging.info(f"總共找到 {len(filtered_tokens)} 個不重複的符合條件代幣")
        return list(filtered_tokens)

    def check_token_exists(self, token_address, file_name="top_traders.xlsx"):
        """檢查token是否已經存在於Excel文件中"""
        try:
            with self.excel_lock:
                df = pd.read_excel(file_name)
                return token_address in df['token_address'].values
        except FileNotFoundError:
            return False
        except Exception as e:
            logging.error(f"檢查token時發生錯誤: {str(e)}")
            return False

    def fetch_data_thread(self, token_address, wallet_type):
        last_token_update = 0
        filtered_tokens = []
        
        while not self.stop_flag:
            try:
                current_time = time.time()
                
                # 每24小時更新一次代幣列表
                if current_time - last_token_update >= 86400:  # 24小時 = 86400秒
                    logging.info("開始更新代幣列表...")
                    filtered_tokens = self.fetch_token_data()
                    last_token_update = current_time
                    logging.info(f"找到 {len(filtered_tokens)} 個符合條件的代幣")
                
                # 如果有符合條件的代幣，輪流處理每個代幣
                if filtered_tokens:
                    for token in filtered_tokens:
                        if self.stop_flag:
                            break
                            
                        # 檢查token是否已存在
                        if self.check_token_exists(token):
                            logging.info(f"代幣 {token} 已存在於資料庫中，跳過處理")
                            continue
                            
                        with self.excel_lock:
                            logging.info(f"開始獲取代幣 {token} 的數據...")
                            fetch_data(token, wallet_type)
                        
                        # 處理完一個代幣後等待一段時間
                        time.sleep(random.randint(60, 120))  # 1-2分鐘的隨機間隔
                
                # 等待一段時間後再次檢查
                time.sleep(random.randint(300, 600))  # 5-10分鐘的隨機間隔
                
            except Exception as e:
                logging.error(f"獲取數據時發生錯誤: {str(e)}")
                time.sleep(60)  # 發生錯誤時等待1分鐘後重試

    def analyze_wallets_thread(self):
        while not self.stop_flag:
            try:
                # 讀取Excel並篩選未分析的數據
                with self.excel_lock:  # 使用鎖來保護Excel文件操作
                    unprocessed_data = load_and_filter_excel()

                if unprocessed_data.empty:
                    logging.info("沒有新的錢包需要分析，等待60秒...")
                    time.sleep(60)
                    continue

                # 逐一處理每個未分析的錢包地址
                for _, row in unprocessed_data.iterrows():
                    if self.stop_flag:
                        break

                    wallet_address = row["wallet_address"]
                    wallet_type = row["wallet_type"]
                    logging.info(f"正在分析錢包: {wallet_address}")
                    
                    post_request(wallet_address, wallet_type)
                    
                    sleep_time = random.randint(60, 120)
                    logging.info(f"等待 {sleep_time} 秒後再分析下一個錢包...")
                    time.sleep(sleep_time)

                    with self.excel_lock:  # 使用鎖來保護Excel文件操作
                        update_is_analyzed(wallet_address=wallet_address)

            except Exception as e:
                logging.error(f"分析錢包時發生錯誤: {str(e)}")
                time.sleep(60)  # 發生錯誤時等待1分鐘後重試

    def start_processing(self, token_address, wallet_type):
        # 創建並啟動數據獲取線程
        fetch_thread = threading.Thread(
            target=self.fetch_data_thread,
            args=(token_address, wallet_type),
            name="FetchThread"
        )
        
        # 創建並啟動錢包分析線程
        analyze_thread = threading.Thread(
            target=self.analyze_wallets_thread,
            name="AnalyzeThread"
        )

        fetch_thread.start()
        analyze_thread.start()

        return fetch_thread, analyze_thread

    def stop_processing(self):
        self.stop_flag = True

def post_request(wallet_address, wallet_type):
    url = "http://127.0.0.1:5031/robots/smartmoney/analyzewallet"         #本地
    # url = "http://172.25.183.177:5031/robots/smartmoney/analyzewallet"    #測試
    # url = "http://192.168.26.10:5031/robots/smartmoney/analyzewallet"      #生產
    
    data = {
        "chain": "SOLANA",
        "wallet_address": wallet_address,
        "wallet_type": wallet_type
    }
    headers = {
        'Content-Type': 'application/json'
    }
    
    response = requests.post(url, json=data, headers=headers)
    
    if response.status_code == 200:
        print(f"分析成功: {wallet_address}")
    else:
        print(f"分析失败: {wallet_address}, 状态码: {response.status_code}")

def update_is_analyzed(file_name="top_traders.xlsx", wallet_address=None):
    # 讀取Excel文件，將已分析的錢包地址標記為1
    df = pd.read_excel(file_name)
    df.loc[df["wallet_address"] == wallet_address, "is_analyzed"] = 1
    df.to_excel(file_name, index=False)
    print(f"Wallet address {wallet_address} is marked as analyzed.")

def load_and_filter_excel(file_name="top_traders.xlsx"):
    # 读取 Excel 文件，获取需要分析的wallet地址（is_analyzed == 0）
    try:
        df = pd.read_excel(file_name)
        unprocessed_data = df[df["is_analyzed"] == 0]
        return unprocessed_data
    except FileNotFoundError:
        print(f"文件 {file_name} 不存在.")
        return pd.DataFrame()  # 返回一个空的 DataFrame

def save_to_excel(data, file_name="top_traders.xlsx"):
    if data:
        try:
            # 如果Excel文件已經存在，讀取它並添加新數據
            existing_df = pd.read_excel(file_name)
            existing_wallets = set(existing_df['wallet_address'].values)

            # 過濾掉已存在的 wallet_address
            filtered_data = [item for item in data if item['wallet_address'] not in existing_wallets]

            if filtered_data:
                # 合併新數據和現有數據
                new_df = pd.DataFrame(filtered_data)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df.to_excel(file_name, index=False)
                print(f"Data saved to {file_name}, added {len(filtered_data)} new records.")
        except FileNotFoundError:
            # 如果文件不存在，则创建一个新文件
            df = pd.DataFrame(data)
            df.to_excel(file_name, index=False)
            print(f"Data saved to {file_name}")
    else:
        print("No data to save.")

def fetch_data(token_address, wallet_type, file_name="top_traders.xlsx"):
    payload_1 = { 'api_key': '10c49deefe86d00a0f6905eb0830a98d',
                'url': f'https://gmgn.ai/defi/quotation/v1/tokens/top_traders/sol/{token_address}?orderby=realized_profit&direction=desc',
                'autoparse': 'true' }
    response = requests.get('https://api.scraperapi.com/', params=payload_1)
    # response = requests.get(
    #     url='https://app.scrapingbee.com/api/v1',
    #     params={
    #         'api_key': '7AZP5D48TNTPJPYUHSUF2XUWCPWNEBCARXFK5NDP0R955K76D0J661TA9RD1LXI0QWLLCWZAWXIIHJIH',
    #         'url': f'https://gmgn.ai/defi/quotation/v1/tokens/top_traders/sol/{token_address}?orderby=realized_profit&direction=desc'
    #     },
    # )
    
    # Check if the response is successful
    if response.status_code == 200:
        data = response.json()
        # Extract the necessary fields: address and token_address
        if 'data' in data:
            new_data = []
            for item in data["data"]:
                # 檢查 tags 是否包含 "sandwich_bot"
                if "tags" in item and "sandwich_bot" in item["tags"]:
                    continue  # 跳過該筆資料

                # 正常處理資料
                new_data.append({
                    "token_address": token_address,
                    "wallet_address": item["address"],
                    'twitter_username': item['twitter_username'] if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
                    'twitter_name': item['twitter_name'] if item['twitter_name'] and 'kol' in item['twitter_name'] else None,
                    'tag': (', '.join(item["tags"]) + ', kol' if item['twitter_name'] else ', '.join(item["tags"])) if item["tags"] else ('kol' if item['twitter_name'] else None),
                    "total_cost": item['total_cost'],
                    "profit": item['realized_profit'],
                    "unrealized_profit": item['unrealized_profit'],
                    "last_active_timestamp": item['last_active_timestamp'],
                    "wallet_type": wallet_type,
                    "is_analyzed": 0
                })
            
            # 呼叫 save_to_excel 函數將數據存入 Excel
            save_to_excel(new_data, file_name)
        else:
            print("Error: No data found.")
    else:
        print(f"Error: Failed to fetch data (Status code {response.status_code})")

def signal_handler(signum, frame):
    logging.info("接收到停止信號，正在停止處理...")
    if 'processor' in globals():
        processor.stop_processing()

if __name__ == "__main__":
    import signal
    signal.signal(signal.SIGINT, signal_handler)  # 註冊 CTRL+C 信號處理器
    
    token_address = "99kBqp1dCstDZk16EdvZ9KijDSYeGakNVdkh3qHjpump"
    wallet_type = 0
    
    processor = DataProcessor()
    fetch_thread = None
    analyze_thread = None
    
    try:
        # 啟動處理
        fetch_thread, analyze_thread = processor.start_processing(token_address, wallet_type)
        
        # 使用事件等待而不是input
        while not processor.stop_flag:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("接收到Ctrl+C，正在停止處理...")
    finally:
        # 確保程序能夠正確停止
        if processor:
            processor.stop_processing()
        if fetch_thread and fetch_thread.is_alive():
            fetch_thread.join(timeout=5)  # 等待最多5秒
        if analyze_thread and analyze_thread.is_alive():
            analyze_thread.join(timeout=5)  # 等待最多5秒
        logging.info("程序已停止")