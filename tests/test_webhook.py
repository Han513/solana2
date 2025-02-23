import requests

def post_request_update_addresses():
    # URL 地址
    url = "http://192.168.26.10:5031/robots/smartmoney/webhook/update-addresses"
    # url = "http://127.0.0.1:5031/robots/smartmoney/webhook/update-addresses"

    # JSON 数据
    data = {
        "chain": "SOLANA",
        "type": "add",
        "address": [
            "GLMwSLoqyy6XnP7AfWbEX7vet266NHjR97NTQawyWbpn"
        ]
    }

    # 请求头
    headers = {
        'Content-Type': 'application/json'
    }

    # 发送 POST 请求
    response = requests.post(url, json=data, headers=headers)

    # 检查响应状态码
    if response.status_code == 200:
        print("请求成功，地址已更新！")
    else:
        print(f"请求失败，状态码: {response.status_code}")

# 调用函数
post_request_update_addresses()