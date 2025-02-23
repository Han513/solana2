import base58
import requests
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

class TokenUtils:
    @staticmethod
    def get_token_info(token_mint_address: str) -> dict:
        """
        獲取代幣的一般信息，返回包括價格的數據。
        """
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    return {
                        "symbol": data['pairs'][0].get('baseToken', {}).get('symbol', None),
                        "url": data['pairs'][0].get('url', "no url"),
                        "marketcap": data['pairs'][0].get('marketCap', 0),
                        "priceNative": float(data['pairs'][0].get('priceNative', 0)),
                        "priceUsd": float(data['pairs'][0].get('priceUsd', 0)),
                        "volume": data['pairs'][0].get('volume', 0),
                        "liquidity": data['pairs'][0].get('liquidity', 0)
                    }
            else:
                data = response.json()
        except Exception as e:
            return {"priceUsd": 0}
        return {"priceUsd": 0}
    
    @staticmethod
    def get_sol_info(token_mint_address: str) -> dict:
        """
        獲取代幣的一般信息，返回包括價格的數據。
        """
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    return {
                        "symbol": data['pairs'][0].get('baseToken', {}).get('symbol', None),
                        "url": data['pairs'][0].get('url', "no url"),
                        "marketcap": data['pairs'][0].get('marketCap', 0),
                        "priceNative": float(data['pairs'][0].get('priceNative', 0)),
                        "priceUsd": float(data['pairs'][0].get('priceUsd', 0)),
                        "volume": data['pairs'][0].get('volume', 0),
                        "liquidity": data['pairs'][0].get('liquidity', 0)
                    }
            else:
                data = response.json()
        except Exception as e:
            return {"priceUsd": 234.8}
        return {"priceUsd": 234.8}

    @staticmethod
    async def get_token_balance(client: AsyncClient, token_account: str) -> dict:
        """
        獲取 SPL 代幣餘額。
        :param client: AsyncClient Solana 客戶端
        :param token_account: 代幣賬戶地址
        :return: 包含餘額數據的字典
        """
        try:
            token_pubkey = Pubkey(base58.b58decode(token_account))
            balance_response = await client.get_token_account_balance(token_pubkey)
            balance = {
                'decimals': balance_response.value.decimals,
                'balance': {
                    'int': int(balance_response.value.amount),
                    'float': float(balance_response.value.ui_amount)
                }
            }
            return balance
        except Exception as e:
            print(f"獲取 SPL 代幣餘額時出錯: {e}")
            return {"decimals": 0, "balance": {"int": 0, "float": 0.0}}

    @staticmethod
    async def get_sol_balance(client: AsyncClient, wallet_address: str) -> dict:
        """
        獲取 SOL 餘額。
        :param client: AsyncClient Solana 客戶端
        :param wallet_address: 錢包地址
        :return: 包含 SOL 餘額的字典
        """
        try:
            pubkey = Pubkey(base58.b58decode(wallet_address))
            balance_response = await client.get_balance(pubkey=pubkey)
            balance = {
                'decimals': 9,
                'balance': {
                    'int': balance_response.value,
                    'float': float(balance_response.value / 10**9)
                }
            }
            return balance
        except Exception as e:
            return {"decimals": 9, "balance": {"int": 0, "float": 0.0}}

    @staticmethod
    async def get_usd_balance(client: AsyncClient, wallet_address: str) -> dict:
        """
        獲取錢包的 SOL 餘額以及對應的 USD 價值。
        """
        try:
            # 查詢 SOL 餘額
            sol_balance = await TokenUtils.get_sol_balance(client, wallet_address)
            sol_usdt_price = TokenUtils.get_sol_info("So11111111111111111111111111111111111111112").get("priceUsd", 0)

            # 計算 USD 價值
            usd_value = sol_balance["balance"]["float"] * sol_usdt_price if sol_usdt_price > 0 else 0
            sol_balance["balance_usd"] = usd_value
            return sol_balance
        except Exception as e:
            print(f"獲取 USD 餘額時出錯: {e}")
            return {
                "decimals": 9,
                "balance": {"int": 0, "float": 0.0},
                "balance_usd": 0.0
            }
