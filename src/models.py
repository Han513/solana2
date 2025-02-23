import os
import re
import asyncio
import logging
from typing import Dict, List, Optional, Set
from flask import Flask
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, as_declarative
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, select, update, text, and_, or_, func, distinct, case, delete
from datetime import datetime, timedelta, timezone
from sqlalchemy.ext.declarative import declared_attr
from cache import RedisCache, generate_cache_key
from config import *
from loguru_logger import logger
from asyncio import gather
from database import (
    Transaction, WalletSummary, Holding, TokenBuyData, ErrorLog,
    sessions, get_utc8_time, make_naive_time
)

load_dotenv()
# 初始化資料庫
Base = declarative_base()
    
# async def init_db():
#     for chain, engine in engines.items():
#         async with engine.begin() as conn:
#             await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn))
#         print(f"✅ {chain} Schema 初始化完成！")
    
DATABASES = {
    "SOLANA": DATABASE_URI_SWAP_SOL,
    "ETH": DATABASE_URI_SWAP_ETH,
    "BASE": DATABASE_URI_SWAP_BASE,
    "BSC": DATABASE_URI_SWAP_BSC,
    "TRON": DATABASE_URI_SWAP_TRON,
}
# DATABASES = {
#     "SOLANA": "postgresql+asyncpg://postgres:henrywork8812601@localhost:5432/smartmoney",
#     "ETH": "postgresql+asyncpg://postgres:henrywork8812601@localhost:5432/smartmoney",
#     "BASE": "postgresql+asyncpg://postgres:henrywork8812601@localhost:5432/smartmoney",
#     "BSC": "postgresql+asyncpg://postgres:henrywork8812601@localhost:5432/smartmoney",
#     "TRON": "postgresql+asyncpg://postgres:henrywork8812601@localhost:5432/smartmoney",
# }
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# 为每条链初始化 engine 和 sessionmaker
engines = {
    chain: create_async_engine(db_uri, echo=False, future=True)
    for chain, db_uri in DATABASES.items()
}

# 创建 sessionmaker 映射
sessions = {
    chain: sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    for chain, engine in engines.items()
}

TZ_UTC8 = timezone(timedelta(hours=8))

def get_utc8_time():
    """获取 UTC+8 当前时间"""
    return datetime.now(TZ_UTC8).replace(tzinfo=None)

def make_naive_time(dt):
    """将时间转换为无时区的格式"""
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

@as_declarative()
class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # 添加一个动态 schema 属性
    __table_args__ = {}
    
    @classmethod
    def with_schema(cls, schema: str):
        cls.__table_args__ = {"schema": schema}
        for table in Base.metadata.tables.values():
            if table.name == cls.__tablename__:
                table.schema = schema
        return cls

# 基本錢包資訊表
class WalletSummary(Base):
    """
    整合的錢包數據表
    """
    __tablename__ = 'wallet'
    # __table_args__ = {'schema': 'solana'}

    id = Column(Integer, primary_key=True, comment='ID')
    address = Column(String(100), nullable=False, unique=True, comment='錢包地址')
    balance = Column(Float, nullable=True, comment='錢包餘額')
    balance_USD = Column(Float, nullable=True, comment='錢包餘額 (USD)')
    chain = Column(String(50), nullable=False, comment='區塊鏈類型')
    tag = Column(String(50), nullable=True, comment='標籤')
    twitter_name = Column(String(50), nullable=True, comment='X名稱')
    twitter_username = Column(String(50), nullable=True, comment='X用戶名')
    is_smart_wallet = Column(Boolean, nullable=True, comment='是否為聰明錢包')
    wallet_type = Column(Integer, nullable=True, comment='0:一般聰明錢，1:pump聰明錢，2:moonshot聰明錢')
    asset_multiple = Column(Float, nullable=True, comment='資產翻倍數(到小數第1位)')
    token_list = Column(String(100), nullable=True, comment='用户最近交易的三种代币信息')

    # 交易數據
    avg_cost_30d = Column(Float, nullable=True, comment='30日平均成本')
    avg_cost_7d = Column(Float, nullable=True, comment='7日平均成本')
    avg_cost_1d = Column(Float, nullable=True, comment='1日平均成本')
    total_transaction_num_30d = Column(Integer, nullable=True, comment='30日總交易次數')
    total_transaction_num_7d = Column(Integer, nullable=True, comment='7日總交易次數')
    total_transaction_num_1d = Column(Integer, nullable=True, comment='1日總交易次數')
    buy_num_30d = Column(Integer, nullable=True, comment='30日買入次數')
    buy_num_7d = Column(Integer, nullable=True, comment='7日買入次數')
    buy_num_1d = Column(Integer, nullable=True, comment='1日買入次數')
    sell_num_30d = Column(Integer, nullable=True, comment='30日賣出次數')
    sell_num_7d = Column(Integer, nullable=True, comment='7日賣出次數')
    sell_num_1d = Column(Integer, nullable=True, comment='1日賣出次數')
    win_rate_30d = Column(Float, nullable=True, comment='30日勝率')
    win_rate_7d = Column(Float, nullable=True, comment='7日勝率')
    win_rate_1d = Column(Float, nullable=True, comment='1日勝率')

    # 盈虧數據
    pnl_30d = Column(Float, nullable=True, comment='30日盈虧')
    pnl_7d = Column(Float, nullable=True, comment='7日盈虧')
    pnl_1d = Column(Float, nullable=True, comment='1日盈虧')
    pnl_percentage_30d = Column(Float, nullable=True, comment='30日盈虧百分比')
    pnl_percentage_7d = Column(Float, nullable=True, comment='7日盈虧百分比')
    pnl_percentage_1d = Column(Float, nullable=True, comment='1日盈虧百分比')
    pnl_pic_30d = Column(String(512), nullable=True, comment='30日每日盈虧圖')
    pnl_pic_7d = Column(String(512), nullable=True, comment='7日每日盈虧圖')
    pnl_pic_1d = Column(String(512), nullable=True, comment='1日每日盈虧圖')
    unrealized_profit_30d = Column(Float, nullable=True, comment='30日未實現利潤')
    unrealized_profit_7d = Column(Float, nullable=True, comment='7日未實現利潤')
    unrealized_profit_1d = Column(Float, nullable=True, comment='1日未實現利潤')
    total_cost_30d = Column(Float, nullable=True, comment='30日總成本')
    total_cost_7d = Column(Float, nullable=True, comment='7日總成本')
    total_cost_1d = Column(Float, nullable=True, comment='1日總成本')
    avg_realized_profit_30d = Column(Float, nullable=True, comment='30日平均已實現利潤')
    avg_realized_profit_7d = Column(Float, nullable=True, comment='7日平均已實現利潤')
    avg_realized_profit_1d = Column(Float, nullable=True, comment='1日平均已實現利潤')

    # 收益分布數據
    distribution_gt500_30d = Column(Integer, nullable=True, comment='30日收益分布 >500% 的次數')
    distribution_200to500_30d = Column(Integer, nullable=True, comment='30日收益分布 200%-500% 的次數')
    distribution_0to200_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-200% 的次數')
    distribution_0to50_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-50% 的次數')
    distribution_lt50_30d = Column(Integer, nullable=True, comment='30日收益分布 <50% 的次數')
    distribution_gt500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 >500% 的比例')
    distribution_200to500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 <50% 的比例')

    distribution_gt500_7d = Column(Integer, nullable=True, comment='7日收益分布 >500% 的次數')
    distribution_200to500_7d = Column(Integer, nullable=True, comment='7日收益分布 200%-500% 的次數')
    distribution_0to200_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-200% 的次數')
    distribution_0to50_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-50% 的次數')
    distribution_lt50_7d = Column(Integer, nullable=True, comment='7日收益分布 <50% 的次數')
    distribution_gt500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 >500% 的比例')
    distribution_200to500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 <50% 的比例')

    # 更新時間和最後交易時間
    update_time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    last_transaction_time = Column(Integer, nullable=True, comment='最後活躍時間')
    is_active = Column(Boolean, nullable=True, comment='是否還是聰明錢')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
class Transaction(Base):
    __tablename__ = 'wallet_transactions'

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="聰明錢錢包地址")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    token_icon = Column(Text, nullable=True, comment="代幣圖片網址")
    token_name = Column(String(100), nullable=True, comment="代幣名稱")
    price = Column(Float, nullable=True, comment="價格")
    amount = Column(Float, nullable=False, comment="數量")
    marketcap = Column(Float, nullable=True, comment="市值")
    value = Column(Float, nullable=True, comment="價值")
    holding_percentage = Column(Float, nullable=True, comment="倉位百分比")
    chain = Column(String(50), nullable=False, comment="區塊鏈")
    realized_profit = Column(Float, nullable=True, comment="已實現利潤")
    realized_profit_percentage = Column(Float, nullable=True, comment="已實現利潤百分比")
    transaction_type = Column(String(10), nullable=False, comment="事件 (buy, sell)")
    transaction_time = Column(Integer, nullable=False, comment="交易時間")
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    signature = Column(String(100), nullable=False, unique=True, comment="交易簽名")

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Holding(Base):
    __tablename__ = 'wallet_holding'

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(255), nullable=False)  # 添加长度限制
    token_address = Column(String(255), nullable=False)  # 添加长度限制
    token_icon = Column(String(255), nullable=True)  # 添加长度限制
    token_name = Column(String(255), nullable=True)  # 添加长度限制
    chain = Column(String(50), nullable=False, default='Unknown')  # 添加长度限制
    amount = Column(Float, nullable=False, default=0.0)
    value = Column(Float, nullable=False, default=0.0)
    value_USDT = Column(Float, nullable=False, default=0.0)
    unrealized_profits = Column(Float, nullable=False, default=0.0)
    pnl = Column(Float, nullable=False, default=0.0)
    pnl_percentage = Column(Float, nullable=False, default=0.0)
    avg_price = Column(Float, nullable=False, default=0.0)
    marketcap = Column(Float, nullable=False, default=0.0)
    is_cleared = Column(Boolean, nullable=False, default=False)
    cumulative_cost = Column(Float, nullable=False, default=0.0)
    cumulative_profit = Column(Float, nullable=False, default=0.0)
    last_transaction_time = Column(Integer, nullable=True, comment='最後活躍時間')
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
class TokenBuyData(Base):
    __tablename__ = 'wallet_buy_data'

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="錢包地址")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    total_amount = Column(Float, nullable=False, default=0.0, comment="代幣總數量")
    total_cost = Column(Float, nullable=False, default=0.0, comment="代幣總成本")
    avg_buy_price = Column(Float, nullable=False, default=0.0, comment="平均買入價格")
    updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="最後更新時間")

class ErrorLog(Base):
    """
    錯誤訊息記錄表
    """
    __tablename__ = 'error_logs'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='ID')
    timestamp = Column(DateTime, nullable=False, default=get_utc8_time, comment='時間')
    module_name = Column(String(100), nullable=True, comment='檔案名稱')
    function_name = Column(String(100), nullable=True, comment='函數名稱')
    error_message = Column(Text, nullable=False, comment='錯誤訊息')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# -------------------------------------------------------------------------------------------------------------------
# async def initialize_database():
#     """
#     初始化多个区块链的数据库，并为每个链创建对应的 Schema。
#     """
#     try:
#         # 遍历 engines 字典，为每个区块链的数据库执行初始化
#         for chain, engine in engines.items():
#             schema_name = chain.lower()  # 使用链名作为 Schema 名称（全小写）
#             async with engine.begin() as conn:
#                 # 创建 Schema（如果不存在）
#                 print(f"正在检查或创建 Schema: {schema_name}...")
#                 await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

#                 # 将所有表映射到对应的 Schema
#                 for table in Base.metadata.tables.values():
#                     table.schema = schema_name  # 为每张表设置 Schema

#                 # 创建表
#                 print(f"正在初始化 {schema_name} Schema 中的表...")
#                 await conn.run_sync(Base.metadata.create_all)
#                 print(f"{schema_name} Schema 初始化完成。")
#     except Exception as e:
#         logging.error(f"数据库初始化失败: {e}")
#         raise

async def initialize_database():
    """初始化多個區塊鏈的資料庫"""
    try:
        for chain, engine in engines.items():
            schema_name = chain.lower()
            async with engine.begin() as conn:
                print(f"正在檢查或創建 Schema: {schema_name}...")
                await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

                print(f"正在初始化 {schema_name} Schema 中的表...")
                await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn))

                print(f"{schema_name} Schema 初始化完成。")

    except Exception as e:
        logging.error(f"數據庫初始化失敗: {e}")
        print(f"數據庫初始化失敗: {e}")  # 直接打印錯誤
        raise

async def write_wallet_data_to_db(session, wallet_data, chain):
    """
    将钱包数据写入或更新 WalletSummary 表
    """
    try:
        schema = chain.lower()
        WalletSummary.with_schema(schema)
        # 查詢是否已經存在相同的 wallet_address
        existing_wallet = await session.execute(
            select(WalletSummary).filter(WalletSummary.address == wallet_data["wallet_address"])
        )
        existing_wallet = existing_wallet.scalars().first()
        if existing_wallet:
            # 如果已經存在，就更新資料
            if wallet_data.get("balance") is not None:
                existing_wallet.balance = wallet_data.get("balance", 0)
            if wallet_data.get("balance_USD") is not None:
                existing_wallet.balance_USD = wallet_data.get("balance_USD", 0)
            if wallet_data.get("chain") is not None:
                existing_wallet.chain = wallet_data.get("chain", "Solana")
            if wallet_data.get("wallet_type") is not None:
                existing_wallet.wallet_type = wallet_data.get("wallet_type", 0)
            if wallet_data.get("asset_multiple") is not None:
                existing_wallet.asset_multiple = float(wallet_data.get("asset_multiple", 0) or 0)
            if wallet_data.get("token_list") is not None:
                existing_wallet.token_list = wallet_data.get("token_list", False)
            if wallet_data["stats_30d"].get("average_cost") is not None:
                existing_wallet.avg_cost_30d = wallet_data["stats_30d"].get("average_cost", 0)
            if wallet_data["stats_7d"].get("average_cost") is not None:
                existing_wallet.avg_cost_7d = wallet_data["stats_7d"].get("average_cost", 0)
            if wallet_data["stats_1d"].get("average_cost") is not None:
                existing_wallet.avg_cost_1d = wallet_data["stats_1d"].get("average_cost", 0)
            if wallet_data["stats_30d"].get("total_transaction_num") is not None:
                existing_wallet.total_transaction_num_30d = wallet_data["stats_30d"].get("total_transaction_num", 0)
            if wallet_data["stats_7d"].get("total_transaction_num") is not None:
                existing_wallet.total_transaction_num_7d = wallet_data["stats_7d"].get("total_transaction_num", 0)
            if wallet_data["stats_1d"].get("total_transaction_num") is not None:
                existing_wallet.total_transaction_num_1d = wallet_data["stats_1d"].get("total_transaction_num", 0)
            if wallet_data["stats_30d"].get("total_buy") is not None:
                existing_wallet.buy_num_30d = wallet_data["stats_30d"].get("total_buy", 0)
            if wallet_data["stats_7d"].get("total_buy") is not None:
                existing_wallet.buy_num_7d = wallet_data["stats_7d"].get("total_buy", 0)
            if wallet_data["stats_1d"].get("total_buy") is not None:
                existing_wallet.buy_num_1d = wallet_data["stats_1d"].get("total_buy", 0)
            if wallet_data["stats_30d"].get("total_sell") is not None:
                existing_wallet.sell_num_30d = wallet_data["stats_30d"].get("total_sell", 0)
            if wallet_data["stats_7d"].get("total_sell") is not None:
                existing_wallet.sell_num_7d = wallet_data["stats_7d"].get("total_sell", 0)
            if wallet_data["stats_1d"].get("total_sell") is not None:
                existing_wallet.sell_num_1d = wallet_data["stats_1d"].get("total_sell", 0)
            if wallet_data["stats_30d"].get("win_rate") is not None:
                existing_wallet.win_rate_30d = wallet_data["stats_30d"].get("win_rate", 0)
            if wallet_data["stats_7d"].get("win_rate") is not None:
                existing_wallet.win_rate_7d = wallet_data["stats_7d"].get("win_rate", 0)
            if wallet_data["stats_1d"].get("win_rate") is not None:
                existing_wallet.win_rate_1d = wallet_data["stats_1d"].get("win_rate", 0)
            if wallet_data["stats_30d"].get("pnl") is not None:
                existing_wallet.pnl_30d = wallet_data["stats_30d"].get("pnl", 0)
            if wallet_data["stats_7d"].get("pnl") is not None:
                existing_wallet.pnl_7d = wallet_data["stats_7d"].get("pnl", 0)
            if wallet_data["stats_1d"].get("pnl") is not None:
                existing_wallet.pnl_1d = wallet_data["stats_1d"].get("pnl", 0)
            if wallet_data["stats_30d"].get("pnl_percentage") is not None:
                existing_wallet.pnl_percentage_30d = wallet_data["stats_30d"].get("pnl_percentage", 0)
            if wallet_data["stats_7d"].get("pnl_percentage") is not None:
                existing_wallet.pnl_percentage_7d = wallet_data["stats_7d"].get("pnl_percentage", 0)
            if wallet_data["stats_1d"].get("pnl_percentage") is not None:
                existing_wallet.pnl_percentage_1d = wallet_data["stats_1d"].get("pnl_percentage", 0)
            if wallet_data["stats_30d"].get("daily_pnl_chart") is not None:
                existing_wallet.pnl_pic_30d = wallet_data["stats_30d"].get("daily_pnl_chart", "")
            if wallet_data["stats_7d"].get("daily_pnl_chart") is not None:
                existing_wallet.pnl_pic_7d = wallet_data["stats_7d"].get("daily_pnl_chart", "")
            if wallet_data["stats_1d"].get("daily_pnl_chart") is not None:
                existing_wallet.pnl_pic_1d = wallet_data["stats_1d"].get("daily_pnl_chart", "")
            if wallet_data["stats_30d"].get("total_unrealized_profit") is not None:
                existing_wallet.unrealized_profit_30d = wallet_data["stats_30d"].get("total_unrealized_profit", 0)
            if wallet_data["stats_7d"].get("total_unrealized_profit") is not None:
                existing_wallet.unrealized_profit_7d = wallet_data["stats_7d"].get("total_unrealized_profit", 0)
            if wallet_data["stats_1d"].get("total_unrealized_profit") is not None:
                existing_wallet.unrealized_profit_1d = wallet_data["stats_1d"].get("total_unrealized_profit", 0)
            if wallet_data["stats_30d"].get("total_cost") is not None:
                existing_wallet.total_cost_30d = wallet_data["stats_30d"].get("total_cost", 0)
            if wallet_data["stats_7d"].get("total_cost") is not None:
                existing_wallet.total_cost_7d = wallet_data["stats_7d"].get("total_cost", 0)
            if wallet_data["stats_1d"].get("total_cost") is not None:
                existing_wallet.total_cost_1d = wallet_data["stats_1d"].get("total_cost", 0)
            if wallet_data["stats_30d"].get("avg_realized_profit") is not None:
                existing_wallet.avg_realized_profit_30d = wallet_data["stats_30d"].get("avg_realized_profit", 0)
            if wallet_data["stats_7d"].get("avg_realized_profit") is not None:
                existing_wallet.avg_realized_profit_7d = wallet_data["stats_7d"].get("avg_realized_profit", 0)
            if wallet_data["stats_1d"].get("avg_realized_profit") is not None:
                existing_wallet.avg_realized_profit_1d = wallet_data["stats_1d"].get("avg_realized_profit", 0)
            if wallet_data["stats_30d"].get("distribution_gt500") is not None:
                existing_wallet.distribution_gt500_30d = wallet_data["stats_30d"].get("distribution_gt500", 0)
            if wallet_data["stats_30d"].get("distribution_200to500") is not None:
                existing_wallet.distribution_200to500_30d = wallet_data["stats_30d"].get("distribution_200to500", 0)
            if wallet_data["stats_30d"].get("distribution_0to200") is not None:
                existing_wallet.distribution_0to200_30d = wallet_data["stats_30d"].get("distribution_0to200", 0)
            if wallet_data["stats_30d"].get("distribution_0to50") is not None:
                existing_wallet.distribution_0to50_30d = wallet_data["stats_30d"].get("distribution_0to50", 0)
            if wallet_data["stats_30d"].get("distribution_lt50") is not None:
                existing_wallet.distribution_lt50_30d = wallet_data["stats_30d"].get("distribution_lt50", 0)
            if wallet_data["stats_30d"].get("distribution_gt500_percentage") is not None:
                existing_wallet.distribution_gt500_percentage_30d = wallet_data["stats_30d"].get("distribution_gt500_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_200to500_percentage") is not None:
                existing_wallet.distribution_200to500_percentage_30d = wallet_data["stats_30d"].get("distribution_200to500_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_0to200_percentage") is not None:
                existing_wallet.distribution_0to200_percentage_30d = wallet_data["stats_30d"].get("distribution_0to200_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_0to50_percentage") is not None:
                existing_wallet.distribution_0to50_percentage_30d = wallet_data["stats_30d"].get("distribution_0to50_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_lt50_percentage") is not None:
                existing_wallet.distribution_lt50_percentage_30d = wallet_data["stats_30d"].get("distribution_lt50_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_gt500") is not None:
                existing_wallet.distribution_gt500_7d = wallet_data["stats_7d"].get("distribution_gt500", 0)
            if wallet_data["stats_7d"].get("distribution_200to500") is not None:
                existing_wallet.distribution_200to500_7d = wallet_data["stats_7d"].get("distribution_200to500", 0)
            if wallet_data["stats_7d"].get("distribution_0to200") is not None:
                existing_wallet.distribution_0to200_7d = wallet_data["stats_7d"].get("distribution_0to200", 0)
            if wallet_data["stats_7d"].get("distribution_0to50") is not None:
                existing_wallet.distribution_0to50_7d = wallet_data["stats_7d"].get("distribution_0to50", 0)
            if wallet_data["stats_7d"].get("distribution_lt50") is not None:
                existing_wallet.distribution_lt50_7d = wallet_data["stats_7d"].get("distribution_lt50", 0)
            if wallet_data["stats_7d"].get("distribution_gt500_percentage") is not None:
                existing_wallet.distribution_gt500_percentage_7d = wallet_data["stats_7d"].get("distribution_gt500_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_200to500_percentage") is not None:
                existing_wallet.distribution_200to500_percentage_7d = wallet_data["stats_7d"].get("distribution_200to500_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_0to200_percentage") is not None:
                existing_wallet.distribution_0to200_percentage_7d = wallet_data["stats_7d"].get("distribution_0to200_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_0to50_percentage") is not None:
                existing_wallet.distribution_0to50_percentage_7d = wallet_data["stats_7d"].get("distribution_0to50_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_lt50_percentage") is not None:
                existing_wallet.distribution_lt50_percentage_7d = wallet_data["stats_7d"].get("distribution_lt50_percentage", 0.0)
            if wallet_data.get("last_transaction_time") is not None:
                existing_wallet.last_transaction_time = wallet_data.get("last_transaction_time", int(datetime.now(timezone.utc).timestamp()))

            existing_wallet.update_time = get_utc8_time()

            await session.commit()
            print(f"Successfully updated wallet: {wallet_data['wallet_address']}")
        else:
            # 如果不存在，就插入新資料
            wallet_summary = WalletSummary(
                address=wallet_data["wallet_address"],
                balance=wallet_data.get("balance", 0),
                balance_USD=wallet_data.get("balance_USD", 0),
                chain=wallet_data.get("chain", "Solana"),
                twitter_name=wallet_data.get("twitter_name", None),
                twitter_username=wallet_data.get("twitter_username", None),
                is_smart_wallet=wallet_data.get("is_smart_wallet", False),
                wallet_type=wallet_data.get("wallet_type", 0),
                asset_multiple=wallet_data.get("asset_multiple", 0),
                token_list=wallet_data.get("token_list", False),
                avg_cost_30d=wallet_data["stats_30d"].get("average_cost", 0),
                avg_cost_7d=wallet_data["stats_7d"].get("average_cost", 0),
                avg_cost_1d=wallet_data["stats_1d"].get("average_cost", 0),
                total_transaction_num_30d = wallet_data["stats_30d"].get("total_transaction_num", 0),
                total_transaction_num_7d = wallet_data["stats_7d"].get("total_transaction_num", 0),
                total_transaction_num_1d = wallet_data["stats_1d"].get("total_transaction_num", 0),
                buy_num_30d=wallet_data["stats_30d"].get("total_buy", 0),
                buy_num_7d=wallet_data["stats_7d"].get("total_buy", 0),
                buy_num_1d=wallet_data["stats_1d"].get("total_buy", 0),
                sell_num_30d=wallet_data["stats_30d"].get("total_sell", 0),
                sell_num_7d=wallet_data["stats_7d"].get("total_sell", 0),
                sell_num_1d=wallet_data["stats_1d"].get("total_sell", 0),
                win_rate_30d=wallet_data["stats_30d"].get("win_rate", 0),
                win_rate_7d=wallet_data["stats_7d"].get("win_rate", 0),
                win_rate_1d=wallet_data["stats_1d"].get("win_rate", 0),
                pnl_30d=wallet_data["stats_30d"].get("pnl", 0),
                pnl_7d=wallet_data["stats_7d"].get("pnl", 0),
                pnl_1d=wallet_data["stats_1d"].get("pnl", 0),
                pnl_percentage_30d=wallet_data["stats_30d"].get("pnl_percentage", 0),
                pnl_percentage_7d=wallet_data["stats_7d"].get("pnl_percentage", 0),
                pnl_percentage_1d=wallet_data["stats_1d"].get("pnl_percentage", 0),
                pnl_pic_30d=wallet_data["stats_30d"].get("daily_pnl_chart", ""),
                pnl_pic_7d=wallet_data["stats_7d"].get("daily_pnl_chart", ""),
                pnl_pic_1d=wallet_data["stats_1d"].get("daily_pnl_chart", ""),
                unrealized_profit_30d=wallet_data["stats_30d"].get("total_unrealized_profit", 0),
                unrealized_profit_7d=wallet_data["stats_7d"].get("total_unrealized_profit", 0),
                unrealized_profit_1d=wallet_data["stats_1d"].get("total_unrealized_profit", 0),
                total_cost_30d=wallet_data["stats_30d"].get("total_cost", 0),
                total_cost_7d=wallet_data["stats_7d"].get("total_cost", 0),
                total_cost_1d=wallet_data["stats_1d"].get("total_cost", 0),
                avg_realized_profit_30d=wallet_data["stats_30d"].get("avg_realized_profit", 0),
                avg_realized_profit_7d=wallet_data["stats_7d"].get("avg_realized_profit", 0),
                avg_realized_profit_1d=wallet_data["stats_1d"].get("avg_realized_profit", 0),
                distribution_gt500_30d=wallet_data["stats_30d"].get("distribution_gt500", 0),
                distribution_200to500_30d=wallet_data["stats_30d"].get("distribution_200to500", 0),
                distribution_0to200_30d=wallet_data["stats_30d"].get("distribution_0to200", 0),
                distribution_0to50_30d=wallet_data["stats_30d"].get("distribution_0to50", 0),
                distribution_lt50_30d=wallet_data["stats_30d"].get("distribution_lt50", 0),
                distribution_gt500_percentage_30d=wallet_data["stats_30d"].get("distribution_gt500_percentage", 0.0),
                distribution_200to500_percentage_30d=wallet_data["stats_30d"].get("distribution_200to500_percentage", 0.0),
                distribution_0to200_percentage_30d=wallet_data["stats_30d"].get("distribution_0to200_percentage", 0.0),
                distribution_0to50_percentage_30d=wallet_data["stats_30d"].get("distribution_0to50_percentage", 0.0),
                distribution_lt50_percentage_30d=wallet_data["stats_30d"].get("distribution_lt50_percentage", 0.0),
                distribution_gt500_7d=wallet_data["stats_7d"].get("distribution_gt500", 0),
                distribution_200to500_7d=wallet_data["stats_7d"].get("distribution_200to500", 0),
                distribution_0to200_7d=wallet_data["stats_7d"].get("distribution_0to200", 0),
                distribution_0to50_7d=wallet_data["stats_7d"].get("distribution_0to50", 0),
                distribution_lt50_7d=wallet_data["stats_7d"].get("distribution_lt50", 0),
                distribution_gt500_percentage_7d=wallet_data["stats_7d"].get("distribution_gt500_percentage", 0.0),
                distribution_200to500_percentage_7d=wallet_data["stats_7d"].get("distribution_200to500_percentage", 0.0),
                distribution_0to200_percentage_7d=wallet_data["stats_7d"].get("distribution_0to200_percentage", 0.0),
                distribution_0to50_percentage_7d=wallet_data["stats_7d"].get("distribution_0to50_percentage", 0.0),
                distribution_lt50_percentage_7d=wallet_data["stats_7d"].get("distribution_lt50_percentage", 0.0),
                update_time=get_utc8_time(),
                last_transaction_time=wallet_data.get("last_transaction_time", int(datetime.now(timezone.utc).timestamp())),
                is_active=True,
            )
            session.add(wallet_summary)
            await session.commit()
            print(f"Successfully added wallet: {wallet_data['wallet_address']}")
        return True
    except Exception as e:
        await session.rollback()
        print(e)
        await log_error(
            session,
            str(e),
            "models",
            "write_wallet_data_to_db",
            f"Failed to save wallet {wallet_data['wallet_address']}"
        )
        return False
    
async def get_wallets_address_by_chain(chain, session):
    """
    根據指定的鏈類型 (chain) 從 WalletSummary 資料表中查詢數據，
    並回傳符合條件的所有地址，且該地址的 is_smart_wallet 為 False。
    """
    try:
        schema = chain.lower()
        WalletSummary.with_schema(schema)
        result = await session.execute(
            select(WalletSummary.address)
            .where(WalletSummary.chain == chain)
            .where(WalletSummary.is_smart_wallet == False)  # 加入 is_smart_wallet 為 False 的條件
        )
        addresses = result.scalars().all()  # 只取出 address 欄位的所有符合條件的地址
        return addresses
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "get_wallets_by_chain",
            f"查詢所有聰明錢列表失敗，原因 {e}"
        )
        return []
    
async def deactivate_wallets(session, addresses):
    """
    根據提供的地址列表，將 WalletSummary 中符合的錢包地址的 is_active 欄位設置為 False。
    
    :param session: 資料庫會話
    :param addresses: 一個包含要更新的地址的列表
    :return: 更新成功的錢包數量
    """
    try:
        # 更新符合條件的錢包地址的 is_active 為 False
        result = await session.execute(
            update(WalletSummary)
            .where(WalletSummary.address.in_(addresses))  # 篩選出符合的錢包地址
            .values(is_active=False)  # 將 is_active 設為 False
        )
        await session.commit()  # 提交交易
        
        return result.rowcount  # 返回更新的行數，即更新成功的錢包數量
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "deactivate_wallets",
            f"更新錢包 is_active 欄位為 False 失敗，原因 {e}"
        )
        return 0  # 若更新失敗，返回 0
    
async def activate_wallets(session, addresses):
    try:
        # 更新符合條件的錢包地址的 is_active 為 False
        result = await session.execute(
            update(WalletSummary)
            .where(WalletSummary.address.in_(addresses))
            .values(is_active=True)
        )
        await session.commit()  # 提交交易
        
        return result.rowcount  # 返回更新的行數，即更新成功的錢包數量
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "deactivate_wallets",
            f"更新錢包 is_active 欄位為 False 失敗，原因 {e}"
        )
        return 0  # 若更新失敗，返回 0
    
async def get_active_wallets(session, chain):
        
        schema = chain.lower()
        WalletSummary.with_schema(schema)
        # async with self.async_session() as session:
        result = await session.execute(
            select(WalletSummary.address).where(WalletSummary.is_active == True)
        )
        return [row[0] for row in result]

async def get_smart_wallets(session, chain):
    """
    获取 WalletSummary 表中 is_smart_wallet 为 True 的钱包地址
    """
    schema = chain.lower()
    WalletSummary.with_schema(schema)
    result = await session.execute(
        select(WalletSummary.address).where(WalletSummary.is_smart_wallet == True)
    )
    return [row[0] for row in result]

async def get_active_or_smart_wallets(session, chain):
    """
    获取 WalletSummary 表中 is_active 或 is_smart_wallet 为 True 的钱包地址
    """
    schema = chain.lower()
    WalletSummary.with_schema(schema)
    result = await session.execute(
        select(WalletSummary.address).where(
            (WalletSummary.is_active == True) | (WalletSummary.is_smart_wallet == True)
        )
    )
    return [row[0] for row in result]

# async def save_transaction(self, tx_data: dict, wallet_address: str, signature: str):
#     """Save transaction record to the database"""
#     async with self.async_session() as session:
#         try:
#             # 創建 Transaction 實例並填充所有字段
#             transaction = Transaction(
#                 wallet_address=wallet_address,
#                 token_address=tx_data['token_address'],
#                 token_icon=tx_data.get('token_icon', ''),  # 如果缺少默認為空
#                 token_name=tx_data.get('token_name', ''),  # 如果缺少默認為空
#                 price=tx_data.get('price', 0),
#                 amount=tx_data.get('amount', 0),
#                 marketcap=tx_data.get('marketcap', 0),
#                 value=tx_data.get('value', 0),
#                 holding_percentage=tx_data.get('holding_percentage', 0),
#                 chain=tx_data.get('chain', 'Unknown'),
#                 realized_profit=tx_data.get('realized_profit', None),  
#                 realized_profit_percentage=tx_data.get('realized_profit_percentage', None),
#                 transaction_type=tx_data.get('transaction_type', 'unknown'),
#                 transaction_time=tx_data.get('transaction_time'),
#                 time=tx_data.get('time', get_utc8_time()),  # 當前時間作為備用
#                 signature=signature
#             )

#             # 將交易記錄添加到數據庫
#             session.add(transaction)
#             await session.commit()
            
#             # 更新 WalletSummary 表中的最後交易時間
#             await session.execute(
#                 update(WalletSummary)
#                 .where(WalletSummary.address == wallet_address)
#                 .values(
#                     last_transaction_time=tx_data['transaction_time']  # 正確傳遞字段值
#                 )
#             )
#             await session.commit()
            
#         except Exception as e:
#             await session.rollback()
#             await log_error(
#             session,
#             str(e),
#             "models",
#             "save_transaction",
#             f"Failed to save transaction {wallet_address}"
#         )

async def save_transaction(session, tx_data: dict, wallet_address: str, signature: str):
    """保存交易記錄到資料庫"""
    try:
        transaction = Transaction(
            wallet_address=wallet_address,
            token_address=tx_data['token_address'],
            token_icon=tx_data.get('token_icon', ''),
            token_name=tx_data.get('token_name', ''),
            price=tx_data.get('price', 0),
            amount=tx_data.get('amount', 0),
            marketcap=tx_data.get('marketcap', 0),
            value=tx_data.get('value', 0),
            holding_percentage=tx_data.get('holding_percentage', 0),
            chain=tx_data.get('chain', 'Unknown'),
            realized_profit=tx_data.get('realized_profit', None),
            realized_profit_percentage=tx_data.get('realized_profit_percentage', None),
            transaction_type=tx_data.get('transaction_type', 'unknown'),
            transaction_time=tx_data.get('transaction_time'),
            time=tx_data.get('time', get_utc8_time()),
            signature=signature
        )

        session.add(transaction)
        await session.commit()

        await session.execute(
            update(WalletSummary)
            .where(WalletSummary.address == wallet_address)
            .values(last_transaction_time=tx_data['transaction_time'])
        )
        await session.commit()

    except Exception as e:
        await session.rollback()
        await log_error(
            session,
            str(e),
            "models",
            "save_transaction",
            f"Failed to save transaction {wallet_address}"
        )

def remove_emoji(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # 表情符号
        "\U0001F300-\U0001F5FF"  # 符号和图片字符
        "\U0001F680-\U0001F6FF"  # 运输和地图符号
        "\U0001F1E0-\U0001F1FF"  # 国旗
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r"", text)

async def save_past_transaction(async_session: AsyncSession, tx_data: dict, wallet_address: str, signature: str, chain):
    """Save or update transaction record to the database"""
    try:
        schema = chain.lower()
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)

        # 使用 SELECT 查询检查是否已存在相同的 signature
        existing_transaction = await async_session.execute(
            select(Transaction).where(Transaction.signature == signature)
        )
        transaction = existing_transaction.scalar()

        tx_data["token_name"] = remove_emoji(tx_data.get('token_name', ''))
        tx_data["transaction_time"] = make_naive_time(tx_data.get("transaction_time"))
        tx_data["time"] = make_naive_time(tx_data.get("time", get_utc8_time()))

        # 如果交易记录已存在，则更新该记录
        if transaction:
            await async_session.execute(
                update(Transaction)
                .where(Transaction.signature == signature)
                .values(
                    wallet_address=wallet_address,
                    token_address=tx_data['token_address'],
                    token_icon=tx_data.get('token_icon', ''),
                    token_name=tx_data.get('token_name', ''),
                    price=tx_data.get('price', 0),
                    amount=tx_data.get('amount', 0),
                    marketcap=tx_data.get('marketcap', 0),
                    value=tx_data.get('value', 0),
                    holding_percentage=tx_data.get('holding_percentage', 0),
                    chain=tx_data.get('chain', 'Unknown'),
                    realized_profit=tx_data.get('realized_profit', None),
                    realized_profit_percentage=tx_data.get('realized_profit_percentage', None),
                    transaction_type=tx_data.get('transaction_type', 'unknown'),
                    transaction_time=tx_data['transaction_time'],
                    time=tx_data['time'],
                )
            )
            await async_session.execute(
                update(WalletSummary)
                .where(WalletSummary.address == wallet_address)
                .values(
                    last_transaction_time=tx_data['transaction_time']
                )
            )
        else:
            # 如果交易记录不存在，则插入新记录
            transaction = Transaction(
                wallet_address=wallet_address,
                token_address=tx_data['token_address'],
                token_icon=tx_data.get('token_icon', ''),
                token_name=tx_data.get('token_name', ''),
                price=tx_data.get('price', 0),
                amount=tx_data.get('amount', 0),
                marketcap=tx_data.get('marketcap', 0),
                value=tx_data.get('value', 0),
                holding_percentage=tx_data.get('holding_percentage', 0),
                chain=tx_data.get('chain', 'Unknown'),
                realized_profit=tx_data.get('realized_profit', None),
                realized_profit_percentage=tx_data.get('realized_profit_percentage', None),
                transaction_type=tx_data.get('transaction_type', 'unknown'),
                transaction_time=tx_data['transaction_time'],
                time=tx_data['time'],
                signature=signature
            )
            async_session.add(transaction)
            await async_session.execute(
                update(WalletSummary)
                .where(WalletSummary.address == wallet_address)
                .values(
                    last_transaction_time=tx_data['transaction_time']
                )
            )
        await async_session.commit()
    except Exception as e:
        print(f"存储交易记录失败: {e}")
        await async_session.rollback()

# async def save_holding(tx_data: dict, wallet_address: str, session: AsyncSession, chain):
#     """Save transaction record to the database"""
#     try:
#         schema = chain.lower()
#         Holding.with_schema(schema)

#         # 查询是否已存在相同 wallet_address 和 token_address 的记录
#         existing_holding = await session.execute(
#             select(Holding).filter(
#                 Holding.wallet_address == wallet_address,
#                 Holding.token_address == tx_data.get('token_address', '')
#             )
#         )
#         existing_holding = existing_holding.scalars().first()

#         holding_data = {
#             "wallet_address": wallet_address,
#             "token_address": tx_data.get('token_address', ''),
#             "token_icon": tx_data.get('token_icon', ''),
#             "token_name": tx_data.get('token_name', ''),
#             "chain": tx_data.get('chain', 'Unknown'),
#             "amount": tx_data.get('amount', 0),
#             "value": tx_data.get('value', 0),
#             "value_USDT": tx_data.get('value_USDT', 0),
#             "unrealized_profits": tx_data.get('unrealized_profit', 0),
#             "pnl": tx_data.get('pnl', 0),
#             "pnl_percentage": tx_data.get('pnl_percentage', 0),
#             "avg_price": tx_data.get('avg_price', 0),
#             "marketcap": tx_data.get('marketcap', 0),
#             "is_cleared": tx_data.get('sell_amount', 0) >= tx_data.get('buy_amount', 0),
#             "cumulative_cost": tx_data.get('cost', 0),
#             "cumulative_profit": tx_data.get('profit', 0),
#             "last_transaction_time": make_naive_time(tx_data.get('last_transaction_time', datetime.now())),
#             "time": make_naive_time(tx_data.get('time', datetime.now())),
#         }

#         if existing_holding:
#             for key, value in holding_data.items():
#                 setattr(existing_holding, key, value)
#             await session.commit()
#         else:
#             holding = Holding(**holding_data)
#             session.add(holding)
#             await session.commit()

#     except Exception as e:
#         await session.rollback()
#         print(f"Error while saving holding for wallet {wallet_address}, token {tx_data.get('token_name', '')}: {str(e)}")
#         await log_error(
#             session,
#             str(e),
#             "models",
#             "save_holding",
#             f"Failed to save holding for wallet {wallet_address}, token {tx_data.get('token_name', '')}"
#         )

async def save_holding(tx_data_list: list, wallet_address: str, session: AsyncSession, chain: str):
    """Save transaction record to the database, and delete tokens no longer held in bulk"""
    try:
        schema = chain.lower()
        Holding.with_schema(schema)

        # 查询数据库中钱包的所有持仓
        existing_holdings = await session.execute(
            select(Holding).filter(Holding.wallet_address == wallet_address)
        )
        existing_holdings = existing_holdings.scalars().all()

        # 提取数据库中现有的 token_address 集合
        existing_token_addresses = {holding.token_address for holding in existing_holdings}

        # 提取 tx_data_list 中当前持有的 token_address 集合
        current_token_addresses = {token.get("token_address") for token in tx_data_list}

        # 计算需要删除的 tokens
        tokens_to_delete = existing_token_addresses - current_token_addresses

        # 删除不再持有的代币记录
        if tokens_to_delete:
            await session.execute(
                delete(Holding).filter(
                    Holding.wallet_address == wallet_address,
                    Holding.token_address.in_(tokens_to_delete)
                )
            )

        # 更新或新增持仓
        for token_data in tx_data_list:
            token_address = token_data.get("token_address")
            if not token_address:
                print(f"Invalid token data: {token_data}")
                continue

            existing_holding = next((h for h in existing_holdings if h.token_address == token_address), None)

            holding_data = {
                "wallet_address": wallet_address,
                "token_address": token_address,
                "token_icon": token_data.get('token_icon', ''),
                "token_name": token_data.get('token_name', ''),
                "chain": token_data.get('chain', 'Unknown'),
                "amount": token_data.get('amount', 0),
                "value": token_data.get('value', 0),
                "value_USDT": token_data.get('value_USDT', 0),
                "unrealized_profits": token_data.get('unrealized_profit', 0),
                "pnl": token_data.get('pnl', 0),
                "pnl_percentage": token_data.get('pnl_percentage', 0),
                "avg_price": token_data.get('avg_price', 0),
                "marketcap": token_data.get('marketcap', 0),
                "is_cleared": token_data.get('sell_amount', 0) >= token_data.get('buy_amount', 0),
                "cumulative_cost": token_data.get('cost', 0),
                "cumulative_profit": token_data.get('profit', 0),
                "last_transaction_time": make_naive_time(token_data.get('last_transaction_time', datetime.now())),
                "time": make_naive_time(token_data.get('time', datetime.now())),
            }

            if existing_holding:
                # 更新现有记录
                for key, value in holding_data.items():
                    setattr(existing_holding, key, value)
            else:
                # 新增记录
                holding = Holding(**holding_data)
                session.add(holding)

        # 提交数据库变更
        await session.commit()

    except Exception as e:
        # 错误处理
        await session.rollback()
        print(f"Error while saving holding for wallet {wallet_address}: {str(e)}")

async def clear_all_holdings(wallet_address: str, session: AsyncSession, chain: str):
    """清除錢包的所有持倉記錄"""
    try:
        schema = chain.lower()
        Holding.with_schema(schema)

        await session.execute(
            delete(Holding).filter(Holding.wallet_address == wallet_address)
        )
        await session.commit()
        print(f"Cleared all holdings for wallet {wallet_address}.")
    except Exception as e:
        await session.rollback()
        print(f"Error while clearing holdings for wallet {wallet_address}: {e}")

async def save_wallet_buy_data(tx_data: dict, wallet_address: str, session: AsyncSession, chain):
    """
    優化後的保存或更新 WalletHoldings 表的持倉數據函數
    """
    try:
        schema = chain.lower()
        TokenBuyData.with_schema(schema)
        
        # 使用單個查詢來獲取或創建記錄
        stmt = select(TokenBuyData).filter(
            TokenBuyData.wallet_address == wallet_address,
            TokenBuyData.token_address == tx_data.get('token_address', '')
        )
        
        # 使用 with_for_update 來避免並發問題
        result = await session.execute(stmt.with_for_update())
        existing_holding = result.scalars().first()

        # 準備數據（移到條件判斷外以避免重複計算）
        total_amount = tx_data.get('total_amount', 0.0)
        total_cost = tx_data.get('total_cost', 0.0)
        avg_buy_price = (tx_data.get('avg_buy_price') or 
                        (total_cost / total_amount if total_amount > 0 else 0.0))
        
        current_time = get_utc8_time()  # 只調用一次時間函數

        if existing_holding:
            # 使用 bulk update 來提高性能
            await session.execute(
                update(TokenBuyData)
                .where(
                    TokenBuyData.wallet_address == wallet_address,
                    TokenBuyData.token_address == tx_data.get('token_address', '')
                )
                .values(
                    total_amount=total_amount,
                    total_cost=total_cost,
                    avg_buy_price=avg_buy_price,
                    updated_at=current_time
                )
            )
        else:
            # 直接創建新記錄
            new_holding = TokenBuyData(
                wallet_address=wallet_address,
                token_address=tx_data.get('token_address', ''),
                total_amount=total_amount,
                total_cost=total_cost,
                avg_buy_price=avg_buy_price,
                updated_at=current_time
            )
            session.add(new_holding)

        # 一次性提交事務
        # await session.commit()

    except Exception as e:
        await session.rollback()
        print(f"Error saving holding for wallet {wallet_address}, "
              f"token {tx_data.get('token_address', '')}: {str(e)}")
        raise  # 重新拋出異常以便上層處理

async def get_token_buy_data(wallet_address: str, token_address: str, session: AsyncSession, chain):
    """
    查询 TokenBuyData 数据表，获取指定钱包地址和代币地址的持仓数据。

    :param wallet_address: 钱包地址
    :param token_address: 代币地址
    :param session: 数据库异步会话
    :return: 包含 avg_buy_price 和 total_amount 的字典。如果没有找到记录，则返回 None。
    """
    try:
        schema = chain.lower()
        TokenBuyData.with_schema(schema)
        # 查询是否存在指定钱包和代币的记录
        result = await session.execute(
            select(TokenBuyData).filter(
                TokenBuyData.wallet_address == wallet_address,
                TokenBuyData.token_address == token_address
            )
        )
        token_data = result.scalars().first()

        if token_data:
            # 返回所需的字段
            return {
                "token_address": token_data.token_address,
                "avg_buy_price": token_data.avg_buy_price,
                "total_amount": token_data.total_amount,
                "total_cost": token_data.total_cost
            }
        else:
            return {
                "token_address": token_address,
                "avg_buy_price": 0,
                "total_amount": 0,
                "total_cost": 0
            }

    except Exception as e:
        print(f"Error while querying TokenBuyData for wallet {wallet_address}, token {token_address}: {str(e)}")

async def reset_wallet_buy_data(wallet_address: str, session: AsyncSession, chain):
    """
    重置指定钱包的所有代币购买数据
    """
    try:
        schema = chain.lower()
        TokenBuyData.with_schema(schema)
        
        # 获取该钱包的所有代币记录
        stmt = select(TokenBuyData).filter(TokenBuyData.wallet_address == wallet_address)
        result = await session.execute(stmt)
        holdings = result.scalars().all()
        
        # 重置所有代币的数据
        for holding in holdings:
            holding.total_amount = 0
            holding.total_cost = 0
            holding.avg_buy_price = 0
            holding.updated_at = get_utc8_time()
        
        await asyncio.shield(session.commit())
    except Exception as e:
        await session.rollback()
        print(f"Error while resetting wallet buy data for {wallet_address}: {str(e)}")

# async def log_error(session, error_message: str, module_name: str, function_name: str = None, additional_info: str = None):
#     """記錄錯誤訊息到資料庫"""
#     try:
#         error_log = ErrorLog(
#             error_message=error_message,
#             module_name=module_name,
#             function_name=function_name,
#         )
#         session.add(error_log)  # 使用異步添加操作
#         await session.commit()  # 提交變更
#     except Exception as e:
#         try:
#             await session.rollback()  # 在錯誤發生時進行回滾
#         except Exception as rollback_error:
#             print(f"回滾錯誤: {rollback_error}")
#         print(f"無法記錄錯誤訊息: {e}")
#     finally:
#         try:
#             await session.close()  # 確保 session 被正確關閉
#         except Exception as close_error:
#             print(f"關閉 session 時錯誤: {close_error}")

async def log_error(session, error_message: str, module_name: str, function_name: str = None, additional_info: str = None):
    """記錄錯誤訊息到資料庫"""
    try:
        error_log = ErrorLog(
            error_message=error_message,
            module_name=module_name,
            function_name=function_name,
        )
        session.add(error_log)
        await session.commit()
    except Exception as e:
        try:
            await session.rollback()
        except Exception as rollback_error:
            print(f"回滾錯誤: {rollback_error}")
        print(f"無法記錄錯誤訊息: {e}")
    finally:
        try:
            await session.close()
        except Exception as close_error:
            print(f"關閉 session 時錯誤: {close_error}")

# -------------------------------------------------------------------API--------------------------------------------------------------------------------
# async def query_all_wallets(sessions):
#     """
#     優化後的跨資料庫查詢函數
#     通過並行查詢提升多資料庫查詢性能
#     """
#     try:
#         async def query_single_db(chain_name: str, session_factory) -> List[dict]:
#             """
#             查詢單個資料庫的錢包數據
#             """
#             try:
#                 print(f"Querying {chain_name}")
#                 async with session_factory() as session:
#                     schema = chain_name.lower()
                    
#                     # 使用原始SQL查詢，避免 Model 類的問題
#                     query = f"""
#                         SELECT * FROM {schema}.wallet 
#                         WHERE is_active = true 
#                         ORDER BY last_transaction_time DESC
#                     """
                    
#                     # 執行查詢
#                     result = await session.execute(text(query))
#                     rows = result.fetchall()
                    
#                     # 獲取列名
#                     columns = result.keys()
                    
#                     # 轉換為字典列表
#                     wallet_dicts = []
#                     for row in rows:
#                         wallet_dict = dict(zip(columns, row))
#                         wallet_dict['chain'] = chain_name
#                         wallet_dicts.append(wallet_dict)
                        
#                     print(f"Found {len(wallet_dicts)} wallets for {chain_name}")
#                     return wallet_dicts

#             except Exception as e:
#                 logging.error(f"查詢 {chain_name} 鏈數據時發生錯誤: {str(e)}")
#                 print(f"Error querying {chain_name}: {str(e)}")
#                 return []

#         # 創建所有資料庫的查詢任務
#         tasks = [
#             query_single_db(chain_name, session_factory)
#             for chain_name, session_factory in sessions.items()
#         ]

#         # 並行執行所有查詢任務
#         all_results = await asyncio.gather(*tasks)

#         # 合併所有結果
#         all_wallets = []
#         for wallets in all_results:
#             all_wallets.extend(wallets)

#         print(f"Total wallets found: {len(all_wallets)}")

#         # 根據最後交易時間排序
#         all_wallets.sort(
#             key=lambda x: x.get('last_transaction_time', 0) or 0,
#             reverse=True
#         )

#         return all_wallets

#     except Exception as e:
#         logging.error(f"跨資料庫查詢錢包數據時發生錯誤: {str(e)}")
#         print(f"Global error: {str(e)}")
#         raise

async def query_all_wallets(sessions):
    """優化後的跨資料庫查詢函數"""
    try:
        async def query_single_db(chain_name: str, session_factory) -> List[dict]:
            try:
                print(f"Querying {chain_name}")
                async with session_factory() as session:
                    schema = chain_name.lower()
                    
                    query = f"""
                        SELECT * FROM {schema}.wallet 
                        WHERE is_active = true 
                        ORDER BY last_transaction_time DESC
                    """
                    
                    result = await session.execute(text(query))
                    rows = result.fetchall()
                    columns = result.keys()
                    
                    wallet_dicts = []
                    for row in rows:
                        wallet_dict = dict(zip(columns, row))
                        wallet_dict['chain'] = chain_name
                        wallet_dicts.append(wallet_dict)
                        
                    print(f"Found {len(wallet_dicts)} wallets for {chain_name}")
                    return wallet_dicts

            except Exception as e:
                logging.error(f"查詢 {chain_name} 鏈數據時發生錯誤: {str(e)}")
                print(f"Error querying {chain_name}: {str(e)}")
                return []

        tasks = [
            query_single_db(chain_name, session_factory)
            for chain_name, session_factory in sessions.items()
        ]

        all_results = await asyncio.gather(*tasks)

        all_wallets = []
        for wallets in all_results:
            all_wallets.extend(wallets)

        print(f"Total wallets found: {len(all_wallets)}")

        all_wallets.sort(
            key=lambda x: x.get('last_transaction_time', 0) or 0,
            reverse=True
        )

        return all_wallets

    except Exception as e:
        logging.error(f"跨資料庫查詢錢包數據時發生錯誤: {str(e)}")
        print(f"Global error: {str(e)}")
        raise

# async def query_wallet_holdings(session_factory, wallet_address, chain):
#     """
#     查詢指定鏈和錢包地址的持倉數據
#     :param session_factory: 對應鏈的 session factory
#     :param wallet_address: 錢包地址
#     :param chain: 區塊鏈名稱
#     :return: 持倉數據列表
#     """
#     try:
#         schema = chain.lower()
#         Holding.with_schema(schema)
#         async with session_factory() as session:
#             query = (
#                 select(Holding)
#                 .where(Holding.wallet_address == wallet_address)
#                 .where(Holding.chain == chain)
#             )
#             result = await session.execute(query)
#             holdings = result.scalars().all()
#             return holdings
#     except Exception as e:
#         logging.error(f"查詢持倉數據失敗: {e}")
#         raise Exception(f"查詢持倉數據失敗: {str(e)}")

async def query_wallet_holdings(session_factory, wallet_address, chain):
    """查詢指定鏈和錢包地址的持倉數據"""
    try:
        schema = chain.lower()
        Holding.with_schema(schema)
        async with session_factory() as session:
            query = (
                select(Holding)
                .where(Holding.wallet_address == wallet_address)
                .where(Holding.chain == chain)
            )
            result = await session.execute(query)
            holdings = result.scalars().all()
            return holdings
    except Exception as e:
        logging.error(f"查詢持倉數據失敗: {e}")
        raise Exception(f"查詢持倉數據失敗: {str(e)}")

# async def get_transactions_by_params(
#     session: AsyncSession,
#     chain: str,
#     wallet_addresses: List[str] = None,
#     token_address: str = None,
#     name: str = None,
#     query_string: str = None,
#     fetch_all: bool = False,
#     transaction_type: str = None,
#     filter_token_address: List[str] = None
# ):
#     """
#     優化後的交易查詢函數，修復窗口函數問題
#     """
    # try:
    #     schema = chain.lower()
    #     Transaction.with_schema(schema)
    #     WalletSummary.with_schema(schema)

    #     now_timestamp = int(datetime.now(timezone.utc).timestamp())
    #     one_hour_ago_timestamp = now_timestamp - 3600

    #     # 智能錢包 CTE
    #     smart_wallets_cte = (
    #         select(WalletSummary.address)
    #         .where(
    #             and_(
    #                 WalletSummary.chain == chain,
    #                 WalletSummary.is_smart_wallet == True
    #             )
    #         )
    #         .cte('smart_wallets')
    #     )

    #     # 計算最近一小時的統計數據 CTE
    #     hourly_stats_cte = (
    #         select(
    #             Transaction.token_address,
    #             func.count(distinct(Transaction.wallet_address)).label('wallet_count'),
    #             func.count(case((Transaction.transaction_type == 'buy', 1))).label('buy_count'),
    #             func.count(case((Transaction.transaction_type == 'sell', 1))).label('sell_count')
    #         )
    #         .where(
    #             and_(
    #                 Transaction.chain == chain,
    #                 Transaction.transaction_time >= one_hour_ago_timestamp
    #             )
    #         )
    #         .group_by(Transaction.token_address)
    #         .cte('hourly_stats')
    #     )

    #     # 構建主查詢
    #     base_query = (
    #         select(
    #             Transaction,
    #             func.coalesce(hourly_stats_cte.c.wallet_count, 0).label('wallet_count_last_hour'),
    #             func.coalesce(hourly_stats_cte.c.buy_count, 0).label('buy_count_last_hour'),
    #             func.coalesce(hourly_stats_cte.c.sell_count, 0).label('sell_count_last_hour')
    #         )
    #         .outerjoin(
    #             hourly_stats_cte,
    #             Transaction.token_address == hourly_stats_cte.c.token_address
    #         )
    #         .where(Transaction.chain == chain)
    #     )

    #     # 添加錢包地址過濾
    #     if wallet_addresses:
    #         base_query = base_query.where(Transaction.wallet_address.in_(wallet_addresses))
    #     elif not any([token_address, name, query_string, fetch_all, transaction_type, filter_token_address]):
    #         base_query = base_query.where(
    #             Transaction.wallet_address.in_(
    #                 select(smart_wallets_cte.c.address)
    #             )
    #         )

    #     # 添加其他過濾條件
    #     if token_address:
    #         base_query = base_query.where(Transaction.token_address == token_address)
    #     if name:
    #         base_query = base_query.where(Transaction.token_name == name)
    #     if query_string:
    #         base_query = base_query.where(
    #             or_(
    #                 Transaction.token_name.ilike(f"%{query_string}%"),
    #                 Transaction.token_address.ilike(f"%{query_string}%")
    #             )
    #         )
    #     if transaction_type:
    #         if transaction_type not in ['buy', 'sell']:
    #             raise ValueError("Invalid transaction_type. Must be 'buy' or 'sell'.")
    #         base_query = base_query.where(Transaction.transaction_type == transaction_type)
    #     if filter_token_address:
    #         base_query = base_query.where(~Transaction.token_address.in_(filter_token_address))

    #     # 添加排序和限制
    #     base_query = base_query.order_by(Transaction.transaction_time.desc())
        
    #     if not fetch_all:
    #         limit = 30 * (len(wallet_addresses) if wallet_addresses else 1)
    #         base_query = base_query.limit(limit)

    #     # 執行查詢
    #     result = await session.execute(base_query)
    #     rows = result.all()

    #     # 格式化結果
    #     return [{
    #         "transaction": row[0],
    #         "wallet_count_last_hour": row[1],
    #         "buy_count_last_hour": row[2],
    #         "sell_count_last_hour": row[3]
    #     } for row in rows]

    # except Exception as e:
    #     raise RuntimeError(f"查询交易记录时发生错误: {str(e)}")

# async def get_transactions_by_params(
#     session: AsyncSession,
#     chain: str,
#     wallet_addresses: List[str] = None,
#     token_address: str = None,
#     name: str = None,
#     query_string: str = None,
#     fetch_all: bool = False,
#     transaction_type: str = None,
#     filter_token_address: List[str] = None
# ):
#     """
#     優化後的交易查詢函數，支持每個錢包返回指定數量的交易
#     """
#     try:
#         schema = chain.lower()
#         Transaction.with_schema(schema)
#         WalletSummary.with_schema(schema)

#         now_timestamp = int(datetime.now(timezone.utc).timestamp())
#         one_hour_ago_timestamp = now_timestamp - 3600

#         # 構建基礎查詢條件
#         base_conditions = [Transaction.chain == chain]
        
#         if token_address:
#             base_conditions.append(Transaction.token_address == token_address)
#         if name:
#             base_conditions.append(Transaction.token_name == name)
#         if query_string:
#             base_conditions.append(
#                 or_(
#                     Transaction.token_name.ilike(f"%{query_string}%"),
#                     Transaction.token_address.ilike(f"%{query_string}%")
#                 )
#             )
#         if transaction_type:
#             if transaction_type not in ['buy', 'sell']:
#                 raise ValueError("Invalid transaction_type. Must be 'buy' or 'sell'.")
#             base_conditions.append(Transaction.transaction_type == transaction_type)
#         if filter_token_address:
#             base_conditions.append(~Transaction.token_address.in_(filter_token_address))

#         all_transactions = []
#         if fetch_all:
#             # fetch_all 模式：查詢所有符合條件的數據
#             if wallet_addresses:
#                 base_conditions.append(Transaction.wallet_address.in_(wallet_addresses))
#             result = await session.execute(
#                 select(Transaction).where(and_(*base_conditions))
#             )
#             all_transactions = result.scalars().all()
#         else:
#             if wallet_addresses:
#                 # 為每個錢包地址單獨查詢
#                 for wallet in wallet_addresses:
#                     # 先查詢最近一小時的數據
#                     hour_conditions = base_conditions + [
#                         Transaction.wallet_address == wallet,
#                         Transaction.transaction_time >= one_hour_ago_timestamp
#                     ]
                    
#                     result = await session.execute(
#                         select(Transaction)
#                         .where(and_(*hour_conditions))
#                         .order_by(Transaction.transaction_time.desc())
#                         .limit(30)
#                     )
#                     hour_transactions = result.scalars().all()
                    
#                     if hour_transactions:
#                         all_transactions.extend(hour_transactions)
#                     else:
#                         # 如果一小時內沒有數據，查詢最新的30筆
#                         fallback_conditions = base_conditions + [Transaction.wallet_address == wallet]
#                         result = await session.execute(
#                             select(Transaction)
#                             .where(and_(*fallback_conditions))
#                             .order_by(Transaction.transaction_time.desc())
#                             .limit(30)
#                         )
#                         all_transactions.extend(result.scalars().all())
#             else:
#                 # 如果沒有指定錢包地址，使用智能錢包
#                 smart_wallets_result = await session.execute(
#                     select(WalletSummary.address)
#                     .where(
#                         and_(
#                             WalletSummary.chain == chain,
#                             WalletSummary.is_smart_wallet == True
#                         )
#                     )
#                 )
#                 smart_wallet_addresses = [row[0] for row in smart_wallets_result]
                
#                 base_conditions.append(Transaction.wallet_address.in_(smart_wallet_addresses))
#                 hour_conditions = base_conditions + [Transaction.transaction_time >= one_hour_ago_timestamp]
                
#                 result = await session.execute(
#                     select(Transaction)
#                     .where(and_(*hour_conditions))
#                     .order_by(Transaction.transaction_time.desc())
#                     .limit(30)
#                 )
#                 hour_transactions = result.scalars().all()
                
#                 if hour_transactions:
#                     all_transactions = hour_transactions
#                 else:
#                     result = await session.execute(
#                         select(Transaction)
#                         .where(and_(*base_conditions))
#                         .order_by(Transaction.transaction_time.desc())
#                         .limit(30)
#                     )
#                     all_transactions = result.scalars().all()

#         if not all_transactions:
#             return []

#         # 獲取統計數據
#         token_addresses = {tx.token_address for tx in all_transactions}
#         stats_query = (
#             select(
#                 Transaction.token_address,
#                 func.count(distinct(Transaction.wallet_address)).label('wallet_count'),
#                 func.count(case((Transaction.transaction_type == 'buy', 1))).label('buy_count'),
#                 func.count(case((Transaction.transaction_type == 'sell', 1))).label('sell_count')
#             )
#             .where(
#                 and_(
#                     Transaction.chain == chain,
#                     Transaction.transaction_time >= one_hour_ago_timestamp,
#                     Transaction.token_address.in_(token_addresses)
#                 )
#             )
#             .group_by(Transaction.token_address)
#         )

#         stats_result = await session.execute(stats_query)
#         stats_data = {
#             row.token_address: {
#                 'wallet_count': row.wallet_count,
#                 'buy_count': row.buy_count,
#                 'sell_count': row.sell_count
#             }
#             for row in stats_result
#         }

#         # 設定默認值
#         default_stats = {'wallet_count': 0, 'buy_count': 0, 'sell_count': 0}

#         # 組合結果並按時間排序
#         sorted_transactions = sorted(all_transactions, key=lambda x: x.transaction_time, reverse=True)
#         return [{
#             "transaction": tx,
#             "wallet_count_last_hour": stats_data.get(tx.token_address, default_stats)['wallet_count'],
#             "buy_count_last_hour": stats_data.get(tx.token_address, default_stats)['buy_count'],
#             "sell_count_last_hour": stats_data.get(tx.token_address, default_stats)['sell_count']
#         } for tx in sorted_transactions]

#     except Exception as e:
#         raise RuntimeError(f"查询交易记录时发生错误: {str(e)}")
# -------------------------------------------------------------------------------------------------------
async def get_wallet_transactions(
    session_factory,
    wallet: str,
    chain: str,
    schema: str,
    base_conditions: list,
    one_hour_ago_timestamp: int
) -> List[Transaction]:
    """單個錢包的交易查詢，使用獨立的session"""
    async with session_factory() as session:
        Transaction.with_schema(schema)
        # 先查詢最近一小時的數據
        hour_conditions = base_conditions + [
            Transaction.wallet_address == wallet,
            Transaction.transaction_time >= one_hour_ago_timestamp
        ]
        
        result = await session.execute(
            select(Transaction)
            .where(and_(*hour_conditions))
            .order_by(Transaction.transaction_time.desc())
            .limit(30)
        )
        hour_transactions = result.scalars().all()
        
        if hour_transactions:
            return hour_transactions
        
        # 如果一小時內沒有數據，查詢最新的30筆
        fallback_conditions = base_conditions + [Transaction.wallet_address == wallet]
        result = await session.execute(
            select(Transaction)
            .where(and_(*fallback_conditions))
            .order_by(Transaction.transaction_time.desc())
            .limit(30)
        )
        return result.scalars().all()

# async def get_transactions_by_params(
#     session: AsyncSession,
#     chain: str,
#     wallet_addresses: List[str] = None,
#     token_address: str = None,
#     name: str = None,
#     query_string: str = None,
#     fetch_all: bool = False,
#     transaction_type: str = None,
#     filter_token_address: List[str] = None
# ):
#     """
#     優化後的交易查詢函數，使用安全的session處理
#     """
#     try:
#         schema = chain.lower()
#         Transaction.with_schema(schema)
#         WalletSummary.with_schema(schema)

#         now_timestamp = int(datetime.now(timezone.utc).timestamp())
#         one_hour_ago_timestamp = now_timestamp - 3600

#         # 構建基礎查詢條件
#         base_conditions = [Transaction.chain == chain]
        
#         if token_address:
#             base_conditions.append(Transaction.token_address == token_address)
#         if name:
#             base_conditions.append(Transaction.token_name == name)
#         if query_string:
#             base_conditions.append(
#                 or_(
#                     Transaction.token_name.ilike(f"%{query_string}%"),
#                     Transaction.token_address.ilike(f"%{query_string}%")
#                 )
#             )
#         if transaction_type:
#             if transaction_type not in ['buy', 'sell']:
#                 raise ValueError("Invalid transaction_type. Must be 'buy' or 'sell'.")
#             base_conditions.append(Transaction.transaction_type == transaction_type)
#         if filter_token_address:
#             base_conditions.append(~Transaction.token_address.in_(filter_token_address))

#         # 獲取當前鏈的 session factory
#         session_factory = sessions.get(chain.upper())
#         if not session_factory:
#             raise ValueError(f"無效的鏈類型: {chain}")

#         if fetch_all:
#             # fetch_all 模式：查詢所有符合條件的數據
#             if wallet_addresses:
#                 base_conditions.append(Transaction.wallet_address.in_(wallet_addresses))
#             result = await session.execute(
#                 select(Transaction).where(and_(*base_conditions))
#             )
#             all_transactions = result.scalars().all()
#         else:
#             if wallet_addresses:
#                 # 並行查詢所有錢包的交易，每個查詢使用獨立的session
#                 wallet_results = await gather(*[
#                     get_wallet_transactions(
#                         session_factory,
#                         wallet,
#                         chain,
#                         schema,
#                         base_conditions.copy(),  # 使用 copy 避免條件互相影響
#                         one_hour_ago_timestamp
#                     )
#                     for wallet in wallet_addresses
#                 ])
#                 all_transactions = [tx for sublist in wallet_results for tx in sublist if sublist]
#             else:
#                 # 如果沒有指定錢包地址，使用智能錢包
#                 smart_wallets_result = await session.execute(
#                     select(WalletSummary.address)
#                     .where(
#                         and_(
#                             WalletSummary.chain == chain,
#                             WalletSummary.is_smart_wallet == True
#                         )
#                     )
#                 )
#                 smart_wallet_addresses = [row[0] for row in smart_wallets_result]
#                 print(smart_wallet_addresses)
                
#                 base_conditions.append(Transaction.wallet_address.in_(smart_wallet_addresses))
#                 hour_conditions = base_conditions + [Transaction.transaction_time >= one_hour_ago_timestamp]
                
#                 result = await session.execute(
#                     select(Transaction)
#                     .where(and_(*hour_conditions))
#                     .order_by(Transaction.transaction_time.desc())
#                     .limit(30)
#                 )
#                 hour_transactions = result.scalars().all()
                
#                 if hour_transactions:
#                     all_transactions = hour_transactions
#                 else:
#                     result = await session.execute(
#                         select(Transaction)
#                         .where(and_(*base_conditions))
#                         .order_by(Transaction.transaction_time.desc())
#                         .limit(30)
#                     )
#                     all_transactions = result.scalars().all()

#         if not all_transactions:
#             return []

#         # 獲取統計數據
#         token_addresses = {tx.token_address for tx in all_transactions}
#         stats_query = (
#             select(
#                 Transaction.token_address,
#                 func.count(distinct(Transaction.wallet_address)).label('wallet_count'),
#                 func.count(case((Transaction.transaction_type == 'buy', 1))).label('buy_count'),
#                 func.count(case((Transaction.transaction_type == 'sell', 1))).label('sell_count')
#             )
#             .where(
#                 and_(
#                     Transaction.chain == chain,
#                     Transaction.transaction_time >= one_hour_ago_timestamp,
#                     Transaction.token_address.in_(token_addresses)
#                 )
#             )
#             .group_by(Transaction.token_address)
#         )

#         stats_result = await session.execute(stats_query)
#         stats_data = {
#             row.token_address: {
#                 'wallet_count': row.wallet_count,
#                 'buy_count': row.buy_count,
#                 'sell_count': row.sell_count
#             }
#             for row in stats_result
#         }

#         # 設定默認值
#         default_stats = {'wallet_count': 0, 'buy_count': 0, 'sell_count': 0}

#         # 組合結果並按時間排序
#         sorted_transactions = sorted(all_transactions, key=lambda x: x.transaction_time, reverse=True)
#         return [{
#             "transaction": tx,
#             "wallet_count_last_hour": stats_data.get(tx.token_address, default_stats)['wallet_count'],
#             "buy_count_last_hour": stats_data.get(tx.token_address, default_stats)['buy_count'],
#             "sell_count_last_hour": stats_data.get(tx.token_address, default_stats)['sell_count']
#         } for tx in sorted_transactions]

#     except Exception as e:
#         raise RuntimeError(f"查询交易记录时发生错误: {str(e)}")

# async def get_transactions_by_params(
#     session: AsyncSession,
#     chain: str,
#     wallet_addresses: List[str] = None,
#     token_address: str = None,
#     name: str = None,
#     query_string: str = None,
#     fetch_all: bool = False,
#     transaction_type: str = None,
#     filter_token_address: List[str] = None,
#     page: int = 1,
#     page_size: int = 30
# ) -> Dict:
#     """
#     優化後的交易查詢函數，支持分頁和高效查詢
#     """
#     try:
#         schema = chain.lower()
#         Transaction.with_schema(schema)
#         WalletSummary.with_schema(schema)

#         now_timestamp = int(datetime.now(timezone.utc).timestamp())
#         one_hour_ago_timestamp = now_timestamp - 3600

#         # 使用 CTE 優化查詢
#         smart_wallets_cte = (
#             select(WalletSummary.address)
#             .where(
#                 and_(
#                     WalletSummary.chain == chain,
#                     WalletSummary.is_smart_wallet == True
#                 )
#             )
#             .cte('smart_wallets')
#         )

#         # 基礎條件
#         base_conditions = [Transaction.chain == chain]
        
#         # 添加各種過濾條件
#         if token_address:
#             base_conditions.append(Transaction.token_address == token_address)
#         if name:
#             base_conditions.append(Transaction.token_name == name)
#         if query_string:
#             base_conditions.append(
#                 or_(
#                     Transaction.token_name.ilike(f"%{query_string}%"),
#                     Transaction.token_address.ilike(f"%{query_string}%")
#                 )
#             )
#         if transaction_type:
#             if transaction_type not in ['buy', 'sell']:
#                 raise ValueError("Invalid transaction_type. Must be 'buy' or 'sell'.")
#             base_conditions.append(Transaction.transaction_type == transaction_type)
#         if filter_token_address:
#             base_conditions.append(~Transaction.token_address.in_(filter_token_address))

#         # 處理錢包地址條件
#         if wallet_addresses:
#             base_conditions.append(Transaction.wallet_address.in_(wallet_addresses))
#         elif not any([token_address, name, query_string, fetch_all, transaction_type]):
#             base_conditions.append(
#                 Transaction.wallet_address.in_(
#                     select(smart_wallets_cte.c.address)
#                 )
#             )

#         # 先獲取總記錄數
#         count_query = (
#             select(func.count())
#             .select_from(Transaction)
#             .where(and_(*base_conditions))
#         )
#         total_count = await session.execute(count_query)
#         total = total_count.scalar()

#         # 主查詢
#         main_query = (
#             select(Transaction)
#             .where(and_(*base_conditions))
#             .order_by(Transaction.transaction_time.desc())
#         )

#         # 應用分頁
#         if not fetch_all:
#             offset = (page - 1) * page_size
#             main_query = main_query.offset(offset).limit(page_size)

#         transactions_result = await session.execute(main_query)
#         transactions = transactions_result.scalars().all()

#         if not transactions:
#             return {"transactions": [], "total": 0}

#         # 獲取統計數據
#         token_addresses = {tx.token_address for tx in transactions}
#         stats_query = (
#             select(
#                 Transaction.token_address,
#                 func.count(distinct(Transaction.wallet_address)).label('wallet_count'),
#                 func.count(case((Transaction.transaction_type == 'buy', 1))).label('buy_count'),
#                 func.count(case((Transaction.transaction_type == 'sell', 1))).label('sell_count')
#             )
#             .where(
#                 and_(
#                     Transaction.chain == chain,
#                     Transaction.transaction_time >= one_hour_ago_timestamp,
#                     Transaction.token_address.in_(token_addresses)
#                 )
#             )
#             .group_by(Transaction.token_address)
#         )

#         stats_result = await session.execute(stats_query)
#         stats_data = {
#             row.token_address: {
#                 'wallet_count': row.wallet_count,
#                 'buy_count': row.buy_count,
#                 'sell_count': row.sell_count
#             }
#             for row in stats_result
#         }

#         # 設定默認值
#         default_stats = {'wallet_count': 0, 'buy_count': 0, 'sell_count': 0}

#         # 組合結果
#         result = [{
#             "transaction": tx,
#             "wallet_count_last_hour": stats_data.get(tx.token_address, default_stats)['wallet_count'],
#             "buy_count_last_hour": stats_data.get(tx.token_address, default_stats)['buy_count'],
#             "sell_count_last_hour": stats_data.get(tx.token_address, default_stats)['sell_count']
#         } for tx in transactions]

#         return {
#             "transactions": result,
#             "total": total
#         }

#     except Exception as e:
#         logging.error(f"查询交易记录时发生错误: {str(e)}")
#         raise RuntimeError(f"查询交易记录时发生错误: {str(e)}")

async def get_transactions_by_params(
    session: AsyncSession,
    chain: str,
    wallet_addresses: List[str] = None,
    token_address: str = None,
    name: str = None,
    query_string: str = None,
    fetch_all: bool = False,
    transaction_type: str = None,
    filter_token_address: List[str] = None,
    page: int = 1,
    page_size: int = 30
) -> Dict:
    """優化後的交易查詢函數"""
    try:
        schema = chain.lower()
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)

        now_timestamp = int(datetime.now(timezone.utc).timestamp())
        one_hour_ago_timestamp = now_timestamp - 3600

        # 使用 CTE 優化查詢
        smart_wallets_cte = (
            select(WalletSummary.address)
            .where(
                and_(
                    WalletSummary.chain == chain,
                    WalletSummary.is_smart_wallet == True
                )
            )
            .cte('smart_wallets')
        )

        # 基礎條件
        base_conditions = [Transaction.chain == chain]
        
        # 添加過濾條件
        if token_address:
            base_conditions.append(Transaction.token_address == token_address)
        if name:
            base_conditions.append(Transaction.token_name == name)
        if query_string:
            base_conditions.append(
                or_(
                    Transaction.token_name.ilike(f"%{query_string}%"),
                    Transaction.token_address.ilike(f"%{query_string}%")
                )
            )
        if transaction_type:
            if transaction_type not in ['buy', 'sell']:
                raise ValueError("Invalid transaction_type. Must be 'buy' or 'sell'.")
            base_conditions.append(Transaction.transaction_type == transaction_type)
        if filter_token_address:
            base_conditions.append(~Transaction.token_address.in_(filter_token_address))

        # 處理錢包地址條件
        if wallet_addresses:
            base_conditions.append(Transaction.wallet_address.in_(wallet_addresses))
        elif not any([token_address, name, query_string, fetch_all, transaction_type]):
            base_conditions.append(
                Transaction.wallet_address.in_(
                    select(smart_wallets_cte.c.address)
                )
            )

        # 獲取總記錄數
        count_query = (
            select(func.count())
            .select_from(Transaction)
            .where(and_(*base_conditions))
        )
        total_count = await session.execute(count_query)
        total = total_count.scalar()

        # 主查詢
        main_query = (
            select(Transaction)
            .where(and_(*base_conditions))
            .order_by(Transaction.transaction_time.desc())
        )

        # 應用分頁
        if not fetch_all:
            offset = (page - 1) * page_size
            main_query = main_query.offset(offset).limit(page_size)

        transactions_result = await session.execute(main_query)
        transactions = transactions_result.scalars().all()
        print(transactions)

        if not transactions:
            return {"transactions": [], "total": 0}

        # 獲取統計數據
        token_addresses = {tx.token_address for tx in transactions}
        stats_query = (
            select(
                Transaction.token_address,
                func.count(distinct(Transaction.wallet_address)).label('wallet_count'),
                func.count(case((Transaction.transaction_type == 'buy', 1))).label('buy_count'),
                func.count(case((Transaction.transaction_type == 'sell', 1))).label('sell_count')
            )
            .where(
                and_(
                    Transaction.chain == chain,
                    Transaction.transaction_time >= one_hour_ago_timestamp,
                    Transaction.token_address.in_(token_addresses)
                )
            )
            .group_by(Transaction.token_address)
        )

        stats_result = await session.execute(stats_query)
        stats_data = {
            row.token_address: {
                'wallet_count': row.wallet_count,
                'buy_count': row.buy_count,
                'sell_count': row.sell_count
            }
            for row in stats_result
        }

        # 設定默認值
        default_stats = {'wallet_count': 0, 'buy_count': 0, 'sell_count': 0}
        print("YY")
        print(stats_data)
        # 組合結果
        result = [{
            "transaction": tx,
            "wallet_count_last_hour": stats_data.get(tx.token_address, default_stats)['wallet_count'],
            "buy_count_last_hour": stats_data.get(tx.token_address, default_stats)['buy_count'],
            "sell_count_last_hour": stats_data.get(tx.token_address, default_stats)['sell_count']
        } for tx in transactions]
        print(result)

        return {
            "transactions": result,
            "total": total
        }

    except Exception as e:
        logging.error(f"查询交易记录时发生错误: {str(e)}")
        raise RuntimeError(f"查询交易记录时发生错误: {str(e)}")

async def get_latest_transactions(session: AsyncSession, chain: str, limit: int = 30):
    """
    查詢最新的交易數據
    :param session: 資料庫會話
    :param chain: 區塊鏈名稱
    :param limit: 返回的交易數量限制
    :return: 最新交易的列表
    """
    try:
        schema = chain.lower()
        Transaction.with_schema(schema)
        query = (
            select(Transaction)
            .where(Transaction.chain == chain)
            .order_by(Transaction.transaction_time.desc())
            .limit(limit)
        )
        result = await session.execute(query)
        return result.scalars().all()
    except Exception as e:
        logging.error(f"查詢最新交易數據失敗: {e}")
        raise RuntimeError(f"查詢最新交易數據失敗: {str(e)}")
    
async def get_transactions_for_wallet(session: AsyncSession, chain: str, wallet_address: str, days: int = 90):
    """
    查詢指定 wallet_address 在過去指定天數內的交易記錄。
    :param session: 資料庫會話
    :param chain: 區塊鏈名稱
    :param wallet_address: 要查詢的錢包地址
    :param days: 查詢的天數範圍，預設為 90 天
    :return: 符合條件的交易列表，每條記錄以字典形式返回。
    """
    try:
        # 計算 90 天前的時間戳
        cutoff_time = int((datetime.utcnow() - timedelta(days=days)).timestamp())

        # 設置使用的 schema
        schema = chain.lower()
        Transaction.with_schema(schema)

        # 構建查詢
        query = (
            select(Transaction)
            .where(
                Transaction.chain == chain,
                Transaction.wallet_address == wallet_address,
                Transaction.transaction_time >= cutoff_time
            )
            .order_by(Transaction.transaction_time.asc())  # 按照 transaction_time 從最舊到最新排序
        )

        # 執行查詢
        result = await session.execute(query)
        transactions = result.scalars().all()

        # 將交易記錄轉換為字典列表
        return [
            {
                "id": tx.id,
                "wallet_address": tx.wallet_address,
                "token_address": tx.token_address,
                "token_icon": tx.token_icon,
                "token_name": tx.token_name,
                "price": tx.price,
                "amount": tx.amount,
                "marketcap": tx.marketcap,
                "value": tx.value,
                "holding_percentage": tx.holding_percentage,
                "chain": tx.chain,
                "realized_profit": tx.realized_profit,
                "realized_profit_percentage": tx.realized_profit_percentage,
                "transaction_type": tx.transaction_type,
                "transaction_time": tx.transaction_time,
                "time": tx.time,
                "signature": tx.signature
            }
            for tx in transactions
        ]
    except Exception as e:
        logging.error(f"查詢錢包 {wallet_address} 的交易記錄失敗: {e}")
        raise RuntimeError(f"查詢錢包 {wallet_address} 的交易記錄失敗: {str(e)}")

async def enrich_transactions(session, transactions, chain, now_timestamp, one_hour_ago_timestamp):
    """
    按照每個 token_address 統計查詢當下時間往前推一小時內的所有交易情況
    """
    enriched_transactions = []
    
    for transaction in transactions:
        # 針對當前交易的 token_address 進行統計
        wallet_count_query = (
            select(func.count(func.distinct(Transaction.wallet_address)))
            .where(Transaction.chain == chain)
            .where(Transaction.token_address == transaction.token_address)
            .where(Transaction.transaction_time >= one_hour_ago_timestamp)
            .where(Transaction.transaction_time <= now_timestamp)
        )
        wallet_count_result = await session.execute(wallet_count_query)
        wallet_count_last_hour = wallet_count_result.scalar() or 0  # 如果沒有數據返回0

        buy_count_query = (
            select(func.count(Transaction.id))
            .where(Transaction.chain == chain)
            .where(Transaction.token_address == transaction.token_address)
            .where(Transaction.transaction_time >= one_hour_ago_timestamp)
            .where(Transaction.transaction_time <= now_timestamp)
            .where(Transaction.transaction_type == "buy")
        )
        buy_count_result = await session.execute(buy_count_query)
        buy_count_last_hour = buy_count_result.scalar() or 0  # 如果沒有數據返回0

        sell_count_query = (
            select(func.count(Transaction.id))
            .where(Transaction.chain == chain)
            .where(Transaction.token_address == transaction.token_address)
            .where(Transaction.transaction_time >= one_hour_ago_timestamp)
            .where(Transaction.transaction_time <= now_timestamp)
            .where(Transaction.transaction_type == "sell")
        )
        sell_count_result = await session.execute(sell_count_query)
        sell_count_last_hour = sell_count_result.scalar() or 0  # 如果沒有數據返回0

        enriched_transactions.append({
            "transaction": transaction,
            "wallet_count_last_hour": wallet_count_last_hour,
            "buy_count_last_hour": buy_count_last_hour,
            "sell_count_last_hour": sell_count_last_hour
        })

    return enriched_transactions

async def get_token_trend_data(session, token_addresses, chain, time_range):
    """
    查詢資料庫中的代幣趨勢數據，並分別返回每個代幣的數據
    """
    try:
        schema = chain.lower()
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)
        all_trends = []

        # 計算時間範圍，根據傳入的時間參數生成 datetime 對象
        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=time_range)
        time_threshold_timestamp = int(time_threshold.timestamp())  # 转换为时间戳（单位：秒）
        now_timestamp = int(datetime.now(timezone.utc).timestamp())

        # 1. 查詢所有相關交易數據，加入 time_range 及限制最多返回 30 筆資料
        transactions_query = select(Transaction).where(
            Transaction.token_address.in_(token_addresses),
            Transaction.chain == chain,
            Transaction.transaction_time >= time_threshold_timestamp,  # 时间 >= 起始时间戳
            Transaction.transaction_time <= now_timestamp,  # 时间 <= 当前时间戳
            Transaction.wallet_address.in_(
                select(WalletSummary.address).where(WalletSummary.is_smart_wallet == True)
            )
        ).order_by(Transaction.transaction_time.desc()).limit(30)  # 限制最多返回30筆資料

        transactions_result = await session.execute(transactions_query)
        transactions = transactions_result.scalars().all()
        # 2. 查詢所有相關 wallet 資料
        wallet_addresses = list(set([tx.wallet_address for tx in transactions]))
        wallet_query = select(WalletSummary.address, WalletSummary.asset_multiple, WalletSummary.wallet_type).where(
            WalletSummary.address.in_(wallet_addresses)
        )
        wallet_data_result = await session.execute(wallet_query)
        wallet_data = {entry[0]: {"asset_multiple": entry[1], "wallet_type": entry[2]} for entry in wallet_data_result.fetchall()}

        # 3. 按照 token_address 分組交易數據
        grouped_transactions = {}
        for tx in transactions:
            if tx.token_address not in grouped_transactions:
                grouped_transactions[tx.token_address] = {'buy': [], 'sell': []}
            if tx.transaction_type == 'buy':
                grouped_transactions[tx.token_address]['buy'].append(tx)
            else:
                grouped_transactions[tx.token_address]['sell'].append(tx)

        # 4. 遍歷每個 token_address，計算並組織資料
        for token_address, tx_data in grouped_transactions.items():
            buy_transactions = tx_data['buy']
            sell_transactions = tx_data['sell']
            is_pump = token_address.endswith("pump")

            # 計算買賣地址數量
            buy_addr_amount = len(set(tx.wallet_address for tx in buy_transactions))
            sell_addr_amount = len(set(tx.wallet_address for tx in sell_transactions))
            total_addr_amount = buy_addr_amount + sell_addr_amount

            token_name = buy_transactions[0].token_name if buy_transactions else (sell_transactions[0].token_name if sell_transactions else "")

            # 統計買入交易數據
            buy_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_buy_vol": tx.amount,
                    "wallet_buy_marketcap": tx.marketcap,
                    "wallet_buy_usd": tx.value,
                    "wallet_buy_holding": tx.holding_percentage,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in buy_transactions
            ]

            # 統計賣出交易數據
            sell_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_sell_vol": tx.amount,
                    "wallet_sell_marketcap": tx.marketcap,
                    "wallet_sell_usd": tx.value,
                    "wallet_sell_pnl": tx.realized_profit or 0,
                    "wallet_sell_pnl_percentage": tx.realized_profit_percentage or 0,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in sell_transactions
            ]

            # 將結果加入到總結果列表
            all_trends.append({
                "buy_addrAmount": buy_addr_amount,
                "sell_addrAmount": sell_addr_amount,
                "total_addr_amount": total_addr_amount,
                "token_name": token_name,
                "token_address": token_address,
                "chain": chain,
                "is_pump": is_pump,
                "time": datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
                "buy": buy_data,
                "sell": sell_data,
            })

        # 返回所有代幣的趨勢數據
        return all_trends

    except Exception as e:
        print(f"查詢代幣趨勢數據失敗，原因 {e}")
        # await log_error(
        #     session,
        #     str(e),
        #     "models",
        #     "get_token_trend_data",
        #     f"查詢代幣趨勢數據失敗，原因 {e}"
        # )
        return None

async def get_token_trend_data_allchain(session, token_addresses, chain, time_range):
    """
    查詢資料庫中的代幣趨勢數據，並分別返回每個代幣的數據
    """
    try:
        schema = chain.lower()
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)
        all_trends = []

        # 計算時間範圍，根據傳入的時間參數生成 datetime 對象
        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=time_range)
        time_threshold_timestamp = int(time_threshold.timestamp())
        now_timestamp = int(datetime.now(timezone.utc).timestamp())

        # 1. 查詢所有相關交易數據，加入 time_range 及限制最多返回 30 筆資料
        transactions_query = select(Transaction).where(
            Transaction.token_address.in_(token_addresses),
            Transaction.chain == chain,
            Transaction.transaction_time >= time_threshold_timestamp,
            Transaction.transaction_time <= now_timestamp,
            Transaction.wallet_address.in_(
                select(WalletSummary.address).where(WalletSummary.is_smart_wallet == True)
            )
        ).order_by(Transaction.transaction_time.desc()).limit(30)

        transactions_result = await session.execute(transactions_query)
        transactions = transactions_result.scalars().all()

        # 如果該chain沒有找到任何交易，直接返回空列表
        if not transactions:
            return []

        # 2. 查詢所有相關 wallet 資料
        wallet_addresses = list(set([tx.wallet_address for tx in transactions]))
        wallet_query = select(WalletSummary.address, WalletSummary.asset_multiple, WalletSummary.wallet_type).where(
            WalletSummary.address.in_(wallet_addresses)
        )
        wallet_data_result = await session.execute(wallet_query)
        wallet_data = {entry[0]: {"asset_multiple": entry[1], "wallet_type": entry[2]} for entry in wallet_data_result.fetchall()}

        # 3. 按照 token_address 分組交易數據
        grouped_transactions = {}
        for tx in transactions:
            if tx.token_address not in grouped_transactions:
                grouped_transactions[tx.token_address] = {'buy': [], 'sell': []}
            if tx.transaction_type == 'buy':
                grouped_transactions[tx.token_address]['buy'].append(tx)
            else:
                grouped_transactions[tx.token_address]['sell'].append(tx)

        # 4. 遍歷每個 token_address，計算並組織資料
        for token_address, tx_data in grouped_transactions.items():
            buy_transactions = tx_data['buy']
            sell_transactions = tx_data['sell']
            is_pump = token_address.endswith("pump")

            buy_addr_amount = len(set(tx.wallet_address for tx in buy_transactions))
            sell_addr_amount = len(set(tx.wallet_address for tx in sell_transactions))
            total_addr_amount = buy_addr_amount + sell_addr_amount

            token_name = buy_transactions[0].token_name if buy_transactions else (sell_transactions[0].token_name if sell_transactions else "")

            buy_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_buy_vol": tx.amount,
                    "wallet_buy_marketcap": tx.marketcap,
                    "wallet_buy_usd": tx.value,
                    "wallet_buy_holding": tx.holding_percentage,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in buy_transactions
            ]

            sell_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_sell_vol": tx.amount,
                    "wallet_sell_marketcap": tx.marketcap,
                    "wallet_sell_usd": tx.value,
                    "wallet_sell_pnl": tx.realized_profit or 0,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in sell_transactions
            ]

            all_trends.append({
                "buy_addrAmount": buy_addr_amount,
                "sell_addrAmount": sell_addr_amount,
                "total_addr_amount": total_addr_amount,
                "token_name": token_name,
                "token_address": token_address,
                "chain": chain,
                "is_pump": is_pump,
                "time": datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
                "buy": buy_data,
                "sell": sell_data,
            })

        return all_trends

    except Exception as e:
        print(f"查詢代幣趨勢數據失敗，原因 {e}")
        # await log_error(
        #     session,
        #     str(e),
        #     "models",
        #     "get_token_trend_data",
        #     f"查詢代幣趨勢數據失敗，原因 {e}"
        # )
        return None

# 主程式
if __name__ == '__main__':
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URI_SWAP')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # 初始化資料庫
    asyncio.run(initialize_database())
    # asyncio.run(init_db())