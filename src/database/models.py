from dataclasses import dataclass
from datetime import datetime
import sqlite3
import os
import logging

@dataclass
class StockRealtime:
    id: int
    stock_code: str
    stock_name: str
    current_price: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    turnover: float
    prev_close: float
    change_percent: float
    change_amount: float
    turnover_rate: float
    speed: float
    main_force_net: float
    timestamp: datetime

def init_database():
    """初始化数据库表结构"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'stock_analysis.db')
    logging.info(f"初始化数据库: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 创建实时数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_realtime (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT NOT NULL,
        stock_name TEXT,
        current_price REAL,
        open_price REAL,
        high_price REAL,
        low_price REAL,
        close_price REAL,
        volume INTEGER,
        turnover REAL,
        prev_close REAL,
        change_percent REAL,
        change_amount REAL,
        turnover_rate REAL,
        speed REAL,
        main_force_net REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 创建涨停板数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_limit_up (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT NOT NULL,
        stock_name TEXT,
        change_percent REAL,
        continuous_limit_up INTEGER,    -- 连板数
        first_limit_up_time TEXT,       -- 首次涨停时间
        break_limit_up_times INTEGER,   -- 打板次数
        rebound_limit_up INTEGER,       -- 是否反包涨停
        date DATE,
        UNIQUE(stock_code, date)
    )''')
    
    # 创建股票代码映射表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_info (
        stock_code TEXT PRIMARY KEY,
        stock_name TEXT NOT NULL,
        market_type TEXT,    -- 主板/创业板/科创板
        industry TEXT,       -- 所属行业
        total_share REAL,    -- 总股本
        float_share REAL     -- 流通股本
    )''')
    
    # 创建历史数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code TEXT NOT NULL,
        trade_date DATE NOT NULL,
        open_price REAL,
        high_price REAL,
        low_price REAL,
        close_price REAL,
        pre_close REAL,
        change_amount REAL,
        change_percent REAL,
        volume INTEGER,
        amount REAL,          -- 成交额
        turnover_rate REAL,   -- 换手率
        total_market REAL,    -- 总市值
        circulate_market REAL,-- 流通市值
        UNIQUE(stock_code, trade_date)
    )''')
    
    # 创建板块表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_sector (
        sector_code TEXT PRIMARY KEY,
        sector_name TEXT NOT NULL,
        sector_type TEXT NOT NULL,
        stock_count INTEGER DEFAULT 0,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 创建股票-板块关系表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_sector_relation (
        stock_code TEXT NOT NULL,
        stock_name TEXT NOT NULL,
        sector_code TEXT NOT NULL,
        sector_type TEXT NOT NULL,
        weight REAL DEFAULT 1.0,
        is_leader INTEGER DEFAULT 0,
        PRIMARY KEY (stock_code, sector_code),
        FOREIGN KEY (sector_code) REFERENCES stock_sector(sector_code)
    )''')
    
    # 创建板块类型表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sector_type (
        type_code TEXT PRIMARY KEY,
        type_name TEXT NOT NULL,
        description TEXT
    )''')
    
    # 插入板块类型数据
    cursor.executemany('''
    INSERT OR REPLACE INTO sector_type (type_code, type_name, description)
    VALUES (?, ?, ?)
    ''', [
        ('region', '地区板块', '按照地理位置划分的板块'),
        ('style', '风格板块', '按照股票特征划分的板块'),
        ('concept', '概念板块', '按照概念题材划分的板块'),
        ('industry', '行业板块', '按照行业属性划分的板块'),
        ('index', '指数板块', '各类股票指数')
    ])
    
    # 创建日线数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_daily (
        stock_code TEXT NOT NULL,
        stock_name TEXT NOT NULL,
        trade_date DATE NOT NULL,
        open_price REAL,
        high_price REAL,
        low_price REAL,
        close_price REAL,
        volume INTEGER,
        amount REAL,
        PRIMARY KEY (stock_code, trade_date)
    )''')
    
    # 创建5分钟数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_5min (
        stock_code TEXT NOT NULL,
        stock_name TEXT NOT NULL,
        trade_date DATE NOT NULL,
        trade_time TIME NOT NULL,
        open_price REAL,
        high_price REAL,
        low_price REAL,
        close_price REAL,
        volume INTEGER,
        amount REAL,
        PRIMARY KEY (stock_code, trade_date, trade_time)
    )''')
    
    # 创建1分钟数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_1min (
        stock_code TEXT NOT NULL,
        stock_name TEXT NOT NULL,
        trade_date DATE NOT NULL,
        trade_time TIME NOT NULL,
        open_price REAL,
        high_price REAL,
        low_price REAL,
        close_price REAL,
        volume INTEGER,
        amount REAL,
        PRIMARY KEY (stock_code, trade_date, trade_time)
    )''')
    
    conn.commit()
    conn.close() 