import os
import sqlite3
from contextlib import contextmanager
from typing import Dict, List
import logging

class DatabaseManager:
    def __init__(self, db_name: str = "stock_analysis.db"):
        """初始化数据库管理器"""
        self.db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), db_name)
        logging.info(f"数据库路径: {self.db_path}")
        
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def get_sectors_by_type(self, sector_type: str) -> List[Dict]:
        """获取指定类型的所有板块"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT sector_code, sector_name, stock_count
            FROM stock_sector
            WHERE sector_type = ?
            ''', (sector_type,))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_sector_stocks(self, sector_code: str) -> List[Dict]:
        """获取板块下的所有股票"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT stock_code, stock_name, weight, is_leader
            FROM stock_sector_relation
            WHERE sector_code = ?
            ''', (sector_code,))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def save_sector_info(self, sector_data: Dict):
        """保存板块数据"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # 开始事务
                cursor.execute('BEGIN TRANSACTION')
                
                # 打印调试信息
                logging.debug(f"保存板块数据: {sector_data}")
                
                # 保存板块信息
                cursor.execute('''
                INSERT OR REPLACE INTO stock_sector (
                    sector_code, sector_name, sector_type,
                    stock_count, update_time
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    sector_data['sector_code'],
                    sector_data['sector_name'],
                    sector_data['sector_type'],
                    len(sector_data.get('stocks', []))
                ))
                
                # 删除旧的股票-板块关系
                cursor.execute('DELETE FROM stock_sector_relation WHERE sector_code = ?',
                             (sector_data['sector_code'],))
                
                # 准备股票数据
                stock_values = []
                for stock in sector_data.get('stocks', []):
                    # 打印调试信息
                    logging.debug(f"处理股票数据: {stock}")
                    
                    if not stock.get('stock_code') or not stock.get('stock_name'):
                        logging.warning(f"跳过无效股票数据: {stock}")
                        continue
                        
                    stock_values.append((
                        stock['stock_code'],
                        stock['stock_name'],
                        sector_data['sector_code'],
                        sector_data['sector_type'],
                        stock.get('weight', 1.0),
                        stock.get('is_leader', 0)
                    ))
                
                # 批量插入股票数据
                if stock_values:
                    logging.debug(f"插入 {len(stock_values)} 条股票数据")
                    cursor.executemany('''
                    INSERT INTO stock_sector_relation (
                        stock_code, stock_name, sector_code,
                        sector_type, weight, is_leader
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', stock_values)
                
                # 提交事务
                conn.commit()
                logging.info(f"成功保存板块数据: {sector_data['sector_name']}")
                return True
                
            except Exception as e:
                conn.rollback()
                logging.error(f"保存板块数据失败: {sector_data.get('sector_code')}")
                logging.error(f"错误类型: {type(e).__name__}")
                logging.error(f"错误信息: {str(e)}")
                raise