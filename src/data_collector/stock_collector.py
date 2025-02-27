from datetime import datetime
from .memory_reader import MemoryReader
from ..database.db_manager import DatabaseManager

class StockCollector:
    def __init__(self):
        self.memory_reader = MemoryReader()
        self.db = DatabaseManager()
        
    def start_collecting(self):
        try:
            self.memory_reader.open_process()
            
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # 收集前80行数据
                for row in range(80):
                    stock_data = self.memory_reader.get_stock_data(row)
                    self._save_to_database(cursor, stock_data)
                    
                conn.commit()
                    
        except Exception as e:
            print(f"数据收集错误: {str(e)}")
            
    def _save_to_database(self, cursor, stock_data):
        # 计算涨跌幅等数据
        change_amount = stock_data['current'] - stock_data['prev_close']
        change_percent = (change_amount / stock_data['prev_close']) * 100
        
        # 更新实时数据
        stock_data['change_percent'] = change_percent
        stock_data['change_amount'] = change_amount
        self.db.update_stock_realtime(stock_data)
        
        # 检查是否涨停
        if change_percent >= 9.9:  # 可以根据不同市场调整涨停判断标准
            self.db.update_limit_up(
                stock_data['code'],
                stock_data.get('name', ''),
                change_percent
            )
        
        cursor.execute('''
        INSERT INTO stock_realtime (
            stock_code, current_price, open_price, high_price, 
            low_price, volume, prev_close, change_percent, 
            change_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            stock_data['code'],
            stock_data['current'],
            stock_data['open'],
            stock_data['high'],
            stock_data['low'],
            stock_data['volume'],
            stock_data['prev_close'],
            change_percent,
            change_amount
        )) 