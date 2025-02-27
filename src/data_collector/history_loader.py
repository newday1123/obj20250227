import os
from datetime import datetime
import logging
from typing import Dict, List
from ..database.db_manager import DatabaseManager

class HistoryLoader:
    def __init__(self):
        self.db = DatabaseManager()
        self.data_paths = {
            'daily': 'E:\\ztdatabase\\bendi\\20240213-1',
            '5min': 'E:\\ztdatabase\\bendi\\5min',
            '1min': 'E:\\ztdatabase\\bendi\\1min'
        }
    
    def _parse_stock_code(self, filename: str) -> str:
        """从文件名解析股票代码"""
        # 格式：BJ#/SH#/SZ# + 股票代码 + .txt
        return filename.split('#')[1].split('.')[0]
    
    def _parse_file_header(self, first_line: str) -> Dict:
        """解析文件头部信息"""
        try:
            # 格式：股票代码 股票名称 数据类型 复权类型
            parts = first_line.strip().split()
            return {
                'stock_code': parts[0],
                'stock_name': parts[1],
                'data_type': parts[2],  # "1分钟线"/"5分钟线"/"日线"
                'adjust_type': parts[3]  # "不复权"
            }
        except Exception as e:
            logging.error(f"解析文件头部失败: {first_line}, 错误: {str(e)}")
            return None
    
    def _parse_daily_data(self, lines: List[str]) -> List[Dict]:
        """解析日线数据"""
        data = []
        # 解析文件头部
        header = self._parse_file_header(lines[0])
        if not header:
            return data
        
        # 从第三行开始解析数据（跳过头部信息和列名）
        for line in lines[2:]:
            try:
                parts = line.strip().split('\t')
                if len(parts) != 7:  # 日期、开盘、最高、最低、收盘、成交量、成交额
                    continue
                    
                data.append({
                    'stock_code': header['stock_code'],  # 添加股票代码
                    'stock_name': header['stock_name'],  # 添加股票名称
                    'trade_date': datetime.strptime(parts[0].strip(), '%Y/%m/%d').date(),
                    'open_price': float(parts[1]),
                    'high_price': float(parts[2]),
                    'low_price': float(parts[3]),
                    'close_price': float(parts[4]),
                    'volume': int(float(parts[5])),  # 处理科学计数法
                    'amount': float(parts[6])
                })
            except Exception as e:
                logging.error(f"解析行数据失败: {line}, 错误: {str(e)}")
                continue
        return data
    
    def _parse_min_data(self, lines: List[str]) -> List[Dict]:
        """解析分钟线数据"""
        data = []
        # 解析文件头部
        header = self._parse_file_header(lines[0])
        if not header:
            return data
        
        # 从第三行开始解析数据（跳过头部信息和列名）
        for line in lines[2:]:
            try:
                parts = line.strip().split('\t')
                if len(parts) != 8:  # 日期、时间、开盘、最高、最低、收盘、成交量、成交额
                    continue
                    
                date_str = parts[0].strip()
                time_str = parts[1].strip()
                
                # 将日期和时间转换为字符串格式
                trade_date = datetime.strptime(date_str, '%Y/%m/%d').strftime('%Y-%m-%d')
                trade_time = datetime.strptime(time_str, '%H%M').strftime('%H:%M:00')
                
                data.append({
                    'stock_code': header['stock_code'],
                    'stock_name': header['stock_name'],
                    'trade_date': trade_date,  # 使用字符串格式
                    'trade_time': trade_time,  # 使用字符串格式
                    'open_price': float(parts[2]),
                    'high_price': float(parts[3]),
                    'low_price': float(parts[4]),
                    'close_price': float(parts[5]),
                    'volume': int(float(parts[6])),
                    'amount': float(parts[7])
                })
            except Exception as e:
                logging.error(f"解析行数据失败: {line}, 错误: {str(e)}")
                continue
        return data
    
    def load_history_data(self):
        """加载所有历史数据"""
        # 加载日线数据
        self._load_data_files('daily')
        
        # 加载5分钟数据
        self._load_data_files('5min')
        
        # 加载1分钟数据
        self._load_data_files('1min')
    
    def _load_data_files(self, data_type: str):
        """加载指定类型的数据文件"""
        path = self.data_paths[data_type]
        logging.info(f"开始加载{data_type}数据，路径: {path}")
        
        for filename in os.listdir(path):
            try:
                if not filename.endswith('.txt'):
                    continue
                    
                file_path = os.path.join(path, filename)
                
                with open(file_path, 'r', encoding='gbk') as f:
                    lines = f.readlines()
                    
                if len(lines) < 3:  # 至少需要头部信息、列名和一行数据
                    logging.warning(f"文件内容不完整: {filename}")
                    continue
                    
                if data_type == 'daily':
                    data = self._parse_daily_data(lines)
                    if data:
                        self.db.save_daily_data(data)
                else:
                    data = self._parse_min_data(lines)
                    if data:
                        if data_type == '5min':
                            self.db.save_5min_data(data)
                        else:
                            self.db.save_1min_data(data)
                        
                logging.info(f"成功加载{data_type}数据: {filename}, 数据条数: {len(data)}")
                
            except Exception as e:
                logging.error(f"处理文件失败: {filename}, 错误: {str(e)}")
                continue 