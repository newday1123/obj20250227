import os
from typing import Dict, List, Tuple
from src.database.db_manager import DatabaseManager
import logging

class SectorLoader:
    def __init__(self, data_path: str = "E:\\ztdatabase\\bankuaiDATA"):
        self.data_path = data_path
        self.db = DatabaseManager()
        self.sector_type_map = {
            "地区板块.txt": "region",
            "风格板块.txt": "style",
            "概念板块.txt": "concept",
            "行业板块.txt": "industry",
            "指数板块.txt": "index"
        }
    
    def load_all_sectors(self):
        """加载所有板块数据"""
        for filename in os.listdir(self.data_path):
            if filename in self.sector_type_map:
                sector_type = self.sector_type_map[filename]
                file_path = os.path.join(self.data_path, filename)
                try:
                    self._process_sector_file(file_path, sector_type)
                    logging.info(f"成功处理文件: {filename}")
                except Exception as e:
                    logging.error(f"处理文件失败: {filename}, 错误: {str(e)}")
    
    def _process_sector_file(self, file_path: str, sector_type: str):
        """处理单个板块文件"""
        current_sector = None
        
        # 尝试不同的编码方式
        encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            # 跳过空行
                            if not line.strip():
                                continue
                                
                            # 解析行数据
                            sector_code, sector_name, stock_code, stock_name = self._parse_line(line)
                            
                            # 如果是新板块或第一个板块
                            if current_sector is None or sector_code != current_sector['sector_code']:
                                # 保存之前的板块数据（如果有）
                                if current_sector is not None and current_sector['stocks']:
                                    self._save_sector_data(current_sector)
                                
                                # 创建新的板块数据
                                current_sector = {
                                    'sector_code': sector_code,
                                    'sector_name': sector_name,
                                    'sector_type': sector_type,
                                    'stocks': []
                                }
                            
                            # 添加股票到当前板块
                            if stock_code and stock_name:  # 确保股票代码和名称都不为空
                                current_sector['stocks'].append({
                                    'stock_code': stock_code,
                                    'stock_name': stock_name,
                                    'weight': 1.0,
                                    'is_leader': 0
                                })
                            
                        except Exception as e:
                            logging.warning(f"处理第{line_num}行时出错: {line.strip()}, 错误: {str(e)}")
                            continue
                    
                    # 保存最后一个板块的数据
                    if current_sector is not None and current_sector['stocks']:
                        self._save_sector_data(current_sector)
                        
                # 如果成功读取文件，跳出编码尝试循环
                break
                
            except UnicodeDecodeError:
                if encoding == encodings[-1]:  # 如果是最后一种编码方式
                    raise
                continue
    
    def _parse_line(self, line: str) -> Tuple[str, str, str, str]:
        """解析单行数据"""
        try:
            parts = line.strip().split('\t')
            if len(parts) != 4:
                raise ValueError(f"无效的行格式: {line}")
            
            sector_code = parts[0].strip()
            sector_name = parts[1].strip()
            stock_code = parts[2].strip()
            stock_name = parts[3].strip()
            
            # 确保所有字段都不为空
            if not all([sector_code, sector_name, stock_code, stock_name]):
                raise ValueError(f"存在空字段: {line}")
            
            # 清理股票名称中的特殊字符，但保留原始名称
            cleaned_name = stock_name
            for char in ['*', 'ST', 'N', 'XD', 'XR', 'DR']:
                cleaned_name = cleaned_name.replace(char, '').strip()
            
            # 如果清理后名称为空，使用原始名称
            stock_name = cleaned_name if cleaned_name else stock_name
            
            # 验证字段格式
            if not sector_code.isdigit() or len(sector_code) != 6:
                raise ValueError(f"无效的板块代码: {sector_code}")
            
            if not stock_code.isdigit() or len(stock_code) != 6:
                raise ValueError(f"无效的股票代码: {stock_code}")
            
            # 最终验证
            if not stock_name:
                raise ValueError(f"股票名称无效: {line}")
            
            return sector_code, sector_name, stock_code, stock_name
        except Exception as e:
            logging.warning(f"解析行时出错: {line.strip()}, 错误: {str(e)}")
            raise
    
    def _validate_stock_data(self, stock: Dict) -> bool:
        """验证股票数据的有效性"""
        try:
            # 检查必要字段是否存在且不为空
            if not stock.get('stock_code'):
                return False
            if not stock.get('stock_name'):
                return False
            
            # 检查股票代码格式
            stock_code = stock['stock_code'].strip()
            if not stock_code.isdigit() or len(stock_code) != 6:
                logging.warning(f"无效的股票代码: {stock_code}")
                return False
            
            # 检查股票名称
            stock_name = stock['stock_name'].strip()
            if not stock_name:
                logging.warning(f"股票名称为空: {stock_code}")
                return False
            
            return True
        except Exception as e:
            logging.warning(f"验证股票数据时出错: {stock}, 错误: {str(e)}")
            return False

    def _save_sector_data(self, sector_data: Dict):
        """保存板块数据到数据库"""
        try:
            # 验证板块数据
            if not sector_data.get('sector_code') or not sector_data.get('sector_name'):
                logging.warning("板块代码或名称为空")
                return
            
            # 验证并过滤股票数据
            valid_stocks = []
            for stock in sector_data.get('stocks', []):
                if self._validate_stock_data(stock):
                    valid_stocks.append(stock)
                else:
                    logging.warning(f"跳过无效股票数据: {stock}")
            
            if not valid_stocks:
                logging.warning(f"板块 {sector_data['sector_code']} 没有有效的股票数据")
                return
            
            # 更新股票列表
            sector_data['stocks'] = valid_stocks
            
            # 保存到数据库
            self.db.save_sector_info(sector_data)
            logging.info(f"成功保存板块数据: {sector_data['sector_name']}, 有效股票数: {len(valid_stocks)}")
        except Exception as e:
            logging.error(f"保存板块数据时出错: {sector_data['sector_code']}, 错误: {str(e)}") 