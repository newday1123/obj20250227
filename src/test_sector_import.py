from src.database.models import init_database
from src.data_collector.sector_loader import SectorLoader
from src.database.db_manager import DatabaseManager
import logging
import os

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_sector_file(file_path: str, sector_type: str):
    """处理单个板块文件"""
    db = DatabaseManager()
    current_sector = None
    
    # 尝试不同的编码
    encodings = ['gbk', 'utf-8', 'utf-8-sig', 'gb2312']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        # 跳过空行
                        if not line.strip():
                            continue
                        
                        # 解析行数据
                        parts = line.strip().split('\t')
                        if len(parts) != 4:
                            logging.warning(f"跳过无效行 {line_num}: {line.strip()}")
                            continue
                        
                        sector_code, sector_name, stock_code, stock_name = parts
                        
                        # 如果是新板块
                        if current_sector is None or sector_code != current_sector['sector_code']:
                            # 保存之前的板块数据
                            if current_sector is not None and current_sector['stocks']:
                                db.save_sector_info(current_sector)
                            
                            # 创建新的板块数据
                            current_sector = {
                                'sector_code': sector_code,
                                'sector_name': sector_name,
                                'sector_type': sector_type,
                                'stocks': []
                            }
                        
                        # 添加股票到当前板块
                        current_sector['stocks'].append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'weight': 1.0,
                            'is_leader': 0
                        })
                        
                    except Exception as e:
                        logging.error(f"处理行 {line_num} 时出错: {str(e)}")
                        continue
                
                # 保存最后一个板块的数据
                if current_sector is not None and current_sector['stocks']:
                    db.save_sector_info(current_sector)
                    
                # 如果成功读取文件，跳出编码尝试循环
                return True
                
        except UnicodeDecodeError:
            logging.warning(f"使用 {encoding} 编码读取失败，尝试下一种编码...")
            continue
        except Exception as e:
            logging.error(f"处理文件 {file_path} 时出错: {str(e)}")
            return False
    
    logging.error(f"所有编码尝试都失败，无法读取文件 {file_path}")
    return False

def main():
    try:
        # 初始化数据库
        logging.info("初始化数据库...")
        init_database()
        
        # 板块文件目录
        data_path = "E:\\ztdatabase\\bankuaiDATA"
        
        # 板块类型映射
        sector_type_map = {
            "地区板块.txt": "region",
            "风格板块.txt": "style",
            "概念板块.txt": "concept",
            "行业板块.txt": "industry",
            "指数板块.txt": "index"
        }
        
        # 处理每个板块文件
        for filename in os.listdir(data_path):
            if filename in sector_type_map:
                file_path = os.path.join(data_path, filename)
                sector_type = sector_type_map[filename]
                
                logging.info(f"开始处理 {filename}...")
                if process_sector_file(file_path, sector_type):
                    logging.info(f"成功处理 {filename}")
                else:
                    logging.error(f"处理 {filename} 失败")
        
        # 验证导入结果
        db = DatabaseManager()
        for sector_type in sector_type_map.values():
            sectors = db.get_sectors_by_type(sector_type)
            logging.info(f"{sector_type} 板块数量: {len(sectors)}")
            
            # 打印前3个板块的详细信息
            for sector in sectors[:3]:
                stocks = db.get_sector_stocks(sector['sector_code'])
                logging.info(f"板块: {sector['sector_name']}, 包含股票数: {len(stocks)}")
        
    except Exception as e:
        logging.error(f"发生错误: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 