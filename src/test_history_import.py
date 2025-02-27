import os
import sys

# 添加项目根目录到 Python 路径
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

from src.database.models import init_database
from src.data_collector.history_loader import HistoryLoader
import logging

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    try:
        # 初始化数据库
        logging.info("初始化数据库...")
        init_database()
        
        # 创建历史数据加载器
        loader = HistoryLoader()
        
        # 加载历史数据
        logging.info("开始加载历史数据...")
        loader.load_history_data()
        
        logging.info("历史数据加载完成!")
        
    except Exception as e:
        logging.error(f"发生错误: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 