from data_collector.stock_collector import StockCollector
from data_collector.sector_loader import SectorLoader
from api.sse_server import start_server
from database.models import init_database
import threading

def main():
    # 初始化数据库
    init_database()
    
    # 加载板块数据
    sector_loader = SectorLoader()
    sector_loader.load_all_sectors()
    
    # 创建数据收集器
    collector = StockCollector()
    
    # 启动数据收集线程
    collector_thread = threading.Thread(target=collector.start_collecting)
    collector_thread.daemon = True
    collector_thread.start()
    
    # 启动SSE服务器
    start_server()

if __name__ == "__main__":
    main() 