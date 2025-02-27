from flask import Flask, Response
from ..database.db_manager import DatabaseManager
import json
import time

app = Flask(__name__)
db = DatabaseManager()

def generate_sse_data():
    while True:
        # 获取实时数据
        data = db.get_realtime_data()
        
        # 格式化为SSE消息
        yield f"data: {json.dumps(data)}\n\n"
        
        # 每秒更新一次
        time.sleep(1)

@app.route('/api/realtime')
def sse_stream():
    return Response(
        generate_sse_data(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
        }
    )

@app.route('/api/stock/<stock_code>')
def get_stock_detail(stock_code):
    data = db.get_stock_detail(stock_code)
    if data:
        return json.dumps(data)
    return {'error': 'Stock not found'}, 404

def start_server():
    app.run(host='0.0.0.0', port=5000, debug=True) 