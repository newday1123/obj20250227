class StockMonitor {
    constructor() {
        this.eventSource = new EventSource('http://localhost:5000/api/realtime');
        this.setupEventListeners();
    }
    
    setupEventListeners() {
        this.eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.updateUI(data);
        };
        
        this.eventSource.onerror = (error) => {
            console.error('SSE错误:', error);
            this.eventSource.close();
        };
    }
    
    updateUI(data) {
        // 更新股票列表
        const stockList = document.getElementById('stock-list');
        stockList.innerHTML = '';
        
        data.forEach(stock => {
            const row = document.createElement('div');
            row.className = 'stock-row';
            row.innerHTML = `
                <span>${stock.stock_code}</span>
                <span>${stock.stock_name}</span>
                <span>${stock.current_price}</span>
                <span class="${stock.change_percent >= 0 ? 'up' : 'down'}">
                    ${stock.change_percent.toFixed(2)}%
                </span>
            `;
            stockList.appendChild(row);
        });
    }
}

// 启动监控
const monitor = new StockMonitor(); 