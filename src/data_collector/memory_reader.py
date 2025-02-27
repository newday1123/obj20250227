import ctypes
from ctypes import wintypes
import win32process
import win32api
import win32con

class MemoryReader:
    def __init__(self):
        self.process_handle = None
        self.base_address = None
        
    def open_process(self, process_name="tdxw.exe"):
        # 获取进程ID
        hwnd = win32api.FindWindow(None, process_name)
        if not hwnd:
            raise Exception(f"未找到进程: {process_name}")
            
        _, process_id = win32process.GetWindowThreadProcessId(hwnd)
        self.process_handle = win32api.OpenProcess(
            win32con.PROCESS_VM_READ | win32con.PROCESS_QUERY_INFORMATION,
            False,
            process_id
        )
        
        if not self.process_handle:
            raise Exception("无法打开进程")
            
    def read_memory(self, address, size):
        buffer = ctypes.create_string_buffer(size)
        bytes_read = ctypes.c_ulong(0)
        
        ctypes.windll.kernel32.ReadProcessMemory(
            self.process_handle,
            address,
            buffer,
            size,
            ctypes.byref(bytes_read)
        )
        
        return buffer.raw
        
    def get_stock_data(self, row_index=0):
        # 基础地址偏移
        base_offsets = {
            'code': 0xF3B919,
            'volume': 0xF3B940,
            'high': 0xF3B92C,
            'current': 0xF3B934,
            'open': 0xF3B928,
            'low': 0xF3B930,
            'prev_close': 0xF3B924,
            'current_volume': 0xF3B944,
            'sell_price': 0xF3B984,
            'buy_volume': 0xF3B970,
            'sell_volume': 0xF3B998
        }
        
        # 计算行偏移
        row_offset = row_index * 0x144  # 假设每行偏移为0x144
        
        stock_data = {}
        for field, offset in base_offsets.items():
            address = self.base_address + offset + row_offset
            data = self.read_memory(address, 4)  # 假设每个字段为4字节
            stock_data[field] = int.from_bytes(data, byteorder='little')
            
        return stock_data 