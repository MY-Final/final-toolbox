"""
文件处理工具类
处理文件的读取和写入
"""
import os
from datetime import datetime
import pandas as pd
from openpyxl import load_workbook
import math


class FileUtils:
    """文件处理工具类，提供静态方法处理文件操作"""
    
    @staticmethod
    def get_file_extension(file_path):
        """获取文件扩展名"""
        return os.path.splitext(file_path)[1].lower()

    @staticmethod
    def create_output_folder(folder_name="拆分结果", clear_old=False):
        """创建输出文件夹，支持清空旧文件"""
        if clear_old and os.path.exists(folder_name):
            for f in os.listdir(folder_name):
                os.remove(os.path.join(folder_name, f))
        os.makedirs(folder_name, exist_ok=True)
        return folder_name

    @staticmethod
    def get_file_name(input_file, idx, ext):
        """获取拆分后文件命名（含日期和编号）"""
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        date_prefix = datetime.now().strftime("%Y%m%d")
        return f"{base_name}_{date_prefix}_{idx:04d}.{ext}"

    @staticmethod
    def count_csv_rows(file_path):
        """计算CSV文件的总行数"""
        with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
            total_rows = sum(1 for _ in f) - 1  # 减去表头
        return total_rows

    @staticmethod
    def read_csv_chunks(file_path, batch_size):
        """读取CSV文件，按块返回"""
        return pd.read_csv(file_path, chunksize=batch_size, low_memory=False)

    @staticmethod
    def get_excel_data(file_path):
        """获取Excel工作表和头部信息"""
        wb = load_workbook(file_path, read_only=True)
        ws = wb.active
        
        total_rows = ws.max_row - 1
        rows_gen = ws.iter_rows(values_only=True)
        header = next(rows_gen)
        
        return ws, rows_gen, header, total_rows

    @staticmethod
    def is_valid_file(file_path):
        """验证文件是否存在且格式受支持"""
        if not os.path.isfile(file_path):
            return False
            
        ext = FileUtils.get_file_extension(file_path)
        return ext in ['.csv', '.xlsx', '.xls']

    @staticmethod
    def get_file_info(file_path):
        """获取文件信息，包括类型、大小、行数等"""
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
            
        file_size = os.path.getsize(file_path)
        # 格式化文件大小
        if file_size < 1024:
            size_str = f"{file_size} 字节"
        elif file_size < 1024 * 1024:
            size_str = f"{file_size/1024:.2f} KB"
        else:
            size_str = f"{file_size/(1024*1024):.2f} MB"
        
        ext = FileUtils.get_file_extension(file_path)
        
        # 获取行数和类型信息
        if ext == '.csv':
            file_type = "CSV 文件"
            try:
                rows = FileUtils.count_csv_rows(file_path)
            except Exception as e:
                raise ValueError(f"读取CSV文件失败: {str(e)}")
        elif ext in ['.xlsx', '.xls']:
            file_type = "Excel 文件"
            try:
                wb = load_workbook(file_path, read_only=True)
                ws = wb.active
                rows = ws.max_row - 1  # 减去表头
            except Exception as e:
                raise ValueError(f"读取Excel文件失败: {str(e)}")
        else:
            raise ValueError(f"不支持的文件格式: {ext}")
            
        # 默认每块最大行数
        batch_size = 49998
        chunks = math.ceil(rows / batch_size) if rows > 0 else 0
        
        return {
            "type": file_type,
            "size": size_str,
            "rows": rows,
            "chunks": chunks
        }


# 为了保持兼容性，提供直接的函数调用
def get_file_extension(file_path):
    return FileUtils.get_file_extension(file_path)


def create_output_folder(folder_name="拆分结果", clear_old=False):
    return FileUtils.create_output_folder(folder_name, clear_old)


def get_file_name(input_file, idx, ext):
    return FileUtils.get_file_name(input_file, idx, ext)


def count_csv_rows(file_path):
    return FileUtils.count_csv_rows(file_path)


def read_csv_chunks(file_path, batch_size):
    return FileUtils.read_csv_chunks(file_path, batch_size)


def get_excel_data(file_path):
    return FileUtils.get_excel_data(file_path)


def is_valid_file(file_path):
    return FileUtils.is_valid_file(file_path)


def get_file_info(file_path):
    return FileUtils.get_file_info(file_path)