"""
文件工具函数
提供简单的文件操作工具函数
"""
from split_data.utils.file_utils import (
    get_file_extension,
    create_output_folder,
    get_file_name,
    detect_encoding,
    count_csv_rows,
    read_csv_chunks,
    get_excel_data,
    is_valid_file,
    get_file_info
)

# 重新导出所有函数，保持API兼容性
__all__ = [
    'get_file_extension',
    'create_output_folder',
    'get_file_name',
    'detect_encoding',
    'count_csv_rows',
    'read_csv_chunks',
    'get_excel_data',
    'is_valid_file',
    'get_file_info'
]

