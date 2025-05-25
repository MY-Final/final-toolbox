"""
Excel/CSV文件拆分工具的主入口文件
"""
import os
import sys

# 添加当前目录到系统路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from split_data.main import main

if __name__ == "__main__":
    main() 