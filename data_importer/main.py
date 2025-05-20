"""
Excel/CSV数据导入MySQL数据库工具
主程序入口文件
"""

from tkinter import messagebox
from data_importer.utils.ui_utils import UiUtils
from data_importer.utils.file_utils import FileUtils
from data_importer.utils.db_utils import DbUtils

def main():
    """程序主入口函数"""
    print("Excel/CSV数据导入MySQL数据库工具")
    print("===================================")
    
    # 选择数据文件
    file_path = UiUtils.select_file()
    
    if file_path:
        # 获取MySQL连接信息
        mysql_info = UiUtils.get_mysql_connection_info()
        
        if mysql_info:
            # 创建数据库表并导入数据
            # 使用闭包传递UI函数给文件工具，避免循环导入
            def load_file_wrapper(path):
                return FileUtils.load_data_file(path, UiUtils.get_csv_settings)
                
            table_name = DbUtils.create_database_from_file(file_path, mysql_info, load_file_wrapper)
            if table_name:
                print(f"处理完成! 数据已导入到MySQL表: {table_name}")
                messagebox.showinfo("成功", f"数据已成功导入到MySQL表: {table_name}")
        else:
            print("未提供MySQL连接信息，操作取消。")
    else:
        print("未选择文件，操作取消。")

if __name__ == '__main__':
    main() 