"""
用户界面工具类
包含文件选择、设置对话框等UI相关功能
"""
import tkinter as tk
from tkinter import Tk, filedialog, StringVar, OptionMenu, messagebox

class UiUtils:
    @staticmethod
    def select_file():
        """选择Excel或CSV文件"""
        root = Tk()
        root.withdraw()  # 隐藏主窗口
        file_path = filedialog.askopenfilename(
            title="选择数据文件", 
            filetypes=[
                ("所有支持的文件", "*.xlsx *.xls *.csv"),
                ("Excel文件", "*.xlsx *.xls"),
                ("CSV文件", "*.csv")
            ]
        )
        root.destroy()
        return file_path

    @staticmethod
    def get_csv_settings():
        """获取CSV文件的导入设置"""
        dialog = tk.Tk()
        dialog.title("CSV导入设置")
        dialog.geometry("350x250")
        
        # 创建标签和输入框
        tk.Label(dialog, text="编码:").grid(row=0, column=0, padx=10, pady=5, sticky="w")
        encoding_var = StringVar(dialog)
        encoding_var.set("auto")  # 默认自动检测
        encodings = ["auto", "utf-8", "utf-8-sig", "gbk", "gb18030", "gb2312", "iso-8859-1", "ascii"]
        encoding_menu = OptionMenu(dialog, encoding_var, *encodings)
        encoding_menu.grid(row=0, column=1, padx=10, pady=5, sticky="w")
        
        tk.Label(dialog, text="分隔符:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        sep_var = StringVar(dialog)
        sep_var.set("auto")  # 默认自动检测
        seps = ["auto", ",", ";", "\\t", "|", " "]
        sep_menu = OptionMenu(dialog, sep_var, *seps)
        sep_menu.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        
        tk.Label(dialog, text="是否有表头:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        header_var = StringVar(dialog)
        header_var.set("有")
        header_options = ["有", "没有"]
        header_menu = OptionMenu(dialog, header_var, *header_options)
        header_menu.grid(row=2, column=1, padx=10, pady=5, sticky="w")

        # 错误处理选项
        tk.Label(dialog, text="错误处理:").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        error_var = StringVar(dialog)
        error_var.set("replace")
        error_options = ["replace", "ignore", "strict"]
        error_menu = OptionMenu(dialog, error_var, *error_options)
        error_menu.grid(row=3, column=1, padx=10, pady=5, sticky="w")
        
        # 创建一个变量存储结果
        result = {"success": False}
        
        def on_submit():
            result["encoding"] = "auto" if encoding_var.get() == "auto" else encoding_var.get()
            result["sep"] = "auto" if sep_var.get() == "auto" else sep_var.get().replace("\\t", "\t")
            result["header"] = 0 if header_var.get() == "有" else None
            result["errors"] = error_var.get()
            result["success"] = True
            dialog.destroy()
        
        def on_cancel():
            dialog.destroy()
        
        # 添加按钮
        submit_button = tk.Button(dialog, text="确认", command=on_submit)
        submit_button.grid(row=4, column=0, padx=10, pady=10)
        
        cancel_button = tk.Button(dialog, text="取消", command=on_cancel)
        cancel_button.grid(row=4, column=1, padx=10, pady=10)
        
        dialog.mainloop()
        
        return result if result["success"] else None

    @staticmethod
    def get_mysql_connection_info():
        """获取MySQL连接信息"""
        dialog = tk.Tk()
        dialog.title("MySQL连接信息")
        dialog.geometry("300x250")
        
        # 创建标签和输入框
        tk.Label(dialog, text="主机:").grid(row=0, column=0, padx=10, pady=5, sticky="w")
        host_entry = tk.Entry(dialog, width=25)
        host_entry.insert(0, "localhost")
        host_entry.grid(row=0, column=1, padx=10, pady=5)
        
        tk.Label(dialog, text="端口:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        port_entry = tk.Entry(dialog, width=25)
        port_entry.insert(0, "3306")
        port_entry.grid(row=1, column=1, padx=10, pady=5)
        
        tk.Label(dialog, text="用户名:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        user_entry = tk.Entry(dialog, width=25)
        user_entry.insert(0, "root")
        user_entry.grid(row=2, column=1, padx=10, pady=5)
        
        tk.Label(dialog, text="密码:").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        password_entry = tk.Entry(dialog, width=25, show="*")
        password_entry.grid(row=3, column=1, padx=10, pady=5)
        
        tk.Label(dialog, text="数据库名:").grid(row=4, column=0, padx=10, pady=5, sticky="w")
        db_entry = tk.Entry(dialog, width=25)
        db_entry.grid(row=4, column=1, padx=10, pady=5)
        
        # 创建一个变量存储结果
        result = {"success": False}
        
        def on_submit():
            result["host"] = host_entry.get()
            result["port"] = int(port_entry.get())
            result["user"] = user_entry.get()
            result["password"] = password_entry.get()
            result["database"] = db_entry.get()
            result["success"] = True
            dialog.destroy()
        
        def on_cancel():
            dialog.destroy()
        
        # 添加按钮
        submit_button = tk.Button(dialog, text="连接", command=on_submit)
        submit_button.grid(row=5, column=0, padx=10, pady=10)
        
        cancel_button = tk.Button(dialog, text="取消", command=on_cancel)
        cancel_button.grid(row=5, column=1, padx=10, pady=10)
        
        dialog.mainloop()
        
        return result if result["success"] else None 