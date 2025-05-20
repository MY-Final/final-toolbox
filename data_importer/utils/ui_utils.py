"""用户界面工具类包含文件选择、设置对话框等UI相关功能"""
import os
import re
import tkinter as tk
from tkinter import Tk, filedialog, StringVar, OptionMenu, messagebox, Text, Scrollbar, Frame, ttk
from datetime import datetime

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

    @staticmethod
    def confirm_column_mapping(preview_info, table_name, column_mappings):
        """显示列映射预览并请求用户确认，支持修改数据类型"""
        dialog = tk.Tk()
        dialog.title(f"列映射预览 - {table_name}")
        dialog.geometry("800x600")
        
        # 创建说明标签
        tk.Label(dialog, text="请确认以下列映射和数据类型，可以直接修改数据类型：", font=("Helvetica", 10, "bold")).pack(pady=10)
        
        # 创建表格框架
        table_frame = Frame(dialog)
        table_frame.pack(fill="both", expand=True, padx=20, pady=5)
        
        # 创建滚动条
        scrollbar_y = Scrollbar(table_frame)
        scrollbar_y.pack(side="right", fill="y")
        
        scrollbar_x = Scrollbar(table_frame, orient="horizontal")
        scrollbar_x.pack(side="bottom", fill="x")
        
        # 创建表格标题
        headers_frame = Frame(table_frame)
        headers_frame.pack(fill="x", side="top")
        
        tk.Label(headers_frame, text="原始列名", width=25, borderwidth=1, relief="solid", font=("Helvetica", 9, "bold")).pack(side="left")
        tk.Label(headers_frame, text="数据库列名", width=25, borderwidth=1, relief="solid", font=("Helvetica", 9, "bold")).pack(side="left")
        tk.Label(headers_frame, text="MySQL类型", width=25, borderwidth=1, relief="solid", font=("Helvetica", 9, "bold")).pack(side="left")
        tk.Label(headers_frame, text="状态", width=10, borderwidth=1, relief="solid", font=("Helvetica", 9, "bold")).pack(side="left")
        
        # 创建表格内容
        content_canvas = tk.Canvas(table_frame, yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
        content_canvas.pack(side="left", fill="both", expand=True)
        
        scrollbar_y.config(command=content_canvas.yview)
        scrollbar_x.config(command=content_canvas.xview)
        
        # 创建内部框架用于存放行
        inner_frame = Frame(content_canvas)
        content_canvas.create_window((0, 0), window=inner_frame, anchor="nw")
        
        # 用于存储类型输入框
        type_entries = []
        
        # 添加每一行
        for i, (orig, curr, type_str) in enumerate(column_mappings):
            row_frame = Frame(inner_frame)
            row_frame.pack(fill="x", pady=1)
            
            # 原始列名
            orig_label = tk.Label(row_frame, text=str(orig), width=25, borderwidth=1, relief="solid", anchor="w", padx=5)
            orig_label.pack(side="left")
            
            # 数据库列名
            curr_label = tk.Label(row_frame, text=str(curr), width=25, borderwidth=1, relief="solid", anchor="w", padx=5)
            curr_label.pack(side="left")
            
            # MySQL类型 - 使用Entry而不是Label，允许编辑
            type_entry = tk.Entry(row_frame, width=25)
            type_entry.insert(0, type_str)
            type_entry.pack(side="left", padx=1)
            type_entries.append((curr, type_entry))  # 保存列名和对应的类型输入框
            
            # 状态
            if str(orig) != str(curr):
                status_label = tk.Label(row_frame, text="已修改", width=10, fg="blue", borderwidth=1, relief="solid")
            else:
                status_label = tk.Label(row_frame, text="", width=10, borderwidth=1, relief="solid")
            status_label.pack(side="left")
        
        # 更新scrollregion
        inner_frame.update_idletasks()
        content_canvas.config(scrollregion=content_canvas.bbox("all"))
        
        # 提供常用MySQL类型的参考
        ref_frame = Frame(dialog)
        ref_frame.pack(fill="x", padx=20, pady=10)
        
        tk.Label(ref_frame, text="常用MySQL类型参考:", font=("Helvetica", 9, "bold")).pack(anchor="w")
        
        ref_types = "整数: TINYINT, SMALLINT, INT, BIGINT (加UNSIGNED表示无符号)\n" + \
                   "小数: DECIMAL(p,s), FLOAT, DOUBLE\n" + \
                   "字符串: VARCHAR(n), TEXT, LONGTEXT\n" + \
                   "日期时间: DATE, DATETIME, TIMESTAMP\n" + \
                   "布尔值: TINYINT(1)"
        
        ref_label = tk.Label(ref_frame, text=ref_types, justify="left")
        ref_label.pack(anchor="w")
        
        # 创建一个变量存储结果
        result = {"confirmed": False, "types": {}}
        
        def on_confirm():
            # 收集所有修改后的类型信息
            for col_name, entry in type_entries:
                result["types"][col_name] = entry.get().strip()
            
            result["confirmed"] = True
            dialog.destroy()
        
        def on_cancel():
            dialog.destroy()
        
        # 创建按钮框架
        button_frame = Frame(dialog)
        button_frame.pack(fill="x", pady=10)
        
        confirm_button = tk.Button(button_frame, text="确认并继续", command=on_confirm, width=15)
        confirm_button.pack(side="left", padx=20)
        
        cancel_button = tk.Button(button_frame, text="取消", command=on_cancel, width=15)
        cancel_button.pack(side="right", padx=20)
        
        dialog.mainloop()
        
        return result

    @staticmethod
    def show_import_report(report):
        """显示导入报告"""
        dialog = tk.Tk()
        dialog.title("数据导入报告")
        dialog.geometry("600x400")
        
        # 创建标题标签
        tk.Label(dialog, text=f"表 {report['table_name']} 导入报告", font=("Helvetica", 12, "bold")).pack(pady=10)
        
        # 创建统计信息框架
        stats_frame = Frame(dialog)
        stats_frame.pack(fill="x", padx=20, pady=5)
        
        # 显示统计信息
        stats_text = f"总行数: {report['total_rows']}\n"
        stats_text += f"成功导入: {report['rows_inserted']} 行\n"
        stats_text += f"失败: {report['error_rows']} 行\n"
        stats_text += f"成功率: {report['success_rate']:.2f}%"
        
        tk.Label(stats_frame, text=stats_text, justify="left").pack(anchor="w")
        
        # 创建分隔线
        separator = Frame(dialog, height=2, bd=1, relief="sunken")
        separator.pack(fill="x", padx=20, pady=10)
        
        # 创建列映射标签
        tk.Label(dialog, text="列映射详情:", font=("Helvetica", 10, "bold")).pack(anchor="w", padx=20)
        
        # 创建列映射框架
        mapping_frame = Frame(dialog)
        mapping_frame.pack(fill="both", expand=True, padx=20, pady=5)
        
        # 创建文本框和滚动条显示列映射
        scrollbar = Scrollbar(mapping_frame)
        scrollbar.pack(side="right", fill="y")
        
        text_area = Text(mapping_frame, wrap="word", yscrollcommand=scrollbar.set)
        text_area.pack(side="left", fill="both", expand=True)
        
        scrollbar.config(command=text_area.yview)
        
        # 插入列映射信息
        mapping_text = "原始列名 -> 数据库列名 (MySQL类型)\n"
        mapping_text += "----------------------------------------\n"
        
        for orig, curr, type_str in report["column_mappings"]:
            if str(orig) != str(curr):
                mapping_text += f"{orig} -> {curr} ({type_str}) ← 已修改\n"
            else:
                mapping_text += f"{orig} -> {curr} ({type_str})\n"
        
        text_area.insert("1.0", mapping_text)
        text_area.config(state="disabled")  # 设为只读
        
        # 创建关闭按钮
        close_button = tk.Button(dialog, text="关闭", command=dialog.destroy, width=15)
        close_button.pack(pady=10)
        
        dialog.mainloop() 