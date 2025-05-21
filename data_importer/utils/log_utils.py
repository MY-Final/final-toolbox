"""
日志管理工具类
用于管理和查看日志文件
"""
import os
import re
import time
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

class LogUtils:
    # 日志目录路径
    LOG_DIR = os.path.join(os.getcwd(), 'logs')
    
    @staticmethod
    def ensure_log_dir():
        """确保日志目录存在"""
        if not os.path.exists(LogUtils.LOG_DIR):
            os.makedirs(LogUtils.LOG_DIR)
            return True
        return os.path.isdir(LogUtils.LOG_DIR)
    
    @staticmethod
    def get_all_logs():
        """获取所有日志文件信息"""
        LogUtils.ensure_log_dir()
        log_files = []
        
        # 遍历日志目录
        if os.path.exists(LogUtils.LOG_DIR):
            for file in os.listdir(LogUtils.LOG_DIR):
                if file.endswith('.log'):
                    file_path = os.path.join(LogUtils.LOG_DIR, file)
                    file_stat = os.stat(file_path)
                    
                    # 提取文件信息
                    size_kb = file_stat.st_size / 1024
                    mod_time = datetime.fromtimestamp(file_stat.st_mtime)
                    
                    # 从文件名中提取表名和时间戳
                    match = re.match(r'import_(.+)_(\d{8}_\d{6})\.log', file)
                    if match:
                        table_name = match.group(1)
                        timestamp = match.group(2)
                        # 转换时间戳格式
                        try:
                            time_obj = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
                            time_str = time_obj.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            time_str = timestamp
                    else:
                        table_name = file.replace('.log', '')
                        time_str = mod_time.strftime("%Y-%m-%d %H:%M:%S")
                    
                    # 尝试从日志内容中提取导入结果
                    import_result = LogUtils.extract_import_result(file_path)
                    
                    log_files.append({
                        'file_name': file,
                        'file_path': file_path,
                        'table_name': table_name,
                        'timestamp': time_str,
                        'size_kb': round(size_kb, 2),
                        'mod_time': mod_time,
                        'import_result': import_result
                    })
        
        # 按修改时间倒序排序，最新的在前面
        log_files.sort(key=lambda x: x['mod_time'], reverse=True)
        return log_files
    
    @staticmethod
    def extract_import_result(log_file_path):
        """从日志文件中提取导入结果信息"""
        result = {
            'status': '未知',
            'total_rows': 0,
            'inserted_rows': 0,
            'error_rows': 0,
            'success_rate': 0,
            'time_taken': '未知'
        }
        
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                # 查找最后50行中的导入结果
                last_lines = lines[-50:] if len(lines) >= 50 else lines
                for line in reversed(last_lines):
                    # 匹配导入完成的结果行
                    if '数据导入完成' in line:
                        # 尝试提取数据
                        match = re.search(r'成功导入 (\d+)/(\d+) 行数据, 失败: (\d+), 用时: (.+), 平均速度', line)
                        if match:
                            result['status'] = '完成'
                            result['inserted_rows'] = int(match.group(1))
                            result['total_rows'] = int(match.group(2))
                            result['error_rows'] = int(match.group(3))
                            result['time_taken'] = match.group(4)
                            if result['total_rows'] > 0:
                                result['success_rate'] = round((result['inserted_rows'] / result['total_rows']) * 100, 2)
                            break
                    # 匹配错误中断
                    elif '错误率过高，中断导入操作' in line:
                        result['status'] = '中断'
                        break
                    elif '数据库操作出错' in line:
                        result['status'] = '失败'
                        break
                
                # 如果状态仍为未知，但找到了行数据，说明可能是部分完成
                if result['status'] == '未知' and result['total_rows'] > 0:
                    result['status'] = '部分完成'
        except Exception as e:
            print(f"读取日志文件出错: {e}")
        
        return result
    
    @staticmethod
    def read_log_content(log_file_path):
        """读取日志文件内容"""
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            return f"读取日志文件出错: {e}"
    
    @staticmethod
    def show_log_manager():
        """显示日志管理界面"""
        # 确保日志目录存在
        if not LogUtils.ensure_log_dir():
            messagebox.showerror("错误", f"无法访问日志目录: {LogUtils.LOG_DIR}")
            return
        
        # 创建主窗口
        window = tk.Tk()
        window.title("导入日志管理")
        window.geometry("1000x600")
        
        # 创建分割窗口
        paned_window = ttk.PanedWindow(window, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 左侧日志列表框架
        left_frame = ttk.Frame(paned_window)
        paned_window.add(left_frame, weight=1)
        
        # 右侧日志内容框架
        right_frame = ttk.Frame(paned_window)
        paned_window.add(right_frame, weight=2)
        
        # 创建日志列表
        columns = ('表名', '时间', '大小', '状态', '成功率')
        tree = ttk.Treeview(left_frame, columns=columns, show='headings')
        
        # 设置列标题
        tree.heading('表名', text='导入表名')
        tree.heading('时间', text='导入时间')
        tree.heading('大小', text='日志大小(KB)')
        tree.heading('状态', text='导入状态')
        tree.heading('成功率', text='成功率(%)')
        
        # 设置列宽
        tree.column('表名', width=150)
        tree.column('时间', width=150)
        tree.column('大小', width=100)
        tree.column('状态', width=80)
        tree.column('成功率', width=80)
        
        # 添加滚动条
        scrollbar = ttk.Scrollbar(left_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(fill=tk.BOTH, expand=True)
        
        # 创建日志内容文本框
        log_text = tk.Text(right_frame, wrap=tk.NONE)
        log_text.pack(fill=tk.BOTH, expand=True)
        
        # 为文本框添加滚动条
        y_scroll = ttk.Scrollbar(right_frame, orient=tk.VERTICAL, command=log_text.yview)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        log_text.configure(yscrollcommand=y_scroll.set)
        
        x_scroll = ttk.Scrollbar(right_frame, orient=tk.HORIZONTAL, command=log_text.xview)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        log_text.configure(xscrollcommand=x_scroll.set)
        
        # 加载日志数据
        log_files = LogUtils.get_all_logs()
        
        # 清空现有数据
        for item in tree.get_children():
            tree.delete(item)
        
        # 添加数据到树视图
        for log in log_files:
            tree.insert('', tk.END, values=(
                log['table_name'],
                log['timestamp'],
                log['size_kb'],
                log['import_result']['status'],
                log['import_result']['success_rate']
            ), tags=(log['file_path'],))
        
        # 日志选择处理函数
        def on_log_select(event):
            selected_items = tree.selection()
            if selected_items:
                item = selected_items[0]
                file_path = tree.item(item, 'tags')[0]
                
                # 清空现有内容
                log_text.delete(1.0, tk.END)
                
                # 读取并显示日志内容
                content = LogUtils.read_log_content(file_path)
                log_text.insert(tk.END, content)
                
                # 滚动到开头
                log_text.see("1.0")
        
        # 绑定选择事件
        tree.bind('<<TreeviewSelect>>', on_log_select)
        
        # 添加工具按钮框架
        button_frame = tk.Frame(window)
        button_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # 刷新按钮
        def refresh_logs():
            # 重新加载日志
            log_files = LogUtils.get_all_logs()
            
            # 清空现有数据
            for item in tree.get_children():
                tree.delete(item)
            
            # 添加数据到树视图
            for log in log_files:
                tree.insert('', tk.END, values=(
                    log['table_name'],
                    log['timestamp'],
                    log['size_kb'],
                    log['import_result']['status'],
                    log['import_result']['success_rate']
                ), tags=(log['file_path'],))
        
        refresh_btn = tk.Button(button_frame, text="刷新", command=refresh_logs)
        refresh_btn.pack(side=tk.LEFT, padx=5)
        
        # 导出日志按钮
        def export_log():
            selected_items = tree.selection()
            if not selected_items:
                messagebox.showwarning("警告", "请先选择要导出的日志")
                return
                
            item = selected_items[0]
            file_path = tree.item(item, 'tags')[0]
            log_content = LogUtils.read_log_content(file_path)
            
            # 获取保存路径
            export_path = filedialog.asksaveasfilename(
                defaultextension=".txt",
                filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")],
                initialfile=os.path.basename(file_path).replace('.log', '.txt')
            )
            
            if export_path:
                try:
                    with open(export_path, 'w', encoding='utf-8') as f:
                        f.write(log_content)
                    messagebox.showinfo("成功", f"日志已导出到: {export_path}")
                except Exception as e:
                    messagebox.showerror("错误", f"导出日志时出错: {e}")
        
        export_btn = tk.Button(button_frame, text="导出日志", command=export_log)
        export_btn.pack(side=tk.LEFT, padx=5)
        
        # 删除日志按钮
        def delete_log():
            selected_items = tree.selection()
            if not selected_items:
                messagebox.showwarning("警告", "请先选择要删除的日志")
                return
            
            item = selected_items[0]
            file_path = tree.item(item, 'tags')[0]
            
            if messagebox.askyesno("确认", f"确定要删除选中的日志吗？\n{os.path.basename(file_path)}"):
                try:
                    os.remove(file_path)
                    tree.delete(item)  # 从树视图中移除
                    log_text.delete(1.0, tk.END)  # 清空文本框
                    messagebox.showinfo("成功", "日志已删除")
                except Exception as e:
                    messagebox.showerror("错误", f"删除日志时出错: {e}")
        
        delete_btn = tk.Button(button_frame, text="删除日志", command=delete_log)
        delete_btn.pack(side=tk.LEFT, padx=5)
        
        # 清空所有日志按钮
        def clear_all_logs():
            if messagebox.askyesno("确认", "确定要删除所有日志文件吗？此操作不可恢复！"):
                try:
                    count = 0
                    for file in os.listdir(LogUtils.LOG_DIR):
                        if file.endswith('.log'):
                            os.remove(os.path.join(LogUtils.LOG_DIR, file))
                            count += 1
                    
                    # 清空树视图和文本框
                    for item in tree.get_children():
                        tree.delete(item)
                    log_text.delete(1.0, tk.END)
                    
                    messagebox.showinfo("成功", f"已删除 {count} 个日志文件")
                except Exception as e:
                    messagebox.showerror("错误", f"清空日志时出错: {e}")
        
        clear_btn = tk.Button(button_frame, text="清空所有日志", command=clear_all_logs)
        clear_btn.pack(side=tk.RIGHT, padx=5)
        
        # 运行主循环
        window.mainloop() 