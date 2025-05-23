"""
Excel/CSV文件拆分工具的主界面模块
提供图形用户界面，让用户可以选择文件、设置参数并执行拆分操作
"""
import os
import sys
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
import threading
from datetime import datetime

from split_data.split import split_file
from split_data.utils.file_utils import get_file_info, is_valid_file
from split_data.utils.ui_utils import center_window, create_tooltip
from split_data.utils.log_utils import setup_logging, get_log_path


class SplitApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel/CSV文件拆分工具")
        self.root.geometry("600x520")  # 增加窗口高度
        self.root.resizable(True, True)
        self.root.minsize(600, 520)  # 增加最小高度
        
        # 设置应用图标
        try:
            if getattr(sys, 'frozen', False):
                # 打包后的路径
                app_path = sys._MEIPASS
            else:
                # 开发环境路径
                app_path = os.path.dirname(os.path.abspath(__file__))
                
            # 如果有图标文件，可以设置
            # self.root.iconbitmap(os.path.join(app_path, "icon.ico"))
        except Exception:
            pass
            
        center_window(self.root)
        
        # 初始化日志
        setup_logging()
        
        # 创建UI元素
        self.create_widgets()
        
        # 拆分任务状态
        self.splitting = False
        self.split_thread = None
        
    def create_widgets(self):
        """创建GUI组件"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="20 20 20 20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 文件选择区域
        file_frame = ttk.LabelFrame(main_frame, text="选择要拆分的文件", padding="10 10 10 10")
        file_frame.pack(fill=tk.X, pady=10)
        
        # 文件路径输入和浏览按钮
        self.file_path = tk.StringVar()
        file_entry = ttk.Entry(file_frame, textvariable=self.file_path)
        file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        browse_btn = ttk.Button(file_frame, text="浏览...", command=self.browse_file)
        browse_btn.pack(side=tk.RIGHT)
        
        # 文件信息显示区
        self.info_frame = ttk.LabelFrame(main_frame, text="文件信息", padding="10 10 10 10")
        self.info_frame.pack(fill=tk.X, pady=10)
        
        self.file_info_text = tk.Text(self.info_frame, height=5, wrap=tk.WORD)
        self.file_info_text.pack(fill=tk.X)
        self.file_info_text.config(state=tk.DISABLED)
        
        # 拆分设置区域
        settings_frame = ttk.LabelFrame(main_frame, text="拆分设置", padding="10 10 10 10")
        settings_frame.pack(fill=tk.X, pady=10)
        
        # 批处理大小设置
        batch_frame = ttk.Frame(settings_frame)
        batch_frame.pack(fill=tk.X, pady=5)
        
        batch_label = ttk.Label(batch_frame, text="每块最大行数:")
        batch_label.pack(side=tk.LEFT, padx=(0, 10))
        
        self.batch_size = tk.IntVar(value=49998)
        batch_entry = ttk.Entry(batch_frame, textvariable=self.batch_size, width=10)
        batch_entry.pack(side=tk.LEFT)
        create_tooltip(batch_entry, "建议Excel拆分不超过50000行/块")
        
        # 并发进程数设置
        workers_frame = ttk.Frame(settings_frame)
        workers_frame.pack(fill=tk.X, pady=5)
        
        workers_label = ttk.Label(workers_frame, text="并行处理进程数:")
        workers_label.pack(side=tk.LEFT, padx=(0, 10))
        
        self.max_workers = tk.IntVar(value=4)
        workers_spin = ttk.Spinbox(workers_frame, from_=1, to=16, textvariable=self.max_workers, width=5)
        workers_spin.pack(side=tk.LEFT)
        create_tooltip(workers_spin, "建议设置为CPU核心数或略低")
        
        # 清空旧输出选项
        self.clear_old = tk.BooleanVar(value=True)
        clear_check = ttk.Checkbutton(settings_frame, text="清空旧的输出文件", variable=self.clear_old)
        clear_check.pack(anchor=tk.W, pady=5)
        
        # 添加一个明显的开始拆分按钮
        start_btn_frame = ttk.Frame(main_frame)
        start_btn_frame.pack(fill=tk.X, pady=10)
        
        # 使用大号字体和明显的按钮样式
        self.split_btn = tk.Button(
            start_btn_frame, 
            text="开始拆分", 
            command=self.start_split,
            bg="#4CAF50",  # 绿色背景
            fg="white",    # 白色文字
            font=("Arial", 12, "bold"),
            height=2,
            relief=tk.RAISED,
            borderwidth=3
        )
        self.split_btn.pack(fill=tk.X, padx=50)
        
        # 拆分进度条区域
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress = ttk.Progressbar(progress_frame, orient=tk.HORIZONTAL, mode='indeterminate')
        self.progress.pack(fill=tk.X)
        
        # 底部按钮区域
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=5)
        
        self.view_log_btn = ttk.Button(btn_frame, text="查看日志", command=self.view_log)
        self.view_log_btn.pack(side=tk.LEFT, padx=5)
        
        exit_btn = ttk.Button(btn_frame, text="退出", command=self.root.destroy)
        exit_btn.pack(side=tk.RIGHT, padx=5)
        
        # 状态栏
        self.status_var = tk.StringVar(value="准备就绪")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))
        
    def browse_file(self):
        """打开文件选择对话框"""
        filetypes = [
            ("支持的文件", "*.xlsx *.xls *.csv"),
            ("Excel文件", "*.xlsx *.xls"),
            ("CSV文件", "*.csv"),
            ("所有文件", "*.*")
        ]
        
        filepath = filedialog.askopenfilename(
            title="选择要拆分的文件",
            filetypes=filetypes
        )
        
        if filepath:
            self.file_path.set(filepath)
            self.update_file_info(filepath)
    
    def update_file_info(self, filepath):
        """更新文件信息显示"""
        if not filepath or not os.path.isfile(filepath):
            self.clear_file_info()
            return
            
        # 获取文件信息
        try:
            info = get_file_info(filepath)
            
            # 更新信息显示
            self.file_info_text.config(state=tk.NORMAL)
            self.file_info_text.delete(1.0, tk.END)
            
            self.file_info_text.insert(tk.END, f"文件名: {os.path.basename(filepath)}\n")
            self.file_info_text.insert(tk.END, f"类型: {info['type']}\n")
            self.file_info_text.insert(tk.END, f"大小: {info['size']}\n")
            self.file_info_text.insert(tk.END, f"行数: {info['rows']}\n")
            self.file_info_text.insert(tk.END, f"预计拆分: {info['chunks']}个文件 (每块{self.batch_size.get()}行)")
            
            self.file_info_text.config(state=tk.DISABLED)
            
        except Exception as e:
            messagebox.showerror("错误", f"读取文件信息失败: {str(e)}")
            self.clear_file_info()
    
    def clear_file_info(self):
        """清空文件信息显示"""
        self.file_info_text.config(state=tk.NORMAL)
        self.file_info_text.delete(1.0, tk.END)
        self.file_info_text.config(state=tk.DISABLED)
    
    def start_split(self):
        """开始拆分文件"""
        filepath = self.file_path.get().strip()
        
        # 验证文件
        if not filepath:
            messagebox.showwarning("警告", "请先选择要拆分的文件!")
            return
            
        if not is_valid_file(filepath):
            messagebox.showerror("错误", "选择的文件无效或不支持的格式")
            return
            
        # 验证参数
        try:
            batch_size = self.batch_size.get()
            if batch_size <= 0:
                raise ValueError("每块行数必须大于0")
                
            max_workers = self.max_workers.get()
            if max_workers <= 0:
                raise ValueError("并行进程数必须大于0")
        except Exception as e:
            messagebox.showerror("参数错误", str(e))
            return
            
        # 防止重复启动
        if self.splitting:
            messagebox.showinfo("提示", "拆分任务正在进行中...")
            return
            
        # 确认开始
        if not messagebox.askyesno("确认", 
                                 f"即将开始拆分文件:\n{os.path.basename(filepath)}\n\n"
                                 f"每块行数: {batch_size}\n"
                                 f"并行进程: {max_workers}\n"
                                 f"清空旧文件: {'是' if self.clear_old.get() else '否'}\n\n"
                                 "确定要开始拆分吗?"):
            return
            
        # 启动拆分线程
        self.splitting = True
        self.split_btn.config(state=tk.DISABLED)
        self.progress.start()
        self.status_var.set("拆分处理中...")
        
        self.split_thread = threading.Thread(
            target=self.run_split_task,
            args=(filepath, batch_size, max_workers, self.clear_old.get())
        )
        self.split_thread.daemon = True
        self.split_thread.start()
        
        # 定期检查线程状态
        self.root.after(100, self.check_split_status)
    
    def run_split_task(self, filepath, batch_size, max_workers, clear_old):
        """在线程中执行拆分任务"""
        try:
            split_file(filepath, batch_size, max_workers, clear_old)
            # 拆分成功
            self.on_split_complete(True)
        except Exception as e:
            # 拆分失败
            self.on_split_complete(False, str(e))
    
    def check_split_status(self):
        """检查拆分线程状态"""
        if self.splitting and not self.split_thread.is_alive():
            # 线程已结束，但回调没有被调用（极少情况）
            self.on_split_complete(False, "拆分过程异常终止")
        elif self.splitting:
            # 继续检查
            self.root.after(100, self.check_split_status)
    
    def on_split_complete(self, success, error_msg=None):
        """拆分完成回调"""
        self.splitting = False
        self.progress.stop()
        self.split_btn.config(state=tk.NORMAL)
        
        if success:
            self.status_var.set(f"拆分完成 - {datetime.now().strftime('%H:%M:%S')}")
            messagebox.showinfo("完成", "文件拆分已完成!\n拆分结果保存在程序目录下的'拆分结果'文件夹中。")
        else:
            self.status_var.set("拆分失败")
            messagebox.showerror("错误", f"拆分过程中发生错误:\n{error_msg}")
    
    def view_log(self):
        """打开日志文件"""
        log_path = get_log_path()
        if os.path.exists(log_path):
            # 使用系统默认应用打开日志文件
            os.startfile(log_path)
        else:
            messagebox.showinfo("提示", "日志文件尚未创建")


def main():
    """程序入口点"""
    root = tk.Tk()
    app = SplitApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
