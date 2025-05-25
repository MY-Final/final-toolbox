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

# 添加当前目录到系统路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# 使用直接导入
from split_data.split import split_file
from split_data.utils.file_utils import get_file_info, is_valid_file
from split_data.utils.ui_utils import center_window, create_tooltip
from split_data.utils.log_utils import setup_logging, get_log_path

# 尝试导入tkinterdnd2
try:
    from tkinterdnd2 import TkinterDnD, DND_FILES
    TKDND_AVAILABLE = True
except ImportError:
    TKDND_AVAILABLE = False

# 定义颜色
PRIMARY_COLOR = "#1976D2"  # 蓝色
SECONDARY_COLOR = "#4CAF50"  # 绿色
BG_COLOR = "#F8F9FA"  # 背景色

class SplitApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel/CSV文件拆分工具")
        self.root.geometry("650x620")
        self.root.resizable(True, True)
        self.root.minsize(650, 620)
        self.root.configure(bg=BG_COLOR)
        
        center_window(self.root)
        
        # 初始化日志
        setup_logging()
        
        # 创建UI元素
        self.create_widgets()
        
        # 拆分任务状态
        self.splitting = False
        self.split_thread = None
        
        # 设置文件拖放支持
        if TKDND_AVAILABLE:
            self.setup_drag_drop()
    
    def setup_drag_drop(self):
        """设置文件拖放支持"""
        # 整个窗口支持拖放
        self.root.drop_target_register(DND_FILES)
        self.root.dnd_bind('<<Drop>>', self.handle_drop)
        
    def handle_drop(self, event):
        """处理文件拖放事件"""
        # 获取拖放的文件路径
        file_path = event.data
        
        # 处理Windows下的路径格式 {文件路径}
        if file_path.startswith('{') and file_path.endswith('}'):
            file_path = file_path[1:-1]
        
        # 处理多个文件（Windows下路径中可能包含空格）
        if ' ' in file_path and (not os.path.exists(file_path) or not os.path.isfile(file_path)):
            # 尝试按空格分割并检查第一个路径
            paths = file_path.split(' ')
            for p in paths:
                if os.path.isfile(p) and is_valid_file(p):
                    file_path = p
                    break
        
        # 检查文件是否有效
        if os.path.isfile(file_path) and is_valid_file(file_path):
            self.file_path.set(file_path)
            self.update_file_info(file_path)
        else:
            messagebox.showerror("错误", f"拖放的文件无效或不支持的格式:\n{file_path}")
        
    def create_widgets(self):
        """创建GUI组件"""
        # 顶部标题
        title_label = tk.Label(
            self.root,
            text="Excel/CSV 文件拆分工具",
            font=("Arial", 18, "bold"),
            fg=PRIMARY_COLOR,
            bg=BG_COLOR,
            pady=10
        )
        title_label.pack(pady=10)
        
        # 主框架
        main_frame = tk.Frame(self.root, bg=BG_COLOR, padx=20, pady=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 文件选择区域
        file_frame = tk.LabelFrame(
            main_frame,
            text="选择文件",
            bg="white",
            font=("Arial", 10),
            padx=10,
            pady=10
        )
        file_frame.pack(fill=tk.X, pady=10)
        
        # 提示文本
        hint_label = tk.Label(
            file_frame,
            text="拖放文件到此处，或选择文件路径:",
            bg="white",
            anchor=tk.W
        )
        hint_label.pack(fill=tk.X, pady=(0, 5))
        
        # 文件路径输入和浏览按钮
        path_frame = tk.Frame(file_frame, bg="white")
        path_frame.pack(fill=tk.X)
        
        self.file_path = tk.StringVar()
        file_entry = tk.Entry(
            path_frame,
            textvariable=self.file_path,
            font=("Arial", 10),
            width=50
        )
        file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        browse_btn = tk.Button(
            path_frame,
            text="浏览...",
            command=self.browse_file,
            bg=PRIMARY_COLOR,
            fg="white",
            padx=10,
            relief=tk.FLAT
        )
        browse_btn.pack(side=tk.RIGHT)
        
        # 文件信息显示区
        info_frame = tk.LabelFrame(
            main_frame,
            text="文件信息",
            bg="white",
            font=("Arial", 10),
            padx=10,
            pady=10
        )
        info_frame.pack(fill=tk.X, pady=10)
        
        self.file_info_text = tk.Text(
            info_frame,
            height=5,
            wrap=tk.WORD,
            font=("Arial", 10),
            bg="white",
            relief=tk.FLAT
        )
        self.file_info_text.pack(fill=tk.X)
        self.file_info_text.config(state=tk.DISABLED)
        
        # 拆分设置区域
        settings_frame = tk.LabelFrame(
            main_frame,
            text="拆分设置",
            bg="white",
            font=("Arial", 10),
            padx=10,
            pady=10
        )
        settings_frame.pack(fill=tk.X, pady=10)
        
        # 批处理大小设置
        batch_frame = tk.Frame(settings_frame, bg="white")
        batch_frame.pack(fill=tk.X, pady=5)
        
        batch_label = tk.Label(
            batch_frame,
            text="每块最大行数:",
            bg="white",
            anchor=tk.W,
            width=15
        )
        batch_label.pack(side=tk.LEFT)
        
        self.batch_size = tk.IntVar(value=49998)
        batch_entry = tk.Entry(
            batch_frame,
            textvariable=self.batch_size,
            width=15
        )
        batch_entry.pack(side=tk.LEFT, padx=10)
        
        # 并发进程数设置
        workers_frame = tk.Frame(settings_frame, bg="white")
        workers_frame.pack(fill=tk.X, pady=5)
        
        workers_label = tk.Label(
            workers_frame,
            text="并行处理进程数:",
            bg="white",
            anchor=tk.W,
            width=15
        )
        workers_label.pack(side=tk.LEFT)
        
        self.max_workers = tk.IntVar(value=4)
        workers_spin = tk.Spinbox(
            workers_frame,
            from_=1,
            to=16,
            textvariable=self.max_workers,
            width=5
        )
        workers_spin.pack(side=tk.LEFT, padx=10)
        
        # 清空旧输出选项
        clear_frame = tk.Frame(settings_frame, bg="white")
        clear_frame.pack(fill=tk.X, pady=5)
        
        self.clear_old = tk.BooleanVar(value=True)
        clear_check = tk.Checkbutton(
            clear_frame,
            text="清空旧的输出文件",
            variable=self.clear_old,
            bg="white",
            anchor=tk.W
        )
        clear_check.pack(side=tk.LEFT)
        
        # 添加开始拆分按钮
        self.split_btn = tk.Button(
            main_frame,
            text="开始拆分",
            command=self.start_split,
            bg=SECONDARY_COLOR,
            fg="white",
            font=("Arial", 14, "bold"),
            padx=20,
            pady=10,
            relief=tk.RAISED,
            bd=1,
            height=2
        )
        self.split_btn.pack(fill=tk.X, pady=15)
        
        # 设置按钮的悬停效果
        self.split_btn.bind("<Enter>", lambda e: self.split_btn.config(background="#388E3C"))
        self.split_btn.bind("<Leave>", lambda e: self.split_btn.config(background=SECONDARY_COLOR))
        
        # 进度条区域
        progress_frame = tk.Frame(main_frame, bg=BG_COLOR)
        progress_frame.pack(fill=tk.X, pady=5)
        
        self.progress = ttk.Progressbar(
            progress_frame,
            orient=tk.HORIZONTAL,
            mode='indeterminate'
        )
        self.progress.pack(fill=tk.X)
        
        # 底部按钮区域
        button_frame = tk.Frame(main_frame, bg=BG_COLOR)
        button_frame.pack(fill=tk.X, pady=10)
        
        self.view_log_btn = tk.Button(
            button_frame,
            text="查看日志",
            command=self.view_log,
            bg=PRIMARY_COLOR,
            fg="white",
            padx=10,
            relief=tk.FLAT
        )
        self.view_log_btn.pack(side=tk.LEFT)
        
        exit_btn = tk.Button(
            button_frame,
            text="退出",
            command=self.root.destroy,
            bg="#757575",
            fg="white",
            padx=10,
            relief=tk.FLAT
        )
        exit_btn.pack(side=tk.RIGHT)
        
        # 状态栏
        self.status_var = tk.StringVar(value="准备就绪")
        status_bar = tk.Label(
            self.root,
            textvariable=self.status_var,
            bd=1,
            relief=tk.SUNKEN,
            anchor=tk.W,
            padx=5,
            pady=2
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # 如果支持拖放，更新状态栏信息
        if TKDND_AVAILABLE:
            self.status_var.set("准备就绪 - 支持文件拖放")
    
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
            # 调用拆分函数
            result = split_file(filepath, batch_size, max_workers, clear_old)
            # 拆分成功
            self.root.after(0, lambda: self.on_split_complete(True))
        except UnicodeDecodeError as e:
            # 特别处理编码错误
            error_msg = f"文件编码错误: {str(e)}\n\n可能是文件包含无法识别的字符，请尝试以下解决方法:\n1. 使用记事本打开文件并另存为UTF-8格式\n2. 使用Excel打开并重新保存文件"
            self.root.after(0, lambda: self.on_split_complete(False, error_msg))
        except Exception as e:
            # 拆分失败
            self.root.after(0, lambda: self.on_split_complete(False, str(e)))
    
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
    # 尝试使用TkinterDnD2
    if TKDND_AVAILABLE:
        root = TkinterDnD.Tk()
    else:
        root = tk.Tk()
    
    app = SplitApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
