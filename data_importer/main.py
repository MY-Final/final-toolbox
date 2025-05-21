"""
Excel/CSV数据导入MySQL数据库工具
主程序入口文件
"""

import tkinter as tk
import threading
import traceback
from tkinter import messagebox, ttk, Frame
from data_importer.utils.ui_utils import UiUtils
from data_importer.utils.file_utils import FileUtils
from data_importer.utils.db_utils import DbUtils
from data_importer.utils.log_utils import LogUtils
from data_importer.utils.config_manager import ConfigManager
import os
import time
import datetime

class GlobalErrorHandler:
    @staticmethod
    def handle_exception(exc_type, exc_value, exc_traceback):
        """全局异常处理器"""
        error_msg = f"发生错误: {exc_type.__name__}: {str(exc_value)}"
        print(error_msg)
        print("详细错误信息:")
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        
        # 在主线程中显示错误对话框
        if hasattr(tk, '_default_root') and tk._default_root:
            tk._default_root.after(0, lambda: messagebox.showerror("错误", error_msg))

def create_main_window():
    """创建主窗口"""
    global root, status_label, import_button, log_button
    
    root = tk.Tk()
    root.title("Excel/CSV数据导入MySQL工具")
    root.geometry("500x300")
    
    # 设置全局异常处理
    tk.Tk.report_callback_exception = GlobalErrorHandler.handle_exception
    
    # 创建欢迎标签
    welcome_label = tk.Label(
        root, 
        text="Excel/CSV数据导入MySQL数据库工具",
        font=("Helvetica", 14, "bold")
    )
    welcome_label.pack(pady=20)
    
    description = """
    此工具可以将Excel或CSV文件数据导入到MySQL数据库。
    支持批量导入、数据类型自动推断和配置文件管理。
    """
    
    desc_label = tk.Label(root, text=description, justify="left")
    desc_label.pack(pady=10)
    
    # 创建状态标签
    status_label = tk.Label(root, text="就绪", fg="blue")
    status_label.pack(pady=5)
    
    # 创建按钮框架
    button_frame = tk.Frame(root)
    button_frame.pack(pady=20)
    
    # 导入数据按钮
    def start_import():
        # 全局变量，确保异常处理中可以访问
        global root, status_label, import_button, log_button, progress_window
        
        try:
            # 更新状态
            status_label.config(text="选择文件中...", fg="blue")
            root.update()
            
            # 选择数据文件
            file_path = UiUtils.select_file()
            
            if not file_path:
                status_label.config(text="已取消文件选择", fg="orange")
                return
            
            status_label.config(text=f"已选择文件: {file_path}", fg="blue")
            root.update()
            
            # 获取MySQL连接信息
            status_label.config(text="正在配置数据库连接...", fg="blue")
            root.update()
            
            try:
                # 先尝试从配置文件加载
                saved_config = ConfigManager.get_db_config()
                mysql_info = UiUtils.get_mysql_connection_info(saved_config)
                
                if not mysql_info:
                    raise ValueError("已取消数据库连接")
                    
                # 保存新的配置
                ConfigManager.save_db_config(
                    mysql_info['host'],
                    mysql_info['port'],
                    mysql_info['database'],
                    mysql_info['user']
                )
                
                print(f"数据库连接信息: {mysql_info['host']}:{mysql_info['port']} 数据库:{mysql_info['database']} 用户:{mysql_info['user']}")
            except ValueError as ve:
                status_label.config(text=str(ve), fg="orange")
                return
            except Exception as e:
                error_msg = f"获取数据库连接信息时出错: {str(e)}"
                print(error_msg)
                status_label.config(text="数据库连接配置失败", fg="red")
                messagebox.showerror("错误", error_msg)
                return
            
            # 检查主窗口是否存在
            try:
                # 禁用按钮，防止重复操作
                import_button.config(state=tk.DISABLED)
                log_button.config(state=tk.DISABLED)
                
                # 显示处理中状态
                status_label.config(text="准备导入数据...", fg="blue")
                root.update()
            except tk.TclError as e:
                print(f"窗口组件访问错误: {str(e)}")
                # 如果主窗口组件不可访问，重新创建主窗口
                root = create_main_window()
                status_label = root.nametowidget(".!label3")  # 获取状态标签
                status_label.config(text="准备导入数据...", fg="blue")
                # 获取按钮
                button_frame = root.nametowidget(".!frame")
                import_button = button_frame.winfo_children()[0]
                log_button = button_frame.winfo_children()[1]
                import_button.config(state=tk.DISABLED)
                log_button.config(state=tk.DISABLED)
                root.update()
                
            # 创建导入进度窗口
            try:
                progress_window = tk.Toplevel(root)
                progress_window.title("导入进度")
                progress_window.geometry("400x150")
                progress_window.transient(root)  # 设置为root的临时窗口
                progress_window.lift()  # 确保窗口在最前面
                progress_window.attributes('-topmost', True)  # 设置为置顶窗口
                
                progress_label = tk.Label(progress_window, text="正在处理数据...", padx=20, pady=10)
                progress_label.pack()
                
                # 创建进度条
                progress_var = tk.DoubleVar()
                progress_bar = ttk.Progressbar(progress_window, variable=progress_var, maximum=100)
                progress_bar.pack(fill=tk.X, padx=20, pady=10)
                
                # 添加步骤指示器
                steps_frame = Frame(progress_window)
                steps_frame.pack(fill=tk.X, padx=20, pady=5)
                
                steps = [
                    "1. 加载文件", 
                    "2. 解析数据", 
                    "3. 确认结构", 
                    "4. 创建表", 
                    "5. 导入数据", 
                    "6. 生成报告"
                ]
                
                step_labels = []
                for i, step in enumerate(steps):
                    step_label = tk.Label(steps_frame, text=step, fg="gray")
                    step_label.grid(row=0, column=i, padx=5)
                    step_labels.append(step_label)
                
                # 添加详细状态文本框
                status_text = tk.Text(progress_window, height=3, width=40)
                status_text.pack(padx=20, pady=5)
                status_text.insert("1.0", "正在初始化...\n")
                
                # 取消按钮（暂不实现取消功能，仅关闭进度窗口）
                def close_progress():
                    try:
                        status_label.config(text="操作已取消", fg="orange")
                        # 重新启用按钮
                        import_button.config(state=tk.NORMAL)
                        log_button.config(state=tk.NORMAL)
                    except tk.TclError:
                        print("无法更新主窗口状态，窗口可能已关闭")
                    progress_window.destroy()
                
                cancel_btn = tk.Button(progress_window, text="关闭", command=close_progress)
                cancel_btn.pack(pady=10)
                
                # 更新窗口显示
                progress_window.update()
                
            except Exception as e:
                print(f"创建进度窗口失败: {str(e)}")
                messagebox.showerror("错误", f"无法创建进度窗口: {str(e)}")
                return
            
            # 使用线程执行导入操作
            def import_thread():
                try:
                    # 更新进度显示 - 启动
                    def update_progress(message, progress=None):
                        def do_update():
                            if progress_window.winfo_exists():
                                progress_label.config(text=message)
                                status_text.insert("end", message + "\n")
                                status_text.see("end")
                                if progress is not None:
                                    progress_var.set(progress)
                                    current_step = min(int(progress // 20), len(step_labels) - 1)
                                    for i, label in enumerate(step_labels):
                                        label.config(fg="green" if i < current_step else "blue" if i == current_step else "gray")
                            print(f"进度更新: {message} - {progress}%")
                        root.after_idle(do_update)
                        root.update_idletasks()
                    
                    update_progress("正在加载数据文件...", 5)
                    
                    # 使用闭包传递UI函数给文件工具，避免循环导入
                    def load_file_wrapper(path):
                        try:
                            update_progress("正在解析数据文件...", 10)
                            df = FileUtils.load_data_file(path, UiUtils.get_csv_settings)
                            
                            # 添加更多进度反馈
                            if df is not None:
                                rows_count = len(df)
                                cols_count = len(df.columns)
                                update_progress(f"文件解析成功，读取到 {rows_count} 行 x {cols_count} 列数据", 15)
                                # 显示文件大小信息
                                file_size_mb = os.path.getsize(path) / (1024 * 1024)
                                update_progress(f"文件大小: {file_size_mb:.2f} MB", 15)
                            else:
                                update_progress("文件解析失败，未能读取数据", 15)
                            return df
                        except Exception as e:
                            error_msg = f"解析文件出错: {str(e)}"
                            print(error_msg)
                            print(traceback.format_exc())
                            update_progress(f"解析文件出错: {str(e)}", 15)
                            raise
                    
                    # 调用导入函数
                    update_progress("正在准备导入数据...", 20)
                    
                    # 先测试数据库连接
                    update_progress("测试数据库连接...", 25)
                    success, message = DbUtils.test_mysql_connection(mysql_info)
                    if not success:
                        update_progress(f"数据库连接失败: {message}", 30)
                        raise Exception(f"数据库连接测试失败: {message}")
                    
                    update_progress("数据库连接成功，开始导入数据...", 35)
                    
                    try:
                        # 增加导入超时监控
                        # 导入结果
                        import_result = {"completed": False, "table_name": None, "error": None}
                        # 用于控制线程终止的事件
                        stop_event = threading.Event()
                        
                        # 定义导入函数
                        def do_import():
                            try:
                                if not stop_event.is_set():
                                    import_result["table_name"] = DbUtils.create_database_from_file(
                                        file_path, mysql_info, 
                                        lambda path: load_file_wrapper(path), # 确保正确传递参数
                                        update_progress)
                                    import_result["completed"] = True
                            except Exception as e:
                                import_result["error"] = str(e)
                                import_result["completed"] = True
                        
                        # 启动导入线程
                        import_thread = threading.Thread(target=do_import)
                        import_thread.daemon = True
                        import_thread.start()
                        
                        # 监控导入进度，提供中断机会
                        import_timeout = 3600  # 1小时超时
                        start_time = time.time()
                        last_progress_time = start_time
                        
                        while import_thread.is_alive():
                            # 更新界面响应
                            root.update()
                            time.sleep(0.1)
                            
                            # 检查是否完成
                            if import_result["completed"]:
                                break
                            
                            # 检查超时
                            current_time = time.time()
                            if current_time - start_time > import_timeout:
                                update_progress("导入操作超时，已执行超过1小时", None)
                                # 在界面上提供选择
                                if messagebox.askyesno("导入超时", 
                                    "导入操作已执行超过1小时，是否继续等待?\n选择'否'将中断操作。"):
                                    # 重置超时时间
                                    start_time = current_time
                                else:
                                    import_result["error"] = "用户取消了长时间运行的导入操作"
                                    stop_event.set()  # 设置停止事件
                                    break
                            
                            # 检查进度是否长时间未更新
                            if current_time - last_progress_time > 300:  # 5分钟无进度更新
                                update_progress("导入操作似乎已停滞...", None)
                                if messagebox.askyesno("导入停滞", 
                                    "导入操作已有5分钟无进度更新，可能已停滞。是否继续等待?\n选择'否'将中断操作。"):
                                    # 重置进度更新时间
                                    last_progress_time = current_time
                                else:
                                    import_result["error"] = "用户取消了停滞的导入操作"
                                    stop_event.set()  # 设置停止事件
                                    break
                        
                        # 检查结果
                        if import_result["error"]:
                            raise Exception(import_result["error"])
                        
                        table_name = import_result["table_name"]
                    except Exception as e:
                        raise e  # 重新抛出异常
                    
                    # 在主线程中更新UI
                    root.after(0, lambda: finish_import(table_name))
                    
                except Exception as e:
                    import traceback
                    error_msg = str(e)
                    tb = traceback.format_exc()
                    print(f"导入出错: {error_msg}\n{tb}")
                    
                    # 记录到日志
                    try:
                        log_dir = os.path.join(os.getcwd(), 'logs')
                        if not os.path.exists(log_dir):
                            os.makedirs(log_dir)
                        
                        # 创建错误日志
                        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        error_log_file = os.path.join(log_dir, f"error_{timestamp}.log")
                        
                        with open(error_log_file, 'w', encoding='utf-8') as f:
                            f.write(f"导入错误: {error_msg}\n\n")
                            f.write("详细错误信息:\n")
                            f.write(tb)
                            f.write("\n\n")
                            f.write(f"导入文件: {file_path}\n")
                            f.write(f"时间: {timestamp}\n")
                        
                        error_msg += f"\n\n详细错误信息已保存至:\n{error_log_file}"
                    except Exception as log_e:
                        print(f"保存错误日志失败: {log_e}")
                    
                    # 在主线程中显示错误
                    root.after(0, lambda: show_error(error_msg))
            
            # 完成导入的回调
            def finish_import(table_name):
                global progress_window, import_button, log_button, status_label
                
                try:
                    # 关闭进度窗口
                    if 'progress_window' in globals() and progress_window.winfo_exists():
                        progress_window.destroy()
                    
                    # 重新启用按钮
                    if 'import_button' in globals():
                        import_button.config(state=tk.NORMAL)
                    if 'log_button' in globals():
                        log_button.config(state=tk.NORMAL)
                    
                    if table_name:
                        if 'status_label' in globals():
                            status_label.config(text=f"导入成功: {table_name}", fg="green")
                        messagebox.showinfo("成功", f"数据已成功导入到MySQL表: {table_name}")
                    else:
                        if 'status_label' in globals():
                            status_label.config(text="导入失败", fg="red")
                except tk.TclError as e:
                    print(f"更新UI时出错: {str(e)}")
                    messagebox.showinfo("成功", f"数据已成功导入到MySQL表: {table_name}")
            
            # 显示错误的回调
            def show_error(error_msg):
                global progress_window, import_button, log_button, status_label
                
                try:
                    # 关闭进度窗口
                    if 'progress_window' in globals() and progress_window.winfo_exists():
                        progress_window.destroy()
                        
                    # 重新启用按钮
                    if 'import_button' in globals():
                        import_button.config(state=tk.NORMAL)
                    if 'log_button' in globals():
                        log_button.config(state=tk.NORMAL)
                    
                    # 更新状态
                    if 'status_label' in globals():
                        status_label.config(text="导入失败", fg="red")
                except tk.TclError as e:
                    print(f"更新UI时出错: {str(e)}")
                
                # 显示错误消息
                messagebox.showerror("导入错误", f"导入数据时出错:\n{error_msg}")
            
            # 启动导入线程
            import_thread = threading.Thread(target=import_thread)
            import_thread.daemon = True
            import_thread.start()
        
        except Exception as e:
            print(f"导入操作失败: {str(e)}")
            messagebox.showerror("错误", f"导入操作失败: {str(e)}")
    
    import_button = tk.Button(
        button_frame, 
        text="导入数据", 
        command=start_import,
        width=15,
        height=2
    )
    import_button.pack(side=tk.LEFT, padx=10)
    
    # 查看日志按钮
    def open_log_manager():
        global status_label, root
        
        try:
            status_label.config(text="正在打开日志管理器...", fg="blue")
            root.update()
            LogUtils.show_log_manager()
            status_label.config(text="就绪", fg="blue")
        except tk.TclError as e:
            print(f"日志管理器操作出错: {str(e)}")
            # 直接打开日志管理器，忽略状态更新
            LogUtils.show_log_manager()
    
    log_button = tk.Button(
        button_frame, 
        text="查看日志", 
        command=open_log_manager,
        width=15,
        height=2
    )
    log_button.pack(side=tk.LEFT, padx=10)
    
    # 退出按钮
    exit_button = tk.Button(
        button_frame, 
        text="退出", 
        command=root.destroy,
        width=15,
        height=2
    )
    exit_button.pack(side=tk.LEFT, padx=10)
    
    # 版本信息
    version_label = tk.Label(
        root, 
        text="版本 1.1.0",
        font=("Helvetica", 8)
    )
    version_label.pack(side=tk.BOTTOM, pady=10)
    
    return root

def main():
    """程序主入口函数"""
    # 声明全局变量
    global root, status_label, import_button, log_button
    
    # 创建主窗口及组件
    root = create_main_window()
    
    # 主循环
    root.mainloop()

if __name__ == '__main__':
    main() 