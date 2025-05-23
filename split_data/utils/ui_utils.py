"""UI工具类处理用户界面相关功能"""

import tkinter as tk


class UiUtils:
    """UI工具类，提供用户界面相关的静态方法"""
    
    @staticmethod
    def center_window(window):
        """将窗口居中显示"""
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        x = (window.winfo_screenwidth() // 2) - (width // 2)
        y = (window.winfo_screenheight() // 2) - (height // 2)
        window.geometry('{}x{}+{}+{}'.format(width, height, x, y))
    
    @staticmethod
    def create_tooltip(widget, text):
        """为控件创建工具提示"""
        return ToolTip(widget, text)


# 为了保持兼容性，提供直接的函数调用
def center_window(window):
    return UiUtils.center_window(window)


def create_tooltip(widget, text):
    return UiUtils.create_tooltip(widget, text)


class ToolTip:
    """创建工具提示类"""
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip = None
        self.widget.bind("<Enter>", self.show_tooltip)
        self.widget.bind("<Leave>", self.hide_tooltip)

    def show_tooltip(self, event=None):
        x, y, _, _ = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 25

        # 创建工具提示窗口
        self.tooltip = tk.Toplevel(self.widget)
        self.tooltip.wm_overrideredirect(True)  # 去掉窗口装饰
        self.tooltip.wm_geometry(f"+{x}+{y}")  # 定位窗口

        label = tk.Label(
            self.tooltip, text=self.text, background="#FFFFDD",
            relief="solid", borderwidth=1, padx=5, pady=2
        )
        label.pack()

    def hide_tooltip(self, event=None):
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None
