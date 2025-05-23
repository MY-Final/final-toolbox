"""日志工具类处理日志记录和管理"""

import os
import logging
from datetime import datetime

# 确保logs目录存在
os.makedirs('logs', exist_ok=True)

# 全局日志文件路径
LOG_PATH = os.path.join("logs", "split_log.txt")


class LogUtils:
    """日志工具类，提供日志记录相关的静态方法"""
    
    @staticmethod
    def setup_logging():
        """初始化日志配置"""
        logging.basicConfig(
            filename=LOG_PATH,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )
        logging.info("-" * 50)
        logging.info(f"拆分会话启动 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    @staticmethod
    def get_log_path():
        """获取日志文件路径"""
        return os.path.abspath(LOG_PATH)

    @staticmethod
    def log_info(message):
        """记录信息日志"""
        logging.info(message)

    @staticmethod
    def log_error(message):
        """记录错误日志"""
        logging.error(message)

    @staticmethod
    def log_warning(message):
        """记录警告日志"""
        logging.warning(message)

    @staticmethod
    def log_start_split(input_file, batch_size, max_workers):
        """记录开始拆分日志"""
        logging.info(f"开始拆分文件: {input_file}")
        logging.info(f"参数：batch_size={batch_size}, 并行度={max_workers}")

    @staticmethod
    def log_split_result(input_file, output_folder, total_chunks, elapsed_time):
        """记录拆分结果日志"""
        logging.info(f"拆分完成: {os.path.basename(input_file)}")
        logging.info(f"输出目录: {output_folder}")
        logging.info(f"生成文件数: {total_chunks}")
        logging.info(f"总耗时: {elapsed_time:.2f}秒")
        logging.info("-" * 50)


# 为了保持兼容性，提供直接的函数调用
def setup_logging():
    return LogUtils.setup_logging()


def get_log_path():
    return LogUtils.get_log_path()


def log_info(message):
    return LogUtils.log_info(message)


def log_error(message):
    return LogUtils.log_error(message)


def log_warning(message):
    return LogUtils.log_warning(message)


def log_start_split(input_file, batch_size, max_workers):
    return LogUtils.log_start_split(input_file, batch_size, max_workers)


def log_split_result(input_file, output_folder, total_chunks, elapsed_time):
    return LogUtils.log_split_result(input_file, output_folder, total_chunks, elapsed_time)
