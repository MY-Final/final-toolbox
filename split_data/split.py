import os
import time
from datetime import datetime
from tqdm import tqdm  # 用于进度条显示

from split_data.utils.file_utils import get_file_extension, create_output_folder
from split_data.utils.split_utils import split_csv_file, split_excel_file
from split_data.utils.log_utils import setup_logging, log_info, log_error, log_split_result


# 这些函数已移动到工具模块中


# 拆分 CSV 主流程
def split_csv(input_file, batch_size, output_folder, max_workers):
    return split_csv_file(input_file, batch_size, output_folder, max_workers)


# 拆分 Excel 主流程
def split_excel(input_file, batch_size, output_folder, max_workers):
    return split_excel_file(input_file, batch_size, output_folder, max_workers)


# 主逻辑函数
def split_file(input_file, batch_size=49998, max_workers=4, clear_old_output=False):
    setup_logging()
    start_time = time.time()
    total_chunks = 0

    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"输入文件不存在: {input_file}")

    ext = get_file_extension(input_file)
    output_folder = create_output_folder(clear_old=clear_old_output)

    try:
        if ext == '.csv':
            total_chunks = split_csv(input_file, batch_size, output_folder, max_workers)
        elif ext in ['.xlsx', '.xls']:
            total_chunks = split_excel(input_file, batch_size, output_folder, max_workers)
        else:
            raise ValueError("仅支持 .csv 或 .xlsx/.xls 格式的文件")
    except Exception as e:
        log_error(f"拆分失败: {str(e)}")
        tqdm.write(f"错误：{str(e)}")
        return

    time.sleep(1)
    elapsed = time.time() - start_time
    tqdm.write("")
    tqdm.write(f"✅ 拆分完成，文件保存在：{output_folder}")
    tqdm.write(f"⏱ 总耗时：{elapsed:.2f} 秒")
    
    # 记录拆分结果
    log_split_result(input_file, output_folder, total_chunks, elapsed)


if __name__ == '__main__':
    split_file(
        input_file="健坤521数据查询.csv",
        batch_size=49998,  # 每块行数
        max_workers=8,  # 并发数
        clear_old_output=True  # 是否清空旧输出
    )
