"""
拆分工具类
提供CSV和Excel文件拆分的核心功能
"""
import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

from split_data.utils.file_utils import get_file_name, read_csv_chunks, get_excel_data
from split_data.utils.log_utils import log_info, log_error


def save_chunk(chunk, output_path, header=None):
    """保存数据块到文件"""
    try:
        # 如果是DataFrame，使用to_csv或to_excel
        if isinstance(chunk, pd.DataFrame):
            if output_path.endswith('.csv'):
                chunk.to_csv(output_path, index=False, encoding='utf-8-sig')
            else:
                chunk.to_excel(output_path, index=False)
        # 如果是Excel行数据，使用pandas保存
        else:
            df = pd.DataFrame(chunk, columns=header)
            if output_path.endswith('.csv'):
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
            else:
                df.to_excel(output_path, index=False)
        return True
    except Exception as e:
        log_error(f"保存数据块失败: {str(e)}")
        return False


def process_csv_chunk(args):
    """处理单个CSV数据块的函数，用于并行处理"""
    chunk, output_file = args
    return save_chunk(chunk, output_file)


def split_csv_file(input_file, batch_size, output_folder, max_workers):
    """拆分CSV文件为多个小文件"""
    log_info(f"开始拆分CSV文件: {input_file}")
    
    # 读取CSV文件，按块处理
    chunks = read_csv_chunks(input_file, batch_size)
    
    # 准备任务列表
    tasks = []
    for i, chunk in enumerate(chunks, 1):
        output_file = os.path.join(output_folder, get_file_name(input_file, i, 'csv'))
        tasks.append((chunk, output_file))
    
    # 使用进度条显示处理进度
    total_chunks = len(tasks)
    with tqdm(total=total_chunks, desc="拆分CSV") as pbar:
        # 使用进程池并行处理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            futures = [executor.submit(process_csv_chunk, task) for task in tasks]
            
            # 等待任务完成并更新进度条
            for future in futures:
                result = future.result()
                pbar.update(1)
    
    log_info(f"CSV文件拆分完成，共生成{total_chunks}个文件")
    return total_chunks


def process_excel_chunk(args):
    """处理单个Excel数据块的函数，用于并行处理"""
    chunk_data, header, output_file = args
    return save_chunk(chunk_data, output_file, header)


def split_excel_file(input_file, batch_size, output_folder, max_workers):
    """拆分Excel文件为多个小文件"""
    log_info(f"开始拆分Excel文件: {input_file}")
    
    # 获取Excel数据
    _, rows_gen, header, total_rows = get_excel_data(input_file)
    
    # 计算总块数
    total_chunks = (total_rows + batch_size - 1) // batch_size
    
    # 准备数据块
    chunks = []
    current_chunk = []
    current_size = 0
    chunk_idx = 1
    
    # 使用进度条显示读取进度
    with tqdm(total=total_rows, desc="读取Excel") as pbar:
        for row in rows_gen:
            current_chunk.append(row)
            current_size += 1
            pbar.update(1)
            
            # 当达到批处理大小时，保存当前块
            if current_size >= batch_size:
                output_file = os.path.join(output_folder, get_file_name(input_file, chunk_idx, 'xlsx'))
                chunks.append((current_chunk, header, output_file))
                current_chunk = []
                current_size = 0
                chunk_idx += 1
        
        # 处理最后一个不完整的块
        if current_chunk:
            output_file = os.path.join(output_folder, get_file_name(input_file, chunk_idx, 'xlsx'))
            chunks.append((current_chunk, header, output_file))
    
    # 使用进度条显示处理进度
    with tqdm(total=len(chunks), desc="拆分Excel") as pbar:
        # 使用进程池并行处理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            futures = [executor.submit(process_excel_chunk, chunk) for chunk in chunks]
            
            # 等待任务完成并更新进度条
            for future in futures:
                result = future.result()
                pbar.update(1)
    
    log_info(f"Excel文件拆分完成，共生成{len(chunks)}个文件")
    return len(chunks)
