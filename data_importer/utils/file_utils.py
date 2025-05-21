"""
文件处理工具类
包含文件读取、编码检测等功能
"""
import os
import pandas as pd
import chardet
import re
import traceback
from tkinter import messagebox

class FileUtils:
    @staticmethod
    def better_detect_encoding(file_path):
        """改进的编码检测，处理常见中文编码问题"""
        # 首先使用chardet检测
        with open(file_path, 'rb') as f:
            raw_data = f.read(50000)  # 读取更多字节提高准确性
            result = chardet.detect(raw_data)
            confidence = result['confidence']
            encoding = result['encoding']
        
        print("编码检测结果: " + str(encoding) + ", 置信度: " + str(round(confidence, 2)))
        
        # 如果置信度较低或者检测到的是单字节编码，添加额外检查
        if confidence < 0.7 or encoding in ['ascii', 'ISO-8859-1']:
            # 检查是否包含中文字符
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    sample = f.read(10000)
                    has_chinese = any('\u4e00' <= char <= '\u9fff' for char in sample)
                    if has_chinese:
                        print("检测到包含中文字符，尝试使用中文编码")
                        # 中文常用编码优先级: GB18030 > GBK > GB2312
                        return 'gb18030'  # GB18030 是最全面的中文编码
            except:
                pass
        
        # 特别处理GB2312检测结果
        if encoding and encoding.lower() in ['gb2312', 'gbk']:
            return 'gb18030'  # 使用GB18030替代，它是GB2312和GBK的超集
            
        # 如果检测到的编码是None或空字符串，使用utf-8
        if not encoding:
            return 'utf-8'
            
        return encoding

    @staticmethod
    def detect_csv_delimiter(file_path, encoding):
        """检测CSV文件的分隔符"""
        # 读取文件前几行
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                sample = ''.join([f.readline() for _ in range(5)])
        except Exception as e:
            print("读取文件进行分隔符检测时出错: " + str(e))
            return ','  # 出错时默认使用逗号
        
        # 常见分隔符及其在样本中的出现次数
        delimiters = [',', ';', '\t', '|']
        counts = {d: 0 for d in delimiters}
        
        # 计算每行中每种分隔符的出现次数并求平均值
        lines = sample.split('\n')
        for line in lines:
            if not line.strip():  # 跳过空行
                continue
            for d in delimiters:
                counts[d] += line.count(d)
        
        # 选择出现最多的分隔符
        max_count = max(counts.values()) if counts.values() else 0
        best_delimiters = [d for d, c in counts.items() if c == max_count and max_count > 0]
        
        if best_delimiters:
            return best_delimiters[0]  # 返回第一个最多出现的分隔符
        else:
            return ','  # 默认使用逗号

    @staticmethod
    def try_read_csv(file_path, encodings, sep, header, errors):
        """尝试使用不同的编码读取CSV文件"""
        exceptions = []
        
        for encoding in encodings:
            try:
                print("尝试使用编码 " + str(encoding) + " 读取CSV")
                # 使用on_bad_lines参数处理错误行，encoding_errors代替errors
                df = pd.read_csv(file_path, encoding=encoding, sep=sep, header=header, 
                               on_bad_lines='warn', engine='python', quoting=0, 
                               escapechar='\\', encoding_errors=errors)
                print("成功使用编码 " + str(encoding) + " 读取CSV")
                return df, encoding
            except Exception as e:
                exceptions.append(str(encoding) + ": " + str(e))
                continue
        
        # 如果所有编码都失败了
        error_msg = "\n".join(exceptions)
        print("所有编码尝试都失败:\n" + error_msg)
        return None, None

    @staticmethod
    def load_data_file(file_path, get_csv_settings_func):
        """根据文件扩展名加载数据文件"""
        from data_importer.utils.data_utils import DataUtils
        
        _, ext = os.path.splitext(file_path)
        
        if ext.lower() in ['.xlsx', '.xls']:
            print("正在读取Excel文件: " + file_path)
            try:
                # 检查文件大小，决定是否使用性能优化模式
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                print(f"文件大小: {file_size_mb:.2f} MB")
                
                # 对于大文件(>20MB)，使用优化的读取方式
                use_optimized_loading = file_size_mb > 20
                
                # 输出用于调试的信息
                print(f"使用优化加载模式: {use_optimized_loading}")
                
                # 对于大文件，尝试使用chunksize分批读取
                if use_optimized_loading:
                    print("文件较大，使用优化读取模式...")
                    try:
                        # 先获取表头信息
                        excel_file = pd.ExcelFile(file_path)
                        sheet_names = excel_file.sheet_names
                        print(f"检测到 {len(sheet_names)} 个工作表: {sheet_names}")
                        
                        # 默认使用第一个表
                        sheet_name = sheet_names[0]
                        print(f"使用第一个工作表: {sheet_name}")
                        
                        # 获取表头
                        header_df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)
                        columns = header_df.columns.tolist()
                        print(f"列名: {columns}")
                        
                        # pandas的read_excel对于某些版本不支持chunksize参数
                        # 使用skiprows参数手动分批读取
                        chunk_size = 5000
                        chunks = []
                        current_row = 1  # 跳过表头
                        total_rows = None
                        
                        # 如果可能，尝试获取总行数
                        try:
                            if 'openpyxl' in pd.__version__:
                                import openpyxl
                                wb = openpyxl.load_workbook(file_path, read_only=True)
                                sheet = wb[sheet_name]
                                # 近似估计行数（可能包含空行）
                                total_rows = sheet.max_row - 1  # 减去表头
                                print(f"预估总行数: {total_rows}")
                        except Exception as e:
                            print(f"获取总行数失败: {e}")
                        
                        print("开始分批读取数据...")
                        chunk_count = 0
                        total_read_rows = 0
                        
                        while True:
                            print(f"读取批次 {chunk_count+1}, 从第 {current_row+1} 行开始")
                            try:
                                # 读取一批数据
                                chunk = pd.read_excel(
                                    file_path,
                                    sheet_name=sheet_name,
                                    skiprows=current_row,
                                    nrows=chunk_size
                                )
                                
                                # 如果没有读到数据，说明已经到文件末尾
                                if len(chunk) == 0:
                                    print("已到达文件末尾")
                                    break
                                
                                # 设置列名与表头一致
                                if len(chunk.columns) == len(columns):
                                    chunk.columns = columns
                                
                                print(f"批次 {chunk_count+1} 读取了 {len(chunk)} 行")
                                total_read_rows += len(chunk)
                                chunks.append(chunk)
                                
                                # 移动到下一批数据
                                current_row += len(chunk)
                                chunk_count += 1
                                
                                # 如果知道总行数且已经读取足够多，则退出循环
                                if total_rows is not None and current_row >= total_rows:
                                    print(f"已读取所有数据行，总计: {total_read_rows}")
                                    break
                            except Exception as e:
                                print(f"批次 {chunk_count+1} 读取错误: {e}")
                                # 如果读取到最后一批发生错误，可能是到了文件末尾
                                if chunk_count > 0:
                                    print("之前已成功读取部分数据，继续处理")
                                    break
                                else:
                                    # 如果第一批就失败，则抛出异常
                                    raise
                        
                        # 合并所有批次
                        if chunks:
                            print(f"合并 {len(chunks)} 个数据批次...")
                            df = pd.concat(chunks, ignore_index=True)
                            print(f"读取完成，总行数: {len(df)}")
                        else:
                            raise Exception("未能读取任何数据")
                    except Exception as chunk_error:
                        print(f"分批读取失败，尝试常规方式: {chunk_error}")
                        # 回退到常规读取方式
                        use_optimized_loading = False
                
                # 如果不使用优化模式或优化模式失败，使用常规方式读取
                if not use_optimized_loading:
                    # 对于Excel文件，尝试使用不同的引擎读取
                    try:
                        df = pd.read_excel(file_path, engine='openpyxl')
                        print("使用openpyxl引擎读取成功")
                    except:
                        try:
                            df = pd.read_excel(file_path, engine='xlrd')
                            print("使用xlrd引擎读取成功")
                        except:
                            # 最后尝试默认引擎
                            df = pd.read_excel(file_path)
                            print("使用默认引擎读取成功")
                
                # 检查是否成功读取数据
                if df is None or len(df) == 0:
                    if df is None:
                        print("读取结果为None")
                    else:
                        print("读取到空数据框，行数: " + str(len(df)))
                        if len(df.columns) > 0:
                            print("列数: " + str(len(df.columns)))
                            print("列名: " + str(df.columns.tolist()))
                    messagebox.showerror("错误", "Excel文件不包含数据或无法正确读取")
                    return None
                    
                # 数据预处理
                print("开始预处理数据...")
                df = DataUtils.preprocess_dataframe(df)
                    
                # 规范化列名
                print("规范化列名...")
                df = DataUtils.normalize_column_names(df)
                print("Excel文件处理完成")
                return df
            except Exception as e:
                print("读取Excel文件出错: " + str(e))
                print(traceback.format_exc())
                messagebox.showerror("错误", "读取Excel文件失败:\n" + str(e))
                return None
        elif ext.lower() == '.csv':
            print("正在读取CSV文件: " + file_path)
            
            # 获取CSV设置
            csv_settings = get_csv_settings_func()
            if not csv_settings:
                return None
                
            user_encoding = csv_settings["encoding"]
            sep = csv_settings["sep"]
            header = csv_settings["header"]
            errors = csv_settings["errors"]
            
            # 自动检测分隔符
            if sep == "auto":
                # 使用快速兼容的编码尝试检测分隔符
                detect_encoding = 'utf-8'
                try:
                    sep = FileUtils.detect_csv_delimiter(file_path, detect_encoding)
                except:
                    try:
                        sep = FileUtils.detect_csv_delimiter(file_path, 'latin1')
                    except:
                        sep = ','
                print("自动检测到分隔符: " + repr(sep))
            
            # 确定要尝试的编码
            encodings_to_try = []
            if user_encoding == "auto":
                # 自动检测编码
                detected_encoding = FileUtils.better_detect_encoding(file_path)
                print("自动检测到编码: " + str(detected_encoding))
                # 编码尝试顺序: 检测到的编码, GB18030, GBK, UTF-8, latin1
                encodings_to_try = [detected_encoding, 'gb18030', 'gbk', 'utf-8', 'utf-8-sig', 'latin1']
            else:
                # 用户指定的编码作为首选，然后是备选编码
                encodings_to_try = [user_encoding, 'gb18030', 'gbk', 'utf-8', 'latin1']
            
            # 移除重复的编码并保持顺序
            unique_encodings = []
            for enc in encodings_to_try:
                if enc and enc not in unique_encodings:
                    unique_encodings.append(enc)
            
            # 尝试使用不同编码读取
            df, successful_encoding = FileUtils.try_read_csv(file_path, unique_encodings, sep, header, errors)
            if df is not None:
                print("成功使用编码 " + str(successful_encoding) + " 读取CSV文件")
                
                # 如果没有表头，自动创建列名
                if header is None:
                    df.columns = ["Column_" + str(i+1) for i in range(len(df.columns))]
                
                # 数据预处理
                df = DataUtils.preprocess_dataframe(df)
                    
                # 规范化列名
                df = DataUtils.normalize_column_names(df)
                
                # 检查是否有数据
                if len(df) == 0:
                    print("警告: 读取到的CSV文件不包含任何数据行")
                    messagebox.showwarning("警告", "CSV文件不包含任何数据行")
                    return None
                    
                return df
            else:
                error_msg = "无法以任何支持的编码读取CSV文件"
                print(error_msg)
                messagebox.showerror("错误", error_msg)
                return None
        else:
            error_msg = "不支持的文件类型: " + ext
            print(error_msg)
            messagebox.showerror("错误", error_msg)
            return None 