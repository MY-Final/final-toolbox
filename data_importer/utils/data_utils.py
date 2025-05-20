"""
数据处理工具类
包含数据预处理、列名规范化等功能
"""
import pandas as pd
import numpy as np
import re

class DataUtils:
    @staticmethod
    def normalize_column_names(df):
        """规范化列名，处理重复列名和特殊字符"""
        # 复制原始列名，用于记录映射关系
        original_columns = df.columns.tolist()
        normalized_columns = []
        
        # 列名计数器，用于处理重复列名
        col_counter = {}
        
        for col in original_columns:
            # 处理特殊字符
            base_name = str(col).strip()
            
            # 移除任何可能导致SQL问题的特殊字符
            # 只保留字母、数字和下划线，其他字符替换为下划线
            clean_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in base_name)
            
            # 确保列名不以数字开头
            if clean_name and clean_name[0].isdigit():
                clean_name = 'col_' + clean_name
                
            # 如果清理后为空，使用默认名称
            if not clean_name:
                clean_name = 'column'
            
            # 检查是否有重复列名
            if clean_name in col_counter:
                col_counter[clean_name] += 1
                normalized_name = clean_name + "_" + str(col_counter[clean_name])
            else:
                col_counter[clean_name] = 0
                normalized_name = clean_name
                
            normalized_columns.append(normalized_name)
        
        # 打印列名修改情况
        column_changes = [(orig, norm) for orig, norm in zip(original_columns, normalized_columns) if str(orig) != norm]
        if column_changes:
            print("以下列名已规范化:")
            for orig, norm in column_changes:
                print("  '" + str(orig) + "' -> '" + str(norm) + "'")
                
        # 设置新的列名
        df.columns = normalized_columns
        return df

    @staticmethod
    def preprocess_dataframe(df):
        """数据预处理，确保数据框的完整性和一致性"""
        print("数据预处理前: 行数 = " + str(len(df)) + ", 列数 = " + str(len(df.columns)))
        
        # 1. 删除所有空列
        empty_cols = [col for col in df.columns if df[col].isna().all()]
        if empty_cols:
            print("删除 " + str(len(empty_cols)) + " 个空列")
            df = df.drop(columns=empty_cols)
        
        # 2. 检查数据类型是否一致
        for col in df.columns:
            try:
                # 尝试转换合适的数据类型
                if df[col].dtype == 'object':
                    # 检查是否可以转换为数值型
                    numeric_values = pd.to_numeric(df[col], errors='coerce')
                    if not numeric_values.isna().all():
                        if (numeric_values % 1 == 0).all():
                            df[col] = numeric_values.astype('Int64')  # 使用可空整型
                        else:
                            df[col] = numeric_values  # 转换为浮点型
            except:
                # 保持原样
                pass
        
        # 3. 确保每行有相同数量的列
        print("预处理后: 行数 = " + str(len(df)) + ", 列数 = " + str(len(df.columns)))
        return df

    @staticmethod
    def debug_data_structure(df):
        """调试数据结构，打印详细信息"""
        print("\n=== 数据结构诊断 ===")
        print("DataFrame形状: " + str(df.shape))
        print("列数: " + str(len(df.columns)))
        print("行数: " + str(len(df)))
        
        # 检查是否有重复列名
        if len(df.columns) != len(set(df.columns)):
            print("警告: 检测到重复列名:")
            col_count = {}
            for col in df.columns:
                col_count[col] = col_count.get(col, 0) + 1
            for col, count in col_count.items():
                if count > 1:
                    print("  '" + str(col) + "' 出现 " + str(count) + " 次")
        
        # 检查数据类型
        print("\n列数据类型:")
        for col in df.columns:
            print("  '" + str(col) + "': " + str(df[col].dtype))
            
            # 检查是否有可能导致SQL问题的特殊字符
            if df[col].dtype == 'object':
                sample = df[col].dropna().astype(str).head(100)
                has_backtick = False
                has_quotes = False
                has_backslash = False
                
                for val in sample:
                    if '`' in val:
                        has_backtick = True
                    if "'" in val or '"' in val:
                        has_quotes = True
                    if '\\' in val:
                        has_backslash = True
                
                if has_backtick or has_quotes or has_backslash:
                    print("    警告: 该列包含可能需要转义的特殊字符:")
                    if has_backtick:
                        print("      - 包含反引号(`)，可能影响SQL标识符")
                    if has_quotes:
                        print("      - 包含引号(' 或 \")，可能影响SQL字符串")
                    if has_backslash:
                        print("      - 包含反斜杠(\\)，可能影响转义序列")
        
        # 检查缺失值
        null_counts = df.isna().sum()
        if null_counts.sum() > 0:
            print("\n缺失值统计:")
            for col, count in null_counts.items():
                if count > 0:
                    percent = round(count / len(df) * 100, 2)
                    print("  '" + str(col) + "': " + str(count) + " 缺失值 (" + str(percent) + "%)")
        
        # 检查数值范围，帮助确定适合的数据类型
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print("\n数值列范围:")
            for col in numeric_cols:
                try:
                    min_val = df[col].min()
                    max_val = df[col].max()
                    print("  '" + str(col) + "': 最小值=" + str(min_val) + ", 最大值=" + str(max_val))
                except:
                    print("  '" + str(col) + "': 无法计算范围")
        
        print("=== 诊断结束 ===\n")

    @staticmethod
    def determine_mysql_type(column_name, series):
        """根据列名和数据确定适合的MySQL数据类型"""
        # 检查是否为空列
        if series.isna().all():
            return "VARCHAR(255)"
            
        # 特殊处理id列，使用BIGINT
        if column_name.lower() == 'id':
            return "BIGINT"
        
        # 获取非空值
        non_null = series.dropna()
        if len(non_null) == 0:
            return "VARCHAR(255)"
        
        # 检查数值类型
        if pd.api.types.is_integer_dtype(series.dtype):
            # 检查整数范围
            try:
                if non_null.min() >= -2147483648 and non_null.max() <= 2147483647:
                    return "INT"
                else:
                    return "BIGINT"
            except:
                return "BIGINT"
        elif pd.api.types.is_float_dtype(series.dtype):
            # 检查是否可以是DECIMAL
            if not np.any(np.isnan(non_null)) and not np.any(np.isinf(non_null)):
                # 计算最大精度和小数位
                try:
                    integers = non_null.apply(lambda x: len(str(int(x))))
                    max_int_digits = integers.max() if len(integers) > 0 else 10
                    
                    decimals = non_null.apply(lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0)
                    max_decimal_digits = decimals.max() if len(decimals) > 0 else 2
                    
                    # 限制精度和小数位
                    precision = min(max_int_digits + max_decimal_digits, 65)
                    scale = min(max_decimal_digits, 30)
                    
                    if precision <= 65:  # MySQL DECIMAL最大精度
                        return "DECIMAL(" + str(precision) + "," + str(scale) + ")"
                except:
                    pass
            
            return "DOUBLE"
        elif pd.api.types.is_datetime64_dtype(series.dtype):
            return "DATETIME"
        else:
            # 检查文本长度
            if series.dtype == 'object':
                # 计算最大字符串长度
                try:
                    max_length = series.astype(str).str.len().max()
                    if max_length is np.nan:
                        max_length = 255
                    
                    if max_length <= 255:
                        # 添加一些额外空间
                        return "VARCHAR(" + str(int(max_length + 50)) + ")"  
                    elif max_length <= 65535:
                        return "TEXT"
                    else:
                        return "LONGTEXT"
                except:
                    return "TEXT"
                    
            return "VARCHAR(255)"

    @staticmethod
    def clean_value_for_mysql(val):
        """
        清理数据值，确保其适合插入到MySQL
        处理None、特殊字符和数据类型转换
        """
        # 处理None和NaN值
        if val is None or pd.isna(val):
            return None
            
        # 处理数值和布尔值
        if isinstance(val, (int, float, bool)):
            return val
            
        # 处理时间类型
        if isinstance(val, (pd.Timestamp, pd.DatetimeTZDtype)):
            return val.to_pydatetime()
            
        # 处理字符串 - 将任何内容转为字符串
        # 这里不需要转义，因为我们使用参数化查询
        return str(val)