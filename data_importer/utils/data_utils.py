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
            col_type = df[col].dtype
            print(f"  '{col}': {col_type}")
            
            # 检查是否有可能导致SQL问题的特殊字符
            if pd.api.types.is_object_dtype(col_type):
                sample = df[col].dropna().astype(str).head(100)
                
                # 检查特殊字符
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
            
            # 增强功能：检测混合数据类型
            non_null = df[col].dropna()
            if len(non_null) > 0:
                sample_values = non_null.head(min(100, len(non_null)))
                type_counts = {}
                
                for val in sample_values:
                    val_type = type(val).__name__
                    type_counts[val_type] = type_counts.get(val_type, 0) + 1
                
                if len(type_counts) > 1:
                    print(f"    警告: 检测到混合数据类型: {type_counts}")
                    # 提供例子
                    examples = {}
                    for val in sample_values[:5]:
                        val_type = type(val).__name__
                        if val_type not in examples:
                            examples[val_type] = val
                    print(f"    类型示例: {examples}")
                
                # 对于字符串类型，检查是否可能是数值型
                if pd.api.types.is_object_dtype(col_type):
                    # 尝试转换为数值
                    numeric_conversion = pd.to_numeric(non_null, errors='coerce')
                    conversion_success_rate = (numeric_conversion.notna().sum() / len(non_null)) * 100
                    
                    if conversion_success_rate > 0 and conversion_success_rate < 100:
                        print(f"    警告: 该列有 {conversion_success_rate:.2f}% 的值可转换为数值型")
                    elif conversion_success_rate == 100:
                        # 检查是否有小数
                        if (numeric_conversion % 1 == 0).all():
                            print(f"    提示: 该列所有值都可以转换为整数")
                        else:
                            print(f"    提示: 该列所有值都可以转换为浮点数")
        
        # 检查缺失值
        null_counts = df.isna().sum()
        if null_counts.sum() > 0:
            print("\n缺失值统计:")
            for col, count in null_counts.items():
                if count > 0:
                    percent = round(count / len(df) * 100, 2)
                    print(f"  '{col}': {count} 缺失值 ({percent}%)")
        
        # 检查数值范围，帮助确定适合的数据类型
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print("\n数值列范围:")
            for col in numeric_cols:
                try:
                    non_null_vals = df[col].dropna()
                    if len(non_null_vals) > 0:
                        min_val = non_null_vals.min()
                        max_val = non_null_vals.max()
                        # 判断是否为整数
                        is_integer = pd.api.types.is_integer_dtype(df[col].dtype) or all(non_null_vals % 1 == 0)
                        
                        # 提供数据类型建议
                        if is_integer:
                            if min_val >= 0:
                                if max_val <= 255:
                                    dtype_suggestion = "TINYINT UNSIGNED"
                                elif max_val <= 65535:
                                    dtype_suggestion = "SMALLINT UNSIGNED"
                                elif max_val <= 4294967295:
                                    dtype_suggestion = "INT UNSIGNED"
                                else:
                                    dtype_suggestion = "BIGINT UNSIGNED"
                            else:
                                if min_val >= -128 and max_val <= 127:
                                    dtype_suggestion = "TINYINT"
                                elif min_val >= -32768 and max_val <= 32767:
                                    dtype_suggestion = "SMALLINT"
                                elif min_val >= -2147483648 and max_val <= 2147483647:
                                    dtype_suggestion = "INT"
                                else:
                                    dtype_suggestion = "BIGINT"
                            print(f"  '{col}': 最小值={min_val}, 最大值={max_val} (建议使用: {dtype_suggestion})")
                        else:
                            print(f"  '{col}': 最小值={min_val}, 最大值={max_val} (浮点数)")
                except Exception as e:
                    print(f"  '{col}': 无法计算范围 - {e}")
        
        # 检查字符串长度
        string_cols = df.select_dtypes(include=['object']).columns
        if len(string_cols) > 0:
            print("\n字符串列长度:")
            for col in string_cols:
                try:
                    non_null_vals = df[col].dropna().astype(str)
                    if len(non_null_vals) > 0:
                        str_lengths = non_null_vals.str.len()
                        max_len = str_lengths.max()
                        avg_len = str_lengths.mean()
                        
                        # 提供数据类型建议
                        if max_len <= 255:
                            suggested_length = min(int(max_len * 1.5), 255)
                            dtype_suggestion = f"VARCHAR({suggested_length})"
                        elif max_len <= 65535:
                            dtype_suggestion = "TEXT"
                        else:
                            dtype_suggestion = "LONGTEXT"
                        
                        print(f"  '{col}': 最大长度={max_len}, 平均长度={avg_len:.1f} (建议使用: {dtype_suggestion})")
                except Exception as e:
                    print(f"  '{col}': 无法计算字符串长度 - {e}")
        
        print("=== 诊断结束 ===\n")

    @staticmethod
    def determine_mysql_type(column_name, series):
        """根据列名和数据确定适合的MySQL数据类型"""
        # 检查是否为空列
        if series.isna().all():
            return "VARCHAR(255)"
            
        # 特殊处理id列，使用BIGINT
        if column_name.lower() == 'id' or column_name.lower().endswith('_id'):
            return "BIGINT"
        
        # 获取非空值
        non_null = series.dropna()
        if len(non_null) == 0:
            return "VARCHAR(255)"
        
        # 检查数据类型的一致性 (新增功能)
        # 抽样检查一些值，看它们的类型是否一致
        sample_values = non_null.head(min(100, len(non_null)))
        sample_types = set()
        for val in sample_values:
            if isinstance(val, (int, np.integer)):
                sample_types.add("integer")
            elif isinstance(val, (float, np.floating)):
                sample_types.add("float")
            elif isinstance(val, (str, np.character)):
                sample_types.add("string")
            elif isinstance(val, (pd.Timestamp, np.datetime64)):
                sample_types.add("datetime")
            else:
                sample_types.add("other")
        
        # 如果检测到混合类型，发出警告
        if len(sample_types) > 1:
            print(f"警告: 列 '{column_name}' 包含混合数据类型: {sample_types}")
            print(f"  将使用更通用的类型以兼容所有值")
            # 如果混合类型包含字符串，优先使用VARCHAR
            if "string" in sample_types:
                # 计算最大字符串长度
                try:
                    str_vals = non_null.astype(str)
                    max_length = str_vals.str.len().max()
                    if max_length is np.nan or max_length <= 0:
                        max_length = 255
                    
                    # 添加一些额外空间
                    return f"VARCHAR({min(int(max_length * 1.5), 65535)})"
                except:
                    return "VARCHAR(255)"
        
        # 检查数值类型
        if pd.api.types.is_integer_dtype(series.dtype):
            # 检查整数范围
            try:
                min_val = non_null.min()
                max_val = non_null.max()
                
                # 检查是否有值超出正常范围
                if pd.isna(min_val) or pd.isna(max_val):
                    return "BIGINT"
                    
                # 更细致的类型判断
                if min_val >= 0:
                    if max_val <= 255:
                        return "TINYINT UNSIGNED"
                    elif max_val <= 65535:
                        return "SMALLINT UNSIGNED"
                    elif max_val <= 16777215:
                        return "MEDIUMINT UNSIGNED"
                    elif max_val <= 4294967295:
                        return "INT UNSIGNED"
                    else:
                        return "BIGINT UNSIGNED"
                else:
                    if min_val >= -128 and max_val <= 127:
                        return "TINYINT"
                    elif min_val >= -32768 and max_val <= 32767:
                        return "SMALLINT"
                    elif min_val >= -8388608 and max_val <= 8388607:
                        return "MEDIUMINT"
                    elif min_val >= -2147483648 and max_val <= 2147483647:
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
                    # 检查是否所有值在一个合理的范围内
                    min_val = non_null.min()
                    max_val = non_null.max()
                    
                    # 检查是否有异常值，如果有，使用DOUBLE
                    if pd.isna(min_val) or pd.isna(max_val) or np.isinf(min_val) or np.isinf(max_val):
                        return "DOUBLE"
                    
                    # 分析各值的整数位数和小数位数
                    str_vals = non_null.apply(lambda x: str(x))
                    
                    # 检查小数位数
                    decimals = str_vals.apply(lambda x: len(x.split('.')[-1]) if '.' in x else 0)
                    max_decimal_digits = decimals.max() if len(decimals) > 0 else 0
                    
                    # 检查整数位数
                    integers = str_vals.apply(lambda x: len(x.split('.')[0]) if '.' in x else len(x))
                    max_int_digits = integers.max() if len(integers) > 0 else 1
                    
                    # 限制精度和小数位
                    precision = min(max_int_digits + max_decimal_digits, 65)
                    scale = min(max_decimal_digits, 30)
                    
                    # 防止精度为0
                    if precision <= 0:
                        precision = 1
                    
                    # 确保精度大于小数位数
                    if scale >= precision:
                        precision = scale + 1
                    
                    # 检查是否都是整数
                    if max_decimal_digits == 0:
                        # 如果都是整数，使用INT类型
                        if max_int_digits <= 10:  # INT最大10位数
                            return "INT"
                        else:
                            return "BIGINT"
                    
                    if precision <= 65:  # MySQL DECIMAL最大精度
                        return f"DECIMAL({precision},{scale})"
                except Exception as e:
                    print(f"DECIMAL类型推断出错，列 '{column_name}': {e}")
                    pass
            
            return "DOUBLE"
        elif pd.api.types.is_datetime64_dtype(series.dtype) or pd.api.types.is_datetime64_ns_dtype(series.dtype):
            # 检查是否需要包含时间部分
            has_time = False
            try:
                for ts in non_null.head(min(100, len(non_null))):
                    if hasattr(ts, 'hour') and (ts.hour != 0 or ts.minute != 0 or ts.second != 0):
                        has_time = True
                        break
                
                if has_time:
                    return "DATETIME"
                else:
                    return "DATE"
            except:
                return "DATETIME"
        elif pd.api.types.is_bool_dtype(series.dtype):
            return "TINYINT(1)"
        else:
            # 处理字符串类型
            if series.dtype == 'object':
                # 尝试转换为字符串计算长度
                try:
                    # 取样本计算最大长度
                    str_sample = non_null.astype(str).head(min(500, len(non_null)))
                    lengths = str_sample.str.len()
                    max_length = lengths.max()
                    
                    # 预留额外空间，但不超过合理范围
                    if max_length is np.nan or max_length <= 0:
                        max_length = 255
                    
                    # 确定适合的字符串类型
                    if max_length <= 255:
                        # 添加一些额外空间，但不超过255
                        return f"VARCHAR({min(int(max_length * 1.5), 255)})"
                    elif max_length <= 65535:
                        return "TEXT"
                    elif max_length <= 16777215:
                        return "MEDIUMTEXT"
                    else:
                        return "LONGTEXT"
                except Exception as e:
                    print(f"字符串类型推断出错，列 '{column_name}': {e}")
                    return "VARCHAR(255)"
            
            # 默认类型
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
        if isinstance(val, (int, float, bool, np.number)):
            # 检查是否为无穷大或NaN
            if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                return None
            return val
            
        # 处理时间类型
        if isinstance(val, (pd.Timestamp, np.datetime64)):
            return val.to_pydatetime()
        elif hasattr(val, 'to_pydatetime'):
            try:
                return val.to_pydatetime()
            except:
                pass
        
        # 确保转换为字符串进行后续处理
        if not isinstance(val, str):
            val = str(val)
        
        # 处理空字符串
        val = val.strip()
        if val == '' or val.lower() in ('null', 'none', 'na', 'nan'):
            return None
            
        # 处理特殊格式
        try:
            # 1. 处理百分比格式，如"220.00%"、"50%"等
            if '%' in val and re.match(r'^-?\s*\d+(\.\d+)?\s*%$', val):
                try:
                    # 去掉百分号和空格，转换为小数（例如：220.00% -> 2.2）
                    percent_value = float(val.strip().rstrip('%').strip()) / 100
                    return percent_value
                except (ValueError, TypeError):
                    pass
                    
            # 2. 处理货币格式，如"$1,234.56"、"￥123"等
            currency_match = re.match(r'^[^\d]*?(-?)[\s]*([0-9,]+(\.\d+)?)[\s]*[^\d]*$', val)
            if currency_match:
                try:
                    sign = -1 if currency_match.group(1) else 1
                    # 移除千位分隔符
                    number_str = currency_match.group(2).replace(',', '')
                    # 解析数值并应用符号
                    return sign * float(number_str)
                except (ValueError, TypeError):
                    pass
                    
            # 3. 尝试解析常见日期格式
            date_patterns = [
                # 年月日 格式
                (r'^(\d{4})[/\-.](\d{1,2})[/\-.](\d{1,2})$', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"),
                # 日月年 格式
                (r'^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})$', lambda m: f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"),
                # 月日年 格式
                (r'^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})$', lambda m: f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"),
                # ISO 格式 (yyyy-mm-ddThh:mm:ss)
                (r'^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$', 
                 lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}:{m.group(6)}")
            ]
            
            for pattern, formatter in date_patterns:
                date_match = re.match(pattern, val)
                if date_match:
                    try:
                        formatted_date = formatter(date_match)
                        parsed_date = pd.to_datetime(formatted_date)
                        return parsed_date.to_pydatetime()
                    except:
                        # 如果日期解析失败，继续尝试其他格式
                        pass
            
            # 4. 检查是否是可转换为数值的字符串
            if re.match(r'^-?\d+(\.\d+)?$', val):
                try:
                    # 判断是整数还是浮点数
                    if '.' in val:
                        return float(val)
                    else:
                        return int(val)
                except (ValueError, TypeError):
                    pass
                
        except Exception as e:
            # 任何转换错误，保留原字符串
            print(f"数据转换警告: {e} (值: {val})")
            
        # 处理超长字符串 - 截断过长的字符串以避免数据库错误
        if len(val) > 65535:  # MySQL TEXT类型的最大长度
            print(f"警告: 截断超长字符串，原长度: {len(val)}")
            val = val[:65535]
            
        # 返回处理后的字符串
        return val