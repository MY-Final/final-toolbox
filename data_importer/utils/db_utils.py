"""
数据库操作工具类
包含MySQL连接、表创建和数据导入等功能
"""
import pymysql
import os
import time
import traceback
import pandas as pd
from tkinter import messagebox
from data_importer.utils.data_utils import DataUtils

class DbUtils:
    @staticmethod
    def escape_sql_identifier(identifier):
        """
        安全地转义SQL标识符（表名、列名等）
        """
        if identifier is None:
            return "NULL"
        # 移除任何已有的反引号并替换为空格，然后用反引号包围
        return "`" + str(identifier).replace("`", " ") + "`"

    @staticmethod
    def execute_create_table(conn, table_name, column_defs):
        """
        直接执行CREATE TABLE语句，避免所有格式化问题
        """
        cursor = conn.cursor()
        try:
            # 使用封装好的转义函数处理表名
            escaped_table_name = DbUtils.escape_sql_identifier(table_name)
            
            # 构建SQL - 使用列表拼接避免任何格式化问题
            sql_parts = []
            sql_parts.append("CREATE TABLE IF NOT EXISTS")
            sql_parts.append(escaped_table_name)
            sql_parts.append("(")
            sql_parts.append(", ".join(column_defs))
            sql_parts.append(")")
            sql_parts.append("CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            # 组合SQL - 使用join而不是+拼接避免格式化问题
            create_sql = " ".join(sql_parts)
            
            print("SQL创建表语句:")
            print(create_sql)
            
            # 执行SQL
            cursor.execute(create_sql)
            conn.commit()
            print("表创建成功: " + str(table_name))
            return True
        except Exception as e:
            print("创建表失败: " + str(e))
            print("错误类型: " + str(type(e)))
            print("SQL: " + create_sql if 'create_sql' in locals() else "SQL未生成")
            print(traceback.format_exc())
            return False

    @staticmethod
    def execute_insert(conn, table_name, columns, values):
        """
        直接执行INSERT语句，避免所有格式化问题
        """
        cursor = conn.cursor()
        try:
            # 使用封装好的转义函数处理表名
            escaped_table = DbUtils.escape_sql_identifier(table_name)
            
            # 转义列名
            escaped_columns = [DbUtils.escape_sql_identifier(col) for col in columns]
            
            # 直接使用参数化构建SQL，完全避免任何%字符问题
            sql_parts = []
            sql_parts.append("INSERT INTO")
            sql_parts.append(escaped_table)
            sql_parts.append("(")
            sql_parts.append(", ".join(escaped_columns))
            sql_parts.append(") VALUES (")
            
            # 添加占位符
            placeholders = []
            for _ in range(len(columns)):
                placeholders.append("%s")
            sql_parts.append(", ".join(placeholders))
            sql_parts.append(")")
            
            # 组合SQL语句
            sql = " ".join(sql_parts)
            
            # 处理值列表，确保所有值都是有效的
            clean_values = []
            for val in values:
                # 处理特殊值类型，避免插入问题
                if isinstance(val, (int, float)) or val is None:
                    clean_values.append(val)
                else:
                    # 确保字符串值正确处理
                    clean_values.append(str(val))
                    
            # 执行SQL，使用参数化查询而不是字符串拼接
            cursor.execute(sql, clean_values)
            return True
        except Exception as e:
            print("行插入失败: " + str(e))
            print("错误类型: " + str(type(e)))
            print("SQL: " + sql if 'sql' in locals() else "SQL未生成")
            print("值数量: " + str(len(values)) + ", 列数量: " + str(len(columns)))
            # 只打印前几个值，避免输出过长
            print("值前几项: " + str(values[:3]) + "..." if len(values) > 3 else str(values))
            # 检查是否有特殊字符
            special_chars_found = False
            for i, val in enumerate(values[:5]):
                if isinstance(val, str):
                    if "%" in val:
                        print(f"值 {i} 中包含百分号: {val}")
                        special_chars_found = True
                    if "`" in val:
                        print(f"值 {i} 中包含反引号: {val}")
                        special_chars_found = True
            if not special_chars_found and len(values) > 5:
                print("未在前5个值中检测到特殊字符，可能在其他值中")
            return False

    @staticmethod
    def create_database_from_file(file_path, mysql_conn_info, load_data_file_func):
        """从数据文件创建MySQL数据库表并导入数据"""
        # 加载数据文件
        df = load_data_file_func(file_path)
        
        if df is None:
            print("无法加载数据文件")
            return None
        
        # 调试数据结构
        DataUtils.debug_data_structure(df)
        
        # 检测和修复数据完整性问题
        print("检测和修复数据完整性问题...")
        
        # 确保没有重复列名
        if len(df.columns) != len(set(df.columns)):
            print("重命名重复列...")
            # 添加后缀以确保列名唯一
            new_columns = []
            seen = set()
            for i, col in enumerate(df.columns):
                if col in seen:
                    count = 1
                    new_col = col + "_" + str(count)
                    while new_col in seen:
                        count += 1
                        new_col = col + "_" + str(count)
                    new_columns.append(new_col)
                    seen.add(new_col)
                    print("  重命名: '" + str(col) + "' -> '" + str(new_col) + "'")
                else:
                    new_columns.append(col)
                    seen.add(col)
            df.columns = new_columns
        
        # 表名基于文件名，添加时间戳确保唯一
        base_table_name = os.path.splitext(os.path.basename(file_path))[0]
        # 移除非字母数字字符
        clean_base_name = ''.join(c if c.isalnum() else '_' for c in base_table_name)
        timestamp = str(int(time.time()))
        table_name = clean_base_name + "_" + timestamp
        print("将创建表: " + table_name)
        
        conn = None
        try:
            # 连接到MySQL数据库
            conn = pymysql.connect(
                host=mysql_conn_info["host"],
                port=mysql_conn_info["port"],
                user=mysql_conn_info["user"],
                password=mysql_conn_info["password"],
                database=mysql_conn_info["database"],
                charset='utf8mb4',
                use_unicode=True
            )
            print("已连接到MySQL数据库: " + mysql_conn_info["database"])
            
            # 从表头创建表结构
            columns = df.columns.tolist()
            
            # 确定每列的数据类型
            column_defs = []
            column_types = []
            for col in columns:
                type_str = DataUtils.determine_mysql_type(col, df[col])
                column_types.append((col, type_str))
                
                # 创建列定义字符串 - 使用转义函数
                escaped_col = DbUtils.escape_sql_identifier(col)
                column_defs.append(escaped_col + " " + type_str)
            
            # 创建表
            if not DbUtils.execute_create_table(conn, table_name, column_defs):
                if conn:
                    conn.close()
                return None
            
            # 将数据写入表中
            print("正在导入数据...")
            
            # 确认列数
            col_count = len(columns)
            print("列数: " + str(col_count))
            
            # 使用逐行插入方法，避免格式化问题
            print("切换到逐行插入模式...")
            rows_inserted = 0
            total_rows = len(df)
            error_rows = 0
            
            # 分批提交，减轻数据库负载
            batch_size = 100
            
            for i, (_, row) in enumerate(df.iterrows()):
                try:
                    # 提取行数据并处理NaN值
                    values = []
                    for col in columns:
                        val = row[col]
                        # 使用专门的函数处理值
                        clean_val = DataUtils.clean_value_for_mysql(val)
                        values.append(clean_val)
                    
                    # 确保值的数量与列数相同
                    if len(values) != len(columns):
                        print("警告: 行 " + str(i) + " 数据不完整, 预期 " + str(len(columns)) + " 列, 实际 " + str(len(values)) + " 列, 已跳过")
                        error_rows += 1
                        continue
                    
                    # 执行插入
                    if DbUtils.execute_insert(conn, table_name, columns, values):
                        rows_inserted += 1
                    else:
                        error_rows += 1
                    
                    # 每batch_size行提交一次
                    if (i + 1) % batch_size == 0:
                        conn.commit()
                        # 使用f-string而不是%格式化，避免%字符问题
                        print(f"已导入 {i + 1}/{total_rows} 行数据 (成功: {rows_inserted}, 失败: {error_rows})")
                    
                except Exception as e:
                    print("行 " + str(i) + " 处理失败: " + str(e))
                    print(traceback.format_exc())
                    error_rows += 1
                    # 如果连续出现多次错误，可能需要中断操作
                    if error_rows > 10 and error_rows / (i + 1) > 0.5:  # 如果错误率超过50%
                        print("错误率过高，中断导入操作")
                        break
            
            # 最后提交任何未提交的更改
            conn.commit()
            
            print(f"数据导入完成: 成功导入 {rows_inserted}/{total_rows} 行数据, 失败: {error_rows}")
            
            return table_name
            
        except Exception as e:
            print(f"数据库操作出错: {e}")
            print(traceback.format_exc())
            messagebox.showerror("数据库错误", f"导入数据时出错:\n{e}")
            return None
        finally:
            # 确保无论如何都关闭连接
            if conn:
                conn.close()