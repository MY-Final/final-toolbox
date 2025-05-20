"""
数据库操作工具类
包含MySQL连接、表创建和数据导入等功能
"""
import pymysql
import os
import time
import traceback
import pandas as pd
import logging
import datetime
from tkinter import messagebox
from data_importer.utils.data_utils import DataUtils
from data_importer.utils.ui_utils import UiUtils
from tqdm import tqdm
import numpy as np

class DbUtils:
    @staticmethod
    def setup_logger(table_name):
        """设置日志记录器"""
        # 确保logs目录存在
        log_dir = os.path.join(os.getcwd(), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # 创建日志文件名，包含表名和时间戳
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"import_{table_name}_{timestamp}.log")
        
        # 配置日志记录器
        logger = logging.getLogger(f"import_{table_name}")
        logger.setLevel(logging.INFO)
        
        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 创建格式化器
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        # 打印日志路径
        print(f"正在记录日志到文件: {log_file}")
        
        return logger

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
        
        # 保存原始列名，用于后续映射展示
        original_columns = df.columns.tolist()
        
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
        
        # 设置日志记录器
        logger = DbUtils.setup_logger(clean_base_name)
        logger.info(f"开始导入文件: {file_path}")
        logger.info(f"目标表名: {table_name}")
        logger.info(f"数据行数: {len(df)}, 列数: {len(df.columns)}")
        
        conn = None
        try:
            # 连接到MySQL数据库
            logger.info(f"连接到MySQL数据库: {mysql_conn_info['database']}@{mysql_conn_info['host']}:{mysql_conn_info['port']}")
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
                
            # 添加列名映射和数据类型预览
            column_mappings = []
            for i, (orig, curr) in enumerate(zip(original_columns, columns)):
                if str(orig) != str(curr):
                    column_mappings.append((orig, curr, column_types[i][1]))
                else:
                    column_mappings.append((orig, curr, column_types[i][1]))
            
            # 记录列类型到日志
            logger.info("自动推断的列数据类型:")
            for col, type_str in column_types:
                logger.info(f"  '{col}': {type_str}")
            
            # 创建预览信息
            preview_info = "列名映射和数据类型预览:\n\n"
            preview_info += "原始列名 -> 数据库列名 (MySQL类型)\n"
            preview_info += "----------------------------------------\n"
            for orig, curr, type_str in column_mappings:
                if str(orig) != str(curr):
                    preview_info += f"{orig} -> {curr} ({type_str}) ← 已修改\n"
                else:
                    preview_info += f"{orig} -> {curr} ({type_str})\n"
            
            # 通过对话框显示预览信息并请求确认，允许修改数据类型
            result = UiUtils.confirm_column_mapping(preview_info, table_name, column_mappings)
            
            if not result["confirmed"]:
                logger.info("用户取消了导入操作")
                print("用户取消了导入操作")
                if conn:
                    conn.close()
                return None
            
            # 应用用户修改的数据类型
            modified_types = result["types"]
            
            # 记录用户修改的类型到日志
            if modified_types:
                logger.info("用户修改的数据类型:")
                for col, type_str in modified_types.items():
                    logger.info(f"  '{col}': {type_str}")
            
            # 重建column_defs，应用用户修改的数据类型
            column_defs = []
            updated_column_types = []
            
            for col in columns:
                # 使用用户修改的类型或保持原来的类型
                if col in modified_types:
                    type_str = modified_types[col]
                    print(f"应用用户修改的类型: '{col}' -> {type_str}")
                else:
                    # 查找原始类型
                    type_str = next((t for c, t in column_types if c == col), "VARCHAR(255)")
                
                updated_column_types.append((col, type_str))
                
                # 创建列定义字符串 - 使用转义函数
                escaped_col = DbUtils.escape_sql_identifier(col)
                column_defs.append(escaped_col + " " + type_str)
            
            # 更新column_mappings用于报告
            for i, (orig, curr, _) in enumerate(column_mappings):
                # 查找更新后的类型
                updated_type = next((t for c, t in updated_column_types if c == curr), "VARCHAR(255)")
                column_mappings[i] = (orig, curr, updated_type)
            
            # 创建表
            logger.info("开始创建表...")
            if not DbUtils.execute_create_table(conn, table_name, column_defs):
                logger.error("表创建失败")
                if conn:
                    conn.close()
                return None
            
            # 将数据写入表中
            logger.info("开始导入数据...")
            print("正在导入数据...")
            
            # 确认列数
            col_count = len(columns)
            print("列数: " + str(col_count))
            
            # 使用逐行插入方法，避免格式化问题
            print("切换到智能批处理导入模式...")
            rows_inserted = 0
            total_rows = len(df)
            error_rows = 0
            
            # 错误详情记录
            errors_detail = []
            
            # 智能批处理参数
            initial_batch_size = 100
            current_batch_size = initial_batch_size
            min_batch_size = 20
            max_batch_size = 500
            
            # 性能追踪
            speed_history = []
            start_time = time.time()
            batch_start_time = start_time
            
            # 使用tqdm创建进度条
            with tqdm(total=total_rows, desc="数据导入进度", unit="行", ncols=100) as pbar:
                i = 0
                while i < total_rows:
                    # 确定当前批次的结束索引
                    end_idx = min(i + current_batch_size, total_rows)
                    batch_size = end_idx - i
                    
                    # 处理当前批次
                    batch_success = 0
                    batch_errors = 0
                    
                    # 记录批次开始时间
                    batch_start_time = time.time()
                    
                    for idx in range(i, end_idx):
                        _, row = next(iter_rows) if 'iter_rows' in locals() else df.iloc[idx:idx+1].iterrows().__next__()
                        
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
                                error_msg = f"行 {idx} 数据不完整, 预期 {len(columns)} 列, 实际 {len(values)} 列"
                                logger.warning(error_msg)
                                error_rows += 1
                                batch_errors += 1
                                errors_detail.append((idx, error_msg))
                                continue
                            
                            # 执行插入
                            if DbUtils.execute_insert(conn, table_name, columns, values):
                                rows_inserted += 1
                                batch_success += 1
                            else:
                                error_msg = f"行 {idx} 插入失败"
                                logger.warning(error_msg)
                                error_rows += 1
                                batch_errors += 1
                                errors_detail.append((idx, error_msg))
                        
                        except Exception as e:
                            error_msg = f"行 {idx} 处理失败: {e}"
                            logger.error(error_msg)
                            logger.error(traceback.format_exc())
                            error_rows += 1
                            batch_errors += 1
                            errors_detail.append((idx, str(e)))
                            
                            # 如果连续出现多次错误，可能需要中断操作
                            if error_rows > 10 and error_rows / (idx + 1) > 0.5:  # 如果错误率超过50%
                                abort_msg = "错误率过高，中断导入操作"
                                logger.error(abort_msg)
                                pbar.write(abort_msg)  # 使用pbar.write替代print
                                break
                    
                    # 批次完成后，提交事务
                    conn.commit()
                    
                    # 计算批次处理时间和速度
                    batch_time = time.time() - batch_start_time
                    batch_speed = batch_size / batch_time if batch_time > 0 else 0  # 行/秒
                    
                    # 记录历史速度用于平滑计算
                    speed_history.append(batch_speed)
                    if len(speed_history) > 10:  # 只保留最近10个批次的速度
                        speed_history.pop(0)
                    
                    # 计算平均速度和预估剩余时间
                    avg_speed = np.mean(speed_history) if speed_history else batch_speed
                    rows_remaining = total_rows - (i + batch_size)
                    eta_seconds = rows_remaining / avg_speed if avg_speed > 0 else 0
                    
                    # 格式化ETA
                    if eta_seconds < 60:
                        eta_str = f"{eta_seconds:.0f}秒"
                    elif eta_seconds < 3600:
                        eta_str = f"{eta_seconds/60:.1f}分钟"
                    else:
                        eta_str = f"{eta_seconds/3600:.1f}小时"
                    
                    # 计算成功率
                    success_rate = (rows_inserted / (i + batch_size)) * 100 if (i + batch_size) > 0 else 0
                    
                    # 动态调整批处理大小
                    if batch_time > 0:
                        # 目标批处理时间为2秒
                        target_time = 2.0
                        ideal_batch_size = int(current_batch_size * (target_time / batch_time))
                        
                        # 限制批处理大小的变化幅度
                        adjustment_factor = min(max(ideal_batch_size / current_batch_size, 0.8), 1.5)
                        new_batch_size = int(current_batch_size * adjustment_factor)
                        
                        # 确保批处理大小在允许范围内
                        current_batch_size = max(min_batch_size, min(new_batch_size, max_batch_size))
                    
                    # 更新进度条
                    pbar.update(batch_size)
                    
                    # 更新进度条描述
                    progress_desc = f"已导入: {rows_inserted}/{i+batch_size} | 速度: {avg_speed:.1f}行/秒 | 批次: {current_batch_size} | ETA: {eta_str}"
                    pbar.set_description(progress_desc)
                    
                    # 记录到日志
                    progress_msg = f"已导入 {i+batch_size}/{total_rows} 行 (成功: {rows_inserted}, 失败: {error_rows}) | 批次大小: {current_batch_size} | 速度: {avg_speed:.1f}行/秒"
                    logger.info(progress_msg)
                    
                    # 移动到下一批
                    i += batch_size
            
            # 记录详细的错误信息
            if errors_detail:
                logger.warning("导入过程中遇到的错误详情:")
                for row_idx, error_msg in errors_detail:
                    logger.warning(f"  行 {row_idx}: {error_msg}")
            
            # 计算总运行时间
            total_time = time.time() - start_time
            avg_speed = rows_inserted / total_time if total_time > 0 else 0
            
            # 生成导入报告
            report = DbUtils.generate_import_report(table_name, column_mappings, rows_inserted, total_rows, error_rows)
            # 添加性能数据到报告
            report["performance"] = {
                "total_time_seconds": total_time,
                "average_speed": avg_speed,
                "final_batch_size": current_batch_size
            }
            UiUtils.show_import_report(report)
            
            # 格式化总时间
            if total_time < 60:
                time_str = f"{total_time:.1f}秒"
            elif total_time < 3600:
                time_str = f"{total_time/60:.1f}分钟"
            else:
                time_str = f"{total_time/3600:.1f}小时"
            
            summary_msg = f"数据导入完成: 成功导入 {rows_inserted}/{total_rows} 行数据, 失败: {error_rows}, 用时: {time_str}, 平均速度: {avg_speed:.1f}行/秒"
            logger.info(summary_msg)
            logger.info("导入操作结束")
            print(summary_msg)  # 保留最终总结信息的打印
            
            return table_name
        
        except Exception as e:
            error_msg = f"数据库操作出错: {e}"
            if 'logger' in locals():
                logger.error(error_msg)
                logger.error(traceback.format_exc())
            print(error_msg)
            print(traceback.format_exc())
            messagebox.showerror("数据库错误", f"导入数据时出错:\n{e}")
            return None
        finally:
            # 确保无论如何都关闭连接
            if conn:
                conn.close()

    @staticmethod
    def generate_import_report(table_name, column_mappings, rows_inserted, total_rows, error_rows):
        """生成导入报告，包含列映射和导入统计信息"""
        success_rate = (rows_inserted / total_rows) * 100 if total_rows > 0 else 0
        
        report = {
            "table_name": table_name,
            "column_mappings": column_mappings,
            "total_rows": total_rows,
            "rows_inserted": rows_inserted,
            "error_rows": error_rows,
            "success_rate": success_rate
        }
        
        return report