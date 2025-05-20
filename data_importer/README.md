# Excel/CSV数据导入MySQL工具

一个用于将Excel或CSV文件数据快速导入到MySQL数据库的工具。

## 功能特点

- 支持Excel (xlsx, xls) 和 CSV文件导入
- 自动推断数据类型
- 智能批处理提高导入速度
- 支持列名映射和数据类型自定义
- 导入进度和性能实时显示
- 详细的导入报告和日志
- 配置文件支持，避免重复输入连接信息

## 配置文件

程序支持从配置文件读取MySQL连接信息，位于 `config` 目录:

1. **JSON格式** (优先): `config/database.json`
2. **INI格式**: `config/database.ini`

当配置文件存在时，程序会询问是否使用已保存的配置。

### 示例配置

JSON格式:
```json
{
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "yourpassword",
        "database": "yourdatabase"
    }
}
```

INI格式:
```ini
[mysql]
host = localhost
port = 3306
user = root
password = yourpassword
database = yourdatabase
```

## 使用方法

1. 运行程序: `python -m data_importer`
2. 选择要导入的Excel或CSV文件
3. 输入或确认MySQL连接信息
4. 检查和确认列映射及数据类型
5. 等待导入完成
6. 查看导入报告