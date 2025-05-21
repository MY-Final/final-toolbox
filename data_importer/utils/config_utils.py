"""
配置文件工具类
用于读取和保存配置信息，减少手动输入
"""
import os
import json
import configparser

class ConfigUtils:
    # 配置文件默认路径
    CONFIG_DIR = os.path.join(os.getcwd(), 'config')
    CONFIG_FILE = os.path.join(CONFIG_DIR, 'database.ini')
    JSON_CONFIG_FILE = os.path.join(CONFIG_DIR, 'database.json')
    
    @staticmethod
    def ensure_config_dir():
        """确保配置目录存在"""
        if not os.path.exists(ConfigUtils.CONFIG_DIR):
            os.makedirs(ConfigUtils.CONFIG_DIR)
    
    @staticmethod
    def save_mysql_config(config):
        """
        保存MySQL连接配置到配置文件
        config: 包含host, port, user, password, database的字典
        """
        ConfigUtils.ensure_config_dir()
        
        # 使用INI格式保存
        parser = configparser.ConfigParser()
        parser['mysql'] = {
            'host': config.get('host', 'localhost'),
            'port': str(config.get('port', 3306)),
            'user': config.get('user', 'root'),
            'password': config.get('password', ''),
            'database': config.get('database', '')
        }
        
        with open(ConfigUtils.CONFIG_FILE, 'w') as f:
            parser.write(f)
        
        # 同时保存为JSON格式，提供更好的兼容性
        with open(ConfigUtils.JSON_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'mysql': {
                    'host': config.get('host', 'localhost'),
                    'port': int(config.get('port', 3306)),
                    'user': config.get('user', 'root'),
                    'password': config.get('password', ''),
                    'database': config.get('database', '')
                }
            }, f, indent=4)
        
        print(f"MySQL配置已保存到: {ConfigUtils.CONFIG_FILE}")
        return True
    
    @staticmethod
    def load_mysql_config():
        """
        从配置文件加载MySQL连接配置
        返回: 包含host, port, user, password, database的字典，如果文件不存在返回None
        """
        # 优先检查JSON配置文件
        if os.path.exists(ConfigUtils.JSON_CONFIG_FILE):
            try:
                with open(ConfigUtils.JSON_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                if 'mysql' in config:
                    mysql_config = config['mysql']
                    return {
                        'host': mysql_config.get('host', 'localhost'),
                        'port': int(mysql_config.get('port', 3306)),
                        'user': mysql_config.get('user', 'root'),
                        'password': mysql_config.get('password', ''),
                        'database': mysql_config.get('database', ''),
                        'success': True
                    }
            except Exception as e:
                print(f"读取JSON配置文件出错: {e}")
        
        # 尝试读取INI配置文件
        if os.path.exists(ConfigUtils.CONFIG_FILE):
            try:
                parser = configparser.ConfigParser()
                parser.read(ConfigUtils.CONFIG_FILE)
                
                if 'mysql' in parser:
                    return {
                        'host': parser['mysql'].get('host', 'localhost'),
                        'port': int(parser['mysql'].get('port', '3306')),
                        'user': parser['mysql'].get('user', 'root'),
                        'password': parser['mysql'].get('password', ''),
                        'database': parser['mysql'].get('database', ''),
                        'success': True
                    }
            except Exception as e:
                print(f"读取INI配置文件出错: {e}")
        
        # 如果没有找到配置文件或读取出错，返回None
        return None 