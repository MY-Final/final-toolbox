"""
配置管理器
用于处理程序配置的保存和加载
"""

import json
import os
from pathlib import Path

class ConfigManager:
    CONFIG_FILE = "config/settings.json"
    
    @staticmethod
    def get_config_path():
        """获取配置文件路径"""
        # 获取程序所在目录
        base_dir = Path(__file__).parent.parent.parent
        return os.path.join(base_dir, ConfigManager.CONFIG_FILE)
    
    @staticmethod
    def load_config():
        """加载配置"""
        try:
            config_path = ConfigManager.get_config_path()
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            print(f"加载配置文件失败: {str(e)}")
            return {}
    
    @staticmethod
    def save_config(config):
        """保存配置"""
        try:
            config_path = ConfigManager.get_config_path()
            # 确保配置目录存在
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"保存配置文件失败: {str(e)}")
            return False
    
    @staticmethod
    def save_db_config(host, port, database, user):
        """保存数据库配置"""
        config = ConfigManager.load_config()
        config['database'] = {
            'host': host,
            'port': port,
            'database': database,
            'user': user
        }
        return ConfigManager.save_config(config)
    
    @staticmethod
    def get_db_config():
        """获取数据库配置"""
        config = ConfigManager.load_config()
        return config.get('database', {}) 