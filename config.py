#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
config.py
--------
Module xử lý cấu hình từ file hoặc biến môi trường.
Cung cấp các thông tin cấu hình như thông tin kết nối cơ sở dữ liệu.
"""

import os
import logging
import json
import yaml
from typing import Dict, Any, Optional, List, Union
from dotenv import load_dotenv
from pathlib import Path

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('config')

# Đường dẫn mặc định đến tệp cấu hình
DEFAULT_CONFIG_PATHS = [
    os.path.join(os.getcwd(), 'config.yaml'),
    os.path.join(os.getcwd(), 'config.yml'),
    os.path.join(os.getcwd(), 'config.json'),
    os.path.join(os.getcwd(), '.env')
]

# Cấu trúc cấu hình mặc định
DEFAULT_CONFIG = {
    'database': {
        'dialect': 'mysql',
        'driver': 'pymysql',
        'host': 'localhost',
        'port': 3306,
        'username': 'root',
        'password': '',
        'database': 'csv_data',
        'charset': 'utf8mb4'
    },
    'csv': {
        'delimiter': None,  # Tự động phát hiện
        'encoding': None,   # Tự động phát hiện
        'chunk_size': 10000,
        'sample_size': 1000
    },
    'table': {
        'if_exists': 'fail'  # 'fail', 'replace', hoặc 'append'
    }
}


class Config:
    """Class quản lý cấu hình ứng dụng."""
    
    def __init__(self, config_file: str = 'config.yaml'):
        """
        Khởi tạo đối tượng Config.
        
        Args:
            config_file (str): Đường dẫn đến tệp cấu hình YAML.
        """
        self.config_file = config_file
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Nạp cấu hình từ tệp YAML.
        
        Returns:
            Dict[str, Any]: Dữ liệu cấu hình.
        """
        try:
            config_path = Path(self.config_file)
            if not config_path.exists():
                logger.warning(f"Tệp cấu hình '{self.config_file}' không tồn tại. Tạo cấu hình mặc định.")
                return self._create_default_config()
                
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.info(f"Đã tải cấu hình từ '{self.config_file}'")
                return config
        except Exception as e:
            logger.error(f"Lỗi khi tải cấu hình: {str(e)}")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """
        Tạo cấu hình mặc định.
        
        Returns:
            Dict[str, Any]: Cấu hình mặc định.
        """
        default_config = {
            "database": {
                "host": "localhost",
                "port": 3306,
                "database": "upload_csv_db",
                "username": "root",
                "password": "",
                "charset": "utf8mb4",
                "driver": "pymysql",
                "echo": False,
                "auto_create_db": True,
                "connect_timeout": 10,
                "pool_size": 5,
                "pool_recycle": 3600
            },
            "csv": {
                "sample_size": 1000,
                "chunk_size": 5000,
                "delimiter": "auto",
                "encoding": "auto",
                "skip_blank_lines": True,
                "skip_bad_lines": True,
                "on_bad_lines": "warn"
            },
            "table": {
                "if_exists": "fail",
                "add_id_column": True,
                "id_column_name": "id",
                "create_indexes": True,
                "create_metadata": True
            },
            "data_types": {
                "text_max_length": 255,
                "date_format": "auto",
                "type_detection": "auto",
                "infer_datetime": True
            },
            "validation": {
                "perform_validation": True,
                "validation_queries": ["row_count", "null_values", "duplicates"],
                "validation_threshold": 95,
                "save_validation_report": True,
                "report_format": "json"
            },
            "logging": {
                "level": "INFO",
                "log_file": "upload_csv.log",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "log_to_console": True,
                "log_to_file": True,
                "max_log_size": 10485760,
                "backup_count": 5
            },
            "error_handling": {
                "dead_letter_queue": True,
                "max_error_rows": 1000,
                "retry_count": 3,
                "retry_interval": 5,
                "error_file_format": "csv",
                "error_dir": "errors"
            },
            "performance": {
                "parallel_processing": False,
                "max_workers": 4,
                "memory_limit": 0,
                "optimize_inserts": True
            },
            "sample_generation": {
                "rows": 1000,
                "columns": 10,
                "null_percentage": 5,
                "include_special_chars": False
            }
        }
        
        # Lưu cấu hình mặc định vào tệp
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            logger.info(f"Đã tạo tệp cấu hình mặc định '{self.config_file}'")
        except Exception as e:
            logger.error(f"Không thể tạo tệp cấu hình mặc định: {str(e)}")
            
        return default_config
    
    def save(self) -> bool:
        """
        Lưu cấu hình hiện tại vào tệp.
        
        Returns:
            bool: True nếu lưu thành công, False nếu lỗi.
        """
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False)
            logger.info(f"Đã lưu cấu hình vào '{self.config_file}'")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi lưu cấu hình: {str(e)}")
            return False
    
    def get(self, section: str, key: Optional[str] = None, default: Any = None) -> Any:
        """
        Lấy giá trị cấu hình.
        
        Args:
            section (str): Phần cấu hình.
            key (Optional[str]): Khóa cấu hình. Nếu None, trả về toàn bộ phần.
            default (Any): Giá trị mặc định nếu không tìm thấy.
            
        Returns:
            Any: Giá trị cấu hình hoặc giá trị mặc định.
        """
        if section not in self.config:
            return default
        
        if key is None:
            return self.config[section]
        
        return self.config[section].get(key, default)
    
    def set(self, section: str, key: str, value: Any) -> None:
        """
        Đặt giá trị cấu hình.
        
        Args:
            section (str): Phần cấu hình.
            key (str): Khóa cấu hình.
            value (Any): Giá trị cấu hình.
        """
        if section not in self.config:
            self.config[section] = {}
        
        self.config[section][key] = value
        logger.debug(f"Đã cập nhật cấu hình: {section}.{key} = {value}")
    
    def get_connection_string(self, include_db: bool = True) -> str:
        """
        Tạo chuỗi kết nối cơ sở dữ liệu.
        
        Args:
            include_db (bool): Có bao gồm tên cơ sở dữ liệu hay không.
            
        Returns:
            str: Chuỗi kết nối SQLAlchemy.
        """
        db_config = self.get('database')
        driver = db_config.get('driver', 'pymysql')
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 3306)
        user = db_config.get('username', 'root')
        password = db_config.get('password', '')
        charset = db_config.get('charset', 'utf8mb4')
        database = db_config.get('database', 'upload_csv_db') if include_db else ''
        
        # Xây dựng chuỗi kết nối
        if password:
            auth = f"{user}:{password}"
        else:
            auth = user
            
        if include_db and database:
            conn_str = f"mysql+{driver}://{auth}@{host}:{port}/{database}?charset={charset}"
        else:
            conn_str = f"mysql+{driver}://{auth}@{host}:{port}?charset={charset}"
            
        return conn_str
    
    def get_db_name(self) -> str:
        """
        Lấy tên cơ sở dữ liệu.
        
        Returns:
            str: Tên cơ sở dữ liệu.
        """
        return self.get('database', 'database', 'upload_csv_db')
    
    def should_auto_create_db(self) -> bool:
        """
        Kiểm tra xem có nên tự động tạo cơ sở dữ liệu hay không.
        
        Returns:
            bool: True nếu nên tự động tạo, False nếu không.
        """
        return self.get('database', 'auto_create_db', True)
    
    def get_sqlalchemy_options(self) -> Dict[str, Any]:
        """
        Lấy các tùy chọn cho SQLAlchemy.
        
        Returns:
            Dict[str, Any]: Các tùy chọn cho SQLAlchemy.
        """
        db_config = self.get('database')
        options = {
            'echo': db_config.get('echo', False),
            'pool_size': db_config.get('pool_size', 5),
            'pool_recycle': db_config.get('pool_recycle', 3600),
            'connect_args': {
                'connect_timeout': db_config.get('connect_timeout', 10)
            }
        }
        return options
    
    def get_pandas_read_csv_options(self) -> Dict[str, Any]:
        """
        Lấy các tùy chọn cho pandas.read_csv.
        
        Returns:
            Dict[str, Any]: Các tùy chọn cho pandas.read_csv.
        """
        csv_config = self.get('csv')
        options = {
            'chunksize': csv_config.get('chunk_size', 5000),
            'skip_blank_lines': csv_config.get('skip_blank_lines', True),
            'on_bad_lines': csv_config.get('on_bad_lines', 'warn'),
        }
        
        # Xử lý delimiter
        delimiter = csv_config.get('delimiter', 'auto')
        if delimiter != 'auto':
            options['delimiter'] = delimiter
            
        # Xử lý encoding
        encoding = csv_config.get('encoding', 'auto')
        if encoding != 'auto':
            options['encoding'] = encoding
            
        return options
    
    def get_all(self) -> Dict[str, Any]:
        """
        Lấy toàn bộ cấu hình.
        
        Returns:
            Dict[str, Any]: Toàn bộ cấu hình.
        """
        return self.config.copy()


# Biến toàn cục lưu trữ instance Config
_config_instance = None

def get_config(config_file: str = 'config.yaml') -> Config:
    """
    Lấy instance Config, tạo mới nếu chưa tồn tại.
    
    Args:
        config_file (str): Đường dẫn đến tệp cấu hình YAML.
        
    Returns:
        Instance Config.
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = Config(config_file)
        
    return _config_instance

def init_config(config_file: str = 'config.yaml') -> Config:
    """
    Khởi tạo cấu hình với đường dẫn cụ thể.
    Sử dụng hàm này để đảm bảo tạo mới instance Config.
    
    Args:
        config_file (str): Đường dẫn đến tệp cấu hình YAML.
        
    Returns:
        Instance Config đã được khởi tạo.
    """
    global _config_instance
    
    # Tạo mới instance Config
    _config_instance = Config(config_file)
    
    return _config_instance


if __name__ == "__main__":
    # Kiểm thử script
    config = get_config()
    
    print("\nCấu hình Cơ sở dữ liệu:")
    for key, value in config.get('database').items():
        print(f"- {key}: {value}")
        
    print("\nCấu hình CSV:")
    for key, value in config.get('csv').items():
        print(f"- {key}: {value}")
        
    print("\nCấu hình Bảng:")
    for key, value in config.get('table').items():
        print(f"- {key}: {value}")
