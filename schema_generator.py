#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
schema_generator.py
----------------
Module suy luận lược đồ từ dữ liệu CSV và tạo câu lệnh SQL CREATE TABLE.
Xử lý việc ánh xạ kiểu dữ liệu từ Pandas sang SQL và tạo câu lệnh SQL động.
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Tuple, Optional, Any, Union

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('schema_generator')


class SchemaGenerator:
    """Class suy luận lược đồ và tạo câu lệnh SQL CREATE TABLE."""
    
    # Ánh xạ kiểu dữ liệu Pandas sang SQL
    PANDAS_TO_SQL_TYPE_MAP = {
        'int64': 'INT',
        'int32': 'INT',
        'int16': 'SMALLINT',
        'int8': 'TINYINT',
        'uint64': 'BIGINT UNSIGNED',
        'uint32': 'INT UNSIGNED',
        'uint16': 'SMALLINT UNSIGNED',
        'uint8': 'TINYINT UNSIGNED',
        'float64': 'DOUBLE',
        'float32': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'DATETIME',
        'datetime64': 'DATETIME',
        'object': 'TEXT',  # Mặc định cho chuỗi
        'category': 'VARCHAR(255)',
        'timedelta[ns]': 'TIME',
        'timedelta': 'TIME'
    }
    
    # Danh sách regex cho các kiểu dữ liệu phổ biến
    DATE_PATTERNS = [
        r'\d{4}-\d{2}-\d{2}',          # YYYY-MM-DD
        r'\d{2}/\d{2}/\d{4}',          # MM/DD/YYYY or DD/MM/YYYY
        r'\d{2}-\d{2}-\d{4}',          # MM-DD-YYYY or DD-MM-YYYY
        r'\d{2}\.\d{2}\.\d{4}'         # DD.MM.YYYY or MM.DD.YYYY
    ]
    
    TIME_PATTERNS = [
        r'\d{2}:\d{2}:\d{2}',          # HH:MM:SS
        r'\d{2}:\d{2}'                 # HH:MM
    ]
    
    EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    PHONE_PATTERN = r'^\+?[\d\s\(\)-]{7,20}$'  # Đơn giản hóa mẫu số điện thoại
    
    def __init__(self, df_sample: pd.DataFrame = None, max_varchar_length: int = 255):
        """
        Khởi tạo SchemaGenerator với DataFrame mẫu từ phân tích CSV.
        
        Args:
            df_sample: DataFrame pandas chứa mẫu dữ liệu từ CSV
            max_varchar_length: Độ dài tối đa cho VARCHAR khi không xác định được chính xác
        """
        self.df_sample = df_sample
        self.max_varchar_length = max_varchar_length
        self.column_types = {}  # Lưu trữ kiểu dữ liệu SQL cho mỗi cột
        self.column_attributes = {}  # Lưu trữ thuộc tính khác (nullable, default, v.v.)
    
    def infer_column_types(self, df: pd.DataFrame = None) -> Dict[str, str]:
        """
        Suy luận kiểu dữ liệu SQL cho mỗi cột dựa trên dữ liệu mẫu.
        
        Args:
            df: DataFrame pandas chứa dữ liệu mẫu, nếu None sẽ sử dụng df_sample
            
        Returns:
            Dictionary ánh xạ từ tên cột sang kiểu dữ liệu SQL
        """
        if df is None:
            df = self.df_sample
            
        if df is None:
            raise ValueError("Không có dữ liệu mẫu để suy luận kiểu dữ liệu")
            
        logger.info(f"Suy luận kiểu dữ liệu cho {len(df.columns)} cột")
        
        for column in df.columns:
            sql_type = self._infer_column_type(df[column])
            self.column_types[column] = sql_type
            
            # Xác định xem cột có thể NULL không
            null_count = df[column].isna().sum()
            is_nullable = null_count > 0
            self.column_attributes[column] = {'nullable': is_nullable}
            
            logger.info(f"Cột '{column}': {sql_type} {'NULL' if is_nullable else 'NOT NULL'}")
            
        return self.column_types
    
    def _infer_column_type(self, series: pd.Series) -> str:
        """
        Suy luận kiểu dữ liệu SQL cho một cột cụ thể.
        
        Args:
            series: Pandas Series chứa dữ liệu cột
            
        Returns:
            Kiểu dữ liệu SQL phù hợp
        """
        # Trước tiên xử lý các cột rỗng hoặc toàn NULL
        if series.isna().all():
            return 'TEXT'
        
        # Lấy kiểu dữ liệu Pandas
        pandas_dtype = str(series.dtype)
        
        # Nếu là số nguyên hoặc số thực, sử dụng ánh xạ trực tiếp
        if pandas_dtype in self.PANDAS_TO_SQL_TYPE_MAP:
            return self.PANDAS_TO_SQL_TYPE_MAP[pandas_dtype]
        
        # Nếu là kiểu 'object' (thường là chuỗi), phân tích thêm nội dung
        if pandas_dtype == 'object':
            # Lấy mẫu các giá trị không phải NULL để phân tích
            sample_values = series.dropna().sample(min(10, len(series.dropna()))).tolist()
            
            if not sample_values:  # Không có giá trị để phân tích
                return 'TEXT'
            
            # Kiểm tra xem tất cả có phải là chuỗi không
            if all(isinstance(val, str) for val in sample_values):
                # Phát hiện kiểu dữ liệu phổ biến từ mẫu chuỗi
                
                # Kiểm tra ngày tháng
                date_matches = [
                    all(any(re.match(pattern, str(val)) for pattern in self.DATE_PATTERNS) 
                        for val in sample_values if val and not pd.isna(val))
                ]
                if all(date_matches) and date_matches:
                    return 'DATE'
                
                # Kiểm tra thời gian
                time_matches = [
                    all(any(re.match(pattern, str(val)) for pattern in self.TIME_PATTERNS) 
                        for val in sample_values if val and not pd.isna(val))
                ]
                if all(time_matches) and time_matches:
                    return 'TIME'
                
                # Kiểm tra email
                email_matches = [
                    bool(re.match(self.EMAIL_PATTERN, str(val))) 
                    for val in sample_values if val and not pd.isna(val)
                ]
                if all(email_matches) and email_matches:
                    return 'VARCHAR(100)'
                
                # Kiểm tra số điện thoại
                phone_matches = [
                    bool(re.match(self.PHONE_PATTERN, str(val))) 
                    for val in sample_values if val and not pd.isna(val)
                ]
                if all(phone_matches) and phone_matches:
                    return 'VARCHAR(20)'
                
                # Xác định độ dài VARCHAR dựa trên độ dài tối đa
                max_length = series.dropna().astype(str).str.len().max()
                
                if max_length <= 10:
                    return 'VARCHAR(10)'
                elif max_length <= 50:
                    return 'VARCHAR(50)'
                elif max_length <= 100:
                    return 'VARCHAR(100)'
                elif max_length <= 255:
                    return 'VARCHAR(255)'
                elif max_length <= 1000:
                    return 'TEXT'
                else:
                    return 'LONGTEXT'
                
            # Nếu không xác định được kiểu cụ thể, mặc định là TEXT
            return 'TEXT'
        
        # Mặc định nếu không nhận dạng được
        return 'TEXT'
    
    def generate_create_table_sql(self, table_name: str, if_not_exists: bool = True, 
                                 primary_key: str = None, add_indexes: List[str] = None) -> str:
        """
        Tạo câu lệnh SQL CREATE TABLE dựa trên lược đồ đã suy luận.
        
        Args:
            table_name: Tên bảng cần tạo
            if_not_exists: Thêm IF NOT EXISTS vào câu lệnh
            primary_key: Tên cột sử dụng làm khóa chính
            add_indexes: Danh sách các cột cần tạo chỉ mục
            
        Returns:
            Câu lệnh SQL CREATE TABLE hoàn chỉnh
        """
        if not self.column_types:
            raise ValueError("Chưa suy luận kiểu dữ liệu cột. Hãy gọi infer_column_types() trước.")
            
        # Chuẩn hóa tên bảng (tránh các ký tự đặc biệt)
        table_name = self._sanitize_name(table_name)
            
        # Bắt đầu xây dựng câu lệnh SQL
        sql_parts = [f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}`{table_name}` ("]
        column_definitions = []
        
        for column, sql_type in self.column_types.items():
            # Chuẩn hóa tên cột
            safe_column = self._sanitize_name(column)
            
            # Thêm định nghĩa cột
            is_nullable = self.column_attributes.get(column, {}).get('nullable', True)
            null_clause = "" if is_nullable else " NOT NULL"
            
            # Nếu cột là khóa chính, thêm thuộc tính
            if primary_key and column == primary_key:
                column_definitions.append(f"  `{safe_column}` {sql_type} NOT NULL PRIMARY KEY")
            else:
                column_definitions.append(f"  `{safe_column}` {sql_type}{null_clause}")
        
        # Nếu yêu cầu khóa chính nhưng không có cột nào được chỉ định
        if primary_key and primary_key not in self.column_types:
            # Thêm cột id làm khóa chính
            column_definitions.insert(0, "  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY")
        
        sql_parts.append(",\n".join(column_definitions))
        
        # Thêm các chỉ mục nếu được yêu cầu
        if add_indexes:
            for idx_column in add_indexes:
                if idx_column in self.column_types and idx_column != primary_key:
                    safe_idx_column = self._sanitize_name(idx_column)
                    sql_parts.append(f",\n  INDEX `idx_{safe_idx_column}` (`{safe_idx_column}`)")
        
        sql_parts.append("\n)")
        
        # Thêm engine và charset
        sql_parts.append(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        
        # Kết hợp tất cả các phần thành câu lệnh SQL
        create_table_sql = "".join(sql_parts)
        
        logger.info(f"Đã tạo câu lệnh CREATE TABLE cho bảng '{table_name}'")
        return create_table_sql
    
    def _sanitize_name(self, name: str) -> str:
        """
        Chuẩn hóa tên cột/bảng để tránh các ký tự không hợp lệ.
        
        Args:
            name: Tên cần chuẩn hóa
            
        Returns:
            Tên đã được chuẩn hóa
        """
        # Xóa các ký tự không phải chữ cái, số, hoặc gạch dưới
        sanitized = re.sub(r'[^\w]', '_', str(name))
        
        # Thay thế nhiều dấu gạch dưới liên tiếp bằng một dấu
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Đảm bảo tên không bắt đầu bằng số
        if sanitized and sanitized[0].isdigit():
            sanitized = 'col_' + sanitized
            
        return sanitized


def infer_schema_and_generate_sql(df: pd.DataFrame, table_name: str, 
                                 primary_key: str = None, 
                                 add_indexes: List[str] = None) -> Tuple[Dict[str, str], str]:
    """
    Hàm tiện ích để suy luận lược đồ và tạo câu lệnh SQL.
    
    Args:
        df: DataFrame pandas từ phân tích CSV
        table_name: Tên bảng SQL cần tạo
        primary_key: Tên cột sử dụng làm khóa chính
        add_indexes: Danh sách các cột cần tạo chỉ mục
        
    Returns:
        Tuple (column_types, create_table_sql) chứa kiểu dữ liệu cột và câu lệnh SQL
    """
    generator = SchemaGenerator(df)
    column_types = generator.infer_column_types()
    create_table_sql = generator.generate_create_table_sql(
        table_name, 
        if_not_exists=True,
        primary_key=primary_key,
        add_indexes=add_indexes
    )
    
    return column_types, create_table_sql


if __name__ == "__main__":
    # Kiểm thử script với một DataFrame mẫu
    import csv_analyzer
    import sys
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        test_table = sys.argv[2] if len(sys.argv) > 2 else "example_table"
    else:
        test_file = "data/example.csv"
        test_table = "example_table"
        
    try:
        # Phân tích tệp CSV
        result = csv_analyzer.analyze_csv_file(test_file)
        df_sample = result['sample_data']
        
        # Suy luận lược đồ và tạo SQL
        column_types, create_table_sql = infer_schema_and_generate_sql(
            df_sample, 
            test_table,
            add_indexes=["id", "name"] if "id" in df_sample.columns or "name" in df_sample.columns else None
        )
        
        print("\nKiểu dữ liệu cột:")
        for col, type_sql in column_types.items():
            print(f"- {col}: {type_sql}")
            
        print("\nCâu lệnh SQL CREATE TABLE:")
        print(create_table_sql)
    except Exception as e:
        print(f"Lỗi: {e}")
