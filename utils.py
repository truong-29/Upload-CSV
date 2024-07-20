#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
utils.py
------------
Module chứa các tiện ích và hàm hỗ trợ cho dự án Upload-CSV.
"""

import os
import re
import logging
import time
import chardet
from typing import List, Dict, Any, Optional, Tuple, Union
import pandas as pd
import numpy as np
from datetime import datetime

# Cấu hình logging cơ bản
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('utils')

def format_file_size(size_in_bytes: int) -> str:
    """
    Định dạng kích thước tệp (bytes) thành chuỗi dễ đọc.
    
    Args:
        size_in_bytes: Kích thước tệp tính bằng bytes.
        
    Returns:
        Chuỗi đã định dạng, ví dụ: "1.23 MB"
    """
    if size_in_bytes < 1024:
        return f"{size_in_bytes} B"
    elif size_in_bytes < 1024 * 1024:
        return f"{size_in_bytes/1024:.2f} KB"
    elif size_in_bytes < 1024 * 1024 * 1024:
        return f"{size_in_bytes/(1024*1024):.2f} MB"
    else:
        return f"{size_in_bytes/(1024*1024*1024):.2f} GB"

def normalize_column_name(name: str) -> str:
    """
    Chuẩn hóa tên cột để phù hợp với quy tắc đặt tên của SQL.
    
    Args:
        name: Tên cột ban đầu.
        
    Returns:
        Tên cột đã chuẩn hóa.
    """
    # Xử lý trường hợp name là None
    if name is None:
        return "unnamed_column"
        
    # Loại bỏ các ký tự đặc biệt, thay thế bằng dấu gạch dưới
    name = re.sub(r'[^\w\s]', '_', str(name))
    # Thay thế khoảng trắng bằng dấu gạch dưới
    name = re.sub(r'\s+', '_', name)
    # Đảm bảo tên bắt đầu bằng chữ cái hoặc dấu gạch dưới
    if name and not re.match(r'^[a-zA-Z_]', name):
        name = f"col_{name}"
    # Chuyển đổi thành chữ thường
    return name.lower()

def get_estimated_row_count(file_path: str, sample_size: int = 1000) -> int:
    """
    Ước tính số hàng trong tệp CSV.
    
    Args:
        file_path: Đường dẫn đến tệp CSV.
        sample_size: Số lượng hàng để lấy mẫu.
        
    Returns:
        Số hàng ước tính.
    """
    try:
        file_size = os.path.getsize(file_path)
        
        # Nếu tệp rỗng, trả về 0
        if file_size == 0:
            return 0
        
        # Đọc một mẫu để ước tính kích thước trung bình mỗi hàng
        with open(file_path, 'rb') as f:
            sample_data = f.read(sample_size * 100)  # Đọc đủ dữ liệu cho sample_size hàng
        
        sample_lines = sample_data.count(b'\n')
        if sample_lines == 0:
            return 1  # Giả sử có ít nhất 1 hàng nếu không tìm thấy newline
        
        avg_line_size = len(sample_data) / sample_lines
        estimated_rows = int(file_size / avg_line_size)
        
        return max(1, estimated_rows)  # Trả về ít nhất 1 hàng
    except Exception as e:
        logger.error(f"Lỗi khi ước tính số hàng: {e}")
        return 0

def detect_encoding(file_path: str, sample_size: int = 10000) -> str:
    """
    Phát hiện mã hóa của tệp văn bản.
    
    Args:
        file_path: Đường dẫn đến tệp văn bản.
        sample_size: Kích thước mẫu để phân tích (byte).
        
    Returns:
        Mã hóa phát hiện được hoặc 'utf-8' nếu không xác định được.
    """
    try:
        with open(file_path, 'rb') as f:
            sample_data = f.read(sample_size)
            
        result = chardet.detect(sample_data)
        encoding = result['encoding']
        confidence = result['confidence']
        
        logger.info(f"Phát hiện mã hóa: {encoding} (độ tin cậy: {confidence:.2f})")
        
        # Kiểm tra độ tin cậy và trả về utf-8 nếu không đủ tin cậy
        if confidence < 0.7 or encoding is None:
            logger.warning("Độ tin cậy thấp, sử dụng utf-8 làm mặc định")
            return 'utf-8'
            
        return encoding
    except Exception as e:
        logger.error(f"Lỗi khi phát hiện mã hóa: {e}")
        return 'utf-8'

def is_likely_datetime(series: pd.Series) -> bool:
    """
    Kiểm tra xem một chuỗi có khả năng là giá trị datetime hay không.
    
    Args:
        series: Pandas Series cần kiểm tra.
        
    Returns:
        True nếu chuỗi có khả năng là datetime.
    """
    # Kiểm tra nếu đã là datetime
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    
    # Thử chuyển đổi
    if series.dtype == object:
        sample = series.dropna().head(100)
        if len(sample) == 0:
            return False
        
        # Kiểm tra các mẫu bằng regex phổ biến
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',                     # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',                     # DD/MM/YYYY hoặc MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',                     # DD-MM-YYYY hoặc MM-DD-YYYY
            r'\d{4}/\d{2}/\d{2}',                     # YYYY/MM/DD
            r'\d{2}/\d{2}/\d{2}',                     # DD/MM/YY hoặc MM/DD/YY
            r'\d{2}-\d{2}-\d{2}',                     # DD-MM-YY hoặc MM-DD-YY
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',   # YYYY-MM-DD HH:MM:SS
            r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',   # DD/MM/YYYY HH:MM:SS
        ]
        
        for pattern in date_patterns:
            matches = sample.astype(str).str.match(pattern)
            if matches.mean() > 0.7:  # Nếu hơn 70% mẫu phù hợp với mẫu ngày
                return True
        
        # Thử chuyển đổi sang datetime
        try:
            pd.to_datetime(sample, errors='raise')
            return True
        except (ValueError, TypeError):
            pass
    
    return False

def is_likely_numeric(series: pd.Series) -> bool:
    """
    Kiểm tra xem một chuỗi có khả năng là số hay không.
    
    Args:
        series: Pandas Series cần kiểm tra.
        
    Returns:
        True nếu chuỗi có khả năng là số.
    """
    # Nếu đã là kiểu số
    if pd.api.types.is_numeric_dtype(series):
        return True
    
    # Thử chuyển đổi
    if series.dtype == object:
        sample = series.dropna().head(100)
        if len(sample) == 0:
            return False
        
        # Thử phân tích
        try:
            # Loại bỏ dấu phẩy nghìn và các định dạng số
            cleaned = sample.astype(str).str.replace(',', '').str.replace(' ', '')
            pd.to_numeric(cleaned)
            return True
        except (ValueError, TypeError):
            pass
    
    return False

def time_execution(func):
    """
    Decorator để đo thời gian thực thi của hàm.
    
    Args:
        func: Hàm cần đo thời gian.
        
    Returns:
        Hàm wrapper.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        duration = end_time - start_time
        logger.info(f"Hàm {func.__name__} thực thi trong {duration:.2f} giây")
        
        return result
    
    return wrapper

def generate_csv_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Tạo bản tóm tắt thống kê cho DataFrame.
    
    Args:
        df: DataFrame cần tóm tắt.
        
    Returns:
        Dictionary chứa thông tin tóm tắt.
    """
    # Xử lý DataFrame rỗng
    if df.empty:
        return {
            'row_count': 0,
            'column_count': 0,
            'columns': {},
            'memory_usage': 0,
            'memory_usage_formatted': '0 B',
            'duplicated_rows': 0,
            'null_values_total': 0
        }
    
    summary = {
        'row_count': len(df),
        'column_count': len(df.columns),
        'columns': {},
        'memory_usage': df.memory_usage(deep=True).sum(),
        'memory_usage_formatted': format_file_size(df.memory_usage(deep=True).sum()),
        'duplicated_rows': df.duplicated().sum(),
        'null_values_total': df.isna().sum().sum()
    }
    
    # Thông tin cho từng cột
    for col in df.columns:
        col_summary = {
            'dtype': str(df[col].dtype),
            'null_count': df[col].isna().sum(),
            'null_percentage': round(df[col].isna().sum() / len(df) * 100, 2) if len(df) > 0 else 0,
            'unique_count': df[col].nunique()
        }
        
        # Thêm thống kê cho cột số
        if pd.api.types.is_numeric_dtype(df[col]):
            # Xử lý trường hợp tất cả các giá trị là NaN
            if df[col].isna().all():
                col_summary.update({
                    'min': None,
                    'max': None,
                    'mean': None,
                    'median': None,
                    'std': None
                })
            else:
                col_summary.update({
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean(),
                    'median': df[col].median(),
                    'std': df[col].std()
                })
        
        # Thêm thống kê cho cột chuỗi
        elif df[col].dtype == object:
            # Tính độ dài trung bình của chuỗi
            if df[col].isna().all():
                col_summary['avg_length'] = 0
            else:
                col_summary['avg_length'] = df[col].astype(str).str.len().mean()
            
            # Top 5 giá trị phổ biến nhất và tần suất
            if df[col].nunique() > 0:
                value_counts = df[col].value_counts().head(5).to_dict()
                col_summary['top_values'] = value_counts
            else:
                col_summary['top_values'] = {}
        
        # Thêm thông tin cột vào summary
        summary['columns'][col] = col_summary
    
    return summary

def detect_delimiter(file_path: str, num_lines: int = 5) -> str:
    """
    Phát hiện dấu phân cách trong tệp CSV.
    
    Args:
        file_path: Đường dẫn đến tệp CSV.
        num_lines: Số dòng để phân tích.
        
    Returns:
        Dấu phân cách phát hiện được, hoặc ',' nếu không xác định được.
    """
    try:
        # Phát hiện mã hóa
        encoding = detect_encoding(file_path)
        
        # Đọc các dòng đầu tiên
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            lines = [f.readline().strip() for _ in range(num_lines)]
            lines = [line for line in lines if line]  # Loại bỏ dòng trống
        
        if not lines:
            return ','
        
        # Các dấu phân cách phổ biến để kiểm tra
        delimiters = [',', ';', '\t', '|', ':']
        counts = {}
        
        for delimiter in delimiters:
            # Đếm số lần xuất hiện của mỗi dấu phân cách và tính số cột
            counts[delimiter] = []
            for line in lines:
                if delimiter in line:
                    counts[delimiter].append(line.count(delimiter) + 1)
            
            # Nếu không có dấu phân cách nào trong dòng, gán 0
            if not counts[delimiter]:
                counts[delimiter] = [0]
        
        # Nếu có sự nhất quán trong số cột và số cột > 1, có thể đó là dấu phân cách
        for delimiter, col_counts in counts.items():
            if len(set(col_counts)) == 1 and col_counts[0] > 1:
                logger.info(f"Phát hiện dấu phân cách: '{delimiter}'")
                return delimiter
        
        # Nếu không tìm thấy dấu phân cách nào, thử với python-csv-sniffer
        try:
            import csv
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                sample = f.read(4096)
                dialect = csv.Sniffer().sniff(sample)
                logger.info(f"Phát hiện dấu phân cách bằng csv.Sniffer: '{dialect.delimiter}'")
                return dialect.delimiter
        except Exception:
            logger.warning("Không thể sử dụng csv.Sniffer, sử dụng ',' làm mặc định")
            
        # Mặc định trả về dấu phẩy
        return ','
        
    except Exception as e:
        logger.error(f"Lỗi khi phát hiện dấu phân cách: {e}")
        return ','

# Thêm các hàm tiện ích mới
def get_file_extension(file_path: str) -> str:
    """
    Lấy phần mở rộng của tệp.
    
    Args:
        file_path: Đường dẫn đến tệp.
        
    Returns:
        Phần mở rộng của tệp (không bao gồm dấu chấm).
    """
    _, ext = os.path.splitext(file_path)
    return ext.lstrip('.').lower()

def create_directory_if_not_exists(directory_path: str) -> bool:
    """
    Tạo thư mục nếu chưa tồn tại.
    
    Args:
        directory_path: Đường dẫn đến thư mục.
        
    Returns:
        True nếu thư mục đã tồn tại hoặc được tạo thành công, False nếu có lỗi.
    """
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"Đã tạo thư mục: {directory_path}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi tạo thư mục {directory_path}: {e}")
        return False
