#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
data_loader.py
------------
Module nạp dữ liệu từ tệp CSV vào cơ sở dữ liệu SQL.
Hỗ trợ đọc dữ liệu theo từng lô (chunks) và xử lý lỗi.
"""

import os
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Union, Iterator, Callable
from sqlalchemy import text
from sqlalchemy.types import TypeEngine
from pandas.io.sql import SQLTable

from config import get_config
from db_connector import get_db_connector
from csv_analyzer import analyze_csv_file
from schema_generator import infer_schema_and_generate_sql
from error_handler import UploadStats, DeadLetterQueue, CSVUploadError, log_and_raise

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('data_loader')


class DataLoader:
    """Class nạp dữ liệu từ CSV vào cơ sở dữ liệu SQL."""
    
    def __init__(self, csv_file_path: str, table_name: Optional[str] = None):
        """
        Khởi tạo DataLoader.
        
        Args:
            csv_file_path: Đường dẫn đến tệp CSV cần nạp.
            table_name: Tên bảng đích trong cơ sở dữ liệu. Nếu None, sẽ lấy từ tên tệp.
        """
        self.csv_file_path = csv_file_path
        
        # Nếu table_name không được cung cấp, lấy từ tên tệp
        if table_name is None:
            base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
            # Chuẩn hóa tên bảng
            table_name = base_name.lower().replace(' ', '_').replace('-', '_')
            
        self.table_name = table_name
        
        # Cấu hình từ config
        self.config = get_config()
        self.csv_config = self.config.get_csv_config()
        self.table_config = self.config.get_table_config()
        
        # Kết nối cơ sở dữ liệu
        self.db_connector = get_db_connector()
        
        # Các thuộc tính sẽ được khởi tạo sau
        self.csv_analysis = None
        self.column_types = None
        self.create_table_sql = None
        self.upload_stats = UploadStats()
        self.dead_letter_queue = DeadLetterQueue()
        
        logger.info(f"Đã khởi tạo DataLoader cho tệp {csv_file_path} -> bảng {table_name}")
    
    def analyze_csv(self) -> Dict[str, Any]:
        """
        Phân tích tệp CSV để xác định cấu trúc.
        
        Returns:
            Kết quả phân tích từ csv_analyzer.
        """
        logger.info(f"Đang phân tích tệp CSV: {self.csv_file_path}")
        
        # Sử dụng sample_size từ cấu hình
        sample_size = self.csv_config.get('sample_size', 1000)
        
        # Thực hiện phân tích
        self.csv_analysis = analyze_csv_file(self.csv_file_path, sample_size)
        
        logger.info(f"Đã phân tích tệp CSV: {self.csv_analysis['num_columns']} cột, "
                  f"mã hóa {self.csv_analysis['encoding']}, "
                  f"phân cách '{self.csv_analysis['delimiter']}'")
                  
        return self.csv_analysis
    
    def infer_schema(self, primary_key: Optional[str] = None, 
                    add_indexes: Optional[List[str]] = None) -> Tuple[Dict[str, str], str]:
        """
        Suy luận lược đồ và tạo câu lệnh SQL từ phân tích CSV.
        
        Args:
            primary_key: Tên cột sử dụng làm khóa chính.
            add_indexes: Danh sách các cột cần tạo chỉ mục.
            
        Returns:
            Tuple (column_types, create_table_sql).
        """
        if not self.csv_analysis:
            self.analyze_csv()
            
        logger.info(f"Đang suy luận lược đồ cho bảng {self.table_name}")
        
        # Suy luận lược đồ từ dữ liệu mẫu
        df_sample = self.csv_analysis['sample_data']
        self.column_types, self.create_table_sql = infer_schema_and_generate_sql(
            df_sample, 
            self.table_name,
            primary_key=primary_key,
            add_indexes=add_indexes
        )
        
        logger.info(f"Đã suy luận lược đồ: {len(self.column_types)} cột")
        return self.column_types, self.create_table_sql
    
    def create_table(self, if_exists: Optional[str] = None) -> bool:
        """
        Tạo bảng trong cơ sở dữ liệu.
        
        Args:
            if_exists: Hành động khi bảng đã tồn tại ('fail', 'replace', 'append').
                      Nếu None, lấy từ cấu hình.
                      
        Returns:
            True nếu tạo bảng thành công, False nếu không.
        """
        if not self.create_table_sql:
            self.infer_schema()
            
        # Sử dụng if_exists từ tham số hoặc cấu hình
        if if_exists is None:
            if_exists = self.table_config.get('if_exists', 'fail')
            
        logger.info(f"Đang tạo bảng {self.table_name} với if_exists='{if_exists}'")
        
        # Tạo bảng
        success, message = self.db_connector.create_table(self.create_table_sql, if_exists)
        
        if success:
            logger.info(f"Đã tạo bảng thành công: {message}")
        else:
            logger.error(f"Lỗi khi tạo bảng: {message}")
            
        return success
    
    def _convert_pandas_to_sql_types(self) -> Dict[str, TypeEngine]:
        """
        Chuyển đổi kiểu dữ liệu từ định nghĩa SQL sang SQLAlchemy Types.
        
        Returns:
            Dict ánh xạ tên cột sang kiểu SQLAlchemy.
        """
        from sqlalchemy.types import (
            Integer, BigInteger, SmallInteger, Float,
            String, Text, Boolean, DateTime, Date, Time
        )
        
        # Ánh xạ từ kiểu SQL chuỗi sang đối tượng SQLAlchemy Type
        sql_to_alchemy_map = {
            'INT': Integer,
            'INTEGER': Integer,
            'BIGINT': BigInteger,
            'BIGINT UNSIGNED': BigInteger,
            'SMALLINT': SmallInteger,
            'TINYINT': SmallInteger,
            'FLOAT': Float,
            'DOUBLE': Float,
            'BOOLEAN': Boolean,
            'TINYINT(1)': Boolean,
            'DATETIME': DateTime,
            'DATE': Date,
            'TIME': Time,
            'TEXT': Text,
            'LONGTEXT': Text
        }
        
        # Đối tượng kiểu SQLAlchemy cho mỗi cột
        dtype_dict = {}
        
        for column, sql_type in self.column_types.items():
            # Xử lý VARCHAR với độ dài
            if sql_type.startswith('VARCHAR'):
                # Trích xuất độ dài từ VARCHAR(n)
                import re
                match = re.search(r'VARCHAR\((\d+)\)', sql_type)
                if match:
                    length = int(match.group(1))
                    dtype_dict[column] = String(length)
                else:
                    dtype_dict[column] = String(255)  # Mặc định
            else:
                # Sử dụng ánh xạ cho các kiểu khác
                alchemy_type = sql_to_alchemy_map.get(sql_type)
                if alchemy_type:
                    dtype_dict[column] = alchemy_type()
                else:
                    # Mặc định nếu không nhận diện được
                    logger.warning(f"Không nhận diện được kiểu SQL: {sql_type}, sử dụng Text")
                    dtype_dict[column] = Text()
        
        return dtype_dict
    
    def _execute_pandas_to_sql(self, df: pd.DataFrame, chunksize: int = None,
                            method: str = 'multi', if_exists: str = 'append') -> bool:
        """
        Thực thi to_sql với các tham số tối ưu.
        
        Args:
            df: DataFrame cần nạp vào SQL.
            chunksize: Kích thước chunk.
            method: Phương thức nạp ('multi' hoặc None).
            if_exists: Hành động khi bảng đã tồn tại ('fail', 'replace', 'append').
            
        Returns:
            True nếu thành công, False nếu có lỗi.
        """
        try:
            # Chuyển đổi kiểu dữ liệu SQL
            dtype = self._convert_pandas_to_sql_types()
            
            # Nạp dữ liệu
            df.to_sql(
                name=self.table_name,
                con=self.db_connector.get_engine(),
                if_exists=if_exists,
                index=False,
                dtype=dtype,
                chunksize=chunksize,
                method=method
            )
            
            # Ghi nhận số hàng thành công
            self.upload_stats.record_success(len(df))
            
            return True
            
        except Exception as e:
            # Ghi nhận lỗi
            self.upload_stats.record_failure(len(df), e)
            
            # Thêm các hàng vào dead letter queue
            self.dead_letter_queue.add_dataframe(
                self.csv_file_path, 
                df, 
                CSVUploadError(f"Lỗi khi nạp dữ liệu: {str(e)}", "bulk_insert_error")
            )
            
            logger.error(f"Lỗi khi nạp dữ liệu vào bảng {self.table_name}: {e}")
            return False
    
    def _create_insert_statement(self, columns: List[str]) -> str:
        """
        Tạo câu lệnh INSERT INTO ... VALUES cơ bản.
        
        Args:
            columns: Danh sách tên cột.
            
        Returns:
            Câu lệnh SQL INSERT.
        """
        columns_str = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join([f":{col}" for col in columns])
        
        return f"INSERT INTO `{self.table_name}` ({columns_str}) VALUES ({placeholders})"
    
    def _execute_manual_insert(self, df: pd.DataFrame) -> bool:
        """
        Thực hiện INSERT thủ công từng hàng với xử lý lỗi chi tiết.
        
        Args:
            df: DataFrame cần nạp vào SQL.
            
        Returns:
            True nếu ít nhất một hàng được chèn thành công, False nếu hoàn toàn thất bại.
        """
        columns = list(df.columns)
        insert_stmt = self._create_insert_statement(columns)
        success_count = 0
        
        # Thực hiện INSERT từng hàng
        with self.db_connector.get_connection() as conn:
            for idx, row in df.iterrows():
                try:
                    # Chuyển đổi row thành dict
                    row_dict = row.to_dict()
                    
                    # Xử lý các giá trị NaN, NaT
                    for key, value in row_dict.items():
                        if pd.isna(value):
                            row_dict[key] = None
                    
                    # Thực thi INSERT
                    conn.execute(text(insert_stmt), row_dict)
                    success_count += 1
                    self.upload_stats.record_success(1)
                    
                except Exception as e:
                    # Ghi nhận lỗi
                    self.upload_stats.record_failure(1, e)
                    
                    # Thêm hàng vào dead letter queue
                    error = CSVUploadError(
                        f"Lỗi khi nạp hàng {idx}: {str(e)}", 
                        "row_insert_error",
                        {"row_index": idx}
                    )
                    self.dead_letter_queue.add_row(
                        self.csv_file_path,
                        row_dict,
                        error,
                        columns
                    )
                    
                    logger.error(f"Lỗi khi nạp hàng {idx}: {e}")
            
            # Commit sau khi hoàn thành tất cả
            if success_count > 0:
                conn.commit()
                
        return success_count > 0
                
    def load_data(self, chunk_method: str = 'auto', manual_insert: bool = False,
                 chunksize: Optional[int] = None, if_exists: Optional[str] = None) -> bool:
        """
        Nạp dữ liệu từ tệp CSV vào bảng SQL.
        
        Args:
            chunk_method: Phương thức xử lý chunk ('auto', 'pandas', 'manual').
            manual_insert: True để sử dụng INSERT thủ công thay vì to_sql.
            chunksize: Kích thước chunk. Nếu None, lấy từ cấu hình.
            if_exists: Hành động khi bảng đã tồn tại. Nếu None, lấy từ cấu hình.
            
        Returns:
            True nếu thành công, False nếu có lỗi.
        """
        if not self.csv_analysis:
            self.analyze_csv()
            
        if not self.column_types:
            self.infer_schema()
        
        # Lấy các tham số từ cấu hình nếu không được chỉ định
        if chunksize is None:
            chunksize = self.csv_config.get('chunk_size', 10000)
            
        if if_exists is None:
            if_exists = self.table_config.get('if_exists', 'fail')
        
        # Tạo bảng nếu cần
        table_created = self.create_table(if_exists)
        if not table_created:
            return False
            
        # Chuẩn bị tham số đọc CSV
        csv_params = {
            'filepath_or_buffer': self.csv_file_path,
            'encoding': self.csv_analysis['encoding'],
            'delimiter': self.csv_analysis['delimiter'],
            'header': 0 if self.csv_analysis['has_header'] else None,
            'names': self.csv_analysis['column_names'] if not self.csv_analysis['has_header'] else None,
            'low_memory': False,
            'on_bad_lines': 'warn'  # Cảnh báo và bỏ qua các dòng lỗi
        }
        
        logger.info(f"Bắt đầu nạp dữ liệu từ {self.csv_file_path} vào bảng {self.table_name}")
        
        try:
            # Phương thức nạp dữ liệu
            if chunk_method == 'auto':
                # Tự động chọn phương thức dựa trên kích thước tệp
                file_size = os.path.getsize(self.csv_file_path)
                chunk_method = 'pandas' if file_size < 100 * 1024 * 1024 else 'manual'
            
            # Nạp toàn bộ tệp vào một DataFrame
            if chunk_method == 'pandas':
                logger.info(f"Đọc toàn bộ tệp CSV vào DataFrame")
                df = pd.read_csv(**csv_params)
                
                if manual_insert:
                    return self._execute_manual_insert(df)
                else:
                    return self._execute_pandas_to_sql(df, chunksize, 'multi', 'append')
            
            # Nạp tệp theo từng lô (chunk)
            elif chunk_method == 'manual':
                logger.info(f"Đọc tệp CSV theo từng lô (chunk_size={chunksize})")
                
                # Thêm tham số chunksize
                csv_params['chunksize'] = chunksize
                
                # Đọc tệp CSV theo từng chunk
                chunks = pd.read_csv(**csv_params)
                success = False
                
                for i, chunk_df in enumerate(chunks):
                    chunk_success = False
                    chunk_size = len(chunk_df)
                    
                    logger.info(f"Đang xử lý chunk {i+1} ({chunk_size} hàng)")
                    
                    if manual_insert:
                        chunk_success = self._execute_manual_insert(chunk_df)
                    else:
                        chunk_success = self._execute_pandas_to_sql(chunk_df, None, 'multi', 'append')
                    
                    # Nếu ít nhất một chunk thành công, đánh dấu thành công
                    if chunk_success:
                        success = True
                
                return success
                
            else:
                raise ValueError(f"Phương thức chunk không hợp lệ: {chunk_method}")
                
        except Exception as e:
            logger.error(f"Lỗi khi nạp dữ liệu: {e}")
            self.upload_stats.record_failure(0, e)  # Số hàng lỗi không xác định
            return False
        finally:
            # Hoàn thành thống kê
            self.upload_stats.complete()
    
    def validate_data(self) -> Dict[str, Any]:
        """
        Thực hiện kiểm tra xác thực cơ bản trên dữ liệu đã nạp.
        
        Returns:
            Dictionary chứa kết quả xác thực.
        """
        logger.info(f"Đang xác thực dữ liệu đã nạp vào bảng {self.table_name}")
        
        validation_results = {}
        
        try:
            with self.db_connector.get_connection() as conn:
                # Đếm số hàng
                row_count_result = conn.execute(text(f"SELECT COUNT(*) FROM `{self.table_name}`"))
                row_count = row_count_result.scalar()
                validation_results['table_row_count'] = row_count
                
                # Lấy mẫu dữ liệu
                sample_query = f"SELECT * FROM `{self.table_name}` LIMIT 5"
                sample_result = conn.execute(text(sample_query))
                sample_data = [dict(row) for row in sample_result]
                validation_results['sample_data'] = sample_data
                
                # Kiểm tra NULL cho từng cột
                null_counts = {}
                for column in self.column_types.keys():
                    null_query = f"SELECT COUNT(*) FROM `{self.table_name}` WHERE `{column}` IS NULL"
                    null_result = conn.execute(text(null_query))
                    null_count = null_result.scalar()
                    null_counts[column] = null_count
                validation_results['null_counts'] = null_counts
            
            # So sánh với thống kê nạp
            validation_results['upload_stats'] = self.upload_stats.get_summary()
            validation_results['validation_status'] = 'Success'
            
            logger.info(f"Xác thực dữ liệu thành công: {row_count} hàng trong bảng")
            
        except Exception as e:
            logger.error(f"Lỗi khi xác thực dữ liệu: {e}")
            validation_results['validation_status'] = 'Failed'
            validation_results['validation_error'] = str(e)
        
        return validation_results
    
    def get_upload_stats(self) -> Dict[str, Any]:
        """
        Lấy thống kê về quá trình nạp dữ liệu.
        
        Returns:
            Dictionary chứa thông tin thống kê.
        """
        return self.upload_stats.get_summary()
    
    def print_upload_stats(self) -> None:
        """In thống kê nạp dữ liệu ra console."""
        self.upload_stats.print_summary()
    
    def get_error_stats(self) -> Dict[str, int]:
        """
        Lấy thống kê về các lỗi đã xảy ra.
        
        Returns:
            Dictionary ánh xạ từ loại lỗi đến số lượng.
        """
        return self.dead_letter_queue.get_error_stats()


def load_csv_to_sql(csv_file_path: str, table_name: Optional[str] = None,
                  chunk_method: str = 'auto', manual_insert: bool = False,
                  chunksize: Optional[int] = None, if_exists: Optional[str] = None,
                  primary_key: Optional[str] = None, add_indexes: Optional[List[str]] = None,
                  validate: bool = True) -> Dict[str, Any]:
    """
    Hàm tiện ích để nạp tệp CSV vào bảng SQL.
    
    Args:
        csv_file_path: Đường dẫn đến tệp CSV.
        table_name: Tên bảng đích. Nếu None, sẽ lấy từ tên tệp.
        chunk_method: Phương thức xử lý chunk ('auto', 'pandas', 'manual').
        manual_insert: True để sử dụng INSERT thủ công thay vì to_sql.
        chunksize: Kích thước chunk. Nếu None, lấy từ cấu hình.
        if_exists: Hành động khi bảng đã tồn tại. Nếu None, lấy từ cấu hình.
        primary_key: Tên cột sử dụng làm khóa chính.
        add_indexes: Danh sách các cột cần tạo chỉ mục.
        validate: True để thực hiện xác thực sau khi nạp.
        
    Returns:
        Dictionary chứa kết quả nạp và xác thực.
    """
    # Khởi tạo DataLoader
    loader = DataLoader(csv_file_path, table_name)
    
    # Phân tích tệp CSV
    loader.analyze_csv()
    
    # Suy luận lược đồ
    loader.infer_schema(primary_key, add_indexes)
    
    # Nạp dữ liệu
    success = loader.load_data(chunk_method, manual_insert, chunksize, if_exists)
    
    result = {
        'csv_file': csv_file_path,
        'table_name': loader.table_name,
        'status': 'Success' if success else 'Failed',
        'load_stats': loader.get_upload_stats(),
        'error_stats': loader.get_error_stats()
    }
    
    # Xác thực nếu yêu cầu và nạp thành công
    if validate and success:
        result['validation'] = loader.validate_data()
    
    # In thống kê
    loader.print_upload_stats()
    
    return result


if __name__ == "__main__":
    # Kiểm thử script
    import sys
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        test_table = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        test_file = "data/example.csv"
        test_table = "example_table"
        
    try:
        result = load_csv_to_sql(
            test_file, 
            test_table,
            if_exists='replace',
            add_indexes=["id", "name"] if test_table == "example_table" else None
        )
        
        if result['status'] == 'Success':
            print("\nNạp dữ liệu thành công!")
            
            if 'validation' in result:
                print("\nKết quả xác thực:")
                print(f"- Số hàng trong bảng: {result['validation']['table_row_count']}")
                print(f"- Trạng thái xác thực: {result['validation']['validation_status']}")
                
                print("\nMẫu dữ liệu (5 hàng đầu):")
                for row in result['validation']['sample_data']:
                    print(row)
        else:
            print("\nNạp dữ liệu thất bại!")
            print(f"Thống kê lỗi: {result['error_stats']}")
            
    except Exception as e:
        print(f"Lỗi: {e}")
