#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
db_connector.py
--------------
Module quản lý kết nối cơ sở dữ liệu.
Thiết lập kết nối an toàn và có thể tái sử dụng đến cơ sở dữ liệu SQL.
"""

import logging
from typing import Dict, Any, Optional, Tuple, List, Union
from sqlalchemy import create_engine, text, Engine, Connection, inspect
from urllib.parse import quote_plus
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from config import get_config

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('db_connector')


class DatabaseConnector:
    """Lớp quản lý kết nối và tương tác với cơ sở dữ liệu."""
    
    def __init__(self):
        """Khởi tạo kết nối cơ sở dữ liệu."""
        self.config = get_config()
        self.engine = None
        self.connection = None
        self.inspector = None
        
    def connect(self) -> bool:
        """
        Khởi tạo kết nối tới cơ sở dữ liệu.
        
        Returns:
            bool: True nếu kết nối thành công, False nếu thất bại.
        """
        try:
            # Lấy cấu hình và thiết lập kết nối
            conn_str = self.config.get_connection_string()
            options = self.config.get_sqlalchemy_options()
            
            # Kiểm tra và tạo database nếu cần
            if self.config.should_auto_create_db():
                self._ensure_database_exists()
            
            # Tạo engine kết nối tới database
            self.engine = sa.create_engine(conn_str, **options)
            self.connection = self.engine.connect()
            self.inspector = inspect(self.engine)
            
            logger.info(f"Đã kết nối thành công đến cơ sở dữ liệu: {self.config.get_db_name()}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Lỗi kết nối cơ sở dữ liệu: {str(e)}")
            return False
    
    def _ensure_database_exists(self) -> None:
        """
        Tạo cơ sở dữ liệu nếu chưa tồn tại.
        """
        db_name = self.config.get_db_name()
        
        try:
            # Kết nối không có tên database
            conn_str = self.config.get_connection_string(include_db=False)
            options = self.config.get_sqlalchemy_options()
            root_engine = sa.create_engine(conn_str, **options)
            
            # Kiểm tra database có tồn tại không
            with root_engine.connect() as conn:
                result = conn.execute(text(f"SHOW DATABASES LIKE '{db_name}'"))
                if not result.fetchone():
                    logger.info(f"Database '{db_name}' không tồn tại. Đang tạo mới...")
                    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
                    logger.info(f"Đã tạo database '{db_name}' thành công.")
                else:
                    logger.info(f"Database '{db_name}' đã tồn tại.")
                
                # Đảm bảo đóng kết nối đến root
                conn.close()
            
            root_engine.dispose()
            
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi tạo database: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Đóng kết nối database."""
        if self.connection:
            self.connection.close()
            self.connection = None
            
        if self.engine:
            self.engine.dispose()
            self.engine = None
            
        logger.info("Đã đóng kết nối cơ sở dữ liệu")
    
    def is_connected(self) -> bool:
        """
        Kiểm tra kết nối còn hoạt động không.
        
        Returns:
            bool: True nếu kết nối còn hoạt động, False nếu đã đóng.
        """
        if not self.connection:
            return False
            
        try:
            # Thực thi truy vấn đơn giản để kiểm tra
            self.connection.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Thực thi truy vấn SQL và trả về kết quả.
        
        Args:
            query: Câu truy vấn SQL.
            params: Các tham số cho truy vấn (optional).
            
        Returns:
            List[Dict[str, Any]]: Danh sách kết quả dạng dict.
            
        Raises:
            SQLAlchemyError: Nếu có lỗi khi thực thi truy vấn.
        """
        if not self.is_connected():
            self.connect()
            
        try:
            result = self.connection.execute(text(query), params or {})
            return [dict(row) for row in result]
        except SQLAlchemyError as e:
            logger.error(f"Lỗi thực thi truy vấn: {str(e)}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Kiểm tra bảng có tồn tại trong cơ sở dữ liệu.
        
        Args:
            table_name: Tên bảng cần kiểm tra.
            
        Returns:
            bool: True nếu bảng tồn tại, False nếu không.
        """
        if not self.is_connected():
            self.connect()
            
        return self.inspector.has_table(table_name)
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Lấy thông tin các cột của bảng.
        
        Args:
            table_name: Tên bảng.
            
        Returns:
            List[Dict[str, Any]]: Danh sách thông tin cột.
        """
        if not self.is_connected():
            self.connect()
            
        if not self.table_exists(table_name):
            logger.warning(f"Bảng '{table_name}' không tồn tại")
            return []
            
        return self.inspector.get_columns(table_name)
    
    def create_table_from_df(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        if_exists: str = 'fail',
        index: bool = False,
        dtype: Optional[Dict[str, Any]] = None,
        schema: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Tạo bảng mới từ DataFrame.
        
        Args:
            df: DataFrame chứa dữ liệu.
            table_name: Tên bảng cần tạo.
            if_exists: Hành động khi bảng đã tồn tại ('fail', 'replace', 'append').
            index: Có sử dụng index của DataFrame làm cột không.
            dtype: Kiểu dữ liệu cho các cột.
            schema: Schema cho việc tạo bảng (thay cho việc tự động suy luận).
            
        Returns:
            bool: True nếu tạo thành công, False nếu thất bại.
        """
        if not self.is_connected():
            self.connect()
            
        if_exists = if_exists or self.config.get('table', 'if_exists', 'fail')
            
        try:
            # Thêm cột ID nếu được cấu hình
            add_id = self.config.get('table', 'add_id_column', True)
            id_name = self.config.get('table', 'id_column_name', 'id')
            
            if add_id and id_name not in df.columns:
                df.insert(0, id_name, range(1, len(df) + 1))
            
            # Tạo bảng từ DataFrame
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=index,
                dtype=dtype,
                schema=schema
            )
            
            logger.info(f"Đã tạo/cập nhật bảng '{table_name}' thành công")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi tạo bảng '{table_name}': {str(e)}")
            return False
    
    def load_csv_to_table(
        self, 
        csv_file: str, 
        table_name: str, 
        if_exists: str = 'fail',
        chunk_size: Optional[int] = None,
        **pd_kwargs
    ) -> Tuple[bool, int]:
        """
        Tải dữ liệu từ file CSV vào bảng.
        
        Args:
            csv_file: Đường dẫn đến file CSV.
            table_name: Tên bảng đích.
            if_exists: Hành động khi bảng đã tồn tại ('fail', 'replace', 'append').
            chunk_size: Số dòng mỗi lần đọc.
            **pd_kwargs: Các tham số bổ sung cho pandas.read_csv.
            
        Returns:
            Tuple[bool, int]: (Thành công/Thất bại, Số dòng đã tải).
        """
        if not os.path.exists(csv_file):
            logger.error(f"File CSV không tồn tại: {csv_file}")
            return False, 0
            
        if not self.is_connected():
            self.connect()
        
        # Lấy cấu hình mặc định
        if_exists = if_exists or self.config.get('table', 'if_exists', 'fail')
        chunk_size = chunk_size or self.config.get('csv', 'chunk_size', 5000)
        
        # Kết hợp tùy chọn đọc CSV từ cấu hình
        read_csv_options = self.config.get_pandas_read_csv_options()
        read_csv_options.update(pd_kwargs)
        
        # Đảm bảo chunksize được thiết lập
        read_csv_options['chunksize'] = chunk_size
        
        total_rows = 0
        
        try:
            # Đọc file CSV theo từng chunk
            for i, chunk in enumerate(pd.read_csv(csv_file, **read_csv_options)):
                # Thiết lập if_exists: 'replace' cho chunk đầu tiên, 'append' cho các chunk sau
                current_if_exists = if_exists if i == 0 else 'append'
                
                # Thêm cột ID nếu được cấu hình và nếu đang ở chế độ replace hoặc chunk đầu tiên
                add_id = self.config.get('table', 'add_id_column', True)
                id_name = self.config.get('table', 'id_column_name', 'id')
                
                if add_id and id_name not in chunk.columns and (current_if_exists == 'replace' or i == 0):
                    # Tính toán giá trị ID bắt đầu
                    start_id = total_rows + 1
                    chunk.insert(0, id_name, range(start_id, start_id + len(chunk)))
                
                # Tải chunk vào cơ sở dữ liệu
                success = self.create_table_from_df(
                    chunk, 
                    table_name, 
                    if_exists=current_if_exists,
                    index=False
                )
                
                if not success:
                    logger.error(f"Lỗi khi tải chunk thứ {i+1} vào bảng '{table_name}'")
                    return False, total_rows
                
                rows_in_chunk = len(chunk)
                total_rows += rows_in_chunk
                logger.info(f"Đã tải chunk {i+1} ({rows_in_chunk} dòng) vào bảng '{table_name}'")
            
            logger.info(f"Hoàn thành tải dữ liệu: {total_rows} dòng vào bảng '{table_name}'")
            return True, total_rows
            
        except Exception as e:
            logger.error(f"Lỗi khi tải CSV '{csv_file}' vào bảng '{table_name}': {str(e)}")
            return False, total_rows
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Lấy số dòng trong bảng.
        
        Args:
            table_name: Tên bảng.
            
        Returns:
            int: Số dòng trong bảng.
        """
        if not self.is_connected():
            self.connect()
            
        if not self.table_exists(table_name):
            logger.warning(f"Bảng '{table_name}' không tồn tại")
            return 0
            
        try:
            result = self.execute_query(f"SELECT COUNT(*) as count FROM `{table_name}`")
            return result[0]['count']
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi đếm số dòng trong bảng '{table_name}': {str(e)}")
            return -1
    
    def get_database_size(self) -> Dict[str, Any]:
        """
        Lấy kích thước của cơ sở dữ liệu.
        
        Returns:
            Dict[str, Any]: Thông tin kích thước database.
        """
        if not self.is_connected():
            self.connect()
            
        db_name = self.config.get_db_name()
        
        try:
            query = """
            SELECT 
                table_schema as 'database',
                ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as 'size_mb'
            FROM 
                information_schema.tables
            WHERE 
                table_schema = :db_name
            GROUP BY 
                table_schema
            """
            result = self.execute_query(query, {'db_name': db_name})
            
            if result:
                return result[0]
            else:
                return {'database': db_name, 'size_mb': 0.0}
                
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi lấy kích thước database: {str(e)}")
            return {'database': db_name, 'size_mb': -1}
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Lấy thông tin chi tiết về bảng.
        
        Args:
            table_name: Tên bảng.
            
        Returns:
            Dict[str, Any]: Thông tin về bảng.
        """
        if not self.is_connected():
            self.connect()
            
        if not self.table_exists(table_name):
            logger.warning(f"Bảng '{table_name}' không tồn tại")
            return {}
            
        try:
            # Lấy thông tin kích thước bảng
            query = """
            SELECT 
                table_name,
                table_rows,
                ROUND(data_length / 1024 / 1024, 2) as data_size_mb,
                ROUND(index_length / 1024 / 1024, 2) as index_size_mb,
                ROUND((data_length + index_length) / 1024 / 1024, 2) as total_size_mb,
                create_time,
                update_time
            FROM 
                information_schema.tables
            WHERE 
                table_schema = :db_name AND table_name = :table_name
            """
            db_name = self.config.get_db_name()
            result = self.execute_query(query, {'db_name': db_name, 'table_name': table_name})
            
            if not result:
                logger.warning(f"Không tìm thấy thông tin cho bảng '{table_name}'")
                return {}
                
            table_info = result[0]
            
            # Lấy thông tin cột
            columns = self.get_table_columns(table_name)
            table_info['columns'] = columns
            table_info['column_count'] = len(columns)
            
            return table_info
            
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi lấy thông tin bảng '{table_name}': {str(e)}")
            return {}
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Xóa toàn bộ dữ liệu trong bảng.
        
        Args:
            table_name: Tên bảng cần xóa dữ liệu.
            
        Returns:
            bool: True nếu thành công, False nếu thất bại.
        """
        if not self.is_connected():
            self.connect()
            
        if not self.table_exists(table_name):
            logger.warning(f"Bảng '{table_name}' không tồn tại")
            return False
            
        try:
            self.execute_query(f"TRUNCATE TABLE `{table_name}`")
            logger.info(f"Đã xóa toàn bộ dữ liệu trong bảng '{table_name}'")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi xóa dữ liệu bảng '{table_name}': {str(e)}")
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """
        Xóa bảng khỏi cơ sở dữ liệu.
        
        Args:
            table_name: Tên bảng cần xóa.
            
        Returns:
            bool: True nếu thành công, False nếu thất bại.
        """
        if not self.is_connected():
            self.connect()
            
        if not self.table_exists(table_name):
            logger.warning(f"Bảng '{table_name}' không tồn tại")
            return True
            
        try:
            self.execute_query(f"DROP TABLE IF EXISTS `{table_name}`")
            logger.info(f"Đã xóa bảng '{table_name}'")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi xóa bảng '{table_name}': {str(e)}")
            return False
    
    def get_all_tables(self) -> List[str]:
        """
        Lấy danh sách tất cả các bảng trong database.
        
        Returns:
            List[str]: Danh sách tên bảng.
        """
        if not self.is_connected():
            self.connect()
            
        return self.inspector.get_table_names()

# Khởi tạo đối tượng DatabaseConnector để sử dụng trong ứng dụng
db_connector = DatabaseConnector()
