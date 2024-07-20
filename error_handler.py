#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
error_handler.py
--------------
Module xử lý lỗi và ghi log cho quá trình nạp dữ liệu CSV vào SQL.
Cung cấp các lớp và hàm để ghi log, theo dõi lỗi và cơ chế hàng đợi thư chết.
"""

import os
import csv
import logging
import traceback
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('error_handler')


class CSVUploadError(Exception):
    """Lớp ngoại lệ tùy chỉnh cho lỗi khi nạp CSV."""
    
    def __init__(self, message: str, error_type: str = "general", details: Optional[Dict[str, Any]] = None):
        """
        Khởi tạo CSVUploadError.
        
        Args:
            message: Thông báo lỗi chính.
            error_type: Loại lỗi (ví dụ: "connection", "datatype", "validation").
            details: Thông tin chi tiết bổ sung về lỗi.
        """
        self.message = message
        self.error_type = error_type
        self.details = details or {}
        self.timestamp = datetime.now()
        
        super().__init__(self.message)
    
    def __str__(self) -> str:
        """Biểu diễn dạng chuỗi của ngoại lệ."""
        if self.details:
            return f"{self.error_type} - {self.message} - Details: {self.details}"
        return f"{self.error_type} - {self.message}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Chuyển đổi ngoại lệ thành dictionary cho việc ghi log hoặc serialization."""
        return {
            "error_type": self.error_type,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details
        }


class UploadStats:
    """Lớp theo dõi thống kê về quá trình nạp dữ liệu."""
    
    def __init__(self):
        """Khởi tạo đối tượng thống kê nạp."""
        self.start_time = datetime.now()
        self.end_time = None
        self.total_rows = 0
        self.successful_rows = 0
        self.failed_rows = 0
        self.errors = []
        
    def record_success(self, num_rows: int = 1) -> None:
        """
        Ghi nhận hàng nạp thành công.
        
        Args:
            num_rows: Số hàng nạp thành công.
        """
        self.successful_rows += num_rows
        self.total_rows += num_rows
        
    def record_failure(self, num_rows: int = 1, error: Optional[Exception] = None) -> None:
        """
        Ghi nhận hàng nạp thất bại.
        
        Args:
            num_rows: Số hàng nạp thất bại.
            error: Ngoại lệ gây ra lỗi (nếu có).
        """
        self.failed_rows += num_rows
        self.total_rows += num_rows
        
        if error:
            if isinstance(error, CSVUploadError):
                self.errors.append(error.to_dict())
            else:
                self.errors.append({
                    "error_type": error.__class__.__name__,
                    "message": str(error),
                    "timestamp": datetime.now().isoformat(),
                    "details": {"traceback": traceback.format_exc()}
                })
    
    def complete(self) -> None:
        """Đánh dấu quá trình nạp là hoàn tất."""
        self.end_time = datetime.now()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Lấy tóm tắt thống kê.
        
        Returns:
            Dictionary chứa thông tin thống kê.
        """
        if not self.end_time:
            self.complete()
            
        duration = (self.end_time - self.start_time).total_seconds()
        success_rate = (self.successful_rows / self.total_rows * 100) if self.total_rows > 0 else 0
        
        return {
            "total_rows": self.total_rows,
            "successful_rows": self.successful_rows,
            "failed_rows": self.failed_rows,
            "success_rate": f"{success_rate:.2f}%",
            "duration_seconds": duration,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "error_count": len(self.errors),
            "status": "Completed" if self.failed_rows == 0 else "Completed with errors"
        }
    
    def print_summary(self) -> None:
        """In tóm tắt thống kê ra console."""
        summary = self.get_summary()
        
        print("\n" + "="*50)
        print(f"KẾT QUẢ NẠP DỮ LIỆU:")
        print("="*50)
        print(f"Tổng số hàng: {summary['total_rows']}")
        print(f"Hàng thành công: {summary['successful_rows']}")
        print(f"Hàng lỗi: {summary['failed_rows']}")
        print(f"Tỷ lệ thành công: {summary['success_rate']}")
        print(f"Thời gian thực thi: {summary['duration_seconds']:.2f} giây")
        print(f"Trạng thái: {summary['status']}")
        
        if summary['error_count'] > 0:
            print(f"\nTổng số lỗi: {summary['error_count']}")
            print("Xem chi tiết trong tệp log hoặc dead letter queue")
        
        print("="*50)


class DeadLetterQueue:
    """
    Lớp quản lý hàng đợi thư chết - lưu các hàng gặp lỗi khi nạp.
    Cho phép lưu các hàng lỗi ra tệp riêng để xem xét sau.
    """
    
    def __init__(self, error_dir: str = "errors"):
        """
        Khởi tạo DeadLetterQueue.
        
        Args:
            error_dir: Thư mục lưu trữ các tệp lỗi.
        """
        self.error_dir = error_dir
        
        # Tạo thư mục nếu chưa tồn tại
        if not os.path.exists(error_dir):
            os.makedirs(error_dir)
            
        self.current_file = None
        self.writer = None
        self.error_counts = {}
    
    def _init_error_file(self, csv_filename: str, column_names: List[str]) -> None:
        """
        Khởi tạo tệp lỗi mới.
        
        Args:
            csv_filename: Tên tệp CSV gốc.
            column_names: Danh sách tên cột.
        """
        # Tạo tên tệp lỗi dựa trên tên tệp gốc và timestamp
        base_name = os.path.splitext(os.path.basename(csv_filename))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_filename = f"{base_name}_errors_{timestamp}.csv"
        
        # Đường dẫn đầy đủ đến tệp lỗi
        self.current_file = os.path.join(self.error_dir, error_filename)
        
        # Thêm cột bổ sung để ghi thông tin lỗi
        extended_columns = column_names + ["error_type", "error_message", "error_timestamp"]
        
        # Tạo tệp lỗi và writer CSV
        with open(self.current_file, 'w', newline='', encoding='utf-8') as f:
            self.writer = csv.DictWriter(f, fieldnames=extended_columns)
            self.writer.writeheader()
            
        logger.info(f"Đã khởi tạo tệp hàng đợi thư chết: {self.current_file}")
    
    def add_row(self, csv_filename: str, row_data: Dict[str, Any], error: Exception, 
                column_names: Optional[List[str]] = None) -> None:
        """
        Thêm một hàng vào hàng đợi thư chết.
        
        Args:
            csv_filename: Tên tệp CSV gốc.
            row_data: Dữ liệu hàng gặp lỗi.
            error: Ngoại lệ gây ra lỗi.
            column_names: Danh sách tên cột (có thể tự động lấy từ row_data).
        """
        # Nếu column_names không được cung cấp, sử dụng keys từ row_data
        if column_names is None:
            column_names = list(row_data.keys())
        
        # Khởi tạo tệp lỗi nếu cần
        if self.current_file is None:
            self._init_error_file(csv_filename, column_names)
        
        # Chuẩn bị hàng để ghi vào tệp lỗi
        error_row = row_data.copy()
        
        # Thêm thông tin lỗi
        if isinstance(error, CSVUploadError):
            error_row["error_type"] = error.error_type
            error_row["error_message"] = error.message
        else:
            error_row["error_type"] = error.__class__.__name__
            error_row["error_message"] = str(error)
            
        error_row["error_timestamp"] = datetime.now().isoformat()
        
        # Ghi hàng vào tệp lỗi
        with open(self.current_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(error_row.keys()))
            writer.writerow(error_row)
        
        # Cập nhật số lượng lỗi
        error_type = error_row["error_type"]
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        
        logger.debug(f"Đã thêm hàng lỗi vào hàng đợi thư chết: {error_type} - {error_row['error_message']}")
    
    def add_dataframe(self, csv_filename: str, df: pd.DataFrame, 
                      error: Union[Exception, List[Exception]], 
                      column_names: Optional[List[str]] = None) -> None:
        """
        Thêm nhiều hàng (DataFrame) vào hàng đợi thư chết.
        
        Args:
            csv_filename: Tên tệp CSV gốc.
            df: DataFrame chứa các hàng lỗi.
            error: Ngoại lệ gây ra lỗi hoặc danh sách ngoại lệ.
            column_names: Danh sách tên cột (mặc định lấy từ df.columns).
        """
        if column_names is None:
            column_names = list(df.columns)
            
        # Khởi tạo tệp lỗi
        if self.current_file is None:
            self._init_error_file(csv_filename, column_names)
        
        # Xử lý trường hợp nhiều lỗi
        if isinstance(error, list) and len(error) == len(df):
            # Mỗi hàng có lỗi riêng
            for i, row in df.iterrows():
                row_dict = row.to_dict()
                row_error = error[i] if i < len(error) else error[-1]
                self.add_row(csv_filename, row_dict, row_error, column_names)
        else:
            # Cùng một lỗi cho tất cả các hàng
            single_error = error[0] if isinstance(error, list) else error
            for _, row in df.iterrows():
                self.add_row(csv_filename, row.to_dict(), single_error, column_names)
    
    def get_error_stats(self) -> Dict[str, int]:
        """
        Lấy thống kê về các loại lỗi.
        
        Returns:
            Dictionary ánh xạ từ loại lỗi đến số lượng.
        """
        return self.error_counts.copy()


def setup_logger(log_file: str = None, log_level: int = logging.INFO) -> logging.Logger:
    """
    Thiết lập logger với cấu hình tùy chỉnh.
    
    Args:
        log_file: Đường dẫn đến tệp log (tùy chọn).
        log_level: Mức độ log (mặc định là INFO).
        
    Returns:
        Đối tượng logger đã cấu hình.
    """
    logger = logging.getLogger('csv_upload')
    logger.setLevel(log_level)
    
    # Định dạng log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler tệp nếu được chỉ định
    if log_file:
        # Tạo thư mục chứa tệp log nếu cần
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Thêm file handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def log_and_raise(message: str, error_type: str = "general", 
                  exception_class: type = CSVUploadError, 
                  details: Optional[Dict[str, Any]] = None, 
                  logger: Optional[logging.Logger] = None) -> None:
    """
    Ghi log lỗi và ném ngoại lệ.
    
    Args:
        message: Thông báo lỗi.
        error_type: Loại lỗi.
        exception_class: Lớp ngoại lệ cần ném (mặc định là CSVUploadError).
        details: Thông tin chi tiết bổ sung.
        logger: Logger để ghi log (nếu None, sử dụng logger mặc định).
    
    Raises:
        Exception: Ngoại lệ được chỉ định với thông điệp và chi tiết.
    """
    if logger is None:
        logger = logging.getLogger('error_handler')
        
    # Ghi log lỗi
    log_message = f"{error_type} - {message}"
    if details:
        log_message += f" - Details: {details}"
    
    logger.error(log_message)
    
    # Ném ngoại lệ
    if exception_class == CSVUploadError:
        raise CSVUploadError(message, error_type, details)
    else:
        raise exception_class(message)


if __name__ == "__main__":
    # Kiểm thử module
    try:
        # Thiết lập logger test
        test_logger = setup_logger("errors/test_error.log")
        test_logger.info("Bắt đầu kiểm thử error_handler.py")
        
        # Kiểm thử UploadStats
        stats = UploadStats()
        stats.record_success(10)
        stats.record_failure(2, CSVUploadError("Lỗi kiểm thử", "test_error", {"test": True}))
        stats.complete()
        stats.print_summary()
        
        # Kiểm thử DeadLetterQueue
        dlq = DeadLetterQueue()
        test_row = {"id": 1, "name": "Test", "value": 100}
        test_error = CSVUploadError("Giá trị không hợp lệ", "validation", {"column": "value", "expected": "string"})
        dlq.add_row("test.csv", test_row, test_error)
        
        print("\nThống kê lỗi từ Dead Letter Queue:")
        print(dlq.get_error_stats())
        
        # Kiểm thử log_and_raise
        try:
            log_and_raise(
                "Lỗi kết nối cơ sở dữ liệu", 
                "connection", 
                details={"host": "localhost", "db": "test_db"},
                logger=test_logger
            )
        except CSVUploadError as e:
            print(f"\nĐã bắt ngoại lệ: {e}")
            print(f"Chi tiết: {e.to_dict()}")
            
    except Exception as e:
        print(f"Lỗi trong quá trình kiểm thử: {e}")
