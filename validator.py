#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
validator.py
----------
Module xác thực dữ liệu đã nạp vào cơ sở dữ liệu.
Thực hiện các kiểm tra để đảm bảo dữ liệu đã được nạp chính xác.
"""

import os
import logging
import pandas as pd
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from sqlalchemy import text, inspect
from tabulate import tabulate

from config import get_config
from db_connector import db_connector
from error_handler import CSVUploadError, log_and_raise

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('validator')


class DataValidator:
    """Class xác thực dữ liệu đã nạp vào cơ sở dữ liệu."""
    
    def __init__(self, table_name: str, csv_file_path: Optional[str] = None, 
                expected_row_count: Optional[int] = None):
        """
        Khởi tạo DataValidator.
        
        Args:
            table_name: Tên bảng cần xác thực.
            csv_file_path: Đường dẫn đến tệp CSV nguồn (nếu có).
            expected_row_count: Số hàng dự kiến trong bảng (nếu biết).
        """
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.expected_row_count = expected_row_count
        
        # Kết nối cơ sở dữ liệu
        self.db_connector = db_connector
        
        # Kết quả xác thực
        self.validation_results = {
            'table_name': table_name,
            'csv_file': csv_file_path,
            'tests': {},
            'overall_status': 'Not Run'
        }
        
        logger.info(f"Đã khởi tạo DataValidator cho bảng {table_name}")
    
    def check_table_exists(self) -> bool:
        """
        Kiểm tra xem bảng có tồn tại trong cơ sở dữ liệu không.
        
        Returns:
            True nếu bảng tồn tại, False nếu không.
        """
        logger.info(f"Kiểm tra bảng {self.table_name} có tồn tại không")
        
        try:
            exists = self.db_connector.table_exists(self.table_name)
            
            test_name = "table_exists"
            self.validation_results['tests'][test_name] = {
                'status': 'Passed' if exists else 'Failed',
                'message': f"Bảng '{self.table_name}' {'tồn tại' if exists else 'không tồn tại'}"
            }
            
            if not exists:
                logger.error(f"Bảng {self.table_name} không tồn tại")
                return False
                
            logger.info(f"Bảng {self.table_name} tồn tại")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra bảng tồn tại: {e}")
            
            test_name = "table_exists"
            self.validation_results['tests'][test_name] = {
                'status': 'Error',
                'message': f"Lỗi: {str(e)}"
            }
            
            return False
    
    def verify_row_count(self) -> bool:
        """
        Kiểm tra số lượng hàng trong bảng, so sánh với số lượng dự kiến nếu được cung cấp.
        
        Returns:
            True nếu số lượng hàng khớp hoặc không có so sánh, False nếu không khớp.
        """
        logger.info(f"Xác minh số lượng hàng trong bảng {self.table_name}")
        
        try:
            # Đếm số hàng
            row_count = self.db_connector.get_table_row_count(self.table_name)
            
            test_name = "row_count"
            self.validation_results['tests'][test_name] = {
                'actual_count': row_count
            }
            
            # So sánh với số lượng dự kiến nếu được cung cấp
            if self.expected_row_count is not None:
                match = row_count == self.expected_row_count
                
                self.validation_results['tests'][test_name].update({
                    'expected_count': self.expected_row_count,
                    'status': 'Passed' if match else 'Failed',
                    'message': (f"Số hàng khớp: {row_count}" if match else 
                               f"Số hàng không khớp: {row_count} (dự kiến: {self.expected_row_count})")
                })
                
                if not match:
                    logger.warning(f"Số hàng không khớp: {row_count} (dự kiến: {self.expected_row_count})")
                    return False
            else:
                self.validation_results['tests'][test_name].update({
                    'status': 'Info',
                    'message': f"Số hàng trong bảng: {row_count} (không có giá trị dự kiến)"
                })
            
            logger.info(f"Số hàng trong bảng {self.table_name}: {row_count}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi xác minh số lượng hàng: {e}")
            
            test_name = "row_count"
            self.validation_results['tests'][test_name] = {
                'status': 'Error',
                'message': f"Lỗi: {str(e)}"
            }
            
            return False
    
    def get_sample_data(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Lấy mẫu dữ liệu từ bảng.
        
        Args:
            limit: Số hàng mẫu cần lấy.
            
        Returns:
            Danh sách các hàng mẫu (list of dicts).
        """
        logger.info(f"Lấy mẫu {limit} hàng từ bảng {self.table_name}")
        
        try:
            # Lấy mẫu dữ liệu
            sample_data = self.db_connector.execute_query(f"SELECT * FROM `{self.table_name}` LIMIT {limit}")
            
            test_name = "sample_data"
            self.validation_results['tests'][test_name] = {
                'status': 'Info',
                'message': f"Đã lấy {len(sample_data)} hàng mẫu",
                'data': sample_data
            }
            
            logger.info(f"Đã lấy {len(sample_data)} hàng mẫu từ bảng {self.table_name}")
            return sample_data
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy mẫu dữ liệu: {e}")
            
            test_name = "sample_data"
            self.validation_results['tests'][test_name] = {
                'status': 'Error',
                'message': f"Lỗi: {str(e)}",
                'data': []
            }
            
            return []
    
    def check_null_values(self) -> Dict[str, int]:
        """
        Kiểm tra số lượng giá trị NULL trong mỗi cột.
        
        Returns:
            Dict ánh xạ tên cột sang số lượng giá trị NULL.
        """
        logger.info(f"Kiểm tra giá trị NULL trong bảng {self.table_name}")
        
        try:
            # Lấy danh sách cột
            columns = self._get_table_columns()
            
            # Kiểm tra NULL cho từng cột
            null_counts = {}
            with self.db_connector.get_connection() as conn:
                for column in columns:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{self.table_name}` WHERE `{column}` IS NULL"))
                    null_count = result.scalar()
                    null_counts[column] = null_count
            
            test_name = "null_values"
            self.validation_results['tests'][test_name] = {
                'status': 'Info',
                'message': f"Đã kiểm tra NULL trên {len(columns)} cột",
                'data': null_counts
            }
            
            # Cảnh báo nếu có quá nhiều NULL
            high_null_cols = {col: count for col, count in null_counts.items() if count > 0}
            if high_null_cols:
                logger.warning(f"Cột có giá trị NULL: {high_null_cols}")
                
            logger.info(f"Đã kiểm tra NULL trên {len(columns)} cột của bảng {self.table_name}")
            return null_counts
            
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra giá trị NULL: {e}")
            
            test_name = "null_values"
            self.validation_results['tests'][test_name] = {
                'status': 'Error',
                'message': f"Lỗi: {str(e)}",
                'data': {}
            }
            
            return {}
    
    def check_column_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Lấy thống kê cơ bản cho các cột số.
        
        Returns:
            Dict ánh xạ tên cột số sang thống kê (min, max, avg, sum).
        """
        logger.info(f"Lấy thống kê cột cho bảng {self.table_name}")
        
        try:
            # Lấy thông tin kiểu dữ liệu cột
            column_info = self._get_column_types()
            
            # Lọc các cột số
            numeric_columns = [col for col, type_info in column_info.items() 
                              if 'INT' in type_info.upper() or 
                                 'FLOAT' in type_info.upper() or 
                                 'DOUBLE' in type_info.upper() or 
                                 'DECIMAL' in type_info.upper()]
            
            # Lấy thống kê cho từng cột số
            column_stats = {}
            with self.db_connector.get_connection() as conn:
                for column in numeric_columns:
                    query = f"""
                        SELECT 
                            MIN(`{column}`) as min_val,
                            MAX(`{column}`) as max_val,
                            AVG(`{column}`) as avg_val,
                            SUM(`{column}`) as sum_val,
                            COUNT(*) as count_val,
                            COUNT(`{column}`) as non_null_count
                        FROM `{self.table_name}`
                    """
                    result = conn.execute(text(query)).fetchone()
                    
                    column_stats[column] = {
                        'min': result.min_val,
                        'max': result.max_val,
                        'avg': result.avg_val,
                        'sum': result.sum_val,
                        'count': result.count_val,
                        'non_null_count': result.non_null_count
                    }
            
            test_name = "column_stats"
            self.validation_results['tests'][test_name] = {
                'status': 'Info',
                'message': f"Đã lấy thống kê cho {len(numeric_columns)} cột số",
                'data': column_stats
            }
            
            logger.info(f"Đã lấy thống kê cho {len(numeric_columns)} cột số của bảng {self.table_name}")
            return column_stats
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy thống kê cột: {e}")
            
            test_name = "column_stats"
            self.validation_results['tests'][test_name] = {
                'status': 'Error',
                'message': f"Lỗi: {str(e)}",
                'data': {}
            }
            
            return {}
    
    def check_duplicates(self, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Kiểm tra hàng trùng lặp dựa trên các cột chỉ định.
        
        Args:
            columns: Danh sách cột để kiểm tra trùng lặp. Nếu None, sử dụng tất cả cột.
            
        Returns:
            Dict chứa kết quả kiểm tra trùng lặp.
        """
        logger.info(f"Kiểm tra hàng trùng lặp trong bảng {self.table_name}")
        
        try:
            # Nếu không chỉ định cột, sử dụng tất cả cột
            if columns is None:
                columns = self._get_table_columns()
            
            columns_str = ", ".join([f"`{col}`" for col in columns])
            
            # Đếm số lượng trùng lặp
            query = f"""
                SELECT {columns_str}, COUNT(*) as count
                FROM `{self.table_name}`
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """
            
            duplicates = []
            with self.db_connector.get_connection() as conn:
                result = conn.execute(text(query))
                duplicates = [dict(row) for row in result]
            
            # Tổng số hàng trùng lặp
            total_duplicates = 0
            if duplicates:
                dup_query = f"""
                    SELECT SUM(dup_count) - COUNT(*)
                    FROM (
                        SELECT {columns_str}, COUNT(*) as dup_count
                        FROM `{self.table_name}`
                        GROUP BY {columns_str}
                        HAVING COUNT(*) > 1
                    ) as t
                """
                with self.db_connector.get_connection() as conn:
                    result = conn.execute(text(dup_query))
                    total_duplicates = result.scalar() or 0
            
            test_name = "duplicates"
            status = 'Passed' if not duplicates else 'Warning'
            
            self.validation_results['tests'][test_name] = {
                'status': status,
                'message': f"{'Không tìm thấy' if not duplicates else f'Tìm thấy {total_duplicates}'} hàng trùng lặp",
                'columns_checked': columns,
                'sample_duplicates': duplicates,
                'total_duplicates': total_duplicates
            }
            
            if duplicates:
                logger.warning(f"Tìm thấy {total_duplicates} hàng trùng lặp trong bảng {self.table_name}")
            else:
                logger.info(f"Không tìm thấy hàng trùng lặp trong bảng {self.table_name}")
                
            return self.validation_results['tests'][test_name]
            
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra hàng trùng lặp: {e}")
            
            test_name = "duplicates"
            self.validation_results['tests'][test_name] = {
                'status': 'Error',
                'message': f"Lỗi: {str(e)}",
                'columns_checked': columns or [],
                'sample_duplicates': [],
                'total_duplicates': 0
            }
            
            return self.validation_results['tests'][test_name]
    
    def compare_with_csv(self, csv_file_path: Optional[str] = None, 
                        encoding: Optional[str] = None, 
                        delimiter: Optional[str] = None) -> Dict[str, Any]:
        """
        So sánh dữ liệu trong bảng với dữ liệu trong tệp CSV gốc.
        
        Args:
            csv_file_path: Đường dẫn đến tệp CSV. Nếu None, sử dụng csv_file_path của instance.
            encoding: Mã hóa ký tự của tệp CSV.
            delimiter: Ký tự phân cách trong tệp CSV.
            
        Returns:
            Dict chứa kết quả so sánh.
        """
        if csv_file_path is None:
            csv_file_path = self.csv_file_path
            
        if not csv_file_path:
            logger.warning("Không có tệp CSV để so sánh")
            
            test_name = "csv_comparison"
            self.validation_results['tests'][test_name] = {
                'status': 'Skipped',
                'message': "Không có tệp CSV để so sánh"
            }
            
            return self.validation_results['tests'][test_name]
        
        logger.info(f"So sánh dữ liệu bảng {self.table_name} với tệp CSV {csv_file_path}")
        
        try:
            # Đọc dữ liệu từ CSV
            csv_df = pd.read_csv(
                csv_file_path,
                encoding=encoding,
                delimiter=delimiter,
                low_memory=False,
                on_bad_lines='warn'
            )
            
            # Lấy dữ liệu từ bảng
            with self.db_connector.get_connection() as conn:
                db_df = pd.read_sql(f"SELECT * FROM `{self.table_name}`", conn)
            
            # So sánh số lượng hàng
            csv_rows = len(csv_df)
            db_rows = len(db_df)
            row_match = csv_rows == db_rows
            
            # So sánh cột
            csv_cols = set(csv_df.columns)
            db_cols = set(db_df.columns)
            common_cols = csv_cols.intersection(db_cols)
            
            # Kiểm tra mẫu dữ liệu - lấy 5 hàng đầu tiên
            sample_comparison = {}
            for col in common_cols:
                # Kiểm tra xem 5 giá trị đầu tiên có khớp không
                csv_sample = csv_df[col].head(5).tolist()
                db_sample = db_df[col].head(5).tolist()
                
                # Chuyển đổi sang kiểu dữ liệu đơn giản để so sánh
                csv_sample = [str(x) if not pd.isna(x) else None for x in csv_sample]
                db_sample = [str(x) if not pd.isna(x) else None for x in db_sample]
                
                # So sánh mẫu
                sample_match = csv_sample == db_sample
                
                sample_comparison[col] = {
                    'match': sample_match,
                    'csv_sample': csv_sample,
                    'db_sample': db_sample
                }
            
            test_name = "csv_comparison"
            status = 'Passed' if row_match and len(common_cols) == len(csv_cols) else 'Warning'
            
            self.validation_results['tests'][test_name] = {
                'status': status,
                'message': f"So sánh với tệp CSV: {'Khớp' if row_match else 'Không khớp'} số hàng",
                'csv_rows': csv_rows,
                'db_rows': db_rows,
                'rows_match': row_match,
                'csv_columns': list(csv_cols),
                'db_columns': list(db_cols),
                'common_columns': list(common_cols),
                'sample_comparison': sample_comparison
            }
            
            if not row_match:
                logger.warning(f"Số hàng không khớp: CSV={csv_rows}, DB={db_rows}")
            else:
                logger.info(f"Số hàng khớp: {csv_rows}")
                
            if len(common_cols) != len(csv_cols):
                missing_cols = csv_cols - db_cols
                extra_cols = db_cols - csv_cols
                
                if missing_cols:
                    logger.warning(f"Các cột CSV thiếu trong DB: {missing_cols}")
                if extra_cols:
                    logger.warning(f"Các cột DB không có trong CSV: {extra_cols}")
            
            return self.validation_results['tests'][test_name]
            
        except Exception as e:
            logger.error(f"Lỗi khi so sánh với tệp CSV: {e}")
            
            test_name = "csv_comparison"
            self.validation_results['tests'][test_name] = {
                'status': 'Error',
                'message': f"Lỗi: {str(e)}"
            }
            
            return self.validation_results['tests'][test_name]
    
    def run_all_checks(self) -> Dict[str, Any]:
        """
        Chạy tất cả các kiểm tra xác thực.
        
        Returns:
            Dict chứa tất cả kết quả xác thực.
        """
        logger.info(f"Chạy tất cả các kiểm tra xác thực cho bảng {self.table_name}")
        
        # Kiểm tra bảng tồn tại
        table_exists = self.check_table_exists()
        if not table_exists:
            self.validation_results['overall_status'] = 'Failed'
            return self.validation_results
        
        # Xác minh số lượng hàng
        self.verify_row_count()
        
        # Lấy mẫu dữ liệu
        self.get_sample_data()
        
        # Kiểm tra NULL
        self.check_null_values()
        
        # Kiểm tra thống kê cột
        self.check_column_stats()
        
        # Kiểm tra trùng lặp
        self.check_duplicates()
        
        # So sánh với CSV gốc nếu có
        if self.csv_file_path:
            self.compare_with_csv()
        
        # Xác định trạng thái tổng thể
        failed_tests = [name for name, test in self.validation_results['tests'].items() 
                      if test.get('status') == 'Failed']
        
        error_tests = [name for name, test in self.validation_results['tests'].items() 
                     if test.get('status') == 'Error']
        
        warning_tests = [name for name, test in self.validation_results['tests'].items() 
                       if test.get('status') == 'Warning']
        
        if failed_tests:
            self.validation_results['overall_status'] = 'Failed'
        elif error_tests:
            self.validation_results['overall_status'] = 'Error'
        elif warning_tests:
            self.validation_results['overall_status'] = 'Warning'
        else:
            self.validation_results['overall_status'] = 'Passed'
        
        logger.info(f"Kết thúc xác thực bảng {self.table_name}, trạng thái: {self.validation_results['overall_status']}")
        return self.validation_results
    
    def _get_table_columns(self) -> List[str]:
        """
        Lấy danh sách cột trong bảng.
        
        Returns:
            Danh sách tên cột.
        """
        try:
            with self.db_connector.get_connection() as conn:
                # Lấy thông tin schema từ bảng
                query = f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{self.table_name}'
                    ORDER BY ORDINAL_POSITION
                """
                result = conn.execute(text(query))
                columns = [row[0] for row in result]
                
            return columns
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách cột: {e}")
            return []
    
    def _get_column_types(self) -> Dict[str, str]:
        """
        Lấy kiểu dữ liệu của các cột trong bảng.
        
        Returns:
            Dict ánh xạ tên cột sang kiểu dữ liệu.
        """
        try:
            with self.db_connector.get_connection() as conn:
                # Lấy thông tin kiểu dữ liệu từ bảng
                query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{self.table_name}'
                    ORDER BY ORDINAL_POSITION
                """
                result = conn.execute(text(query))
                column_types = {row[0]: row[1] for row in result}
                
            return column_types
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy kiểu dữ liệu cột: {e}")
            return {}
    
    def print_results(self) -> None:
        """In kết quả xác thực ra console."""
        # Tổng hợp kết quả
        overall_status = self.validation_results['overall_status']
        
        print("\n" + "="*50)
        print(f"KẾT QUẢ XÁC THỰC CHO BẢNG {self.table_name}")
        print("="*50)
        print(f"TRẠNG THÁI TỔNG THỂ: {overall_status}")
        print("-"*50)
        
        # In thông tin cơ bản
        if 'table_exists' in self.validation_results['tests']:
            print(f"Bảng tồn tại: {self.validation_results['tests']['table_exists']['status']}")
            
        if 'row_count' in self.validation_results['tests']:
            test = self.validation_results['tests']['row_count']
            print(f"Số hàng: {test.get('actual_count', 'N/A')}")
            if 'expected_count' in test:
                print(f"Số hàng dự kiến: {test['expected_count']}")
                
        # In thông tin NULL
        if 'null_values' in self.validation_results['tests'] and 'data' in self.validation_results['tests']['null_values']:
            null_data = self.validation_results['tests']['null_values']['data']
            
            if null_data:
                print("\nGiá trị NULL:")
                null_cols = [(col, count) for col, count in null_data.items() if count > 0]
                
                if null_cols:
                    print(tabulate(null_cols, headers=["Cột", "Số NULL"], tablefmt="simple"))
                else:
                    print("Không có giá trị NULL")
        
        # In mẫu dữ liệu
        if 'sample_data' in self.validation_results['tests'] and 'data' in self.validation_results['tests']['sample_data']:
            sample_data = self.validation_results['tests']['sample_data']['data']
            
            if sample_data:
                print("\nMẫu dữ liệu (tối đa 5 hàng):")
                # Chuyển đổi danh sách dict thành list of lists
                headers = list(sample_data[0].keys())
                rows = [[row.get(col) for col in headers] for row in sample_data]
                print(tabulate(rows, headers=headers, tablefmt="simple"))
        
        # In các thông tin khác
        if 'duplicates' in self.validation_results['tests']:
            test = self.validation_results['tests']['duplicates']
            print(f"\nHàng trùng lặp: {test.get('total_duplicates', 0)}")
        
        print("="*50)
    
    def save_results(self, output_file: str) -> None:
        """
        Lưu kết quả xác thực ra tệp JSON.
        
        Args:
            output_file: Đường dẫn đến tệp đầu ra.
        """
        # Đảm bảo thư mục tồn tại
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Lưu kết quả
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.validation_results, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Đã lưu kết quả xác thực vào tệp {output_file}")


def validate_table(table_name: str, csv_file_path: Optional[str] = None, 
                 expected_row_count: Optional[int] = None, 
                 output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Hàm tiện ích để xác thực dữ liệu trong bảng.
    
    Args:
        table_name: Tên bảng cần xác thực.
        csv_file_path: Đường dẫn đến tệp CSV nguồn (nếu có).
        expected_row_count: Số hàng dự kiến trong bảng (nếu biết).
        output_file: Đường dẫn để lưu kết quả xác thực (nếu cần).
        
    Returns:
        Dict chứa kết quả xác thực.
    """
    # Khởi tạo validator
    validator = DataValidator(table_name, csv_file_path, expected_row_count)
    
    # Chạy tất cả các kiểm tra
    results = validator.run_all_checks()
    
    # In kết quả
    validator.print_results()
    
    # Lưu kết quả nếu cần
    if output_file:
        validator.save_results(output_file)
    
    return results


if __name__ == "__main__":
    # Kiểm thử script
    import sys
    
    if len(sys.argv) > 1:
        test_table = sys.argv[1]
        test_csv = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        test_table = "example_table"
        test_csv = "data/example.csv" if os.path.exists("data/example.csv") else None
        
    try:
        results = validate_table(
            test_table, 
            test_csv,
            output_file=f"validation_{test_table}.json"
        )
        
        if results['overall_status'] == 'Passed':
            print("\nXác thực thành công!")
        else:
            print(f"\nXác thực kết thúc với trạng thái: {results['overall_status']}")
            
    except Exception as e:
        print(f"Lỗi: {e}")
