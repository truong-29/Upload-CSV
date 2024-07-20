#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Ứng dụng chính để tải dữ liệu CSV vào cơ sở dữ liệu MySQL.
"""

import os
import sys
import logging
import argparse
import pandas as pd
from time import time
from datetime import datetime

from config import get_config
from db_connector import db_connector
from validator import DataValidator as Validator
from error_handler import CSVUploadError, UploadStats, DeadLetterQueue
from csv_analyzer import CSVAnalyzer

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """
    Phân tích các tham số dòng lệnh.
    
    Returns:
        argparse.Namespace: Đối tượng chứa các tham số dòng lệnh.
    """
    parser = argparse.ArgumentParser(description='Tải dữ liệu từ file CSV vào MySQL.')
    
    parser.add_argument('--csv-file', required=True, help='Đường dẫn đến file CSV cần tải')
    parser.add_argument('--table-name', required=True, help='Tên bảng đích trong cơ sở dữ liệu')
    parser.add_argument('--config-file', default='config.yaml', help='File cấu hình (mặc định: config.yaml)')
    parser.add_argument('--chunk-size', type=int, help='Số dòng mỗi lần đọc từ CSV')
    parser.add_argument('--skip-validation', action='store_true', help='Bỏ qua kiểm tra dữ liệu')
    parser.add_argument('--if-exists', choices=['fail', 'replace', 'append'], 
                       help='Hành động khi bảng đã tồn tại')
    parser.add_argument('--verbose', '-v', action='store_true', help='Hiển thị thông tin chi tiết')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Thử nghiệm không thực sự tải vào cơ sở dữ liệu')
    parser.add_argument('--no-header', action='store_true', help='File CSV không có header')
    parser.add_argument('--header-row', type=int, default=0, help='Chỉ định dòng header (mặc định: 0)')
    parser.add_argument('--encoding', help='Mã hóa của file CSV')
    parser.add_argument('--delimiter', help='Ký tự phân cách trong CSV')
    parser.add_argument('--analyze-only', action='store_true', 
                       help='Chỉ phân tích file CSV mà không tải vào database')
    parser.add_argument('--schema-file', help='File schema xác định kiểu dữ liệu cho các cột')
    parser.add_argument('--chunk-method', choices=['pandas', 'manual'], 
                       help='Phương pháp xử lý chunk: pandas hoặc manual')
    
    return parser.parse_args()

def process_chunk_manually(df, table_name, if_exists, chunk_index, is_first_chunk):
    """
    Xử lý một chunk dữ liệu theo cách thủ công.
    
    Args:
        df (pd.DataFrame): DataFrame cần xử lý
        table_name (str): Tên bảng đích
        if_exists (str): Hành động khi bảng đã tồn tại
        chunk_index (int): Chỉ số của chunk
        is_first_chunk (bool): Có phải chunk đầu tiên không
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    # Thiết lập if_exists: 'replace' cho chunk đầu tiên, 'append' cho các chunk sau
    current_if_exists = if_exists if is_first_chunk else 'append'
    
    # Tải chunk vào cơ sở dữ liệu
    success = db_connector.create_table_from_df(
        df, 
        table_name, 
        if_exists=current_if_exists,
        index=False
    )
    
    if success:
        logger.info(f"Đã tải chunk {chunk_index+1} ({len(df)} dòng) vào bảng '{table_name}'")
    else:
        logger.error(f"Lỗi khi tải chunk thứ {chunk_index+1} vào bảng '{table_name}'")
    
    return success

def load_csv_to_db(args):
    """
    Tải dữ liệu từ file CSV vào cơ sở dữ liệu.
    
    Args:
        args (argparse.Namespace): Các tham số dòng lệnh
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    csv_file = args.csv_file
    table_name = args.table_name
    
    # Kiểm tra file tồn tại
    if not os.path.exists(csv_file):
        logger.error(f"File CSV không tồn tại: {csv_file}")
        return False
    
    # Lấy cấu hình và kết nối database
    config = get_config()
    
    # Thiết lập các tham số từ dòng lệnh hoặc cấu hình
    chunk_size = args.chunk_size or config.get('csv', 'chunk_size', 5000)
    if_exists = args.if_exists or config.get('table', 'if_exists', 'fail')
    
    # Tùy chọn đọc CSV
    csv_options = config.get_pandas_read_csv_options()
    
    # Ghi đè các tùy chọn từ tham số dòng lệnh
    if args.no_header:
        csv_options['header'] = None
    
    if args.header_row is not None:
        csv_options['header'] = args.header_row
    
    if args.encoding:
        csv_options['encoding'] = args.encoding
    
    if args.delimiter:
        csv_options['delimiter'] = args.delimiter
    
    # Nếu chỉ phân tích CSV
    if args.analyze_only:
        analyzer = CSVAnalyzer()
        result = analyzer.analyze_file(csv_file, **csv_options)
        print("\n=== Kết quả phân tích CSV ===")
        for key, value in result.items():
            print(f"{key}: {value}")
        return True
    
    # Kết nối cơ sở dữ liệu
    if not db_connector.is_connected() and not db_connector.connect():
        logger.error("Không thể kết nối đến cơ sở dữ liệu")
        return False
    
    # Kiểm tra trùng lặp bảng
    if db_connector.table_exists(table_name) and if_exists == 'fail':
        logger.error(f"Bảng '{table_name}' đã tồn tại và if_exists='fail'")
        return False
    
    # Thử nghiệm không tải vào DB
    if args.dry_run:
        logger.info(f"Chế độ thử nghiệm: Sẽ không tải dữ liệu vào bảng '{table_name}'")
        df_sample = pd.read_csv(csv_file, nrows=5, **csv_options)
        logger.info(f"Mẫu dữ liệu (5 dòng đầu):\n{df_sample}")
        return True
    
    start_time = time()
    total_rows = 0
    
    try:
        # Xác định phương pháp xử lý chunk
        chunk_method = args.chunk_method or config.get('csv', 'chunk_method', 'pandas')
        
        if chunk_method == 'pandas':
            # Phương pháp 1: Sử dụng chức năng có sẵn của db_connector
            logger.info(f"Đang tải CSV vào bảng '{table_name}' bằng phương pháp 'pandas'...")
            success, rows = db_connector.load_csv_to_table(
                csv_file,
                table_name,
                if_exists=if_exists,
                chunk_size=chunk_size,
                **csv_options
            )
            total_rows = rows
            
        else:
            # Phương pháp 2: Xử lý thủ công từng chunk
            logger.info(f"Đang tải CSV vào bảng '{table_name}' bằng phương pháp 'manual'...")
            
            # Đảm bảo chunksize được thiết lập trong tùy chọn
            csv_options['chunksize'] = chunk_size
            
            # Đọc file CSV theo từng chunk
            reader = pd.read_csv(csv_file, **csv_options)
            
            # Xử lý từng chunk
            for i, chunk in enumerate(reader):
                is_first_chunk = (i == 0)
                if not process_chunk_manually(chunk, table_name, if_exists, i, is_first_chunk):
                    return False
                
                total_rows += len(chunk)
        
        execution_time = time() - start_time
        
        # Hiển thị thông tin kết quả
        logger.info(f"Hoàn thành tải dữ liệu: {total_rows} dòng vào bảng '{table_name}'")
        logger.info(f"Thời gian thực hiện: {execution_time:.2f} giây")
        
        if args.verbose:
            # Hiển thị thông tin chi tiết về bảng
            table_info = db_connector.get_table_info(table_name)
            if table_info:
                print("\n=== Thông tin bảng ===")
                for key, value in table_info.items():
                    if key != 'columns':  # Hiển thị tất cả trừ chi tiết cột
                        print(f"{key}: {value}")
            
            # Hiển thị kích thước database
            db_size = db_connector.get_database_size()
            if db_size:
                print(f"\nKích thước database: {db_size['size_mb']} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Lỗi khi tải dữ liệu: {str(e)}")
        return False
    finally:
        # Đóng kết nối
        db_connector.disconnect()

def main():
    """Hàm chính của ứng dụng."""
    # Phân tích tham số
    args = parse_arguments()
    
    # Hiển thị thông tin
    logger.info(f"Bắt đầu tải dữ liệu từ {args.csv_file} vào bảng {args.table_name}")
    
    try:
        # Thực hiện tải dữ liệu
        success = load_csv_to_db(args)
        
        # Kết thúc ứng dụng với mã tương ứng
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Đã hủy quá trình tải dữ liệu bởi người dùng")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Lỗi không xác định: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
