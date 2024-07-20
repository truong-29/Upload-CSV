#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
csv_analyzer.py
--------------
Module phân tích cấu trúc tệp CSV đầu vào.
Phát hiện ký tự phân cách, dòng tiêu đề, kiểu mã hóa và lấy mẫu dữ liệu ban đầu.
"""

import pandas as pd
import csv
import io
import chardet
import logging
from typing import Dict, List, Tuple, Optional, Any, Union
import os

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('csv_analyzer')

class CSVAnalyzer:
    """Class phân tích cấu trúc tệp CSV đầu vào."""
    
    def __init__(self, file_path: str, sample_size: int = 1000):
        """
        Khởi tạo CSVAnalyzer với đường dẫn tệp CSV.
        
        Args:
            file_path: Đường dẫn đến tệp CSV cần phân tích
            sample_size: Số dòng để đọc trong lần lấy mẫu đầu tiên
        """
        self.file_path = file_path
        self.sample_size = sample_size
        self.encoding = None
        self.delimiter = None
        self.has_header = True  # Mặc định là có header
        self.header_row = 0  # Mặc định là dòng đầu tiên
        self.column_names = []
        self.sample_df = None
        
        # Kiểm tra tệp tồn tại
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Không tìm thấy tệp CSV: {file_path}")
    
    def detect_encoding(self) -> str:
        """
        Phát hiện kiểu mã hóa của tệp CSV.
        
        Returns:
            Kiểu mã hóa phát hiện được (ví dụ: 'utf-8', 'latin-1')
        """
        logger.info(f"Phát hiện kiểu mã hóa của tệp {self.file_path}")
        
        # Đọc một phần nhỏ của tệp để phát hiện mã hóa
        with open(self.file_path, 'rb') as f:
            raw_data = f.read(min(10000, os.path.getsize(self.file_path)))
        
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        
        logger.info(f"Đã phát hiện mã hóa: {encoding} (độ tin cậy: {confidence:.2f})")
        
        # Nếu độ tin cậy thấp, sử dụng UTF-8 làm mặc định an toàn
        if confidence < 0.7:
            logger.warning(f"Độ tin cậy thấp ({confidence:.2f}), sử dụng UTF-8 làm mặc định")
            encoding = 'utf-8'
        
        self.encoding = encoding
        return encoding
        
    def detect_delimiter(self) -> str:
        """
        Phát hiện ký tự phân cách của tệp CSV.
        
        Returns:
            Ký tự phân cách phát hiện được (ví dụ: ',', ';', '\t')
        """
        logger.info(f"Phát hiện ký tự phân cách của tệp {self.file_path}")
        
        # Đảm bảo đã phát hiện mã hóa
        if not self.encoding:
            self.detect_encoding()
        
        # Đọc một số dòng đầu tiên để phát hiện delimiter
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                sample = ''.join(f.readline() for _ in range(min(5, self.sample_size)))
            
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            delimiter = dialect.delimiter
            
            logger.info(f"Đã phát hiện ký tự phân cách: '{delimiter}'")
            self.delimiter = delimiter
            return delimiter
            
        except Exception as e:
            logger.warning(f"Không thể tự động phát hiện ký tự phân cách: {e}")
            logger.info("Thử đếm tần suất các ký tự phân cách tiềm năng...")
            
            # Phương pháp dự phòng: Đếm tần suất các ký tự phân cách phổ biến
            potential_delimiters = [',', ';', '\t', '|', ' ']
            delimiter_counts = {d: sample.count(d) for d in potential_delimiters}
            
            # Loại bỏ các ký tự không xuất hiện hoặc xuất hiện quá nhiều
            filtered_delimiters = {d: c for d, c in delimiter_counts.items() 
                                if c > 0 and c < len(sample) / 2}
            
            if filtered_delimiters:
                delimiter = max(filtered_delimiters, key=filtered_delimiters.get)
                logger.info(f"Sử dụng ký tự phân cách phổ biến nhất: '{delimiter}'")
                self.delimiter = delimiter
                return delimiter
            
            # Nếu không tìm thấy, mặc định là dấu phẩy
            logger.warning("Không thể xác định ký tự phân cách, sử dụng dấu phẩy làm mặc định")
            self.delimiter = ','
            return ','
    
    def detect_header(self) -> Tuple[bool, int]:
        """
        Phát hiện xem tệp CSV có dòng tiêu đề không và ở vị trí nào.
        
        Returns:
            (has_header, header_row): has_header là True nếu có dòng tiêu đề,
                                     header_row là vị trí dòng tiêu đề (0-based)
        """
        logger.info(f"Phát hiện dòng tiêu đề của tệp {self.file_path}")
        
        # Đảm bảo đã phát hiện mã hóa và ký tự phân cách
        if not self.encoding:
            self.detect_encoding()
        if not self.delimiter:
            self.detect_delimiter()
        
        try:
            # Đọc một số dòng đầu tiên để phát hiện header
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                sample_lines = [f.readline().strip() for _ in range(min(5, self.sample_size))]
            
            # Sử dụng CSV Sniffer để phát hiện header
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header('\n'.join(sample_lines))
            
            logger.info(f"Đã phát hiện header: {'Có' if has_header else 'Không'}")
            
            # Nếu không có header, tạo tên cột mặc định
            if not has_header:
                # Đếm số cột bằng cách phân tích dòng đầu tiên
                num_columns = len(next(csv.reader([sample_lines[0]], delimiter=self.delimiter)))
                self.column_names = [f"column_{i+1}" for i in range(num_columns)]
                logger.info(f"Không có tiêu đề, sử dụng tên cột mặc định: {self.column_names}")
            
            self.has_header = has_header
            self.header_row = 0 if has_header else None
            
            return has_header, 0 if has_header else None
            
        except Exception as e:
            logger.warning(f"Không thể tự động phát hiện tiêu đề: {e}")
            logger.info("Giả định có tiêu đề ở dòng đầu tiên (mặc định)")
            
            self.has_header = True
            self.header_row = 0
            
            return True, 0
    
    def get_sample_data(self, nrows: int = None) -> pd.DataFrame:
        """
        Lấy mẫu dữ liệu từ tệp CSV.
        
        Args:
            nrows: Số dòng cần đọc, mặc định là giá trị sample_size
            
        Returns:
            DataFrame pandas chứa mẫu dữ liệu
        """
        if nrows is None:
            nrows = self.sample_size
            
        logger.info(f"Lấy mẫu {nrows} dòng dữ liệu từ tệp {self.file_path}")
        
        # Đảm bảo đã phát hiện tất cả thông tin cần thiết
        if not self.encoding:
            self.detect_encoding()
        if not self.delimiter:
            self.detect_delimiter()
        if self.header_row is None:
            self.detect_header()
        
        try:
            # Đọc mẫu dữ liệu với các tham số đã phát hiện
            df = pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                header=0 if self.has_header else None,
                nrows=nrows,
                low_memory=False
            )
            
            # Nếu không có header, sử dụng tên cột đã tạo trước đó
            if not self.has_header and self.column_names:
                df.columns = self.column_names
            else:
                self.column_names = list(df.columns)
            
            # Lưu mẫu DataFrame
            self.sample_df = df
            
            logger.info(f"Đã đọc mẫu dữ liệu, kích thước: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Lỗi khi đọc mẫu dữ liệu: {e}")
            raise
    
    def analyze(self) -> Dict[str, Any]:
        """
        Phân tích đầy đủ tệp CSV và trả về kết quả.
        
        Returns:
            Dictionary chứa tất cả thông tin đã phân tích
        """
        logger.info(f"Bắt đầu phân tích tệp {self.file_path}")
        
        # Thực hiện tất cả các bước phân tích
        encoding = self.detect_encoding()
        delimiter = self.detect_delimiter()
        has_header, header_row = self.detect_header()
        df_sample = self.get_sample_data()
        
        # Tổng hợp kết quả
        result = {
            'file_path': self.file_path,
            'encoding': encoding,
            'delimiter': delimiter,
            'has_header': has_header,
            'header_row': header_row,
            'column_names': self.column_names,
            'num_columns': len(self.column_names),
            'sample_rows': df_sample.shape[0],
            'sample_data': df_sample
        }
        
        logger.info(f"Phân tích tệp CSV hoàn tất: {result['num_columns']} cột")
        return result


def analyze_csv_file(file_path: str, sample_size: int = 1000) -> Dict[str, Any]:
    """
    Hàm tiện ích để phân tích một tệp CSV.
    
    Args:
        file_path: Đường dẫn đến tệp CSV cần phân tích
        sample_size: Số dòng để đọc trong lần lấy mẫu đầu tiên
        
    Returns:
        Dictionary chứa tất cả thông tin đã phân tích
    """
    analyzer = CSVAnalyzer(file_path, sample_size)
    return analyzer.analyze()


if __name__ == "__main__":
    # Kiểm thử script với một tệp CSV mẫu trong thư mục data/
    import sys
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
    else:
        test_file = "data/example.csv"
        
    try:
        result = analyze_csv_file(test_file)
        print("\nThông tin tệp CSV:")
        print(f"- Đường dẫn: {result['file_path']}")
        print(f"- Mã hóa: {result['encoding']}")
        print(f"- Ký tự phân cách: '{result['delimiter']}'")
        print(f"- Có tiêu đề: {'Có' if result['has_header'] else 'Không'}")
        print(f"- Số cột: {result['num_columns']}")
        print(f"- Tên các cột: {', '.join(result['column_names'])}")
        print("\nMẫu dữ liệu (5 dòng đầu):")
        print(result['sample_data'].head(5))
    except Exception as e:
        print(f"Lỗi: {e}")
