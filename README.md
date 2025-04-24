# Dự án: Tải Dữ liệu CSV vào Cơ sở Dữ liệu SQL và Truy Vấn

## Tổng quan
Dự án này xây dựng một quy trình tự động hóa bằng Python để chuyển đổi dữ liệu từ các tệp CSV có cấu trúc đa dạng vào hệ quản trị cơ sở dữ liệu quan hệ (SQL Database), cụ thể là MySQL. Chương trình tự động phân tích cấu trúc tệp CSV, suy luận kiểu dữ liệu, tạo cấu trúc bảng và tải dữ liệu vào cơ sở dữ liệu, sau đó thực hiện các kiểm tra xác thực để đảm bảo tính toàn vẹn của dữ liệu.

## Thách thức cốt lõi
Điểm nổi bật của dự án là khả năng thích ứng với các tệp CSV có cấu trúc khác nhau:
- Tự động phát hiện ký tự phân cách (comma, semicolon, tab...)
- Xác định dòng tiêu đề và vị trí của nó
- Suy luận kiểu dữ liệu hợp lý cho từng cột
- Xử lý các vấn đề về mã hóa ký tự (UTF-8, Latin-1...)
- Hỗ trợ nhiều tùy chọn nạp dữ liệu (thay thế, bổ sung, phân khối...)

## Tính năng chính
- **Phân tích tự động**: Tự động phát hiện cấu trúc CSV mà không cần cấu hình thủ công
- **Suy luận lược đồ**: Tự động xác định kiểu dữ liệu SQL phù hợp cho từng cột
- **Nạp dữ liệu linh hoạt**: Hỗ trợ nhiều phương pháp nạp dữ liệu (pandas, thủ công)
- **Xử lý chunk**: Tối ưu bộ nhớ với khả năng xử lý tệp CSV lớn theo từng phần
- **Xác thực dữ liệu**: Kiểm tra tính toàn vẹn sau khi nạp
- **Xử lý lỗi mạnh mẽ**: Ghi log chi tiết và cơ chế xử lý ngoại lệ
- **Cấu hình linh hoạt**: Thông qua tệp YAML hoặc biến môi trường

## Cấu trúc dự án
Dự án được tổ chức thành các module chuyên biệt:

```
upload-csv/
│
├── main.py                  # Script chính điều phối toàn bộ quy trình
├── csv_analyzer.py          # Phân tích cấu trúc tệp CSV (encoding, delimiter, header)
├── schema_generator.py      # Suy luận lược đồ và tạo câu lệnh SQL CREATE TABLE
├── db_connector.py          # Quản lý kết nối và tương tác với cơ sở dữ liệu
├── data_loader.py           # Nạp dữ liệu từ CSV vào SQL với nhiều phương pháp
├── validator.py             # Xác thực dữ liệu đã nạp bằng các truy vấn SQL
├── utils.py                 # Các tiện ích và hàm hỗ trợ chung
├── config.py                # Xử lý cấu hình từ file hoặc biến môi trường
├── error_handler.py         # Xử lý lỗi và ghi log chi tiết
├── config.yaml              # File cấu hình mặc định
│
├── requirements.txt         # Danh sách thư viện phụ thuộc
│
├── data/                    # Thư mục chứa các tệp CSV mẫu
│   ├── example.csv
│   └── ...
│
└── errors/                  # Thư mục chứa log lỗi và dữ liệu không nạp được
    └── ...
```

## Quy trình hoạt động

### Pha 1: Phân tích và Chuẩn bị
1. **Phân tích CSV**: Phát hiện ký tự phân cách, dòng tiêu đề, mã hóa ký tự
2. **Suy luận lược đồ**: Xác định kiểu dữ liệu SQL tối ưu cho mỗi cột
3. **Tạo câu lệnh SQL**: Xây dựng câu lệnh CREATE TABLE động

### Pha 2: Nạp dữ liệu
1. **Kết nối CSDL**: Thiết lập kết nối an toàn, tạo DB nếu cần
2. **Tạo bảng**: Thực thi câu lệnh CREATE TABLE
3. **Đọc và nạp dữ liệu**: Sử dụng Pandas hoặc phương pháp thủ công
4. **Xử lý lỗi**: Theo dõi và xử lý các hàng không nạp được

### Pha 3: Xác thực và hoàn tất
1. **Kiểm tra số lượng**: So sánh số dòng trong CSV và bảng database
2. **Kiểm tra tính toàn vẹn**: Thực hiện các truy vấn xác thực
3. **Báo cáo kết quả**: Cung cấp thông tin về quá trình nạp

## Yêu cầu hệ thống
- Python 3.6 trở lên
- MySQL/MariaDB (hoặc hệ CSDL SQL tương thích khác)
- Các thư viện Python:
  - pandas >= 1.3.0
  - sqlalchemy >= 1.4.0
  - pymysql >= 1.0.2
  - chardet >= 4.0.0
  - numpy >= 1.20.0 
  - pytest >= 6.2.5
  - python-dotenv >= 0.19.0
  - pyyaml >= 6.0
  - tqdm >= 4.62.0

## Cài đặt
1. Clone repository:
```bash
git clone https://github.com/username/upload-csv.git
cd upload-csv
```

2. Cài đặt các thư viện phụ thuộc:
```bash
pip install -r requirements.txt
```

3. Cấu hình thông tin kết nối CSDL trong tệp `config.yaml` hoặc tạo file `.env`:
```yaml
# config.yaml
database:
  host: localhost
  port: 3306
  user: root
  password: your_password
  database: csv_import
  auto_create_db: true
```

## Cách sử dụng
### Sử dụng dòng lệnh
```bash
python main.py --csv-file data/example.csv --table-name customers
```

### Tham số đầy đủ
```bash
python main.py --csv-file data/example.csv --table-name customers --chunk-size 1000 --if-exists replace --delimiter "," --encoding "utf-8" --skip-validation --verbose
```

### Tham số:
- `--csv-file`: Đường dẫn đến tệp CSV cần nạp (bắt buộc)
- `--table-name`: Tên bảng đích trong cơ sở dữ liệu (bắt buộc)
- `--config-file`: Tệp cấu hình (mặc định: config.yaml)
- `--chunk-size`: Số dòng xử lý mỗi lần (tối ưu bộ nhớ)
- `--if-exists`: Hành động khi bảng tồn tại (fail/replace/append)
- `--delimiter`: Ký tự phân cách trong CSV (tự động phát hiện nếu không chỉ định)
- `--encoding`: Mã hóa của tệp CSV (tự động phát hiện nếu không chỉ định)
- `--no-header`: Chỉ định tệp CSV không có dòng tiêu đề
- `--header-row`: Chỉ định dòng tiêu đề (mặc định: 0)
- `--schema-file`: Tệp xác định schema thay vì tự động suy luận
- `--analyze-only`: Chỉ phân tích CSV mà không nạp vào database
- `--dry-run`: Thử nghiệm không thực sự tải vào database
- `--skip-validation`: Bỏ qua bước xác thực sau khi nạp
- `--verbose`: Hiển thị thông tin chi tiết
- `--chunk-method`: Phương pháp xử lý chunk (pandas/manual)

## Ví dụ sử dụng
### Phân tích tệp CSV mà không nạp vào database
```bash
python main.py --csv-file data/example.csv --analyze-only
```

### Nạp dữ liệu với tùy chọn thay thế bảng hiện có
```bash
python main.py --csv-file data/sales.csv --table-name sales --if-exists replace
```

### Xử lý tệp CSV lớn theo từng phần
```bash
python main.py --csv-file data/large_dataset.csv --table-name data --chunk-size 10000 --chunk-method manual
```

## Chiến lược tối ưu hiệu năng
1. **Xử lý phân khối**: Sử dụng `chunk-size` để xử lý từng phần tệp CSV lớn
2. **Chỉ định kiểu dữ liệu**: Sử dụng schema-file để tránh phải suy luận kiểu dữ liệu
3. **Chỉ số (Indexes)**: Tạo chỉ số cho các cột thường xuyên truy vấn
4. **Giao dịch (Transactions)**: Nạp dữ liệu trong các giao dịch để tăng tốc
5. **Phương pháp "multi"**: Tối ưu hóa câu lệnh INSERT khi sử dụng pandas

## Xử lý lỗi và debugging
- Log chi tiết được lưu trong thư mục `errors/`
- Dữ liệu không nạp được sẽ được lưu trong "dead-letter queue"
- Tùy chọn `--verbose` cung cấp thông tin chi tiết hơn khi chạy

## Đóng góp
Đóng góp cho dự án luôn được chào đón:
1. Fork repository
2. Tạo nhánh tính năng mới (`git checkout -b feature/amazing-feature`)
3. Commit thay đổi (`git commit -m 'Add amazing feature'`)
4. Push lên nhánh (`git push origin feature/amazing-feature`)
5. Tạo Pull Request

## Giấy phép
[MIT](LICENSE)

## Tác giả
[Phùng Đình Trường]
