# Project Glamira ETL Pipeline

## 📖 Giới thiệu dự án
Project Glamira là một Data Pipeline (ETL) hoàn chỉnh được thiết kế để tự động hóa việc trích xuất (Extract), biến đổi (Transform) và tải (Load) dữ liệu hành vi người dùng và thông tin sản phẩm từ hệ thống. 

Đặc biệt, module Crawler được xây dựng với cơ chế Anti-Bot mạnh mẽ, khả năng chịu lỗi (Fault-tolerance) và tự động ghi nhớ tiến trình (Checkpoint) để hoạt động bền bỉ trên môi trường Cloud (Google Cloud Platform - GCP).

---

## 📂 Cấu trúc thư mục (Project Structure)

```text
UNIGAP-ProjectGlamira/
├── config/
│   ├── __init__.py
│   └── get_mongo_connection.py      # Thiết lập kết nối đến MongoDB
├── data/
│   ├── raw/                         # Dữ liệu gốc (chưa xử lý)
│   └── processed/                   # Dữ liệu xuất ra và file Checkpoint
│       ├── error_404_productid.txt  # Log ID sản phẩm bị lỗi 404
│       ├── success_productid.txt    # Log ID sản phẩm đã crawl thành công (Checkpoint)
│       └── product_names.csv        # File CSV backup/log kết quả crawl
├── etl/
│   ├── extract/
│   │   ├── __init__.py
│   │   └── product_crawler.py       # Script crawl tên sản phẩm 
│   ├── load/
│   │   └── __init__.py
│   └── transform/
│       ├── __init__.py
│       └── ip_processing.py         # Script xử lý chuẩn hóa IP người dùng
├── src/
│   ├── __init__.py
│   ├── checkpoint_manager.py        # Quản lý đọc/ghi Checkpoint (Tránh crawl lại)
│   └── get_data_from_env.py         # Đọc cấu hình đường dẫn từ file .env
├── tests/                           
│   └── __init__.py
├── .env                             # File chứa biến môi trường
├── .gitignore                       # File bỏ qua các file không cần push lên Git
├── poetry.lock                      # Khóa phiên bản các thư viện dependencies
├── pyproject.toml                   # Cấu hình dự án & danh sách thư viện (Poetry)
└── README.md                        # Tài liệu dự án
```

---

## Tính năng nổi bật
1. Cơ chế Anti-Bot & Vượt Tường Lửa (Bypass 403/429):

* Giả lập TLS Fingerprint thông qua thư viện curl_cffi (đóng giả Chrome, Edge, Safari).

* Tích hợp mạng lưới Proxy (từ webshare.io) để xoay vòng IP, tránh bị block IP khi chạy trên máy ảo GCP.

2. Quản lý Tiến trình Thông minh (Checkpointing):

* Sử dụng checkpoint_manager.py để lưu trữ trạng thái các ID đã xử lý vào data/processed/. Nếu script bị ngắt giữa chừng (do cúp điện, rớt mạng), lần chạy sau sẽ tự động bỏ qua các ID đã làm xong, tiết kiệm tối đa thời gian.

3. Xử lý & Chuẩn hóa Vị trí IP (IP Processing):

* Tích hợp thư viện IP2Location và quét file .BIN cục bộ để giải mã nhanh vị trí (Quốc gia, Vùng, Thành phố) mà không cần gọi API bên ngoài.

* Data Quality Control: Tự động lọc bỏ các IP không hợp lệ, thiếu thông tin vị trí (-, n/a, N/A).

4. Tối ưu hóa Database (MongoDB):

* Phân lô dữ liệu tự động (Cursor Batching) giới hạn batch_size(100) giúp xử lý mượt mà hàng chục ngàn bản ghi mà không bị lỗi.

* Hỗ trợ Compound Indexes giúp tăng tốc độ truy vấn data từ raw_data.

---

## Hướng dẫn cài đặt (Môi trường Ubuntu/GCP)
1. Yêu cầu hệ thống (Prerequisites)
* Hệ điều hành: Linux (Ubuntu/Debian) hoặc MacOS.

* Python: >= 3.10

* Package Manager: Poetry

2. Cài đặt môi trường ảo và thư viện
Dự án sử dụng Poetry để quản lý thư viện chặt chẽ. Cài đặt các dependencies bằng lệnh sau:

```
# Ép Poetry tạo thư mục .venv ngay bên trong thư mục dự án
poetry config virtualenvs.in-project true

# Cài đặt toàn bộ thư viện từ file pyproject.toml
poetry install
```
3. Cấu hình biến môi trường (.env)
* Tạo một file .env ở thư mục gốc của dự án và khai báo các thông số sau:

```
# Cấu hình MongoDB
MONGO_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/
DB_NAME='your_db'

# Đường dẫn lưu file log
PRODUCT_NAME_PATH='UNIGAP-ProjectGlamira/data/processed/product_names.csv'

# IP data path
IP_DATA_PATH = "UNIGAP-ProjectGlamira/data/raw/ip_data/IP-COUNTRY-REGION-CITY.BIN"

#Checkpoint file path
SUCCESS_FILE_PATH = 'UNIGAP-ProjectGlamira/data/processed/success_productid.txt'
ERROR_404_FILE_PATH = 'UNIGAP-ProjectGlamira/data/processed/error_404_productid.txt'
```

4. Cấu hình Proxy (Bắt buộc khi chạy trên Cloud)
* Mở file etl/extract/product_crawler.py, tìm đến list proxy_list và cập nhật danh sách proxy của bạn (VD: từ Webshare) theo định dạng:
http://username:password@ip_address:port
 
---

## Hướng dẫn chạy Script
1. Kịch bản 1: Chạy Crawler thu thập tên sản phẩm
* Để chạy kịch bản cào dữ liệu, hãy đứng ở thư mục gốc của dự án và sử dụng lệnh poetry run:

```
poetry run python -m etl.extract.product_crawler
```

* Luồng hoạt động của Script:

  * Kết nối MongoDB, lấy ra tổng số ID cần crawl từ bảng raw_data.

  * Đối chiếu với file success_productid.txt để loại trừ các ID đã hoàn thành.

  * Chia lô nhỏ (Batch) và gọi đa luồng (Multi-threading) kết hợp với xoay vòng Proxy.

  * Xử lý logic bóc tách HTML để lấy tên sản phẩm.

  * Ghi realtime trạng thái vào file text (Checkpoint), ghi log vào file CSV và lưu data sạch vào Collection product_names trong MongoDB.

2. Kịch bản 2: Chạy Transform xử lý định vị IP
* Để chạy kịch bản xử lý định vị IP, hãy đứng ở thư mục gốc của dự án và sử dụng lệnh poetry run:

```
poetry run python -m etl.transform.ip_processing
```

* Luồng hoạt động của Script:

  * Quét collection raw_data và sử dụng Pipeline Aggregation để gom nhóm các IP duy nhất (Unique IPs).

  * Load database cục bộ .BIN của IP2Location.

  * Tiến hành đối chiếu, chuẩn hóa Data Quality (loại bỏ dữ liệu rác, thiếu thông tin).

  * Lưu đồng loạt (Bulk Insert) dữ liệu sạch vào collection ip_locations.

---

## Quản lý dữ liệu log (Reset tiến trình)
Nếu bạn muốn chạy lại dữ liệu từ con số 0, hãy xóa các file checkpoint trong thư mục processed:

```
rm data/processed/*
```

---

## Hạn chế hiện tại (Limitations)

Dù đã được tối ưu hóa, dự án hiện tại vẫn còn một số giới hạn nhất định:
1. **Phụ thuộc vào Proxy miễn phí:** Do sử dụng gói Proxy Datacenter miễn phí (Webshare), tiến trình Crawler bị giới hạn băng thông (1GB/tháng). Nếu crawl dữ liệu lớn (>10.000 trang), hệ thống sẽ trả về lỗi `402 Payment Required`.
2. **Độ nhạy cảm với cấu trúc HTML:** Logic bóc tách tên sản phẩm phụ thuộc vào cấu trúc thẻ HTML hiện tại của website (VD: `h1.page-title span`, thẻ `meta og:title`). Nếu phía website thay đổi giao diện, hàm BeautifulSoup có thể sẽ không bắt được dữ liệu.
3. **Dữ liệu IP tĩnh:** Module IP Processing đang sử dụng file Database cục bộ (`.BIN`). Nếu không tải bản cập nhật mới thường xuyên, một số dải IP mới cấp phát có thể không được nhận diện chính xác.
4. **Thiếu tính năng Tự động hóa & Lập lịch:** Hiện tại, Pipeline vẫn đang được kích hoạt thủ công (Manual trigger) thông qua các lệnh terminal trên máy ảo.

---

## Định hướng cải tiến (Future Roadmap)

Để biến dự án thành một hệ thống Data Platform cấp độ doanh nghiệp (Enterprise-level), dưới đây là các bước cải tiến tiếp theo:

1. **Tích hợp Cloud Data Lake (GCS):** Xây dựng module `export_to_gcs.py` để tự động xuất dữ liệu sạch từ MongoDB sang định dạng tối ưu (Parquet/JSONL) và đẩy lên **Google Cloud Storage (GCS)**.
2. **Đưa dữ liệu lên Data Warehouse:** Thiết lập luồng đẩy dữ liệu từ GCS vào **Google BigQuery** để phục vụ cho các team Data Analytics/BI truy vấn và làm báo cáo.
3. **Nâng cấp hạ tầng Proxy:** Chuyển đổi sang các dịch vụ Rotating Residential Proxy trả phí (như BrightData, SmartProxy) hoặc các API Scraping chuyên dụng để tăng tốc độ crawl (lên 50-100 luồng) và tránh hoàn toàn lỗi 402/403.
4. **Lên lịch tự động (Orchestration):** Đưa toàn bộ các script này vào các tool xử lý chạy tự động (Apache Airflow, ...) (thay vì chạy bash script/cronjob) để quản lý luồng chạy tự động theo ngày/tuần, tự động retry khi lỗi pipeline.
5. **Hệ thống Cảnh báo (Alerting):** Tích hợp webhook (Discord/Telegram) để tự động gửi tin báo cáo tình hình xử lý lỗi