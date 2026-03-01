# Project Glamira ETL Pipeline

## Giới thiệu dự án
Project Glamira là một Data Pipeline (ETL) hoàn chỉnh được thiết kế để tự động hóa việc trích xuất (Extract), biến đổi (Transform) và tải (Load) dữ liệu hành vi người dùng và thông tin sản phẩm từ hệ thống. 

Dự án không chỉ sở hữu module Crawler với cơ chế Anti-Bot mạnh mẽ và khả năng tự động ghi nhớ tiến trình (Checkpoint), mà còn mở rộng khả năng xử lý dữ liệu lớn bằng cách chuẩn hóa định dạng Parquet, tích hợp lưu trữ trên Google Cloud Storage (GCS) và xây dựng Data Warehouse tại Google BigQuery để sẵn sàng cho các bài toán phân tích.

---

## Cấu trúc thư mục (Project Structure)

```text
UNIGAP-ProjectGlamira/
├── config/
│   ├── __init__.py
│   └── get_mongo_connection.py      # Thiết lập kết nối đến MongoDB
├── data/
│   ├── raw/                         # Dữ liệu gốc (chưa xử lý)
│   └── processed/                   # Dữ liệu xuất ra và file Checkpoint
│       ├── crawl_result/
│       │   ├── error_404_productid.txt  # Log ID sản phẩm bị lỗi 404
│       │   ├── success_productid.txt    # Log ID sản phẩm đã crawl thành công (Checkpoint)
│       │   └── product_names.csv        # File CSV backup/log kết quả crawl
│       └── parquet_result/          # Dữ liệu đã chuyển đổi sang định dạng Parquet
│           └── checkpoints/         # Dữ liệu checkpoint để export dữ liệu
├── etl/
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── product_crawler.py       # Script crawl tên sản phẩm 
│   ├── load/
│   │   ├── __init__.py
│   │   ├── export_to_bigquery.py    # Xử lý load data từ GCS vào BigQuery
│   │   ├── export_to_gcs.py         # Upload file Parquet lên Data Lake (GCS)
│   │   └── trigger_bigquery_load.py # Trigger kích hoạt tiến trình Load từ GCS vào BigQuery
│   └── transform/
│       ├── __init__.py
│       └── ip_processing.py         # Script xử lý chuẩn hóa IP người dùng
├── src/                             # Các module tiện ích bổ trợ
│   ├── __init__.py
│   ├── checkpoint_manager.py        # Quản lý đọc/ghi Checkpoint
│   ├── get_data_from_env.py         
├── tests/                           # Monitoring & Testing Data                  
│   ├── __init__.py
│   └── raw_data_profiling.sql       # Script SQL chạy profiling trên BigQuery
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

* Sử dụng phương pháp checkpoint để lưu trữ trạng thái các ID dữ liệu đã xử lý vào data/processed/. Nếu script bị ngắt giữa chừng (do cúp điện, rớt mạng), lần chạy sau sẽ tự động bỏ qua các ID đã làm xong, tiết kiệm tối đa thời gian.

3. Xử lý & Chuẩn hóa Vị trí IP (IP Processing):

* Tích hợp thư viện IP2Location và quét file .BIN cục bộ để giải mã nhanh vị trí (Quốc gia, Vùng, Thành phố) mà không cần gọi API bên ngoài.

* Data Quality Control: Tự động lọc bỏ các IP không hợp lệ, thiếu thông tin vị trí (-, n/a, N/A).

4. Tối ưu hóa Database (MongoDB):

* Phân lô dữ liệu tự động (Cursor Batching) giới hạn batch_size(100) giúp xử lý mượt mà hàng chục ngàn bản ghi mà không bị lỗi.

* Hỗ trợ Compound Indexes giúp tăng tốc độ truy vấn data từ raw_data.

5. Data Export & Cloud Integration (GCS & BigQuery):

* Data Lake (GCS): `export_to_gcs.py` tự động hóa việc đẩy các file Parquet đã chuẩn hóa lên kho lưu trữ đám mây.

* Data Warehouse (BigQuery): Các script `export_to_bigquery.py` và `trigger_bigquery_load.py` đảm nhiệm việc định nghĩa schema và load dữ liệu từ GCS vào BigQuery, sẵn sàng cho phân tích.

6. Testing & Data Monitoring:

* Bổ sung các script SQL để kiểm tra, thống kê chất lượng dữ liệu đẩy vào BigQuery

* Sử dụng raw_data_profiling.sql để thực hiện Data Profiling trực tiếp trên BigQuery, đánh giá tính toàn vẹn và phân phối của dữ liệu sau khi load.
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
PRODUCT_NAME_PATH='UNIGAP-ProjectGlamira/data/processed/crawl_result/product_names.csv'

# IP data path
IP_DATA_PATH = "UNIGAP-ProjectGlamira/data/raw/ip_data/IP-COUNTRY-REGION-CITY.BIN"

#Checkpoint file path
SUCCESS_FILE_PATH = 'UNIGAP-ProjectGlamira/data/processed/crawl_result/success_productid.txt'
ERROR_404_FILE_PATH = 'UNIGAP-ProjectGlamira/data/processed/crawl_result/error_404_productid.txt'

#Parquet file path
PARQUET_PATH = 'UNIGAP-ProjectGlamira/data/processed/parquet_result'

#GCP config
BUCKET_NAME = 'your_bucket'
#GCP key file path (delete if running on VM)
#GCP_KEY_FILE_PATH = 'UNIGAP-ProjectGlamira/data/gcp_key/gcp_key.json'
```

4. Cấu hình Proxy (Bắt buộc khi chạy trên Cloud)
* Mở file etl/extract/product_crawler.py, tìm đến list proxy_list và cập nhật danh sách proxy của bạn (VD: từ Webshare) theo định dạng:
_http://username:password@ip_address:port_
 
5. Ủy quyền GCP:
* Trong trường hợp chạy từ local: Đảm bảo đã tải file JSON Service Account từ Google Cloud Console và đặt vào thư mục data/gcp_key/. Phân quyền Storage Object Admin và BigQuery Data Editor cho Service Account này.
* Trong trường hợp chạy trên VM của GCP: Đảm bảo VM được sử dụng Service account có quyền Storage Object Admin và BigQuery Data Editor và Access scopes chọn option Allow full access to all Cloud APIs
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

3. Kịch bản 3: Chuyển đổi dữ liệu và đẩy lên GCS
* Để chạy kịch bản đẩy dữ liệu lên GCS, hãy đứng ở thư mục gốc của dự án và sử dụng lệnh poetry run:
```
poetry run python -m etl.load.export_to_gcs
```

4. Kịch bản 4: Load dữ liệu từ GCS vào BigQuery
* Để chạy kịch bản load dữ liệu từ GCS vào BigQuery, hãy đứng ở thư mục gốc của dự án và sử dụng lệnh poetry run:
```
poetry run python -m etl.load.export_to_bigquery
```

5. Kịch bản 5: Trigger auto load dữ liệu từ GCS vào BigQuery
* Để chạy kịch bản load dữ liệu từ GCS vào BigQuery, hãy sử dụng Cloud run function và deploy script `trigger_bigquery_load.py`

6. Kịch bản 6: Kiểm tra chất lượng dữ liệu trên BigQuery
* Copy nội dung file `tests/raw_data_profiling.sql`và chạy trên giao diện BigQuery Workspace để xem các chỉ số thống kê về Null, Duplicates, và phân phối dữ liệu.
---

## Quản lý dữ liệu log (Reset tiến trình)
* Nếu bạn muốn chạy lại dữ liệu crawl từ con số 0, hãy xóa các file checkpoint trong thư mục processed/crawl_result:

```
rm data/processed/crawl_result/*
```

* Nếu bạn muốn chạy lại dữ liệu đẩy lên GCS từ con số 0, hãy xóa các file checkpoint trong thư mục processed/parquet_result/checkpoints:

```
rm data/processed/parquet_result/checkpoints/*
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

1. **Nâng cấp hạ tầng Proxy:** Chuyển đổi sang các dịch vụ Rotating Residential Proxy trả phí (như BrightData, SmartProxy) hoặc các API Scraping chuyên dụng để tăng tốc độ crawl (lên 50-100 luồng) và tránh hoàn toàn lỗi 402/403.
2. **Tự động hóa toàn phần:** Đưa toàn bộ các script này vào các tool xử lý chạy tự động (Apache Airflow, ...) (thay vì chạy bash script/cronjob) để quản lý luồng chạy tự động theo ngày/tuần, tự động retry khi lỗi pipeline.
3. **Hệ thống Cảnh báo (Alerting):** Tích hợp webhook (Discord/Telegram) để tự động gửi tin báo cáo tình hình xử lý lỗi
4. **Nâng cấp công cụ Transform (dbt):** Áp dụng dbt ngay trên BigQuery để chuyển đổi dữ liệu (ELT) từ bảng Raw sang các bảng Dim/Fact chuẩn sao (Star Schema), tối ưu cho các công cụ analytical
5. **Ứng dụng Serverless (Cloud Run):** Đóng gói các module ETL thành các Docker Container và triển khai lên Google Cloud Run.