# dbt_glamira: Data Modeling & Technical Specification

## Giới thiệu
Thư mục `dbt_glamira` chứa toàn bộ mã nguồn mô hình hóa dữ liệu (Data Modeling) sử dụng **dbt (data build tool)**. 

Nhiệm vụ của module này là thực hiện bước **Transform (T)** trong quy trình ELT: Lấy dữ liệu thô (Raw Data) từ BigQuery, làm sạch, chuẩn hóa, và xây dựng thành một Data Warehouse chuẩn mô hình Star Schema, sẵn sàng phục vụ cho các BI Dashboard trên Looker Studio.

---
## Cấu trúc thư mục
```text
dbt_glamira/
├── models/
│   ├── staging/                    # Lớp staging: Chứa các view làm sạch dữ liệu thô (đổi tên cột, ép kiểu dữ liệu). Tỉ lệ 1:1 với dữ liệu gốc.
│   │   ├── src_glamira.yml         # Khai báo mapping với dataset raw_layer trên BigQuery
│   │   └── stg_config.yml          # Khai báo mô tả và unit test cho lớp staging
│   ├── core/                       # Lớp core: Chứa các bảng dim, fact kết nối logic nghiệp vụ, xử lý tính toán phức tạp.
│   │   └── core_config.yml            # Khai báo mô tả và unit test cho lớp core
│   └── mart/                       # Lớp mart: Chứa các bảng tổng hợp sẵn sàng đẩy lên BI Tool/Looker để làm báo cáo.
│       └── mart_config.yml            # Khai báo mô tả và unit test cho lớp mart
├── macros/                         # Chứa các đoạn code Jinja/SQL tái sử dụng được nhiều lần.
│   └── generate_schema_name.sql    # Macro cấu hình tự động chia layer staging/core/mart thành các dataset riêng biệt trên BigQuery. 
├── tests/                          # Chứa các Unit Test tự định nghĩa để kiểm tra chất lượng dữ liệu.
├── seeds/                          # Chứa các file .csv dạng mapping tĩnh.
│   └── exchange_rates.csv          # File mapping tĩnh chứa tỉ giá tiền tệ
├── dbt_project.yml                 # File chứa thông tin định tuyến cấu hình của dự án.
├── packages.yml                    # Nơi khai báo các thư viện bổ trợ của dbt.
├── package-lock.yml                # File khóa phiên bản thư viện (dbt tự sinh ra sau khi chạy lệnh dbt deps).
├── .gitignore                      # File bỏ qua các file không cần push lên Git
└── README.md                       # Tài liệu hướng dẫn này.
```
---

## Kiến trúc Dữ liệu
Mô hình dữ liệu được chia thành 3 lớp (layer) vật lý nghiêm ngặt, được cấu hình tự động phân luồng schema thông qua macro `generate_schema_name`:

### 1. Lớp Staging (`schema: staging` | `materialized: view`)
Lớp tiền xử lý, đóng vai trò trích xuất và làm sạch dữ liệu từ nguồn gốc (`raw_layer`).
* **`stg_raw_data`:** Cốt lõi của hệ thống. 
  * Xử lý Unnest mảng JSON phức tạp (`cart_products`).
  * **Business Logic:** Chuẩn hóa cột giá tiền (`price`) bằng Regex (loại bỏ ký tự rác, đồng nhất dấu thập phân `.` và `,`) để ép kiểu về `NUMERIC` an toàn.
  * Tối ưu chi phí bằng Incremental Load (chỉ quét dữ liệu ngày hiện tại và hôm qua).
* **`stg_exchange_rates`:** Xử lý file cấu hình tĩnh tĩnh (Seed), ứng dụng Window Function (`LEAD`) để tìm ra ngày hết hạn (`valid_to`) của từng phiên bản tỷ giá.
* **`stg_ip_locations` & `stg_product_names`:** Chuẩn hóa tên cột để mapping với các bảng dimension.

### 2. Lớp Core (`schema: core` | `materialized: table`)
Lớp Data Warehouse chính thức, thiết kế theo mô hình Star Schema. Các bảng được liên kết với nhau bằng Khóa thay thế (Surrogate Key) sinh ra bởi thư viện `dbt_utils`.
* **Dimension Tables:**
  * `dim_customer`, `dim_location`, `dim_product`: Lưu trữ các thuộc tính phân tích. Tự động xử lý Missing Data bằng cách sinh ra các dòng mặc định `'Unknown'`.
  * `dim_date`: Bảng thời gian được tự động generate bằng mảng `GENERATE_DATE_ARRAY`, bóc tách đầy đủ các chiều phân tích (Thứ, Ngày, Tháng, Quý, Năm, Weekend).
  * `dim_exchange_rate`: Lưu trữ lịch sử tỷ giá theo dạng SCD (Slowly Changing Dimension).
* **Fact Table (`fact_sales_order`):**
  * Bảng sự kiện lưu trữ chi tiết từng dòng sản phẩm trong đơn hàng.
  * Sử dụng Incremental Load với chiến lược `insert_overwrite` và phân vùng theo ngày (`partition_by order_date`).
  * Khóa chính kết hợp `order_id` + `product_id` + `ROW_NUMBER()` để loại bỏ hoàn toàn nguy cơ trùng lặp.

### 3. Lớp Mart (`schema: mart` | `materialized: table`)
Lớp Data Mart phục vụ trực tiếp cho báo cáo.
* **`mart_sales_analysis`:** Bảng phi chuẩn hóa (Denormalized) gom toàn bộ các Dimension vào Fact. Đảm bảo Looker Studio chỉ cần query từ 1 bảng duy nhất để tối ưu hiệu suất.
* **Quy đổi tỉ giá:** `sales_amount_usd`. 
  * Công thức tính: Doanh thu gốc (`sales_amount`) nhân (`*`) với tỷ giá (`exchange_rate`).
  * **Logic Map Tỷ giá:** Tỷ giá được lấy linh hoạt dựa trên ngày thực hiện đơn hàng (`order_date` nằm giữa `valid_from` và `valid_to` của đồng tiền tương ứng).

---
## Hướng dẫn cài đặt
1. Yêu cầu hệ thống
* Python: >= 3.10
* Package Manager: Poetry
* Tài khoản Google Cloud Platform (GCP) có quyền truy cập BigQuery.

2. Thiết lập Service Account (Xác thực GCP)
* Đảm bảo bạn đã có file khóa xác thực JSON dành riêng cho dbt (ví dụ: dbt-sa-key.json)

3. Cài đặt thư viện
* Tại thư mục gốc của dự án, mở Terminal và chạy lệnh sau để Poetry tự động đọc file pyproject.toml và cài đặt dbt-bigquery cùng các thư viện liên quan:
```
poetry install
```

4. Cấu hình kết nối (profiles.yml)
Tạo hoặc mở file cấu hình dbt mặc định của hệ thống tại đường dẫn ~/.dbt/profiles.yml và điền thông tin sau:

```
glamira_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-project-name
      dataset: your-project-dataset
      threads: 4
      keyfile: .../dbt-sa-key.json       # Đường dẫn đến file xác thực GCP
      location: your-data-location       # Bắt buộc khớp với khu vực chứa dữ liệu Raw
```
---
## Hướng dẫn khởi chạy 

Để hệ thống dbt hoạt động, hãy đảm bảo bạn đã cấu hình file `profiles.yml` trỏ đúng vào Project BigQuery.

**Bước 1: Cài đặt thư viện phụ trợ**
Tải thư viện `dbt_utils` để hỗ trợ sinh Surrogate Key.
```
dbt deps
```

**Bước 2: Nạp dữ liệu tĩnh (Seeds)**
Nạp file exchange_rates.csv vào BigQuery
```
dbt seed
```

**Bước 3: Biên dịch và chạy Models**
Chạy toàn bộ pipeline từ Staging -> Core -> Mart.
```
dbt run
```

**Bước 4: Kiểm thử dữ liệu**
Kiểm tra các ràng buộc Not Null, Unique và Referential Integrity (Khóa ngoại) đã định nghĩa trong các file _config.yml
```
dbt test
```